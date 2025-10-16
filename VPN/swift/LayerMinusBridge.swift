import Foundation
import Network
import os
import Darwin


enum MemoryGauge {
    private static let q = DispatchQueue(label: "com.silentpass.vpn.memgauge")
    private static var timer: DispatchSourceTimer?
    private static var _rssBytes: Int64 = 0
    
    private static func readPhysFootprintBytes() -> Int64 {
        var info = task_vm_info_data_t()
        var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size) / 4
        let kr: kern_return_t = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count)
            }
        }
        guard kr == KERN_SUCCESS else { return _rssBytes } // 失败时沿用上次
        return Int64(info.phys_footprint)
    }
    
    static func start() {
        guard timer == nil else { return }
        let t = DispatchSource.makeTimerSource(queue: q)
        t.schedule(deadline: .now(), repeating: .seconds(1))
        t.setEventHandler {
            _rssBytes = readPhysFootprintBytes()
        }
        timer = t
        t.resume()
    }
    @inline(__always) static func rssMB() -> Int { Int(_rssBytes / 1024 / 1024) }
    @inline(__always) static func rssBytes() -> Int64 { _rssBytes }
    /// 需要“这一刻”的数值时调用（例如 CREATED/DESTROYED 立即刷一次）
    @inline(__always) static func forceRefresh() {
        q.sync { _rssBytes = readPhysFootprintBytes() }
    }
}

// MARK: - Global metrics & path state (very small footprint)
private enum BridgeGlobals {
    // 串行队列保护全局状态（避免额外依赖 atomics）
    static let q = DispatchQueue(label: "LayerMinusBridge.globals")
    static var activeConns: Int = 0
        // ENETDOWN 退避：按接口类型分别记录（避免蜂窝抖动影响 Wi-Fi）
        enum Iface { case cellular, wifi, other }
        static var pathDownUntilByIface: [Iface: UInt64] = [:]  // DispatchTime.uptimeNanoseconds
        static var enetdownHitsByIface: [Iface: Int] = [:]      // 连续触发计数（轻量）
    
        @inline(__always)
        static func iface(for path: NWPath?) -> Iface {
            guard let p = path else { return .other }
            if p.usesInterfaceType(.cellular) { return .cellular }
            if p.usesInterfaceType(.wifi)     { return .wifi }
            return .other
        }
}

@inline(__always)
private func rssMB() -> Int {
    var info = mach_task_basic_info()
    var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size / MemoryLayout<natural_t>.size)
    let kr = withUnsafeMutablePointer(to: &info) {
        $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
            task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count)
        }
    }
    return kr == KERN_SUCCESS ? Int(info.resident_size / 1024 / 1024) : -1
}

// MARK: - LayerMinusBridge as an Actor
public actor LayerMinusBridge {
    
    
    
    // 小工具：毫秒级延迟
    @inline(__always)
    private func delayMs(_ ms: Int) async {
        try? await Task.sleep(nanoseconds: UInt64(ms) * 1_000_000)
    }

    enum ConnectionState {
        case idle
        case connecting
        case connected
        case closing
        case closed
    }

    private var connectionState = ConnectionState.idle

    private func transitionTo(_ newState: ConnectionState) -> Bool {
        // Validate state transitions
        switch (connectionState, newState) {
        case (.idle, .connecting),
            (.connecting, .connected),
            (.connected, .closing),
            (.closing, .closed),
            (.connecting, .closing),
            (.idle, .closed):
            connectionState = newState
            return true
        default:
            log("Invalid state transition: \(connectionState) -> \(newState)")
            return false
        }
    }

    private func handleConnectionLoss() async {
        log("Connection loss detected")
        if connectionState == .connected {
            cancel(reason: "connection_viability_lost")
        }
    }

    private func monitorConnectionHealth() async {
        while connectionState == .connected {
            
            // Log health check
            log("Health check: state=\(connectionState) upstream=\(upstream != nil) closed=\(closed)")
            
            try? await Task.sleep(nanoseconds: 30_000_000_000) // 30s
        }
    }

    public func gracefulShutdown() async {
        guard transitionTo(.closing) else { return }
    
        cancelWatchdog()
        
        // 清理 upstream 回调并置空（先在 actor 上做）
        upstream?.stateUpdateHandler = nil
        upstream?.pathUpdateHandler = nil
        upstream?.viabilityUpdateHandler = nil
        upstream?.betterPathUpdateHandler = nil
        let upstreamConn = upstream
        upstream = nil
        let clientConn = client

        // 发送 FIN
        if let up = upstreamConn {
            try? await up.sendAsync(nil, final: true)
            try? await Task.sleep(nanoseconds: 500_000_000)
        }
        
        try? await clientConn.sendAsync(nil, final: true)
        try? await Task.sleep(nanoseconds: 100_000_000)
        
        // 在事件队列上取消
        await withCheckedContinuation { cont in
            eventQueue.async {
                upstreamConn?.cancel()
                clientConn.cancel()
                cont.resume()
            }
        }
        
        _ = transitionTo(.closed)
        onClosed?(id)
    }

    private func setupFirstByteWatchdog() {
        cancelWatchdog()
        let watchdog = DispatchSource.makeTimerSource(queue: eventQueue)

        var timeoutSec = ProcessInfo.processInfo.environment["VPN_FIRST_BYTE_TIMEOUT"]
            .flatMap(Double.init) ?? 60.0

        // 针对 Telegram 目的端放宽
        
        // 识别 Instagram 流量
        let isInstagram = targetHost.contains("instagram") ||
                          targetHost.contains("fbcdn") ||
                          targetHost.contains("facebook")
        
        if targetHost.hasSuffix("telegram.org") || targetHost.hasPrefix("149.154.") || isInstagram {
            timeoutSec = 90.0  // 介于 75–90s
        }
        
            // 蜂窝网络下，443 端口普遍更易受抖动/拥塞影响：把首包超时放宽到 90s
            if upstream?.currentPath?.usesInterfaceType(.cellular) == true && targetPort == 443 {
                timeoutSec = max(timeoutSec, 90.0)
            }

        let timeoutMs = Int(timeoutSec * 1000)
        watchdog.schedule(deadline: .now() + .milliseconds(timeoutMs))
        watchdog.setEventHandler { [weak self] in
            Task { [weak self] in
                guard let self else { return }
                if await self.checkShouldCancelForTimeout() {
                    await self.cancel(reason: "first_byte_timeout after \(timeoutSec)s")
                }
            }
        }
        firstByteWatchdog = watchdog
        watchdog.resume()
    }

    private func checkShouldCancelForTimeout() -> Bool {
        return tFirstByte == nil && connectionState == .connected && !closed
    }

    private func cancelWatchdog() {
        firstByteWatchdog?.setEventHandler {}
        firstByteWatchdog?.cancel()
        firstByteWatchdog = nil
    }

    private func attemptConnection() async -> Bool {
        guard transitionTo(.connecting) else { return false }
        
        // This should integrate with your existing connectUpstreamAndRun logic
        // Return true if connection succeeds, false otherwise
        // For now, returning placeholder
        return false
    }

    private func connectWithRetry(maxAttempts: Int = 3) async {
        var attempt = 0
        var backoffMs = 100
        
        while attempt < maxAttempts && !closed {
            attempt += 1
            
            if await attemptConnection() {
                return // Success
            }
            
            guard attempt < maxAttempts else {
                cancel(reason: "Max retry attempts reached")
                return
            }
            
            // Exponential backoff
            try? await Task.sleep(nanoseconds: UInt64(backoffMs * 1_000_000))
            backoffMs = min(backoffMs * 2, 5000) // Cap at 5 seconds
        }
    }

    // Immutable
    nonisolated public let id: UInt64
    nonisolated private let client: NWConnection
    nonisolated private let targetHost: String
    nonisolated private let targetPort: Int
    nonisolated private let verbose: Bool
    nonisolated private let onClosed: ((UInt64) -> Void)?
    nonisolated private let connectInfo: String?
    
    nonisolated private func infoTag() -> String {
        guard let s = connectInfo, !s.isEmpty else { return "" }
        return " [\(s)]"
    }

    // A dedicated queue only for NWConnection callbacks & timers.
    // Not for state synchronization (the actor handles that).
    nonisolated private let eventQueue: DispatchQueue

    // Runtime state
    private var upstream: NWConnection?
    private var usingBridge = false
    private var closed = false

    // KPI
    private var tStart: DispatchTime = .now()
    private var tHandoff: DispatchTime?
    private var tReady: DispatchTime?
    private var tFirstSend: DispatchTime?
    private var tFirstByte: DispatchTime?
    private var firstByteWatchdog: DispatchSourceTimer?

    // Counters
    private var bytesUp: Int = 0
    private var bytesDown: Int = 0

    // MARK: Init
    public init(
        id: UInt64,
        client: NWConnection,
        targetHost: String,
        targetPort: Int,
        verbose: Bool = false,
        connectInfo: String? = nil,
        onClosed: ((UInt64) -> Void)? = nil
    ) {
        self.id = id
        self.client = client
        self.targetHost = targetHost
        self.targetPort = targetPort
        self.verbose = verbose
        self.onClosed = onClosed
        self.connectInfo = connectInfo
        self.eventQueue = DispatchQueue(label: "LayerMinusBridge.\(id)", qos: .userInitiated)
        // 简化日志，避免访问 actor 隔离的方法
        let info = connectInfo.map { " [\($0)]" } ?? ""
        // 活动连接 +1 并打印 RSS
        BridgeGlobals.q.sync { BridgeGlobals.activeConns &+= 1 }
        let active = BridgeGlobals.q.sync { BridgeGlobals.activeConns }
        let msg = "🟢 CREATED LayerMinusBridge #\(id)\(info) | active_conns=\(active) rss_mb=\(rssMB()) new RSS=\(MemoryGauge.rssMB())"
        // os_log 的格式串必须是 StaticString；动态内容通过占位符传入
        os_log("%{public}@", msg)
        // 启动统一内存计量器（只会启动一次）
        MemoryGauge.start()
        
    }

//    #if DEBUG
    nonisolated private let vpnLog = OSLog(subsystem: "com.silentpass.vpn", category: "LayerMinusBridge")
    @inline(__always)
    nonisolated private func log(_ msg: @autoclosure () -> String, type: OSLogType = .info) {
        let tag = "[LayerMinusBridge #\(id)\(infoTag()) \(targetHost):\(targetPort)] "
        os_log("%{public}@", log: vpnLog, type: type, tag + msg())
    }
//    #else
//    @inline(__always)
//    private func log(_ msg: @autoclosure () -> String, type: OSLogType = .info) { }
//    #endif

    // Called by ServerConnection at handoff moment
    public func markHandoffNow() {
        tHandoff = .now()
    }

    // MARK: Start
    public func start(withFirstBody firstBodyBase64: String) {
        log("start -> \(targetHost):\(targetPort), firstBody(Base64) len=\(firstBodyBase64.count)")
        guard !closed else { return }
        guard transitionTo(.connecting) else { return }  // State transition

        tStart = .now()
        log("start -> \(targetHost):\(targetPort), firstBody(Base64) len=\(firstBodyBase64.count)")

        if let th = tHandoff {
            let ms = Double(tStart.uptimeNanoseconds &- th.uptimeNanoseconds) / 1e6
            log(String(format: "KPI handoff_to_start_ms=%.1f", ms))
        }

        guard let firstBody = Data(base64Encoded: firstBodyBase64) else {
            log("firstBody base64 decode failed")
            cancel(reason: "Invalid Base64")
            return
        }
        
        

        if firstBody.count > 0 {
            let preview = firstBody.prefix(16).map { String(format: "%02x", $0) }.joined(separator: " ")
            log("firstBody decoded bytes=\(firstBody.count), preview: \(preview)")
        }
        
        
        Task { [weak self] in
            guard let self = self else { return }
            if !(await self.clientWritableQuickCheck()) {
                await self.cancel(reason: "client not writable (pre-connect)")
                return
            }
            // 如果此前检测到 ENETDOWN，则延迟；否则直接连接
            let nowNs = DispatchTime.now().uptimeNanoseconds
           
                let delayNs: UInt64 = BridgeGlobals.q.sync {
                    let iface = BridgeGlobals.iface(for: self.client.currentPath)
                    let until = BridgeGlobals.pathDownUntilByIface[iface] ?? 0
                    return nowNs < until ? (until - nowNs) : 0
                }
            
            
            if delayNs > 0 {
                
                    let ms = Int(Double(delayNs) / 1e6)
                    let iface = BridgeGlobals.q.sync { BridgeGlobals.iface(for: self.client.currentPath) }
                    self.log("path_down backoff: iface=\(iface) delay=\(ms)ms before connect")
                
                
                self.eventQueue.asyncAfter(deadline: .now() + .nanoseconds(Int(delayNs))) { [weak self] in
                    Task { [weak self] in
                        await self?.connectUpstreamAndRun(firstBody: firstBody)
                    }
                }
            } else {
                await self.connectUpstreamAndRun(firstBody: firstBody)
            }
        }
        
    }

    // MARK: Upstream connect
    private func connectUpstreamAndRun(firstBody: Data) {

        guard upstream == nil, !usingBridge, !closed else {
            log("skip connect: upstream=\(upstream != nil) usingBridge=\(usingBridge) closed=\(closed)")
            return
        }
        
            // 再次确认客户端仍可写（极端竞态下的二次保护）
            eventQueue.async { [weak self] in
                guard let self = self else { return }
                Task {
                    if !(await self.clientWritableQuickCheck()) {
                        await self.cancel(reason: "client not writable (pre-upstream)")
                    }
                }
            }

        guard let port = NWEndpoint.Port(rawValue: UInt16(targetPort)) else {
            log("invalid port \(targetPort)")
            cancel(reason: "invalid port")
            return
        }

        let host = NWEndpoint.Host(targetHost)
        let params = NWParameters.tcp
        
            // 🔧 更稳的蜂窝表现：禁用 TFO、开启 Handover、启用 noDelay
            if let tcp = params.defaultProtocolStack.internetProtocol as? NWProtocolTCP.Options {
                tcp.enableFastOpen = false   // 避免部分运营商/站点的 TFO 兼容问题
                tcp.noDelay = true
            }
        //params.multipathServiceType = .handover  // 蜂窝抖动时更顺滑地切换基站/承载
        params.allowLocalEndpointReuse = true   // 🔹减少频繁重连时的本地端口占用/WAIT 状态拖累

        log("Connecting to upstream \(targetHost):\(targetPort)")
        let up = NWConnection(host: host, port: port, using: params)
        upstream = up

        up.stateUpdateHandler = { [weak up] st in
            Task { [weak self] in
                guard let self else { return }
                await self.handleUpstreamState(st, up: up, firstBody: firstBody) // 内部已有 guard !closed
            }
        }

        up.start(queue: eventQueue)
    }
    
    private func clientWritableQuickCheck() async -> Bool {
        if closed { return false }
        do {
            try await client.sendAsync(Data())   // 零长度写：仅用于探测
            return true
        } catch {
            log("probe: client not writable in Bridge (\(error))")
            return false
        }
    }

    private func handleUpstreamState(_ st: NWConnection.State, up: NWConnection?, firstBody: Data) async {
        guard !closed else { return }

        guard connectionState == .connecting || connectionState == .connected else {
            log("ignore upstream state \(st) in state=\(connectionState)")
            return
        }

        
        switch st {
        case .ready:
            guard !usingBridge else { return }   // ✅ 防止重复启动桥接
            log("upstream ready to \(targetHost):\(targetPort)")
            tReady = .now()
            usingBridge = true
            _ = transitionTo(.connected)  // State transition
            
            // Start health monitoring
            // Task { await monitorConnectionHealth() }

            // 首包完成后再进入直通
            if !firstBody.isEmpty {
                await sendFirstBody(firstBody)
                // ✅ 若首包失败触发了 cancel，这里直接返回，避免继续桥接
                if closed { return }
            }

            if let up = up {
                up.betterPathUpdateHandler = { [weak self] available in
                    Task { [weak self] in if let self, available { await self.log("Better path available, consider migration") } }
                }
                up.viabilityUpdateHandler = { [weak self] viable in
                    Task { [weak self] in if let self { await self.log("viable=\(viable)") } }
                }
                up.pathUpdateHandler = { [weak self] path in
                    Task { [weak self] in
                        if let self {
                            let iface = BridgeGlobals.iface(for: path)
                            self.log("path status=\(path.status) iface=\(iface) expensive=\(path.isExpensive) constrained=\(path.isConstrained)")
                        }
                    }
                }
            }
            if let up = up {
                await bridgeConnections(client: client, remote: up)
            }

        case .waiting(let error):
            guard !closed else { return }
            log("upstream waiting: \(error)")

        case .failed(let error):
            guard !closed else { return }
            log("upstream failed: \(error)")
            
            // 检查特定错误代码
            if case .posix(let code) = error {
                switch code {
                case .ECONNRESET:
                    log("Connection reset detected (ECONNRESET), no retry in DIRECT mode")
                case .ENETDOWN:
                    /*
                    // ‼️ BUG FIX: Disable the global backoff mechanism.
                    // This logic incorrectly penalized all new connections, regardless of their network path.
                    // A failure on the cellular interface should not block a new connection over Wi-Fi.
                    let jitterMs = 400 + Int(arc4random_uniform(500)) // [400,900)
                    let now = DispatchTime.now().uptimeNanoseconds
                    let currentUntil = BridgeGlobals.q.sync { BridgeGlobals.pathDownUntil }
                    // 若已设置且距离现在 < 3s，则不刷新，避免持续粘滞
                    let newUntil: UInt64 = (currentUntil > now && currentUntil - now < 3_000_000_000)
                        ? currentUntil
                        : now &+ UInt64(jitterMs) * 1_000_000
                    BridgeGlobals.q.sync { BridgeGlobals.pathDownUntil = newUntil }
                    */
                    log("Network down (ENETDOWN) detected. Canceling this connection without setting a global backoff.")
                    await delayMs(50)
                    if !closed { cancel(reason: "path_down(enetdown)") }
                    return
                case .ECANCELED:
                    log("ECANCELED → soft-delay cancel")
                    await delayMs(150)               // 给 in-flight 的 completion 一个窗口
                    if !closed { cancel(reason: "upstream failed(ECANCELED): \(error)") }
                    return
                default:
                    break
                }
            }
            await delayMs(50) // 普通失败也稍微缓一缓
            if !closed { cancel(reason: "upstream failed: \(error)") }

        case .cancelled:
            guard !closed else { return }
            log("upstream cancelled")
            await delayMs(100)
            if !closed { cancel(reason: "upstream cancelled (delayed)") }

        default:
            log("upstream state: \(st)")
        }
    }


    // MARK: Pipe bridge (true backpressure)
    private func bridgeConnections(client: NWConnection, remote: NWConnection) async {
       
        var c2sHadError = false
        var s2cHadError = false
        await withTaskGroup(of: Void.self) { group in
            group.addTask { [weak self] in
                guard let self = self else { return }
                do {
                    var chunkCount = 0
                    // 传递 bridgeId 和 connectInfo
                    for try await data in client.receiveStream(
                        maxLength: GLOBAL_MAX_BUFFER,
                        bridgeId: self.id,
                        connectInfo: self.connectInfo
                    ) {
                        chunkCount += 1
                        await self.addUpBytes(data.count)
                        
                        if chunkCount % 100 == 0 {
                            await self.log("C->S chunks:\(chunkCount) bytes:\(data.count)")
                        }
                        
                        try await remote.sendAsync(data)
                    }
                } catch {
                    await self.log("C->S error (benign, no immediate cancel): \(error)")
                    c2sHadError = true
                    return
                }
                try? await remote.sendAsync(nil, final: true)
            }
            
            group.addTask { [weak self] in
                guard let self = self else { return }
                do {
                    // 同样传递 bridgeId 和 connectInfo
                    for try await data in remote.receiveStream(
                        maxLength: GLOBAL_MAX_BUFFER,
                        bridgeId: self.id,
                        connectInfo: self.connectInfo
                    ) {
                        await self.onFirstDownBytes(n: data.count)
                        try await client.sendAsync(data)
                    }
                } catch {
                    await self.log("S->C error (benign, no immediate cancel): \(error)")
                    s2cHadError = true
                    return
                }
                try? await client.sendAsync(nil, final: true)
            }
        }

        // 统一收尾：不再固定 500ms cancel，改为“等待另一侧结束或空闲超时”
        if !closed {
            let bothOk = !(c2sHadError || s2cHadError)
            let reason = bothOk ? "bridge completed" : "bridge half-close"

            // 等待对端在短期内自然结束（尤其是大图/视频上行完成）
            var waited = 0
            while !closed && waited < 20_000 { // 20s 上限
                await delayMs(200)
                waited += 200
                // 探測 client 是否可寫；若不可寫，提早退出
                    if !(await clientWritableQuickCheck()) {
                        cancel(reason: "client gone during idle wait (\(waited)ms)")
                        break
                    }
                // 如果真的都空了，跳出
                // （这里保持简单：由对端 EOF 驱动结束；无额外探针）
            }

            if !closed { cancel(reason: reason + " waited_ms=\(waited)") }
        }
    
    }

    // Helpers (actor-isolated mutations)
    private func addUpBytes(_ n: Int) {
        bytesUp += n
    }
    
    private func onFirstDownBytes(n: Int) {
        if tFirstByte == nil {
            tFirstByte = .now()
            // stop watchdog - using new method
            cancelWatchdog()

            // KPI: TTFB
            if let fb = tFirstByte {
                let ttfbMs = Double(fb.uptimeNanoseconds &- tStart.uptimeNanoseconds) / 1e6
                log(String(format: "KPI immediate TTFB_ms=%.1f", ttfbMs))
                if let ts = tFirstSend {
                    let segMs = Double(fb.uptimeNanoseconds &- ts.uptimeNanoseconds) / 1e6
                    log(String(format: "KPI firstSend_to_firstRecv_ms=%.1f", segMs))
                }
            }
        }
        bytesDown += n
    }

    // MARK: First body
    private func sendFirstBody(_ data: Data) async {
        guard let up = upstream, !closed else { return }
        do {
            try await up.sendAsync(data)
            tFirstSend = .now()
            log("sent firstBody")

            // Use the new setupFirstByteWatchdog method
            setupFirstByteWatchdog()
        } catch {
            cancel(reason: "firstBody send error: \(error)")
        }
    }

    // MARK: Cancel / KPI
    public func cancel(reason: String) {
        guard !closed else { return }
            
            if connectionState != .closing && connectionState != .closed {
                _ = transitionTo(.closing)
            }
            closed = true
            cancelWatchdog()
            kpiLog(reason: reason)
            
            // 清理 handlers（在 actor 上下文中，同步执行）
            upstream?.stateUpdateHandler = nil
            upstream?.pathUpdateHandler = nil
            upstream?.viabilityUpdateHandler = nil
            upstream?.betterPathUpdateHandler = nil
            
            // 保存引用
            let upToCancel = upstream
            let clientToCancel = client
            upstream = nil
            
            // 在事件队列上取消连接
            eventQueue.async {
                upToCancel?.cancel()
                clientToCancel.cancel()
            }
            
            _ = transitionTo(.closed)
            onClosed?(id)
            // 活动连接 -1 并打印 RSS
            let left = BridgeGlobals.q.sync { BridgeGlobals.activeConns &-= 1; return BridgeGlobals.activeConns }
            log("CANCEL trigger id=\(id) reason=\(reason) | active_conns=\(left) rss_mb=\(rssMB()) RSS=\(MemoryGauge.rssMB())")

    }

    private func kpiLog(reason: String) {
        let now = DispatchTime.now()
        let durMs = Double(now.uptimeNanoseconds - tStart.uptimeNanoseconds) / 1e6
        var extra = ""
        if let fb = tFirstByte {
            let fbMs = Double(fb.uptimeNanoseconds - tStart.uptimeNanoseconds) / 1e6
            extra += String(format: " first_byte_ms=%.1f", fbMs)
        }
        extra += " half_close_events=\(closed ? 1 : 0)"
        log("KPI host=\(targetHost):\(targetPort) reason=\(reason) up_bytes=\(bytesUp) down_bytes=\(bytesDown) dur_ms=\(String(format: "%.1f", durMs))\(extra)")
    }

    deinit {
        let needsCleanup = !closed
        if needsCleanup {
            // 立即清理所有 handlers（这是线程安全的）
            upstream?.stateUpdateHandler = nil
            upstream?.pathUpdateHandler = nil
            upstream?.viabilityUpdateHandler = nil
            upstream?.betterPathUpdateHandler = nil
            
            // 保存引用
            let upToCancel = upstream
            let clientToCancel = client
            
            // 异步取消（避免死锁）
            eventQueue.async {
                upToCancel?.cancel()
                clientToCancel.cancel()
            }
        }
        // 这里不再做 activeConns--（由 cancel 统一扣减），仅打印当前 RSS/活动数
        log("🔵 DEINIT LayerMinusBridge #\(id), cleanup needed: \(needsCleanup) | active_conns=\(BridgeGlobals.q.sync { BridgeGlobals.activeConns }) rss_mb=\(rssMB()) RSS=\(MemoryGauge.rssMB())")

    }

}


// MARK: - Async wrappers & pull-driven receive (no prefetch → true backpressure)


struct NWReceiveSequence: AsyncSequence {
    typealias Element = Data
    
    struct Iterator: AsyncIteratorProtocol {
        // MARK: - Local static logger just for Iterator
        #if DEBUG
            private static let logger = OSLog(subsystem: "com.silentpass.vpn", category: "NWReceive")
            @inline(__always)
            private static func slog(_ msg: @autoclosure () -> String, type: OSLogType = .debug) {
                os_log("%{public}@", log: logger, type: type, msg())
            }
        #else
            @inline(__always) private static func slog(_ msg: @autoclosure () -> String, type: OSLogType = .debug) { }
        #endif
        
        let conn: NWConnection
        private let baseMax: Int
        private var consecutiveEmptyReads = 0
        private var consecutiveDataReads = 0
        private var currentBufferSize = 64 * 1024
        
        // 统计小块累计量，辅助触发增长
        private var accumBytes: Int = 0
        
        private let bridgeId: UInt64
        private let connectInfo: String?
        
        // 首包暖机时间戳（纳秒，DispatchTime.now().uptimeNanoseconds）
        private var firstByteAt: UInt64? = nil
        
        // 缓冲区配置
        // 允许更低的最小缓冲以发挥“暖机跳变”的作用
        private let minBuffer = 64 * 1024
        
        private let maxBuffer = GLOBAL_MAX_BUFFER
        private let growthStep = 512 * 1024
        private let memoryWarningThreshold = 45 * 1024 * 1024
        
        init(conn: NWConnection, max: Int, bridgeId: UInt64, connectInfo: String?) {
            self.conn = conn
            self.baseMax = max
            self.bridgeId = bridgeId
            self.connectInfo = connectInfo
            
        }
        
        private func makeLogTag() -> String {
            let info = connectInfo.map { " [\($0)]" } ?? ""
            return "[LayerMinusBridge \(bridgeId)\(info)]"
        }
        
        
        mutating func next() async throws -> Data? {
            // 动态调整缓冲区大小
            adjustBufferSize()

            while let d = try await conn.recv(max: Swift.min(currentBufferSize, baseMax)) {
                if !d.isEmpty {
                    
                    // 首个下行字节：记录暖机起点，并进行一次性“跃迁到 64KB”（若当前更小）
                    if firstByteAt == nil {
                        firstByteAt = DispatchTime.now().uptimeNanoseconds
                        if currentBufferSize < 64 * 1024 {
                        
                            Self.slog("\(makeLogTag()) 🔵🔵🔵 Warmup jump: \(currentBufferSize/1024)KB → 64KB after first byte")
                            
                            currentBufferSize = 64 * 1024

                        }
                    }

                    
                    consecutiveEmptyReads = 0
                    consecutiveDataReads += 1
                    
                    
                    // 统计小块总量 —— 为累计触发增长做准备
                    accumBytes &+= d.count
                    
                    // 放宽额外计数触发：由 3/4 改为 1/2
                    if d.count >= currentBufferSize * 1 / 2 {
                        consecutiveDataReads += 2
                    }
                    
                    return d
                } else {
                    consecutiveEmptyReads += 1
                    consecutiveDataReads = 0
                    
                    if consecutiveEmptyReads > 1 {
                        let sleepMs = Swift.min(consecutiveEmptyReads, 10)
                        try? await Task.sleep(nanoseconds: UInt64(sleepMs * 1_000_000))
                    }
                }
            }
            return nil
        }
        
        private mutating func adjustBufferSize() {
            // 获取当前内存使用量
            MemoryGauge.forceRefresh()
            let currentMemory = MemoryGauge.rssBytes()   // bytes（phys_footprint）
            
            // 暖机窗口：首包后 2 秒内提高收缩下限到 32KB
            let nowNs = DispatchTime.now().uptimeNanoseconds
            let inWarmup: Bool = {
                if let t0 = firstByteAt {
                    return nowNs &- t0 < 2_000_000_000 // 2s
                }
                return false
            }()
            
            // 暖机期希望维持更高的下限（避免首屏被过度收缩）
            let warmupMin = 128 * 1024
            
            // 内存压力检查
            if currentMemory >= memoryWarningThreshold {
                // 达到警戒线：仅在当前缓冲 > 256KB 时减半；<=128KB 保持不变
                if currentBufferSize > 256 * 1024 {
                    
                    let floor = inWarmup ? Swift.max(minBuffer, warmupMin) : minBuffer
                    let newSize = Swift.max(currentBufferSize / 2, floor)
                    
                    
                    Self.slog("\(makeLogTag()) 🔵🔵🔵 currentMemory Memory pressure RSS \(currentMemory/(1024*1024))MB ≥ \(memoryWarningThreshold/(1024*1024))MB: "
                        + "shrink \(currentBufferSize/1024)KB → \(newSize/1024)KB")
                    
                    currentBufferSize = newSize
                } else {
                    
                    Self.slog("\(makeLogTag()) 🔵🔵🔵 currentMemory Memory pressure RSS\(currentMemory/(1024*1024))MB ≥ \(memoryWarningThreshold/(1024*1024))MB: "
                        + "buffer kept \(currentBufferSize/1024)KB (≤128KB)")
                    
                }
                return
            }
            
            // 增长逻辑：仅在内存充足时；支持“小块累计触发”
            if currentBufferSize < maxBuffer &&
                (consecutiveDataReads >= 3 || accumBytes >= currentBufferSize * 2) {
                let projectedMemory = currentMemory + Int64(growthStep)
                if projectedMemory < memoryWarningThreshold {
                    let newSize = Swift.min(currentBufferSize + growthStep, maxBuffer)
                    if newSize != currentBufferSize {
                        
                        Self.slog("\(makeLogTag()) 🔵🔵🔵 Buffer growing: \(currentBufferSize/1024)KB → \(newSize/1024)KB (currentMemory RSS: \(currentMemory/(1024*1024))MB)")
                        
                        currentBufferSize = newSize
                        
                        // 增长后清理一部分累计，避免连锁暴涨
                        accumBytes = accumBytes / 2
                    }
                } else {
                    Self.slog("\(makeLogTag()) 🔵🔵🔵 Growth blocked: would exceed memory threshold (current RSS: \(currentMemory/(1024*1024))MB)")
                    
                }
            }
            // 缩减逻辑
            // 收缩逻辑：暖机期禁收缩；非暖机更温和且提高空读阈值
            else if !inWarmup && consecutiveEmptyReads >= 4 && currentBufferSize > minBuffer {
                let floor = minBuffer
                let targetSize: Int
                
                if currentBufferSize > 256 * 1024 {
                    // >256KB：改为陡降 1/2
                    targetSize = Swift.max(currentBufferSize / 2, floor)
                } else {
                    // ≤256KB：沿用旧策略（6~8→3/4，9~12→2/3，其它→1/2）
                    switch consecutiveEmptyReads {
                        case 6...8:
                        targetSize = Swift.max(currentBufferSize * 3 / 4, floor)
                        case 9...12:
                        targetSize = Swift.max(currentBufferSize * 2 / 3, floor)
                        default:
                        targetSize = Swift.max(currentBufferSize / 2, floor)
                    }
                }
                
                if targetSize != currentBufferSize {
                    Self.slog("\(makeLogTag()) 🔵🔵🔵 Buffer shrinking: \(currentBufferSize/1024)KB → \(targetSize/1024)KB (idle reads \(consecutiveEmptyReads))")
                    currentBufferSize = targetSize
                }
            }
            
            
            if inWarmup && consecutiveEmptyReads >= 6 {
                Self.slog("\(makeLogTag()) 🔵🔵🔵 Warmup(no-shrink): empty=\(consecutiveEmptyReads) keep=\(currentBufferSize/1024)KB")
            }
            
            
            // ⏫ 时间边界保障：首包后 0.6s/1.2s 内至少拉到 256/512KB
            if let t0 = firstByteAt {
                    let elapsed = nowNs &- t0
                if elapsed > 600_000_000 && currentBufferSize < 256 * 1024 {
                    currentBufferSize = 256 * 1024
               
                    Self.slog("\(makeLogTag()) 🔵🔵🔵 Warmup time-guard: bump → 256KB")
                
                } else if elapsed > 1_200_000_000 && currentBufferSize < 512 * 1024 {
                    currentBufferSize = 512 * 1024
                
                    Self.slog("\(makeLogTag()) 🔵🔵🔵 Warmup time-guard: bump → 512KB")
                
                }
            }
        }
    }
    
    
    let conn: NWConnection
    let max: Int
    let bridgeId: UInt64
    let connectInfo: String?
    
    init(conn: NWConnection, max: Int, bridgeId: UInt64, connectInfo: String?) {
        self.conn = conn
        self.max = max
        self.bridgeId = bridgeId
        self.connectInfo = connectInfo
    }
    
    func makeAsyncIterator() -> Iterator {
        Iterator(conn: conn, max: max, bridgeId: bridgeId, connectInfo: connectInfo)
    }
}

private let GLOBAL_MAX_BUFFER = 3 * 1024 * 1024
extension NWConnection {
    
    /// Pull-driven stream: next() triggers exactly one receive
    func receiveStream(maxLength: Int, bridgeId: UInt64 = 0, connectInfo: String? = nil) -> NWReceiveSequence {
        NWReceiveSequence(conn: self, max: GLOBAL_MAX_BUFFER, bridgeId: bridgeId, connectInfo: connectInfo)
    }
}

extension NWConnection {
    func recv(max: Int) async throws -> Data? {
        try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { cont in
                self.receive(minimumIncompleteLength: 1, maximumLength: max) { data, _, isComplete, error in
                    if let error {
                        cont.resume(throwing: error)
                    } else if isComplete {
                        cont.resume(returning: nil)  // EOF
                    } else {
                        cont.resume(returning: data ?? Data())
                    }
                }
            }
        }, onCancel: {
            // 注意：这里违反了线程模型，但作为防止资源泄漏的最后防线
            // 正常情况下不应该执行到这里
            self.cancel()
        })
    }
    
    func sendAsync(_ data: Data?, final: Bool = false) async throws {
        try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                let completion: NWConnection.SendCompletion = .contentProcessed { error in
                    if let error {
                        cont.resume(throwing: error)
                    } else {
                        cont.resume(returning: ())
                    }
                }
                
                if final && data == nil {
                    self.send(content: nil, contentContext: .finalMessage, isComplete: true, completion: completion)
                } else {
                    self.send(content: data, contentContext: .defaultMessage, isComplete: false, completion: completion)
                }
            }
        }, onCancel: {
            // 注意：这里违反了线程模型，但作为防止资源泄漏的最后防线
            // 正常情况下不应该执行到这里
            self.cancel()
        })
    }
}
