import Foundation
import Network
import os

enum L {
    static func lm(_ s: @autoclosure () -> String) { print("[LayerMinusBridge] \(s())") }
    static func sc(_ s: @autoclosure () -> String) { print("[ServerConnection] \(s())") }
}

public final class LayerMinusBridge {
    
    public var onFirstByteTTFBMs: ((Double) -> Void)?
    public var onNoResponse: (() -> Void)?
    
    // —— 100-Continue 兼容：观测到上游 100 后，直到客户端真正发出实体前，避免过早 half-close 上游
    private var saw100Continue = false
    private var bodyBytesAfter100: Int = 0

    // —— Speedtest 特征流：直发与“上行卡住”探测
    private var smallC2UEvents = 0
    private var bytesUpAtFirstSend = 0
    private var uploadStuckTimer: DispatchSourceTimer?
    private var uploadStuck = false
    @inline(__always)
    private var isSpeedtestTarget: Bool {
        // 仅对 8080/443 且域名命中测速特征的连接生效
        let h = targetHost.lowercased()
        return (targetPort == 8080 || targetPort == 443) &&
               (h.hasSuffix("ooklaserver.net")
                 || h.hasSuffix("speedtest.net")
                 || h.contains("measurementlab")
                 || h.contains("mlab"))
    }

    private var sendSeq: UInt64 = 0
    private var inflight = Set<UInt64>()
    
    // --- 上行(c->u)微批：64KB 或 5ms 触发 ---
    private var cuBuffer = Data()
    private var cuFlushTimer: DispatchSourceTimer?
    private let CU_FLUSH_BYTES = 32 * 1024
    private let CU_FLUSH_MS = 12
    // 上限防护
    private let CU_BUFFER_LIMIT = 256 * 1024
    // 当定时到点但缓冲小于该值时，允许再延一次以攒到更“胖”的报文
    private let CU_MIN_FLUSH_BYTES = 8 * 1024
    private let CU_EXTRA_MS = 10
    
    private let connectInfo: String?
    // 路由元信息（用于统计/打点）
    
    public let id: UInt64
    private let client: NWConnection
    private let targetHost: String
    private let targetPort: Int
    private let verbose: Bool
    
    // --- KPI & 半关闭守护 ---
    private var tStart: DispatchTime = .now()
    private var tReady: DispatchTime?
    private var tFirstByte: DispatchTime?
    private var bytesUp: Int = 0
    private var bytesDown: Int = 0
    private var drainTimer: DispatchSourceTimer?
    // 半关闭后的“空闲超时”窗口：只要仍有对向数据活动就续期，空闲 >= 25s 才收尾
    private let drainGrace: TimeInterval = 25.0
    // 记录哪一侧先 EOF，用于判断是否处于 half-close
    private var eofUpstream = false
    private var eofClient = false
    
    
    private let onClosed: ((UInt64) -> Void)?
    
    private let queue: DispatchQueue
    private var upstream: NWConnection?
    private var closed = false
    
    // —— 生存门闸：所有回调入口先判存活，避免已取消后仍访问资源
    private let stateLock = NSLock()
    @inline(__always) private func alive() -> Bool {
        stateLock.lock(); defer { stateLock.unlock() }
        return !closed
    }
    
    // 数据面日志（仅在 verbose 为 true 时打印）
    @inline(__always)
    private func vlog(_ msg: String) {
        guard verbose else { return }
        log(msg)
    }
    
    
    init(
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
        self.queue = DispatchQueue(label: "LayerMinusBridge.\(id)", qos: .userInitiated)
        // 简单的生命周期日志
        NSLog("🟢 CREATED LayerMinusBridge #\(id) for \(targetHost):\(targetPort)\(infoTag())")
    }
    
    deinit {
        log("🔴 DESTROYED LayerMinusBridge #\(id)")
        if !closed {
            print("⚠️ WARNING: LayerMinusBridge #\(id) destroyed without proper closing!")
        }
    }
    
    @inline(__always)
    private func log(_ msg: String) {
        NSLog("[LayerMinusBridge \(id), \(infoTag())] %@", msg)
    }
    
    // --- 追加KPI ---
    private var tHandoff: DispatchTime?
    private var tFirstSend: DispatchTime?
    
    private func infoTag() -> String {
        guard let s = connectInfo, !s.isEmpty else { return "" }
        return " [\(s)]"
    }
    
    public func start(withFirstBody firstBodyBase64: String) {
        queue.async { [weak self] in
            guard let self = self else { return }
            
            guard self.alive() else { return }
            
            // KPI: 记录会话起点，用于计算 hsRTT / TTFB / 总时长
            self.tStart = .now()
            self.log("start -> \(self.targetHost):\(self.targetPort), firstBody(Base64) len=\(firstBodyBase64.count)")
            
            
            
            // KPI: handoff -> start（应用层排队/解析耗时）
            if let th = self.tHandoff {
                let ms = Double(self.tStart.uptimeNanoseconds &- th.uptimeNanoseconds) / 1e6
                self.log(String(format: "KPI handoff_to_start_ms=%.1f", ms))
            }
            
            guard let firstBody = Data(base64Encoded: firstBodyBase64) else {
                
                self.log("firstBody base64 decode failed")
                self.queue.async { self.cancel(reason: "Invalid Base64") }
                return
            }
            
            self.log("firstBody decoded bytes=\(firstBody.count)")
            
            // 打印前几个字节用于调试
            if firstBody.count > 0 {
                let preview = firstBody.prefix(16).map { String(format: "%02x", $0) }.joined(separator: " ")
                self.log("firstBody preview: \(preview)")
            }
            
            // 开始连接上游并转发数据
            self.connectUpstreamAndRun(firstBody: firstBody)
            
        }
    }
    
    
    
    
    public func cancel(reason: String) {
        // 幂等关闭，防止重入/竞态
        stateLock.lock()
        if closed { stateLock.unlock(); return }
        closed = true
        stateLock.unlock()
        
        // 停止所有守护/缓冲（置空 handler 防循环引用）
        firstByteWatchdog?.setEventHandler {}
        firstByteWatchdog?.cancel(); firstByteWatchdog = nil
        drainTimer?.setEventHandler {}
        drainTimer?.cancel(); drainTimer = nil

        uploadStuckTimer?.setEventHandler {}
        uploadStuckTimer?.cancel(); uploadStuckTimer = nil

        cuFlushTimer?.setEventHandler {}
        cuFlushTimer?.cancel(); cuFlushTimer = nil
        
        
        
        cuBuffer.removeAll(keepingCapacity: false)
        
        kpiLog(reason: reason)
        log("cancel: \(reason)")
        
        // 关闭上游连接
        upstream?.cancel()
        upstream = nil
        
        // 关闭客户端连接
        client.cancel()
        
        // 通知 ServerConnection
        onClosed?(id)
    }
    
    private func connectUpstreamAndRun(firstBody: Data) {
        guard let port = NWEndpoint.Port(rawValue: UInt16(targetPort)) else {
            log("invalid port \(targetPort)")
            cancel(reason: "invalid port")
            return
        }
        
        let host = NWEndpoint.Host(targetHost)
        let params = NWParameters.tcp
        
        // // 配置 TCP 参数（更稳妥的默认：443/80 保持 noDelay；keepalive 稍放宽）
        if let tcp = params.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
            let preferNoDelay = (targetPort == 443 || targetPort == 80 || targetPort == 8080)
            tcp.noDelay = preferNoDelay
            tcp.enableKeepalive = true
            tcp.keepaliveIdle = 30
        }
        
        log("Connecting to upstream \(targetHost):\(targetPort)")
        
        let up = NWConnection(host: host, port: port, using: params)
        upstream = up
        
        up.stateUpdateHandler = { [weak self] st in
            guard let self = self else { return }
            
            guard self.alive() else { return }
            
            
            switch st {
            case .ready:
                self.log("upstream ready to \(self.targetHost):\(self.targetPort)")
                // KPI: 记录上游就绪时刻（握手完成）
                self.tReady = .now()
                // 发送首包到上游
                if !firstBody.isEmpty {
                    
                    // 记录首包发送完成时刻（用于 firstSend -> firstRecv）
                    self.sendToUpstream(firstBody, remark: "firstBody")
                }
                
                // 启动双向数据泵
                self.pumpClientToUpstream()
                self.pumpUpstreamToClient()
                
            case .waiting(let error):
                self.log("upstream waiting: \(error)")
                
            case .failed(let error):
                self.log("upstream failed: \(error)")
                self.queue.async { self.cancel(reason: "upstream failed") }
                
            case .cancelled:
                self.log("upstream cancelled")
                self.queue.async { self.cancel(reason: "upstream cancelled") }
                
            default:
                self.log("upstream state: \(st)")
            }
        }
        
        up.start(queue: queue)
    }

	// 来自 ServerConnection 的 handoff 瞬间标记
	public func markHandoffNow() {
		queue.async { [weak self] in
			self?.tHandoff = .now()
		}
	}
    
    private func scheduleCUFlush(allowExtend: Bool = true) {
        cuFlushTimer?.cancel()
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(CU_FLUSH_MS))
        
        t.setEventHandler { [weak self] in
            guard let s = self, s.alive() else { return }
            
            // 若到点仍很小且允许再延一次，则小幅延时后再冲刷
            if !s.closed, allowExtend, s.cuBuffer.count > 0, s.cuBuffer.count < s.CU_MIN_FLUSH_BYTES {
                s.cuFlushTimer?.cancel()
                let t2 = DispatchSource.makeTimerSource(queue: s.queue)
                t2.schedule(deadline: .now() + .milliseconds(s.CU_EXTRA_MS))
                
                t2.setEventHandler { [weak s] in
                    guard let s = s, s.alive() else { return }
                    s.flushCUBuffer()
                }
                
                
                s.cuFlushTimer = t2
                t2.resume()
            } else {
                s.flushCUBuffer()
            }
        }
        cuFlushTimer = t
        t.resume()
    }

    private func flushCUBuffer(force: Bool = false) {
        guard !cuBuffer.isEmpty, !closed else { return }
        if !force, cuBuffer.count < CU_MIN_FLUSH_BYTES { return }
        let payload = cuBuffer
        cuBuffer.removeAll(keepingCapacity: true)
        self.sendToUpstream(payload, remark: "c->u")
    }
    
    private func appendToCUBuffer(_ d: Data) {
        cuBuffer.append(d)
        if cuBuffer.count >= CU_BUFFER_LIMIT {
            flushCUBuffer(force: true)
        }
    }
    
    private func pumpClientToUpstream() {
        if closed { return }
        
        client.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in
            
            
            guard let self = self else { return }
            if !self.alive() { return }
            
            if let err = err {
                self.log("client recv err: \(err)")
                
                // 避免在 receive 回调栈内同步取消，引发重入/竞态
                self.queue.async { self.cancel(reason: "client recv err") }
                
                
                
                
                return
            }
            
            if let d = data, !d.isEmpty {
                
                self.vlog("recv from client: \(d.count)B")

                // —— 仅对测速流的 30–100B 小块“直发”，其余仍按微批策略处理
                if self.isSpeedtestTarget && (30...100).contains(d.count) {
                    self.smallC2UEvents &+= 1
                    self.sendToUpstream(d, remark: "c->u")
                } else {
                    // 其它：累积到缓冲，达到阈值立即冲刷，否则启动短定时器
                    self.cuBuffer.append(d)
                }


                // 若已收到上游的 100-Continue，则统计 100 之后客户端是否真的发了实体
                if self.saw100Continue {
                    self.bodyBytesAfter100 &+= d.count
                }


                if !self.isSpeedtestTarget {
                    if self.cuBuffer.count >= self.CU_FLUSH_BYTES { self.flushCUBuffer() }
                    else { self.scheduleCUFlush() }
                }

                if self.isSpeedtestTarget && (30...100).contains(d.count) {
                    self.sendToUpstream(d, remark: "c->u")
                } else {
                    self.appendToCUBuffer(d)
                }
            }
            
            
            if isComplete {
                self.log("client EOF")
                
                self.eofClient = true

                // 先把缓冲冲刷出去，再进入排水期/或酌情 half-close
                self.flushCUBuffer()


                // ★ 若已观测到上游发过 100-Continue，但客户端尚未发送任何实体，
                //   暂不 half-close 上游（避免把请求体“宣告写完”）；仅进入空闲计时，等待自然收尾
                if (self.saw100Continue && self.bodyBytesAfter100 == 0) || self.uploadStuck {
                    self.scheduleDrainCancel(hint: "client EOF (deferred half-close due to 100-Continue)")
                } else {
                    // 常规路径：half-close 上游写端，再进入排水
                    self.upstream?.send(content: nil, completion: .contentProcessed({ _ in }))
                    self.scheduleDrainCancel(hint: "client EOF")
                }
                
                return
            }
            
            // 继续接收
            self.pumpClientToUpstream()
        }
    }
    
    private func scheduleDrainCancel(hint: String) {
        // half-close 空闲计时器：每次调用都会重置，确保有活动就不收尾
        drainTimer?.cancel()
        
        let timer = DispatchSource.makeTimerSource(queue: queue)
        
        
        timer.schedule(deadline: .now() + drainGrace)
        timer.setEventHandler { [weak self] in
            guard let s = self, s.alive() else { return }
            s.cancel(reason: "drain timeout after \(hint)")
        }
        drainTimer = timer
        timer.resume()
    }
    
    private func pumpUpstreamToClient() {
        if closed { return }
        
        upstream?.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in
            
            guard let self = self else { return }
            if !self.alive() { return }
            
            if self.closed { return }
            
            if let err = err {
                self.log("upstream recv err: \(err)")
                self.queue.async { self.cancel(reason: "upstream recv err") }
                return
            }
            
            if let d = data, !d.isEmpty {
                self.vlog("recv from upstream: \(d.count)B")
                if self.tFirstByte == nil {
                    self.tFirstByte = .now()
                    
                    
                    self.firstByteWatchdog?.cancel(); self.firstByteWatchdog = nil
                    // KPI: 即时打印首字节到达延迟 (TTFB)
                    let ttfbMs = Double(self.tFirstByte!.uptimeNanoseconds &- self.tStart.uptimeNanoseconds) / 1e6
                    self.log(String(format: "KPI immediate TTFB_ms=%.1f", ttfbMs))
                    
                    self.onFirstByteTTFBMs?(ttfbMs)  // ← 通知 QoS
                    
                    // KPI: 首包发送完成 -> 首字节回流（纯传输/排队段）
                    if let ts = self.tFirstSend {
                        let segMs = Double(self.tFirstByte!.uptimeNanoseconds &- ts.uptimeNanoseconds) / 1e6
                        self.log(String(format: "KPI firstSend_to_firstRecv_ms=%.1f", segMs))
                    }
                }
                
                // ★ 轻量识别 100-Continue（只看明文前缀；TLS 情况下这里拿到的是解密后的应用数据）
                if !self.saw100Continue {
                    if let s = String(data: d.prefix(16), encoding: .ascii),
                       s.hasPrefix("HTTP/1.1 100") || s.hasPrefix("HTTP/1.0 100") {
                        self.saw100Continue = true
                        self.log("observed upstream HTTP 100-Continue")
                    }
                }
                

                self.bytesDown &+= d.count
                self.sendToClient(d, remark: "u->c")
                
                // 若客户端已 EOF（half-close），但上游仍在下发数据，则刷新空闲计时器，避免早收尾
                if self.eofClient {
                    self.scheduleDrainCancel(hint: "client EOF (upstream->client activity)")
                }
                
            }
            
            if isComplete {
                self.log("upstream EOF")
                self.eofUpstream = true
                // 半关闭客户端写入，进入排水期
                self.client.send(content: nil, completion: .contentProcessed({ _ in }))
                self.scheduleDrainCancel(hint: "upstream EOF")
                return
            }
            
            // 继续接收
            self.pumpUpstreamToClient()
        }
    }
    
    // 首包回包看门狗（避免黑洞 60–100s 挂死；测速上传场景按策略放宽/禁用）
    private var firstByteWatchdog: DispatchSourceTimer?
    
    private func sendToUpstream(_ data: Data, remark: String) {
        
        guard let up = upstream, alive() else {
            log("Cannot send to upstream: upstream=\(upstream != nil), closed=\(closed)")
            return
        }
        
        let seq = { sendSeq &+= 1; return sendSeq }()
        
        inflight.insert(seq)
        
        vlog("send \(remark) \(data.count)B -> upstream #\(seq)")
        
        up.send(content: data, completion: .contentProcessed({ [weak self] err in
            
            guard let self = self, self.alive() else { return }
            
            // 去重：只处理一次完成回调
            guard self.inflight.remove(seq) != nil else {
                self.log("WARN dup completion for #\(seq), ignore")
                return
            }
            if let err = err {
                self.log("upstream send err: \(err)")
                self.queue.async { self.cancel(reason: "upstream send err") }
                return
            }


            // 发送成功日志 + 计数
            if remark == "firstBody" && self.tFirstSend == nil {
                self.tFirstSend = .now()
                self.log("sent firstBody successfully (mark tFirstSend)")

                // —— Speedtest “上行卡住”探测：首包发出后启动 8s 观察窗口
                if self.isSpeedtestTarget {
                    self.bytesUpAtFirstSend = self.bytesUp
                    self.uploadStuckTimer?.cancel()
                    let t = DispatchSource.makeTimerSource(queue: self.queue)

                    t.schedule(deadline: .now() + .seconds(8))
                    t.setEventHandler { [weak self] in
                        guard let s = self, s.alive() else { return }
                        let upDelta = max(0, s.bytesUp - s.bytesUpAtFirstSend)
                        if upDelta < 8 * 1024 && s.smallC2UEvents >= 6 && !s.uploadStuck {
                            s.uploadStuck = true
                            s.log("UPLOAD_STUCK origin=\(s.targetHost):\(s.targetPort) up=\(upDelta)B down=\(s.bytesDown)B small_events=\(s.smallC2UEvents)")
                            // 仅延后收尾由 drain idle 控制（不主动 half-close）
                            s.scheduleDrainCancel(hint: "UPLOAD_STUCK")
                        }
                    }
                    
                    self.uploadStuckTimer = t
                    t.resume()
                }


            } else {
                self.vlog("sent \(remark) successfully")
            }


            self.bytesUp &+= data.count
            // 仅在首包时设置 TTFB 看门狗
            if remark == "firstBody" {
                let isSpeedtestHost =
                    self.targetHost.hasSuffix("ooklaserver.net") ||
                    self.targetHost.hasSuffix("speedtest.net") ||
                    self.targetHost.contains("measurementlab")
                let disableWatchdog = (self.targetPort == 8080) && isSpeedtestHost
                let watchdogDelay: TimeInterval = disableWatchdog ? 15.0 : 2.5
                
                let wd = DispatchSource.makeTimerSource(queue: self.queue)
                wd.schedule(deadline: .now() + watchdogDelay)
                wd.setEventHandler { [weak self] in
                    
                    guard let s = self, s.alive() else { return }
                    
                    s.log("KPI watchdog: no first byte within \(Int(watchdogDelay*1000))ms after firstBody; fast-fail")
                    s.cancel(reason: "first_byte_timeout")
                }
                self.firstByteWatchdog = wd
                wd.resume()
            }
        }))
    }
    
    private func kpiLog(reason: String) {
        let now = DispatchTime.now()
        let durMs = Double(now.uptimeNanoseconds &- tStart.uptimeNanoseconds) / 1e6
        let hsMs: Double? = tReady.map { diffMs(start: tStart, end: $0) }
        let fbMs: Double? = tFirstByte.map { diffMs(start: tStart, end: $0) }
        func f(_ x: Double?) -> String { x.map { String(format: "%.1f", $0) } ?? "-" }
        log("KPI host=\(targetHost):\(targetPort) reason=\(reason) hsRTT_ms=\(f(hsMs)) ttfb_ms=\(f(fbMs)) up_bytes=\(bytesUp) down_bytes=\(bytesDown) dur_ms=\(String(format: "%.1f", durMs))")
    }
    

    private func sendToClient(_ data: Data, remark: String) {
        guard alive() else {
            log("Cannot send to client: closed=\(closed)")
            return
        }
        
        vlog("send \(remark) \(data.count)B -> client")
        
        client.send(content: data, completion: .contentProcessed({ [weak self] err in
            guard let self = self, self.alive() else { return }
            if let err = err {
                self.log("client send err: \(err)")
                self.queue.async { self.cancel(reason: "client send err") }
            } else {
                self.vlog("sent \(remark) successfully")
            }
        }))
    }
    
    private func diffMs(start: DispatchTime, end: DispatchTime) -> Double {
        return Double(end.uptimeNanoseconds &- start.uptimeNanoseconds) / 1e6
    }
}
