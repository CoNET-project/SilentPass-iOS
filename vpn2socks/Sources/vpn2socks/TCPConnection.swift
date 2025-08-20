//
//  TCPConnection.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Rewritten on 2025-08-17.
//  Updated for DNSInterceptor (actor) on 2025-08-18
//

import NetworkExtension
import Network
import Foundation

// 代表一个从 Packet Tunnel 转发到本地 SOCKS5 服务器的 TCP 连接。
// 假设 127.0.0.1:8888 已经有 SOCKS5 服务在监听。
final actor TCPConnection {
    
    // 新增：定义最大段大小 (MTU 1400 - IP Header 20 - TCP Header 20 = 1360)
        private let MSS: Int = 1360

    
    // 新增：客户端 SYN 选项信息
    private struct SynOptionInfo {
        var mss: UInt16? = nil
        var windowScale: UInt8? = nil
        var sackPermitted: Bool = false
        var timestamp: (tsVal: UInt32, tsEcr: UInt32)? = nil
        var rawOptions: Data = Data()
    }
    private var clientSynOptions: SynOptionInfo?
    @inline(__always)
    private func be16(_ a: UInt8, _ b: UInt8) -> UInt16 { (UInt16(a) << 8) | UInt16(b) }

    @inline(__always)
    private func be32(_ a: UInt8, _ b: UInt8, _ c: UInt8, _ d: UInt8) -> UInt32 {
        (UInt32(a) << 24) | (UInt32(b) << 16) | (UInt32(c) << 8) | UInt32(d)
    }
    
    private func parseSynOptionsFromTcpHeader(_ tcpHeaderAndOptions: Data) -> SynOptionInfo? {
        guard tcpHeaderAndOptions.count >= 20 else { return nil }

        // data offset: 4-bit words -> bytes
        let dataOffsetWords = (tcpHeaderAndOptions[12] >> 4) & 0x0F
        let headerLen = Int(dataOffsetWords) * 4
        guard headerLen >= 20, tcpHeaderAndOptions.count >= headerLen else { return nil }

        let options = tcpHeaderAndOptions[20..<headerLen]
        var info = SynOptionInfo(rawOptions: Data(options))

        var i = options.startIndex
        while i < options.endIndex {
            let kind = options[i]; i = options.index(after: i)

            switch kind {
            case 0: // EOL
                return info
            case 1: // NOP
                continue
            default:
                if i >= options.endIndex { return info }
                let len = Int(options[i]); i = options.index(after: i)
                // 长度检查
                let optStart = options.index(i, offsetBy: -2) // 回到 kind/len 开头
                guard len >= 2,
                      options.distance(from: optStart, to: options.endIndex) >= len else { return info }
                let payloadStart = options.index(optStart, offsetBy: 2)
                let payloadEnd   = options.index(optStart, offsetBy: len)

                switch kind {
                case 2: // MSS (len=4)
                    if len == 4 {
                        let b0 = options[payloadStart]
                        let b1 = options[options.index(payloadStart, offsetBy: 1)]
                        info.mss = be16(b0, b1)
                    }
                case 3: // Window Scale (len=3)
                    if len == 3 {
                        info.windowScale = options[payloadStart]
                    }
                case 4: // SACK Permitted (len=2)
                    if len == 2 {
                        info.sackPermitted = true
                    }
                case 8: // Timestamp (len=10)
                    if len == 10 {
                        let p0 = payloadStart
                        let p1 = options.index(p0, offsetBy: 4)
                        let p2 = options.index(p1, offsetBy: 4)
                        let tsVal = be32(options[p0],
                                         options[options.index(p0, offsetBy: 1)],
                                         options[options.index(p0, offsetBy: 2)],
                                         options[options.index(p0, offsetBy: 3)])
                        let tsEcr = be32(options[p1],
                                         options[options.index(p1, offsetBy: 1)],
                                         options[options.index(p1, offsetBy: 2)],
                                         options[options.index(p1, offsetBy: 3)])
                        info.timestamp = (tsVal, tsEcr)
                    }
                default:
                    // 其它选项忽略
                    break
                }

                i = payloadEnd
            }
        }

        return info
    }

    
    
    public func acceptClientSyn(tcpHeaderAndOptions tcpSlice: Data) async {
        // 解析 TCP 选项，自动设置 peerSupportsSack 等
        noteClientSyn(tcpHeaderAndOptions: tcpSlice)
        // 再按你原有流程发带 SACK-Permitted 的 SYN-ACK
        await sendSynAckIfNeeded()
    }

    public func acceptClientSyn(ipv4Packet packet: Data) async {
        noteClientSyn(ipv4Packet: packet)
        await sendSynAckIfNeeded()
    }
    
    public func noteClientSyn(tcpHeaderAndOptions: Data) {
        guard let parsed = parseSynOptionsFromTcpHeader(tcpHeaderAndOptions) else {
            log("Client SYN options: <unparsable>")
            return
        }
        clientSynOptions = parsed

        if parsed.sackPermitted {
            peerSupportsSack = true
        }
        var parts: [String] = []
        if let mss = parsed.mss { parts.append("MSS=\(mss)") }
        if let ws = parsed.windowScale { parts.append("WS=\(ws)") }
        if parsed.sackPermitted { parts.append("SACK-Permitted") }
        if let ts = parsed.timestamp { parts.append("TS(val=\(ts.tsVal),ecr=\(ts.tsEcr))") }
        log("Client SYN options: " + (parts.isEmpty ? "<none>" : parts.joined(separator: ", ")))
    }
    
    public func noteClientSyn(ipv4Packet: Data) {
        // IPv4 最小 20B
        guard ipv4Packet.count >= 20 else { return }
        // IHL
        let ihl = (ipv4Packet[0] & 0x0F) * 4
        guard ihl >= 20, ipv4Packet.count >= Int(ihl) + 20 else { return }
        // 协议号需为 TCP(6)
        guard ipv4Packet[9] == 6 else { return }
        let tcpStart = Int(ihl)
        let tcpHeaderMin = ipv4Packet[tcpStart ..< min(ipv4Packet.count, tcpStart + 60)] // 取最多 60B（TCP头+选项最大）
        noteClientSyn(tcpHeaderAndOptions: Data(tcpHeaderMin))
    }

    private var nextExpectedSequence: UInt32 = 0  // 期望接收的下一个序列号


    private static let socksHost = NWEndpoint.Host("127.0.0.1")
    private static let socksPort = NWEndpoint.Port(integerLiteral: 8888)

    /// 未建立 SOCKS 之前的入站缓冲软上限（字节）
    private static let pendingSoftCapBytes = 64 * 1024

    
    // MARK: - Identity

    let key: String
    private let packetFlow: SendablePacketFlow
    private var closeContinuation: CheckedContinuation<Void, Never>?

    private let sourceIP: IPv4Address
    private let sourcePort: UInt16
    private let destIP: IPv4Address
    private let destPort: UInt16
    private let destinationHost: String?   // 上层（如 DNS 拦截或 L7）已知的域名（可空）

    // MARK: - TCP handshake & seq

    private var synAckSent = false
    private let serverInitialSequenceNumber: UInt32   // 首次 SYN-ACK 使用的固定 ISS（用于重复 SYN 重发）
    private var handshakeAcked = false                // 三次握手最终 ACK 是否已收到

    private let initialClientSequenceNumber: UInt32   // 客户端发来的 SYN.SEQ
    private var serverSequenceNumber: UInt32 = arc4random() // 我方 SEQ（会在发送后递增）
    private var clientSequenceNumber: UInt32 = 0            // 期望客户端的下一个 SEQ（=最新已收末端）
    private let queue = DispatchQueue(label: "tcp.connection.timer", qos: .userInitiated)
    private let lingerAfterUpstreamEOFSeconds: UInt64 = 5

    // MARK: - SOCKS state

    private enum SocksState { case idle, connecting, greetingSent, methodOK, connectSent, established, closed }
    private var socksState: SocksState = .idle
    private var socksConnection: NWConnection?

    // 在 SOCKS 未建立前缓存客户端负载，待建立后一次性冲刷（首段常为 TLS ClientHello，可用于 SNI 解析）
    private var pendingClientBytes: [Data] = []
    private var pendingBytesTotal: Int = 0
    
    // === Actor 内新增：状态 ===
    private var upstreamEOF = false
    private var lingerTask: Task<Void, Never>? = nil
    
    // SACK 相关
    private var sackEnabled = true  // 是否启用 SACK
    private var peerSupportsSack = false  // 对端是否支持 SACK

    // === Actor 内新增：封装好的 actor 方法（只在 actor 内部读写属性）===
    @inline(__always)
    private func markUpstreamEOF() {
        upstreamEOF = true
    }

    @inline(__always)
    private func clearUpstreamEOF() {
        upstreamEOF = false
    }

    @inline(__always)
    private func isUpstreamEOF() -> Bool {
        upstreamEOF
    }
    
    // SOCKS 读取错误
    private enum SocksReadError: Error { case closed }

    // 统一决策后的目标（避免重复计算）
    private enum SocksTarget {
        case ip(IPv4Address, port: UInt16)
        case domain(String, port: UInt16)
    }
    private var chosenTarget: SocksTarget?

    // 仅在 actor 内修改状态，避免并发警告
    private func setSocksState(_ newState: SocksState) async {
        self.socksState = newState
    }
    
    private static func loopParams() -> NWParameters {
        let p = NWParameters.tcp
        p.requiredInterfaceType = .loopback   // 关键
        p.allowLocalEndpointReuse = true
        return p
    }
    
/// 动态生成的连接标识符，用于日志记录。
    private var dynamicLogKey: String {
        let domainName: String?

        // 1. 优先使用 SOCKS 连接最终选择的域名（chosenTarget），因为它最准确。
        if case .domain(let host, _) = chosenTarget {
            domainName = host
        } else {
            // 2. 回退到初始化时传入的域名（destinationHost），并确保其非空。
            if let host = destinationHost, !host.isEmpty {
                domainName = host
            } else {
                domainName = nil
            }
        }

        let sourceString = "\(sourceIP):\(sourcePort)"
        var destinationString = "\(destIP)"

        // 使用全角括号（）包含域名
        if let domain = domainName {
            destinationString += "（\(domain)）"
        }
        destinationString += ":\(destPort)"

        return "\(sourceString)->\(destinationString)"
    }

    
    
    // 读取至多 max 字节（至少 1 字节，除非对端关闭）
    private func receiveChunk(max: Int) async throws -> Data {
        // 防止 socksConnection 已被置空导致挂起
        guard let conn = socksConnection else { throw SocksReadError.closed }
        return try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Data, Error>) in
            conn.receive(minimumIncompleteLength: 1, maximumLength: max) { data, _, isComplete, error in
                if let error = error { cont.resume(throwing: error); return }
                if isComplete {
                    if let d = data, !d.isEmpty { cont.resume(returning: d) }
                    else { cont.resume(throwing: SocksReadError.closed) }
                    return
                }
                cont.resume(returning: data ?? Data())
            }
        }
    }

    // 精确读取 n 字节
    private func receiveExactly(_ n: Int) async throws -> Data {
        var buf = Data(); buf.reserveCapacity(n)
        while buf.count < n {
            let chunk = try await receiveChunk(max: n - buf.count)
            if chunk.isEmpty { throw SocksReadError.closed }
            buf.append(chunk)
        }
        return buf
    }
    
    // MARK: - Pending buffer helpers

    /// 追加到未建立 SOCKS 的缓冲，并进行软上限裁剪（保留首段）
    private func appendToPending(_ data: Data) {
        pendingClientBytes.append(data)
        pendingBytesTotal &+= data.count
        trimPendingIfNeeded()
    }

    /// 将缓冲裁剪到软上限以内：尽量保留 index 0（通常是 TLS ClientHello）
    private func trimPendingIfNeeded() {
        guard pendingBytesTotal > TCPConnection.pendingSoftCapBytes,
              !pendingClientBytes.isEmpty else { return }

        var dropped = 0
        let idx = 1 // 保留第 0 段
        while pendingBytesTotal > TCPConnection.pendingSoftCapBytes && idx < pendingClientBytes.count {
            let sz = pendingClientBytes[idx].count
            pendingClientBytes.remove(at: idx)
            pendingBytesTotal &-= sz
            dropped &+= sz
            // 不递增 idx，因为移除了当前位置，下一段自动顶上来
        }
        if dropped > 0 {
            log("Pending buffer capped at \(TCPConnection.pendingSoftCapBytes)B; dropped \(dropped)B; now \(pendingBytesTotal)B.")
        }
    }

    // MARK: - Init
    
    

    init(
        key: String,
        packetFlow: SendablePacketFlow,
        sourceIP: IPv4Address,
        sourcePort: UInt16,
        destIP: IPv4Address,
        destPort: UInt16,
        destinationHost: String?,
        initialSequenceNumber: UInt32
    ) {
        self.key = key
        self.packetFlow = packetFlow
        self.sourceIP = sourceIP
        self.sourcePort = sourcePort
        self.destIP = destIP
        self.destPort = destPort
        self.destinationHost = destinationHost
        self.serverInitialSequenceNumber = self.serverSequenceNumber
        self.initialClientSequenceNumber = initialSequenceNumber
        self.clientSequenceNumber = initialSequenceNumber
        
        self.sackEnabled = true  // 启用 SACK

        let source = "\(sourceIP):\(sourcePort)"
        var destination = "\(destIP)"
        if let domain = destinationHost, !domain.isEmpty {
            destination += "（\(domain)）"
        }
        destination += ":\(destPort)"
        let initialKey = "\(source)->\(destination)"

        NSLog("[TCPConnection \(initialKey)] Initialized. InitialClientSeq: \(initialSequenceNumber)")
        
    }

    // MARK: - Lifecycle

    /// 启动到本地 SOCKS5 服务器的连接。挂起直到连接关闭才返回。
    func start() async {
        guard socksConnection == nil else { return }
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                self.closeContinuation = continuation
                let conn = NWConnection(
                    host: TCPConnection.socksHost,  // 127.0.0.1
                    port: TCPConnection.socksPort,  // 8888
                    using: TCPConnection.loopParams()
                )
                self.socksConnection = conn
                self.socksState = .connecting
                conn.stateUpdateHandler = { [weak self] st in
                    Task { await self?.handleStateUpdate(st) }
                }
                self.log("Starting connection to SOCKS proxy...")
                conn.start(queue: .global(qos: .userInitiated))
            }
    }

    /// 幂等的发送 SYN-ACK（只做一次），并推进我方 SEQ
    public func sendSynAckIfNeeded() async {
        guard !synAckSent else { return }
        await sendSynAckWithOptions()   // ← 改成带 SACK-Permitted 的版本
    }

    /// 关闭并清理连接
    func close() {
        guard socksState != .closed else { return }
        socksState = .closed
        
        // 取消所有定时器
        cancelRetransmitTimer()
        
        // 清理缓存
        if !outOfOrderPackets.isEmpty {
            log("Clearing \(outOfOrderPackets.count) buffered out-of-order packets")
            outOfOrderPackets.removeAll()
        }
        
        // 关闭连接
        if socksConnection?.state != .cancelled {
            log("Closing connection.")
            socksConnection?.cancel()
        }
        diagTask?.cancel()
        socksConnection = nil
        
        closeContinuation?.resume()
        closeContinuation = nil
    }
    
    // 添加诊断方法
    private func getConnectionStats() -> String {
        return """
        Connection Stats:
        - Next expected seq: \(nextExpectedSequence)
        - Client seq (ACK): \(clientSequenceNumber)
        - Server seq: \(serverSequenceNumber)
        - Buffered packets: \(outOfOrderPackets.count)
        - SOCKS state: \(socksState)
        """
    }
    
    
    

    // MARK: - NWConnection State (actor-safe)
    

    private func generateSackBlocks() -> [(UInt32, UInt32)] {
        guard !outOfOrderPackets.isEmpty else { return [] }
                
        var blocks: [(UInt32, UInt32)] = []
        var currentStart: UInt32?
        var lastEnd: UInt32?
        
        for packet in outOfOrderPackets {
            let packetEnd = packet.seq &+ UInt32(packet.data.count)
            
            if let start = currentStart, let end = lastEnd {
                if packet.seq == end {
                    lastEnd = packetEnd
                } else {
                    blocks.append((start, end))
                    currentStart = packet.seq
                    lastEnd = packetEnd
                }
            } else {
                currentStart = packet.seq
                lastEnd = packetEnd
            }
        }
        
        if let start = currentStart, let end = lastEnd {
            blocks.append((start, end))
        }
        
        // TCP 最多 4 个 SACK 块
        if blocks.count > 4 {
            blocks = Array(blocks.prefix(4))
        }
        
        return blocks
    }
    
    
    // MARK: - 定时器方法（使用新名称）
    private var retransmitTimer: DispatchSourceTimer?  // 使用不同的名字避免冲突
        private var retransmitRetries = 0
        private let maxRetransmitRetries = 3
    
    private func cancelRetransmitTimer() {
        retransmitTimer?.cancel()
        retransmitTimer = nil
        retransmitRetries = 0
    }
    
    private func handleRetransmitTimeout() async {
        retransmitRetries += 1
        if retransmitRetries > maxRetransmitRetries {
            log("Maximum retries reached for seq: \(nextExpectedSequence)")
            if let firstBuffered = outOfOrderPackets.first {
                nextExpectedSequence = firstBuffered.seq
                processBufferedPackets()
            }
            cancelRetransmitTimer()
            return
        }

        log("Retransmit timeout #\(retransmitRetries) for seq: \(nextExpectedSequence)")
        for _ in 0..<3 { sackEnabled && peerSupportsSack ? sendAckWithSack() : sendDuplicateAck() }

        // 仅重设时间，不再新建 timer
        let nextTimeoutMs = 200 * (1 << retransmitRetries)
        retransmitTimer?.schedule(deadline: .now() + .milliseconds(nextTimeoutMs))
    }
    
    private var diagTask: Task<Void, Never>?
    private func startDiagnosticsLoop() {
        diagTask?.cancel()
        diagTask = Task { [weak self] in
            while let self, await self.socksState != .closed {
                await self.log(self.getDetailedStats())
                try? await Task.sleep(nanoseconds: 5_000_000_000)
            }
        }
    }
    
    private func startRetransmitTimer() {
        cancelRetransmitTimer()
                
        // 使用实例的 queue
        let timer = DispatchSource.makeTimerSource(queue: self.queue)
        let timeoutMs: UInt64 = 200
        
        timer.schedule(deadline: .now() + .milliseconds(Int(timeoutMs)))
        
        timer.setEventHandler { [weak self] in
            Task { [weak self] in
                guard let self = self else { return }
                await self.handleRetransmitTimeout()
            }
        }
        
        timer.resume()
        retransmitTimer = timer
        
        log("Started retransmit timer for seq: \(nextExpectedSequence)")
    }
    
    

    

    private func handleStateUpdate(_ newState: NWConnection.State) async {
        switch newState {
            case .ready:
                log("TCP connection to proxy is ready. Starting SOCKS handshake.")
                await setSocksState(.greetingSent)
                await performSocksHandshake()

            case .waiting(let err):
                log("NWConnection waiting: \(err.localizedDescription)")
                // 常见为 prohibited / no route —— 多半没排除 127.0.0.0/8 或没走 loopback

            case .preparing:
                log("NWConnection preparing...")

            case .failed(let err):
                log("NWConnection failed: \(err.localizedDescription)")
                close()

            case .cancelled:
                close()

            default:
                break
            }
    }

    // MARK: - SOCKS5 Handshake

    /// 步骤 1：发送 greeting（支持 NoAuth）
    private func performSocksHandshake() async {
        guard let connection = socksConnection else { return }
        let handshake: [UInt8] = [0x05, 0x01, 0x00]

        connection.send(content: Data(handshake), completion: .contentProcessed({ [weak self] error in
            Task { [weak self] in
                guard let self else { return }
                if let error = error {
                    await self.log("Handshake send error: \(error)")
                    await self.close()
                    return
                }
                await self.log("Step 1: Handshake sent. Waiting for server response...")
                await self.receiveHandshakeResponse()
            }
        }))
    }

    /// 步骤 1 的应答：必须是 0x05 0x00（NoAuth）
    private func receiveHandshakeResponse() async {
        guard let connection = socksConnection else { return }

        connection.receive(minimumIncompleteLength: 2, maximumLength: 2) { [weak self] (data, _, _, error) in
            Task { [weak self] in
                guard let self else { return }

                if let error = error {
                    await self.log("Handshake receive error: \(error)")
                    await self.close()
                    return
                }

                guard let data = data, data.count == 2, data[0] == 0x05, data[1] == 0x00 else {
                    await self.log("Handshake failed. Server response: \(data?.hexEncodedString() ?? "empty")")
                    await self.close()
                    return
                }

                await self.log("Step 1: Handshake response received. Server accepted 'No Auth'.")
                await self.setSocksState(.methodOK)
                await self.sendSocksConnectRequest()  // 这里内部会先统一决策目标
            }
        }
    }

    /// 统一决策：本次 SOCKS CONNECT 用 IP 还是 DOMAIN（目的：最大化命中“假IP→域名”并支持 SNI 回退）
    private func decideSocksTarget() async -> SocksTarget {
        // 1) 优先使用上层已经给的域名（例如 ConnectionManager 已截获到域）
        if let host = destinationHost, !host.isEmpty {
            if isFakeIP(destIP) {
                await DNSInterceptor.shared.registerMapping(fakeIP: destIP, domain: host.lowercased())
            }
            log("[TCP] Use destinationHost=\(host) as DOMAIN target.")
            return .domain(host, port: destPort)
        }

        // 2) 假 IP → 查 DNSInterceptor 的映射
        if isFakeIP(destIP) {
            if let mapped = await DNSInterceptor.shared.getDomain(forFakeIP: destIP) {
                log("[TCP] FakeIP \(destIP) matched domain \(mapped).")
                return .domain(mapped, port: destPort)
            }
            // 3) 未命中：尝试从首段缓存解析 TLS SNI
            if let hello = firstBufferedPayload(), let sni = parseSNIHost(from: hello) {
                await DNSInterceptor.shared.registerMapping(fakeIP: destIP, domain: sni)
                log("[TCP] FakeIP \(destIP) no map; recovered SNI=\(sni).")
                return .domain(sni, port: destPort)
            }
            // 4) 仍然失败：退回 IP（Local Proxy 会因假 IP 失败，但让路径自洽）
            log("[TCP] FakeIP \(destIP) no map & no SNI. Fallback to IP.")
            return .ip(destIP, port: destPort)
        }

        // 真实 IP：可选 SNI 优先（有些上游需要域名握手）
        if let hello = firstBufferedPayload(), let sni = parseSNIHost(from: hello) {
            log("[TCP] Real IP \(destIP) but SNI=\(sni) found. Prefer DOMAIN.")
            return .domain(sni, port: destPort)
        }

        // 否则就直接用 IP
        return .ip(destIP, port: destPort)
    }

    /// 步骤 2：发送 CONNECT 请求（优先域名）。内部会调用统一决策。
    private func sendSocksConnectRequest() async {
        guard let connection = socksConnection else { return }

        // 如果还没选过，先做一次统一决策；否则复用上次决策
        let target: SocksTarget
        if let chosen = self.chosenTarget {
            target = chosen
        } else {
            let decided = await decideSocksTarget()
            self.chosenTarget = decided
            target = decided
        }

        var request = Data()
        request.append(0x05) // Version
        request.append(0x01) // CONNECT
        request.append(0x00) // RSV

        let logMessage: String
        switch target {
        case .domain(let host, let port):
            logMessage = "Step 2: Sending connect request for DOMAIN \(host):\(port)"
            request.append(0x03) // ATYP: Domain
            let dom = Data(host.utf8)
            request.append(UInt8(dom.count))
            request.append(dom)
            request.append(contentsOf: [UInt8(port >> 8), UInt8(port & 0xFF)])
        case .ip(let ip, let port):
            logMessage = "Step 2: Sending connect request for IP \(ip):\(port)"
            request.append(0x01)          // ATYP: IPv4
            request.append(ip.rawValue)   // 4 bytes
            request.append(contentsOf: [UInt8(port >> 8), UInt8(port & 0xFF)])
        }

        await setSocksState(.connectSent)

        connection.send(content: request, completion: .contentProcessed({ [weak self] error in
            Task { [weak self] in
                guard let self else { return }
                if let error = error {
                    await self.log("Connect request send error: \(error)")
                    await self.close()
                    return
                }
                await self.log(logMessage)
                await self.receiveConnectResponse()
            }
        }))
    }

    /// 步骤 2 的应答（0x05 0x00 …）
    private func receiveConnectResponse() async {
        guard socksConnection != nil else { return }

        do {
            // 固定头 4B：VER, REP, RSV, ATYP
            let head = try await receiveExactly(4)
            let ver = head[0], rep = head[1], atyp = head[3]

            guard ver == 0x05 else {
                log("SOCKS Step 2: Bad VER \(ver)")
                close(); return
            }
            guard rep == 0x00 else {
                log("SOCKS Step 2: Connect rejected. REP=\(String(format:"0x%02x", rep))")
                close(); return
            }

            // 读取 BND.ADDR
            switch atyp {
            case 0x01: // IPv4
                _ = try await receiveExactly(4)
            case 0x04: // IPv6
                _ = try await receiveExactly(16)
            case 0x03: // Domain
                let len = try await receiveExactly(1)[0]
                if len > 0 { _ = try await receiveExactly(Int(len)) }
            default:
                log("SOCKS Step 2: Unknown ATYP \(String(format:"0x%02x", atyp))")
                close(); return
            }

            // 读取 BND.PORT
            _ = try await receiveExactly(2)

            await setSocksState(.established)
            log("SOCKS Step 2: Connect response consumed. SOCKS tunnel established!")

            // 再保险地确保 SYN-ACK 已发
            await sendSynAckIfNeeded()

            // 冲刷缓存
            await onSocksEstablishedAndFlush()

            // 开始转发上游数据
            await readFromSocks()

        } catch {
            log("SOCKS Step 2: Connect response receive error: \(error)")
            close()
        }
    }
    
    // 处理有序包
    private func processInOrderPacket(payload: Data, sequenceNumber: UInt32) {
        nextExpectedSequence = sequenceNumber &+ UInt32(payload.count)
        self.clientSequenceNumber = nextExpectedSequence
        
        log("In-order packet: seq=\(sequenceNumber), len=\(payload.count), next=\(nextExpectedSequence)")
        
        sendPureAck()
        
        if socksState != .established {
            appendToPending(payload)
            log("Buffering \(payload.count) bytes (SOCKS not ready)")
        } else {
            sendRawToSocks(payload)
        }
    }


    // MARK: - Data forwarding (App -> SOCKS)


    private let maxOutOfOrderPackets = 100  // 防止内存泄漏的上限
    
    // 统计
    private var duplicateAckCount = 0
    private var lastAckedSequence: UInt32 = 0
    
    
    // 缓存乱序包
    private func bufferOutOfOrderPacket(payload: Data, sequenceNumber: UInt32) {
        // 检查是否已经缓存过
        if outOfOrderPackets.contains(where: { $0.seq == sequenceNumber }) {
            log("Out-of-order packet already buffered, seq: \(sequenceNumber)")
            return
        }
        
        // 添加到缓存，保持排序
        let packet = (seq: sequenceNumber, data: payload)
        
        // 二分查找插入位置，保持序列号有序
        var left = 0
        var right = outOfOrderPackets.count
        
        while left < right {
            let mid = (left + right) / 2
            if outOfOrderPackets[mid].seq < sequenceNumber {
                left = mid + 1
            } else {
                right = mid
            }
        }
        
        outOfOrderPackets.insert(packet, at: left)
        
        log("Buffered out-of-order packet, seq: \(sequenceNumber), len: \(payload.count), total buffered: \(outOfOrderPackets.count)")
        
        // 防止内存泄漏
        if outOfOrderPackets.count > maxOutOfOrderPackets {
            // 移除最旧的包
            let removed = outOfOrderPackets.removeFirst()
            log("WARNING: Dropped old out-of-order packet due to buffer limit, seq: \(removed.seq)")
        }
    }
    
    private func processBufferedPackets() {
        var processed = 0
        
        while !outOfOrderPackets.isEmpty {
            let packet = outOfOrderPackets[0]
            
            if packet.seq == nextExpectedSequence {
                outOfOrderPackets.removeFirst()
                
                nextExpectedSequence = packet.seq &+ UInt32(packet.data.count)
                self.clientSequenceNumber = nextExpectedSequence
                
                log("Processing buffered: seq=\(packet.seq), len=\(packet.data.count)")
                
                if socksState != .established {
                    appendToPending(packet.data)
                } else {
                    sendRawToSocks(packet.data)
                }
                
                processed += 1
                
            } else if packet.seq < nextExpectedSequence {
                outOfOrderPackets.removeFirst()
                log("Dropping outdated buffered packet: seq=\(packet.seq)")
                
            } else {
                break
            }
        }
        
        if processed > 0 {
            log("Processed \(processed) buffered packets, remaining: \(outOfOrderPackets.count)")
            sendPureAck()
        }
    }
    

    /// 处理从 VPN 隧道来的 TCP 负载，发往 SOCKS（未建立则缓冲）
    func handlePacket(payload: Data, sequenceNumber: UInt32) {
        guard !payload.isEmpty else { return }

        if socksConnection == nil && socksState == .idle {
            Task { await self.start() }
        }
        
        // 初始化序列号追踪
        if !handshakeAcked {
            handshakeAcked = true
            nextExpectedSequence = sequenceNumber
            lastAckedSequence = sequenceNumber
            log("Handshake completed, initial data seq: \(sequenceNumber)")
        }

        // 处理不同序列号的情况
        if sequenceNumber == nextExpectedSequence {
            // 情况1: 正好是期望的包
            processInOrderPacket(payload: payload, sequenceNumber: sequenceNumber)
            
            // 取消超时定时器（因为收到了期望的包）
            cancelRetransmitTimer()
            
            // 重置重复 ACK 计数
            duplicateAckCount = 0
            
            // 检查是否有后续的乱序包可以处理
            processBufferedPackets()
            
        } else if sequenceNumber > nextExpectedSequence {
            // 情况2: 未来的包（乱序）
            bufferOutOfOrderPacket(payload: payload, sequenceNumber: sequenceNumber)
            
            // 发送带 SACK 的 ACK
            if sackEnabled && peerSupportsSack {
                sendAckWithSack()
            } else {
                sendDuplicateAck()
            }
            
            // 启动或重置丢包定时器
            startRetransmitTimer()
            
        } else {
            // 情况3: 重复的包（已经接收过）
            log("Duplicate packet received, seq: \(sequenceNumber), expected: \(nextExpectedSequence)")
            
            // 仍然发送 ACK 确认（可能是对端没收到我们的 ACK）
            sendPureAck()
        }
    }
    
    /// 发送带 SACK 选项的 ACK
    private func sendAckWithSack() {
        let sackBlocks = generateSackBlocks()
                
                if sackBlocks.isEmpty {
                    sendPureAck()
                    return
                }
                
                // 创建带 SACK 选项的 TCP 头
                let ackNumber = self.clientSequenceNumber
                
                // 计算 TCP 选项长度
                let sackOptionLength = 2 + (sackBlocks.count * 8)
                let tcpOptionsLength = 2 + sackOptionLength  // 2 个 NOP + SACK
                let tcpHeaderLength = 20 + tcpOptionsLength
                
                // 确保 4 字节对齐
                let paddedLength = ((tcpHeaderLength + 3) / 4) * 4
                let paddingNeeded = paddedLength - tcpHeaderLength
                
                var tcp = Data(count: paddedLength)
                
                // 基本 TCP 头
                tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
                tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
                
                withUnsafeBytes(of: serverSequenceNumber.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
                withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
                
                tcp[12] = UInt8((paddedLength / 4) << 4)  // Data offset
                tcp[13] = TCPFlags.ack.rawValue
                tcp[14] = 0xFF; tcp[15] = 0xFF  // Window size
                tcp[16] = 0; tcp[17] = 0        // Checksum (placeholder)
                tcp[18] = 0; tcp[19] = 0        // Urgent pointer
                
                // TCP 选项
                var optionOffset = 20
                
                // NOP + NOP (用于对齐)
                tcp[optionOffset] = 0x01; optionOffset += 1
                tcp[optionOffset] = 0x01; optionOffset += 1
                
                // SACK 选项
                tcp[optionOffset] = 0x05  // SACK option kind
                tcp[optionOffset + 1] = UInt8(sackOptionLength)  // Length
                optionOffset += 2
                
                // SACK 块 - 修复：正确访问元组成员
                for block in sackBlocks {
                    // 访问元组的第一个元素 (start)
                    withUnsafeBytes(of: block.0.bigEndian) {
                        tcp.replaceSubrange(optionOffset..<(optionOffset+4), with: $0)
                    }
                    optionOffset += 4
                    
                    // 访问元组的第二个元素 (end)
                    withUnsafeBytes(of: block.1.bigEndian) {
                        tcp.replaceSubrange(optionOffset..<(optionOffset+4), with: $0)
                    }
                    optionOffset += 4
                }
                
                // 填充（如果需要）
                for _ in 0..<paddingNeeded {
                    tcp[optionOffset] = 0x00  // End of options
                    optionOffset += 1
                }
                
                // 创建 IP 头并发送
                var ip = createIPv4Header(payloadLength: tcp.count)
                
                let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
                tcp[16] = UInt8(tcpCsum >> 8)
                tcp[17] = UInt8(tcpCsum & 0xFF)
                
                let ipCsum = ipChecksum(&ip)
                ip[10] = UInt8(ipCsum >> 8)
                ip[11] = UInt8(ipCsum & 0xFF)
                
                packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
                
                // 修复：使用元组索引访问
                let blockDescriptions = sackBlocks.map { "[\($0.0)-\($0.1)]" }.joined(separator: ", ")
                log("Sent ACK with SACK: ACK=\(ackNumber), blocks: \(blockDescriptions)")
    }
    
    /// 在 SYN-ACK 中添加 SACK-Permitted 选项
    private func sendSynAckWithOptions() async {
        guard !synAckSent else { return }
        synAckSent = true
        
        let seq = serverInitialSequenceNumber
        let ackNumber = initialClientSequenceNumber &+ 1
        
        // 创建带选项的 TCP 头
        // MSS(4) + SACK-Permitted(2) + NOP(1) + NOP(1) + Timestamp(10) = 18 bytes
        // 对齐到 20 bytes (加 2 个 NOP)
        var tcp = Data(count: 40)  // 20 基本 + 20 选项
        
        // 基本 TCP 头
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        
        withUnsafeBytes(of: seq.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        
        tcp[12] = 0xA0  // Data offset = 10 (40 bytes / 4)
        tcp[13] = TCPFlags([.syn, .ack]).rawValue
        tcp[14] = 0xFF; tcp[15] = 0xFF  // Window
        tcp[16] = 0; tcp[17] = 0        // Checksum
        tcp[18] = 0; tcp[19] = 0        // Urgent
        
        // TCP 选项
        var offset = 20
        
        // MSS 选项
        tcp[offset] = 0x02; tcp[offset+1] = 0x04  // MSS option
        tcp[offset+2] = 0x05; tcp[offset+3] = 0xB4  // 1460
        offset += 4
        
        // SACK-Permitted 选项
        tcp[offset] = 0x04; tcp[offset+1] = 0x02  // SACK-Permitted
        offset += 2
        
        // 填充 NOP 到对齐
        while offset < 40 {
            tcp[offset] = 0x01  // NOP
            offset += 1
        }
        
        // 发送
        var ip = createIPv4Header(payloadLength: tcp.count)
        
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8)
        tcp[17] = UInt8(tcpCsum & 0xFF)
        
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8)
        ip[11] = UInt8(ipCsum & 0xFF)
        
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        
        // 更新序列号
        self.serverSequenceNumber = seq &+ 1
        self.clientSequenceNumber = self.initialClientSequenceNumber &+ 1
        
        log("Sent SYN-ACK with SACK-Permitted option")
    }
    
    /// 获取连接统计信息
    func getDetailedStats() -> String {
        let sackBlocks = generateSackBlocks()
                
        // 修复：使用元组索引
        let blockDescriptions = sackBlocks.map { "[\($0.0)-\($0.1)]" }.joined(separator: ", ")
        
        return """
        === TCP Connection Stats ===
        Sequence Numbers:
          - Next expected: \(nextExpectedSequence)
          - Last ACKed: \(clientSequenceNumber)
          - Server seq: \(serverSequenceNumber)
        
        Out-of-Order:
          - Buffered packets: \(outOfOrderPackets.count)
          - SACK blocks: \(blockDescriptions)
          - Duplicate ACKs: \(duplicateAckCount)
        
        Timers:
          - Retransmit timer: \(retransmitTimer != nil ? "Active" : "Inactive")
          - Retries: \(retransmitRetries)/\(maxRetransmitRetries)
        
        Features:
          - SACK enabled: \(sackEnabled)
          - Peer supports SACK: \(peerSupportsSack)
        
        SOCKS State: \(socksState)
        """
    }
    
    // 定期打印统计（用于调试）
    private func startDiagnostics() {
        Timer.scheduledTimer(withTimeInterval: 5.0, repeats: true) { _ in
            Task {
                await self.log(self.getDetailedStats())
            }
        }
    }
    
    
    
    // 发送重复ACK（用于快速重传）
    private func sendDuplicateAck() {
        duplicateAckCount += 1
            log("Duplicate ACK #\(duplicateAckCount) for seq: \(clientSequenceNumber)")
        
        if duplicateAckCount >= 3 {
            log("Fast retransmit triggered")
        }
        
        sendPureAck()
}

    /// SOCKS 刚建立：冲刷缓冲数据
    private func onSocksEstablishedAndFlush() async {
        guard socksState == .established else { return }
        if pendingClientBytes.isEmpty { return }

        let totalBuffered = pendingBytesTotal
        log("Flushing \(totalBuffered) bytes buffered before SOCKS established.")

        for chunk in pendingClientBytes {
            sendRawToSocks(chunk)
        }
        pendingClientBytes.removeAll(keepingCapacity: false)
        pendingBytesTotal = 0
    }

    /// 统一向 SOCKS 发送，并记录前 16B 预览日志
    private func sendRawToSocks(_ data: Data) {
        let preview = data.prefix(16).hexEncodedString()
        log("Forwarding \(data.count) bytes to SOCKS proxy. first16 = \(preview)")
        socksConnection?.send(content: data, completion: .contentProcessed({ [weak self] err in
            Task { [weak self] in
                if let err = err {
                    await self?.log("Error sending data to SOCKS: \(err)")
                    await self?.close()
                }
            }
        }))
    }
    
    @inline(__always)
    private func ensureLingerAndMaybeClose() {
        if lingerTask == nil {
            let delay = self.lingerAfterUpstreamEOFSeconds
            lingerTask = Task { [weak self] in
                try? await Task.sleep(nanoseconds: delay * 1_000_000_000)
                guard let self else { return }
                await self.close()
            }
        }
    }
    
    @inline(__always)
    private func cancelLinger() {
        lingerTask?.cancel()
        lingerTask = nil
    }

    /// 发送纯 ACK（不携带数据，不消耗我方序号）
    private func sendPureAck() {
        let ackNumber = self.clientSequenceNumber
        var tcp = createTCPHeader(
            payloadLen: 0,
            flags: [.ack],
            sequenceNumber: self.serverSequenceNumber,
            acknowledgementNumber: ackNumber
        )
        var ip = createIPv4Header(payloadLength: tcp.count)
        let csum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(csum >> 8); tcp[17] = UInt8(csum & 0xFF)
        let ipcsum = ipChecksum(&ip)
        ip[10] = UInt8(ipcsum >> 8); ip[11] = UInt8(ipcsum & 0xFF)
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
    }

    // MARK: - Data forwarding (SOCKS -> App)

    /// 持续从 SOCKS 读取数据，写回隧道
    private func readFromSocks() async {
        socksConnection?.receive(minimumIncompleteLength: 1, maximumLength: 16384) { [weak self] data, _, isComplete, error in
            // 回调不是在 actor 上 ⇒ 先 hop 回 actor
            Task { [weak self] in
                guard let self else { return }

                if let error = error {
                    await self.log("Receive error from SOCKS, closing connection: \(error)")
                    await self.sendFinToClient()
                    await self.close()
                    return
                }

                if isComplete {
                    // 这些都是 actor 方法 ⇒ 安全
                    await self.sendFinToClient()
                    await self.markUpstreamEOF()
                    await self.ensureLingerAndMaybeClose()
                    return
                }

                if let data, !data.isEmpty {
                    await self.log("Received \(data.count) bytes from SOCKS, writing to tunnel.")
                    await self.writeToTunnel(payload: data)
                }

                await self.readFromSocks() // 递归下一轮读取（actor 方法）
            }
        }
    }

    // MARK: - TCP 发送：SYN-ACK / FIN-ACK / data

    /// 第一次真正发出 SYN-ACK（内部），并推进本端序号
    private func sendSynAck() async {
        guard !synAckSent else { return }
        synAckSent = true

        sendSynAckCore(seq: serverInitialSequenceNumber, logPrefix: "Sending SYN-ACK to client.")

        // SYN 消耗一个序号
        self.serverSequenceNumber = serverInitialSequenceNumber &+ 1
        
        // 初始化客户端序列号追踪
        self.clientSequenceNumber = self.initialClientSequenceNumber &+ 1
        self.nextExpectedSequence = self.initialClientSequenceNumber &+ 1
    }

    /// 用固定 ISS 发送（重发用，不改变任何状态）
    public func retransmitSynAckDueToDuplicateSyn() async {
        guard !handshakeAcked else { return }
        // 重发带 SACK-Permitted 的 SYN-ACK，但不改 synAckSent/serverSequenceNumber 等状态
        let seq = serverInitialSequenceNumber
        let ackNumber = initialClientSequenceNumber &+ 1

        var tcp = Data(count: 40)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: seq.bigEndian)        { tcp.replaceSubrange(4..<8,  with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian)  { tcp.replaceSubrange(8..<12, with: $0) }
        tcp[12] = 0xA0
        tcp[13] = TCPFlags([.syn, .ack]).rawValue
        tcp[14] = 0xFF; tcp[15] = 0xFF
        // MSS + SACK-Permitted + NOP 填充
        var off = 20
        tcp[off] = 0x02; tcp[off+1] = 0x04; tcp[off+2] = 0x05; tcp[off+3] = 0xB4; off += 4
        tcp[off] = 0x04; tcp[off+1] = 0x02; off += 2
        while off < 40 { tcp[off] = 0x01; off += 1 }

        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcs = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcs >> 8); tcp[17] = UInt8(tcs & 0xFF)
        let ics = ipChecksum(&ip)
        ip[10] = UInt8(ics >> 8); ip[11] = UInt8(ics & 0xFF)
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])

        log("Retransmitted SYN-ACK (dup SYN) with options")
    }

    private func sendSynAckCore(seq: UInt32, logPrefix: String) {
        log(logPrefix)
        let ackNumber = initialClientSequenceNumber &+ 1

        var tcp = createTCPHeader(
            payloadLen: 0,
            flags: [.syn, .ack],
            sequenceNumber: seq,
            acknowledgementNumber: ackNumber
        )
        var ip  = createIPv4Header(payloadLength: tcp.count)

        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8)
        tcp[17] = UInt8(tcpCsum & 0xFF)

        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8)
        ip[11] = UInt8(ipCsum & 0xFF)

        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
    }

    /// 收到 App 侧纯 ACK（通常是第三次握手）
    public func onInboundAck(ackNumber: UInt32) {
        if ackNumber > serverSequenceNumber {
            log("ACK advanced beyond sent data! ack=\(ackNumber) sent=\(serverSequenceNumber)")
        }
        if !handshakeAcked {
            let expected = serverInitialSequenceNumber &+ 1
            if ackNumber >= expected {
                handshakeAcked = true
                nextExpectedSequence = initialClientSequenceNumber &+ 1
                // 乐观认为对端支持 SACK（后续如实现 SYN 选项解析，再改为真实值）
                peerSupportsSack = true
                log("Handshake completed (ACK: \(ackNumber))")
            }
        }
    }

    /// 上游关闭/错误时，对客户端发送 FIN-ACK
    private func sendFinToClient() async {
        let ackNumber = self.clientSequenceNumber
        var tcp = createTCPHeader(
            payloadLen: 0,
            flags: [.fin, .ack],
            sequenceNumber: self.serverSequenceNumber,
            acknowledgementNumber: ackNumber
        )
        var ip = createIPv4Header(payloadLength: tcp.count)

        let csum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(csum >> 8); tcp[17] = UInt8(csum & 0xFF)
        let ipcsum = ipChecksum(&ip)
        ip[10] = UInt8(ipcsum >> 8); ip[11] = UInt8(ipcsum & 0xFF)

        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])

        // FIN 消耗一个序号
        self.serverSequenceNumber &+= 1
    }

    /// 将从 SOCKS 收到的数据打包成 TCP 段写回 App（PSH,ACK）
    private func writeToTunnel(payload: Data) async {
        let ackNumber = self.clientSequenceNumber
        var currentSeq = self.serverSequenceNumber
        var remainingData = payload
        var packets: [Data] = []

        // 循环直到所有数据都被分段
        while !remainingData.isEmpty {
            // 1. 确定当前段的大小（不超过MSS）
            let segmentSize = min(remainingData.count, MSS)
            // 使用 prefix 和 dropFirst 进行高效的数据切片
            let segment = remainingData.prefix(segmentSize)
            remainingData = remainingData.dropFirst(segmentSize)

            // 2. 设置TCP标志。ACK必须设置。
            var flags: TCPFlags = [.ack]
            // 实践中常在所有或最后一个分段设置 PSH (Push) 标志
            flags.insert(.psh)

            // 3. 创建TCP头
            var tcp = createTCPHeader(
                payloadLen: segment.count,
                flags: flags,
                sequenceNumber: currentSeq,
                acknowledgementNumber: ackNumber
            )
            
            // 4. 创建IP头
            var ip  = createIPv4Header(payloadLength: tcp.count + segment.count)

            // 5. 计算校验和
            // 注意：segment 是 Data.SubSequence，需要转换为 Data 以便计算校验和
            let payloadData = Data(segment)
            let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: payloadData)
            tcp[16] = UInt8(tcpCsum >> 8)
            tcp[17] = UInt8(tcpCsum & 0xFF)

            let ipCsum = ipChecksum(&ip)
            ip[10] = UInt8(ipCsum >> 8)
            ip[11] = UInt8(ipCsum & 0xFF)

            // 将完整的数据包添加到批处理列表
            packets.append(ip + tcp + segment)

            // 6. 更新下一个段的序列号
            currentSeq &+= UInt32(segment.count)
        }

        if !packets.isEmpty {
            // 7. 一次性写入所有分段的数据包
            // 关键：必须为 packets 数组中的每个包提供对应的协议
            let protocols = Array(repeating: AF_INET as NSNumber, count: packets.count)
            packetFlow.writePackets(packets, withProtocols: protocols)

            // 8. 最终更新我方的序列号
            self.serverSequenceNumber = currentSeq
        }
    }

    // MARK: - Helpers for unified decision

    /// 取第一段非空、非全零的缓冲数据（通常是 TLS ClientHello）
    private func firstBufferedPayload() -> Data? {
        for d in pendingClientBytes where !d.isEmpty {
            var allZero = true
            for b in d { if b != 0 { allZero = false; break } }
            if !allZero { return d }
        }
        return nil
    }

    // MARK: - Logging

    private func log(_ message: String) {
        NSLog("[TCPConnection \(dynamicLogKey)] \(message)")
    }

    // MARK: - Packet builders & checksums (简化版)

    private func createIPv4Header(payloadLength: Int) -> Data {
        var header = Data(count: 20)
        header[0] = 0x45                    // Version=4, IHL=5
        let totalLength = 20 + payloadLength
        header[2] = UInt8(totalLength >> 8)
        header[3] = UInt8(totalLength & 0xFF)
        header[4] = 0x00; header[5] = 0x00  // Identification
        header[6] = 0x40; header[7] = 0x00  // DF, offset=0
        header[8] = 64                      // TTL
        header[9] = 6                       // TCP
        header[10] = 0; header[11] = 0      // checksum placeholder

        // 源=伪服务器 IP（destIP），目的=客户端 IP（sourceIP）
        let src = [UInt8](destIP.rawValue)
        let dst = [UInt8](sourceIP.rawValue)
        header[12] = src[0]; header[13] = src[1]; header[14] = src[2]; header[15] = src[3]
        header[16] = dst[0]; header[17] = dst[1]; header[18] = dst[2]; header[19] = dst[3]

        return header
    }
    
    private var availableWindow: UInt16 = 65535
    
    private var outOfOrderPackets: [(seq: UInt32, data: Data)] = []

    private func createTCPHeader(payloadLen: Int, flags: TCPFlags, sequenceNumber: UInt32, acknowledgementNumber: UInt32) -> Data {
        var h = Data(count: 20)
        // 方向：源端口=远端(伪服务器)端口；目的端口=客户端(app)端口
        h[0] = UInt8(destPort >> 8);   h[1] = UInt8(destPort & 0xFF)     // Source Port
        h[2] = UInt8(sourcePort >> 8); h[3] = UInt8(sourcePort & 0xFF)   // Destination Port

        withUnsafeBytes(of: sequenceNumber.bigEndian) { h.replaceSubrange(4..<8,  with: $0) }
        withUnsafeBytes(of: acknowledgementNumber.bigEndian) { h.replaceSubrange(8..<12, with: $0) }

        h[12] = 0x50                       // Data offset = 5
        h[13] = flags.rawValue
        h[14] = UInt8(availableWindow >> 8)
        h[15] = UInt8(availableWindow & 0xFF)
        h[16] = 0;    h[17] = 0            // Checksum placeholder
        h[18] = 0;    h[19] = 0            // Urgent pointer
        return h
    }

    private func tcpChecksum(ipHeader ip: Data, tcpHeader tcp: Data, payload: Data) -> UInt16 {
        var pseudo = Data()
        // 源/目的（与 IP 头一致）
        pseudo.append(ip[12...15]); pseudo.append(ip[16...19])
        pseudo.append(0)              // reserved
        pseudo.append(6)              // TCP
        let tcpLen = UInt16(tcp.count + payload.count)
        pseudo.append(UInt8(tcpLen >> 8)); pseudo.append(UInt8(tcpLen & 0xFF))

        var sumData = pseudo + tcp + payload
        if sumData.count % 2 == 1 { sumData.append(0) }

        var sum: UInt32 = 0
        for i in stride(from: 0, to: sumData.count, by: 2) {
            let word = (UInt16(sumData[i]) << 8) | UInt16(sumData[i+1])
            sum &+= UInt32(word)
        }
        while (sum >> 16) != 0 { sum = (sum & 0xFFFF) &+ (sum >> 16) }
        return ~UInt16(sum & 0xFFFF)
    }

    private func ipChecksum(_ h: inout Data) -> UInt16 {
        h[10] = 0; h[11] = 0
        var sum: UInt32 = 0
        for i in stride(from: 0, to: h.count, by: 2) {
            let word = (UInt16(h[i]) << 8) | UInt16(h[i+1])
            sum &+= UInt32(word)
        }
        while (sum >> 16) != 0 { sum = (sum & 0xFFFF) &+ (sum >> 16) }
        return ~UInt16(sum & 0xFFFF)
    }
}

// Helper: TCP Flags
struct TCPFlags: OptionSet {
    let rawValue: UInt8
    static let fin = TCPFlags(rawValue: 1 << 0)
    static let syn = TCPFlags(rawValue: 1 << 1)
    static let rst = TCPFlags(rawValue: 1 << 2)
    static let psh = TCPFlags(rawValue: 1 << 3)
    static let ack = TCPFlags(rawValue: 1 << 4)
}

// MARK: - Helpers outside actor

@inline(__always)
private func isFakeIP(_ ip: IPv4Address) -> Bool {
    // RFC 2544: 198.18.0.0/15 -> 198.18.0.0 ~ 198.19.255.255
    let b = [UInt8](ip.rawValue)
    return b.count == 4 && b[0] == 198 && (b[1] & 0xFE) == 18   // 18 或 19
}

/// 解析 TLS ClientHello 的 SNI（只取第一个 host_name）
private func parseSNIHost(from clientHello: Data) -> String? {
    // TLS record header: 5 bytes
    guard clientHello.count >= 5 else { return nil }
    let contentType = clientHello[0]        // 0x16 = handshake
    guard contentType == 0x16 else { return nil }
    // total TLS record length
    let recLen = Int(clientHello[3]) << 8 | Int(clientHello[4])
    guard clientHello.count >= 5 + recLen else { return nil }

    // Handshake header starts at 5
    var i = 5
    guard i + 4 <= clientHello.count else { return nil }
    let hsType = clientHello[i]             // 0x01 = ClientHello
    guard hsType == 0x01 else { return nil }
    let hsLen = Int(clientHello[i+1]) << 16 | Int(clientHello[i+2]) << 8 | Int(clientHello[i+3])
    i += 4
    guard i + hsLen <= clientHello.count else { return nil }

    // client_version(2) + random(32)
    guard i + 2 + 32 <= clientHello.count else { return nil }
    i += 2 + 32

    // session_id
    guard i + 1 <= clientHello.count else { return nil }
    let sidLen = Int(clientHello[i]); i += 1
    guard i + sidLen <= clientHello.count else { return nil }
    i += sidLen

    // cipher_suites
    guard i + 2 <= clientHello.count else { return nil }
    let csLen = Int(clientHello[i]) << 8 | Int(clientHello[i+1]); i += 2
    guard i + csLen <= clientHello.count else { return nil }
    i += csLen

    // compression_methods
    guard i + 1 <= clientHello.count else { return nil }
    let compLen = Int(clientHello[i]); i += 1
    guard i + compLen <= clientHello.count else { return nil }
    i += compLen

    // extensions
    guard i + 2 <= clientHello.count else { return nil }
    let extTotal = Int(clientHello[i]) << 8 | Int(clientHello[i+1]); i += 2
    guard i + extTotal <= clientHello.count else { return nil }
    let extEnd = i + extTotal

    while i + 4 <= extEnd {
        let extType = Int(clientHello[i]) << 8 | Int(clientHello[i+1])
        let extLen  = Int(clientHello[i+2]) << 8 | Int(clientHello[i+3])
        i += 4
        guard i + extLen <= extEnd else { return nil }

        if extType == 0x0000 { // server_name
            var j = i
            guard j + 2 <= i + extLen else { return nil }
            let listLen = Int(clientHello[j]) << 8 | Int(clientHello[j+1]); j += 2
            guard j + listLen <= i + extLen else { return nil }
            // 只取第一个
            guard j + 3 <= i + extLen else { return nil }
            let nameType = clientHello[j]; j += 1 // 0x00 = host_name
            guard nameType == 0x00 else { return nil }
            let hostLen = Int(clientHello[j]) << 8 | Int(clientHello[j+1]); j += 2
            guard j + hostLen <= i + extLen else { return nil }
            let hostData = clientHello[j..<(j+hostLen)]
            if let host = String(bytes: hostData, encoding: .utf8), !host.isEmpty {
                return host.lowercased()
            }
            return nil
        }
        i += extLen
    }
    return nil
}

extension Data {
    fileprivate func hexEncodedString() -> String {
        map { String(format: "%02hhx", $0) }.joined()
    }
}
