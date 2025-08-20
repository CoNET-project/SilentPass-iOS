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
    private func markUpstreamEOF() { upstreamEOF = true }

    @inline(__always)
    private func clearUpstreamEOF() { upstreamEOF = false }

    @inline(__always)
    private func isUpstreamEOF() -> Bool { upstreamEOF }
    
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
        if case .domain(let host, _) = chosenTarget {
            domainName = host
        } else {
            if let host = destinationHost, !host.isEmpty {
                domainName = host
            } else {
                domainName = nil
            }
        }

        let sourceString = "\(sourceIP):\(sourcePort)"
        var destinationString = "\(destIP)"

        if let domain = domainName {
            destinationString += "（\(domain)）"
        }
        destinationString += ":\(destPort)"

        return "\(sourceString)->\(destinationString)"
    }

    // 读取至多 max 字节（至少 1 字节，除非对端关闭）
    private func receiveChunk(max: Int) async throws -> Data {
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
        var idx = 1 // 保留第 0 段
        while pendingBytesTotal > TCPConnection.pendingSoftCapBytes && idx < pendingClientBytes.count {
            let sz = pendingClientBytes[idx].count
            pendingClientBytes.remove(at: idx)
            pendingBytesTotal &-= sz
            dropped &+= sz
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
        await sendSynAckWithOptions()   // ← 带 SACK-Permitted 与正确的 MSS
    }

    /// 关闭并清理连接
    func close() {
        guard socksState != .closed else { return }
        socksState = .closed
        
        // 取消所有定时器
        cancelRetransmitTimer()
        cancelDelayedAckTimer()
        
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
        var currentEnd: UInt32?
        
        for packet in outOfOrderPackets {
            let packetEnd = packet.seq &+ UInt32(packet.data.count)
            
            if let start = currentStart, let end = currentEnd {
                // 检查是否可以合并
                if packet.seq == end {
                    // 连续，扩展当前块
                    currentEnd = packetEnd
                } else {
                    // 不连续，保存当前块并开始新块
                    blocks.append((start, end))
                    if blocks.count >= 4 { break } // 最多4个SACK块
                    currentStart = packet.seq
                    currentEnd = packetEnd
                }
            } else {
                // 第一个块
                currentStart = packet.seq
                currentEnd = packetEnd
            }
        }
        
        // 添加最后一个块
        if let start = currentStart, let end = currentEnd, blocks.count < 4 {
            blocks.append((start, end))
        }
        
        return blocks
    }
    
    // 7. 添加诊断方法
    private func diagnosePacketLoss() -> String {
        var diagnosis = "Packet Loss Diagnosis:\n"
        
        if outOfOrderPackets.isEmpty {
            diagnosis += "  - No buffered packets\n"
        } else {
            diagnosis += "  - Buffered packets: \(outOfOrderPackets.count)\n"
            diagnosis += "  - Expected seq: \(nextExpectedSequence)\n"
            
            if let first = outOfOrderPackets.first {
                let gap = first.seq - nextExpectedSequence
                diagnosis += "  - First buffered seq: \(first.seq)\n"
                diagnosis += "  - Gap size: \(gap) bytes\n"
                
                if gap > 0 {
                    diagnosis += "  - Missing sequence range: \(nextExpectedSequence)-\(first.seq - 1)\n"
                }
            }
            
            // 检查缓冲包的连续性
            var hasGaps = false
            for i in 1..<outOfOrderPackets.count {
                let prev = outOfOrderPackets[i-1]
                let curr = outOfOrderPackets[i]
                let prevEnd = prev.seq &+ UInt32(prev.data.count)
                if curr.seq != prevEnd {
                    hasGaps = true
                    let gapSize = curr.seq - prevEnd
                    diagnosis += "  - Gap in buffer: \(gapSize) bytes between seq \(prevEnd) and \(curr.seq)\n"
                }
            }
            
            if !hasGaps && outOfOrderPackets.count > 1 {
                diagnosis += "  - Buffered packets are contiguous\n"
            }
        }
        
        return diagnosis
    }
    
    // MARK: - 定时器方法（重传）
    private var retransmitTimer: DispatchSourceTimer?
    private var retransmitRetries = 0
    private let maxRetransmitRetries = 3
    
    private func cancelRetransmitTimer() {
        retransmitTimer?.cancel()
        retransmitTimer = nil
        retransmitRetries = 0
    }
    
    private func handleRetransmitTimeout() async {
        retransmitRetries += 1
            
        log("Retransmit timeout #\(retransmitRetries) for seq: \(nextExpectedSequence), buffered: \(outOfOrderPackets.count)")
        
        if retransmitRetries > maxRetransmitRetries {
            log("Max retries reached, checking for buffered packets...")
            
            // 检查是否有可以跳过的小gap
            if let firstBuffered = outOfOrderPackets.first {
                let gap = firstBuffered.seq - nextExpectedSequence
                if gap <= 1460 { // 小于一个MSS的gap
                    log("Small gap detected (\(gap) bytes), skipping to seq \(firstBuffered.seq)")
                    nextExpectedSequence = firstBuffered.seq
                    processBufferedPackets()
                }
            }
            cancelRetransmitTimer()
            return
        }
        
        // 发送重复ACK触发快速重传
        if sackEnabled && peerSupportsSack {
            // 发送3个带SACK的ACK
            for i in 0..<3 {
                sendAckWithSack()
                log("Sent duplicate ACK with SACK #\(i+1)")
            }
        } else {
            // 发送3个普通重复ACK
            for i in 0..<3 {
                sendDuplicateAck()
            }
        }
        
        // 指数退避
        let nextTimeoutMs = 200 * (1 << min(retransmitRetries, 4))
        retransmitTimer?.schedule(deadline: .now() + .milliseconds(nextTimeoutMs))
        log("Next retry in \(nextTimeoutMs)ms")
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
    
    // MARK: - Delayed ACK（新增）
    private var delayedAckTimer: DispatchSourceTimer?
    private var delayedAckPacketsSinceLast: Int = 0
    private let delayedAckTimeoutMs: Int = 40

    private func cancelDelayedAckTimer() {
        delayedAckTimer?.cancel()
        delayedAckTimer = nil
        delayedAckPacketsSinceLast = 0
    }

    private func scheduleDelayedAck() {
        delayedAckPacketsSinceLast += 1
            
        // 每2个段或40ms发送ACK
        if delayedAckPacketsSinceLast >= 2 {
            sendPureAck()
            log("Immediate ACK (2 segments received)")
            cancelDelayedAckTimer()
            return
        }
        
        // 启动延迟定时器（如果还没有）
        if delayedAckTimer == nil {
            let timer = DispatchSource.makeTimerSource(queue: self.queue)
            timer.schedule(deadline: .now() + .milliseconds(delayedAckTimeoutMs))
            timer.setEventHandler { [weak self] in
                Task { [weak self] in
                    guard let self = self else { return }
                    await self.sendPureAck()
                    await self.log("Delayed ACK timeout (40ms)")
                    await self.cancelDelayedAckTimer()
                }
            }
            timer.resume()
            delayedAckTimer = timer
        }
    }

    // MARK: - State updates

    private func handleStateUpdate(_ newState: NWConnection.State) async {
        switch newState {
            case .ready:
                log("TCP connection to proxy is ready. Starting SOCKS handshake.")
                await setSocksState(.greetingSent)
                await performSocksHandshake()

            case .waiting(let err):
                log("NWConnection waiting: \(err.localizedDescription)")

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
                await self.sendSocksConnectRequest()
            }
        }
    }

    /// 统一决策：本次 SOCKS CONNECT 用 IP 还是 DOMAIN
    private func decideSocksTarget() async -> SocksTarget {
        if let host = destinationHost, !host.isEmpty {
            if isFakeIP(destIP) {
                await DNSInterceptor.shared.registerMapping(fakeIP: destIP, domain: host.lowercased())
            }
            log("[TCP] Use destinationHost=\(host) as DOMAIN target.")
            return .domain(host, port: destPort)
        }
        if isFakeIP(destIP) {
            if let mapped = await DNSInterceptor.shared.getDomain(forFakeIP: destIP) {
                log("[TCP] FakeIP \(destIP) matched domain \(mapped).")
                return .domain(mapped, port: destPort)
            }
            if let hello = firstBufferedPayload(), let sni = parseSNIHost(from: hello) {
                await DNSInterceptor.shared.registerMapping(fakeIP: destIP, domain: sni)
                log("[TCP] FakeIP \(destIP) no map; recovered SNI=\(sni).")
                return .domain(sni, port: destPort)
            }
            log("[TCP] FakeIP \(destIP) no map & no SNI. Fallback to IP.")
            return .ip(destIP, port: destPort)
        }
        if let hello = firstBufferedPayload(), let sni = parseSNIHost(from: hello) {
            log("[TCP] Real IP \(destIP) but SNI=\(sni) found. Prefer DOMAIN.")
            return .domain(sni, port: destPort)
        }
        return .ip(destIP, port: destPort)
    }

    /// 步骤 2：发送 CONNECT 请求
    private func sendSocksConnectRequest() async {
        guard let connection = socksConnection else { return }

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

    /// 步骤 2 的应答
    private func receiveConnectResponse() async {
        guard socksConnection != nil else { return }

        do {
            let head = try await receiveExactly(4)
            let ver = head[0], rep = head[1], atyp = head[3]

            guard ver == 0x05 else { log("SOCKS Step 2: Bad VER \(ver)"); close(); return }
            guard rep == 0x00 else { log("SOCKS Step 2: Connect rejected. REP=\(String(format:"0x%02x", rep))"); close(); return }

            switch atyp {
            case 0x01: _ = try await receiveExactly(4)  // IPv4
            case 0x04: _ = try await receiveExactly(16) // IPv6
            case 0x03:
                let len = try await receiveExactly(1)[0]
                if len > 0 { _ = try await receiveExactly(Int(len)) }
            default:
                log("SOCKS Step 2: Unknown ATYP \(String(format:"0x%02x", atyp))"); close(); return
            }
            _ = try await receiveExactly(2) // BND.PORT

            await setSocksState(.established)
            log("SOCKS Step 2: Connect response consumed. SOCKS tunnel established!")

            await sendSynAckIfNeeded()
            await onSocksEstablishedAndFlush()
            await readFromSocks()
        } catch {
            log("SOCKS Step 2: Connect response receive error: \(error)")
            close()
        }
    }
    
    // 处理有序包
    private func processInOrderPacket(payload: Data, sequenceNumber: UInt32) {
        // 更新期望序列号
        let oldNext = nextExpectedSequence
        nextExpectedSequence = sequenceNumber &+ UInt32(payload.count)
        clientSequenceNumber = nextExpectedSequence
        
        log("In-order packet: seq=\(sequenceNumber), len=\(payload.count), next=\(nextExpectedSequence)")
        
        // 转发数据
        if socksState != .established {
            appendToPending(payload)
            log("Buffering \(payload.count) bytes (SOCKS not ready)")
        } else {
            sendRawToSocks(payload)
        }
        
        // 有序包使用延迟ACK
        scheduleDelayedAck()
    }

    // MARK: - Data forwarding (App -> SOCKS)

    private let maxOutOfOrderPackets = 100  // 防止内存泄漏的上限
    
    // 统计
    private var duplicateAckCount = 0
    private var lastAckedSequence: UInt32 = 0
    
    // 缓存乱序包
    private func bufferOutOfOrderPacket(payload: Data, sequenceNumber: UInt32) {
        if outOfOrderPackets.contains(where: { $0.seq == sequenceNumber }) {
            log("Out-of-order packet already buffered, seq: \(sequenceNumber)")
            return
        }
        let packet = (seq: sequenceNumber, data: payload)
        var left = 0, right = outOfOrderPackets.count
        while left < right {
            let mid = (left + right) / 2
            if outOfOrderPackets[mid].seq < sequenceNumber { left = mid + 1 } else { right = mid }
        }
        outOfOrderPackets.insert(packet, at: left)
        log("Buffered out-of-order packet, seq: \(sequenceNumber), len: \(payload.count), total buffered: \(outOfOrderPackets.count)")
        if outOfOrderPackets.count > maxOutOfOrderPackets {
            let removed = outOfOrderPackets.removeFirst()
            log("WARNING: Dropped old out-of-order packet due to buffer limit, seq: \(removed.seq)")
        }
    }
    
    private func processBufferedPackets() {
        var processed = 0
            
        while !outOfOrderPackets.isEmpty {
            let packet = outOfOrderPackets[0]
            
            if packet.seq == nextExpectedSequence {
                // 这个包现在可以处理了
                outOfOrderPackets.removeFirst()
                
                nextExpectedSequence = packet.seq &+ UInt32(packet.data.count)
                clientSequenceNumber = nextExpectedSequence
                
                log("Processing buffered packet: seq=\(packet.seq), len=\(packet.data.count), new next=\(nextExpectedSequence)")
                
                if socksState != .established {
                    appendToPending(packet.data)
                } else {
                    sendRawToSocks(packet.data)
                }
                processed += 1
                
            } else if packet.seq < nextExpectedSequence {
                // 过期的包，丢弃
                let overlap = nextExpectedSequence - packet.seq
                log("Dropping outdated buffered packet: seq=\(packet.seq), overlap=\(overlap)")
                outOfOrderPackets.removeFirst()
                
            } else {
                // 还有gap，停止处理
                break
            }
        }
        
        if processed > 0 {
            log("Processed \(processed) buffered packets, remaining: \(outOfOrderPackets.count)")
            // 处理完立即ACK
            sendPureAck()
            cancelDelayedAckTimer()
        }
    }

    /// 处理从 VPN 隧道来的 TCP 负载，发往 SOCKS（未建立则缓冲）
    func handlePacket(payload: Data, sequenceNumber: UInt32) {
        guard !payload.isEmpty else { return }

        if socksConnection == nil && socksState == .idle {
            Task { await self.start() }
        }
        
        // 修复：正确初始化序列号追踪
        if !handshakeAcked {
            handshakeAcked = true
            // 初始数据包的序列号就是下一个期望的序列号
            nextExpectedSequence = sequenceNumber
            clientSequenceNumber = sequenceNumber  // 不要加1
            lastAckedSequence = sequenceNumber
            log("Handshake completed (ACK: \(sequenceNumber + 1))")
        }

        // 处理不同序列号的情况
        if sequenceNumber == nextExpectedSequence {
            // 有序包
            processInOrderPacket(payload: payload, sequenceNumber: sequenceNumber)
            cancelRetransmitTimer()
            duplicateAckCount = 0
            processBufferedPackets()
            
        } else if sequenceNumber > nextExpectedSequence {
            // 未来的包（乱序）
            let gap = sequenceNumber - nextExpectedSequence
            log("Out-of-order packet: seq=\(sequenceNumber), expected=\(nextExpectedSequence), gap=\(gap)")
            
            bufferOutOfOrderPacket(payload: payload, sequenceNumber: sequenceNumber)
            
            // 立即发送ACK（带SACK）
            cancelDelayedAckTimer()
            if sackEnabled && peerSupportsSack {
                sendAckWithSack()
            } else {
                sendDuplicateAck()
            }
            
            // 只在首次出现gap时启动重传定时器
            if retransmitTimer == nil {
                startRetransmitTimer()
            }
            
        } else {
            // 重复的包
            let behind = nextExpectedSequence - sequenceNumber
            log("Duplicate packet: seq=\(sequenceNumber), expected=\(nextExpectedSequence), behind by \(behind)")
            
            // 可能是对端没收到我们的ACK，立即重发ACK
            cancelDelayedAckTimer()
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
        let ackNumber = self.clientSequenceNumber
        
        let sackOptionLength = 2 + (sackBlocks.count * 8)
        let tcpOptionsLength = 2 + sackOptionLength  // 2 个 NOP + SACK
        let tcpHeaderLength = 20 + tcpOptionsLength
        let paddedLength = ((tcpHeaderLength + 3) / 4) * 4
        let paddingNeeded = paddedLength - tcpHeaderLength
                
        var tcp = Data(count: paddedLength)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: serverSequenceNumber.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        tcp[12] = UInt8((paddedLength / 4) << 4)  // Data offset
        tcp[13] = TCPFlags.ack.rawValue
        tcp[14] = 0xFF; tcp[15] = 0xFF  // Window
        tcp[16] = 0; tcp[17] = 0        // Checksum
        tcp[18] = 0; tcp[19] = 0        // Urgent
        
        var optionOffset = 20
        tcp[optionOffset] = 0x01; optionOffset += 1 // NOP
        tcp[optionOffset] = 0x01; optionOffset += 1 // NOP
        
        tcp[optionOffset] = 0x05
        tcp[optionOffset + 1] = UInt8(sackOptionLength)
        optionOffset += 2
        for block in sackBlocks {
            withUnsafeBytes(of: block.0.bigEndian) { tcp.replaceSubrange(optionOffset..<(optionOffset+4), with: $0) }
            optionOffset += 4
            withUnsafeBytes(of: block.1.bigEndian) { tcp.replaceSubrange(optionOffset..<(optionOffset+4), with: $0) }
            optionOffset += 4
        }
        for _ in 0..<paddingNeeded { tcp[optionOffset] = 0x00; optionOffset += 1 }

        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        
        let blockDescriptions = sackBlocks.map { "[\($0.0)-\($0.1)]" }.joined(separator: ", ")
        log("Sent ACK with SACK: ACK=\(ackNumber), blocks: \(blockDescriptions)")
    }
    
    /// 在 SYN-ACK 中添加选项（MSS 按常量，含 SACK-Permitted）
    private func sendSynAckWithOptions() async {
        guard !synAckSent else { return }
        synAckSent = true
        
        let seq = serverInitialSequenceNumber
        let ackNumber = initialClientSequenceNumber &+ 1
        
        var tcp = Data(count: 40)  // 20 基本 + 20 选项
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        
        withUnsafeBytes(of: seq.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        
        tcp[12] = 0xA0  // data offset=10 (40 bytes)
        tcp[13] = TCPFlags([.syn, .ack]).rawValue
        tcp[14] = 0xFF; tcp[15] = 0xFF  // Window
        tcp[16] = 0; tcp[17] = 0        // Checksum
        tcp[18] = 0; tcp[19] = 0        // Urgent
        
        var offset = 20
        
        // MSS 选项（使用常量 1360）
        let mss = UInt16(MSS)
        tcp[offset] = 0x02; tcp[offset+1] = 0x04
        tcp[offset+2] = UInt8(mss >> 8)
        tcp[offset+3] = UInt8(mss & 0xFF)
        offset += 4
        
        // SACK-Permitted
        tcp[offset] = 0x04; tcp[offset+1] = 0x02
        offset += 2
        
        // 其余填充 NOP
        while offset < 40 { tcp[offset] = 0x01; offset += 1 }
        
        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        
        self.serverSequenceNumber = seq &+ 1
        self.clientSequenceNumber = self.initialClientSequenceNumber &+ 1
        
        log("Sent SYN-ACK with SACK-Permitted option (MSS=\(MSS))")
    }
    
    /// 获取连接统计信息
    func getDetailedStats() -> String {
        let sackBlocks = generateSackBlocks()
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
            Task { await self.log(self.getDetailedStats()) }
        }
    }
    
    // 发送重复ACK（用于快速重传）
    private func sendDuplicateAck() {
        duplicateAckCount += 1
        log("Duplicate ACK #\(duplicateAckCount) for seq: \(clientSequenceNumber)")
        if duplicateAckCount >= 3 { log("Fast retransmit triggered") }
        sendPureAck()
    }

    /// SOCKS 刚建立：冲刷缓冲数据
    private func onSocksEstablishedAndFlush() async {
        guard socksState == .established else { return }
        if pendingClientBytes.isEmpty { return }

        let totalBuffered = pendingBytesTotal
        log("Flushing \(totalBuffered) bytes buffered before SOCKS established.")

        for chunk in pendingClientBytes { sendRawToSocks(chunk) }
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
            Task { [weak self] in
                guard let self else { return }

                if let error = error {
                    await self.log("Receive error from SOCKS, closing connection: \(error)")
                    await self.sendFinToClient()
                    await self.close()
                    return
                }

                if isComplete {
                    await self.sendFinToClient()
                    await self.markUpstreamEOF()
                    await self.ensureLingerAndMaybeClose()
                    return
                }

                if let data, !data.isEmpty {
                    // 我们会带 ACK 回去，因此可以取消 pending 的延迟 ACK
                    await self.cancelDelayedAckTimer()
                    await self.log("Received \(data.count) bytes from SOCKS, writing to tunnel.")
                    await self.writeToTunnel(payload: data)
                }

                await self.readFromSocks()
            }
        }
    }

    // MARK: - TCP 发送：SYN-ACK / FIN-ACK / data

    /// 第一次真正发出 SYN-ACK（保留以备需要）
    private func sendSynAck() async {
        guard !synAckSent else { return }
        synAckSent = true
        sendSynAckCore(seq: serverInitialSequenceNumber, logPrefix: "Sending SYN-ACK to client.")
        self.serverSequenceNumber = serverInitialSequenceNumber &+ 1
        self.clientSequenceNumber = self.initialClientSequenceNumber &+ 1
        self.nextExpectedSequence = self.initialClientSequenceNumber &+ 1
    }

    /// 用固定 ISS 发送（重发用，不改变任何状态）
    public func retransmitSynAckDueToDuplicateSyn() async {
        guard !handshakeAcked else { return }
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
        // MSS + SACK-Permitted + NOP 填充（MSS 使用常量）
        var off = 20
        let mss = UInt16(MSS)
        tcp[off] = 0x02; tcp[off+1] = 0x04; tcp[off+2] = UInt8(mss >> 8); tcp[off+3] = UInt8(mss & 0xFF); off += 4
        tcp[off] = 0x04; tcp[off+1] = 0x02; off += 2
        while off < 40 { tcp[off] = 0x01; off += 1 }

        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcs = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcs >> 8); tcp[17] = UInt8(tcs & 0xFF)
        let ics = ipChecksum(&ip)
        ip[10] = UInt8(ics >> 8); ip[11] = UInt8(ics & 0xFF)
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])

        log("Retransmitted SYN-ACK (dup SYN) with options (MSS=\(MSS))")
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
                // 不再乐观启用 SACK，保持由 SYN 解析结果决定
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
        self.serverSequenceNumber &+= 1
    }

    /// 将从 SOCKS 收到的数据打包成 TCP 段写回 App（PSH,ACK）
    private func writeToTunnel(payload: Data) async {
        let ackNumber = self.clientSequenceNumber
        var currentSeq = self.serverSequenceNumber
        var remainingData = payload[...]
        var packets: [Data] = []

        // 发送前：取消 pending 的延迟 ACK（我们会在数据包里带 ACK）
        cancelDelayedAckTimer()

        while !remainingData.isEmpty {
            let segmentSize = min(remainingData.count, MSS)
            let segment = remainingData.prefix(segmentSize)
            remainingData = remainingData.dropFirst(segmentSize)

            var flags: TCPFlags = [.ack]
            flags.insert(.psh)

            var tcp = createTCPHeader(
                payloadLen: segment.count,
                flags: flags,
                sequenceNumber: currentSeq,
                acknowledgementNumber: ackNumber
            )
            var ip  = createIPv4Header(payloadLength: tcp.count + segment.count)

            let payloadData = Data(segment)
            let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: payloadData)
            tcp[16] = UInt8(tcpCsum >> 8)
            tcp[17] = UInt8(tcpCsum & 0xFF)

            let ipCsum = ipChecksum(&ip)
            ip[10] = UInt8(ipCsum >> 8)
            ip[11] = UInt8(ipCsum & 0xFF)

            packets.append(ip + tcp + segment)
            currentSeq &+= UInt32(segment.count)
        }

        if !packets.isEmpty {
            let protocols = Array(repeating: AF_INET as NSNumber, count: packets.count)
            packetFlow.writePackets(packets, withProtocols: protocols)
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
        let context = socksState == .established ? "[EST]" : "[PRE]"
        NSLog("[TCPConnection \(dynamicLogKey)] \(context) \(message)")
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
        h[0] = UInt8(destPort >> 8);   h[1] = UInt8(destPort & 0xFF)     // Source Port (server->client)
        h[2] = UInt8(sourcePort >> 8); h[3] = UInt8(sourcePort & 0xFF)   // Destination Port (client)
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
    let recLen = Int(clientHello[3]) << 8 | Int(clientHello[4])
    guard clientHello.count >= 5 + recLen else { return nil }

    var i = 5
    guard i + 4 <= clientHello.count else { return nil }
    let hsType = clientHello[i]             // 0x01 = ClientHello
    guard hsType == 0x01 else { return nil }
    let hsLen = Int(clientHello[i+1]) << 16 | Int(clientHello[i+2]) << 8 | Int(clientHello[i+3])
    i += 4
    guard i + hsLen <= clientHello.count else { return nil }

    guard i + 2 + 32 <= clientHello.count else { return nil }
    i += 2 + 32

    guard i + 1 <= clientHello.count else { return nil }
    let sidLen = Int(clientHello[i]); i += 1
    guard i + sidLen <= clientHello.count else { return nil }
    i += sidLen

    guard i + 2 <= clientHello.count else { return nil }
    let csLen = Int(clientHello[i]) << 8 | Int(clientHello[i+1]); i += 2
    guard i + csLen <= clientHello.count else { return nil }
    i += csLen

    guard i + 1 <= clientHello.count else { return nil }
    let compLen = Int(clientHello[i]); i += 1
    guard i + compLen <= clientHello.count else { return nil }
    i += compLen

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
