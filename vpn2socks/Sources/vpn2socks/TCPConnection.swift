//
//  TCPConnection.swift
//  vpn2socks
//
//  Fixed version with proper sequence number handling and SACK
//

import NetworkExtension
import Network
import Foundation

final actor TCPConnection {
    
    // MARK: - Constants
    private let MSS: Int = 1360
    private static let socksHost = NWEndpoint.Host("127.0.0.1")
    private static let socksPort = NWEndpoint.Port(integerLiteral: 8888)
    private static let pendingSoftCapBytes = 64 * 1024
    
    // MARK: - Identity
    let key: String
    private let packetFlow: SendablePacketFlow
    private var closeContinuation: CheckedContinuation<Void, Never>?
    
    private let sourceIP: IPv4Address
    private let sourcePort: UInt16
    private let destIP: IPv4Address
    private let destPort: UInt16
    private let destinationHost: String?
    
    // MARK: - TCP State
    private var synAckSent = false
    private let serverInitialSequenceNumber: UInt32
    private var handshakeAcked = false
    
    private let initialClientSequenceNumber: UInt32
    private var serverSequenceNumber: UInt32
    private var clientAckNumber: UInt32 = 0  // 我们要发给客户端的ACK号
    private var nextExpectedSequence: UInt32 = 0  // 期望接收的下一个序列号
    
    // MARK: - SACK Support
    private var sackEnabled = true
    private var peerSupportsSack = false
    private var clientSynOptions: SynOptionInfo?
    
    private struct SynOptionInfo {
        var mss: UInt16? = nil
        var windowScale: UInt8? = nil
        var sackPermitted: Bool = false
        var timestamp: (tsVal: UInt32, tsEcr: UInt32)? = nil
        var rawOptions: Data = Data()
    }
    
    // MARK: - Out-of-order handling
    private var outOfOrderPackets: [(seq: UInt32, data: Data)] = []
    private let maxOutOfOrderPackets = 100
    
    // MARK: - Timers
    private let queue = DispatchQueue(label: "tcp.connection.timer", qos: .userInitiated)
    private var delayedAckTimer: DispatchSourceTimer?
    private var delayedAckPacketsSinceLast: Int = 0
    private let delayedAckTimeoutMs: Int = 40
    
    private var retransmitTimer: DispatchSourceTimer?
    private var retransmitRetries = 0
    private let maxRetransmitRetries = 3
    
    // MARK: - SOCKS State
    private enum SocksState {
        case idle, connecting, greetingSent, methodOK, connectSent, established, closed
    }
    private var socksState: SocksState = .idle
    private var socksConnection: NWConnection?
    
    // MARK: - Buffers
    private var pendingClientBytes: [Data] = []
    private var pendingBytesTotal: Int = 0
    
    private var upstreamEOF = false
    private var lingerTask: Task<Void, Never>? = nil
    private let lingerAfterUpstreamEOFSeconds: UInt64 = 5
    
    private enum SocksReadError: Error { case closed }
    
    private enum SocksTarget {
        case ip(IPv4Address, port: UInt16)
        case domain(String, port: UInt16)
    }
    private var chosenTarget: SocksTarget?
    
    // MARK: - Initialization
    
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
        
        // 初始化序列号
        self.serverSequenceNumber = arc4random()
        self.serverInitialSequenceNumber = self.serverSequenceNumber
        self.initialClientSequenceNumber = initialSequenceNumber
        
        // 初始设置：客户端SYN后，我们期望的下一个序列号
        self.nextExpectedSequence = initialSequenceNumber &+ 1
        self.clientAckNumber = initialSequenceNumber &+ 1
        
        let source = "\(sourceIP):\(sourcePort)"
        var destination = "\(destIP)"
        if let domain = destinationHost, !domain.isEmpty {
            destination += "（\(domain)）"
        }
        destination += ":\(destPort)"
        
        NSLog("[TCPConnection \(source)->\(destination)] Initialized. InitialClientSeq: \(initialSequenceNumber)")
    }
    
    // MARK: - Lifecycle
    
    func start() async {
        guard socksConnection == nil else { return }
        
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            self.closeContinuation = continuation
            
            let conn = NWConnection(
                host: TCPConnection.socksHost,
                port: TCPConnection.socksPort,
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
    
    func close() {
        guard socksState != .closed else { return }
        socksState = .closed
        
        cancelRetransmitTimer()
        cancelDelayedAckTimer()
        
        if !outOfOrderPackets.isEmpty {
            log("Clearing \(outOfOrderPackets.count) buffered out-of-order packets")
            outOfOrderPackets.removeAll()
        }
        
        if socksConnection?.state != .cancelled {
            log("Closing connection.")
            socksConnection?.cancel()
        }
        
        socksConnection = nil
        closeContinuation?.resume()
        closeContinuation = nil
    }
    
    // MARK: - SYN Handling
    
    public func acceptClientSyn(tcpHeaderAndOptions tcpSlice: Data) async {
        noteClientSyn(tcpHeaderAndOptions: tcpSlice)
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
    
    public func sendSynAckIfNeeded() async {
        guard !synAckSent else { return }
        await sendSynAckWithOptions()
    }
    
    public func retransmitSynAckDueToDuplicateSyn() async {
        // 如果握手还没完成，重发SYN-ACK
        guard !handshakeAcked else { return }
        
        let seq = serverInitialSequenceNumber
        let ackNumber = initialClientSequenceNumber &+ 1
        
        var tcp = Data(count: 40)
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
        
        // MSS option
        let mss = UInt16(MSS)
        tcp[offset] = 0x02; tcp[offset+1] = 0x04
        tcp[offset+2] = UInt8(mss >> 8)
        tcp[offset+3] = UInt8(mss & 0xFF)
        offset += 4
        
        // SACK-Permitted
        tcp[offset] = 0x04; tcp[offset+1] = 0x02
        offset += 2
        
        // Fill with NOP
        while offset < 40 { tcp[offset] = 0x01; offset += 1 }
        
        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)
        
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        
        log("Retransmitted SYN-ACK (due to duplicate SYN) with SACK-Permitted option")
    }
    
    private func sendSynAckWithOptions() async {
        guard !synAckSent else { return }
        synAckSent = true
        
        let seq = serverInitialSequenceNumber
        let ackNumber = initialClientSequenceNumber &+ 1
        
        var tcp = Data(count: 40)
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
        
        // MSS option
        let mss = UInt16(MSS)
        tcp[offset] = 0x02; tcp[offset+1] = 0x04
        tcp[offset+2] = UInt8(mss >> 8)
        tcp[offset+3] = UInt8(mss & 0xFF)
        offset += 4
        
        // SACK-Permitted
        tcp[offset] = 0x04; tcp[offset+1] = 0x02
        offset += 2
        
        // Fill with NOP
        while offset < 40 { tcp[offset] = 0x01; offset += 1 }
        
        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)
        
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        
        self.serverSequenceNumber = seq &+ 1
        
        log("Sent SYN-ACK with SACK-Permitted option (MSS=\(MSS))")
    }
    
    // MARK: - Packet Handling
    
    func handlePacket(payload: Data, sequenceNumber: UInt32) {
        guard !payload.isEmpty else { return }
        
        // 启动SOCKS连接（如果还没有）
        if socksConnection == nil && socksState == .idle {
            Task { await self.start() }
        }
        
        // 第一个数据包表示握手完成
        if !handshakeAcked {
            handshakeAcked = true
            log("Handshake completed (first data packet received)")
        }
        
        // 处理序列号
        if sequenceNumber == nextExpectedSequence {
            // 有序包
            processInOrderPacket(payload: payload, sequenceNumber: sequenceNumber)
            
            // 尝试处理缓冲的包
            processBufferedPackets()
            
        } else if sequenceNumber > nextExpectedSequence {
            // 乱序包
            let gap = sequenceNumber - nextExpectedSequence
            log("Out-of-order packet: seq=\(sequenceNumber), expected=\(nextExpectedSequence), gap=\(gap)")
            
            bufferOutOfOrderPacket(payload: payload, sequenceNumber: sequenceNumber)
            
            // 立即发送带SACK的ACK
            sendAckWithSack()
            
        } else {
            // 重复包
            let behind = nextExpectedSequence - sequenceNumber
            log("Duplicate packet: seq=\(sequenceNumber), expected=\(nextExpectedSequence), behind by \(behind)")
            
            // 可能对端没收到ACK，重发ACK
            sendPureAck()
        }
    }
    
    private func processInOrderPacket(payload: Data, sequenceNumber: UInt32) {
        // 更新期望序列号
        nextExpectedSequence = sequenceNumber &+ UInt32(payload.count)
        clientAckNumber = nextExpectedSequence
        
        log("In-order packet: seq=\(sequenceNumber), len=\(payload.count), next=\(nextExpectedSequence)")
        
        // 转发数据
        if socksState != .established {
            appendToPending(payload)
            log("Buffering \(payload.count) bytes (SOCKS not ready)")
        } else {
            sendRawToSocks(payload)
        }
        
        // 使用延迟ACK
        scheduleDelayedAck()
    }
    
    private func bufferOutOfOrderPacket(payload: Data, sequenceNumber: UInt32) {
        // 检查是否已存在
        if outOfOrderPackets.contains(where: { $0.seq == sequenceNumber }) {
            log("Out-of-order packet already buffered, seq: \(sequenceNumber)")
            return
        }
        
        // 二分查找插入位置
        let packet = (seq: sequenceNumber, data: payload)
        var left = 0, right = outOfOrderPackets.count
        
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
                clientAckNumber = nextExpectedSequence
                
                log("Processing buffered packet: seq=\(packet.seq), len=\(packet.data.count), new next=\(nextExpectedSequence)")
                
                if socksState != .established {
                    appendToPending(packet.data)
                } else {
                    sendRawToSocks(packet.data)
                }
                
                processed += 1
                
            } else if packet.seq < nextExpectedSequence {
                // 过期的包
                log("Dropping outdated buffered packet: seq=\(packet.seq)")
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
    
    // MARK: - ACK Management
    
    public func onInboundAck(ackNumber: UInt32) {
        // 这是客户端确认的我们的序列号
        if ackNumber > serverSequenceNumber {
            log("ACK advanced beyond sent data! ack=\(ackNumber) sent=\(serverSequenceNumber)")
        }
    }
    
    private func sendPureAck() {
        let ackNumber = clientAckNumber
        
        var tcp = createTCPHeader(
            payloadLen: 0,
            flags: [.ack],
            sequenceNumber: serverSequenceNumber,
            acknowledgementNumber: ackNumber
        )
        
        var ip = createIPv4Header(payloadLength: tcp.count)
        let csum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(csum >> 8); tcp[17] = UInt8(csum & 0xFF)
        let ipcsum = ipChecksum(&ip)
        ip[10] = UInt8(ipcsum >> 8); ip[11] = UInt8(ipcsum & 0xFF)
        
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
    }
    
    private func sendAckWithSack() {
        let sackBlocks = generateSackBlocks()
        if sackBlocks.isEmpty {
            sendPureAck()
            return
        }
        
        let ackNumber = clientAckNumber
        
        // 计算TCP选项长度
        let sackOptionLength = 2 + (sackBlocks.count * 8)
        let tcpOptionsLength = 2 + sackOptionLength  // 2 NOP + SACK
        let tcpHeaderLength = 20 + tcpOptionsLength
        let paddedLength = ((tcpHeaderLength + 3) / 4) * 4
        let paddingNeeded = paddedLength - tcpHeaderLength
        
        var tcp = Data(count: paddedLength)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: serverSequenceNumber.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        tcp[12] = UInt8((paddedLength / 4) << 4)
        tcp[13] = TCPFlags.ack.rawValue
        tcp[14] = 0xFF; tcp[15] = 0xFF
        tcp[16] = 0; tcp[17] = 0
        tcp[18] = 0; tcp[19] = 0
        
        // 添加选项
        var optionOffset = 20
        tcp[optionOffset] = 0x01; optionOffset += 1  // NOP
        tcp[optionOffset] = 0x01; optionOffset += 1  // NOP
        
        // SACK选项
        tcp[optionOffset] = 0x05
        tcp[optionOffset + 1] = UInt8(sackOptionLength)
        optionOffset += 2
        
        for block in sackBlocks {
            withUnsafeBytes(of: block.0.bigEndian) { tcp.replaceSubrange(optionOffset..<(optionOffset+4), with: $0) }
            optionOffset += 4
            withUnsafeBytes(of: block.1.bigEndian) { tcp.replaceSubrange(optionOffset..<(optionOffset+4), with: $0) }
            optionOffset += 4
        }
        
        // 填充
        for _ in 0..<paddingNeeded {
            tcp[optionOffset] = 0x00
            optionOffset += 1
        }
        
        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)
        
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        
        let blockDescriptions = sackBlocks.map { "[\($0.0)-\($0.1)]" }.joined(separator: ", ")
        log("Sent ACK with SACK: ACK=\(ackNumber), blocks: \(blockDescriptions)")
    }
    
    private func generateSackBlocks() -> [(UInt32, UInt32)] {
        guard !outOfOrderPackets.isEmpty else { return [] }
        
        var blocks: [(UInt32, UInt32)] = []
        var currentStart: UInt32?
        var currentEnd: UInt32?
        
        for packet in outOfOrderPackets {
            let packetEnd = packet.seq &+ UInt32(packet.data.count)
            
            if let start = currentStart, let end = currentEnd {
                if packet.seq == end {
                    // 连续，扩展当前块
                    currentEnd = packetEnd
                } else {
                    // 不连续，保存当前块
                    blocks.append((start, end))
                    if blocks.count >= 4 { break }  // 最多4个SACK块
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
    
    // MARK: - Delayed ACK
    
    private func scheduleDelayedAck() {
        delayedAckPacketsSinceLast += 1
        
        // 每2个段或40ms发送ACK
        if delayedAckPacketsSinceLast >= 2 {
            sendPureAck()
            log("Immediate ACK (2 segments received)")
            cancelDelayedAckTimer()
            return
        }
        
        // 启动延迟定时器
        if delayedAckTimer == nil {
            let timer = DispatchSource.makeTimerSource(queue: queue)
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
    
    private func cancelDelayedAckTimer() {
        delayedAckTimer?.cancel()
        delayedAckTimer = nil
        delayedAckPacketsSinceLast = 0
    }
    
    // MARK: - Retransmit Timer
    
    private func cancelRetransmitTimer() {
        retransmitTimer?.cancel()
        retransmitTimer = nil
        retransmitRetries = 0
    }
    
    // MARK: - SOCKS Connection
    
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
        request.append(0x05)  // Version
        request.append(0x01)  // CONNECT
        request.append(0x00)  // RSV
        
        let logMessage: String
        switch target {
        case .domain(let host, let port):
            logMessage = "Step 2: Sending connect request for DOMAIN \(host):\(port)"
            request.append(0x03)  // ATYP: Domain
            let dom = Data(host.utf8)
            request.append(UInt8(dom.count))
            request.append(dom)
            request.append(contentsOf: [UInt8(port >> 8), UInt8(port & 0xFF)])
        case .ip(let ip, let port):
            logMessage = "Step 2: Sending connect request for IP \(ip):\(port)"
            request.append(0x01)  // ATYP: IPv4
            request.append(ip.rawValue)
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
    
    private func receiveConnectResponse() async {
        guard socksConnection != nil else { return }
        
        do {
            let head = try await receiveExactly(4)
            let ver = head[0], rep = head[1], atyp = head[3]
            
            guard ver == 0x05 else {
                log("SOCKS Step 2: Bad VER \(ver)")
                close()
                return
            }
            
            guard rep == 0x00 else {
                log("SOCKS Step 2: Connect rejected. REP=\(String(format:"0x%02x", rep))")
                close()
                return
            }
            
            // Skip bind address
            switch atyp {
            case 0x01: _ = try await receiveExactly(4)   // IPv4
            case 0x04: _ = try await receiveExactly(16)  // IPv6
            case 0x03:
                let len = try await receiveExactly(1)[0]
                if len > 0 { _ = try await receiveExactly(Int(len)) }
            default:
                log("SOCKS Step 2: Unknown ATYP \(String(format:"0x%02x", atyp))")
                close()
                return
            }
            
            _ = try await receiveExactly(2)  // BND.PORT
            
            await setSocksState(.established)
            log("SOCKS Step 2: Connect response consumed. SOCKS tunnel established!")
            
            await onSocksEstablishedAndFlush()
            await readFromSocks()
            
        } catch {
            log("SOCKS Step 2: Connect response receive error: \(error)")
            close()
        }
    }
    
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
    
    private func readFromSocks() async {
        socksConnection?.receive(minimumIncompleteLength: 1, maximumLength: 16384) { [weak self] data, _, isComplete, error in
            Task { [weak self] in
                guard let self else { return }
                
                if let error = error {
                    await self.log("Receive error from SOCKS: \(error)")
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
                    await self.cancelDelayedAckTimer()
                    await self.log("Received \(data.count) bytes from SOCKS, writing to tunnel.")
                    await self.writeToTunnel(payload: data)
                }
                
                await self.readFromSocks()
            }
        }
    }
    
    private func writeToTunnel(payload: Data) async {
        let ackNumber = clientAckNumber
        var currentSeq = serverSequenceNumber
        var remainingData = payload[...]
        var packets: [Data] = []
        
        cancelDelayedAckTimer()
        
        while !remainingData.isEmpty {
            let segmentSize = min(remainingData.count, MSS)
            let segment = remainingData.prefix(segmentSize)
            remainingData = remainingData.dropFirst(segmentSize)
            
            var tcp = createTCPHeader(
                payloadLen: segment.count,
                flags: [.ack, .psh],
                sequenceNumber: currentSeq,
                acknowledgementNumber: ackNumber
            )
            
            var ip = createIPv4Header(payloadLength: tcp.count + segment.count)
            
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
            serverSequenceNumber = currentSeq
        }
    }
    
    private func sendFinToClient() async {
        let ackNumber = clientAckNumber
        
        var tcp = createTCPHeader(
            payloadLen: 0,
            flags: [.fin, .ack],
            sequenceNumber: serverSequenceNumber,
            acknowledgementNumber: ackNumber
        )
        
        var ip = createIPv4Header(payloadLength: tcp.count)
        let csum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(csum >> 8); tcp[17] = UInt8(csum & 0xFF)
        let ipcsum = ipChecksum(&ip)
        ip[10] = UInt8(ipcsum >> 8); ip[11] = UInt8(ipcsum & 0xFF)
        
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        serverSequenceNumber &+= 1
    }
    
    // MARK: - Helper Functions
    
    private func log(_ message: String) {
        let context = socksState == .established ? "[EST]" : "[PRE]"
        let key = dynamicLogKey
        NSLog("[TCPConnection \(key)] \(context) \(message)")
    }
    
    private var dynamicLogKey: String {
        let sourceString = "\(sourceIP):\(sourcePort)"
        var destinationString = "\(destIP)"
        
        if let host = destinationHost, !host.isEmpty {
            destinationString += "（\(host)）"
        } else if case .domain(let host, _) = chosenTarget {
            destinationString += "（\(host)）"
        }
        
        destinationString += ":\(destPort)"
        return "\(sourceString)->\(destinationString)"
    }
    
    private func setSocksState(_ newState: SocksState) async {
        self.socksState = newState
    }
    
    private func markUpstreamEOF() {
        upstreamEOF = true
    }
    
    private func ensureLingerAndMaybeClose() {
        if lingerTask == nil {
            let delay = self.lingerAfterUpstreamEOFSeconds
            lingerTask = Task { [weak self] in
                try? await Task.sleep(nanoseconds: delay * 1_000_000_000)
                guard let self = self else { return }
                await self.close()
            }
        }
    }
    
    private func appendToPending(_ data: Data) {
        pendingClientBytes.append(data)
        pendingBytesTotal &+= data.count
        trimPendingIfNeeded()
    }
    
    private func trimPendingIfNeeded() {
        guard pendingBytesTotal > TCPConnection.pendingSoftCapBytes,
              !pendingClientBytes.isEmpty else { return }
        
        var dropped = 0
        
        // Keep removing from index 1 (preserving first segment) until under limit
        while pendingBytesTotal > TCPConnection.pendingSoftCapBytes && pendingClientBytes.count > 1 {
            let sz = pendingClientBytes[1].count
            pendingClientBytes.remove(at: 1)
            pendingBytesTotal &-= sz
            dropped &+= sz
        }
        
        if dropped > 0 {
            log("Pending buffer capped at \(TCPConnection.pendingSoftCapBytes)B; dropped \(dropped)B")
        }
    }
    
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
    
    private static func loopParams() -> NWParameters {
        let p = NWParameters.tcp
        p.requiredInterfaceType = .loopback
        p.allowLocalEndpointReuse = true
        return p
    }
    
    private func receiveChunk(max: Int) async throws -> Data {
        guard let conn = socksConnection else { throw SocksReadError.closed }
        
        return try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Data, Error>) in
            conn.receive(minimumIncompleteLength: 1, maximumLength: max) { data, _, isComplete, error in
                if let error = error {
                    cont.resume(throwing: error)
                    return
                }
                if isComplete {
                    if let d = data, !d.isEmpty {
                        cont.resume(returning: d)
                    } else {
                        cont.resume(throwing: SocksReadError.closed)
                    }
                    return
                }
                cont.resume(returning: data ?? Data())
            }
        }
    }
    
    private func receiveExactly(_ n: Int) async throws -> Data {
        var buf = Data()
        buf.reserveCapacity(n)
        
        while buf.count < n {
            let chunk = try await receiveChunk(max: n - buf.count)
            if chunk.isEmpty { throw SocksReadError.closed }
            buf.append(chunk)
        }
        
        return buf
    }
    
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
        }
        
        return .ip(destIP, port: destPort)
    }
    
    // MARK: - Packet Creation
    
    private func createIPv4Header(payloadLength: Int) -> Data {
        var header = Data(count: 20)
        header[0] = 0x45  // Version=4, IHL=5
        let totalLength = 20 + payloadLength
        header[2] = UInt8(totalLength >> 8)
        header[3] = UInt8(totalLength & 0xFF)
        header[4] = 0x00; header[5] = 0x00  // ID
        header[6] = 0x40; header[7] = 0x00  // DF
        header[8] = 64  // TTL
        header[9] = 6   // TCP
        header[10] = 0; header[11] = 0  // Checksum
        
        // Source = server (destIP), Dest = client (sourceIP)
        let src = [UInt8](destIP.rawValue)
        let dst = [UInt8](sourceIP.rawValue)
        header[12] = src[0]; header[13] = src[1]; header[14] = src[2]; header[15] = src[3]
        header[16] = dst[0]; header[17] = dst[1]; header[18] = dst[2]; header[19] = dst[3]
        
        return header
    }
    
    private func createTCPHeader(
        payloadLen: Int,
        flags: TCPFlags,
        sequenceNumber: UInt32,
        acknowledgementNumber: UInt32
    ) -> Data {
        var h = Data(count: 20)
        h[0] = UInt8(destPort >> 8); h[1] = UInt8(destPort & 0xFF)
        h[2] = UInt8(sourcePort >> 8); h[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: sequenceNumber.bigEndian) { h.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: acknowledgementNumber.bigEndian) { h.replaceSubrange(8..<12, with: $0) }
        h[12] = 0x50  // Data offset = 5
        h[13] = flags.rawValue
        h[14] = 0xFF; h[15] = 0xFF  // Window
        h[16] = 0; h[17] = 0  // Checksum
        h[18] = 0; h[19] = 0  // Urgent
        return h
    }
    
    private func tcpChecksum(ipHeader ip: Data, tcpHeader tcp: Data, payload: Data) -> UInt16 {
        var pseudo = Data()
        pseudo.append(ip[12...15])
        pseudo.append(ip[16...19])
        pseudo.append(0)  // Reserved
        pseudo.append(6)  // TCP
        let tcpLen = UInt16(tcp.count + payload.count)
        pseudo.append(UInt8(tcpLen >> 8))
        pseudo.append(UInt8(tcpLen & 0xFF))
        
        var sumData = pseudo + tcp + payload
        if sumData.count % 2 == 1 { sumData.append(0) }
        
        var sum: UInt32 = 0
        for i in stride(from: 0, to: sumData.count, by: 2) {
            let word = (UInt16(sumData[i]) << 8) | UInt16(sumData[i+1])
            sum &+= UInt32(word)
        }
        
        while (sum >> 16) != 0 {
            sum = (sum & 0xFFFF) &+ (sum >> 16)
        }
        
        return ~UInt16(sum & 0xFFFF)
    }
    
    private func ipChecksum(_ h: inout Data) -> UInt16 {
        h[10] = 0; h[11] = 0
        var sum: UInt32 = 0
        
        for i in stride(from: 0, to: h.count, by: 2) {
            let word = (UInt16(h[i]) << 8) | UInt16(h[i+1])
            sum &+= UInt32(word)
        }
        
        while (sum >> 16) != 0 {
            sum = (sum & 0xFFFF) &+ (sum >> 16)
        }
        
        return ~UInt16(sum & 0xFFFF)
    }
    
    // MARK: - SYN Option Parsing
    
    private func parseSynOptionsFromTcpHeader(_ tcpHeaderAndOptions: Data) -> SynOptionInfo? {
        guard tcpHeaderAndOptions.count >= 20 else { return nil }
        
        let dataOffsetWords = (tcpHeaderAndOptions[12] >> 4) & 0x0F
        let headerLen = Int(dataOffsetWords) * 4
        guard headerLen >= 20, tcpHeaderAndOptions.count >= headerLen else { return nil }
        
        let options = tcpHeaderAndOptions[20..<headerLen]
        var info = SynOptionInfo(rawOptions: Data(options))
        
        var i = options.startIndex
        while i < options.endIndex {
            let kind = options[i]
            i = options.index(after: i)
            
            switch kind {
            case 0:  // EOL
                return info
            case 1:  // NOP
                continue
            default:
                if i >= options.endIndex { return info }
                let len = Int(options[i])
                i = options.index(after: i)
                
                let optStart = options.index(i, offsetBy: -2)
                guard len >= 2,
                      options.distance(from: optStart, to: options.endIndex) >= len else { return info }
                
                let payloadStart = options.index(optStart, offsetBy: 2)
                let payloadEnd = options.index(optStart, offsetBy: len)
                
                switch kind {
                case 2:  // MSS
                    if len == 4 {
                        let b0 = options[payloadStart]
                        let b1 = options[options.index(payloadStart, offsetBy: 1)]
                        info.mss = be16(b0, b1)
                    }
                case 3:  // Window Scale
                    if len == 3 {
                        info.windowScale = options[payloadStart]
                    }
                case 4:  // SACK Permitted
                    if len == 2 {
                        info.sackPermitted = true
                    }
                case 8:  // Timestamp
                    if len == 10 {
                        // Parse timestamp values if needed
                    }
                default:
                    break
                }
                
                i = payloadEnd
            }
        }
        
        return info
    }
    
    @inline(__always)
    private func be16(_ a: UInt8, _ b: UInt8) -> UInt16 {
        (UInt16(a) << 8) | UInt16(b)
    }
}

// MARK: - Supporting Types

struct TCPFlags: OptionSet {
    let rawValue: UInt8
    static let fin = TCPFlags(rawValue: 1 << 0)
    static let syn = TCPFlags(rawValue: 1 << 1)
    static let rst = TCPFlags(rawValue: 1 << 2)
    static let psh = TCPFlags(rawValue: 1 << 3)
    static let ack = TCPFlags(rawValue: 1 << 4)
}

private func isFakeIP(_ ip: IPv4Address) -> Bool {
    let b = [UInt8](ip.rawValue)
    return b.count == 4 && b[0] == 198 && (b[1] & 0xFE) == 18
}

extension Data {
    fileprivate func hexEncodedString() -> String {
        map { String(format: "%02hhx", $0) }.joined()
    }
}
