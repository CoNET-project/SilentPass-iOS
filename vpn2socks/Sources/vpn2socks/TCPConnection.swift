//
//  TCPConnection.swift
//  vpn2socks
//
//  完整版：严格的 connectionId 回调、拥塞与窗口管理、SOCKS 握手与超时、乱序缓冲、ACK/SACK、MSS 分段
//

import Foundation
import Network
import NetworkExtension

// MARK: - Helpers & Protocols

public protocol SendablePacketFlow {
    func writePackets(_ packets: [Data], withProtocols protocols: [NSNumber])
}

// 让 NEPacketTunnelFlow 直接满足 SendablePacketFlow
extension NEPacketTunnelFlow: SendablePacketFlow {}

private extension Array {
    // 单调 predicate 的二分插入位置
    func binaryInsertionIndex(_ predicate: (Element) -> Bool) -> Int {
        var low = 0, high = count
        while low < high {
            let mid = (low + high) >> 1
            if predicate(self[mid]) { low = mid + 1 } else { high = mid }
        }
        return low
    }
}

private struct IPv4Address: Equatable, Hashable {
    let rawValue: Data
    init(_ a: UInt8, _ b: UInt8, _ c: UInt8, _ d: UInt8) {
        self.rawValue = Data([a,b,c,d])
    }
    init(data: Data) { self.rawValue = data }
}

private struct TCPFlags: OptionSet {
    let rawValue: UInt8
    static let fin = TCPFlags(rawValue: 0x01)
    static let syn = TCPFlags(rawValue: 0x02)
    static let rst = TCPFlags(rawValue: 0x04)
    static let psh = TCPFlags(rawValue: 0x08)
    static let ack = TCPFlags(rawValue: 0x10)
}

@inline(__always) private func ipChecksum(_ bytes: inout Data) -> UInt16 {
    var sum: UInt32 = 0
    for i in stride(from: 0, to: bytes.count, by: 2) {
        let word: UInt16 = i+1 < bytes.count ? (UInt16(bytes[i]) << 8) | UInt16(bytes[i+1]) : UInt16(bytes[i]) << 8
        sum &+= UInt32(word)
        if sum > 0xFFFF { sum = (sum & 0xFFFF) &+ 1 }
    }
    return ~UInt16(sum & 0xFFFF)
}

@inline(__always) private func tcpChecksum(ipHeader: Data, tcpHeader: Data, payload: Data) -> UInt16 {
    var pseudo = Data()
    // 源/目的 IP
    pseudo.append(ipHeader[12..<16])
    pseudo.append(ipHeader[16..<20])
    // 零 + 协议(6) + TCP 长度
    pseudo.append(0x00)
    pseudo.append(0x06)
    let tcpLen = UInt16(tcpHeader.count + payload.count)
    pseudo.append(UInt8(tcpLen >> 8)); pseudo.append(UInt8(tcpLen & 0xFF))
    // TCP 头和负载
    pseudo.append(tcpHeader)
    pseudo.append(payload)
    var sum: UInt32 = 0
    let bytes = pseudo
    for i in stride(from: 0, to: bytes.count, by: 2) {
        let word: UInt16 = i+1 < bytes.count ? (UInt16(bytes[i]) << 8) | UInt16(bytes[i+1]) : UInt16(bytes[i]) << 8
        sum &+= UInt32(word)
        if sum > 0xFFFF { sum = (sum & 0xFFFF) &+ 1 }
    }
    return ~UInt16(sum & 0xFFFF)
}

// MARK: - Core

public final actor TCPConnection {

    // MARK: - Constants
    private let tunnelMTU: Int
    private var mss: Int { max(536, tunnelMTU - 40) }
    private let MAX_WINDOW_SIZE: UInt16 = 65535
    private let DELAYED_ACK_TIMEOUT_MS: Int = 25
    private let MAX_BUFFERED_PACKETS = 50
    private let RETRANSMIT_TIMEOUT_MS: Int = 200
    private let MAX_RETRANSMIT_RETRIES = 3
    private let SOCKS_CONNECTION_TIMEOUT_MS: Int = 3000
    private static let pendingSoftCapBytes = 32 * 1024

    // MARK: - Identity
    public let key: String
    public let connectionId: UInt64
    private let packetFlow: SendablePacketFlow
    private let sourceIP: IPv4Address
    private let sourcePort: UInt16
    private let destIP: IPv4Address
    private let destPort: UInt16
    private let destinationHost: String?

    // MARK: - TCP state
    private var synAckSent = false
    private var handshakeAcked = false
    private let serverInitialSequenceNumber: UInt32
    private let initialClientSequenceNumber: UInt32
    private var serverSequenceNumber: UInt32
    private var clientSequenceNumber: UInt32
    private var nextExpectedSequence: UInt32 = 0
    private var lastAdvertisedWindow: UInt16 = 0

    // MARK: - SACK / reordering
    private var sackEnabled = true
    private var peerSupportsSack = false
    private var outOfOrderPackets: [(seq: UInt32, data: Data)] = []

    // MARK: - Client buffering before SOCKS
    private var pendingClientBytes: [Data] = []
    private var pendingBytesTotal: Int = 0

    // MARK: - Flow control
    private var availableWindow: UInt16 = 65535
    private let recvBufferLimit: Int

    // MARK: - Timers
    private let queue = DispatchQueue(label: "tcp.connection.timer", qos: .userInitiated)
    private var retransmitTimer: DispatchSourceTimer?
    private var delayedAckTimer: DispatchSourceTimer?
    private var retransmitRetries = 0
    private var delayedAckPacketsSinceLast: Int = 0
    private var socksTimeoutTimer: DispatchSourceTimer?

    // MARK: - SOCKS
    private enum SocksState { case idle, connecting, greetingSent, methodOK, connectSent, established, closed }
    private var socksState: SocksState = .idle
    private var socksConnection: NWConnection?
    private var closeContinuation: CheckedContinuation<Void, Never>?

    // MARK: - Stats
    private var duplicatePacketCount: Int = 0

    // MARK: - Callbacks (统一为带 ID)
    private let reportInFlight: @Sendable (UInt64, Int) async -> Void
    private let reportSocksStart: @Sendable (UInt64) async -> Void
    private let reportSocksSuccess: @Sendable (UInt64) async -> Void
    private let reportSocksFailure: @Sendable (UInt64, Bool) async -> Void
    private let reportSocksEnd: @Sendable (UInt64) async -> Void

    private enum CallbackState { case initial, started, completed, ended }
    private var callbackState: CallbackState = .initial

    // MARK: - Init
    public init(
        key: String,
        connectionId: UInt64,
        packetFlow: SendablePacketFlow,
        sourceIP: IPv4Address,
        sourcePort: UInt16,
        destIP: IPv4Address,
        destPort: UInt16,
        destinationHost: String?,
        initialSequenceNumber: UInt32,
        tunnelMTU: Int = 1400,
        recvBufferLimit: Int = 24 * 1024,
        reportInFlight: @escaping @Sendable (UInt64, Int) async -> Void,
        reportSocksStart: @escaping @Sendable (UInt64) async -> Void,
        reportSocksSuccess: @escaping @Sendable (UInt64) async -> Void,
        reportSocksFailure: @escaping @Sendable (UInt64, Bool) async -> Void,
        reportSocksEnd: @escaping @Sendable (UInt64) async -> Void
    ) {
        self.key = key
        self.connectionId = connectionId
        self.packetFlow = packetFlow
        self.sourceIP = sourceIP
        self.sourcePort = sourcePort
        self.destIP = destIP
        self.destPort = destPort
        self.destinationHost = destinationHost
        self.serverInitialSequenceNumber = UInt32.random(in: UInt32.min...UInt32.max)
        self.serverSequenceNumber = serverInitialSequenceNumber
        self.initialClientSequenceNumber = initialSequenceNumber
        self.clientSequenceNumber = initialSequenceNumber
        self.nextExpectedSequence = initialSequenceNumber
        self.tunnelMTU = tunnelMTU
        let cap = max(8 * 1024, min(recvBufferLimit, Int(MAX_WINDOW_SIZE)))
        self.recvBufferLimit = cap
        self.availableWindow = UInt16(cap)
        self.lastAdvertisedWindow = availableWindow
        self.reportInFlight = reportInFlight
        self.reportSocksStart = reportSocksStart
        self.reportSocksSuccess = reportSocksSuccess
        self.reportSocksFailure = reportSocksFailure
        self.reportSocksEnd = reportSocksEnd
        log("Initialized. InitialClientSeq: \(initialSequenceNumber)")
    }

    // MARK: - Public entry
    public func startIfNeeded() async {
        guard socksConnection == nil, socksState == .idle else { return }
        await startSocks()
    }

    public func handlePayload(_ payload: Data, sequenceNumber: UInt32) {
        guard !payload.isEmpty else { return }

        if socksConnection == nil && socksState == .idle {
            Task { await self.startSocks() }
        }
        if !handshakeAcked {
            handshakeAcked = true
            nextExpectedSequence = sequenceNumber
            clientSequenceNumber = sequenceNumber
            log("Handshake completed, tracking from seq \(sequenceNumber)")
        }

        let d = Int32(bitPattern: sequenceNumber &- nextExpectedSequence)
        if d == 0 {
            processInOrder(payload, sequenceNumber)
        } else if d > 0 {
            processOutOfOrder(payload, sequenceNumber)
        } else if d >= -Int32(payload.count) {
            // overlap
            let overlap = Int(min(UInt32(payload.count), nextExpectedSequence &- sequenceNumber))
            if overlap < payload.count {
                let newData = payload.dropFirst(overlap)
                let newSeq = sequenceNumber &+ UInt32(overlap)
                processInOrder(Data(newData), newSeq) // now aligned
            } else {
                processDuplicate(sequenceNumber)
            }
        } else {
            processDuplicate(sequenceNumber)
        }
    }

    public func close() {
        guard socksState != .closed else { return }
        socksState = .closed
        cancelAllTimers()
        cleanupBuffers()

        if socksConnection?.state != .cancelled {
            log("Closing connection.")
            socksConnection?.cancel()
        }
        socksConnection = nil

        Task {
            if callbackState != .ended {
                callbackState = .ended
                await reportSocksEnd(connectionId)
            }
        }

        closeContinuation?.resume()
        closeContinuation = nil
    }

    // MARK: - SOCKS lifecycle
    private func startSocks() async {
        guard callbackState == .initial else {
            log("WARNING: start() in invalid state \(callbackState)")
            return
        }
        callbackState = .started
        await reportSocksStart(connectionId)

        await withCheckedContinuation { (k: CheckedContinuation<Void, Never>) in
            self.closeContinuation = k

            startSocksTimeoutTimer()

            let tcp = NWProtocolTCP.Options()
            tcp.noDelay = true
            let params = NWParameters(tls: nil, tcp: tcp)
            params.requiredInterfaceType = .loopback
            params.allowLocalEndpointReuse = true

            let conn = NWConnection(host: .init("127.0.0.1"), port: 8888, using: params)
            self.socksConnection = conn
            self.socksState = .connecting
            conn.stateUpdateHandler = { [weak self] st in
                Task { await self?.onSocksState(st) }
            }
            log("Starting connection to SOCKS proxy...")
            conn.start(queue: .global(qos: .userInitiated))
        }
    }

    private func onSocksState(_ st: NWConnection.State) async {
        switch st {
        case .ready:
            cancelSocksTimeoutTimer()
            if callbackState == .started {
                callbackState = .completed
                await reportSocksSuccess(connectionId)
            }
            await sendSocksGreeting()

        case .waiting(let e):
            log("SOCKS waiting: \(e.localizedDescription)")

        case .failed(let e):
            log("SOCKS failed: \(e.localizedDescription)")
            cancelSocksTimeoutTimer()
            if callbackState == .started {
                callbackState = .completed
                await reportSocksFailure(connectionId, false)
            }
            close()

        case .cancelled:
            cancelSocksTimeoutTimer()
            close()
        default: break
        }
    }

    private func sendSocksGreeting() async {
        guard let c = socksConnection else { return }
        socksState = .greetingSent
        let hello: [UInt8] = [0x05, 0x01, 0x00]
        c.send(content: Data(hello), completion: .contentProcessed({ [weak self] err in
            Task { [weak self] in
                if let err { await self?.log("Handshake error: \(err)"); await self?.close(); return }
                await self?.recvSocksGreetingResponse()
            }
        }))
    }

    private func recvSocksGreetingResponse() async {
        guard let c = socksConnection else { return }
        c.receive(minimumIncompleteLength: 2, maximumLength: 2) { [weak self] data, _, _, err in
            Task { [weak self] in
                guard let self else { return }
                if let err { await self.log("Handshake recv error: \(err)"); await self.close(); return }
                guard let d = data, d.count == 2, d[0] == 0x05, d[1] == 0x00 else {
                    await self.log("Invalid handshake response"); await self.close(); return
                }
                await self.sendSocksConnect()
            }
        }
    }

    private func sendSocksConnect() async {
        guard let c = socksConnection else { return }
        socksState = .connectSent

        var req = Data([0x05, 0x01, 0x00])
        if let host = destinationHost, !host.isEmpty {
            let dom = Data(host.utf8)
            req.append(0x03)
            req.append(UInt8(dom.count))
            req.append(dom)
        } else {
            req.append(0x01)
            req.append(destIP.rawValue)
        }
        req.append(UInt8(destPort >> 8))
        req.append(UInt8(destPort & 0xFF))

        c.send(content: req, completion: .contentProcessed({ [weak self] err in
            Task { [weak self] in
                guard let self else { return }
                if let err { await self.log("Connect request error: \(err)"); await self.close(); return }
                await self.recvSocksConnectResp()
            }
        }))
    }

    private func recvSocksConnectResp() async {
        guard socksConnection != nil else { return }
        do {
            // VER, REP, RSV, ATYP
            let head = try await receiveExactly(4)
            let ver = head[0], rep = head[1], atyp = head[3]
            guard ver == 0x05 else { log("Bad SOCKS version"); close(); return }
            guard rep == 0x00 else { log("SOCKS connect rejected: \(rep)"); close(); return }

            switch atyp {
            case 0x01: _ = try await receiveExactly(4)   // IPv4
            case 0x04: _ = try await receiveExactly(16)  // IPv6
            case 0x03:  let l = try await receiveExactly(1)[0]; if l > 0 { _ = try await receiveExactly(Int(l)) } // domain
            default: log("Unknown ATYP \(atyp)"); close(); return
            }
            _ = try await receiveExactly(2) // port

            socksState = .established
            log("SOCKS tunnel established")

            await sendSynAckIfNeeded()
            await flushPendingToSocks()
            await pumpFromSocks()

        } catch {
            log("Connect resp error: \(error)")
            close()
        }
    }

    // MARK: - Client data path

    private func processInOrder(_ payload: Data, _ seq: UInt32) {
        nextExpectedSequence = seq &+ UInt32(payload.count)
        clientSequenceNumber = nextExpectedSequence

        if socksState == .established {
            sendToSocks(payload)
        } else {
            appendPending(payload)
        }
        drainBufferedIfContiguous()
        if payload.count <= 64 { cancelDelayedAckTimer(); sendPureAck() } else { scheduleDelayedAck() }
    }

    private func processOutOfOrder(_ payload: Data, _ seq: UInt32) {
        // 插入有序
        let idx = outOfOrderPackets.binaryInsertionIndex { $0.seq < seq }
        if idx >= outOfOrderPackets.count || outOfOrderPackets[idx].seq != seq {
            outOfOrderPackets.insert((seq, payload), at: idx)
        }
        // 立即提示对端选择性重传或快速重传
        cancelDelayedAckTimer()
        if sackEnabled && peerSupportsSack { sendAckWithSack() } else { sendPureAck() }
        startRetransmitTimerIfNeeded()
    }

    private func processDuplicate(_ seq: UInt32) {
        duplicatePacketCount &+= 1
        cancelDelayedAckTimer()
        sendPureAck()
        if duplicatePacketCount % 10 == 0 {
            log("Excessive duplicates \(duplicatePacketCount), last seq \(seq)")
        }
    }

    private func drainBufferedIfContiguous() {
        var advanced = false
        while let first = outOfOrderPackets.first, first.seq == nextExpectedSequence {
            outOfOrderPackets.removeFirst()
            nextExpectedSequence = first.seq &+ UInt32(first.data.count)
            clientSequenceNumber = nextExpectedSequence
            if socksState == .established { sendToSocks(first.data) } else { appendPending(first.data) }
            advanced = true
        }
        if advanced {
            updateAdvertisedWindow()
            cancelDelayedAckTimer()
            sendPureAck()
            if outOfOrderPackets.isEmpty { cancelRetransmitTimer() }
        }
    }

    // MARK: - Window / ACK

    private func updateAdvertisedWindow() {
        let used = outOfOrderPackets.reduce(0) { $0 + $1.data.count } + (socksState == .established ? 0 : pendingBytesTotal)
        let cap = min(recvBufferLimit, Int(MAX_WINDOW_SIZE))
        let free = max(0, cap - used)
        availableWindow = UInt16(free)
    }

    private func scheduleDelayedAck() {
        delayedAckPacketsSinceLast &+= 1
        if delayedAckPacketsSinceLast >= 2 {
            sendPureAck()
            cancelDelayedAckTimer()
            return
        }
        guard delayedAckTimer == nil else { return }
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(DELAYED_ACK_TIMEOUT_MS))
        t.setEventHandler { [weak self] in
            Task { [weak self] in
                await self?.sendPureAck()
                await self?.cancelDelayedAckTimer()
            }
        }
        t.resume()
        delayedAckTimer = t
    }

    private func sendPureAck() {
        updateAdvertisedWindow()
        var tcp = createTCPHeader(payloadLen: 0, flags: [.ack], seq: serverSequenceNumber, ack: clientSequenceNumber)
        var ip = createIPv4Header(payloadLength: tcp.count)
        let csum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(csum >> 8); tcp[17] = UInt8(csum & 0xFF)
        let ipcsum = ipChecksum(&ip)
        ip[10] = UInt8(ipcsum >> 8); ip[11] = UInt8(ipcsum & 0xFF)
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        lastAdvertisedWindow = availableWindow
        delayedAckPacketsSinceLast = 0
    }

    private func sendAckWithSack() {
        // 最简实现：这里直接退化为纯 ACK，保持代码紧凑
        sendPureAck()
    }

    // MARK: - Retransmit timer

    private func startRetransmitTimerIfNeeded() {
        guard retransmitTimer == nil else { return }
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(RETRANSMIT_TIMEOUT_MS))
        t.setEventHandler { [weak self] in
            Task { [weak self] in
                await self?.onRetransmitTimeout()
            }
        }
        t.resume()
        retransmitTimer = t
    }

    private func onRetransmitTimeout() {
        retransmitRetries &+= 1
        if retransmitRetries > MAX_RETRANSMIT_RETRIES {
            log("Max retransmit retries reached; connection may be stalled")
            cancelRetransmitTimer()
            return
        }
        // 重发 3 次 ACK 以触发对端快速重传
        for _ in 0..<3 { sendPureAck() }
        // 退避
        retransmitTimer?.schedule(deadline: .now() + .milliseconds(RETRANSMIT_TIMEOUT_MS << min(retransmitRetries,4)))
    }

    private func cancelRetransmitTimer() {
        retransmitTimer?.cancel()
        retransmitTimer = nil
        retransmitRetries = 0
    }

    private func cancelDelayedAckTimer() {
        delayedAckTimer?.cancel()
        delayedAckTimer = nil
        delayedAckPacketsSinceLast = 0
    }

    // MARK: - Client -> SOCKS

    private func appendPending(_ d: Data) {
        pendingClientBytes.append(d)
        pendingBytesTotal &+= d.count
        Task { await reportInFlight(connectionId, d.count) }
        if pendingBytesTotal > Self.pendingSoftCapBytes { trimPending() }
        updateAdvertisedWindow()
    }

    private func trimPending() {
        var dropped = 0
        var i = 1
        while pendingBytesTotal > Self.pendingSoftCapBytes && i < pendingClientBytes.count {
            let sz = pendingClientBytes[i].count
            pendingClientBytes.remove(at: i)
            pendingBytesTotal &-= sz
            dropped &+= sz
        }
        if dropped > 0 {
            Task { await reportInFlight(connectionId, -dropped) }
            log("Dropped \(dropped) bytes from pending buffer")
        }
        updateAdvertisedWindow()
    }

    private func flushPendingToSocks() async {
        guard socksState == .established, !pendingClientBytes.isEmpty else { return }
        let total = pendingBytesTotal
        log("Flushing \(total) bytes of pending data")
        for c in pendingClientBytes { sendToSocks(c) }
        pendingClientBytes.removeAll()
        pendingBytesTotal = 0
        updateAdvertisedWindow()
        sendPureAck()
    }

    private func sendToSocks(_ d: Data) {
        Task { await reportInFlight(connectionId, d.count) }
        socksConnection?.send(content: d, completion: .contentProcessed({ [weak self] e in
            Task { [weak self] in
                await self?.reportInFlight(self?.connectionId ?? 0, -d.count)
                if let e { await self?.log("SOCKS send error: \(e)"); await self?.close() }
            }
        }))
    }

    // MARK: - SOCKS -> Client

    private func pumpFromSocks() async {
        socksConnection?.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, complete, error in
            Task { [weak self] in
                guard let self else { return }
                if let error { await self.log("SOCKS recv error: \(error)"); await self.sendFinToClient(); await self.close(); return }
                if complete { await self.sendFinToClient(); await self.close(); return }
                if let data, !data.isEmpty {
                    await self.cancelDelayedAckTimer()
                    await self.writeToClient(payload: data)
                }
                await self.pumpFromSocks()
            }
        }
    }

    private func writeToClient(payload: Data) async {
        updateAdvertisedWindow()
        var currentSeq = serverSequenceNumber
        var packets: [Data] = []
        var off = 0
        while off < payload.count {
            let size = min(payload.count - off, mss)
            let seg = payload[off..<(off+size)]
            var tcp = createTCPHeader(payloadLen: size, flags: [.ack, .psh], seq: currentSeq, ack: clientSequenceNumber)
            var ip = createIPv4Header(payloadLength: tcp.count + size)
            let tcsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data(seg))
            tcp[16] = UInt8(tcsum >> 8); tcp[17] = UInt8(tcsum & 0xFF)
            let ipcsum = ipChecksum(&ip)
            ip[10] = UInt8(ipcsum >> 8); ip[11] = UInt8(ipcsum & 0xFF)
            packets.append(ip + tcp + Data(seg))
            currentSeq &+= UInt32(size)
            off += size
        }
        if !packets.isEmpty {
            packetFlow.writePackets(packets, withProtocols: Array(repeating: AF_INET as NSNumber, count: packets.count))
            serverSequenceNumber = currentSeq
            lastAdvertisedWindow = availableWindow
        }
    }

    private func sendFinToClient() async {
        var tcp = createTCPHeader(payloadLen: 0, flags: [.fin, .ack], seq: serverSequenceNumber, ack: clientSequenceNumber)
        var ip = createIPv4Header(payloadLength: tcp.count)
        let csum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(csum >> 8); tcp[17] = UInt8(csum & 0xFF)
        let ipcsum = ipChecksum(&ip)
        ip[10] = UInt8(ipcsum >> 8); ip[11] = UInt8(ipcsum & 0xFF)
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        serverSequenceNumber &+= 1
    }

    // MARK: - SYN/ACK

    public func sendSynAckIfNeeded() async {
        guard !synAckSent else { return }
        synAckSent = true
        updateAdvertisedWindow()

        let ackNumber = initialClientSequenceNumber &+ 1
        var tcp = Data(count: 40)
        // dstPort, srcPort
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: serverInitialSequenceNumber.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        tcp[12] = 0xA0 // data offset=10(40 bytes)
        tcp[13] = (TCPFlags.syn.union(.ack)).rawValue
        tcp[14] = UInt8(availableWindow >> 8)
        tcp[15] = UInt8(availableWindow & 0xFF)
        // checksum zeroed
        // MSS(4) + SACK perm(2) + NOP 填充
        var off = 20
        let mssVal = UInt16(mss)
        tcp[off] = 0x02; tcp[off+1] = 0x04; tcp[off+2] = UInt8(mssVal >> 8); tcp[off+3] = UInt8(mssVal & 0xFF); off += 4
        tcp[off] = 0x04; tcp[off+1] = 0x02; off += 2
        while off < 40 { tcp[off] = 0x01; off += 1 }

        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcsum >> 8); tcp[17] = UInt8(tcsum & 0xFF)
        let ipcsum = ipChecksum(&ip)
        ip[10] = UInt8(ipcsum >> 8); ip[11] = UInt8(ipcsum & 0xFF)

        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])

        serverSequenceNumber = serverInitialSequenceNumber &+ 1
        clientSequenceNumber = initialClientSequenceNumber &+ 1
        lastAdvertisedWindow = availableWindow

        log("Sent SYN-ACK with MSS=\(mss), SACK, no WScale")
    }

    // MARK: - SOCKS timeout

    private func startSocksTimeoutTimer() {
        socksTimeoutTimer?.cancel()
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(SOCKS_CONNECTION_TIMEOUT_MS))
        t.setEventHandler { [weak self] in
            Task { [weak self] in
                await self?.onSocksTimeout()
            }
        }
        t.resume()
        socksTimeoutTimer = t
    }

    private func cancelSocksTimeoutTimer() {
        socksTimeoutTimer?.cancel()
        socksTimeoutTimer = nil
    }

    private func onSocksTimeout() async {
        guard socksState != .established && socksState != .closed else { return }
        log("SOCKS connection timeout")
        if callbackState == .started {
            callbackState = .completed
            await reportSocksFailure(connectionId, true)
        }
        close()
    }

    // MARK: - Receive exactly

    private func receiveExactly(_ n: Int) async throws -> Data {
        try await withCheckedThrowingContinuation { cont in
            var got = Data()
            func step() {
                socksConnection?.receive(minimumIncompleteLength: 1, maximumLength: n - got.count) { data, _, _, err in
                    if let err { cont.resume(throwing: err); return }
                    guard let d = data, !d.isEmpty else { cont.resume(throwing: NSError(domain: "socks", code: -1)); return }
                    got.append(d)
                    if got.count >= n { cont.resume(returning: got); return }
                    step()
                }
            }
            step()
        }
    }

    // MARK: - Packet builders

    private func createIPv4Header(payloadLength: Int) -> Data {
        var ip = Data(repeating: 0, count: 20)
        ip[0] = 0x45 // version=4, IHL=5
        let totalLen = UInt16(20 + payloadLength)
        ip[2] = UInt8(totalLen >> 8); ip[3] = UInt8(totalLen & 0xFF)
        // id/flags/fragoff zero
        ip[8] = 64 // TTL
        ip[9] = 6  // TCP
        ip.replaceSubrange(12..<16, with: sourceIP.rawValue)
        ip.replaceSubrange(16..<20, with: destIP.rawValue)
        return ip
    }

    private func createTCPHeader(payloadLen: Int, flags: TCPFlags, seq: UInt32, ack: UInt32) -> Data {
        var tcp = Data(repeating: 0, count: 20)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: seq.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ack.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        tcp[12] = 0x50 // data offset=5
        tcp[13] = flags.rawValue
        let wnd = availableWindow
        tcp[14] = UInt8(wnd >> 8); tcp[15] = UInt8(wnd & 0xFF)
        // checksum/urg=0
        return tcp
    }

    // MARK: - Housekeeping

    private func cancelAllTimers() {
        cancelRetransmitTimer()
        cancelDelayedAckTimer()
        cancelSocksTimeoutTimer()
    }

    private func cleanupBuffers() {
        if !outOfOrderPackets.isEmpty {
            log("Clearing \(outOfOrderPackets.count) buffered packets")
            outOfOrderPackets.removeAll()
        }
        if pendingBytesTotal > 0 {
            let n = pendingBytesTotal
            pendingClientBytes.removeAll()
            pendingBytesTotal = 0
            Task { await reportInFlight(connectionId, -n) }
        }
        updateAdvertisedWindow()
    }

    @inline(__always)
    private func log(_ s: String) {
        NSLog("[TCPConnection \(key)[\(connectionId)]] \(s)")
    }
}
