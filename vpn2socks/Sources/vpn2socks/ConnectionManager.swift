//  ConnectionManager.swift
//  vpn2socks
//
//  Optimized + actor-safety fixes
//

import Foundation
import NetworkExtension
import Network

final actor ConnectionManager {

    // MARK: - Properties

    private let packetFlow: SendablePacketFlow
    private let fakeDNSServer: IPv4Address
    private let dnsInterceptor = DNSInterceptor.shared

    // 连接管理
    private var tcpConnections: [String: TCPConnection] = [:]
    private var pendingSyns: Set<String> = []

    // 统计
    private struct Stats {
        var totalConnections: Int = 0
        var activeConnections: Int = 0
        var duplicateSyns: Int = 0
        var failedConnections: Int = 0
        var bytesReceived: Int = 0
        var bytesSent: Int = 0
        var startTime: Date = Date()
    }
    private var stats = Stats()

    // 定时器
    private var statsTimer: Task<Void, Never>?
    private let statsInterval: TimeInterval = 30.0

    // 限制
    private let maxConnections = 100
    private let connectionTimeout: TimeInterval = 60.0

    // 止血
    private var shedding = false
    private var pausedReads = false
    private var dropNewConnections = false
    private var logSampleN = 1
    private let rssHighMB: UInt64 = 40
    private let rssLowMB:  UInt64 = 34
    private let maxConnsDuringShedding = 60
    private var lastTrimTime = Date.distantPast
    private let trimCooldown: TimeInterval = 3.0

    // “墓碑”表：关闭后的尾包吞掉
    private var recentlyClosed: [String: Date] = [:]
    private let tombstoneTTL: TimeInterval = 3.0

    // UDP/ICMP 限流
    private var lastICMPReply: [String: Date] = [:]
    private let icmpReplyInterval: TimeInterval = 1.0
    private let cleanupInterval: TimeInterval = 60.0

    // 采样计数器
    private let logCounterQueue = DispatchQueue(label: "connmgr.log.counter.q")
    private var logCounter: UInt64 = 0

    // MARK: - Init

    init(packetFlow: SendablePacketFlow, fakeDNSServer: String) {
        self.packetFlow = packetFlow
        self.fakeDNSServer = IPv4Address(fakeDNSServer)!
    }

    deinit {
        statsTimer?.cancel()
    }

    // MARK: - Public

    nonisolated func start() {
        Task { [weak self] in
            guard let self = self else { return }
            await self.startInternal()
        }
    }

    // MARK: - Start

    private func startInternal() async {
        startStatsTimer()
        startCleanupTask()
        startCleaner()
        await readPackets()
    }

    private func startStatsTimer() {
        statsTimer?.cancel()
        statsTimer = Task { [weak self] in
            while !Task.isCancelled {
                let interval: Int
                if let shedding = await self?.shedding, shedding { interval = 60 } else { interval = 30 }
                try? await Task.sleep(nanoseconds: UInt64(interval) * 1_000_000_000)
                guard let self = self else { break }
                await self.printStats()
            }
        }
    }

    private func startCleanupTask() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 60 * 1_000_000_000)
                guard let self = self else { break }
                await self.cleanupStaleConnections()
            }
        }
    }

    private func printStats() {
        let uptime = Date().timeIntervalSince(stats.startTime)
        let uptimeStr = formatUptime(uptime)
        let msg = """

        === ConnectionManager Statistics ===
        Uptime: \(uptimeStr)
        Connections:
          - Total: \(stats.totalConnections)
          - Active: \(tcpConnections.count)
          - Failed: \(stats.failedConnections)
          - Duplicate SYNs: \(stats.duplicateSyns)
        Traffic:
          - Received: \(formatBytes(stats.bytesReceived))
          - Sent: \(formatBytes(stats.bytesSent))
        Memory:
          - Pending SYNs: \(pendingSyns.count)
          - TCP Connections: \(tcpConnections.count)
        ====================================
        """
        NSLog(msg)
    }

    private func formatUptime(_ seconds: TimeInterval) -> String {
        let h = Int(seconds) / 3600
        let m = (Int(seconds) % 3600) / 60
        let s = Int(seconds) % 60
        return String(format: "%02d:%02d:%02d", h, m, s)
    }

    private func formatBytes(_ bytes: Int) -> String {
        if bytes < 1024 { return "\(bytes) B" }
        if bytes < 1024 * 1024 { return String(format: "%.2f KB", Double(bytes)/1024) }
        if bytes < 1024 * 1024 * 1024 { return String(format: "%.2f MB", Double(bytes)/(1024*1024)) }
        return String(format: "%.2f GB", Double(bytes)/(1024*1024*1024))
    }

    private func cleanupStaleConnections() {
        // 预留：若添加 lastActivity，可在此清理超时连接
    }

    // MARK: - Packet Reading

    private func readPackets() async {
        while true {
            if pausedReads {
                try? await Task.sleep(nanoseconds: 50_000_000)
                continue
            }
            let (datas, _) = await packetFlow.readPackets()
            let batch = datas.prefix(64)
            for pkt in batch {
                autoreleasepool {
                    if let ip = IPv4Packet(data: pkt) {
                        switch ip.`protocol` {
                        case 6: Task { await self.handleTCPPacket(ip) }
                        case 17: Task { await self.handleUDPPacket(ip) }
                        default: break
                        }
                    }
                }
            }
        }
    }

    // MARK: - Shedding

    private func handleRSS(_ rssMB: UInt64) {
        if !shedding && rssMB >= rssHighMB {
            shedding = true
            pausedReads = true
            dropNewConnections = true
            logSampleN = 8
            NSLog("[ConnectionManager] RSS=\(rssMB)MB ≥ \(rssHighMB)MB，进入止血")
            maybeTrimConnections(targetMax: maxConnsDuringShedding)
            return
        }
        if shedding && rssMB > rssLowMB {
            maybeTrimConnections(targetMax: maxConnsDuringShedding)
            return
        }
        if shedding && rssMB <= rssLowMB {
            shedding = false
            pausedReads = false
            dropNewConnections = false
            logSampleN = 1
            NSLog("[ConnectionManager] RSS=\(rssMB)MB ≤ \(rssLowMB)MB，退出止血")
        }
    }

    private func maybeTrimConnections(targetMax: Int) {
        let now = Date()
        guard now.timeIntervalSince(lastTrimTime) >= trimCooldown else { return }
        lastTrimTime = now
        let current = tcpConnections.count
        guard current > targetMax else { return }
        let needClose = current - targetMax
        let victims = tcpConnections.keys.sorted().prefix(needClose)
        var closed = 0
        for key in victims {
            if let conn = tcpConnections[key] {
                Task { await conn.close() }
                if tcpConnections.removeValue(forKey: key) != nil {
                    closed += 1
                    recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
                }
            }
        }
        stats.activeConnections = tcpConnections.count
        NSLog("[ConnectionManager] Shedding: 关闭 \(closed) 条连接（剩余 \(tcpConnections.count)）")
    }

    // MARK: - UDP/DNS

    private func handleUDPPacket(_ ipPacket: IPv4Packet) async {
        guard let udp = UDPDatagram(data: ipPacket.payload) else { return }
        if ipPacket.destinationAddress == self.fakeDNSServer && udp.destinationPort == 53 {
            await handleDNSQuery(ipPacket: ipPacket, udp: udp)
        } else {
            logUDPPacket(ipPacket: ipPacket, udp: udp)
        }
    }

    private func handleDNSQuery(ipPacket: IPv4Packet, udp: UDPDatagram) async {
        let qtype = extractQType(from: udp.payload) ?? 0
        guard let result = await dnsInterceptor.handleQueryAndCreateResponse(for: udp.payload) else { return }

        let resp = makeIPv4UDPReply(
            srcIP: self.fakeDNSServer,
            dstIP: ipPacket.sourceAddress,
            srcPort: 53,
            dstPort: udp.sourcePort,
            payload: result.response
        )
        packetFlow.writePackets([resp], withProtocols: [AF_INET as NSNumber])
        stats.bytesSent += resp.count
        NSLog("[ConnectionManager] DNS Reply to \(ipPacket.sourceAddress):\(udp.sourcePort) qtype=\(qtypeName(qtype)) len=\(result.response.count)")
    }

    // MARK: - TCP

    private func handleTCPPacket(_ ipPacket: IPv4Packet) async {
        guard let tcpSegment = TCPSegment(data: ipPacket.payload) else { return }

        let key = makeConnectionKey(
            srcIP: ipPacket.sourceAddress,
            srcPort: tcpSegment.sourcePort,
            dstIP: ipPacket.destinationAddress,
            dstPort: tcpSegment.destinationPort
        )

        if tcpSegment.isSYN && !tcpSegment.isACK {
            await handleSYN(ipPacket: ipPacket, tcpSegment: tcpSegment, key: key)
            return
        }

        if tcpSegment.isRST {
            await handleConnectionClose(key: key)
            return
        }

        if tcpSegment.isFIN {
            if let connection = tcpConnections[key] {
                await connection.onInboundFin(seq: tcpSegment.sequenceNumber)
            } else {
                handleOrphanPacket(key: key, tcpSegment: tcpSegment)
            }
            return
        }

        if let connection = tcpConnections[key] {
            await handleEstablishedConnection(connection: connection, tcpSegment: tcpSegment)
        } else {
            handleOrphanPacket(key: key, tcpSegment: tcpSegment)
        }
    }

    private func handleSYN(ipPacket: IPv4Packet, tcpSegment: TCPSegment, key: String) async {
        if pendingSyns.contains(key) {
            stats.duplicateSyns += 1
            NSLog("[ConnectionManager] Ignoring duplicate SYN for \(key)")
            return
        }

        if let existing = tcpConnections[key] {
            stats.duplicateSyns += 1
            await existing.retransmitSynAckDueToDuplicateSyn()
            NSLog("[ConnectionManager] Existing connection, retransmitting SYN-ACK for \(key)")
            return
        }

        if dropNewConnections {
            stats.failedConnections += 1
            if (stats.failedConnections % logSampleN) == 0 {
                NSLog("[ConnectionManager] Shedding: 拒绝新连接 \(key)")
            }
            return
        }

        if tcpConnections.count >= maxConnections {
            NSLog("[ConnectionManager] Connection limit reached (\(maxConnections)), rejecting \(key)")
            return
        }

        pendingSyns.insert(key)

        let tcpBytes = ipPacket.payload
        guard tcpBytes.count >= 20 else { pendingSyns.remove(key); return }
        let dataOffsetInWords = tcpBytes[12] >> 4
        let tcpHeaderLen = max(20, Int(dataOffsetInWords) * 4)
        guard tcpHeaderLen <= tcpBytes.count else { pendingSyns.remove(key); return }
        let tcpSlice = tcpBytes.prefix(tcpHeaderLen)

        var domainForSocks: String? = nil
        let dstIP = ipPacket.destinationAddress
        if await dnsInterceptor.contains(dstIP) {
            domainForSocks = await lookupDomainWithBackoff(fakeIP: dstIP)
            if domainForSocks != nil {
                await dnsInterceptor.retain(fakeIP: dstIP)
                NSLog("[ConnectionManager] Fake IP \(dstIP) mapped to domain: \(domainForSocks!)")
            } else {
                NSLog("[ConnectionManager] Warning: No domain mapping for fake IP \(dstIP)")
            }
        }

        let newConn = TCPConnection(
            key: key,
            packetFlow: packetFlow,
            sourceIP: ipPacket.sourceAddress,
            sourcePort: tcpSegment.sourcePort,
            destIP: dstIP,
            destPort: tcpSegment.destinationPort,
            destinationHost: domainForSocks,
            initialSequenceNumber: tcpSegment.sequenceNumber
        )

        tcpConnections[key] = newConn
        await newConn.setOnBytesBackToTunnel { [weak self] n in
            guard let self = self else { return }
            let mss = max(536, newConn.tunnelMTU - 40)
            let segs = (n + mss - 1) / mss
            let estimatedBytes = n + segs * 40
            Task { await self.bumpSentBytes(estimatedBytes) }
        }
        pendingSyns.remove(key)
        stats.totalConnections += 1
        stats.activeConnections = tcpConnections.count

        await newConn.acceptClientSyn(tcpHeaderAndOptions: Data(tcpSlice))

        // 用 Task 启动 SOCKS，并在完成后通过 actor 方法进行清理与“墓碑”写入
        Task { [weak self] in
            guard let self = self else { return }
            await newConn.start()
            await self.finishAndCleanup(key: key, dstIP: dstIP)
        }
    }
    
    private func addSentBytes(_ n: Int) {
        stats.bytesSent += n
    }
    
    func bumpSentBytes(_ n: Int) async {
        guard n > 0 else { return }
        stats.bytesSent &+= n
    }

    private func handleConnectionClose(key: String) async {
        guard let connection = tcpConnections[key] else { return }
        NSLog("[ConnectionManager] Closing connection: \(key)")
        await connection.close()
        // 结束时由 finishAndCleanup 统一清理
    }

    private func handleEstablishedConnection(
        connection: TCPConnection,
        tcpSegment: TCPSegment
    ) async {
        if tcpSegment.payload.isEmpty && tcpSegment.isACK {
            await connection.onInboundAck(ackNumber: tcpSegment.acknowledgementNumber)
        } else if !tcpSegment.payload.isEmpty {
            await connection.handlePacket(
                payload: tcpSegment.payload,
                sequenceNumber: tcpSegment.sequenceNumber
            )
            stats.bytesReceived += tcpSegment.payload.count
        }
    }

    /// 只在安全场景静默吞掉，不干预数据通路
    private func handleOrphanPacket(key: String, tcpSegment: TCPSegment) {
        if pendingSyns.contains(key),
           tcpSegment.payload.isEmpty,
           tcpSegment.isACK {
            return
        }
        if let expires = recentlyClosed[key], Date() < expires {
            return
        }
        if !tcpSegment.payload.isEmpty || !tcpSegment.isACK {
            NSLog("[ConnectionManager] Orphan packet for \(key), flags: SYN=\(tcpSegment.isSYN) ACK=\(tcpSegment.isACK) FIN=\(tcpSegment.isFIN) RST=\(tcpSegment.isRST)")
        }
    }

    // MARK: - Actor helpers (修复 actor 隔离写入)

    private func finishAndCleanup(key: String, dstIP: IPv4Address) async {
        NSLog("[ConnectionManager] Connection \(key) completed")

        if await dnsInterceptor.contains(dstIP) {
            await dnsInterceptor.release(fakeIP: dstIP)
        }
        // 写入墓碑并移除连接（都在 actor 隔离上下文内）
        markRecentlyClosed(key)
        await removeConnection(key: key)
    }

    private func markRecentlyClosed(_ key: String) {
        recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
    }

    private func removeConnection(key: String) async {
        if tcpConnections.removeValue(forKey: key) != nil {
            stats.activeConnections = tcpConnections.count
            NSLog("[ConnectionManager] Removed connection: \(key) (active: \(tcpConnections.count))")
        }
    }

    // MARK: - Helpers

    private func makeConnectionKey(
        srcIP: IPv4Address,
        srcPort: UInt16,
        dstIP: IPv4Address,
        dstPort: UInt16
    ) -> String {
        return "\(srcIP):\(srcPort)->\(dstIP):\(dstPort)"
    }

    private func lookupDomainWithBackoff(fakeIP: IPv4Address) async -> String? {
        if let d = await dnsInterceptor.getDomain(forFakeIP: fakeIP) { return d }
        for attempt in 1...4 {
            try? await Task.sleep(nanoseconds: 50_000_000)
            if let d = await dnsInterceptor.getDomain(forFakeIP: fakeIP) {
                NSLog("[ConnectionManager] Domain found after \(attempt) retries")
                return d
            }
        }
        return nil
    }

    // MARK: - DNS utils

    private func qtypeName(_ qtype: UInt16) -> String {
        switch qtype {
        case 1: return "A"
        case 28: return "AAAA"
        case 5: return "CNAME"
        case 15: return "MX"
        case 16: return "TXT"
        case 33: return "SRV"
        default: return "TYPE\(qtype)"
        }
    }

    private func extractQType(from dnsQuery: Data) -> UInt16? {
        guard dnsQuery.count >= 12 else { return nil }
        var idx = 12
        while idx < dnsQuery.count {
            let len = Int(dnsQuery[idx])
            if len == 0 { idx += 1; break }
            if (len & 0xC0) == 0xC0 { idx += 2; break }
            idx += 1 + len
            if idx > dnsQuery.count { return nil }
        }
        guard idx + 2 <= dnsQuery.count else { return nil }
        return (UInt16(dnsQuery[idx]) << 8) | UInt16(dnsQuery[idx + 1])
    }

    // MARK: - Packet builders

    private func makeIPv4UDPReply(
        srcIP: IPv4Address,
        dstIP: IPv4Address,
        srcPort: UInt16,
        dstPort: UInt16,
        payload: Data
    ) -> Data {
        var ip = Data(count: 20)
        ip[0] = 0x45
        ip[1] = 0x00
        let totalLen = 20 + 8 + payload.count
        ip[2] = UInt8(totalLen >> 8); ip[3] = UInt8(totalLen & 0xff)
        ip[4] = 0x00; ip[5] = 0x00
        ip[6] = 0x40; ip[7] = 0x00
        ip[8] = 64
        ip[9] = 17
        ip[10] = 0x00; ip[11] = 0x00

        let src = [UInt8](srcIP.rawValue)
        let dst = [UInt8](dstIP.rawValue)
        ip[12] = src[0]; ip[13] = src[1]; ip[14] = src[2]; ip[15] = src[3]
        ip[16] = dst[0]; ip[17] = dst[1]; ip[18] = dst[2]; ip[19] = dst[3]

        var udp = Data(count: 8)
        udp[0] = UInt8(srcPort >> 8); udp[1] = UInt8(srcPort & 0xff)
        udp[2] = UInt8(dstPort >> 8); udp[3] = UInt8(dstPort & 0xff)
        let udpLen = 8 + payload.count
        udp[4] = UInt8(udpLen >> 8); udp[5] = UInt8(udpLen & 0xff)
        udp[6] = 0x00; udp[7] = 0x00

        let udpCsum = calculateChecksum(
            forUdp: udp,
            sourceAddress: srcIP,
            destinationAddress: dstIP,
            payload: payload
        )
        udp[6] = UInt8(udpCsum >> 8)
        udp[7] = UInt8(udpCsum & 0xff)

        var ipCopy = ip
        let ipCsum = ipv4HeaderChecksum(&ipCopy)
        ipCopy[10] = UInt8(ipCsum >> 8)
        ipCopy[11] = UInt8(ipCsum & 0xff)

        return ipCopy + udp + payload
    }

    private func ipv4HeaderChecksum(_ header: inout Data) -> UInt16 {
        header[10] = 0; header[11] = 0
        var sum: UInt32 = 0
        for i in stride(from: 0, to: header.count, by: 2) {
            let word = (UInt32(header[i]) << 8) | UInt32(header[i+1])
            sum &+= word
        }
        while (sum >> 16) != 0 {
            sum = (sum & 0xFFFF) &+ (sum >> 16)
        }
        return ~UInt16(truncatingIfNeeded: sum)
    }

    private func calculateChecksum(
        forUdp header: Data,
        sourceAddress: IPv4Address,
        destinationAddress: IPv4Address,
        payload: Data
    ) -> UInt16 {
        var pseudo = Data()
        pseudo.append(sourceAddress.rawValue)
        pseudo.append(destinationAddress.rawValue)
        pseudo.append(0x00)
        pseudo.append(17)
        let totalLen = header.count + payload.count
        pseudo.append(UInt8(totalLen >> 8))
        pseudo.append(UInt8(totalLen & 0xFF))

        var toSum = pseudo + header + payload
        if toSum.count % 2 != 0 { toSum.append(0) }

        var sum: UInt32 = 0
        for i in stride(from: 0, to: toSum.count, by: 2) {
            let word = (UInt16(toSum[i]) << 8) | UInt16(toSum[i+1])
            sum &+= UInt32(word)
        }
        while (sum >> 16) != 0 {
            sum = (sum & 0xFFFF) &+ (sum >> 16)
        }
        let res = ~UInt16(sum & 0xFFFF)
        return res == 0 ? 0xFFFF : res
    }

    private func logUDPPacket(ipPacket: IPv4Packet, udp: UDPDatagram) {
        let dstPort = udp.destinationPort

        // 对 UDP/443、UDP/80 立即回 ICMP Port Unreachable（不做节流）
        if dstPort == 443 || dstPort == 80 {
            let icmp = ICMPPacket.unreachable(for: ipPacket)
            packetFlow.writePackets([icmp.data], withProtocols: [NSNumber(value: AF_INET)])
            NSLog("[ConnectionManager] QUIC/UDP->\(ipPacket.destinationAddress):\(dstPort) len=\(udp.payload.count); ICMP Port Unreachable sent immediately")
            return
        }

        // 其它 UDP：保持原有限流 ICMP 回复
        let flowKey = "\(ipPacket.sourceAddress):\(udp.sourcePort)->\(ipPacket.destinationAddress):\(dstPort)"
        let now = Date()
        if let last = lastICMPReply[flowKey], now.timeIntervalSince(last) < icmpReplyInterval {
            return
        }
        lastICMPReply[flowKey] = now

        let icmp = ICMPPacket.unreachable(for: ipPacket)
        packetFlow.writePackets([icmp.data], withProtocols: [NSNumber(value: AF_INET)])
        NSLog("[ConnectionManager] ICMP Unreachable sent for UDP \(flowKey)")
    }

    private func startCleaner() {
        Task.detached { [weak self] in
            while let strongSelf = self {
                try? await Task.sleep(nanoseconds: UInt64(strongSelf.cleanupInterval * 1_000_000_000))
                await strongSelf.cleanExpiredICMPReplies()
            }
        }
    }

    private func cleanExpiredICMPReplies() {
        let now = Date()
        lastICMPReply = lastICMPReply.filter { now.timeIntervalSince($0.value) < cleanupInterval }
        NSLog("[ConnectionManager] Cleaned up expired ICMP entries, left=\(lastICMPReply.count)")
    }
    
    
}

extension ConnectionManager {
    nonisolated func prepareForStop() {
        // 可以在这里做 flush stats、tombstone 清理、防止 RST 风暴等
        NSLog("[ConnectionManager] prepareForStop called")
    }
}
