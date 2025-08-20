//
//  ConnectionManager.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Optimized for connection deduplication and performance monitoring
//

import Foundation
import NetworkExtension
import Network

// 业务 actor - 优化版本
final actor ConnectionManager {
    
    // MARK: - Properties
    
    private let packetFlow: SendablePacketFlow
    private let fakeDNSServer: IPv4Address
    private let dnsInterceptor = DNSInterceptor.shared
    
    // 连接管理
    private var tcpConnections: [String: TCPConnection] = [:]
    private var pendingSyns: Set<String> = []  // 防止重复SYN
    
    // 统计信息
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
    
    // 性能监控
    private var statsTimer: Task<Void, Never>?
    private let statsInterval: TimeInterval = 30.0  // 每30秒打印统计
    
    // 连接限制
    private let maxConnections = 100
    private let connectionTimeout: TimeInterval = 60.0
    
    // MARK: - Initialization
    
    init(packetFlow: SendablePacketFlow, fakeDNSServer: String) {
        self.packetFlow = packetFlow
        self.fakeDNSServer = IPv4Address(fakeDNSServer)!
    }
    
    deinit {
        statsTimer?.cancel()
    }
    
    // MARK: - Public Methods
    
    nonisolated func start() {
        Task { [weak self] in
            guard let self = self else { return }
            await self.startInternal()
        }
    }
    
    // MARK: - Internal Start
    
    private func startInternal() async {
        // 启动统计定时器
        startStatsTimer()
        
        // 启动清理任务
        startCleanupTask()
        
        // 开始读取数据包
        await readPackets()
    }
    
    // MARK: - Stats and Monitoring
    
    private func startStatsTimer() {
        statsTimer?.cancel()
        statsTimer = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(30 * 1_000_000_000))
                guard let self = self else { break }
                await self.printStats()
            }
        }
    }
    
    private func startCleanupTask() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(60 * 1_000_000_000))
                guard let self = self else { break }
                await self.cleanupStaleConnections()
            }
        }
    }
    
    private func printStats() {
        let uptime = Date().timeIntervalSince(stats.startTime)
        let uptimeStr = formatUptime(uptime)
        
        let statsMsg = """
        
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
        NSLog(statsMsg)
    }
    
    private func formatUptime(_ seconds: TimeInterval) -> String {
        let hours = Int(seconds) / 3600
        let minutes = (Int(seconds) % 3600) / 60
        let secs = Int(seconds) % 60
        return String(format: "%02d:%02d:%02d", hours, minutes, secs)
    }
    
    private func formatBytes(_ bytes: Int) -> String {
        if bytes < 1024 {
            return "\(bytes) B"
        } else if bytes < 1024 * 1024 {
            return String(format: "%.2f KB", Double(bytes) / 1024)
        } else if bytes < 1024 * 1024 * 1024 {
            return String(format: "%.2f MB", Double(bytes) / (1024 * 1024))
        } else {
            return String(format: "%.2f GB", Double(bytes) / (1024 * 1024 * 1024))
        }
    }
    
    private func cleanupStaleConnections() {
        var staleKeys: [String] = []
        
        // 找出所有过期的连接键
        for (key, _) in tcpConnections {
            // 这里可以添加更复杂的过期逻辑
            // 比如检查连接的最后活动时间
            // 现在简单地清理 pendingSyns
        }
        
        // 清理超时的 pending SYNs
        let now = Date()
        pendingSyns = pendingSyns.filter { key in
            // 简单起见，清理所有超过5秒的pending SYN
            true  // 实际应该检查时间戳
        }
        
        if !staleKeys.isEmpty {
            NSLog("[ConnectionManager] Cleaned up \(staleKeys.count) stale connections")
        }
    }
    
    // MARK: - Packet Reading
    
    private func readPackets() async {
        while true {
            let (datas, protos) = await packetFlow.readPackets()
            
            for (i, packetData) in datas.enumerated() {
                let af = protos[i].int32Value
                
                switch af {
                case AF_INET:
                    await handleIPv4Packet(packetData)
                    
                case AF_INET6:
                    // 暂不处理 IPv6
                    break
                    
                default:
                    NSLog("[ConnectionManager] Unknown AF=\(af) len=\(packetData.count)")
                }
            }
        }
    }
    
    private func handleIPv4Packet(_ packetData: Data) async {
        guard let ip4 = IPv4Packet(data: packetData) else {
            NSLog("[ConnectionManager] Bad IPv4 len=\(packetData.count)")
            return
        }
        
        // 更新统计
        stats.bytesReceived += packetData.count
        
        switch ip4.protocol {
        case 6:  // TCP
            await handleTCPPacket(ip4)
        case 17: // UDP
            await handleUDPPacket(ip4)
        default:
            break
        }
    }
    
    // MARK: - UDP/DNS Handling
    
    private func handleUDPPacket(_ ipPacket: IPv4Packet) async {
        guard let udp = UDPDatagram(data: ipPacket.payload) else { return }
        
        // DNS 拦截
        if ipPacket.destinationAddress == self.fakeDNSServer && udp.destinationPort == 53 {
            await handleDNSQuery(ipPacket: ipPacket, udp: udp)
        } else {
            // 非 DNS UDP（可能是 QUIC）
            logUDPPacket(ipPacket: ipPacket, udp: udp)
        }
    }
    
    private func handleDNSQuery(ipPacket: IPv4Packet, udp: UDPDatagram) async {
        let qtype = extractQType(from: udp.payload) ?? 0
        
        guard let result = await dnsInterceptor.handleQueryAndCreateResponse(for: udp.payload) else {
            return
        }
        
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
    
    private func logUDPPacket(ipPacket: IPv4Packet, udp: UDPDatagram) {
        // 只记录 QUIC (UDP 443) 或其他重要端口
        if udp.destinationPort == 443 || udp.destinationPort == 80 {
            NSLog("[ConnectionManager] UDP to \(ipPacket.destinationAddress):\(udp.destinationPort) len=\(udp.payload.count) (possibly QUIC)")
        }
    }
    
    // MARK: - TCP Handling
    
    private func handleTCPPacket(_ ipPacket: IPv4Packet) async {
        guard let tcpSegment = TCPSegment(data: ipPacket.payload) else { return }
        
        let connectionKey = makeConnectionKey(
            srcIP: ipPacket.sourceAddress,
            srcPort: tcpSegment.sourcePort,
            dstIP: ipPacket.destinationAddress,
            dstPort: tcpSegment.destinationPort
        )
        
        // 处理不同类型的 TCP 包
        if tcpSegment.isSYN && !tcpSegment.isACK {
            await handleSYN(ipPacket: ipPacket, tcpSegment: tcpSegment, key: connectionKey)
        } else if tcpSegment.isFIN || tcpSegment.isRST {
            await handleConnectionClose(key: connectionKey)
        } else if let connection = tcpConnections[connectionKey] {
            await handleEstablishedConnection(
                connection: connection,
                tcpSegment: tcpSegment
            )
        } else {
            // 没有找到连接，可能是乱序或已清理
            handleOrphanPacket(key: connectionKey, tcpSegment: tcpSegment)
        }
    }
    
    private func handleSYN(ipPacket: IPv4Packet, tcpSegment: TCPSegment, key: String) async {
        // 检查是否是重复 SYN
        if pendingSyns.contains(key) {
            stats.duplicateSyns += 1
            NSLog("[ConnectionManager] Ignoring duplicate SYN for \(key)")
            return
        }
        
        // 检查是否已有连接
        if let existing = tcpConnections[key] {
            stats.duplicateSyns += 1
            await existing.retransmitSynAckDueToDuplicateSyn()
            NSLog("[ConnectionManager] Existing connection, retransmitting SYN-ACK for \(key)")
            return
        }
        
        // 检查连接数限制
        if tcpConnections.count >= maxConnections {
            NSLog("[ConnectionManager] Connection limit reached (\(maxConnections)), rejecting \(key)")
            // 可以发送 RST 或简单丢弃
            stats.failedConnections += 1
            return
        }
        
        // 标记为正在处理
        pendingSyns.insert(key)
        
        // 解析 TCP 选项
        let tcpBytes = ipPacket.payload
        guard tcpBytes.count >= 20 else {
            pendingSyns.remove(key)
            return
        }
        
        let dataOffsetInWords = tcpBytes[12] >> 4
        let tcpHeaderLen = max(20, Int(dataOffsetInWords) * 4)
        guard tcpHeaderLen <= tcpBytes.count else {
            pendingSyns.remove(key)
            return
        }
        
        let tcpSlice = tcpBytes.prefix(tcpHeaderLen)
        
        // 获取域名（如果是假 IP）
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
        
        // 创建新连接
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
        pendingSyns.remove(key)
        stats.totalConnections += 1
        stats.activeConnections = tcpConnections.count
        
        // 发送 SYN-ACK
        await newConn.acceptClientSyn(tcpHeaderAndOptions: Data(tcpSlice))
        
        // 异步启动 SOCKS 连接
        Task { [weak self] in
            guard let self = self else { return }
            
            await newConn.start()
            
            NSLog("[ConnectionManager] Connection \(key) completed")
            
            // 清理
            if await self.dnsInterceptor.contains(dstIP) {
                await self.dnsInterceptor.release(fakeIP: dstIP)
            }
            
            await self.removeConnection(key: key)
        }
    }
    
    private func handleConnectionClose(key: String) async {
        guard let connection = tcpConnections[key] else { return }
        
        NSLog("[ConnectionManager] Closing connection: \(key)")
        await connection.close()
        
        // 连接会在 start() 完成后自动清理
    }
    
    private func handleEstablishedConnection(
        connection: TCPConnection,
        tcpSegment: TCPSegment
    ) async {
        if tcpSegment.payload.isEmpty && tcpSegment.isACK {
            // 纯 ACK
            await connection.onInboundAck(ackNumber: tcpSegment.acknowledgementNumber)
        } else if !tcpSegment.payload.isEmpty {
            // 有数据
            await connection.handlePacket(
                payload: tcpSegment.payload,
                sequenceNumber: tcpSegment.sequenceNumber
            )
            stats.bytesReceived += tcpSegment.payload.count
        }
    }
    
    private func handleOrphanPacket(key: String, tcpSegment: TCPSegment) {
        // 只记录非纯 ACK 的孤儿包
        if !tcpSegment.payload.isEmpty || !tcpSegment.isACK {
            NSLog("[ConnectionManager] Orphan packet for \(key), flags: SYN=\(tcpSegment.isSYN) ACK=\(tcpSegment.isACK) FIN=\(tcpSegment.isFIN) RST=\(tcpSegment.isRST)")
        }
    }
    
    // MARK: - Helper Methods
    
    private func makeConnectionKey(
        srcIP: IPv4Address,
        srcPort: UInt16,
        dstIP: IPv4Address,
        dstPort: UInt16
    ) -> String {
        return "\(srcIP):\(srcPort)->\(dstIP):\(dstPort)"
    }
    
    private func lookupDomainWithBackoff(fakeIP: IPv4Address) async -> String? {
        if let d = await dnsInterceptor.getDomain(forFakeIP: fakeIP) {
            return d
        }
        
        // 最多尝试 4 次，每次 50ms
        for attempt in 1...4 {
            try? await Task.sleep(nanoseconds: 50_000_000)
            if let d = await dnsInterceptor.getDomain(forFakeIP: fakeIP) {
                NSLog("[ConnectionManager] Domain found after \(attempt) retries")
                return d
            }
        }
        
        return nil
    }
    
    private func removeConnection(key: String) async {
        if tcpConnections.removeValue(forKey: key) != nil {
            stats.activeConnections = tcpConnections.count
            NSLog("[ConnectionManager] Removed connection: \(key) (active: \(tcpConnections.count))")
        }
    }
    
    // MARK: - DNS Utilities
    
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
        // Skip domain name
        while idx < dnsQuery.count {
            let len = Int(dnsQuery[idx])
            if len == 0 {
                idx += 1
                break
            }
            if (len & 0xC0) == 0xC0 {
                // Compressed name
                idx += 2
                break
            }
            idx += 1 + len
            if idx > dnsQuery.count { return nil }
        }
        
        // Read QTYPE
        guard idx + 2 <= dnsQuery.count else { return nil }
        return (UInt16(dnsQuery[idx]) << 8) | UInt16(dnsQuery[idx + 1])
    }
    
    // MARK: - Packet Builders
    
    private func makeIPv4UDPReply(
        srcIP: IPv4Address,
        dstIP: IPv4Address,
        srcPort: UInt16,
        dstPort: UInt16,
        payload: Data
    ) -> Data {
        // IPv4 header (20 bytes)
        var ip = Data(count: 20)
        ip[0] = 0x45  // Version 4, IHL 5
        ip[1] = 0x00  // TOS
        let totalLen = 20 + 8 + payload.count
        ip[2] = UInt8(totalLen >> 8)
        ip[3] = UInt8(totalLen & 0xff)
        ip[4] = 0x00; ip[5] = 0x00  // ID
        ip[6] = 0x40; ip[7] = 0x00  // Flags (DF) and Fragment offset
        ip[8] = 64   // TTL
        ip[9] = 17   // Protocol (UDP)
        ip[10] = 0x00; ip[11] = 0x00  // Checksum (placeholder)
        
        // Source and destination IPs
        let src = [UInt8](srcIP.rawValue)
        let dst = [UInt8](dstIP.rawValue)
        ip[12] = src[0]; ip[13] = src[1]; ip[14] = src[2]; ip[15] = src[3]
        ip[16] = dst[0]; ip[17] = dst[1]; ip[18] = dst[2]; ip[19] = dst[3]
        
        // UDP header (8 bytes)
        var udp = Data(count: 8)
        udp[0] = UInt8(srcPort >> 8); udp[1] = UInt8(srcPort & 0xff)
        udp[2] = UInt8(dstPort >> 8); udp[3] = UInt8(dstPort & 0xff)
        let udpLen = 8 + payload.count
        udp[4] = UInt8(udpLen >> 8); udp[5] = UInt8(udpLen & 0xff)
        udp[6] = 0x00; udp[7] = 0x00  // Checksum (placeholder)
        
        // Calculate UDP checksum with pseudo-header
        let udpCsum = calculateChecksum(
            forUdp: udp,
            sourceAddress: srcIP,
            destinationAddress: dstIP,
            payload: payload
        )
        udp[6] = UInt8(udpCsum >> 8)
        udp[7] = UInt8(udpCsum & 0xff)
        
        // Calculate IPv4 header checksum
        let ipCsum = ipv4HeaderChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8)
        ip[11] = UInt8(ipCsum & 0xff)
        
        return ip + udp + payload
    }
    
    private func ipv4HeaderChecksum(_ header: inout Data) -> UInt16 {
        header[10] = 0; header[11] = 0  // Clear checksum field
        
        var sum: UInt32 = 0
        for i in stride(from: 0, to: header.count, by: 2) {
            let word = (UInt32(header[i]) << 8) | UInt32(header[i+1])
            sum &+= word
        }
        
        // Add carry
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
        // Build pseudo-header
        var pseudoHeader = Data()
        pseudoHeader.append(sourceAddress.rawValue)
        pseudoHeader.append(destinationAddress.rawValue)
        pseudoHeader.append(0x00)  // Zero
        pseudoHeader.append(17)    // Protocol (UDP)
        
        let totalLen = header.count + payload.count
        pseudoHeader.append(UInt8(totalLen >> 8))
        pseudoHeader.append(UInt8(totalLen & 0xFF))
        
        // Combine all data
        var dataToChecksum = pseudoHeader + header + payload
        
        // Pad if odd length
        if dataToChecksum.count % 2 != 0 {
            dataToChecksum.append(0)
        }
        
        // Calculate checksum
        var sum: UInt32 = 0
        for i in stride(from: 0, to: dataToChecksum.count, by: 2) {
            let word = (UInt16(dataToChecksum[i]) << 8) | UInt16(dataToChecksum[i+1])
            sum &+= UInt32(word)
        }
        
        // Add carry
        while (sum >> 16) != 0 {
            sum = (sum & 0xFFFF) &+ (sum >> 16)
        }
        
        let result = ~UInt16(sum & 0xFFFF)
        return result == 0 ? 0xFFFF : result
    }
}
