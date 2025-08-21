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
                let shedding = await self?.shedding ?? false
                let interval: Int = shedding ? 60 : 30
                try? await Task.sleep(nanoseconds: UInt64(interval) * 1_000_000_000)
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
            // 止血暂停读取：小睡一会儿，避免忙等
            if pausedReads {
                try? await Task.sleep(nanoseconds: 50_000_000) // 50ms
                continue
            }

            let (datas, _) = await packetFlow.readPackets()
            let batch = datas.prefix(64)

            for packetData in batch {
                autoreleasepool {
                    if let ipPacket = IPv4Packet(data: packetData) {
                        switch ipPacket.protocol {
                        case 6: Task { await self.handleTCPPacket(ipPacket) }
                        case 17: Task { await self.handleUDPPacket(ipPacket) }
                        default: break
                        }
                    }
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
    
    // ===== 在 ConnectionManager 内新增状态与参数 =====
    private var shedding = false                 // 是否处于止血中
    private var pausedReads = false              // 暂停读取数据包
    private var dropNewConnections = false       // 拒绝新连接（丢弃SYN）
    private var logSampleN = 1                   // 日志采样：1=每条都打
    private let rssHighMB: UInt64 = 40           // 进入止血阈值
    private let rssLowMB:  UInt64 = 34           // 退出止血阈值（滞后）
    private let maxConnsDuringShedding = 60      // 止血期保留的连接上限
    private var lastTrimTime = Date.distantPast  // 上次裁剪时间
    private let trimCooldown: TimeInterval = 3.0 // 裁剪冷却，避免过于频繁

    
    // ===== 完整实现 handleRSS =====
    private func handleRSS(_ rssMB: UInt64) {
        // 进入止血
        if !shedding && rssMB >= rssHighMB {
            shedding = true
            pausedReads = true
            dropNewConnections = true
            logSampleN = 8 // 降低日志量：每8条打1条

            NSLog("[ConnectionManager] RSS=\(rssMB)MB ≥ \(rssHighMB)MB，进入止血：暂停读取、拒绝新连接、降采样日志（1/\(logSampleN)）")

            // 优先做一次裁剪
            maybeTrimConnections(targetMax: maxConnsDuringShedding)

            return
        }

        // 止血维持期（高于低阈值）：轻量动作即可
        if shedding && rssMB > rssLowMB {
            // 每隔一段时间再尝试轻量裁剪，防止反弹
            maybeTrimConnections(targetMax: maxConnsDuringShedding)
            return
        }

        // 退出止血
        if shedding && rssMB <= rssLowMB {
            shedding = false
            pausedReads = false
            dropNewConnections = false
            logSampleN = 1 // 恢复正常日志

            NSLog("[ConnectionManager]  RSS=\(rssMB)MB ≤ \(rssLowMB)MB，退出止血：恢复读取与建连，日志采样恢复")
        }
    }
    
    // ===== 连接裁剪（按字典序近似“最老优先”）=====
    private func maybeTrimConnections(targetMax: Int) {
        let now = Date()
        guard now.timeIntervalSince(lastTrimTime) >= trimCooldown else { return }
        lastTrimTime = now

        let current = tcpConnections.count
        guard current > targetMax else { return }

        let needClose = current - targetMax
        // 没有 lastActivity 时间戳时，用 key 的稳定顺序近似“旧连接”
        let victims = tcpConnections.keys.sorted().prefix(needClose)

        var closed = 0
        for key in victims {
            if let conn = tcpConnections[key] {
                Task { await conn.close() }
                if let _ = tcpConnections.removeValue(forKey: key) {
                    closed += 1
                    recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
                }
                
            }
        }
        stats.activeConnections = tcpConnections.count
        NSLog("[ConnectionManager] Shedding: 关闭 \(closed) 条连接（剩余 \(tcpConnections.count)）以降内存")
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
        } else if tcpSegment.isRST {
            
            await handleConnectionClose(key: connectionKey)
        } else if tcpSegment.isFIN {
            if let connection = tcpConnections[connectionKey] {
                await connection.onInboundFin(seq: tcpSegment.sequenceNumber)
            } else {
                // 如果找不到连接，仍旧不强制关闭，防止误判
                // 可选：记录孤儿 FIN
            }
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
        
        if dropNewConnections {
            stats.failedConnections += 1
            // 这里选择“丢弃”而非主动 RST，以最小开销止血
            if (stats.failedConnections % logSampleN) == 0 {
                NSLog("[ConnectionManager] Shedding: 拒绝新连接 \(key)")
            }
            return
        }
        
        // 检查连接数限制
        if tcpConnections.count >= maxConnections {
            NSLog("[ConnectionManager] Connection limit reached (\(maxConnections)), rejecting \(key)")
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
            recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
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
    
    private var lastICMPReply: [String: Date] = [:]
    private let icmpReplyInterval: TimeInterval = 1.0   // 每个 flow 最少 1 秒回一次

    // 计数器与其保护队列
    private let logCounterQueue = DispatchQueue(label: "connmgr.log.counter.q")
    private var logCounter: UInt64 = 0
    
    private func logUDPPacket(ipPacket: IPv4Packet, udp: UDPDatagram) {
        let dstPort = udp.destinationPort

        // 针对 QUIC/HTTP3 (UDP 443) 或 80，直接丢弃，避免 storm
        if dstPort == 443 || dstPort == 80 {
            // 采样打印，使用串行队列保护的计数器，避免数据竞争
            let n: UInt64 = logCounterQueue.sync {
                logCounter &+= 1
                return logCounter
            }
            if (n % UInt64(max(1, logSampleN))) == 0 {
                NSLog("[ConnectionManager] Dropping QUIC/UDP packet to \(ipPacket.destinationAddress):\(dstPort) len=\(udp.payload.count)")
            }
            return
        }

        
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
    
    private let cleanupInterval: TimeInterval = 60.0   // 每 60 秒清理一次
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
    

    //private var tcpConnections: [String: TCPConnection] = [:]
    private var recentlyClosed: [String: Date] = [:]    // flowKey -> expiresAt（墓碑表）
    private let tombstoneTTL: TimeInterval = 3.0        // 3 秒足够吃掉多数尾包
    
}
