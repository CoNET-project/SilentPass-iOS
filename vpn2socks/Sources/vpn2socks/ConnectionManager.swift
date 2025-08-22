//  ConnectionManager.swift
//  vpn2socks
//
//  Optimized + actor-safety fixes + enhanced memory management
//

import Foundation
import NetworkExtension
import Network
import os.log

private let logger = Logger(subsystem: "com.vpn2socks", category: "ConnectionManager")

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
    private var memoryMonitorTimer: Task<Void, Never>?
    private let statsInterval: TimeInterval = 30.0

    // CRITICAL: 降低限制以适应 iOS 内存约束
    private let maxConnections = 35  // 从 100 降低
    private let connectionTimeout: TimeInterval = 30.0  // 从 60 降低
    private let maxIdleTime: TimeInterval = 10.0  // 积极的空闲超时
    
    // 内存管理阈值（MB）
    private let memoryNormalMB: UInt64 = 25
    private let memoryWarningMB: UInt64 = 30  // 从35MB降到30MB
    private let memoryCriticalMB: UInt64 = 45
    private let memoryEmergencyMB: UInt64 = 48
    
    // 止血模式（你原有的 shedding 逻辑）
    private var shedding = false
    private var pausedReads = false
    private var dropNewConnections = false
    private var logSampleN = 1
    private let maxConnsDuringShedding = 20  // 从 60 降低
    private var lastTrimTime = Date.distantPast
    private let trimCooldown: TimeInterval = 0.5  // 从 3.0 降低

    // "墓碑"表：关闭后的尾包吸掉
    private var recentlyClosed: [String: Date] = [:]
    private let tombstoneTTL: TimeInterval = 2.0  // 从 3.0 降低

    // UDP/ICMP 限流
    private var lastICMPReply: [String: Date] = [:]
    private let icmpReplyInterval: TimeInterval = 1.0
    private let cleanupInterval: TimeInterval = 30.0  // 从 60 降低，更频繁清理

    // 采样计数器
    private let logCounterQueue = DispatchQueue(label: "connmgr.log.counter.q")
    private var logCounter: UInt64 = 0
    
    // 内存压力状态
    private var isMemoryPressure = false
    private var lastMemoryCheckTime = Date()
    private let memoryCheckInterval: TimeInterval = 5.0  // 每5秒检查内存

    // MARK: - Init

    init(packetFlow: SendablePacketFlow, fakeDNSServer: String) {
        self.packetFlow = packetFlow
        self.fakeDNSServer = IPv4Address(fakeDNSServer)!
        logger.info("[ConnectionManager] Initialized with limits: max=\(self.maxConnections) connections")
    }

    deinit {
        statsTimer?.cancel()
        memoryMonitorTimer?.cancel()
    }

    // MARK: - Public Interface

    nonisolated func start() {
        Task { [weak self] in
            guard let self = self else { return }
            await self.startInternal()
        }
    }
    
    // 新增：供 PacketTunnelProvider 调用的内存清理方法
    func performMemoryCleanup(targetCount: Int) async {
        logger.warning("[Memory] Cleanup requested, target: \(targetCount) connections")
        await trimConnections(targetMax: targetCount)
    }
    
    func emergencyCleanup() async {
        logger.critical("[Memory] EMERGENCY cleanup - closing ALL connections")


        // Stop intake & reading immediately
        dropNewConnections = true
        pausedReads = true


        // Close all live conns
        for (key, conn) in tcpConnections {
        await conn.close()
        recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
        }
        tcpConnections.removeAll(keepingCapacity: false)
        pendingSyns.removeAll(keepingCapacity: false)
        stats.activeConnections = 0


        lastICMPReply.removeAll(keepingCapacity: false)
        _ = autoreleasepool { }


        logger.critical("[Memory] Emergency cleanup complete")


        // PHASE1: Cooldowns —
        // a) Always keep reads paused for 1.5s to let ARC/OS settle
        Task { [weak self] in
            try? await Task.sleep(nanoseconds: 1_500_000_000)
            await self?.maybeUnpauseReadsAfterCooldown()
        }


        // b) Block new connections for 5s or until memory < critical
        Task { [weak self] in
            try? await Task.sleep(nanoseconds: 5_000_000_000)
            await self?.maybeLiftIntakeBanAfterCooldown()
        }
    }

    // MARK: - Start

    private func startInternal() async {
        startStatsTimer()
        startMemoryMonitor()
        startCleanupTask()
        startAdaptiveBufferTask()  // 新增
        startCleaner()
        await readPackets()
    }
    
    private func startMemoryMonitor() {
        memoryMonitorTimer?.cancel()
        memoryMonitorTimer = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(self?.memoryCheckInterval ?? 5) * 1_000_000_000)
                guard let self = self else { break }
                await self.checkMemoryPressure()
            }
        }
    }
    
    private func checkMemoryPressure() async {
        let memoryMB = getCurrentMemoryUsageMB()
        let now = Date()
        
        // 避免过于频繁的检查
        guard now.timeIntervalSince(lastMemoryCheckTime) >= 1.0 else { return }
        lastMemoryCheckTime = now
        
        // 根据内存使用情况采取不同措施
        if memoryMB >= memoryEmergencyMB {
            logger.critical("💀 EMERGENCY: Memory \(memoryMB)MB >= \(self.memoryEmergencyMB)MB")
            await emergencyCleanup()
        } else if memoryMB >= memoryCriticalMB {
            logger.critical("⚠️ CRITICAL: Memory \(memoryMB)MB >= \(self.memoryCriticalMB)MB")
            isMemoryPressure = true
            shedding = true
            pausedReads = true
            dropNewConnections = true
            await trimConnections(targetMax: 5)
        } else if memoryMB >= memoryWarningMB {
            logger.warning("⚠️ WARNING: Memory \(memoryMB)MB >= \(self.memoryWarningMB)MB")
            isMemoryPressure = true
            shedding = true
            await trimConnections(targetMax: maxConnsDuringShedding)
        } else if memoryMB < memoryNormalMB && isMemoryPressure {
            logger.info("✅ Memory recovered: \(memoryMB)MB < \(self.memoryNormalMB)MB")
            isMemoryPressure = false
            shedding = false
            pausedReads = false
            dropNewConnections = false
            logSampleN = 1
        }
        
        // 定期记录内存状态
        if memoryMB > 30 || stats.totalConnections % 10 == 0 {
            logger.debug("[Memory] Current: \(memoryMB)MB, Connections: \(self.tcpConnections.count)")
        }
    }
    
    private func getCurrentMemoryUsageMB() -> UInt64 {
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size) / 4
        
        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: 1) {
                task_info(mach_task_self_,
                         task_flavor_t(MACH_TASK_BASIC_INFO),
                         $0,
                         &count)
            }
        }
        
        if result == KERN_SUCCESS {
            return info.resident_size / (1024 * 1024)
        }
        return 0
    }

    private func startStatsTimer() {
        statsTimer?.cancel()
        statsTimer = Task { [weak self] in
            while !Task.isCancelled {
                let interval = await self?.isMemoryPressure == true ? 60.0 : 30.0  // Fixed: Added .0 for TimeInterval
                try? await Task.sleep(nanoseconds: UInt64(interval) * 1_000_000_000)
                guard let self = self else { break }
                await self.printStats()
            }
        }
    }

    private func startCleanupTask() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(self?.cleanupInterval ?? 30) * 1_000_000_000)
                guard let self = self else { break }
                await self.cleanupStaleConnections()
                await self.cleanupTombstones()
            }
        }
    }
    
    private func cleanupStaleConnections() async {
        let now = Date()
        var staleKeys: [String] = []
        
        // Note: TCPConnection needs to implement isIdle method
        // For now, we'll skip idle checking
        // TODO: Add isIdle method to TCPConnection
        
        if !staleKeys.isEmpty {
            logger.info("[Cleanup] Removing \(staleKeys.count) idle connections")
            for key in staleKeys {
                if let conn = tcpConnections[key] {
                    await conn.close()
                    tcpConnections.removeValue(forKey: key)
                    recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
                }
            }
            stats.activeConnections = tcpConnections.count
        }
    }
    
    private func cleanupTombstones() async {
        let now = Date()
        let beforeCount = recentlyClosed.count
        recentlyClosed = recentlyClosed.filter { now.timeIntervalSince($0.value) < tombstoneTTL }
        
        if beforeCount != recentlyClosed.count {
            logger.debug("[Cleanup] Removed \(beforeCount - self.recentlyClosed.count) tombstones")
        }
    }

    private func printStats() async {
        let uptime = Date().timeIntervalSince(stats.startTime)
        let uptimeStr = formatUptime(uptime)
        let memoryMB = getCurrentMemoryUsageMB()
        
        logger.info("""
        
        === ConnectionManager Statistics ===
        Uptime: \(uptimeStr)
        Memory: \(memoryMB)MB (pressure: \(self.isMemoryPressure), shedding: \(self.shedding))
        Connections:
          - Total: \(self.stats.totalConnections)
          - Active: \(self.tcpConnections.count)/\(self.maxConnections)
          - Failed: \(self.stats.failedConnections)
          - Duplicate SYNs: \(self.stats.duplicateSyns)
        Traffic:
          - Received: \(self.formatBytes(self.stats.bytesReceived))
          - Sent: \(self.formatBytes(self.stats.bytesSent))
        Buffers:
          - Pending SYNs: \(self.pendingSyns.count)
          - Tombstones: \(self.recentlyClosed.count)
        ====================================
        """)
        
        // 计算平均缓冲区大小
        var totalBufferSize = 0
        for (_, conn) in tcpConnections {
            totalBufferSize += await conn.recvBufferLimit
        }
        
        let avgBufferSize = tcpConnections.isEmpty ? 0 : totalBufferSize / tcpConnections.count
        
        logger.info("""
            Buffer Management:
              - Avg Buffer Size: \(self.formatBytes(avgBufferSize))
              - Load Factor: \(String(format: "%.1f%%", Double(self.tcpConnections.count) / Double(self.maxConnections) * 100))
              - Memory Pressure: \(self.getMemoryPressureLevel())
        """)
    }

    private func formatUptime(_ seconds: TimeInterval) -> String {
        let h = Int(seconds) / 3600
        let m = (Int(seconds) % 3600) / 60
        let s = Int(seconds) % 60
        return String(format: "%02d:%02d:%02d", h, m, s)
    }

    private func formatBytes(_ bytes: Int) -> String {
        if bytes < 1024 { return "\(bytes)B" }
        if bytes < 1024 * 1024 { return String(format: "%.1fKB", Double(bytes)/1024) }
        if bytes < 1024 * 1024 * 1024 { return String(format: "%.1fMB", Double(bytes)/(1024*1024)) }
        return String(format: "%.1fGB", Double(bytes)/(1024*1024*1024))
    }

    // MARK: - Packet Reading

    private func readPackets() async {
        while true {
            if pausedReads || isMemoryPressure {
                try? await Task.sleep(nanoseconds: isMemoryPressure ? 100_000_000 : 50_000_000)
                continue
            }


            let (datas, _) = await packetFlow.readPackets()


            // PHASE1: tighter batch when pressured (we also set isMemoryPressure externally)
            let batchSize = isMemoryPressure ? 8 : 32 // was 16 : 32
            let batch = datas.prefix(batchSize)


            for pkt in batch {
            _ = autoreleasepool {
                if let ip = IPv4Packet(data: pkt) {
                    switch ip.`protocol` {
                        case 6: Task { await self.handleTCPPacket(ip) }
                        case 17: Task { await self.handleUDPPacket(ip) }
                        case 1: Task { await self.handleICMPPacket(ip) }
                        default: break
                        }
                    }
                }
            }


            // Optional: a short yield helps prevent immediate burst after big reads
            if isMemoryPressure { try? await Task.sleep(nanoseconds: 5_000_000) } // 5ms
        }
    }
    
    private func handleICMPPacket(_ ipPacket: IPv4Packet) async {
        // 目前不对入站 ICMP 做回复，避免形成“ICMP->ICMP”的循环与额外内存消耗。
        // 我们已在 UDP 分支中对 QUIC/HTTP3 及其他 UDP 发送 ICMP Unreachable，见 logUDPPacket()。
        // 如需 Echo Reply，建议等 ICMPPacket 支持带上 identifier/sequence 的构造再启用。
    }

    // MARK: - Connection Trimming (Enhanced)

    private func trimConnections(targetMax: Int) async {
        let current = tcpConnections.count
        guard current > targetMax else { return }
        
        let now = Date()
        guard now.timeIntervalSince(lastTrimTime) >= trimCooldown else { return }
        lastTrimTime = now
        
        let needClose = current - targetMax
        
        // 优先关闭空闲连接
        var victims: [String] = []
        
        // TODO: When TCPConnection has isIdle method, use it here
        // For now, just close oldest connections
        let keys = Array(tcpConnections.keys.sorted().prefix(needClose))
        victims.append(contentsOf: keys)
        
        // 执行关闭
        var closed = 0
        for key in victims {
            if let conn = tcpConnections[key] {
                await conn.close()
                if tcpConnections.removeValue(forKey: key) != nil {
                    closed += 1
                    recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
                }
            }
        }
        
        stats.activeConnections = tcpConnections.count
        logger.warning("[Trim] Closed \(closed) connections (remaining: \(self.tcpConnections.count))")
    }

    // MARK: - UDP/DNS (保持原有逻辑)

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
        
        let _src = String(describing: ipPacket.sourceAddress)
        let _qtype = qtypeName(qtype)
        let _msg = "[DNS] Reply to \(_src):\(udp.sourcePort) qtype=\(_qtype)"
        logger.debug("\(_msg)")
    }

    // MARK: - TCP (增强内存管理)

    private func handleTCPPacket(_ ipPacket: IPv4Packet) async {
        guard let tcpSegment = TCPSegment(data: ipPacket.payload) else { return }

        let key = makeConnectionKey(
            srcIP: ipPacket.sourceAddress,
            srcPort: tcpSegment.sourcePort,
            dstIP: ipPacket.destinationAddress,
            dstPort: tcpSegment.destinationPort
        )
        
        // 检查墓碑（避免处理已关闭连接的包）
        if let expires = recentlyClosed[key], Date() < expires {
            return
        }

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
        // 内存压力下拒绝新连接
        if isMemoryPressure || dropNewConnections {
            stats.failedConnections += 1
            if (stats.failedConnections % logSampleN) == 0 {
                logger.warning("[Memory] Rejecting connection under pressure: \(key)")
            }
            return
        }
        
        // 检查连接限制
        if tcpConnections.count >= maxConnections {
            stats.failedConnections += 1
            logger.warning("[Limit] Connection limit reached (\(self.maxConnections)), rejecting \(key)")
            return
        }

        if pendingSyns.contains(key) {
            stats.duplicateSyns += 1
            return
        }

        if let existing = tcpConnections[key] {
            stats.duplicateSyns += 1
            await existing.retransmitSynAckDueToDuplicateSyn()
            return
        }

        pendingSyns.insert(key)
        defer { pendingSyns.remove(key) }

        let tcpBytes = ipPacket.payload
        guard tcpBytes.count >= 20 else { return }
        let dataOffsetInWords = tcpBytes[12] >> 4
        let tcpHeaderLen = min(60, max(20, Int(dataOffsetInWords) * 4))
        guard tcpHeaderLen <= tcpBytes.count else { return }
        let tcpSlice = tcpBytes.prefix(tcpHeaderLen)

        var domainForSocks: String? = nil
        let dstIP = ipPacket.destinationAddress
        if await dnsInterceptor.contains(dstIP) {
            domainForSocks = await lookupDomainWithBackoff(fakeIP: dstIP)
            if domainForSocks != nil {
                await dnsInterceptor.retain(fakeIP: dstIP)
                
                let _dst = String(describing: dstIP)
                let _host = domainForSocks!   // 这里已在 if domainForSocks != nil 分支内，安全
                let _msg2 = "[DNS] Fake IP \(_dst) mapped to: \(_host)"
                logger.debug("\(_msg2)")
            }
        }
        
        // 计算动态缓冲区大小
           let bufferSize = adaptiveBufferSize()
           let pressureLevel = getMemoryPressureLevel()
           
           // 根据内存压力进一步调整
           let finalBufferSize = pressureLevel >= 2
               ? bufferSize / 2  // 内存压力大时减半
               : bufferSize
        
        

        // 创建连接（使用减小的缓冲区）
        let newConn = TCPConnection(
            key: key,
            packetFlow: packetFlow,
            sourceIP: ipPacket.sourceAddress,
            sourcePort: tcpSegment.sourcePort,
            destIP: dstIP,
            destPort: tcpSegment.destinationPort,
            destinationHost: domainForSocks,
            initialSequenceNumber: tcpSegment.sequenceNumber,
            tunnelMTU: 1400,
            recvBufferLimit: 32 * 1024  // 减小缓冲区
        )

        tcpConnections[key] = newConn
        stats.totalConnections += 1
        stats.activeConnections = tcpConnections.count

        await newConn.acceptClientSyn(tcpHeaderAndOptions: Data(tcpSlice))

        // 记录日志（调试用）
        if stats.totalConnections % 10 == 0 {
            logger.debug("[Adaptive] Buffer size: \(finalBufferSize) bytes for \(self.tcpConnections.count)/\(self.maxConnections) connections")
        }
    
        // 启动连接
        Task { [weak self] in
            guard let self = self else { return }
            await newConn.start()
            await self.finishAndCleanup(key: key, dstIP: dstIP)
        }
    }

    private func handleConnectionClose(key: String) async {
        guard let connection = tcpConnections[key] else { return }
        logger.debug("[Connection] Closing: \(key)")
        await connection.close()
        tcpConnections.removeValue(forKey: key)
        recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
        stats.activeConnections = tcpConnections.count
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
            logger.debug("[Orphan] Packet for \(key), flags: SYN=\(tcpSegment.isSYN) ACK=\(tcpSegment.isACK) FIN=\(tcpSegment.isFIN) RST=\(tcpSegment.isRST)")
        }
    }

    // MARK: - Cleanup

    private func finishAndCleanup(key: String, dstIP: IPv4Address) async {
        logger.debug("[Connection] Completed: \(key)")

        if await dnsInterceptor.contains(dstIP) {
            await dnsInterceptor.release(fakeIP: dstIP)
        }
        
        tcpConnections.removeValue(forKey: key)
        recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
        stats.activeConnections = tcpConnections.count
    }

    func bumpSentBytes(_ n: Int) async {
        guard n > 0 else { return }
        stats.bytesSent &+= n
    }

    // MARK: - Helper Methods (保持原有实现)

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
                logger.debug("[DNS] Domain found after \(attempt) retries")
                return d
            }
        }
        return nil
    }

    // MARK: - DNS/ICMP Utils (保持原有实现)

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

    // MARK: - Packet builders (保持原有实现)

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

        // 对 UDP/443、UDP/80 立即回 ICMP Port Unreachable
        if dstPort == 443 || dstPort == 80 {
            let icmp = ICMPPacket.unreachable(for: ipPacket)
            packetFlow.writePackets([icmp.data], withProtocols: [NSNumber(value: AF_INET)])
            
            let _dst2 = String(describing: ipPacket.destinationAddress)
            let _msg3 = "[UDP] QUIC/HTTP3 -> \(_dst2):\(dstPort), sent ICMP unreachable"
            logger.debug("\(_msg3)")
            
            return
        }

        // 其它 UDP：限流 ICMP 回复
        let flowKey = "\(ipPacket.sourceAddress):\(udp.sourcePort)->\(ipPacket.destinationAddress):\(dstPort)"
        let now = Date()
        if let last = lastICMPReply[flowKey], now.timeIntervalSince(last) < icmpReplyInterval {
            return
        }
        lastICMPReply[flowKey] = now

        let icmp = ICMPPacket.unreachable(for: ipPacket)
        packetFlow.writePackets([icmp.data], withProtocols: [NSNumber(value: AF_INET)])
        logger.debug("[UDP] ICMP Unreachable sent for \(flowKey)")
    }

    private func startCleaner() {
        Task.detached { [weak self] in
            while let strongSelf = self {
                try? await Task.sleep(nanoseconds: UInt64(await strongSelf.cleanupInterval * 1_000_000_000))
                await strongSelf.cleanExpiredICMPReplies()
            }
        }
    }

    private func cleanExpiredICMPReplies() {
        let now = Date()
        let beforeCount = lastICMPReply.count
        lastICMPReply = lastICMPReply.filter { now.timeIntervalSince($0.value) < cleanupInterval }
        
        if beforeCount != lastICMPReply.count {
            logger.debug("[Cleanup] Removed \(beforeCount - self.lastICMPReply.count) ICMP entries")
        }
    }
    
    // New helpers (actor‑isolated)
    private func maybeUnpauseReadsAfterCooldown() {
        // If we are still in emergency/critical, keep paused; otherwise resume reads
        let mem = getCurrentMemoryUsageMB()
        if mem < memoryCriticalMB { // <45MB
            pausedReads = false
        }
    }


    private func maybeLiftIntakeBanAfterCooldown() {
        let mem = getCurrentMemoryUsageMB()
        if mem < memoryCriticalMB { // <45MB
            dropNewConnections = false
        }
    }
    
    // MARK: - Adaptive Buffer Management
    private func adaptiveBufferSize() -> Int {
        let baseSize = 16 * 1024
        let currentConnections = tcpConnections.count
        let loadFactor = Double(currentConnections) / Double(maxConnections)
        
        // 连接越多，每个连接的缓冲区越小
        // loadFactor: 0.0 -> 2.0x, 0.5 -> 1.5x, 1.0 -> 1.0x
        let multiplier = 2.0 - loadFactor
        let adaptedSize = Int(Double(baseSize) * multiplier)
        
        // 限制范围：8KB - 32KB
        return max(8 * 1024, min(32 * 1024, adaptedSize))
    }
    
    // 获取当前内存压力等级
    private func getMemoryPressureLevel() -> Int {
        let memoryMB = getCurrentMemoryUsageMB()
        if memoryMB >= memoryCriticalMB { return 3 }  // 严重
        if memoryMB >= memoryWarningMB { return 2 }   // 警告
        if memoryMB >= memoryNormalMB { return 1 }    // 正常偏高
        return 0  // 正常
    }
    
    // ConnectionManager.swift - 添加定期调整任务
    private func startAdaptiveBufferTask() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 10_000_000_000) // 每10秒
                guard let self = self else { break }
                await self.adjustAllConnectionBuffers()
            }
        }
    }

    private func adjustAllConnectionBuffers() async {
        let newSize = adaptiveBufferSize()
        let pressureLevel = getMemoryPressureLevel()
        
        // 只在内存压力变化时调整
        guard pressureLevel >= 1 else { return }
        
        for (_, conn) in tcpConnections {
            await conn.adjustBufferSize(newSize)
            
            // 如果内存压力大，触发清理
            if pressureLevel >= 2 {
                await conn.handleMemoryPressure(targetBufferSize: newSize / 2)
            }
        }
        
        logger.debug("[Adaptive] Adjusted buffers for \(self.tcpConnections.count) connections to \(newSize) bytes")
    }
    
    
}

// MARK: - Extension for Shutdown

extension ConnectionManager {
    nonisolated func prepareForStop() {
        Task { [weak self] in
            guard let self = self else { return }
            logger.info("[Shutdown] Preparing to stop...")
            
            // 停止定时器
            await self.stopTimers()
            
            // 关闭所有连接
            await self.closeAllConnections()
            
            logger.info("[Shutdown] Cleanup complete")
        }
    }
    
    private func stopTimers() {
        statsTimer?.cancel()
        memoryMonitorTimer?.cancel()
        statsTimer = nil
        memoryMonitorTimer = nil
    }
    
    private func closeAllConnections() async {
        for (_, conn) in tcpConnections {
            await conn.close()
        }
        tcpConnections.removeAll()
        pendingSyns.removeAll()
        recentlyClosed.removeAll()
        lastICMPReply.removeAll()
    }
    
    
}

