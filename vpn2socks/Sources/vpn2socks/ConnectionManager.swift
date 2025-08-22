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
    private let pushPorts: Set<UInt16> = [5223, 5228, 5229, 5230, 443]
    
    func isPushConnection(_ c: TCPConnection) async -> Bool {  // 添加 async
        let port = c.destPort                      // was: await c.destPort
        if pushPorts.contains(port) { return true }

        if port == 443 {
            let host = (c.destinationHost)?.lowercased() ?? ""   // was: (await c.destinationHost)?
            if host == "mtalk.google.com"
                || host.hasSuffix(".push.apple.com")
                || host == "api.push.apple.com" {
                return true
            }
        }
        return false
    }
    
    private func protectPushService() async {
        
        
        for (_, conn) in tcpConnections {
            let port = conn.destPort
            if port == 5223 || port == 5228 {
                
                // 确保Push连接有足够缓冲区
                let currentBuffer = await conn.recvBufferLimit
                if currentBuffer < 32 * 1024 {
                    await conn.adjustBufferSize(48 * 1024)
                    logger.info("[Push] Enhanced buffer for push service")
                }
            }
        }
    }
    
    private var processedPackets = Set<String>()
    private let packetCacheTTL: TimeInterval = 1.0
    // 添加连接优先级枚举定义
    private enum ConnectionPriority: Int, Comparable {
        case low = 0
        case normal = 1
        case high = 2
        case critical = 3
        
        static func < (lhs: ConnectionPriority, rhs: ConnectionPriority) -> Bool {
            return lhs.rawValue < rhs.rawValue
        }
    }
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
    private let maxConnections = 60  // 从 100 降低
    private let connectionTimeout: TimeInterval = 45.0  // 从 60 降低
    private let maxIdleTime: TimeInterval = 20.0  // 积极的空闲超时
    
    // 内存管理阈值（MB）
    private let memoryNormalMB: UInt64 = 20     // 从25降低
    private let memoryWarningMB: UInt64 = 35  // 从35MB降到30MB
    private let memoryCriticalMB: UInt64 = 45   // 从45降低
    private let memoryEmergencyMB: UInt64 = 55
    
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
    private var keepCritical: Bool = true
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
        if !keepCritical {
            // 完全清理模式 - 关闭所有连接
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
            autoreleasepool { }
            
            logger.critical("[Memory] Emergency cleanup complete")
            
            // Cooldowns
            Task { [weak self] in
                try? await Task.sleep(nanoseconds: 1_500_000_000)
                await self?.maybeUnpauseReadsAfterCooldown()
            }
            
            Task { [weak self] in
                try? await Task.sleep(nanoseconds: 5_000_000_000)
                await self?.maybeLiftIntakeBanAfterCooldown()
            }
            return
        }
        
        // 保护模式：保留关键连接
        var toClose: [String] = []
        
        for (key, conn) in tcpConnections {
            // 检查是否是 Push 连接
            if await isPushConnection(conn) {
                logger.info("[Emergency] Keeping Push connection: \(key)")
                continue
            }
            
            // 检查是否是其他重要服务
            let port = conn.destPort
            if port == 443 {
                if let host = conn.destinationHost,
                   host.lowercased().contains("apple.com") || host.lowercased().contains("icloud.com") {
                    logger.info("[Emergency] Keeping Apple service: \(key)")
                    continue
                }
            }
            
            toClose.append(key)
        }
        
        // 关闭非关键连接
        for key in toClose {
            if let conn = tcpConnections[key] {
                await conn.close()
                tcpConnections.removeValue(forKey: key)
                recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
            }
        }
        
        logger.critical("[Emergency] Kept \(self.tcpConnections.count) critical connections, closed \(toClose.count)")

    }

    // MARK: - Start

    private func startInternal() async {
        logger.info("[ConnectionManager] Starting all subsystems...")
        
        // 基础定时器
        startStatsTimer()           // 统计输出
        startMemoryMonitor()         // 内存监控
        
        // 清理任务
        startCleanupTask()          // 定期清理
        startCleaner()              // ICMP 清理
        
        // 优化任务
        startAdaptiveBufferTask()   // 自适应缓冲
        startHealthMonitor()        // 健康监控
        startConnectionMonitor()    // 连接监控
        startConnectionPoolOptimizer() // 连接池优化
        
        logger.info("[ConnectionManager] All subsystems started")
        
        // 开始处理数据包
        await readPackets()
    }
    
    // 新增：健康监控任务
    private func startHealthMonitor() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 20_000_000_000) // 每20秒
                guard let self = self else { break }
                await self.monitorConnectionPool()
            }
        }
    }

    // 新增：连接监控任务
    private func startConnectionMonitor() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 10_000_000_000) // 每10秒
                guard let self = self else { break }
                await self.checkCriticalConnections()
            }
        }
    }

    // 新增：检查关键连接
    private func checkCriticalConnections() async {
        
        var hasAnyPush = false
        var missingPorts = Set<UInt16>()
        let requiredPorts: Set<UInt16> = [5223, 5228, 5229, 5230]
        var activePorts = Set<UInt16>()
        
        // 检查现有 Push 连接
        for (_, conn) in tcpConnections {
            if await isPushConnection(conn) {  // 添加 await
                hasAnyPush = true
                let port = conn.destPort  // 修改为 destPort
                activePorts.insert(port)
            }
        }
        
        // 计算缺失的端口
        missingPorts = requiredPorts.subtracting(activePorts)
        
        if !hasAnyPush {
            logger.critical("[Monitor] ⚠️ NO Push connections active!")
            // 触发告警或重连逻辑
            await triggerPushReconnection()
        } else if !missingPorts.isEmpty {
            logger.warning("[Monitor] Missing Push ports: \(missingPorts)")
        } else {
            logger.info("[Monitor] ✅ Push services healthy")
        }
        
        // 确保 Push 连接有足够资源
        await protectPushService()
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
    
    private func triggerPushReconnection() async {
        // 这里可以：
        // 1. 通知应用层重新初始化 Push
        // 2. 或者主动触发系统的网络重连
        // 3. 记录事件供分析
        
        logger.critical("[Push] Triggering reconnection attempt")
        lastPushReconnectTime = Date()
        
        // 通知应用层（如果有回调机制）
        // notificationHandler?.pushConnectionLost()
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
            /// 严重：保留核心连接
            await trimConnections(targetMax: 10)
            dropNewConnections = true
        } else if memoryMB >= memoryWarningMB {
            logger.warning("⚠️ WARNING: Memory \(memoryMB)MB >= \(self.memoryWarningMB)MB")
            // 警告：温和限制
            await trimConnections(targetMax: maxConnsDuringShedding)
            // 不立即禁止新连接，而是限流
            if tcpConnections.count >= maxConnsDuringShedding {
                dropNewConnections = true
            }
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
        logger.info("[Stats] Starting statistics timer (interval: 30s)")
        
        statsTimer = Task { [weak self] in
            while !Task.isCancelled {
                guard let self = self else { break }
                try? await Task.sleep(nanoseconds: 30_000_000_000)
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
    
    public var lastActivityTime = Date()
    
    private func cleanupStaleConnections() async {
        let now = Date()
        var staleKeys: [String] = []
        
        for (key, conn) in tcpConnections {
            // 检查是否是 Push 连接
            if await isPushConnection(conn) {
                // Push 连接给予更长的空闲时间
                if let lastActivity = await conn.getLastActivityTime() {
                    if now.timeIntervalSince(lastActivity) > 300 { // 5分钟
                        staleKeys.append(key)
                    }
                }
            } else {
                // 普通连接正常清理
                if let lastActivity = await conn.getLastActivityTime() {
                    if now.timeIntervalSince(lastActivity) > 60 {
                        staleKeys.append(key)
                    }
                }
            }
        }
        
        // 清理前再次确认保护 Push 服务
        await protectPushService()
        
        // 执行清理
        for key in staleKeys {
            if let conn = tcpConnections[key] {
                await conn.close()
                tcpConnections.removeValue(forKey: key)
                recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
            }
        }
    }
    
    // 2. 添加 checkIfPushServiceNeeded 方法
    private func checkIfPushServiceNeeded(port: UInt16) async -> Bool {
        // 检查是否有其他活跃的 Push 连接
        for (_, conn) in tcpConnections {
            let connPort = conn.destPort
            if connPort == port {
                // 已有同端口的连接，不需要重连
                return false
            }
        }
        // 没有找到相同端口的连接，需要重连
        return true
    }
    
    private func cleanupLowPriorityConnections() async {
        var lowPriorityConnections: [(String, ConnectionPriority, Date?)] = []
        
        // 收集所有低优先级连接
        for (key, conn) in tcpConnections {
            let port = conn.destPort
            let priority = getConnectionPriority(destPort: port)
            
            // 跳过关键服务
            if port == 5223 || port == 5228 {
                continue
            }
            
            // 只收集低优先级连接
            if priority <= .normal {
                let lastActivity = await conn.getLastActivityTime()
                lowPriorityConnections.append((key, priority, lastActivity))
            }
        }
        
        // 按优先级和活动时间排序
        lowPriorityConnections.sort { (a, b) in
            // 先按优先级排序（低优先级在前）
            if a.1 != b.1 {
                return a.1 < b.1
            }
            // 相同优先级按最后活动时间排序（久未使用的在前）
            let aTime = a.2 ?? Date.distantPast
            let bTime = b.2 ?? Date.distantPast
            return aTime < bTime
        }
        
        // 确定要关闭的连接数量
        let targetCloseCount = min(
            lowPriorityConnections.count,
            max(1, tcpConnections.count / 3)  // 最多关闭1/3的连接
        )
        
        // 关闭选中的连接
        var closedCount = 0
        for i in 0..<targetCloseCount {
            let (key, priority, lastActivity) = lowPriorityConnections[i]
            if let conn = tcpConnections[key] {
                let idleTime = lastActivity.map { Date().timeIntervalSince($0) } ?? 0
                logger.info("[Cleanup] Closing low priority connection \(key) (priority: \(priority.rawValue), idle: \(Int(idleTime))s)")
                
                await conn.close()
                tcpConnections.removeValue(forKey: key)
                recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
                closedCount += 1
            }
        }
        
        if closedCount > 0 {
            stats.activeConnections = tcpConnections.count
            logger.info("[Cleanup] Closed \(closedCount) low priority connections")
        }
    }
    
    private func monitorConnectionPool() async {
        _ = tcpConnections.count
        _ = getCurrentMemoryUsageMB()
        
        // 检查连接池健康度
        let healthScore = calculateHealthScore()
        
        if healthScore < 0.5 {
            logger.warning("[Health] Poor health: \(healthScore)")
            
            // 分级响应
            if healthScore < 0.3 {
                // 紧急：清理所有非关键连接
                await emergencyCleanup(keepCritical: true)
            } else if healthScore < 0.4 {
                // 严重：清理低优先级和空闲连接
                await cleanupLowPriorityConnections()
            } else {
                // 警告：只清理空闲超过30秒的
                await cleanupIdleConnections(maxIdle: 30)
            }
        }
    }
    
    private func emergencyCleanup(keepCritical: Bool) async {
        for (key, conn) in tcpConnections {
            let port = conn.destPort
            
            // 保留Push服务
            if keepCritical && (port == 5223 || port == 5228) {
                continue
            }
            
            // 保留Apple服务
            if keepCritical && port == 443 {
                if let host = conn.destinationHost,
                   host.contains("apple.com") {
                    continue
                }
            }
            
            // 关闭其他所有连接
            await conn.close()
            tcpConnections.removeValue(forKey: key)
            recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
        }
        
        logger.critical("[Emergency] Kept only critical connections")
    }
    
    private func handleCriticalConnectionLoss(key: String, port: UInt16) async {
        guard let conn = tcpConnections[key] else { return }
        let isPush = await self.isPushConnection(conn)
        guard isPush else { return }

        logger.info("[Reconnect] Scheduling reconnection for push service on port \(port)")
        lastPushReconnectTime = Date()
        Task {
            try? await Task.sleep(nanoseconds: 3_000_000_000)
            if await self.checkIfPushServiceNeeded(port: port) {
                logger.info("[Reconnect] Attempting to restore push connection")
                // TODO: 触发重建（按你的项目实际接入点）
            }
        }
    }

    
    private func calculateHealthScore() -> Double {
        let connectionUtilization = Double(tcpConnections.count) / Double(maxConnections)
        let failureRate = Double(stats.failedConnections) / max(1.0, Double(stats.totalConnections))
        let memoryPressure = Double(getCurrentMemoryUsageMB()) / Double(memoryWarningMB)
        
        // 计算健康分数（0-1，越高越健康）
        let score = 1.0 - (connectionUtilization * 0.3 + failureRate * 0.4 + memoryPressure * 0.3)
        return max(0.0, min(1.0, score))
    }
    
    private func makeRoomForHighPriorityConnection(priority: ConnectionPriority) async -> Bool {
        // 收集所有可关闭的连接（不包括关键连接）
            var victims: [(String, ConnectionPriority, Date?)] = []
            
            for (key, conn) in tcpConnections {
                let port = conn.destPort
                let connPriority = getConnectionPriority(destPort: port)
                
                // 永远不关闭 Push 服务
                if port == 5223 || port == 5228 {
                    continue
                }
                
                // 只收集比请求优先级低的连接
                if connPriority < priority {
                    let lastActivity = await conn.getLastActivityTime()
                    victims.append((key, connPriority, lastActivity))
                }
            }
            
            // 如果没有可关闭的连接，尝试关闭同优先级但空闲的
            if victims.isEmpty && priority == .high {
                for (key, conn) in tcpConnections {
                    let port = conn.destPort
                    let connPriority = getConnectionPriority(destPort: port)
                    
                    if connPriority == .high {
                        if let lastActivity = await conn.getLastActivityTime(),
                           Date().timeIntervalSince(lastActivity) > 30 {
                            victims.append((key, connPriority, lastActivity))
                        }
                    }
                }
            }
            
            guard !victims.isEmpty else {
                logger.error("[Priority] No connections available to close")
                return false
            }
            
            // 按活动时间排序，关闭最久未使用的
            victims.sort { (a, b) in
                let aTime = a.2 ?? Date.distantPast
                let bTime = b.2 ?? Date.distantPast
                return aTime < bTime
            }
            
            // 关闭第一个
            let victim = victims[0]
            if let conn = tcpConnections[victim.0] {
                logger.info("[Priority] Closing \(victim.0) (priority: \(victim.1.rawValue)) for high priority connection")
                await conn.close()
                tcpConnections.removeValue(forKey: victim.0)
                recentlyClosed[victim.0] = Date().addingTimeInterval(tombstoneTTL)
                stats.activeConnections = tcpConnections.count
                return true
            }
            
            return false
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
        
        // 更新最近拒绝计数
        updateRecentRejections()
        
        // 计算拒绝率
        let rejectionRate = stats.totalConnections > 0
            ? (Double(stats.failedConnections) / Double(stats.totalConnections) * 100.0)
            : 0.0
        
        logger.info("""
            Performance Metrics:
              - Connection Success Rate: \(String(format: "%.1f%%", 100.0 - rejectionRate))
              - Avg Connection Lifetime: \(self.calculateAvgLifetime())
              - Peak Connections: \(self.peakConnections)
              - Rejection Count (last min): \(self.recentRejections)
        """)
        
        // 如果拒绝率过高，自动调整
        if rejectionRate > 20 {
            logger.warning("High rejection rate detected, consider increasing connection limit")
        }
        
        // 修复质量统计部分
        let avgQuality = self.connectionQualities.values  // 添加 self.
                .map { $0.score }
                .reduce(0, +) / Double(max(1, self.connectionQualities.count))  // 添加 self.
            
        // 计算高流量连接数
        var highTrafficCount = 0
        for (_, conn) in tcpConnections {
            if await conn.isHighTraffic {
                highTrafficCount += 1
            }
        }
    
        
        logger.info("""
            Connection Quality:
              - Average Score: \(String(format: "%.2f", avgQuality))
              - Buffer Overflows: \(self.connectionQualities.values.map { $0.overflowCount }.reduce(0, +))  // 添加 self.
              - High Traffic Conns: \(highTrafficCount)
            """)
            
        var pushTotal = 0
        var pushBufferSum = 0
        var byPort: [UInt16:Int] = [5223:0, 5228:0, 5229:0, 5230:0, 443:0]

        for (_, conn) in tcpConnections {
            if await self.isPushConnection(conn) {
                pushTotal += 1
                pushBufferSum += await conn.recvBufferLimit
                let p = conn.destPort
                if byPort[p] != nil { byPort[p]! += 1 }
                else if p == 443 { byPort[443]! += 1 } // 443 + SNI 兜底计到 443
            }
        }

        let avgPushBuf = pushTotal > 0 ? pushBufferSum / pushTotal : 0

        logger.info("""
        Push Service Status:
          - Active Connections: \(pushTotal)
          - By Port: 5223=\(byPort[5223]!), 5228=\(byPort[5228]!), 5229=\(byPort[5229]!), 5230=\(byPort[5230]!), 443=\(byPort[443]!)
          - Avg Buffer Size: \(avgPushBuf) bytes
          - Last Reconnect: \(self.timeSinceLastPushReconnect())
        """)
        
        var pushConnections: [String: Int] = [:]
            var pushBufferTotal = 0
            
        for (_, conn) in tcpConnections {
                if await isPushConnection(conn) {
                    let port = conn.destPort
                    let buffer = await conn.recvBufferLimit
                    
                    let service = identifyPushService(port: port, conn: conn)
                    pushConnections[service] = (pushConnections[service] ?? 0) + 1
                    pushBufferTotal += buffer
                }
            }
            
            logger.info("""
            Push Service Health:
              - Apple Push (5223): \(pushConnections["APNs"] ?? 0)
              - FCM (5228): \(pushConnections["FCM"] ?? 0)
              - Other Push: \(pushConnections["Other"] ?? 0)
              - Total Buffer: \(self.formatBytes(pushBufferTotal))
              - Protection Active: \(pushConnections.count > 0 ? "✅" : "❌")
            """)
    }
    
    private func identifyPushService(port: UInt16, conn: TCPConnection) -> String {
        switch port {
        case 5223: return "APNs"
        case 5228: return "FCM"
        case 5229, 5230: return "FCM-Alt"
        case 443:
            // 可以根据 SNI 或 IP 进一步识别
            return "Push-HTTPS"
        default: return "Other"
        }
    }
    
    private var lastPushReconnectTime = Date.distantPast
    // 添加辅助方法
    private func timeSinceLastPushReconnect() -> String {
        let interval = Date().timeIntervalSince(lastPushReconnectTime)
        if interval < 60 {
            return "\(Int(interval))s ago"
        } else if interval < 3600 {
            return "\(Int(interval / 60))m ago"
        } else if interval < 86400 {
            return "\(Int(interval / 3600))h ago"
        } else {
            return "Never"
        }
    }
    
    // 定期评估连接质量
    private func evaluateConnectionQuality() async {
        for (key, conn) in tcpConnections {
            var quality = connectionQualities[key] ?? ConnectionQuality(key: key)
            
            // 更新质量指标
            quality.overflowCount = await conn.getOverflowCount()
            quality.lastUpdate = Date()
            
            connectionQualities[key] = quality
            
            // 如果质量太差，考虑关闭并重建
            if quality.score < 0.3 {
                logger.warning("[Quality] Poor connection quality for \(key): \(quality.score)")
                // 可以触发重连逻辑
            }
        }
    }
    
    private var connectionQualities: [String: ConnectionQuality] = [:]
    
    private struct ConnectionQuality {
        let key: String
        var rtt: TimeInterval = 0
        var packetLoss: Double = 0
        var throughput: Double = 0
        var overflowCount: Int = 0
        var lastUpdate: Date = Date()
        
        var score: Double {
            // 计算质量分数（0-1）
            let rttScore = max(0, 1 - (rtt / 1.0)) // RTT < 1秒为好
            let lossScore = max(0, 1 - packetLoss)
            let overflowScore = max(0, 1 - Double(overflowCount) / 10.0)
            return (rttScore + lossScore + overflowScore) / 3.0
        }
    }
    
    private var peakConnections: Int = 0
    private var recentRejections: Int = 0
    private var lastRejectionsReset = Date()
    private var connectionStartTimes: [String: Date] = [:]
    private var connectionLifetimes: [TimeInterval] = []
    
    private func calculateAvgLifetime() -> String {
        guard !connectionLifetimes.isEmpty else { return "N/A" }
        let avg = connectionLifetimes.reduce(0, +) / Double(connectionLifetimes.count)
        return String(format: "%.1fs", avg)
    }

    private func updateRecentRejections() {
        let now = Date()
       // 每分钟重置一次
       if now.timeIntervalSince(lastRejectionsReset) >= 60 {
           recentRejections = 0
           lastRejectionsReset = now
       }
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
                autoreleasepool {
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
        
        // 先保护 Push 服务
        await protectPushService()
        
        // 收集可关闭的连接（排除 Push）
        var victims: [(String, TCPConnection, Date?)] = []
        
        for (key, conn) in tcpConnections {
            // 跳过 Push 连接
            if await isPushConnection(conn) {
                continue
            }
            
            let lastActivity = await conn.getLastActivityTime()
            victims.append((key, conn, lastActivity))
        }
        
        // 按活动时间排序
        victims.sort { (a, b) in
            let aTime = a.2 ?? Date.distantPast
            let bTime = b.2 ?? Date.distantPast
            return aTime < bTime
        }
        
        // 关闭需要的数量
        let needClose = current - targetMax
        for i in 0..<min(needClose, victims.count) {
            let (key, conn, _) = victims[i]
            await conn.close()
            tcpConnections.removeValue(forKey: key)
            recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
        }
        
        stats.activeConnections = tcpConnections.count
        logger.info("[Trim] Protected Push connections, closed \(min(needClose, victims.count)) others")

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
        // 创建唯一标识符
        let packetID = "\(ipPacket.sourceAddress):\(tcpSegment.sourcePort)-\(ipPacket.destinationAddress):\(tcpSegment.destinationPort)-\(tcpSegment.sequenceNumber)"
        
        // 检查是否已处理
        if processedPackets.contains(packetID) {
            return // 跳过重复包
        }
        
        // 记录已处理
        processedPackets.insert(packetID)
        
        // 定时清理缓存
        Task {
            try? await Task.sleep(nanoseconds: UInt64(packetCacheTTL * 1_000_000_000))
            processedPackets.remove(packetID)
        }
        
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
    
    private func getConnectionPriority(destPort: UInt16) -> ConnectionPriority {
        // 不再需要 conn 参数，直接根据端口判断基础优先级
        switch destPort {
        case 5223, 5228, 5229, 5230: return .critical  // Push 服务
        case 443: return .high      // HTTPS
        case 80: return .normal     // HTTP
        default: return .low
        }
    }
    
    private func closeLowPriorityConnection() async {
        // 查找低优先级连接
        var lowestPriorityKey: String?
        var lowestPriority = ConnectionPriority.critical
        
        for (key, conn) in tcpConnections {
            let port = conn.destPort
            let priority = getConnectionPriority(destPort: port)
            
            if priority < lowestPriority {
                lowestPriority = priority
                lowestPriorityKey = key
            }
        }
        
        // 关闭找到的低优先级连接
        if let key = lowestPriorityKey, let conn = tcpConnections[key] {
            logger.info("[Priority] Closing low priority connection \(key) to make room")
            await conn.close()
            tcpConnections.removeValue(forKey: key)
            recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
            stats.activeConnections = tcpConnections.count
        }
    }

    private func handleSYN(ipPacket: IPv4Packet, tcpSegment: TCPSegment, key: String) async {
        
        let priority = getConnectionPriority(destPort: tcpSegment.destinationPort)
                
        // 内存压力下的优先级判断
        if isMemoryPressure {
            if priority < .high {
                stats.failedConnections += 1
                recentRejections += 1
                updateRecentRejections()
                logger.info("[Priority] Rejecting low priority connection under memory pressure")
                return
            }
        }
        
        // 连接数达到限制时的智能处理
        if tcpConnections.count >= maxConnections {
            // 高优先级连接尝试腾出空间
            if priority >= .high {
                let madeRoom = await makeRoomForHighPriorityConnection(priority: priority)
                if !madeRoom {
                    stats.failedConnections += 1
                    recentRejections += 1
                    logger.warning("[Priority] Cannot make room for high priority connection")
                    return
                }
            } else {
                // 低优先级直接拒绝
                stats.failedConnections += 1
                recentRejections += 1
                logger.info("[Priority] Rejecting low priority connection, limit reached")
                return
            }
        }
        
        // 内存压力下拒绝新连接
        if dropNewConnections {
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
        
        // 创建连接时记录开始时间
        connectionStartTimes[key] = Date()
        
        // FIX: Removed the second, invalid redeclaration of 'priority'
        let bufferSize = calculateBufferSizeForPriority(priority)
        
        // 更新峰值连接数
        if tcpConnections.count > peakConnections {
            peakConnections = tcpConnections.count
        }

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
            recvBufferLimit: bufferSize // 使用计算后的缓冲区大小
        )

        tcpConnections[key] = newConn
        stats.totalConnections += 1
        stats.activeConnections = tcpConnections.count

        // FIX: The ambiguity is resolved by removing the duplicate variables above.
        await newConn.acceptClientSyn(tcpHeaderAndOptions: Data(tcpSlice))

        // 记录日志（调试用）
        if stats.totalConnections % 10 == 0 {
             // The original `finalBufferSize` is no longer defined, updated log message
            logger.debug("[Adaptive] Buffer size: \(bufferSize) bytes for \(self.tcpConnections.count)/\(self.maxConnections) connections")
        }
    
        // 启动连接
        Task { [weak self] in
            guard let self = self else { return }
            await newConn.start()
            await self.finishAndCleanup(key: key, dstIP: dstIP)
        }
    }
    
    // 新增：根据优先级计算缓冲区大小
    private func calculateBufferSizeForPriority(_ priority: ConnectionPriority) -> Int {
        switch priority {
        case .critical:
            return 64 * 1024  // 48KB for critical connections
        case .high:
            return 48 * 1024  // 32KB for high priority
        case .normal:
            return 32 * 1024  // 16KB for normal
        case .low:
            return 16 * 1024   // 8KB for low priority
        }
    }

    private func handleConnectionClose(key: String) async {
        guard let connection = tcpConnections[key] else { return }
        
        let port = connection.destPort
        let priority = getConnectionPriority(destPort: port)
        
        logger.debug("[Connection] Closing: \(key) (priority: \(priority.rawValue))")
        
        await connection.close()
        tcpConnections.removeValue(forKey: key)
        recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
        stats.activeConnections = tcpConnections.count
        
        // 对于关键连接，触发重连机制
        if priority == .critical {
            await handleCriticalConnectionLoss(key: key, port: port)
        }
    }
    
    private func startConnectionPoolOptimizer() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 30_000_000_000) // 每30秒
                guard let self = self else { break }
                await self.optimizeConnectionPool()
            }
        }
    }
    
    private func optimizeConnectionPool() async {
        let memoryMB = getCurrentMemoryUsageMB()
        let activeCount = tcpConnections.count
        let healthScore = calculateHealthScore()
        
        logger.info("""
        [Optimizer] Pool status:
        - Connections: \(activeCount)/\(self.maxConnections)  // 添加 self
        - Memory: \(memoryMB)MB
        - Health Score: \(String(format: "%.2f", healthScore))
        """)
        
        // 根据健康分数调整策略
        if healthScore < 0.3 {
            // 健康度很差，激进清理
            logger.warning("[Optimizer] Poor health, aggressive cleanup triggered")
            await trimConnections(targetMax: self.maxConnections / 2)  // 添加 self
        } else if healthScore < 0.5 {
            // 健康度一般，温和清理
            logger.info("[Optimizer] Moderate health, gentle cleanup triggered")
            await cleanupIdleConnections(maxIdle: 30)
        } else if healthScore > 0.8 && activeCount < self.maxConnections / 2 {  // 添加 self
            // 健康度良好且连接数较少，可以增加缓冲区
            await increaseBuffersForActiveConnections()
        }
    }

    // 新增：清理空闲连接
    private func cleanupIdleConnections(maxIdle: TimeInterval) async {
        let now = Date()
        var idleConnections: [(String, TimeInterval)] = []
        
        for (key, conn) in tcpConnections {
            if let lastActivity = await conn.getLastActivityTime() {
                let idleTime = now.timeIntervalSince(lastActivity)
                if idleTime > maxIdle {
                    idleConnections.append((key, idleTime))
                }
            }
        }
        
        // 按空闲时间排序，优先关闭最久未使用的
        idleConnections.sort { $0.1 > $1.1 }
        
        // 关闭前 25% 最空闲的连接
        let toClose = max(1, idleConnections.count / 4)
        for i in 0..<min(toClose, idleConnections.count) {
            let (key, idleTime) = idleConnections[i]
            if let conn = tcpConnections[key] {
                logger.info("[Cleanup] Closing idle connection \(key) (idle: \(Int(idleTime))s)")
                await conn.close()
                tcpConnections.removeValue(forKey: key)
                recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
            }
        }
        
        stats.activeConnections = tcpConnections.count
    }

    // 新增：为活跃连接增加缓冲区
    private func increaseBuffersForActiveConnections() async {
        for (_, conn) in tcpConnections {
            let currentBuffer = await conn.recvBufferLimit
            if currentBuffer < 32 * 1024 {
                await conn.adjustBufferSize(currentBuffer + 8 * 1024)
            }
        }
        logger.info("[Optimizer] Increased buffers for active connections")
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
           
       // 计算连接生命周期
       if let startTime = connectionStartTimes[key] {
           let lifetime = Date().timeIntervalSince(startTime)
           connectionLifetimes.append(lifetime)
           
           // 保持最近100个连接的生命周期记录
           if connectionLifetimes.count > 100 {
               connectionLifetimes.removeFirst()
           }
           
           connectionStartTimes.removeValue(forKey: key)
       }
       
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
                try? await Task.sleep(nanoseconds: UInt64(strongSelf.cleanupInterval * 1_000_000_000))
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

