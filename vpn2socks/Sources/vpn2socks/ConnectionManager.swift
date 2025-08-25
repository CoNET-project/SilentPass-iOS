//  ConnectionManager.swift
//  vpn2socks
//
//  Optimized + actor-safety fixes + enhanced memory management + APNs bypass + Social Media support
//

import Foundation
import NetworkExtension
import Network
import os.log

private let logger = Logger(subsystem: "com.vpn2socks", category: "ConnectionManager")

final actor ConnectionManager {
    
    // ✅ 新增：APNs IP 段检测
    private let apnsIPRanges: [IPv4Range] = [
        IPv4Range(cidr: "17.0.0.0/8")!,           // 主要 Apple 服务网段
        IPv4Range(cidr: "23.0.0.0/8")!,          // Apple CDN
        IPv4Range(cidr: "143.224.0.0/12")!,      // Apple 服务
        IPv4Range(cidr: "17.248.128.0/18")!,     // Apple Push 专用
        IPv4Range(cidr: "17.252.156.0/22")!      // Apple Push 备用
    ]
    
    private let pushPorts: Set<UInt16> = [5223, 5228, 5229, 5230, 443]
    
    // ✅ 检查 IP 是否属于 APNs 网段
    private func isAPNsIP(_ ip: IPv4Address) -> Bool {
        let ipU32 = ip.u32
        return apnsIPRanges.contains { range in
            range.contains(ipU32)
        }
    }
    
    func isPushConnection(_ c: TCPConnection) async -> Bool {
        let port = c.destPort
        if pushPorts.contains(port) { return true }

        if port == 443 {
            let host = (c.destinationHost)?.lowercased() ?? ""
            if host == "mtalk.google.com"
                || host.hasSuffix(".push.apple.com")
                || host == "api.push.apple.com" {
                return true
            }
        }
        return false
    }
    
    // 新增：检测社交媒体连接
    private func isSocialMediaConnection(_ c: TCPConnection) async -> Bool {
        return await c.isSocialMediaConnection()
    }
    
    // 新增：检测YouTube连接
    private func isYouTubeConnection(_ c: TCPConnection) -> Bool {
        guard let host = c.destinationHost?.lowercased() else { return false }
        return host.contains("youtube.com") ||
               host.contains("ytimg.com") ||
               host.contains("ggpht.com") ||
               host.contains("googlevideo.com") ||
               host.contains("googleusercontent.com")
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
        var apnsBypassedConnections: Int = 0  // ✅ 新增：APNs 绕过统计
        var youtubeConnections: Int = 0  // 新增：YouTube连接统计
        var socialMediaConnections: Int = 0  // 新增：社交媒体连接统计
    }
    private var stats = Stats()

    // 定时器
    private var statsTimer: Task<Void, Never>?
    private var memoryMonitorTimer: Task<Void, Never>?
    private var highFrequencyOptimizer: Task<Void, Never>?  // 新增：高频优化器
    private let statsInterval: TimeInterval = 30.0

    // CRITICAL: 降低限制以适应 iOS 内存约束
    private let maxConnections = 60
    private let connectionTimeout: TimeInterval = 45.0
    private let maxIdleTime: TimeInterval = 60.0
    
    // 内存管理阈值（MB）
    private let memoryNormalMB: UInt64 = 30
    private let memoryWarningMB: UInt64 = 45
    private let memoryCriticalMB: UInt64 = 55
    private let memoryEmergencyMB: UInt64 = 60
    
    // 止血模式
    private var shedding = false
    private var pausedReads = false
    private var dropNewConnections = false
    private var logSampleN = 1
    private let maxConnsDuringShedding = 20
    private var lastTrimTime = Date.distantPast
    private let trimCooldown: TimeInterval = 0.5

    // "墓碑"表：关闭后的尾包吸掉
    private var recentlyClosed: [String: Date] = [:]
    private let tombstoneTTL: TimeInterval = 2.0

    // UDP/ICMP 限流
    private var lastICMPReply: [String: Date] = [:]
    private let icmpReplyInterval: TimeInterval = 1.0
    private let cleanupInterval: TimeInterval = 30.0

    // 采样计数器
    private let logCounterQueue = DispatchQueue(label: "connmgr.log.counter.q")
    private var logCounter: UInt64 = 0
    
    // 内存压力状态
    private var isMemoryPressure = false
    private var lastMemoryCheckTime = Date()
    private let memoryCheckInterval: TimeInterval = 5.0
    private var lastMemoryPressureTime = Date()
    private var keepCritical: Bool = true
    
    // 新增：连接质量监控
    private var connectionQualities: [String: ConnectionQuality] = [:]
    
    private struct ConnectionQuality {
        let key: String
        var rtt: TimeInterval = 0
        var packetLoss: Double = 0
        var throughput: Double = 0
        var overflowCount: Int = 0
        var lastUpdate: Date = Date()
        var bufferAdjustments: Int = 0  // 新增：缓冲区调整次数
        
        var score: Double {
            let rttScore = max(0, 1 - (rtt / 1.0))
            let lossScore = max(0, 1 - packetLoss)
            let overflowScore = max(0, 1 - Double(overflowCount) / 10.0)
            return (rttScore + lossScore + overflowScore) / 3.0
        }
    }
    
    // 新增：社交媒体统计
    private struct SocialMediaStats {
        var scrollEvents: Int = 0
        var videoStarts: Int = 0
        var videoInterrupts: Int = 0
        var averageScrollSpeed: Double = 0
        var bufferAdjustments: Int = 0
    }
    private var socialStats = SocialMediaStats()
    
    // MARK: - Init

    init(packetFlow: SendablePacketFlow, fakeDNSServer: String) {
        self.packetFlow = packetFlow
        self.fakeDNSServer = IPv4Address(fakeDNSServer)!
        logger.info("[ConnectionManager] Initialized with limits: max=\(self.maxConnections) connections + APNs bypass + Social Media support")
    }

    deinit {
        statsTimer?.cancel()
        memoryMonitorTimer?.cancel()
        highFrequencyOptimizer?.cancel()
    }

    // MARK: - Public Interface

    nonisolated func start() {
        Task { [weak self] in
            guard let self = self else { return }
            await self.startInternal()
        }
    }
    
    func performMemoryCleanup(targetCount: Int) async {
        logger.warning("[Memory] Cleanup requested, target: \(targetCount) connections")
        await trimConnections(targetMax: targetCount)
    }

    
    
    func emergencyCleanup() async {
        if !keepCritical {
            logger.critical("[Memory] EMERGENCY cleanup - closing ALL connections")
            
            dropNewConnections = true
            pausedReads = true
            
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
            // 保护Push、YouTube和正在使用的社交媒体连接
            if await isPushConnection(conn) {
                logger.info("[Emergency] Keeping Push connection: \(key)")
                continue
            }
            
            if isYouTubeConnection(conn) {
                let stats = await conn.getBackpressureStats()
                if stats.isActive {
                    logger.info("[Emergency] Keeping active YouTube connection: \(key)")
                    continue
                }
            }
            
            if await isSocialMediaConnection(conn) {
                let stats = await conn.getBackpressureStats()
                if stats.isActive && stats.dataRate > 1000 {  // 活跃且有流量
                    logger.info("[Emergency] Keeping active social media connection: \(key)")
                    continue
                }
            }
            
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
        
        startStatsTimer()
        startMemoryMonitor()
        startHighFrequencyOptimizer()  // 新增
        startCleanupTask()
        startCleaner()
        startAdaptiveBufferTask()
        startHealthMonitor()
        startConnectionMonitor()
        startConnectionPoolOptimizer()
        
        logger.info("[ConnectionManager] All subsystems started")
        
        await readPackets()
    }
    
    // 新增：高频优化器（100ms检查）
    private func startHighFrequencyOptimizer() {
        highFrequencyOptimizer?.cancel()
        highFrequencyOptimizer = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms
                guard let self = self else { break }
                
                // 批量优化所有连接
                await self.optimizeAllConnectionsRapidly()
            }
        }
    }
    
    private func optimizeAllConnectionsRapidly() async {
        let memoryMB = getCurrentMemoryUsageMB()
        
        // 批量处理，减少开销
        var connectionsToOptimize: [(String, TCPConnection)] = []
        
        for (key, conn) in tcpConnections {
            connectionsToOptimize.append((key, conn))
        }
        
        // 并行优化
        await withTaskGroup(of: Void.self) { group in
            for (_, conn) in connectionsToOptimize {
                group.addTask { [weak self] in
                    guard let self = self else { return }
                    
                    await conn.optimizeBufferBasedOnFlow()
                    
                    // 特殊连接处理
                    // 注意：isYouTubeConnection 不是 async 方法，所以不需要 await
                    if await self.isYouTubeConnection(conn) {
                        let stats = await conn.getBackpressureStats()
                        if !stats.isActive && stats.bufferSize > 48 * 1024 {
                            await conn.adjustBufferSize(48 * 1024)
                        }
                    }
                    
                    // 社交媒体连接需要 await
                    if await self.isSocialMediaConnection(conn) {
                        await self.handleSocialMediaOptimization(conn)
                    }
                }
            }
        }
    }
    
    private func handleSocialMediaOptimization(_ conn: TCPConnection) async {
        let stats = await conn.getBackpressureStats()
        
        // 检测滚动模式（快速小请求）
        if stats.dataRate > 0 && stats.dataRate < 10000 {  // 低速率但活跃
            socialStats.scrollEvents += 1
            // 滚动时保持中等缓冲区
            if stats.bufferSize > 64 * 1024 {
                await conn.adjustBufferSize(64 * 1024)
                socialStats.bufferAdjustments += 1
            }
        } else if stats.dataRate > 100000 {  // 高速率（视频）
            socialStats.videoStarts += 1
            // 视频需要更大缓冲区
            if stats.bufferSize < 128 * 1024 {
                await conn.adjustBufferSize(128 * 1024)
                socialStats.bufferAdjustments += 1
            }
        }
    }
    
    private func startHealthMonitor() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 20_000_000_000)
                guard let self = self else { break }
                await self.monitorConnectionPool()
            }
        }
    }

    private func startConnectionMonitor() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 10_000_000_000)
                guard let self = self else { break }
                await self.checkCriticalConnections()
            }
        }
    }

    private func checkCriticalConnections() async {
        var hasAnyPush = false
        var missingPorts = Set<UInt16>()
        let requiredPorts: Set<UInt16> = [5223, 5228, 5229, 5230]
        var activePorts = Set<UInt16>()
        
        for (_, conn) in tcpConnections {
            if await isPushConnection(conn) {
                hasAnyPush = true
                let port = conn.destPort
                activePorts.insert(port)
            }
        }
        
        missingPorts = requiredPorts.subtracting(activePorts)
        
        if !hasAnyPush {
            logger.critical("[Monitor] ⚠️ NO Push connections active!")
            await triggerPushReconnection()
        } else if !missingPorts.isEmpty {
            logger.warning("[Monitor] Missing Push ports: \(missingPorts)")
        } else {
            logger.info("[Monitor] ✅ Push services healthy")
        }
        
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
    
    private var lastPushReconnectTime = Date.distantPast
    
    private func triggerPushReconnection() async {
        logger.critical("[Push] Triggering reconnection attempt")
        lastPushReconnectTime = Date()
    }
    
    private func checkMemoryPressure() async {
        let memoryMB = getCurrentMemoryUsageMB()
        let now = Date()
        
        guard now.timeIntervalSince(lastMemoryCheckTime) >= 1.0 else { return }
        lastMemoryCheckTime = now
        
        if memoryMB >= memoryEmergencyMB {
            logger.critical("💀 EMERGENCY: Memory \(memoryMB)MB >= \(self.memoryEmergencyMB)MB")
            await emergencyCleanup()
        } else if memoryMB >= memoryCriticalMB {
            logger.critical("⚠️ CRITICAL: Memory \(memoryMB)MB >= \(self.memoryCriticalMB)MB")
            await trimConnections(targetMax: 10)
            dropNewConnections = true
        } else if memoryMB >= memoryWarningMB {
            logger.warning("⚠️ WARNING: Memory \(memoryMB)MB >= \(self.memoryWarningMB)MB")
            await trimConnections(targetMax: maxConnsDuringShedding)
            if tcpConnections.count >= maxConnsDuringShedding {
                dropNewConnections = true
            }
        } else if memoryMB < memoryNormalMB && isMemoryPressure {
            // 等待稳定后再恢复
            if now.timeIntervalSince(lastMemoryPressureTime) > 5.0 {
                logger.info("✅ Memory recovered: \(memoryMB)MB")
                isMemoryPressure = false
                dropNewConnections = false
                
                // 主动尝试恢复Push连接
                await restoreCriticalConnections()
            }
        }
        
        if memoryMB > 30 || stats.totalConnections % 10 == 0 {
            logger.debug("[Memory] Current: \(memoryMB)MB, Connections: \(self.tcpConnections.count)")
        }
    }
    
    private func restoreCriticalConnections() async {
        // 检查并恢复Push服务
        logger.info("[Recovery] Checking critical connections...")
            
        // 使用辅助方法检查
        if !(await hasPushConnection()) {
            logger.warning("[Recovery] No push connections found, attempting to restore...")
            
            // 临时允许新连接用于恢复Push服务
            let originalDropState = dropNewConnections
            dropNewConnections = false
            
            // 触发Push重连
            lastPushReconnectTime = Date()
            
            // 给一些时间让Push连接建立
            Task {
                try? await Task.sleep(nanoseconds: 3_000_000_000) // 3秒
                
                // 如果还没有其他内存压力，恢复原始状态
                if !self.isMemoryPressure {
                    self.dropNewConnections = originalDropState
                }
            }
        }
        
        // 检查并调整现有连接的缓冲区
        await adjustConnectionBuffers()
    }
    
    // 分离缓冲区调整逻辑
    private func adjustConnectionBuffers() async {
        for (_, conn) in tcpConnections {
            let currentBuffer = await conn.recvBufferLimit
            
            // 恢复正常缓冲区大小
            if await isPushConnection(conn) {
                if currentBuffer < 32 * 1024 {
                    await conn.adjustBufferSize(32 * 1024)
                    logger.debug("[Recovery] Restored push connection buffer to 32KB")
                }
            } else if isYouTubeConnection(conn) {
                if currentBuffer < 96 * 1024 {
                    await conn.adjustBufferSize(96 * 1024)
                    logger.debug("[Recovery] Restored YouTube connection buffer to 96KB")
                }
            } else if await isSocialMediaConnection(conn) {
                if currentBuffer < 64 * 1024 {
                    await conn.adjustBufferSize(64 * 1024)
                    logger.debug("[Recovery] Restored social media connection buffer to 64KB")
                }
            } else {
                if currentBuffer < 16 * 1024 {
                    await conn.adjustBufferSize(16 * 1024)
                }
            }
        }
    }
    
    private func hasPushConnection() async -> Bool {
        for (_, conn) in tcpConnections {
            if await isPushConnection(conn) {
                return true
            }
        }
        return false
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
            // 不同类型连接的不同超时时间
            let maxIdle: TimeInterval
            
            if await isPushConnection(conn) {
                maxIdle = 300  // Push连接5分钟
            } else if isYouTubeConnection(conn) {
                maxIdle = 120  // YouTube 2分钟
            } else if await isSocialMediaConnection(conn) {
                maxIdle = 90   // 社交媒体1.5分钟
            } else {
                maxIdle = 60   // 普通连接1分钟
            }
            
            if let lastActivity = await conn.getLastActivityTime() {
                if now.timeIntervalSince(lastActivity) > maxIdle {
                    staleKeys.append(key)
                }
            }
        }
        
        await protectPushService()
        
        for key in staleKeys {
            if let conn = tcpConnections[key] {
                await conn.close()
                tcpConnections.removeValue(forKey: key)
                recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
            }
        }
        
        if staleKeys.count > 0 {
            logger.info("[Cleanup] Removed \(staleKeys.count) stale connections")
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
        
        // 统计不同类型连接
        var pushCount = 0
        var youtubeCount = 0
        var socialCount = 0
        
        for (_, conn) in tcpConnections {
            if await isPushConnection(conn) {
                pushCount += 1
            } else if isYouTubeConnection(conn) {
                youtubeCount += 1
            } else if await isSocialMediaConnection(conn) {
                socialCount += 1
            }
        }
        
        logger.info("""
        
        === ConnectionManager Statistics ===
        Uptime: \(uptimeStr)
        Memory: \(memoryMB)MB (pressure: \(self.isMemoryPressure), shedding: \(self.shedding))
        Connections:
          - Total: \(self.stats.totalConnections)
          - Active: \(self.tcpConnections.count)/\(self.maxConnections)
          - Push: \(pushCount)
          - YouTube: \(youtubeCount)
          - Social Media: \(socialCount)
          - Failed: \(self.stats.failedConnections)
          - APNs Bypassed: \(self.stats.apnsBypassedConnections)
          - Duplicate SYNs: \(self.stats.duplicateSyns)
        Traffic:
          - Received: \(self.formatBytes(self.stats.bytesReceived))
          - Sent: \(self.formatBytes(self.stats.bytesSent))
        Buffers:
          - Pending SYNs: \(self.pendingSyns.count)
          - Tombstones: \(self.recentlyClosed.count)
        ====================================
        """)
        
        // 社交媒体统计
        if socialCount > 0 {
            logger.info("""
            Social Media Performance:
              - Scroll Events: \(self.socialStats.scrollEvents)
              - Video Starts: \(self.socialStats.videoStarts)
              - Video Interrupts: \(self.socialStats.videoInterrupts)
              - Buffer Adjustments: \(self.socialStats.bufferAdjustments)
            """)
        }
        
        // 连接质量统计
        await evaluateConnectionQuality()
        
        let avgQuality = connectionQualities.values
            .map { $0.score }
            .reduce(0, +) / Double(max(1, connectionQualities.count))
        
        var highTrafficCount = 0
        var totalBufferSize = 0
        
        for (_, conn) in tcpConnections {
            let stats = await conn.getBackpressureStats()
            totalBufferSize += stats.bufferSize
            if stats.dataRate > 100000 {  // 100KB/s以上算高流量
                highTrafficCount += 1
            }
        }
        
        let avgBufferSize = tcpConnections.isEmpty ? 0 : totalBufferSize / tcpConnections.count
        
        logger.info("""
        Connection Quality:
          - Average Score: \(String(format: "%.2f", avgQuality))
          - High Traffic Conns: \(highTrafficCount)
          - Avg Buffer Size: \(self.formatBytes(avgBufferSize))
          - Buffer Overflows: \(self.connectionQualities.values.map { $0.overflowCount }.reduce(0, +))
        """)
    }
    
    private func evaluateConnectionQuality() async {
        for (key, conn) in tcpConnections {
            var quality = connectionQualities[key] ?? ConnectionQuality(key: key)
            
            quality.overflowCount = await conn.getOverflowCount()
            quality.lastUpdate = Date()
            
            // 获取背压统计
            let stats = await conn.getBackpressureStats()
            if stats.dataRate > 0 {
                quality.throughput = stats.dataRate
            }
            
            connectionQualities[key] = quality
            
            if quality.score < 0.3 {
                logger.warning("[Quality] Poor connection quality for \(key): \(quality.score)")
            }
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

            let batchSize = isMemoryPressure ? 8 : 32
            let batch = datas.prefix(batchSize)

            for pkt in batch {
                if let ip = IPv4Packet(data: pkt) {
                    // ✅ 检查是否为 APNs IP 段，如果是则跳过处理
                    if isAPNsIP(ip.destinationAddress) {
                        stats.apnsBypassedConnections += 1
                        logger.debug("[APNs] Bypassing packet to APNs IP: \(String(describing: ip.destinationAddress))")
                        continue
                    }
                    
                    switch ip.`protocol` {
                    case 6: Task { await self.handleTCPPacket(ip) }
                    case 17: Task { await self.handleUDPPacket(ip) }
                    case 1: Task { await self.handleICMPPacket(ip) }
                    default: break
                    }
                }
            }

            if isMemoryPressure { try? await Task.sleep(nanoseconds: 5_000_000) }
        }
    }
    
    private func handleICMPPacket(_ ipPacket: IPv4Packet) async {
        // 目前不对入站 ICMP 做回复，避免形成"ICMP->ICMP"的循环与额外内存消耗。
    }

    // MARK: - Connection Trimming (Enhanced)

    private func trimConnections(targetMax: Int) async {
        let current = tcpConnections.count
        guard current > targetMax else { return }
        
        await protectPushService()
        
        var victims: [(String, TCPConnection, Date?, ConnectionPriority)] = []
        
        for (key, conn) in tcpConnections {
            // 保护关键连接
            if await isPushConnection(conn) {
                continue
            }
            
            // YouTube和社交媒体的活跃连接也要保护
            if isYouTubeConnection(conn) {
                let stats = await conn.getBackpressureStats()
                if stats.isActive && stats.dataRate > 10000 {  // 10KB/s以上
                    continue
                }
            }
            
            let isSocial = await isSocialMediaConnection(conn)
            if isSocial {
                let stats = await conn.getBackpressureStats()
                if stats.isActive && stats.dataRate > 10000 {  // 10KB/s以上
                    continue
                }
            }
            
            let lastActivity = await conn.getLastActivityTime()
            let priority = getConnectionPriority(destPort: conn.destPort)
            victims.append((key, conn, lastActivity, priority))
        }
        
        // 按优先级和活动时间排序
        victims.sort { (a, b) in
            if a.3 != b.3 {
                return a.3 < b.3  // 低优先级先关
            }
            let aTime = a.2 ?? Date.distantPast
            let bTime = b.2 ?? Date.distantPast
            return aTime < bTime  // 旧连接先关
        }
        
        let needClose = current - targetMax
        for i in 0..<min(needClose, victims.count) {
            let (key, conn, _, _) = victims[i]
            await conn.close()
            tcpConnections.removeValue(forKey: key)
            recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
        }
        
        stats.activeConnections = tcpConnections.count
        logger.info("[Trim] Protected critical connections, closed \(min(needClose, victims.count)) others")
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

    // MARK: - DNS处理 (适配智能代理)
        
    private func handleDNSQuery(ipPacket: IPv4Packet, udp: UDPDatagram) async {
        let qtype = extractQType(from: udp.payload) ?? 0
        
        // ✅ 使用改进的异步DNS处理
        guard let result = await dnsInterceptor.handleQueryAndCreateResponse(for: udp.payload) else {
            logger.warning("[DNS] Failed to process DNS query")
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
        
        let _src = String(describing: ipPacket.sourceAddress)
        let _qtype = qtypeName(qtype)
        
        // ✅ 区分处理类型的日志
        if result.fakeIP != nil {
            logger.debug("[DNS] Fake IP reply to \(_src):\(udp.sourcePort) qtype=\(_qtype)")
        } else {
            logger.info("[DNS] Forwarded reply to \(_src):\(udp.sourcePort) qtype=\(_qtype) (likely APNs)")
        }
    }

    // MARK: - TCP

    private func handleTCPPacket(_ ipPacket: IPv4Packet) async {
        guard let tcpSegment = TCPSegment(data: ipPacket.payload) else { return }
        
        // ✅ 检查目标IP是否为APNs网段
        if isAPNsIP(ipPacket.destinationAddress) {
            stats.apnsBypassedConnections += 1
            logger.debug("[APNs] Bypassing TCP to APNs IP: \(String(describing: ipPacket.destinationAddress)):\(tcpSegment.destinationPort)")
            return
        }
        
        let packetID = "\(ipPacket.sourceAddress):\(tcpSegment.sourcePort)->\(ipPacket.destinationAddress):\(tcpSegment.destinationPort)-\(tcpSegment.sequenceNumber)"
        
        if processedPackets.contains(packetID) {
            return
        }
        
        processedPackets.insert(packetID)
        
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
        
        // 处理RST包
        if tcpSegment.isRST {
            if let connection = tcpConnections[key] {
                // 通知连接收到RST
                await connection.handleRSTReceived()
            }
            await handleConnectionClose(key: key)
            return
        }
        
        // 处理ACK包并提取窗口大小
        if tcpSegment.isACK && !tcpSegment.isSYN {
            if let connection = tcpConnections[key] {
                // 从原始TCP数据中提取窗口大小
                let tcpData = ipPacket.payload
                if tcpData.count >= 16 {
                    let windowSize = (UInt16(tcpData[14]) << 8) | UInt16(tcpData[15])
                    await connection.onInboundAckWithWindow(
                        ackNumber: tcpSegment.acknowledgementNumber,
                        windowSize: windowSize
                    )
                } else {
                    // 数据不足时使用默认方法
                    await connection.onInboundAck(ackNumber: tcpSegment.acknowledgementNumber)
                }
            }
        }
        
        if let expires = recentlyClosed[key], Date() < expires {
            return
        }

        if tcpSegment.isSYN && !tcpSegment.isACK {
            await handleSYN(ipPacket: ipPacket, tcpSegment: tcpSegment, key: key)
            return
        }

        if tcpSegment.isFIN {
            if let connection = tcpConnections[key] {
                await connection.onInboundFin(seq: tcpSegment.sequenceNumber)
                // 检测是否是社交媒体的中断
                if await isSocialMediaConnection(connection) {
                    socialStats.videoInterrupts += 1
                    await connection.handleUserInterrupt()
                }
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
        switch destPort {
        case 5223, 5228, 5229, 5230: return .critical
        case 443: return .high
        case 80: return .normal
        default: return .low
        }
    }

    private func handleSYN(ipPacket: IPv4Packet, tcpSegment: TCPSegment, key: String) async {
        let priority = getConnectionPriority(destPort: tcpSegment.destinationPort)
        
        // 检查是否是特殊服务
        var domainForSocks: String? = nil
        let dstIP = ipPacket.destinationAddress
        if await dnsInterceptor.contains(dstIP) {
            domainForSocks = await lookupDomainWithBackoff(fakeIP: dstIP)
            if domainForSocks != nil {
                await dnsInterceptor.retain(fakeIP: dstIP)
                
                let _dst = String(describing: dstIP)
                let _host = domainForSocks!
                let _msg2 = "[DNS] Fake IP \(_dst) mapped to: \(_host)"
                logger.debug("\(_msg2)")
            }
        }
        
        let isYouTube = domainForSocks?.lowercased().contains("youtube") ?? false ||
                        domainForSocks?.lowercased().contains("googlevideo") ?? false
        
        let domainLower = domainForSocks?.lowercased() ?? ""
        let isSocial = domainLower.contains("twitter") ||
                       domainLower.contains("instagram") ||
                       domainLower.contains("facebook") ||
                       domainLower.contains("tiktok") ||
                       domainLower.contains("x.com") ||
                       domainLower.contains("twimg")
        
        if isMemoryPressure {
            // 内存压力下，只允许关键连接
            let shouldReject = (priority < .high) && !isYouTube && !isSocial
            if shouldReject {
                stats.failedConnections += 1
                recentRejections += 1
                updateRecentRejections()
                logger.info("[Priority] Rejecting low priority connection under memory pressure")
                return
            }
        }
        
        if tcpConnections.count >= maxConnections {
            // 为高优先级连接腾出空间
            let needMakeRoom = (priority >= .high) || isYouTube || isSocial
            if needMakeRoom {
                await makeRoomForHighPriorityConnection(priority: priority)
            } else {
                stats.failedConnections += 1
                recentRejections += 1
                logger.info("[Priority] Rejecting low priority connection, limit reached")
                return
            }
        }
        
        if dropNewConnections {
            // 特殊服务例外
            let needAllow = isYouTube || isSocial || (priority == .critical)
            if !needAllow {
                stats.failedConnections += 1
                if (stats.failedConnections % logSampleN) == 0 {
                    logger.warning("[Memory] Rejecting connection under pressure: \(key)")
                }
                return
            }
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
        
        connectionStartTimes[key] = Date()
        
        // 计算初始缓冲区大小
        let bufferSize = calculateBufferSizeForService(
            host: domainForSocks,
            port: tcpSegment.destinationPort,
            isYouTube: isYouTube,
            isSocial: isSocial,
            priority: priority
        )
        
        if tcpConnections.count > peakConnections {
            peakConnections = tcpConnections.count
        }
        
        // 更新统计
        if isYouTube {
            stats.youtubeConnections += 1
        } else if isSocial {
            stats.socialMediaConnections += 1
        }

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
            recvBufferLimit: bufferSize
        )

        tcpConnections[key] = newConn
        stats.totalConnections += 1
        stats.activeConnections = tcpConnections.count

        await newConn.acceptClientSyn(tcpHeaderAndOptions: Data(tcpSlice))

        if stats.totalConnections % 10 == 0 {
            logger.debug("[Adaptive] Buffer size: \(bufferSize) bytes for \(self.tcpConnections.count)/\(self.maxConnections) connections")
        }
        
        Task { [weak self] in
            guard let self = self else { return }
            await newConn.start()
            await self.finishAndCleanup(key: key, dstIP: dstIP)
        }
    }
    
    private func calculateBufferSizeForService(
        host: String?,
        port: UInt16,
        isYouTube: Bool,
        isSocial: Bool,
        priority: ConnectionPriority
    ) -> Int {
        // 检查当前内存压力
        let memoryMB = getCurrentMemoryUsageMB()
        let pressureFactor = memoryMB < memoryNormalMB ? 1.0 : 0.7
        
        // YouTube特殊处理
        if isYouTube {
            return Int(192 * 1024 * pressureFactor)  // 192KB或134KB
        }
        
        // 社交媒体特殊处理
        if isSocial {
            return Int(96 * 1024 * pressureFactor)   // 96KB或67KB
        }
        
        // 基于优先级
        switch priority {
        case .critical:
            return Int(128 * 1024 * pressureFactor)
        case .high:
            return Int(64 * 1024 * pressureFactor)
        case .normal:
            return Int(32 * 1024 * pressureFactor)
        case .low:
            return Int(16 * 1024 * pressureFactor)
        }
    }
    
    private func makeRoomForHighPriorityConnection(priority: ConnectionPriority) async {
        var victims: [(String, ConnectionPriority, Date?)] = []
        
        for (key, conn) in tcpConnections {
            let port = conn.destPort
            let connPriority = getConnectionPriority(destPort: port)
            
            // 不关闭Push连接
            if port == 5223 || port == 5228 {
                continue
            }
            
            // 不关闭活跃的YouTube和社交媒体连接
            
            // 保护活跃的YouTube连接
            if keepCritical && isYouTubeConnection(conn) {
                let stats = await conn.getBackpressureStats()
                if stats.isActive {
                    continue
                }
            }
            
            // 保护活跃的社交媒体连接
            if keepCritical {
                let isSocial = await isSocialMediaConnection(conn)
                if isSocial {
                    let stats = await conn.getBackpressureStats()
                    if stats.isActive {
                        continue
                    }
                }
            }
            
            if connPriority < priority {
                let lastActivity = await conn.getLastActivityTime()
                victims.append((key, connPriority, lastActivity))
            }
        }
        
        guard !victims.isEmpty else {
            logger.error("[Priority] No connections available to close")
            return
        }
        
        // 按时间排序，关闭最旧的
        victims.sort { (a, b) in
            let aTime = a.2 ?? Date.distantPast
            let bTime = b.2 ?? Date.distantPast
            return aTime < bTime
        }
        
        let victim = victims[0]
        if let conn = tcpConnections[victim.0] {
            logger.info("[Priority] Closing \(victim.0) for high priority connection")
            await conn.close()
            tcpConnections.removeValue(forKey: victim.0)
            recentlyClosed[victim.0] = Date().addingTimeInterval(tombstoneTTL)
            stats.activeConnections = tcpConnections.count
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
        
        if priority == .critical {
            await handleCriticalConnectionLoss(key: key, port: port)
        }
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
            }
        }
    }
    
    private func checkIfPushServiceNeeded(port: UInt16) async -> Bool {
        for (_, conn) in tcpConnections {
            let connPort = conn.destPort
            if connPort == port {
                return false
            }
        }
        return true
    }
    
    private func startConnectionPoolOptimizer() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 30_000_000_000)
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
        - Connections: \(activeCount)/\(self.maxConnections)
        - Memory: \(memoryMB)MB
        - Health Score: \(String(format: "%.2f", healthScore))
        """)
        
        if healthScore < 0.3 {
            logger.warning("[Optimizer] Poor health, aggressive cleanup triggered")
            await trimConnections(targetMax: self.maxConnections / 2)
        } else if healthScore < 0.5 {
            logger.info("[Optimizer] Moderate health, gentle cleanup triggered")
            await cleanupIdleConnections(maxIdle: 30)
        } else if healthScore > 0.8 && activeCount < self.maxConnections / 2 {
            await increaseBuffersForActiveConnections()
        }
    }
    
    private func calculateHealthScore() -> Double {
        let connectionUtilization = Double(tcpConnections.count) / Double(maxConnections)
        let failureRate = Double(stats.failedConnections) / max(1.0, Double(stats.totalConnections))
        let memoryPressure = Double(getCurrentMemoryUsageMB()) / Double(memoryWarningMB)
        
        let score = 1.0 - (connectionUtilization * 0.3 + failureRate * 0.4 + memoryPressure * 0.3)
        return max(0.0, min(1.0, score))
    }

    private func cleanupIdleConnections(maxIdle: TimeInterval) async {
        let now = Date()
        var idleConnections: [(String, TimeInterval)] = []
        
        for (key, conn) in tcpConnections {
            // 不清理关键连接
            if await isPushConnection(conn) {
                continue
            }
            
            if let lastActivity = await conn.getLastActivityTime() {
                let idleTime = now.timeIntervalSince(lastActivity)
                if idleTime > maxIdle {
                    idleConnections.append((key, idleTime))
                }
            }
        }
        
        idleConnections.sort { $0.1 > $1.1 }
        
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

    private func increaseBuffersForActiveConnections() async {
        for (_, conn) in tcpConnections {
            let currentBuffer = await conn.recvBufferLimit
            let stats = await conn.getBackpressureStats()
            
            if stats.isActive && currentBuffer < 64 * 1024 {
                await conn.adjustBufferSize(currentBuffer + 16 * 1024)
            }
        }
        logger.info("[Optimizer] Increased buffers for active connections")
    }

    private func handleEstablishedConnection(
        connection: TCPConnection,
        tcpSegment: TCPSegment
    ) async {
        if tcpSegment.payload.isEmpty && tcpSegment.isACK {
            // 纯ACK包处理
            await connection.onInboundAck(ackNumber: tcpSegment.acknowledgementNumber)
        } else if !tcpSegment.payload.isEmpty {
            // 处理带数据的包
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
        
        if let startTime = connectionStartTimes[key] {
            let lifetime = Date().timeIntervalSince(startTime)
            connectionLifetimes.append(lifetime)
            
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

    // MARK: - DNS/ICMP Utils

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

        if dstPort == 443 || dstPort == 80 {
            let icmp = ICMPPacket.unreachable(for: ipPacket)
            packetFlow.writePackets([icmp.data], withProtocols: [NSNumber(value: AF_INET)])
            
            let _dst2 = String(describing: ipPacket.destinationAddress)
            let _msg3 = "[UDP] QUIC/HTTP3 -> \(_dst2):\(dstPort), sent ICMP unreachable"
            logger.debug("\(_msg3)")
            
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
    
    private func maybeUnpauseReadsAfterCooldown() {
        let mem = getCurrentMemoryUsageMB()
        if mem < memoryCriticalMB {
            pausedReads = false
        }
    }

    private func maybeLiftIntakeBanAfterCooldown() {
        let mem = getCurrentMemoryUsageMB()
        if mem < memoryCriticalMB {
            dropNewConnections = false
        }
    }
    
    // MARK: - Adaptive Buffer Management
    private func adaptiveBufferSize() -> Int {
        let baseSize = 16 * 1024
        let currentConnections = tcpConnections.count
        let loadFactor = Double(currentConnections) / Double(maxConnections)
        
        let multiplier = 2.0 - loadFactor
        let adaptedSize = Int(Double(baseSize) * multiplier)
        
        return max(8 * 1024, min(32 * 1024, adaptedSize))
    }
    
    private func getMemoryPressureLevel() -> Int {
        let memoryMB = getCurrentMemoryUsageMB()
        if memoryMB >= memoryCriticalMB { return 3 }
        if memoryMB >= memoryWarningMB { return 2 }
        if memoryMB >= memoryNormalMB { return 1 }
        return 0
    }
    
    private func startAdaptiveBufferTask() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 10_000_000_000)
                guard let self = self else { break }
                await self.adjustAllConnectionBuffers()
            }
        }
    }

    private func adjustAllConnectionBuffers() async {
        let pressureLevel = getMemoryPressureLevel()
        
        // 正常情况下不需要调整
        guard pressureLevel >= 1 else { return }
        
        for (_, conn) in tcpConnections {
            // 让连接自己处理（它们有100ms的快速优化）
            // 这里只处理内存压力情况
            if pressureLevel >= 2 {
                let targetSize = adaptiveBufferSize()
                await conn.handleMemoryPressure(targetBufferSize: targetSize / 2)
            }
        }
        
        if pressureLevel >= 2 {
            logger.debug("[Adaptive] Memory pressure adjustment for \(self.tcpConnections.count) connections")
        }
    }
    
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
    
    private func identifyPushService(port: UInt16, conn: TCPConnection) -> String {
        switch port {
        case 5223: return "APNs"
        case 5228: return "FCM"
        case 5229, 5230: return "FCM-Alt"
        case 443:
            if let host = conn.destinationHost?.lowercased() {
                if host.contains("push.apple") { return "APNs-HTTPS" }
                if host.contains("mtalk.google") { return "FCM-HTTPS" }
            }
            return "Push-HTTPS"
        default: return "Other"
        }
    }
    
    private func monitorConnectionPool() async {
        let healthScore = calculateHealthScore()
        
        if healthScore < 0.5 {
            logger.warning("[Health] Poor health: \(healthScore)")
            
            if healthScore < 0.3 {
                await emergencyCleanup(keepCritical: true)
            } else if healthScore < 0.4 {
                await cleanupLowPriorityConnections()
            } else {
                await cleanupIdleConnections(maxIdle: 30)
            }
        }
    }
    
    private func emergencyCleanup(keepCritical: Bool) async {
        for (key, conn) in tcpConnections {
            let port = conn.destPort
            
            // 保护关键服务
            if keepCritical && (port == 5223 || port == 5228) {
                continue
            }
            
            if isYouTubeConnection(conn) {
                let stats = await conn.getBackpressureStats()
                if stats.isActive {
                    continue
                }
            }
            
            let isSocial = await isSocialMediaConnection(conn)
            if isSocial {
                let stats = await conn.getBackpressureStats()
                if stats.isActive {
                    continue
                }
            }
            
            if keepCritical && port == 443 {
                if let host = conn.destinationHost,
                   host.contains("apple.com") {
                    continue
                }
            }
            
            await conn.close()
            tcpConnections.removeValue(forKey: key)
            recentlyClosed[key] = Date().addingTimeInterval(tombstoneTTL)
        }
        
        logger.critical("[Emergency] Kept only critical connections")
    }
    
    private func cleanupLowPriorityConnections() async {
        var lowPriorityConnections: [(String, ConnectionPriority, Date?)] = []
        
        for (key, conn) in tcpConnections {
            let port = conn.destPort
            let priority = getConnectionPriority(destPort: port)
            
            // 不清理Push服务
            if port == 5223 || port == 5228 {
                continue
            }
            
            // 不清理活跃的YouTube和社交媒体
            let isYouTube = isYouTubeConnection(conn)
            let isSocial = await isSocialMediaConnection(conn)
            if isYouTube || isSocial {
                let stats = await conn.getBackpressureStats()
                if stats.isActive {
                    continue
                }
            }
            
            if priority <= .normal {
                let lastActivity = await conn.getLastActivityTime()
                lowPriorityConnections.append((key, priority, lastActivity))
            }
        }
        
        lowPriorityConnections.sort { (a, b) in
            if a.1 != b.1 {
                return a.1 < b.1
            }
            let aTime = a.2 ?? Date.distantPast
            let bTime = b.2 ?? Date.distantPast
            return aTime < bTime
        }
        
        let targetCloseCount = min(
            lowPriorityConnections.count,
            max(1, tcpConnections.count / 3)
        )
        
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
}

// MARK: - Extension for Shutdown

extension ConnectionManager {
    nonisolated func prepareForStop() {
        Task { [weak self] in
            guard let self = self else { return }
            await self.stopTimers()
            await self.closeAllConnections()
        }
    }
    
    private func stopTimers() {
        statsTimer?.cancel()
        memoryMonitorTimer?.cancel()
        highFrequencyOptimizer?.cancel()
        statsTimer = nil
        memoryMonitorTimer = nil
        highFrequencyOptimizer = nil
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
