//
//  TCPConnection.swift
//  vpn2socks
//
//  Optimized version with YouTube, Social Media & Core Performance improvements
//

import NetworkExtension
import Network
import Foundation

private struct BackpressureMetrics {
    var lastDataReceivedTime = Date()
    var lastDataSentTime = Date()
    var consecutiveIdleCycles = 0
    var recentDataRate: Double = 0
    var peakDataRate: Double = 0
    var isActive = false
    
    // 滑动窗口记录最近的数据量
    var recentDataPoints: [(Date, Int)] = []
    let windowDuration: TimeInterval = 1.0 // 1秒窗口
    
    var growthTrend: Double = 0  // 流量增长趋势
    var lastGrowthCheck = Date()
    
    var overflowCount: Int = 0
    var lastOverflowTime: Date?
    
    mutating func recordDataReceived(_ bytes: Int) {
        let now = Date()
        lastDataReceivedTime = now
        isActive = true
        consecutiveIdleCycles = 0
        
        // 更新滑动窗口
        recentDataPoints.append((now, bytes))
        recentDataPoints = recentDataPoints.filter {
            now.timeIntervalSince($0.0) <= windowDuration
        }
        
        // 计算当前速率
        let totalBytes = recentDataPoints.reduce(0) { $0 + $1.1 }
        recentDataRate = Double(totalBytes) / windowDuration
        peakDataRate = max(peakDataRate, recentDataRate)
    }
    
    mutating func markIdle() {
        consecutiveIdleCycles += 1
        if consecutiveIdleCycles > 3 {
            isActive = false
            recentDataRate = 0
        }
    }
    
    var timeSinceLastData: TimeInterval {
        Date().timeIntervalSince(lastDataReceivedTime)
    }
    
    mutating func recordOverflow() {
        overflowCount += 1
        lastOverflowTime = Date()
    }
    
    var recentOverflowRate: Double {
        guard let lastTime = lastOverflowTime else { return 0 }
        let timeSince = Date().timeIntervalSince(lastTime)
        guard timeSince > 0 else { return Double(overflowCount) }
        return Double(overflowCount) / timeSince
    }
}

// 流量模式检测 - 增强版
private struct TrafficPattern {
    var requestCount: Int = 0
    var smallPayloadCount: Int = 0  // < 10KB的请求
    var mediumPayloadCount: Int = 0 // 10KB-100KB的请求
    var largePayloadCount: Int = 0  // > 100KB的请求
    var lastRequestTime: Date = Date()
    var isBatchPattern: Bool = false
    var isYouTubePattern: Bool = false
    var connectionAge: TimeInterval = 0
    
    // 社交媒体模式新增字段
    var interruptCount: Int = 0
    var lastInterruptTime: Date?
    var averageSessionDuration: TimeInterval = 0
    var isSocialMediaPattern: Bool = false
    var isScrollingPattern: Bool = false
    var videoStartCount: Int = 0
    var videoInterruptCount: Int = 0
    
    mutating func analyze() {
        // 检测批量小请求模式（如YouTube缩略图）
        if smallPayloadCount > 5 &&
           Date().timeIntervalSince(lastRequestTime) < 2.0 {
            isBatchPattern = true
        }
        
        // YouTube特征：前期大量小请求，后期有大请求
        if connectionAge < 5.0 && smallPayloadCount > 8 {
            isYouTubePattern = true
        } else if connectionAge > 5.0 && largePayloadCount > 2 {
            isYouTubePattern = true
        }
        
        // 社交媒体特征：小中大请求混合
        if smallPayloadCount > 5 &&
           mediumPayloadCount > 2 &&
           requestCount > 10 {
            isSocialMediaPattern = true
        }
    }
    
    mutating func detectSocialMediaBehavior() {
        // 检测频繁中断模式（快速滚动）
        if interruptCount > 3 &&
           lastInterruptTime != nil &&
           Date().timeIntervalSince(lastInterruptTime!) < 5.0 {
            isScrollingPattern = true
        }
    }
}

// 区分Keep-Alive和实际数据
private struct TrafficType {
    var isKeepAlive: Bool = false
    var isActualData: Bool = false
    var lastKeepAliveTime: Date?
    var lastActualDataTime: Date?
}

enum ContentType {
    case video
    case images
    case mixed
    case text
}

final actor TCPConnection {

    private var backpressure = BackpressureMetrics()
    private var trafficPattern = TrafficPattern()
    private var trafficType = TrafficType()
    
    // 优化的缓冲区大小常量 - 改为静态常量
       private static let MIN_BUFFER = 4 * 1024
       private static let DEFAULT_BUFFER = 64 * 1024
       private static let MAX_BUFFER = 256 * 1024
       private static let BURST_BUFFER = 512 * 1024
       private static let YOUTUBE_BUFFER = 192 * 1024
       
       // 新增的YouTube相关常量
       private static let YOUTUBE_MAX_BUFFER = 256 * 1024  // YouTube最大缓冲
       private static let YOUTUBE_EXPAND_STEP = 64 * 1024  // 每次扩展64KB
       
       // 社交媒体专用缓冲区常量
       private static let SOCIAL_MIN_BUFFER = 32 * 1024
       private static let SOCIAL_NORMAL_BUFFER = 96 * 1024
       private static let SOCIAL_SCROLL_BUFFER = 64 * 1024
       private static let SOCIAL_VIDEO_BUFFER = 128 * 1024
    
    
    
    // 为了方便访问，添加实例属性
    private var MIN_BUFFER: Int { Self.MIN_BUFFER }
    private var DEFAULT_BUFFER: Int { Self.DEFAULT_BUFFER }
    private var MAX_BUFFER: Int { Self.MAX_BUFFER }
    private var BURST_BUFFER: Int { Self.BURST_BUFFER }
    private var YOUTUBE_BUFFER: Int { Self.YOUTUBE_BUFFER }
    private var SOCIAL_MIN_BUFFER: Int { Self.SOCIAL_MIN_BUFFER }
    private var SOCIAL_NORMAL_BUFFER: Int { Self.SOCIAL_NORMAL_BUFFER }
    private var SOCIAL_SCROLL_BUFFER: Int { Self.SOCIAL_SCROLL_BUFFER }
    private var SOCIAL_VIDEO_BUFFER: Int { Self.SOCIAL_VIDEO_BUFFER }
    
    // 快速响应阈值
    // TCPConnection.swift - 添加动态阈值
    private var FAST_SHRINK_THRESHOLD: TimeInterval {
        if isYouTubeConnection() {
            return 0.2  // YouTube: 200ms
        } else if isSocialMediaConnection() {
            return 0.15 // 社交媒体: 150ms
        } else {
            return 0.1  // 其他: 100ms
        }
    }
    
    private var NORMAL_SHRINK_THRESHOLD: TimeInterval {
        if isYouTubeConnection() {
            return 1.0  // YouTube: 1秒
        } else if isSocialMediaConnection() {
            return 0.75 // 社交媒体: 750ms
        } else {
            return 0.5  // 其他: 500ms
        }
    }
    
    // 连接启动时间（用于判断新连接）
    private var connectionStartTime: Date?
    private var isWarmingUp = true
    private var lastDataCheckpoint: Date = Date()
    private var highFrequencyMonitorTask: Task<Void, Never>?
    
    enum ConnectionPriority: Int, Comparable {
        case low = 0
        case normal = 1
        case high = 2
        case critical = 3
        
        static func < (lhs: ConnectionPriority, rhs: ConnectionPriority) -> Bool {
            return lhs.rawValue < rhs.rawValue
        }
    }
    
    // MARK: - 客户端拒绝检测相关属性
    private var clientAdvertisedWindow: UInt16 = 65535
    private var zeroWindowProbeCount: Int = 0
    private var lastRefusalTime: Date?
    
    private func detectClientRefusal() -> Bool {
        // YouTube和社交媒体连接需要更多证据才判定为拒绝
        if isYouTubeConnection() || isSocialMediaConnection() || trafficPattern.isYouTubePattern {
            // 更宽容的阈值
            if duplicatePacketCount > 20 || retransmitRetries >= MAX_RETRANSMIT_RETRIES * 2 {
                return true
            }
            return false
        }
        
        // 检测各种拒绝信号
        if duplicatePacketCount > 10 {
            log("[Refusal] Too many duplicate ACKs: \(duplicatePacketCount)")
            return true
        }
        
        if socksState == .closed || socksConnection == nil {
            return true
        }
        
        if retransmitRetries >= MAX_RETRANSMIT_RETRIES {
            return true
        }
        
        // 中等信号：组合判断
        var refusalScore = 0
        
        if duplicatePacketCount > 5 {
            refusalScore += duplicatePacketCount / 5
        }
        
        if overflowCount > 5 {
            refusalScore += overflowCount / 5
        }
        
        if clientAdvertisedWindow == 0 {
            refusalScore += zeroWindowProbeCount
        }
        
        if outOfOrderPackets.count > MAX_BUFFERED_PACKETS / 2 {
            refusalScore += 2
        }
        
        // 综合评分超过阈值
        if refusalScore >= 5 {
            log("[Refusal] Refusal score: \(refusalScore) - client likely refusing data")
            return true
        }
        
        return false
    }
    
    func handleRSTReceived() {
        log("[RST] Connection reset by peer")
        lastRefusalTime = Date()
        
        // 立即收缩并清理
        shrinkToMinimumImmediate()
        clearAllBuffers()
    }
    
    public func shrinkToMinimumImmediate() {
        let oldSize = recvBufferLimit
        
        // YouTube和社交媒体连接收缩到更大的最小值
        let minSize: Int
        if isYouTubeConnection() {
            minSize = 48 * 1024  // YouTube保持48KB
        } else if isSocialMediaConnection() {
            minSize = SOCIAL_MIN_BUFFER  // 社交媒体保持32KB
        } else {
            minSize = MIN_BUFFER  // 普通连接4KB
        }
        
        recvBufferLimit = minSize
        updateAdvertisedWindow()
        
        // 修复类型转换问题
        if availableWindow < UInt16(lastAdvertisedWindow / 2) {
            sendPureAck()
            lastAdvertisedWindow = availableWindow
        }
        
        log("[Backpressure] Emergency shrink: \(oldSize) -> \(minSize) bytes")
    }
    
    private func clearAllBuffers() {
        outOfOrderPackets.removeAll()
        
        if socksState != .established {
            pendingClientBytes.removeAll()
            pendingBytesTotal = 0
        }
        
        backpressure = BackpressureMetrics()
    }
    
    private func trimBuffersAggressively() {
        let maxKeep = 2
        
        if outOfOrderPackets.count > maxKeep {
            let toRemove = outOfOrderPackets.count - maxKeep
            outOfOrderPackets.removeFirst(toRemove)
            log("[Trim] Aggressively removed \(toRemove) out-of-order packets")
        }
        
        if pendingClientBytes.count > 1 {
            let keepFirst = pendingClientBytes.first
            pendingClientBytes.removeAll()
            if let first = keepFirst {
                pendingClientBytes.append(first)
                pendingBytesTotal = first.count
            }
            log("[Trim] Cleared pending data except handshake")
        }
    }
    
    // 处理社交媒体中断
    private func trimBuffersForInterrupt() {
        // 保留最少的数据
        while outOfOrderPackets.count > 2 {
            outOfOrderPackets.removeFirst()
        }
        
        // 清理待发送数据（视频流可能不再需要）
        if pendingClientBytes.count > 2 {
            let keep = pendingClientBytes.prefix(2)
            pendingClientBytes = Array(keep)
            pendingBytesTotal = keep.reduce(0) { $0 + $1.count }
        }
    }

    private func handleClientRefusal() {
        // 差异化处理
        if isYouTubeConnection() {
            let youtubeMinBuffer = 48 * 1024
            recvBufferLimit = max(youtubeMinBuffer, recvBufferLimit / 2)
            log("[YouTube] Gentle shrink to \(recvBufferLimit)")
        } else if isSocialMediaConnection() {
            recvBufferLimit = max(SOCIAL_MIN_BUFFER, recvBufferLimit / 2)
            log("[Social] Gentle shrink to \(recvBufferLimit)")
        } else {
            log("[Refusal] Client refusing data - emergency shrink to \(MIN_BUFFER)")
            lastRefusalTime = Date()
            recvBufferLimit = MIN_BUFFER
            shrinkToMinimumImmediate()
        }
        
        if bufferedBytesForWindow() > recvBufferLimit {
            trimBuffersAggressively()
        }
        
        backpressure.markIdle()
        backpressure.consecutiveIdleCycles = 10
    }
    
    // MARK: - Constants
    public let tunnelMTU: Int
    private var mss: Int { max(536, tunnelMTU - 40) }
    private let MAX_WINDOW_SIZE: UInt16 = 65535
    private var DELAYED_ACK_TIMEOUT_MS: Int = 25
    private let MAX_BUFFERED_PACKETS = 24
    private let RETRANSMIT_TIMEOUT_MS: Int = 200
    private let MAX_RETRANSMIT_RETRIES = 3

    // MARK: - Connection Identity
    let key: String
    private let packetFlow: SendablePacketFlow
    private let sourceIP: IPv4Address
    private let sourcePort: UInt16
    private let destIP: IPv4Address
    public let destPort: UInt16
    public let destinationHost: String?

    // MARK: - TCP State
    private var synAckSent = false
    private var handshakeAcked = false
    private let serverInitialSequenceNumber: UInt32
    private let initialClientSequenceNumber: UInt32
    private var serverSequenceNumber: UInt32
    private var clientSequenceNumber: UInt32
    private var nextExpectedSequence: UInt32 = 0
    private var lastAckedSequence: UInt32 = 0

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

    // MARK: - Buffering
    private var outOfOrderPackets: [(seq: UInt32, data: Data)] = []
    private var pendingClientBytes: [Data] = []
    private var pendingBytesTotal: Int = 0
    private static let pendingSoftCapBytes = 32 * 1024

    // MARK: - SOCKS State
    private enum SocksState {
        case idle, connecting, greetingSent, methodOK, connectSent, established, closed
    }
    private var socksState: SocksState = .idle
    private var socksConnection: NWConnection?
    private var chosenTarget: SocksTarget?
    private var closeContinuation: CheckedContinuation<Void, Never>?

    private enum SocksTarget {
        case ip(IPv4Address, port: UInt16)
        case domain(String, port: UInt16)
    }

    // MARK: - Timers
    private let queue = DispatchQueue(label: "tcp.connection.timer", qos: .userInitiated)
    private var retransmitTimer: DispatchSourceTimer?
    private var delayedAckTimer: DispatchSourceTimer?
    private var retransmitRetries = 0
    private var delayedAckPacketsSinceLast: Int = 0

    // MARK: - Statistics
    private var duplicatePacketCount: Int = 0
    private var outOfOrderPacketCount: Int = 0
    private var retransmittedPackets: Int = 0
    private var overflowCount: Int = 0
    private var lastOverflowTime: Date?

    // MARK: - Flow Control
    private var availableWindow: UInt16 = 65535
    private var congestionWindow: UInt32 { 10 * UInt32(mss) }
    private var slowStartThreshold: UInt32 = 65535

    public var recvBufferLimit: Int
    private var lastAdvertisedWindow: UInt16 = 0
    
    public var onBytesBackToTunnel: ((Int) -> Void)?
    
    func setOnBytesBackToTunnel(_ cb: @escaping (Int) -> Void) {
        self.onBytesBackToTunnel = cb
    }
    
    public func getLastActivityTime() async -> Date? {
        return lastActivityTime
    }
    
    private let priority: ConnectionPriority
    public private(set) var keepAliveInterval: TimeInterval = 45.0
    private var lastKeepAliveSent: Date?
    
    public func getKeepAliveTelemetry() async -> (interval: TimeInterval, lastSent: Date?, lastActivity: Date?) {
        return (keepAliveInterval, lastKeepAliveSent, await getLastActivityTime())
    }

    private nonisolated func computeKeepAliveInterval(host: String?, port: UInt16) -> TimeInterval {
        switch (host, port) {
        case (_, 5223): return 180.0
        case (_, 5228): return 180.0
        case let (h, 443) where (h?.contains("apple.com") ?? false): return 120.0
        case (_, 443): return 80.0
        case (_, 80):  return 60.0
        default:       return 45.0
        }
    }

    // MARK: - YouTube & Social Media Detection
    public func isYouTubeConnection() -> Bool {
        guard let host = destinationHost?.lowercased() else { return false }
        return host.contains("youtube.com") ||
               host.contains("ytimg.com") ||
               host.contains("ggpht.com") ||
               host.contains("googlevideo.com") ||
               host.contains("googleusercontent.com")
    }
    
    public func isSocialMediaConnection() -> Bool {
        guard let host = destinationHost?.lowercased() else { return false }
        
        // Twitter/X
        if host.contains("twitter.com") || host.contains("twimg.com") ||
           host.contains("t.co") || host.contains("x.com") {
            return true
        }
        
        // Instagram
        if host.contains("instagram.com") || host.contains("cdninstagram.com") ||
           host.contains("fbcdn.net") {
            return true
        }
        
        // TikTok
        if host.contains("tiktok.com") || host.contains("tiktokcdn.com") ||
           host.contains("musical.ly") {
            return true
        }
        
        // Facebook
        if host.contains("facebook.com") || host.contains("fb.com") {
            return true
        }
        
        return false
    }
    
    private func optimizeForYouTube() {
        guard isYouTubeConnection() else { return }
        
        if recvBufferLimit < YOUTUBE_BUFFER {
            recvBufferLimit = YOUTUBE_BUFFER
            log("[YouTube] Optimized buffer to \(YOUTUBE_BUFFER) bytes")
        }
        
        DELAYED_ACK_TIMEOUT_MS = 10
        trafficPattern.isYouTubePattern = true
        
        updateAdvertisedWindow()
        if availableWindow < UInt16(lastAdvertisedWindow / 2) {
            sendPureAck()
        }
    }
    
    private func optimizeForSocialMedia() {
        guard isSocialMediaConnection() else { return }
        
        if trafficPattern.isScrollingPattern {
            adjustBufferForScrolling()
        } else if isVideoLoading() {
            adjustBufferForVideo()
        } else {
            adjustBufferForNormalBrowsing()
        }
    }
    
    private func adjustBufferForScrolling() {
        if recvBufferLimit > SOCIAL_SCROLL_BUFFER {
            recvBufferLimit = SOCIAL_SCROLL_BUFFER
            log("[Social] Scrolling detected, optimized to \(SOCIAL_SCROLL_BUFFER)")
        }
        DELAYED_ACK_TIMEOUT_MS = 5
    }
    
    private func adjustBufferForVideo() {
        if recvBufferLimit < SOCIAL_VIDEO_BUFFER {
            recvBufferLimit = SOCIAL_VIDEO_BUFFER
            log("[Social] Video loading, expanded to \(SOCIAL_VIDEO_BUFFER)")
        }
    }
    
    private func adjustBufferForNormalBrowsing() {
        if recvBufferLimit != SOCIAL_NORMAL_BUFFER {
            recvBufferLimit = SOCIAL_NORMAL_BUFFER
            log("[Social] Normal browsing, set to \(SOCIAL_NORMAL_BUFFER)")
        }
    }
    
    private func isVideoLoading() -> Bool {
        return trafficPattern.largePayloadCount > 2 ||
               backpressure.recentDataRate > Double(mss * 20)
    }
    
    // TCPConnection.swift - 增强滚动检测
    private func detectScrollingPattern() -> Bool {
        // 检查最近的数据模式
        guard backpressure.recentDataPoints.count >= 5 else { return false }
        
        let recent = backpressure.recentDataPoints.suffix(5)
        let sizes = recent.map { $0.1 }
        let avgSize = sizes.reduce(0, +) / sizes.count
        
        // 滚动特征：小包频繁（图片预览）
        let isSmallPackets = avgSize < 10 * 1024
        let isFrequent = recent.last!.0.timeIntervalSince(recent.first!.0) < 1.0
        
        if isSmallPackets && isFrequent {
            log("[Social] Scrolling pattern detected")
            return true
        }
        
        return false
    }
    
    // 处理用户中断（滚动离开）
    func handleUserInterrupt() {
        trafficPattern.interruptCount += 1
        trafficPattern.lastInterruptTime = Date()
        
        // 检测滚动
        if detectScrollingPattern() {
            trafficPattern.isScrollingPattern = true
            let targetSize = SOCIAL_SCROLL_BUFFER
            if recvBufferLimit > targetSize {
                recvBufferLimit = targetSize
                trimBuffersForInterrupt()
                log("[Social] Quick shrink on scroll to \(targetSize)")
            }
        }
    }
    
    // 快速恢复机制
    private func quickRecoveryAfterInterrupt() {
        if !trafficPattern.isScrollingPattern &&
           Date().timeIntervalSince(trafficPattern.lastInterruptTime ?? Date()) > 0.5 {
            
            if recvBufferLimit < SOCIAL_NORMAL_BUFFER {
                expandBufferImmediate(to: SOCIAL_NORMAL_BUFFER)
                log("[Social] Quick recovery after scroll stop")
            }
            
            if Date().timeIntervalSince(trafficPattern.lastInterruptTime ?? Date()) > 2.0 {
                trafficPattern.interruptCount = 0
                trafficPattern.isScrollingPattern = false
            }
        }
    }
    
    private func predictNextContentType() -> ContentType {
        if trafficPattern.largePayloadCount > trafficPattern.smallPayloadCount * 2 {
            return .video
        } else if trafficPattern.smallPayloadCount > 10 {
            return .images
        } else {
            return .mixed
        }
    }
    
    private func preemptiveOptimization() {
        let predicted = predictNextContentType()
        
        switch predicted {
        case .video:
            if isYouTubeConnection() && recvBufferLimit < YOUTUBE_BUFFER {
                expandBufferImmediate(to: YOUTUBE_BUFFER)
            } else if isSocialMediaConnection() && recvBufferLimit < SOCIAL_VIDEO_BUFFER {
                expandBufferImmediate(to: SOCIAL_VIDEO_BUFFER)
            }
        case .images:
            if recvBufferLimit != SOCIAL_NORMAL_BUFFER {
                recvBufferLimit = SOCIAL_NORMAL_BUFFER
            }
        case .mixed, .text:
            adaptBufferToFlow()
        }
    }
    
    private func warmupConnection() async {
        guard isWarmingUp else { return }
        
        let age = Date().timeIntervalSince(connectionStartTime ?? Date())
        
        if isYouTubeConnection() {
            if age < 3.0 && recvBufferLimit < YOUTUBE_BUFFER {
                expandBufferImmediate(to: YOUTUBE_BUFFER)
                log("[YouTube Warmup] Quick expansion to: \(YOUTUBE_BUFFER) bytes")
            }
        } else if isSocialMediaConnection() {
            if age < 3.0 && recvBufferLimit < SOCIAL_NORMAL_BUFFER {
                expandBufferImmediate(to: SOCIAL_NORMAL_BUFFER)
                log("[Social Warmup] Quick expansion to: \(SOCIAL_NORMAL_BUFFER) bytes")
            }
        } else {
            if age < 3.0 && recvBufferLimit < 128 * 1024 {
                let target = min(128 * 1024, recvBufferLimit * 2)
                expandBufferImmediate(to: target)
                log("[Warmup] Quick expansion: \(recvBufferLimit) bytes")
            }
        }
        
        if age > 5.0 {
            isWarmingUp = false
        }
    }
    
    
    private func getMinBufferSize() -> Int {
        if isYouTubeConnection() {
            return 48 * 1024
        } else if isSocialMediaConnection() {
            return SOCIAL_MIN_BUFFER
        } else {
            return MIN_BUFFER
        }
    }
    
    // TCPConnection.swift - 添加防抖动属性
    private var lastExpansionTime: Date?
    private var lastShrinkTime: Date?
    private var expansionCooldown: TimeInterval {
        if isYouTubeConnection() {
            return 0.5  // YouTube: 500ms冷却
        } else {
            return 0.3  // 其他: 300ms冷却
        }
    }

    
    
    // MARK: - 核心改进：智能扩展
    private func shouldExpandBuffer() -> Bool {
        // 检查扩展冷却期
        if let lastExpansion = lastExpansionTime {
            let timeSinceExpansion = Date().timeIntervalSince(lastExpansion)
            if timeSinceExpansion < expansionCooldown {
                log("[Throttle] Expansion blocked: \(Int((expansionCooldown - timeSinceExpansion) * 1000))ms cooldown remaining")
                return false
            }
        }
        
        // 检查收缩后的稳定期
        if let lastShrink = lastShrinkTime {
            let timeSinceShrink = Date().timeIntervalSince(lastShrink)
            if timeSinceShrink < 0.2 {  // 收缩后200ms内不扩展
                return false
            }
        }
        
        // 必须有实际流量
        guard isReallyActive() else { return false }
        
        // 必须有近期数据（100ms内）
        guard Date().timeIntervalSince(backpressure.lastDataReceivedTime) < 0.1 else { return false }
        
        // 必须有实际使用需求
        let utilizationRate = Double(bufferedBytesForWindow()) / Double(recvBufferLimit)
        guard utilizationRate > 0.7 else { return false }
        
        // 必须有增长趋势或溢出
        return backpressure.growthTrend > 0.1 || backpressure.overflowCount > 0
    }
    
    private func intelligentExpand() {
        guard shouldExpandBuffer() else { return }
            
            let targetSize: Int
            if backpressure.overflowCount > 0 {
                // 有溢出：激进扩展
                if isYouTubeConnection() {
                    targetSize = min(Self.YOUTUBE_MAX_BUFFER, recvBufferLimit * 2)  // 使用 Self.
                } else {
                    targetSize = min(Self.MAX_BUFFER, recvBufferLimit * 3)  // 使用 Self.
                }
            } else if backpressure.growthTrend > 0.3 {
                // 高增长：适度扩展
                if isYouTubeConnection() {
                    targetSize = min(Self.YOUTUBE_MAX_BUFFER, recvBufferLimit + Self.YOUTUBE_EXPAND_STEP)  // 使用 Self.
                } else {
                    targetSize = min(Self.MAX_BUFFER, recvBufferLimit * 2)  // 使用 Self.
                }
            } else {
                // 温和扩展
                targetSize = min(Self.MAX_BUFFER, recvBufferLimit + 32 * 1024)  // 使用 Self.
            }
            
            if targetSize > recvBufferLimit {
                recvBufferLimit = targetSize
                lastExpansionTime = Date()
                updateAdvertisedWindow()
                maybeSendWindowUpdate(reason: "intelligent-expand")
                log("[Expand] Based on actual demand to \(targetSize)")
            }
    }
    
    // 核心改进：精准流量感知
    private func classifyTraffic(payload: Data) {
        // 检测Keep-Alive包（通常很小）
        if payload.count <= 1 {
            trafficType.isKeepAlive = true
            trafficType.lastKeepAliveTime = Date()
        } else {
            trafficType.isActualData = true
            trafficType.lastActualDataTime = Date()
            
            // 只有实际数据才更新背压指标
            backpressure.recordDataReceived(payload.count)
        }
    }
    
    private func isReallyActive() -> Bool {
        // 必须有实际数据，不只是Keep-Alive
        guard let lastData = trafficType.lastActualDataTime else { return false }
        return Date().timeIntervalSince(lastData) < 0.5
    }
    
    // MARK: - 核心改进：零延迟恢复
    private func handleBufferOverflow() {
        
        #if DEBUG
        logBufferState("Before Overflow Handling")  // 溢出前状态
        #endif
        
        backpressure.recordOverflow()
        overflowCount += 1
        lastOverflowTime = Date()
        
        // 立即扩容（不等待）
        let emergency = min(MAX_BUFFER, recvBufferLimit * 3)
        if emergency > recvBufferLimit {
            let oldSize = recvBufferLimit
            recvBufferLimit = emergency
            updateAdvertisedWindow()
            
            // 立即发送窗口更新
            sendPureAck()
            lastAdvertisedWindow = availableWindow
            
            log("[EMERGENCY] Overflow detected! Immediate expand: \(oldSize) -> \(emergency)")
        }
    }
    
    // MARK: - 高频监控任务（50ms）
    private func startHighFrequencyMonitor() {
        highFrequencyMonitorTask?.cancel()
        highFrequencyMonitorTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 50_000_000)  // 50ms
                guard let self = self else { break }
                
                await self.rapidAdjustment()
            }
        }
    }
    
    private func rapidAdjustment() {
        let now = Date()
        let idleTime = now.timeIntervalSince(backpressure.lastDataReceivedTime)
        let usage = bufferedBytesForWindow()
        let rate = Double(usage) / Double(recvBufferLimit)
        
        // 修正：确保100ms判断生效
        if idleTime > 0.1 {  // 100ms
            if rate < 0.1 && recvBufferLimit > getMinBufferSize() {
                let minSize = getMinBufferSize()
                recvBufferLimit = minSize
                updateAdvertisedWindow()
                log("[FastShrink] 100ms idle detected, shrunk to \(minSize)")
                return
            }
        }
        
        // 预防性扩容应该更保守
        if rate > 0.9 && isReallyActive() && backpressure.growthTrend > 0.3 {
            let target = min(MAX_BUFFER, recvBufferLimit + 16 * 1024)  // 渐进式增长
            if target > recvBufferLimit {
                recvBufferLimit = target
                updateAdvertisedWindow()
                log("[Preventive] Buffer expanded by 16KB to \(target)")
            }
        }
    }

    // MARK: - Initialization
    init(
        key: String,
        packetFlow: SendablePacketFlow,
        sourceIP: IPv4Address,
        sourcePort: UInt16,
        destIP: IPv4Address,
        destPort: UInt16,
        destinationHost: String?,
        initialSequenceNumber: UInt32,
        tunnelMTU: Int = 1400,
        recvBufferLimit: Int = 16 * 1024,
        priority: ConnectionPriority = .normal,
        onBytesBackToTunnel: ((Int) -> Void)? = nil
    ) {
        // 先初始化所有存储属性
        self.key = key
        self.packetFlow = packetFlow
        self.sourceIP = sourceIP
        self.sourcePort = sourcePort
        self.destIP = destIP
        self.destPort = destPort
        self.destinationHost = destinationHost
        self.priority = priority
        self.tunnelMTU = tunnelMTU
        self.connectionStartTime = Date()
        self.onBytesBackToTunnel = onBytesBackToTunnel
        
        // 初始化序列号
        let initialServerSeq = arc4random()
        self.serverSequenceNumber = initialServerSeq
        self.serverInitialSequenceNumber = initialServerSeq
        self.initialClientSequenceNumber = initialSequenceNumber
        self.clientSequenceNumber = initialSequenceNumber
        self.nextExpectedSequence = initialSequenceNumber
        self.lastAckedSequence = initialSequenceNumber
        
        // 计算智能缓冲区大小（使用静态方法）
        let smartBufferSize = Self.calculateInitialBufferSize(
            host: destinationHost,
            port: destPort,
            baseSize: recvBufferLimit
        )
        
        let cap = max(8 * 1024, min(smartBufferSize, Int(MAX_WINDOW_SIZE)))
        
        // 初始化缓冲区相关属性
        self.recvBufferLimit = cap
        self.availableWindow = UInt16(cap)
        self.lastAdvertisedWindow = UInt16(cap)
        
        // 计算 keep-alive 间隔
        let interval: TimeInterval
        switch (destinationHost, destPort) {
        case (_, 5223), (_, 5228):
            interval = 180.0
        case let (host, 443) where host?.contains("apple.com") ?? false:
            interval = 120.0
        case (_, 443):
            interval = 60.0
        case (_, 80):
            interval = 30.0
        default:
            interval = 45.0
        }
        self.keepAliveInterval = interval
        
        NSLog("[TCPConnection \(key)] Initialized. Buffer: \(cap)")
    }
    
    // 静态方法用于计算初始缓冲区大小
    private static func calculateInitialBufferSize(host: String?, port: UInt16, baseSize: Int) -> Int {
        guard let h = host?.lowercased() else {
            return port == 443 ? 64 * 1024 : baseSize
        }
        
        // YouTube相关域名 - 大缓冲区
        if h.contains("youtube.com") || h.contains("ytimg.com") ||
           h.contains("ggpht.com") || h.contains("googlevideo.com") ||
           h.contains("googleusercontent.com") {
            return YOUTUBE_BUFFER
        }
        
        // 社交媒体
        if h.contains("twitter") || h.contains("instagram") ||
           h.contains("facebook") || h.contains("tiktok") ||
           h.contains("x.com") || h.contains("twimg") {
            return SOCIAL_NORMAL_BUFFER
        }
        
        // 其他视频服务
        if h.contains("netflix") || h.contains("hulu") ||
           h.contains("twitch") || h.contains("vimeo") {
            return 128 * 1024
        }
        
        // CDN
        if h.contains("cloudfront") || h.contains("akamai") ||
           h.contains("fastly") || h.contains("cloudflare") {
            return 80 * 1024
        }
        
        // 基于端口的默认值
        switch port {
        case 5223, 5228:
            return 32 * 1024
        case 443:
            return 64 * 1024
        case 80:
            return 48 * 1024
        default:
            return baseSize
        }
    }
    
    deinit {
        highFrequencyMonitorTask?.cancel()
    }

    // 当前用于接收重组的已占用字节数
    @inline(__always)
    private func bufferedBytesForWindow() -> Int {
        var used = outOfOrderPackets.reduce(0) { $0 + $1.data.count }
        if socksState != .established {
            used += pendingBytesTotal
        }
        return used
    }

    // 依据缓冲占用更新 advertised window
    @inline(__always)
    private func updateAdvertisedWindow() {
        let cap = min(recvBufferLimit, Int(MAX_WINDOW_SIZE))
        let used = bufferedBytesForWindow()
        let free = max(0, cap - used)
        self.availableWindow = UInt16(free)
    }

    // 若窗口显著增长，主动发 Window Update
    @inline(__always)
    private func maybeSendWindowUpdate(reason: String) {
        let prev = lastAdvertisedWindow
        updateAdvertisedWindow()
        let nowWnd = availableWindow
        
        // 修正：窗口增长超过MSS或从0恢复时才更新
        let grewFromZero = (prev == 0 && nowWnd > 0)
        let grewSignificantly = nowWnd > prev && Int(nowWnd - prev) >= mss
        
        if grewFromZero || grewSignificantly {
            sendPureAck()
            lastAdvertisedWindow = nowWnd
            log("Window update (\(reason)): \(prev) -> \(nowWnd) bytes")
        }
    }
    
    // TCPConnection.swift - 更精确的增长趋势计算
    // TCPConnection.swift - 修正 calculateGrowthTrend 方法
    private func calculateGrowthTrend() -> Double {
        guard backpressure.recentDataPoints.count >= 10 else { return 0 }
            
        // 使用移动平均
        let windowSize = 5
        let older = backpressure.recentDataPoints.prefix(windowSize)
        let newer = backpressure.recentDataPoints.suffix(windowSize)
        
        let olderAvg = Double(older.map { $0.1 }.reduce(0, +)) / Double(windowSize)
        let newerAvg = Double(newer.map { $0.1 }.reduce(0, +)) / Double(windowSize)
        
        guard olderAvg > 0 else { return 0 }
        
        let trend = (newerAvg - olderAvg) / olderAvg
        
        // 平滑处理，避免过度反应
        return min(1.0, max(-0.5, trend))
    }

    // MARK: - Lifecycle
    func start() async {
        guard socksConnection == nil else { return }
        
        // 在 start 时执行优化（init 中不能调用 actor-isolated 方法）
        if isYouTubeConnection() {
            optimizeForYouTube()
        } else if isSocialMediaConnection() {
            optimizeForSocialMedia()
        }
        
        // 启动高频监控
        startHighFrequencyMonitor()
        
        // 启动预热
        Task {
            await warmupConnection()
        }
        
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            self.closeContinuation = continuation
            let tcpOptions = NWProtocolTCP.Options()
            tcpOptions.noDelay = true
            let conn = NWConnection(
                host: NWEndpoint.Host("127.0.0.1"),
                port: NWEndpoint.Port(integerLiteral: 8888),
                using: {
                    let p = NWParameters(tls: nil, tcp: tcpOptions)
                    p.requiredInterfaceType = .loopback
                    p.allowLocalEndpointReuse = true
                    return p
                }()
            )
            self.socksConnection = conn
            self.socksState = .connecting

            conn.stateUpdateHandler = { [weak self] st in
                Task { await self?.handleStateUpdate(st) }
            }

            log("Starting connection to SOCKS proxy...")
            conn.start(queue: .global(qos: .userInitiated))
        }
    }

    func close() {
        guard socksState != .closed else { return }
        
        socksState = .closed
        cancelKeepAliveTimer()
        highFrequencyMonitorTask?.cancel()
        outOfOrderPackets.removeAll(keepingCapacity: false)
        pendingClientBytes.removeAll(keepingCapacity: false)
        socksConnection?.forceCancel()
        socksConnection = nil
        closeContinuation?.resume()
        closeContinuation = nil
        log("Closing connection.")
    }

    // MARK: - Sequence helpers
    @inline(__always) private func seqDiff(_ a: UInt32, _ b: UInt32) -> Int32 {
        Int32(bitPattern: a &- b)
    }
    @inline(__always) private func seqLT(_ a: UInt32, _ b: UInt32) -> Bool { seqDiff(a, b) < 0 }
    @inline(__always) private func seqLE(_ a: UInt32, _ b: UInt32) -> Bool { seqDiff(a, b) <= 0 }

    // MARK: - Packet Handling
    func handlePacket(payload: Data, sequenceNumber: UInt32) {
        guard !payload.isEmpty else { return }

        // 分类流量（Keep-Alive vs 实际数据）
        classifyTraffic(payload: payload)
        
        // 检测是否需要紧急收缩
        if detectClientRefusal() {
            handleClientRefusal()
            return
        }
        
        updateLastActivity()
        
        // 更新流量模式
        updateTrafficPattern(payloadSize: payload.count)
        
        // Start SOCKS connection if needed
        if socksConnection == nil && socksState == .idle {
            Task { await self.start() }
        }

        // Initialize sequence tracking on first data packet
        if !handshakeAcked {
            initializeSequenceTracking(sequenceNumber)
        }

        // Wrap-around safe decision
        let d = seqDiff(sequenceNumber, nextExpectedSequence)

        if d == 0 {
            processInOrderPacket(payload: payload, sequenceNumber: sequenceNumber)
        } else if d > 0 {
            processOutOfOrderPacket(payload: payload, sequenceNumber: sequenceNumber)
        } else if d >= -Int32(payload.count) {
            processOverlappingPacket(payload: payload, sequenceNumber: sequenceNumber)
        } else {
            processDuplicatePacket(sequenceNumber: sequenceNumber)
        }
    }
    
    private func updateTrafficPattern(payloadSize: Int) {
        trafficPattern.requestCount += 1
        trafficPattern.lastRequestTime = Date()
        trafficPattern.connectionAge = Date().timeIntervalSince(connectionStartTime ?? Date())
        
        // 分类请求大小
        if payloadSize < 10 * 1024 {
            trafficPattern.smallPayloadCount += 1
        } else if payloadSize < 100 * 1024 {
            trafficPattern.mediumPayloadCount += 1
        } else {
            trafficPattern.largePayloadCount += 1
        }
        
        trafficPattern.analyze()
        trafficPattern.detectSocialMediaBehavior()
        
        // 增强的批量检测
        if trafficPattern.smallPayloadCount > 5 &&
           Date().timeIntervalSince(trafficPattern.lastRequestTime) < 2.0 {
            batchDetection.recordBatchRequest()
            
            // 只有确认的批量模式才扩展
            if batchDetection.shouldTriggerExpansion {
                if isYouTubeConnection() || trafficPattern.isYouTubePattern {
                    log("[YouTube] Confirmed batch pattern (count: \(batchDetection.consecutiveBatchRequests))")
                    
                    #if DEBUG
                    logBufferState("Batch Pattern Detected")  // 批量模式检测
                    #endif
                    expandForConfirmedBatch()
                }
            }
        } else if Date().timeIntervalSince(trafficPattern.lastRequestTime) > 3.0 {
            // 超过3秒没有批量请求，重置
            batchDetection.reset()
        }
    }
    
    
    // 新增：临时延长收缩阈值
    private var temporaryShrinkExtension: Date?

    
    // TCPConnection.swift - 批量检测相关属性
    private var temporaryShrinkExtensionTime: Date?

    // 临时延长收缩阈值的方法
    private func temporarilyExtendShrinkThreshold() {
        temporaryShrinkExtensionTime = Date().addingTimeInterval(3.0)  // 3秒内使用更长的收缩时间
        log("[Batch] Extending shrink threshold for 3 seconds")
    }

    // 获取实际的快速收缩阈值
    private var actualFastShrinkThreshold: TimeInterval {
        // 检查是否在延长期内
        if let extendTime = temporaryShrinkExtensionTime {
            if Date() < extendTime {
                return 0.5  // 批量模式期间，500ms才收缩
            } else {
                // 过期了，清理
                temporaryShrinkExtensionTime = nil
            }
        }
        
        // 返回正常的阈值
        return FAST_SHRINK_THRESHOLD
    }

    // 在 checkAndShrinkImmediate 方法中使用 actualFastShrinkThreshold
    private func checkAndShrinkImmediate() {
        let idleTime = Date().timeIntervalSince(backpressure.lastDataReceivedTime)
        
        if idleTime > actualFastShrinkThreshold {
            let usage = bufferedBytesForWindow()
            let utilizationRate = Double(usage) / Double(recvBufferLimit)
            
            if utilizationRate < 0.1 && recvBufferLimit > getMinBufferSize() {
                let minSize = getMinBufferSize()
                
                // YouTube特殊处理：分步收缩
                if isYouTubeConnection() && recvBufferLimit > Self.YOUTUBE_BUFFER {  // 使用 Self.
                    // 先收缩到192KB
                    recvBufferLimit = Self.YOUTUBE_BUFFER
                    log("[YouTube-Shrink] Step 1: shrink to \(Self.YOUTUBE_BUFFER)")
                } else {
                    recvBufferLimit = minSize
                    log("[FastShrink] \(Int(idleTime * 1000))ms idle, shrink to \(minSize)")
                }
                
                updateAdvertisedWindow()
                return
            }
        }
        
        if idleTime > NORMAL_SHRINK_THRESHOLD && recvBufferLimit > getMinBufferSize() * 2 {
            let minSize = getMinBufferSize()
            recvBufferLimit = max(minSize, recvBufferLimit / 2)
            updateAdvertisedWindow()
            log("[Shrink] \(Int(idleTime * 1000))ms idle, shrink to \(recvBufferLimit)")
        }
    }
    

        
        // 创建一个 Sendable 的状态结构体
        public struct BufferState: Sendable {
            public let bufferSize: Int
            public let usage: Int
            public let utilizationRate: Double
            public let overflowCount: Int
            public let batchCount: Int
            public let isYouTube: Bool
            public let isSocialMedia: Bool
            public let connectionType: String
            
            
        }
    
    
    
    // 添加详细的缓冲区状态日志方法
    private func logBufferState(_ context: String) {
        #if DEBUG
        let usage = bufferedBytesForWindow()
        let utilizationRate = Double(usage) * 100.0 / Double(recvBufferLimit)
        
        let stats = """
        [\(context)] Buffer State:
        - Current: \(recvBufferLimit) bytes
        - Usage: \(usage)/\(recvBufferLimit) (\(String(format: "%.1f%%", utilizationRate)))
        - Last Expansion: \(lastExpansionTime.map { String(format: "%.1fs ago", -$0.timeIntervalSinceNow) } ?? "never")
        - Last Shrink: \(lastShrinkTime.map { String(format: "%.1fs ago", -$0.timeIntervalSinceNow) } ?? "never")
        - Batch Count: \(batchDetection.consecutiveBatchRequests)
        - Batch Confirmed: \(batchDetection.isConfirmedBatch)
        - Overflow: \(backpressure.overflowCount)
        - Growth Trend: \(String(format: "%.2f", backpressure.growthTrend))
        - Connection Type: \(getConnectionTypeString())
        """
        log(stats)
        #endif
    }
    
    // 辅助方法：获取连接类型字符串
    private func getConnectionTypeString() -> String {
        if isYouTubeConnection() {
            return "YouTube"
        } else if isSocialMediaConnection() {
            return "Social Media"
        } else if destPort == 5223 || destPort == 5228 {
            return "Push Service"
        } else {
            return "General"
        }
    }

    // 完整的 expandForConfirmedBatch 方法
    private func expandForConfirmedBatch() {
        // 检查冷却期
        if let lastTime = lastExpansionTime,
           Date().timeIntervalSince(lastTime) < expansionCooldown {
            return
        }
        
        let targetSize: Int
        if isYouTubeConnection() {
            // YouTube批量模式：直接扩展到较大值
            if recvBufferLimit < Self.YOUTUBE_BUFFER {  // ✅ 使用 Self.
                targetSize = Self.YOUTUBE_BUFFER
            } else if recvBufferLimit < Self.YOUTUBE_MAX_BUFFER {  // ✅ 使用 Self.
                targetSize = min(Self.YOUTUBE_MAX_BUFFER, recvBufferLimit + Self.YOUTUBE_EXPAND_STEP)  // ✅ 使用 Self.
            } else {
                return
            }
            
            expandBufferImmediate(to: targetSize)
            lastExpansionTime = Date()
            log("[YouTube-Batch] Confirmed batch expansion to \(targetSize)")
            
        #if DEBUG
        logBufferState("Batch Expansion")  // 添加详细日志
        #endif
            
            // 批量模式下，临时延长收缩时间
            temporarilyExtendShrinkThreshold()
        }
    }
    
    // TCPConnection.swift - 添加防重复检测
    private var lastBatchExpansionTime: Date?
    private var batchExpansionCount: Int = 0
    
    
    private func expandForBatchRequests() {
        // 防止重复扩展
        if let lastTime = lastBatchExpansionTime,
           Date().timeIntervalSince(lastTime) < 2.0 {
            return
        }
        
        let targetSize: Int
        
        if trafficPattern.isYouTubePattern || isYouTubeConnection() {
            // YouTube渐进式扩展
            if recvBufferLimit < 96 * 1024 {
                targetSize = 96 * 1024
            } else if recvBufferLimit < Self.YOUTUBE_BUFFER {  // ✅ 使用 Self.
                targetSize = Self.YOUTUBE_BUFFER
            } else if recvBufferLimit < Self.YOUTUBE_MAX_BUFFER {  // ✅ 使用 Self.
                // 允许扩展到256KB
                targetSize = min(Self.YOUTUBE_MAX_BUFFER, recvBufferLimit + Self.YOUTUBE_EXPAND_STEP)  // ✅ 使用 Self.
            } else {
                return  // 已达到最大值
            }
            log("[YouTube] Batch mode detected, expanding to \(targetSize)")
        } else if trafficPattern.isSocialMediaPattern || isSocialMediaConnection() {
            targetSize = Self.SOCIAL_NORMAL_BUFFER  // ✅ 使用 Self.
        } else {
            targetSize = min(128 * 1024, recvBufferLimit * 2)
        }
        
        if targetSize > recvBufferLimit {
            expandBufferImmediate(to: targetSize)
            lastBatchExpansionTime = Date()
            batchExpansionCount += 1
        }
    }
    
    private func evaluateBufferExpansion() {
        // 检查是否在拒绝恢复期
        if let refusalTime = lastRefusalTime {
            let recoveryTime = isYouTubeConnection() ? 2.0 : (isSocialMediaConnection() ? 3.0 : 5.0)
            let timeSinceRefusal = Date().timeIntervalSince(refusalTime)
            if timeSinceRefusal < recoveryTime {
                log("[Backpressure] Expansion blocked: \(recoveryTime - timeSinceRefusal)s until recovery")
                return
            }
        }
        
        // 使用智能扩展
        intelligentExpand()
    }
    
    private func expandBufferImmediate(to size: Int) {
        let oldSize = recvBufferLimit
        recvBufferLimit = size
        updateAdvertisedWindow()
        maybeSendWindowUpdate(reason: "immediate-expand")
        log("[Backpressure] Buffer expanded: \(oldSize) -> \(size) bytes")
        #if DEBUG
           logBufferState("After Expansion")  // 添加详细日志
        #endif
    }

    private func shrinkBufferImmediate() {
        let minSize = getMinBufferSize()
        if recvBufferLimit > minSize {
            let oldSize = recvBufferLimit
            recvBufferLimit = minSize
            lastShrinkTime = Date()  // 记录收缩时间
            
            if bufferedBytesForWindow() > minSize {
                trimBuffersToFit()
            }
            
            updateAdvertisedWindow()
            log("[Backpressure] Buffer shrunk: \(oldSize) -> \(minSize) bytes")
            
        #if DEBUG
        logBufferState("After Shrink")  // 添加详细日志
        #endif
        }
    }
    
    // TCPConnection.swift - 增强批量检测
    private struct BatchDetection {
        var consecutiveBatchRequests: Int = 0
        var firstBatchTime: Date?
        var lastBatchTime: Date?
        var isConfirmedBatch: Bool = false
        
        mutating func recordBatchRequest() {
            let now = Date()
            
            if let last = lastBatchTime,
               now.timeIntervalSince(last) > 2.0 {
                // 超过2秒，重置计数
                reset()
            }
            
            if firstBatchTime == nil {
                firstBatchTime = now
            }
            
            lastBatchTime = now
            consecutiveBatchRequests += 1
            
            // 连续3次批量请求才确认
            if consecutiveBatchRequests >= 3 {
                isConfirmedBatch = true
            }
        }
        
        mutating func reset() {
            consecutiveBatchRequests = 0
            firstBatchTime = nil
            lastBatchTime = nil
            isConfirmedBatch = false
        }
        
        var shouldTriggerExpansion: Bool {
            return isConfirmedBatch &&
                   consecutiveBatchRequests >= 3 &&
                   (lastBatchTime?.timeIntervalSince(firstBatchTime ?? Date()) ?? 0) < 2.0
        }
    }
    
    private var batchDetection = BatchDetection()

    private func trimBuffersToFit() {
        while bufferedBytesForWindow() > recvBufferLimit && !outOfOrderPackets.isEmpty {
            outOfOrderPackets.removeFirst()
        }
    }

    private func initializeSequenceTracking(_ sequenceNumber: UInt32) {
        handshakeAcked = true
        nextExpectedSequence = sequenceNumber
        clientSequenceNumber = sequenceNumber
        lastAckedSequence = sequenceNumber
        log("Handshake completed, tracking from seq: \(sequenceNumber)")
    }

    private func processInOrderPacket(payload: Data, sequenceNumber: UInt32) {
        // 只有实际数据才记录背压
        if trafficType.isActualData {
            backpressure.recordDataReceived(payload.count)
            // 更新增长趋势 - 使用新的计算方法
            backpressure.growthTrend = calculateGrowthTrend()
        }
        
        nextExpectedSequence = sequenceNumber &+ UInt32(payload.count)
        clientSequenceNumber = nextExpectedSequence
        lastAckedSequence = nextExpectedSequence

        cancelRetransmitTimer()

        // 检查是否需要立即扩展缓冲区
        evaluateBufferExpansion()
        
        // Forward data
        if socksState == .established {
            sendRawToSocks(payload)
        } else {
            appendToPending(payload)
        }

        // Process any buffered packets that are now in order
        processBufferedPackets()
        
        // 预测性扩展
        if backpressure.growthTrend > 0.2 {
            let predictedNeed = Int(Double(recvBufferLimit) * (1 + backpressure.growthTrend))
            let targetSize = min(MAX_BUFFER, predictedNeed)
            let threshold = recvBufferLimit + (recvBufferLimit / 2)
            if targetSize > threshold {
                expandBufferImmediate(to: targetSize)
                log("[Predictive] Buffer expanded based on growth trend: \(backpressure.growthTrend)")
            }
        }

        // ACK policy
        if isYouTubeConnection() || isSocialMediaConnection() ||
           trafficPattern.isBatchPattern || payload.count <= 64 {
            cancelDelayedAckTimer()
            sendPureAck()
        } else {
            scheduleDelayedAck()
        }
    }
    
    
    private func processOutOfOrderPacket(payload: Data, sequenceNumber: UInt32) {
        outOfOrderPacketCount += 1

        bufferOutOfOrderPacket(payload: payload, sequenceNumber: sequenceNumber)

        cancelDelayedAckTimer()
        if sackEnabled && peerSupportsSack {
            sendAckWithSack()
        } else {
            sendDuplicateAck()
        }

        if retransmitTimer == nil {
            startRetransmitTimer()
        }
    }

    private func processOverlappingPacket(payload: Data, sequenceNumber: UInt32) {
        let overlapU32 = nextExpectedSequence &- sequenceNumber
        let overlap = Int(min(UInt32(payload.count), overlapU32))

        if overlap >= payload.count {
            processDuplicatePacket(sequenceNumber: sequenceNumber)
        } else {
            let newDataStart = overlap
            let newData = payload[newDataStart...]
            let adjustedSeq = sequenceNumber &+ UInt32(newDataStart)

            if adjustedSeq == nextExpectedSequence {
                processInOrderPacket(payload: Data(newData), sequenceNumber: adjustedSeq)
            } else {
                processOutOfOrderPacket(payload: Data(newData), sequenceNumber: adjustedSeq)
            }
        }
    }

    private func processDuplicatePacket(sequenceNumber: UInt32) {
        duplicatePacketCount += 1

        if duplicatePacketCount > 5 && recvBufferLimit > MIN_BUFFER {
            NSLog("[Backpressure] Multiple duplicate ACKs, client may be refusing data")
            shrinkToMinimumImmediate()
        }
        
        cancelDelayedAckTimer()
        sendPureAck()

        if duplicatePacketCount % 10 == 0 {
            log("Excessive duplicates: \(duplicatePacketCount) total, last seq: \(sequenceNumber)")
        }
    }

    // 预检查空间（90%预防性扩容）
    private func bufferOutOfOrderPacket(payload: Data, sequenceNumber: UInt32) {
        // 预检查空间（90%预防性扩容）
        let willUse = bufferedBytesForWindow() + payload.count
        if willUse > recvBufferLimit * 9 / 10 {
            handleBufferOverflow()
        }
        
        // Check if already buffered
        if outOfOrderPackets.contains(where: { $0.seq == sequenceNumber }) { return }

        // Insert in sorted order
        let packet = (seq: sequenceNumber, data: payload)
        let index = outOfOrderPackets.binarySearch { seqLT($0.seq, sequenceNumber) }
        outOfOrderPackets.insert(packet, at: index)

        // 如果还是满了
        while outOfOrderPackets.count > MAX_BUFFERED_PACKETS {
            outOfOrderPackets.removeFirst()
            handleBufferOverflow()  // 每次溢出都尝试扩容
            log("Buffer overflow: dropped packet and expanded")
        }
        
        // 检查总缓冲区使用情况
        if bufferedBytesForWindow() > recvBufferLimit {
            handleBufferOverflow()
            trimBuffers()
            log("Buffer exceeded limit, trimming to \(recvBufferLimit)")
        }
        
        updateAdvertisedWindow()
    }

    private func processBufferedPackets() {
        var processed = 0

        while !outOfOrderPackets.isEmpty {
            let packet = outOfOrderPackets[0]

            if packet.seq == nextExpectedSequence {
                outOfOrderPackets.removeFirst()

                nextExpectedSequence = packet.seq &+ UInt32(packet.data.count)
                clientSequenceNumber = nextExpectedSequence

                if socksState == .established {
                    sendRawToSocks(packet.data)
                } else {
                    appendToPending(packet.data)
                }

                processed += 1
            } else if seqLT(packet.seq, nextExpectedSequence) {
                outOfOrderPackets.removeFirst()
            } else {
                break
            }
        }

        if processed > 0 {
            updateAdvertisedWindow()
            cancelDelayedAckTimer()
            sendPureAck()

            if outOfOrderPackets.isEmpty {
                cancelRetransmitTimer()
            }
            
            adaptBufferToFlow()
        }
        
        if outOfOrderPackets.count > 10 && duplicatePacketCount > 5 {
            handleClientRefusal()
        }
    }

    // MARK: - ACK Management
    private func scheduleDelayedAck() {
        delayedAckPacketsSinceLast += 1

        let ackThreshold = (isYouTubeConnection() || isSocialMediaConnection() || trafficPattern.isBatchPattern) ? 1 : 2
        
        if delayedAckPacketsSinceLast >= ackThreshold {
            sendPureAck()
            cancelDelayedAckTimer()
            return
        }

        if delayedAckTimer == nil {
            let timer = DispatchSource.makeTimerSource(queue: self.queue)
            let timeout = isYouTubeConnection() ? 10 : DELAYED_ACK_TIMEOUT_MS
            timer.schedule(deadline: .now() + .milliseconds(timeout))
            timer.setEventHandler { [weak self] in
                Task { [weak self] in
                    await self?.sendPureAck()
                    await self?.cancelDelayedAckTimer()
                }
            }
            timer.resume()
            delayedAckTimer = timer
        }
    }

    private func sendDuplicateAck() {
        duplicatePacketCount += 1
        if duplicatePacketCount >= 3 {
            log("Fast retransmit triggered after 3 duplicate ACKs")
        }
        sendPureAck()
    }

    // MARK: - SACK Generation
    private func generateSackBlocks() -> [(UInt32, UInt32)] {
        guard !outOfOrderPackets.isEmpty else { return [] }

        var blocks: [(UInt32, UInt32)] = []
        var currentStart: UInt32?
        var currentEnd: UInt32?

        for packet in outOfOrderPackets {
            let packetEnd = packet.seq &+ UInt32(packet.data.count)

            if let start = currentStart, let end = currentEnd {
                if seqLE(packet.seq, end) {
                    currentEnd = max(end, packetEnd)
                } else {
                    blocks.append((start, end))
                    if blocks.count >= 4 { break }
                    currentStart = packet.seq
                    currentEnd = packetEnd
                }
            } else {
                currentStart = packet.seq
                currentEnd = packetEnd
            }
        }

        if let start = currentStart, let end = currentEnd, blocks.count < 4 {
            blocks.append((start, end))
        }

        return blocks
    }

    // MARK: - Retransmission Timer
    private func startRetransmitTimer() {
        cancelRetransmitTimer()

        let timer = DispatchSource.makeTimerSource(queue: self.queue)
        let timeout = RETRANSMIT_TIMEOUT_MS * (1 << min(retransmitRetries, 4))
        timer.schedule(deadline: .now() + .milliseconds(timeout))
        timer.setEventHandler { [weak self] in
            Task { [weak self] in
                await self?.handleRetransmitTimeout()
            }
        }
        timer.resume()
        retransmitTimer = timer
    }

    private func handleRetransmitTimeout() async {
        retransmitRetries += 1
        retransmittedPackets += 1

        if retransmitRetries > MAX_RETRANSMIT_RETRIES {
            shrinkToMinimumImmediate()
            
            if let firstBuffered = outOfOrderPackets.first {
                let gap = firstBuffered.seq &- nextExpectedSequence
                if gap <= UInt32(mss) {
                    log("Skipping small gap of \(gap) bytes after max retries")
                    nextExpectedSequence = firstBuffered.seq
                    processBufferedPackets()
                    cancelRetransmitTimer()
                    return
                }
            }
            log("Max retransmit retries reached, connection may be stalled")
            cancelRetransmitTimer()
            return
        }

        for _ in 0..<3 {
            if sackEnabled && peerSupportsSack {
                sendAckWithSack()
            } else {
                sendPureAck()
            }
        }
        startRetransmitTimer()
    }

    // MARK: - TCP Packet Construction
    func sendSynAckWithOptions() async {
        guard !synAckSent else { return }
        synAckSent = true

        updateAdvertisedWindow()

        let seq = serverInitialSequenceNumber
        let ackNumber = initialClientSequenceNumber &+ 1

        var tcp = Data(count: 40)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)

        withUnsafeBytes(of: seq.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }

        tcp[12] = 0xA0  // Data offset = 10 (40 bytes)
        tcp[13] = TCPFlags([.syn, .ack]).rawValue

        tcp[14] = UInt8(availableWindow >> 8)
        tcp[15] = UInt8(availableWindow & 0xFF)

        tcp[16] = 0; tcp[17] = 0  // Checksum
        tcp[18] = 0; tcp[19] = 0  // Urgent

        // TCP options
        var offset = 20

        // MSS
        let mssVal = UInt16(self.mss)
        tcp[offset] = 0x02; tcp[offset+1] = 0x04
        tcp[offset+2] = UInt8(mssVal >> 8)
        tcp[offset+3] = UInt8(mssVal & 0xFF)
        offset += 4

        // SACK Permitted
        tcp[offset] = 0x04; tcp[offset+1] = 0x02
        offset += 2

        // Padding with NOPs
        while offset < 40 {
            tcp[offset] = 0x01
            offset += 1
        }

        // Checksums
        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)

        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])

        self.serverSequenceNumber = seq &+ 1
        self.clientSequenceNumber = self.initialClientSequenceNumber &+ 1
        self.lastAdvertisedWindow = availableWindow

        log("Sent SYN-ACK with MSS=\(self.mss), SACK, no WScale")
    }

    private func sendPureAck() {
        updateAdvertisedWindow()
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

        lastAdvertisedWindow = availableWindow
        delayedAckPacketsSinceLast = 0
    }

    private func sendAckWithSack() {
        updateAdvertisedWindow()
        let sackBlocks = generateSackBlocks()
        if sackBlocks.isEmpty {
            sendPureAck()
            return
        }

        let ackNumber = self.clientSequenceNumber

        // TCP header size with SACK option
        let sackOptionLength = 2 + (sackBlocks.count * 8)
        let tcpOptionsLength = 2 + sackOptionLength
        let tcpHeaderLength = 20 + tcpOptionsLength
        let paddedLength = ((tcpHeaderLength + 3) / 4) * 4

        var tcp = Data(count: paddedLength)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: serverSequenceNumber.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        tcp[12] = UInt8((paddedLength / 4) << 4)
        tcp[13] = TCPFlags.ack.rawValue
        tcp[14] = UInt8(availableWindow >> 8)
        tcp[15] = UInt8(availableWindow & 0xFF)
        tcp[16] = 0; tcp[17] = 0
        tcp[18] = 0; tcp[19] = 0

        // SACK option
        var optionOffset = 20
        tcp[optionOffset] = 0x01; optionOffset += 1 // NOP
        tcp[optionOffset] = 0x01; optionOffset += 1 // NOP
        tcp[optionOffset] = 0x05 // SACK
        tcp[optionOffset + 1] = UInt8(sackOptionLength)
        optionOffset += 2

        for block in sackBlocks {
            withUnsafeBytes(of: block.0.bigEndian) { tcp.replaceSubrange(optionOffset..<(optionOffset+4), with: $0) }
            optionOffset += 4
            withUnsafeBytes(of: block.1.bigEndian) { tcp.replaceSubrange(optionOffset..<(optionOffset+4), with: $0) }
            optionOffset += 4
        }

        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)

        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        lastAdvertisedWindow = availableWindow
        delayedAckPacketsSinceLast = 0
    }

    // MARK: - Data Forwarding
    private func writeToTunnel(payload: Data) async {
        updateAdvertisedWindow()
        let ackNumber = self.clientSequenceNumber
        var currentSeq = self.serverSequenceNumber
        var packets: [Data] = []

        cancelDelayedAckTimer()

        // Split into MSS-sized segments
        var offset = 0
        while offset < payload.count {
            let segmentSize = min(payload.count - offset, mss)
            let segment = payload[offset..<(offset + segmentSize)]

            var tcp = createTCPHeader(
                payloadLen: segmentSize,
                flags: [.ack, .psh],
                sequenceNumber: currentSeq,
                acknowledgementNumber: ackNumber
            )
            var ip = createIPv4Header(payloadLength: tcp.count + segmentSize)

            let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data(segment))
            tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)

            let ipCsum = ipChecksum(&ip)
            ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)

            packets.append(ip + tcp + Data(segment))
            currentSeq &+= UInt32(segmentSize)
            offset += segmentSize
        }

        if !packets.isEmpty {
            let protocols = Array(repeating: AF_INET as NSNumber, count: packets.count)
            packetFlow.writePackets(packets, withProtocols: protocols)

            let total = packets.reduce(0) { $0 + $1.count }
            onBytesBackToTunnel?(total)

            self.serverSequenceNumber = currentSeq
            lastAdvertisedWindow = availableWindow
        }
    }

    // MARK: - SOCKS Handling
    private func handleStateUpdate(_ newState: NWConnection.State) async {
        switch newState {
        case .ready:
            log("SOCKS connection ready")
            await setSocksState(.greetingSent)
            await performSocksHandshake()

        case .waiting(let err):
            log("SOCKS waiting: \(err.localizedDescription)")

        case .failed(let err):
            log("SOCKS failed: \(err.localizedDescription)")
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
                    await self.log("Handshake error: \(error)")
                    await self.close()
                    return
                }
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
                    await self.log("Invalid handshake response")
                    await self.close()
                    return
                }

                await self.setSocksState(.methodOK)
                await self.sendSocksConnectRequest()
            }
        }
    }

    private func sendSocksConnectRequest() async {
        guard let connection = socksConnection else { return }

        let target = await decideSocksTarget()
        self.chosenTarget = target

        var request = Data()
        request.append(0x05) // Version
        request.append(0x01) // CONNECT
        request.append(0x00) // RSV

        switch target {
        case .domain(let host, let port):
            request.append(0x03) // ATYP: Domain
            let dom = Data(host.utf8)
            request.append(UInt8(dom.count))
            request.append(dom)
            request.append(contentsOf: [UInt8(port >> 8), UInt8(port & 0xFF)])

        case .ip(let ip, let port):
            request.append(0x01) // ATYP: IPv4
            request.append(ip.rawValue)
            request.append(contentsOf: [UInt8(port >> 8), UInt8(port & 0xFF)])
        }

        await setSocksState(.connectSent)

        connection.send(content: request, completion: .contentProcessed({ [weak self] error in
            Task { [weak self] in
                guard let self else { return }
                if let error = error {
                    await self.log("Connect request error: \(error)")
                    await self.close()
                    return
                }
                await self.receiveConnectResponse()
            }
        }))
    }

    private func receiveConnectResponse() async {
        guard socksConnection != nil else { return }

        do {
            let head = try await receiveExactly(4)
            let ver = head[0], rep = head[1], atyp = head[3]

            guard ver == 0x05 else { log("Bad SOCKS version"); close(); return }
            guard rep == 0x00 else { log("SOCKS connect rejected: \(rep)"); close(); return }

            // Skip bound address
            switch atyp {
            case 0x01: _ = try await receiveExactly(4)  // IPv4
            case 0x04: _ = try await receiveExactly(16) // IPv6
            case 0x03:
                let len = try await receiveExactly(1)[0]
                if len > 0 { _ = try await receiveExactly(Int(len)) }
            default:
                log("Unknown ATYP: \(atyp)"); close(); return
            }
            _ = try await receiveExactly(2) // Port

            await setSocksState(.established)
            
            log("SOCKS tunnel established")
            
            // 连接建立后针对YouTube和社交媒体优化
            if isYouTubeConnection() && recvBufferLimit < YOUTUBE_BUFFER {
                adjustBufferSize(YOUTUBE_BUFFER)
                log("[YouTube] Buffer optimized after SOCKS established")
            } else if isSocialMediaConnection() && recvBufferLimit < SOCIAL_NORMAL_BUFFER {
                adjustBufferSize(SOCIAL_NORMAL_BUFFER)
                log("[Social] Buffer optimized after SOCKS established")
            }
            
            startKeepAlive()
            
            await sendSynAckIfNeeded()
            await flushPendingData()
            await readFromSocks()

        } catch {
            log("Connect response error: \(error)")
            close()
        }
    }
    
    @inline(__always)
    private func sendRawToSocksOnce(_ data: Data) async -> Bool {
        guard let conn = socksConnection else { return false }
        return await withCheckedContinuation { cont in
            conn.send(content: data, completion: .contentProcessed { err in
                cont.resume(returning: err == nil)
            })
        }
    }
    
    // 自适应缓冲区管理
    private func adaptBufferToFlow() {
        let utilizationRate = Double(bufferedBytesForWindow()) / Double(recvBufferLimit)
        
        // YouTube和社交媒体连接特殊处理
        if isYouTubeConnection() || trafficPattern.isYouTubePattern {
            if utilizationRate > 0.7 && recvBufferLimit < YOUTUBE_BUFFER {
                adjustBufferSize(YOUTUBE_BUFFER)
                return
            }
        }
        
        if isSocialMediaConnection() || trafficPattern.isSocialMediaPattern {
            if trafficPattern.isScrollingPattern && recvBufferLimit > SOCIAL_SCROLL_BUFFER {
                adjustBufferSize(SOCIAL_SCROLL_BUFFER)
                return
            } else if utilizationRate > 0.7 && recvBufferLimit < SOCIAL_NORMAL_BUFFER {
                adjustBufferSize(SOCIAL_NORMAL_BUFFER)
                return
            }
        }
        
        // 通用逻辑
        if utilizationRate > 0.9 && recvBufferLimit < 64 * 1024 {
            adjustBufferSize(min(recvBufferLimit * 2, 64 * 1024))
        } else if utilizationRate < 0.1 && recvBufferLimit > getMinBufferSize() {
            adjustBufferSize(max(recvBufferLimit / 2, getMinBufferSize()))
        }
    }
    
    // 公开属性和方法
    public var remotePort: UInt16? { destPort }
    public var remoteIPv4: String? { String(describing: destIP) }
    public var sniHost: String? { extractedSNI }
    public var originalHostname: String? { destinationHost }
    public var isHighTraffic: Bool = false
    
    private var extractedSNI: String?
    
    private func extractSNIFromTLSHandshake(_ data: Data) {
        guard data.count > 43,
              data[0] == 0x16, // TLS Handshake
              data[5] == 0x01  // ClientHello
        else { return }
        // 实际TLS解析逻辑（简化）
    }
    
    private func getConnectionPriority() -> ConnectionPriority? {
        if isYouTubeConnection() || isSocialMediaConnection() {
            return .high
        }
        
        switch destPort {
        case 5223: return .critical
        case 443: return .high
        case 80: return .normal
        default: return .low
        }
    }
    
    public func getOverflowCount() async -> Int {
        return overflowCount
    }
    
    private var isFlushing = false

    private func flushPendingData() async {
        guard socksState == .established, !pendingClientBytes.isEmpty else { return }

        let queue = pendingClientBytes
        pendingClientBytes.removeAll(keepingCapacity: true)

        log("Flushing \(pendingBytesTotal) bytes of pending data")

        for chunk in queue {
            var sentOK = false
            var backoffNs: UInt64 = 20_000_000 // 20ms

            for attempt in 1...3 {
                if await sendRawToSocksOnce(chunk) {
                    sentOK = true
                    break
                } else {
                    log("flushPendingData: send failed (attempt \(attempt)), backing off \(backoffNs/1_000_000)ms")
                    try? await Task.sleep(nanoseconds: backoffNs)
                    backoffNs = min(backoffNs << 1, 200_000_000)
                }
            }

            if sentOK {
                pendingBytesTotal -= chunk.count
                lastActivityTime = Date()
                maybeSendWindowUpdate(reason: "post-send")
            } else {
                pendingClientBytes.insert(chunk, at: 0)
                break
            }
        }
    }

    private func readFromSocks() async {
        socksConnection?.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [weak self] data, _, isComplete, error in
            Task { [weak self] in
                guard let self else { return }
                
                if let error = error {
                    await self.log("SOCKS receive error: \(error)")
                    await self.sendFinToClient()
                    await self.close()
                    return
                }
                
                if isComplete {
                    await self.sendFinToClient()
                    await self.close()
                    return
                }
                
                if let data, !data.isEmpty {
                    await self.cancelDelayedAckTimer()
                    await self.writeToTunnel(payload: data)
                } else {
                    await self.markFlowIdle()
                }
                
                await self.readFromSocks()
            }
        }
    }
    
    private func markFlowIdle() {
        backpressure.markIdle()
        
        if backpressure.consecutiveIdleCycles > 2 {
            evaluateBufferShrinkage()
        }
    }
    
    private func evaluateBufferShrinkage() {
        // 忽略Keep-Alive，只看实际数据
        if !isReallyActive() && recvBufferLimit > getMinBufferSize() {
            let timeSinceData = trafficType.lastActualDataTime.map { Date().timeIntervalSince($0) } ?? 999.0
            
            if timeSinceData > 0.1 {  // 100ms无实际数据
                recvBufferLimit = getMinBufferSize()
                updateAdvertisedWindow()
                log("[Shrink] No actual data for \(timeSinceData)s")
            }
        }
    }
    
    // 公开方法供外部监控
    public func getBackpressureStats() async -> (
        bufferSize: Int,
        usage: Int,
        dataRate: Double,
        isActive: Bool
    ) {
        return (
            recvBufferLimit,
            bufferedBytesForWindow(),
            backpressure.recentDataRate,
            backpressure.isActive
        )
    }

    public func optimizeBufferBasedOnFlow() async {
        if !isReallyActive() && backpressure.timeSinceLastData > 0.1 {
            shrinkBufferImmediate()
        }
    }

    // MARK: - Helper Methods
    private func cancelAllTimers() {
        cancelRetransmitTimer()
        cancelDelayedAckTimer()
        cancelKeepAliveTimer()
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

    private func cleanupBuffers() {
        if !outOfOrderPackets.isEmpty {
            log("Clearing \(outOfOrderPackets.count) buffered packets")
            outOfOrderPackets.removeAll()
        }
        pendingClientBytes.removeAll()
        pendingBytesTotal = 0
        maybeSendWindowUpdate(reason: "cleanupBuffers")
    }

    private func appendToPending(_ data: Data) {
        pendingClientBytes.append(data)
        pendingBytesTotal += data.count

        if pendingBytesTotal > pendingSoftCapBytes {
            trimPendingBuffer()
        }
        
        if pendingBytesTotal > recvBufferLimit {
            trimBuffers()
            log("Pending bytes exceeded recv limit, aggressive trimming")
        }
        
        updateAdvertisedWindow()
    }
    
    private var isEmergencyMemoryPressure: Bool {
        return false
    }

    private var pendingSoftCapBytes: Int {
        return (socksState == .established) ? (24 * 1024) : (12 * 1024)
    }

    private func trimPendingBuffer() {
        guard !pendingClientBytes.isEmpty else { return }

        if !isEmergencyMemoryPressure {
            return
        }
        
        var dropped = 0
        let idx = 1

        while pendingBytesTotal > pendingSoftCapBytes && idx < pendingClientBytes.count {
            let size = pendingClientBytes[idx].count
            pendingClientBytes.remove(at: idx)
            pendingBytesTotal -= size
            dropped += size
        }

        if dropped > 0 {
            log("Dropped \(dropped) bytes from pending buffer")
        }
        updateAdvertisedWindow()
    }

    private func sendRawToSocks(_ data: Data) {
        socksConnection?.send(content: data, completion: .contentProcessed({ [weak self] err in
            if let err = err {
                Task {
                    await self?.log("SOCKS send error: \(err)")
                    await self?.handleClientRefusal()
                    await self?.close()
                }
                return
            }
            
            Task {
                await self?.onDataSuccessfullySent(data.count)
                await self?.recomputeWindowAndMaybeAck(expand: true, reason: "post-send")
            }
        }))
    }
    
    private func onDataSuccessfullySent(_ bytes: Int) {
        backpressure.lastDataSentTime = Date()
        
        let timeSinceLastReceive = backpressure.timeSinceLastData
        
        if timeSinceLastReceive > 0.05 {
            if !backpressure.isActive && bufferedBytesForWindow() < recvBufferLimit / 4 {
                evaluateBufferShrinkage()
            }
        }
    }
    
    @inline(__always)
    private func recomputeWindowAndMaybeAck(expand: Bool, reason: String) {
        let prev = availableWindow
        updateAdvertisedWindow()
        if !expand { return }
        if availableWindow > prev {
            maybeSendWindowUpdate(reason: reason)
        }
    }

    private func setSocksState(_ newState: SocksState) async {
        self.socksState = newState
    }

    private func sendSynAckIfNeeded() async {
        guard !synAckSent else { return }
        await sendSynAckWithOptions()
    }

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

    // MARK: - Decision Making
    private func decideSocksTarget() async -> SocksTarget {
        if let host = destinationHost, !host.isEmpty {
            if isFakeIP(destIP) {
                await DNSInterceptor.shared.registerMapping(fakeIP: destIP, domain: host.lowercased())
            }
            return .domain(host, port: destPort)
        }

        if isFakeIP(destIP) {
            if let mapped = await DNSInterceptor.shared.getDomain(forFakeIP: destIP) {
                return .domain(mapped, port: destPort)
            }
        }

        return .ip(destIP, port: destPort)
    }

    // MARK: - Receive Helpers
    private enum SocksReadError: Error { case closed }

    private func receiveExactly(_ n: Int) async throws -> Data {
        guard socksConnection != nil else { throw SocksReadError.closed }

        var buf = Data()
        buf.reserveCapacity(n)

        while buf.count < n {
            let chunk = try await receiveChunk(max: n - buf.count)
            if chunk.isEmpty { throw SocksReadError.closed }
            buf.append(chunk)
        }

        return buf
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

    // MARK: - SYN Options Parsing
    func acceptClientSyn(tcpHeaderAndOptions tcpSlice: Data) async {
        parseSynOptions(tcpSlice)
        await sendSynAckIfNeeded()
    }

    private func parseSynOptions(_ tcpHeader: Data) {
        guard tcpHeader.count >= 20 else { return }

        let dataOffsetWords = (tcpHeader[12] >> 4) & 0x0F
        let headerLen = Int(dataOffsetWords) * 4
        guard headerLen >= 20, tcpHeader.count >= headerLen else { return }

        let options = tcpHeader[20..<headerLen]
        var info = SynOptionInfo(rawOptions: Data(options))

        var i = options.startIndex
        while i < options.endIndex {
            let kind = options[i]
            i = options.index(after: i)

            switch kind {
            case 0: // EOL
                break
            case 1: // NOP
                continue
            default:
                if i >= options.endIndex { break }
                let len = Int(options[i])
                i = options.index(after: i)

                guard len >= 2,
                      options.distance(from: options.index(i, offsetBy: -2), to: options.endIndex) >= len else { break }

                let payloadStart = options.index(i, offsetBy: -2)
                let payloadEnd = options.index(payloadStart, offsetBy: len)

                switch kind {
                case 2: // MSS
                    if len == 4 {
                        let b0 = options[options.index(payloadStart, offsetBy: 2)]
                        let b1 = options[options.index(payloadStart, offsetBy: 3)]
                        info.mss = (UInt16(b0) << 8) | UInt16(b1)
                    }
                case 3: // Window Scale
                    if len == 3 {
                        info.windowScale = options[options.index(payloadStart, offsetBy: 2)]
                    }
                case 4: // SACK Permitted
                    if len == 2 {
                        info.sackPermitted = true
                    }
                default:
                    break
                }

                i = payloadEnd
            }
        }

        clientSynOptions = info
        if info.sackPermitted {
            peerSupportsSack = true
        }

        log("Client SYN options: MSS=\(info.mss ?? 0), SACK=\(info.sackPermitted)")
    }

    func retransmitSynAckDueToDuplicateSyn() async {
        guard !handshakeAcked else { return }
        updateAdvertisedWindow()
        let seq = serverInitialSequenceNumber
        let ackNumber = initialClientSequenceNumber &+ 1

        var tcp = Data(count: 40)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: seq.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        tcp[12] = 0xA0
        tcp[13] = TCPFlags([.syn, .ack]).rawValue

        tcp[14] = UInt8(availableWindow >> 8)
        tcp[15] = UInt8(availableWindow & 0xFF)

        // MSS + SACK-permitted
        var off = 20
        let mssVal = UInt16(self.mss)
        tcp[off] = 0x02; tcp[off+1] = 0x04
        tcp[off+2] = UInt8(mssVal >> 8); tcp[off+3] = UInt8(mssVal & 0xFF)
        off += 4
        tcp[off] = 0x04; tcp[off+1] = 0x02
        off += 2
        while off < 40 { tcp[off] = 0x01; off += 1 }

        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcs = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcs >> 8); tcp[17] = UInt8(tcs & 0xFF)
        let ics = ipChecksum(&ip)
        ip[10] = UInt8(ics >> 8); ip[11] = UInt8(ics & 0xFF)

        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        lastAdvertisedWindow = availableWindow
        log("Retransmitted SYN-ACK due to duplicate SYN")
    }

    func onInboundAck(ackNumber: UInt32) {
        updateLastActivity()
        
        if !handshakeAcked {
            let expected = serverInitialSequenceNumber &+ 1
            if ackNumber >= expected {
                handshakeAcked = true
                nextExpectedSequence = initialClientSequenceNumber &+ 1
                serverSequenceNumber = serverInitialSequenceNumber &+ 1
                log("Handshake ACK received")
            }
        }
    }
    
    func onInboundAckWithWindow(ackNumber: UInt32, windowSize: UInt16) {
        updateLastActivity()
        
        let oldWindow = clientAdvertisedWindow
        clientAdvertisedWindow = windowSize
        
        if windowSize == 0 {
            zeroWindowProbeCount += 1
            log("[Window] Client advertised zero window (probe #\(zeroWindowProbeCount))")
            
            if zeroWindowProbeCount > 2 {
                handleClientRefusal()
            }
        } else if oldWindow == 0 && windowSize > 0 {
            zeroWindowProbeCount = 0
            log("[Window] Client window recovered: \(windowSize)")
        }
        
        onInboundAck(ackNumber: ackNumber)
    }
    
    func adjustBufferSize(_ newSize: Int) {
        let oldSize = recvBufferLimit
        recvBufferLimit = max(getMinBufferSize(), min(newSize, Int(MAX_WINDOW_SIZE)))
        
        if oldSize != recvBufferLimit {
            log("Buffer adjusted: \(oldSize) -> \(recvBufferLimit) bytes")
            updateAdvertisedWindow()
            
            if recvBufferLimit < oldSize && bufferedBytesForWindow() > recvBufferLimit {
                trimBuffers()
            }
        }
    }
    
    private func trimBuffers() {
        while outOfOrderPackets.count > MAX_BUFFERED_PACKETS / 2 {
            outOfOrderPackets.removeFirst()
        }
        
        if pendingBytesTotal > recvBufferLimit {
            trimPendingBuffer()
        }
        
        log("Buffers trimmed due to size reduction")
    }
    
    public func handleMemoryPressure(targetBufferSize: Int) async {
        adjustBufferSize(targetBufferSize)
        
        if bufferedBytesForWindow() > recvBufferLimit {
            trimBuffers()
            log("Memory pressure: trimmed buffers to \(recvBufferLimit) bytes")
        }
    }

    // MARK: - Packet Creation Helpers
    private func createIPv4Header(payloadLength: Int) -> Data {
        var header = Data(count: 20)
        header[0] = 0x45
        let totalLength = 20 + payloadLength
        header[2] = UInt8(totalLength >> 8)
        header[3] = UInt8(totalLength & 0xFF)
        header[4] = 0x00; header[5] = 0x00
        header[6] = 0x40; header[7] = 0x00
        header[8] = 64
        header[9] = 6
        header[10] = 0; header[11] = 0

        let src = [UInt8](destIP.rawValue)
        let dst = [UInt8](sourceIP.rawValue)
        header[12] = src[0]; header[13] = src[1]; header[14] = src[2]; header[15] = src[3]
        header[16] = dst[0]; header[17] = dst[1]; header[18] = dst[2]; header[19] = dst[3]

        return header
    }

    private func createTCPHeader(payloadLen: Int, flags: TCPFlags, sequenceNumber: UInt32, acknowledgementNumber: UInt32) -> Data {
        var h = Data(count: 20)
        h[0] = UInt8(destPort >> 8); h[1] = UInt8(destPort & 0xFF)
        h[2] = UInt8(sourcePort >> 8); h[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: sequenceNumber.bigEndian) { h.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: acknowledgementNumber.bigEndian) { h.replaceSubrange(8..<12, with: $0) }
        h[12] = 0x50
        h[13] = flags.rawValue
        h[14] = UInt8(availableWindow >> 8)
        h[15] = UInt8(availableWindow & 0xFF)
        h[16] = 0; h[17] = 0
        h[18] = 0; h[19] = 0
        return h
    }

    private func tcpChecksum(ipHeader ip: Data, tcpHeader tcp: Data, payload: Data) -> UInt16 {
        var pseudo = Data()
        pseudo.append(ip[12...15])
        pseudo.append(ip[16...19])
        pseudo.append(0)
        pseudo.append(6)
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

    private func log(_ message: String) {
        
//        let context = socksState == .established ? "[EST]" : "[PRE]"
//        NSLog("[TCPConnection \(key)] \(context) \(message)")
    }

    func onInboundFin(seq: UInt32) async {
        if seq == clientSequenceNumber {
            clientSequenceNumber &+= 1
        }
        sendPureAck()
    }
    
    // MARK: - Keep-Alive
    private var keepAliveTimer: DispatchSourceTimer?
    private var lastActivityTime = Date()
    private var keepAliveProbesSent = 0
    private let maxKeepAliveProbes = 3
    
    private func startKeepAlive() {
        let interval = computeKeepAliveInterval(host: self.destinationHost, port: self.destPort)
        self.keepAliveInterval = interval
        keepAliveTimer = DispatchSource.makeTimerSource(queue: queue)
        keepAliveTimer?.schedule(deadline: .now() + interval, repeating: interval)
        keepAliveTimer?.setEventHandler { [weak self] in
            guard let self = self else { return }
            Task.detached { [weak self] in
                await self?.sendKeepAlive()
            }
        }
        self.lastKeepAliveSent = Date()
        keepAliveTimer?.resume()
        log("Keep-alive started with interval: \(interval)s for port \(destPort)")
    }
    
    private func sendKeepAlive() async {
        let timeSinceLastActivity = Date().timeIntervalSince(lastActivityTime)
        
        if timeSinceLastActivity < keepAliveInterval {
            keepAliveProbesSent = 0
            return
        }
        
        guard socksState == .established else {
            cancelKeepAliveTimer()
            return
        }
        
        await sendKeepAliveProbe()
        
        keepAliveProbesSent += 1
        
        if keepAliveProbesSent >= maxKeepAliveProbes {
            log("Keep-alive timeout after \(maxKeepAliveProbes) probes, closing connection")
            close()
        }
    }

    private func sendKeepAliveProbe() async {
        let probeSeq = serverSequenceNumber &- 1
        let ackNumber = clientSequenceNumber
        
        var tcp = Data(count: 20)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        
        withUnsafeBytes(of: probeSeq.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        
        tcp[12] = 0x50
        tcp[13] = TCPFlags.ack.rawValue
        tcp[14] = UInt8(availableWindow >> 8)
        tcp[15] = UInt8(availableWindow & 0xFF)
        tcp[16] = 0; tcp[17] = 0
        tcp[18] = 0; tcp[19] = 0
        
        var ip = createIPv4Header(payloadLength: tcp.count)
        let tcpCsum = tcpChecksum(ipHeader: ip, tcpHeader: tcp, payload: Data())
        tcp[16] = UInt8(tcpCsum >> 8); tcp[17] = UInt8(tcpCsum & 0xFF)
        let ipCsum = ipChecksum(&ip)
        ip[10] = UInt8(ipCsum >> 8); ip[11] = UInt8(ipCsum & 0xFF)
        
        packetFlow.writePackets([ip + tcp], withProtocols: [AF_INET as NSNumber])
        
        log("Keep-alive probe sent (probe #\(keepAliveProbesSent + 1))")
    }

    private func cancelKeepAliveTimer() {
        keepAliveTimer?.cancel()
        keepAliveTimer = nil
        keepAliveProbesSent = 0
    }

    private func updateLastActivity() {
        lastActivityTime = Date()
        keepAliveProbesSent = 0
    }
}

// MARK: - Helper Types
struct TCPFlags: OptionSet {
    let rawValue: UInt8
    static let fin = TCPFlags(rawValue: 1 << 0)
    static let syn = TCPFlags(rawValue: 1 << 1)
    static let rst = TCPFlags(rawValue: 1 << 2)
    static let psh = TCPFlags(rawValue: 1 << 3)
    static let ack = TCPFlags(rawValue: 1 << 4)
}

// MARK: - Utilities
private func isFakeIP(_ ip: IPv4Address) -> Bool {
    let b = [UInt8](ip.rawValue)
    return b.count == 4 && b[0] == 198 && (b[1] & 0xFE) == 18
}

// MARK: - Extensions
extension Array where Element == (seq: UInt32, data: Data) {
    func binarySearch(predicate: (Element) -> Bool) -> Int {
        var left = 0
        var right = count
        while left < right {
            let mid = (left + right) / 2
            if predicate(self[mid]) {
                left = mid + 1
            } else {
                right = mid
            }
        }
        return left
    }
}

extension Data {
    fileprivate func hexEncodedString() -> String {
        map { String(format: "%02hhx", $0) }.joined()
    }
}
