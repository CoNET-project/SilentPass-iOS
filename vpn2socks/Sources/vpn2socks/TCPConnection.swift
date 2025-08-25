//
//  TCPConnection.swift
//  vpn2socks
//
//  Optimized version based on log analysis (revised + dynamic rwnd w/ window updates)
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
    
    var growthTrend: Double = 0  // 流量增长趋势
       var lastGrowthCheck = Date()
       
       mutating func updateGrowthTrend() {
           guard recentDataPoints.count >= 5 else { return }
           
           let firstHalf = recentDataPoints.prefix(recentDataPoints.count / 2)
           let secondHalf = recentDataPoints.suffix(recentDataPoints.count / 2)
           
           let firstAvg = Double(firstHalf.map { $0.1 }.reduce(0, +)) / Double(firstHalf.count)
           let secondAvg = Double(secondHalf.map { $0.1 }.reduce(0, +)) / Double(secondHalf.count)
           
           growthTrend = (secondAvg - firstAvg) / max(firstAvg, 1.0)
       }
    
    var overflowCount: Int = 0  // 添加溢出计数
        var lastOverflowTime: Date?
        
        mutating func recordOverflow() {
            overflowCount += 1
            lastOverflowTime = Date()
        }
        
        var recentOverflowRate: Double {
            guard let lastTime = lastOverflowTime else { return 0 }
            let timeSince = Date().timeIntervalSince(lastTime)
            guard timeSince > 0 else { return Double(overflowCount) }
            return Double(overflowCount) / timeSince  // 溢出次数/秒
        }
}




final actor TCPConnection {

    private var backpressure = BackpressureMetrics()
    private let MIN_BUFFER = 4 * 1024  // 最小4KB
    private let MAX_BUFFER = 256 * 1024    // 增加到 256KB (原来可能是 64KB)
    private let BURST_BUFFER = 512 * 1024  // 突发流量缓冲区 512KB
    
    
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
    private var clientAdvertisedWindow: UInt16 = 65535  // 记录客户端通告的窗口大小
    private var zeroWindowProbeCount: Int = 0           // 零窗口探测计数
    private var lastRefusalTime: Date?                  // 上次拒绝时间
    
    private func detectClientRefusal() -> Bool {
        // 严重信号：立即返回true
        
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
            
        // 直接收缩到最小
        recvBufferLimit = MIN_BUFFER
        
        // 立即更新窗口
        updateAdvertisedWindow()
        
        // 如果需要，发送窗口更新
        if availableWindow < lastAdvertisedWindow / 2 {
            sendPureAck()  // 通知对端窗口变小了
            lastAdvertisedWindow = availableWindow
        }
        
        log("[Backpressure] Emergency shrink: \(oldSize) -> \(MIN_BUFFER) bytes")
    }
    
    private func handleSocksError(_ error: Error?) -> Bool {
        if let error = error {
            // SOCKS发送失败，说明客户端拒绝
            return true
        }
        
        // 检查SOCKS连接状态
        if socksState != .established {
            return true
        }
        
        return false
    }

    private func clearAllBuffers() {
        // 清空乱序包缓冲
        outOfOrderPackets.removeAll()
        
        // 清空待发送数据
        if socksState != .established {
            pendingClientBytes.removeAll()
            pendingBytesTotal = 0
        }
        
        // 重置背压状态
        backpressure = BackpressureMetrics()
    }
    
    // MARK: - 激进的缓冲区清理
    private func trimBuffersAggressively() {
        // 只保留最新的几个包
        let maxKeep = 2
        
        if outOfOrderPackets.count > maxKeep {
            let toRemove = outOfOrderPackets.count - maxKeep
            outOfOrderPackets.removeFirst(toRemove)
            log("[Trim] Aggressively removed \(toRemove) out-of-order packets")
        }
        
        // 清理待发送数据（保留TLS握手等关键数据）
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

    private func handleClientRefusal() {
        log("[Refusal] Client refusing data - emergency shrink to \(MIN_BUFFER)")
            
            lastRefusalTime = Date()
            
            
            // 立即收缩到最小
            let oldSize = recvBufferLimit
            recvBufferLimit = MIN_BUFFER
            
            // 立即收缩
            shrinkToMinimumImmediate()
        
            // 清理缓冲
            if bufferedBytesForWindow() > MIN_BUFFER {
                trimBuffersAggressively()
            }
            
            // 通知背压管理器
            backpressure.markIdle()
            backpressure.consecutiveIdleCycles = 10  // 标记为极度空闲
    }
    
    // MARK: - Constants
    public let tunnelMTU: Int
    private var mss: Int { max(536, tunnelMTU - 40) } // 40 = IPv4+TCP 基础头
    private let MAX_WINDOW_SIZE: UInt16 = 65535
    private let DELAYED_ACK_TIMEOUT_MS: Int = 25
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
    private static let pendingSoftCapBytes = 32 * 1024 //64 * 1024

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

    // MARK: - Flow Control
    private var availableWindow: UInt16 = 65535
    private var congestionWindow: UInt32 { 10 * UInt32(mss) }
    private var slowStartThreshold: UInt32 = 65535

    // NEW: advertised receive buffer cap (no window scale, so <= 65535)
    public var recvBufferLimit: Int
    // NEW: remember last advertised window to decide whether to send a Window Update
    private var lastAdvertisedWindow: UInt16 = 0
    
    public var onBytesBackToTunnel: ((Int) -> Void)?
    
    func setOnBytesBackToTunnel(_ cb: @escaping (Int) -> Void) {
        self.onBytesBackToTunnel = cb
    }
    
    // 添加公开的方法来获取最后活动时间
    public func getLastActivityTime() async -> Date? {
        return lastActivityTime
    }
    
    // 添加优先级属性
    private let priority: ConnectionPriority
    public private(set) var keepAliveInterval: TimeInterval = 45.0
    private var lastKeepAliveSent: Date?
    public func getKeepAliveTelemetry() async -> (interval: TimeInterval, lastSent: Date?, lastActivity: Date?) {
        return (keepAliveInterval, lastKeepAliveSent, await getLastActivityTime())
    }

    private nonisolated func computeKeepAliveInterval(host: String?, port: UInt16) -> TimeInterval {
        switch (host, port) {
        case (_, 5223): return 180.0     // Apple/FCM Push（你日志里 5223 的保活大多是 180s）
        case (_, 5228): return 180.0     // FCM
        case let (h, 443) where (h?.contains("apple.com") ?? false): return 120.0
        case (_, 443): return 80.0
        case (_, 80):  return 60.0
        default:       return 45.0
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
        recvBufferLimit: Int = 16 * 1024, //   60 * 1024, // NEW: 动态窗口的软上限
        priority: ConnectionPriority = .normal,  // 新增参数
        onBytesBackToTunnel: ((Int) -> Void)? = nil
    ) {
        // --- Initialization Phase 1: Assign all properties directly ---
                
        self.key = key
        self.packetFlow = packetFlow
        self.sourceIP = sourceIP
        self.sourcePort = sourcePort
        self.destIP = destIP
        self.destPort = destPort
        self.destinationHost = destinationHost
        self.priority = priority
        self.tunnelMTU = tunnelMTU
        
        // Initialize sequence numbers
        let initialServerSeq = arc4random()
        self.serverSequenceNumber = initialServerSeq
        self.serverInitialSequenceNumber = initialServerSeq
        self.initialClientSequenceNumber = initialSequenceNumber
        self.clientSequenceNumber = initialSequenceNumber
        self.nextExpectedSequence = initialSequenceNumber
        
        // Calculate and set buffer/window properties
        // This is the main fix: Calculate 'cap' first, then use it to initialize all dependent properties.
        let cap = max(8 * 1024, min(recvBufferLimit, Int(MAX_WINDOW_SIZE)))
        self.recvBufferLimit = cap
        self.availableWindow = UInt16(cap)
        self.lastAdvertisedWindow = UInt16(cap) // FIX: Initialize directly from 'cap', not from another property.
        
        // Call the nonisolated helper and assign its result
        // This must be done BEFORE the initializer exits.

        // --- Initialization Complete ---
        // Now it's safe to call methods or perform other logic.
        let interval: TimeInterval
           switch (destinationHost, destPort) {
           case (_, 5223), (_, 5228):  // Push 服务
               interval = 180.0
           case let (host, 443) where host?.contains("apple.com") ?? false:
               interval = 120.0  // Apple 服务特殊处理
           case (_, 443):   // 普通 HTTPS
               interval = 60.0
           case (_, 80):    // HTTP
               interval = 30.0
           default:
               interval = 45.0
           }
        self.keepAliveInterval = interval
        NSLog("[TCPConnection \(key)] Initialized. InitialClientSeq: \(initialSequenceNumber)")
    }

    // NEW: 当前用于接收重组的已占用字节数
    @inline(__always)
    private func bufferedBytesForWindow() -> Int {
        var used = outOfOrderPackets.reduce(0) { $0 + $1.data.count }
        if socksState != .established {
            used += pendingBytesTotal // SOCKS 未建好前的待转发数据也占用接收缓冲
        }
        return used
    }

    // NEW: 依据缓冲占用更新 advertised window（不使用 WS，<= 65535）
    @inline(__always)
    private func updateAdvertisedWindow() {
        let cap = min(recvBufferLimit, Int(MAX_WINDOW_SIZE))
        let used = bufferedBytesForWindow()
        let free = max(0, cap - used)
        self.availableWindow = UInt16(free)
    }

    // NEW: 若窗口显著增长（或 0->>0），主动发 Window Update（纯 ACK）
    @inline(__always)
    private func maybeSendWindowUpdate(reason: String) {
        let prev = lastAdvertisedWindow
        updateAdvertisedWindow()
        let nowWnd = availableWindow
        let grewFromZero = (prev == 0 && nowWnd > 0)
        let grewByMSS = nowWnd > prev && Int(nowWnd &- prev) >= mss
        if grewFromZero || grewByMSS {
            sendPureAck()
            lastAdvertisedWindow = nowWnd
            log("Window update (\(reason)): \(nowWnd) bytes")
        }
    }

    // MARK: - Lifecycle
    func start() async {
        guard socksConnection == nil else { return }
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
        cancelKeepAliveTimer()  // 新增
        // 确保完全清理
           outOfOrderPackets.removeAll(keepingCapacity: false)
           pendingClientBytes.removeAll(keepingCapacity: false)
           socksConnection?.forceCancel()  // 强制取消
           socksConnection = nil
           closeContinuation?.resume()
           closeContinuation = nil
            log("Closing connection.")
    }

    // MARK: - Sequence helpers (wrap-around safe)
    @inline(__always) private func seqDiff(_ a: UInt32, _ b: UInt32) -> Int32 {
        Int32(bitPattern: a &- b)
    }
    @inline(__always) private func seqLT(_ a: UInt32, _ b: UInt32) -> Bool { seqDiff(a, b) < 0 }
    @inline(__always) private func seqLE(_ a: UInt32, _ b: UInt32) -> Bool { seqDiff(a, b) <= 0 }

    // MARK: - Packet Handling (Optimized)
    func handlePacket(payload: Data, sequenceNumber: UInt32) {
        guard !payload.isEmpty else { return }

        // 检测是否需要紧急收缩
            if detectClientRefusal() {
                handleClientRefusal()
                return
            }
        
        updateLastActivity()  // 新增
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
    
    // MARK: - 动态缓冲区管理（基于背压）
    private func evaluateBufferExpansion() {
        
        // 检查是否在拒绝恢复期
           if let refusalTime = lastRefusalTime {
               let timeSinceRefusal = Date().timeIntervalSince(refusalTime)
               if timeSinceRefusal < 5.0 {
                   // 拒绝后5秒内不允许扩展
                   log("[Backpressure] Expansion blocked: \(5.0 - timeSinceRefusal)s until recovery")
                   return
               }
           }
        
        
        let usage = bufferedBytesForWindow()
        let utilizationRate = Double(usage) / Double(recvBufferLimit)
        
        // 检测突发流量模式
        let isBurstTraffic = backpressure.recentDataRate > Double(mss * 10) // 高速流量
        
        if utilizationRate > 0.75 && backpressure.isActive {
            // 根据流量模式选择扩展策略
            let targetSize: Int
            if isBurstTraffic {
                // 突发流量：激进扩展（4倍）
                targetSize = min(BURST_BUFFER, recvBufferLimit * 4)
            } else if utilizationRate > 0.9 {
                // 接近满载：快速扩展（3倍）
                targetSize = min(MAX_BUFFER, recvBufferLimit * 3)
            } else {
                // 常规扩展（2倍）
                targetSize = min(MAX_BUFFER, recvBufferLimit * 2)
            }
            
            if targetSize > recvBufferLimit {
                expandBufferImmediate(to: targetSize)
            }
        }
    }
    
    // 添加突发流量检测
    private func detectTrafficBurst() -> Bool {
        guard backpressure.recentDataPoints.count >= 3 else { return false }
        
        let recent = backpressure.recentDataPoints.suffix(3)
        let avgBytes = recent.map { $0.1 }.reduce(0, +) / recent.count
        
        // 如果最近平均流量超过 MSS 的 5 倍，认为是突发流量
        return avgBytes > mss * 5
    }
    
    
    private func expandBufferImmediate(to size: Int) {
        let oldSize = recvBufferLimit
        recvBufferLimit = size
        updateAdvertisedWindow()
        maybeSendWindowUpdate(reason: "backpressure-expand")
        log("[Backpressure] Buffer expanded: \(oldSize) -> \(size) bytes")
    }

    private func shrinkBufferImmediate() {
        // 立即收缩到最小值
        if recvBufferLimit > MIN_BUFFER {
            let oldSize = recvBufferLimit
            recvBufferLimit = MIN_BUFFER
            
            // 清理超出新限制的缓冲数据
            if bufferedBytesForWindow() > MIN_BUFFER {
                trimBuffersToFit()
            }
            
            updateAdvertisedWindow()
            log("[Backpressure] Buffer shrunk: \(oldSize) -> \(MIN_BUFFER) bytes")
        }
    }

    private func trimBuffersToFit() {
        // 保留最重要的数据，清理其余部分
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
        // Update sequence numbers
        // 记录背压数据
            backpressure.recordDataReceived(payload.count)
       
            backpressure.updateGrowthTrend()
        
        nextExpectedSequence = sequenceNumber &+ UInt32(payload.count)
        clientSequenceNumber = nextExpectedSequence
        lastAckedSequence = nextExpectedSequence

        // Cancel retransmit timer since we got expected data
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
        
        if backpressure.growthTrend > 0.2 {  // 流量增长超过 20%
            let predictedNeed = Int(Double(recvBufferLimit) * (1 + backpressure.growthTrend))
            let targetSize = min(MAX_BUFFER, predictedNeed)
            let threshold = recvBufferLimit + (recvBufferLimit / 2)  // 相当于 1.5 倍
            if targetSize > threshold {
                expandBufferImmediate(to: targetSize)
                log("[Predictive] Buffer expanded based on growth trend: \(backpressure.growthTrend)")
            }
        }

        // ACK policy: tiny payloads 立即 ACK，其它走延迟 ACK
        if payload.count <= 64 {
            cancelDelayedAckTimer()
            sendPureAck()
        } else {
            scheduleDelayedAck()
        }
    }

    private func processOutOfOrderPacket(payload: Data, sequenceNumber: UInt32) {
        outOfOrderPacketCount += 1

        // Buffer the packet for later processing
        bufferOutOfOrderPacket(payload: payload, sequenceNumber: sequenceNumber)

        // Send immediate ACK with SACK to trigger fast retransmit
        cancelDelayedAckTimer()
        if sackEnabled && peerSupportsSack {
            sendAckWithSack()
        } else {
            sendDuplicateAck()
        }

        // Start retransmit timer if not already running
        if retransmitTimer == nil {
            startRetransmitTimer()
        }
    }

    private func processOverlappingPacket(payload: Data, sequenceNumber: UInt32) {
        // overlap = already-have part length (wrap safe)
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

        // 新增：检测是否需要紧急收缩
        if duplicatePacketCount > 5 && recvBufferLimit > MIN_BUFFER {
            NSLog("[Backpressure] Multiple duplicate ACKs, client may be refusing data")
            shrinkToMinimumImmediate()
        }
    
        // Send immediate ACK to update peer's view of our receive window
        cancelDelayedAckTimer()
        sendPureAck()

        if duplicatePacketCount % 10 == 0 {
            log("Excessive duplicates: \(duplicatePacketCount) total, last seq: \(sequenceNumber)")
        }
    }

    // MARK: - Buffer Management (Optimized)
    private func bufferOutOfOrderPacket(payload: Data, sequenceNumber: UInt32) {
        // Check if already buffered (exact same seq)
            if outOfOrderPackets.contains(where: { $0.seq == sequenceNumber }) { return }

            // Insert in sorted order using wrap-safe comparator
            let packet = (seq: sequenceNumber, data: payload)
            let index = outOfOrderPackets.binarySearch { seqLT($0.seq, sequenceNumber) }
            outOfOrderPackets.insert(packet, at: index)

            // Limit buffer size
            while outOfOrderPackets.count > MAX_BUFFERED_PACKETS {
                outOfOrderPackets.removeFirst()
                log("Buffer overflow: dropped oldest packet")
            }
            
            // 新增：检查总缓冲区使用情况
            if bufferedBytesForWindow() > recvBufferLimit {
                trimBuffers()  // 调用 trimBuffers
                log("Buffer exceeded limit, trimming to \(recvBufferLimit)")
            }
            
            updateAdvertisedWindow() // shrink only
        
        // 限制缓冲区大小
        while outOfOrderPackets.count > MAX_BUFFERED_PACKETS {
            outOfOrderPackets.removeFirst()
            backpressure.recordOverflow()  // 记录溢出
            log("Buffer overflow: dropped oldest packet")
        }
        
        // 检查总缓冲区使用情况
        if bufferedBytesForWindow() > recvBufferLimit {
            backpressure.recordOverflow()  // 记录溢出
            trimBuffers()
            log("Buffer exceeded limit, trimming to \(recvBufferLimit)")
        }
        
    }

    private func processBufferedPackets() {
        var processed = 0

        while !outOfOrderPackets.isEmpty {
            let packet = outOfOrderPackets[0]

            if packet.seq == nextExpectedSequence {
                // now in order
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
                // old packet, discard
                outOfOrderPackets.removeFirst()
            } else {
                // still a gap
                break
            }
        }

        if processed > 0 {
            updateAdvertisedWindow() // window might grow after draining out-of-order
            cancelDelayedAckTimer()
            sendPureAck()

            if outOfOrderPackets.isEmpty {
                cancelRetransmitTimer()
            }
            
            // 新增：检查是否需要调整缓冲区
            adaptBufferToFlow()
        }
        
        if outOfOrderPackets.count > 10 && duplicatePacketCount > 5 {
            // 大量乱序包且重复ACK，可能是客户端问题
            handleClientRefusal()
        }
    }

    // MARK: - ACK Management (Optimized)
    private func scheduleDelayedAck() {
        delayedAckPacketsSinceLast += 1

        // Send immediate ACK every 2 packets
        if delayedAckPacketsSinceLast >= 2 {
            sendPureAck()
            cancelDelayedAckTimer()
            return
        }

        if delayedAckTimer == nil {
            let timer = DispatchSource.makeTimerSource(queue: self.queue)
            timer.schedule(deadline: .now() + .milliseconds(DELAYED_ACK_TIMEOUT_MS))
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

    // MARK: - SACK Generation (Optimized)
    private func generateSackBlocks() -> [(UInt32, UInt32)] {
        guard !outOfOrderPackets.isEmpty else { return [] }

        var blocks: [(UInt32, UInt32)] = []
        var currentStart: UInt32?
        var currentEnd: UInt32?

        for packet in outOfOrderPackets {
            let packetEnd = packet.seq &+ UInt32(packet.data.count)

            if let start = currentStart, let end = currentEnd {
                if seqLE(packet.seq, end) {
                    // Overlapping or contiguous, extend current block (best-effort; wrap unlikely here)
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

    // MARK: - Retransmission Timer (Optimized)
    private func startRetransmitTimer() {
        cancelRetransmitTimer()

        let timer = DispatchSource.makeTimerSource(queue: self.queue)
        let timeout = RETRANSMIT_TIMEOUT_MS * (1 << min(retransmitRetries, 4)) // Exponential backoff
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
            // 新增：超过重传次数，立即收缩
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

        // trigger fast retransmit
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

        updateAdvertisedWindow() // NEW

        let seq = serverInitialSequenceNumber
        let ackNumber = initialClientSequenceNumber &+ 1

        var tcp = Data(count: 40)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)

        withUnsafeBytes(of: seq.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }

        tcp[12] = 0xA0  // Data offset = 10 (40 bytes)
        tcp[13] = TCPFlags([.syn, .ack]).rawValue

        tcp[14] = UInt8(availableWindow >> 8)   // NEW: 动态窗口
        tcp[15] = UInt8(availableWindow & 0xFF) // NEW

        tcp[16] = 0; tcp[17] = 0        // Checksum
        tcp[18] = 0; tcp[19] = 0        // Urgent

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

        // Padding with NOPs (no Window Scale)
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
        updateAdvertisedWindow() // NEW
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

        // 更新记录，避免不必要的 Window Update
        lastAdvertisedWindow = availableWindow
        delayedAckPacketsSinceLast = 0
    }

    private func sendAckWithSack() {
        updateAdvertisedWindow() // NEW
        let sackBlocks = generateSackBlocks()
        if sackBlocks.isEmpty {
            sendPureAck()
            return
        }

        let ackNumber = self.clientSequenceNumber

        // TCP header size with SACK option
        let sackOptionLength = 2 + (sackBlocks.count * 8)
        let tcpOptionsLength = 2 + sackOptionLength  // 2 NOPs + SACK
        let tcpHeaderLength = 20 + tcpOptionsLength
        let paddedLength = ((tcpHeaderLength + 3) / 4) * 4

        var tcp = Data(count: paddedLength)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        withUnsafeBytes(of: serverSequenceNumber.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        tcp[12] = UInt8((paddedLength / 4) << 4)
        tcp[13] = TCPFlags.ack.rawValue
        tcp[14] = UInt8(availableWindow >> 8)   // NEW
        tcp[15] = UInt8(availableWindow & 0xFF) // NEW
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

    // MARK: - Data Forwarding (Optimized)
    private func writeToTunnel(payload: Data) async {
        
        updateAdvertisedWindow() // NEW
        let ackNumber = self.clientSequenceNumber
        var currentSeq = self.serverSequenceNumber
        var packets: [Data] = []

        // Cancel delayed ACK since we're sending data with ACK
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

            // 精确统计：所有 IP+TCP+payload 的总字节
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

        // Send SOCKS5 greeting
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
            
            // 新增：连接建立后可能调整缓冲区
            // 如果是高优先级连接，可以增大缓冲区
            if let priority = getConnectionPriority() {
                let adjustedSize = priority == .high ? recvBufferLimit * 2 : recvBufferLimit
                adjustBufferSize(adjustedSize)
            }
            
            
            startKeepAlive()  // 新增
            
            
            
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
    
    // 添加自适应缓冲区管理
    private func adaptBufferToFlow() {
        let utilizationRate = Double(bufferedBytesForWindow()) / Double(recvBufferLimit)
            
        // 增加滞后区间
        if utilizationRate > 0.9 && recvBufferLimit < 64 * 1024 {
            // 使用率超过90%才扩展
            adjustBufferSize(min(recvBufferLimit * 2, 64 * 1024))
        } else if utilizationRate < 0.1 && recvBufferLimit > 4 * 1024 {
            // 使用率低于20%才收缩（原来是30%）
            adjustBufferSize(max(recvBufferLimit / 2, 4 * 1024))
        }
    }
    
    // 暴露给 ConnectionManager 使用的属性
        public var remotePort: UInt16? { destPort }
        public var remoteIPv4: String? {
            String(describing: destIP)
        }
        public var sniHost: String? {
            // 从 TLS ClientHello 中提取的 SNI（需要实现）
            return extractedSNI
        }
        public var originalHostname: String? { destinationHost }
        
        // 存储提取的 SNI
        private var extractedSNI: String?
        
        // 在处理数据包时提取 SNI
        private func extractSNIFromTLSHandshake(_ data: Data) {
            // TLS ClientHello 解析逻辑
            // 这里是简化版本，实际需要完整解析
            guard data.count > 43,
                  data[0] == 0x16, // TLS Handshake
                  data[5] == 0x01  // ClientHello
            else { return }
            
            // 解析 SNI extension（简化示例）
            // 实际实现需要完整的 TLS 解析
        }
    
    
    
    // 辅助方法：获取连接优先级（可根据端口或域名判断）
    private func getConnectionPriority() -> ConnectionPriority? {
        switch destPort {
            case 5223: return .critical  // Apple Push
            case 443: return .high      // HTTPS
            case 80: return .normal     // HTTP
            default: return .low
        }
    }
    
    
    // 避免并发重入 flush
    private var isFlushing = false

    private func flushPendingData() async {
        guard socksState == .established, !pendingClientBytes.isEmpty else { return }

        // 复制出队列，清空原数组与计数；若部分发送失败，会再回填到队首
        let queue = pendingClientBytes
        pendingClientBytes.removeAll(keepingCapacity: true)

        // 不先清零 pendingBytesTotal，逐块成功后再扣减，失败则回填保持总数正确
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
                    backoffNs = min(backoffNs << 1, 200_000_000) // 封顶 ~200ms
                }
            }

            if sentOK {
                // 成功：从计数中扣减，并尝试通告窗口扩大（会在显著增长时发纯 ACK）
                pendingBytesTotal -= chunk.count
                lastActivityTime = Date()
                maybeSendWindowUpdate(reason: "post-send") // 会在增长显著时写“Window update (post-send)”日志
            } else {
                // 仍失败：把当前块放回队首，停止 flush，留待后续条件更好时再发
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
                    // 没有数据，标记为空闲
                    await self.markFlowIdle()
                }
                
                await self.readFromSocks()
            }
        }
    }
    
    private func markFlowIdle() {
        backpressure.markIdle()
        
        // 如果连续空闲，考虑收缩缓冲区
        if backpressure.consecutiveIdleCycles > 2 {
            evaluateBufferShrinkage()
        }
    }
    
    // MARK: - 公开方法供外部监控
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
        if !backpressure.isActive && backpressure.timeSinceLastData > 0.2 {
            // 超过1秒没有数据流，立即收缩到最小
            shrinkBufferImmediate()
        }
    }
    

    // MARK: - Helper Methods
    private func makeLoopbackParameters() -> NWParameters {
        let tcpOptions = NWProtocolTCP.Options()
        tcpOptions.noDelay = true // 禁用 Nagle 算法，降低延迟
        let p = NWParameters(tls: nil, tcp: tcpOptions) // 使用 TCP 选项初始化参数
        p.requiredInterfaceType = .loopback
        p.allowLocalEndpointReuse = true
        return p
    }

    private func cancelAllTimers() {
        cancelRetransmitTimer()
        cancelDelayedAckTimer()
        cancelKeepAliveTimer()  // 新增
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
        // 释放缓冲 -> 可能需要发 Window Update
        maybeSendWindowUpdate(reason: "cleanupBuffers")
    }

    private func appendToPending(_ data: Data) {
        pendingClientBytes.append(data)
        pendingBytesTotal += data.count

        // 检查是否超过软限制
        if pendingBytesTotal > pendingSoftCapBytes {
            trimPendingBuffer()
        }
        
        // 新增：检查是否超过硬限制（recvBufferLimit）
        if pendingBytesTotal > recvBufferLimit {
            trimBuffers()  // 调用 trimBuffers 进行更激进的清理
            log("Pending bytes exceeded recv limit, aggressive trimming")
        }
        
        updateAdvertisedWindow() // shrink only
    }
    
    private var isEmergencyMemoryPressure: Bool {
        // TODO: 将来这里可以接 ConnectionManager 的内存监控
        return false
    }

    private func trimPendingBuffer() {
        guard !pendingClientBytes.isEmpty else { return }

        if !isEmergencyMemoryPressure { // 新增：常态不丢
            return
        }
        
        var dropped = 0
        let idx = 1 // Keep first segment (usually TLS ClientHello)


        while pendingBytesTotal > pendingSoftCapBytes && idx < pendingClientBytes.count { // PHASE1
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
                    // 新增：立即收缩
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
            
            // 更快的收缩判断（原来是 0.1 秒，改为 0.05 秒）
            if timeSinceLastReceive > 0.05 {
                // 但只在流量真正停止时才收缩
                if !backpressure.isActive && bufferedBytesForWindow() < recvBufferLimit / 4 {
                    evaluateBufferShrinkage()
                }
            }
    }

    private func evaluateBufferShrinkage() {
        let usage = bufferedBytesForWindow()
        let utilizationRate = Double(usage) / Double(recvBufferLimit)
        
        // 只在使用率极低且确认无流量时收缩
        if utilizationRate < 0.05 && !backpressure.isActive &&
           backpressure.timeSinceLastData > 1.0 {
            // 完全空闲，收缩到最小
            shrinkBufferImmediate()
        } else if utilizationRate < 0.2 && backpressure.timeSinceLastData > 0.5 {
            // 低使用率，逐步收缩（但保留更多空间）
            let newSize = max(MIN_BUFFER * 4, recvBufferLimit / 2)
            if newSize < recvBufferLimit {
                recvBufferLimit = newSize
                updateAdvertisedWindow()
                log("[Backpressure] Gradual shrink: -> \(newSize) bytes")
            }
        }
    }
    
    
    @inline(__always)
    private func expandWindowAfterSend() {
        // 1) 重新计算可用空间
        let cap = min(recvBufferLimit, Int(MAX_WINDOW_SIZE))
        let used = bufferedBytesForWindow()         // 你已有的函数
        let free = max(0, cap - used)
        let target = UInt16(free)

        // 2) 若窗口确实变大了，更新并尝试发 Window Update
        if target > availableWindow {
            availableWindow = target
            maybeSendWindowUpdate(reason: "post-send")  // 现有函数，会内部触发纯 ACK
            lastAdvertisedWindow = availableWindow      // 与你现有语义对齐
        }
    }
    
    @inline(__always)
    private func recomputeWindowAndMaybeAck(expand: Bool, reason: String) {
        let prev = availableWindow
        // 统一用“官方”计算：cap - used
        updateAdvertisedWindow()  // 已存在的方法，会更新 availableWindow（cap-used）
        // 收缩：只更新数值，不主动发 ACK（TCP 允许窗口收缩；必要时对端会探测）
        if !expand { return }
        // 扩张：在增长显著（0->正 或 ≥1 MSS）时发 Window Update（纯 ACK）
        if availableWindow > prev {
            maybeSendWindowUpdate(reason: reason)  // 已存在的方法，内部会更新 lastAdvertisedWindow
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
        updateAdvertisedWindow() // NEW
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

        // MSS + SACK-permitted, no WS
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
        updateLastActivity()  // 新增
        
        if !handshakeAcked {
            let expected = serverInitialSequenceNumber &+ 1
            if ackNumber >= expected {
                handshakeAcked = true
                nextExpectedSequence = initialClientSequenceNumber &+ 1
                // Advance our send sequence because SYN consumes one
                serverSequenceNumber = serverInitialSequenceNumber &+ 1
                log("Handshake ACK received")
            }
        }
    }
    
    func onInboundAckWithWindow(ackNumber: UInt32, windowSize: UInt16) {
        updateLastActivity()
            
            // 更新客户端通告窗口
            let oldWindow = clientAdvertisedWindow
            clientAdvertisedWindow = windowSize
            
            // 检测零窗口
            if windowSize == 0 {
                zeroWindowProbeCount += 1
                log("[Window] Client advertised zero window (probe #\(zeroWindowProbeCount))")
                
                // 零窗口持续，考虑收缩
                if zeroWindowProbeCount > 2 {
                    handleClientRefusal()
                }
            } else if oldWindow == 0 && windowSize > 0 {
                // 窗口恢复
                zeroWindowProbeCount = 0
                log("[Window] Client window recovered: \(windowSize)")
            }
            
            // 调用原有的ACK处理
            onInboundAck(ackNumber: ackNumber)
    }
    
    
    // MARK: - 添加收缩原因
    private func shrinkBufferDueToBackpressure() {
        let oldSize = recvBufferLimit
        
        // 基于不同原因选择收缩大小
        let newSize: Int
        if zeroWindowProbeCount > 0 {
            // 客户端窗口问题：收缩到1/4
            newSize = max(MIN_BUFFER, recvBufferLimit / 4)
        } else if duplicatePacketCount > 5 {
            // 重复ACK：收缩到1/2
            newSize = max(MIN_BUFFER, recvBufferLimit / 2)
        } else {
            // 正常收缩：收缩到2/3
            newSize = max(MIN_BUFFER, recvBufferLimit * 2 / 3)
        }
        
        recvBufferLimit = newSize
        updateAdvertisedWindow()
        
        log("[Backpressure] Shrink due to backpressure: \(oldSize) -> \(newSize)")
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
        let context = socksState == .established ? "[EST]" : "[PRE]"
        NSLog("[TCPConnection \(key)] \(context) \(message)")
    }

    func onInboundFin(seq: UInt32) async {
        // 仅在期望的序号位置上推进一个字节（FIN 消耗一个序号）
        if seq == clientSequenceNumber {
            clientSequenceNumber &+= 1
        }
        // 立即回 ACK，避免对端重传 FIN
        sendPureAck()
        // 不主动关闭，由上游（SOCKS/服务端）完成后再走正常关闭流程
    }
    

    // Replace the fixed pending cap constant with a computed property
    private var pendingSoftCapBytes: Int { // PHASE1: smaller cap before SOCKS up
        return (socksState == .established) ? (24 * 1024) : (12 * 1024)
    }
    
    private var keepAliveTimer: DispatchSourceTimer?
    
    private var lastActivityTime = Date()
    private var keepAliveProbesSent = 0
    private let maxKeepAliveProbes = 3
    
    private func startKeepAlive() {
        
        let interval = computeKeepAliveInterval(host: self.destinationHost, port: self.destPort)
        self.keepAliveInterval = interval            // ← 统一来源
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
    
    
    
    // TCPConnection.swift - 在类的属性部分添加
    public var isHighTraffic: Bool = false
    private var overflowCount: Int = 0
    private var lastOverflowTime: Date?

    // 添加获取溢出计数的方法
    public func getOverflowCount() async -> Int {
        return overflowCount
    }

    // 在处理缓冲区溢出的地方增加计数
    private func handleBufferOverflow() async {
        overflowCount += 1
        lastOverflowTime = Date()
        
        let currentSize = recvBufferLimit
        let newSize = min(currentSize * 2, 1024 * 192) // 最大128KB
        
        if currentSize < newSize {
            log("[Buffer] Overflow detected (#\(overflowCount)), expanding from \(currentSize) to \(newSize)")
            adjustBufferSize(newSize)
            isHighTraffic = true
        } else {
            log("[Buffer] Maximum size reached, dropping packets (overflow count: \(overflowCount))")
        }
    }
    
    // MARK: - Keep-Alive Implementation
    private func sendKeepAlive() async {
        // 检查连接是否活跃
        let timeSinceLastActivity = Date().timeIntervalSince(lastActivityTime)
        
        
        // 如果最近有活动，跳过保活
        if timeSinceLastActivity < keepAliveInterval {
            keepAliveProbesSent = 0 // 重置探测计数
            return
        }
        
        // 检查连接状态
        guard socksState == .established else {
            cancelKeepAliveTimer()
            return
        }
        
        // 发送 TCP Keep-Alive 探测（零字节 ACK）
        await sendKeepAliveProbe()
        
        keepAliveProbesSent += 1
        
        // 如果超过最大探测次数，关闭连接
        if keepAliveProbesSent >= maxKeepAliveProbes {
            log("Keep-alive timeout after \(maxKeepAliveProbes) probes, closing connection")
            close()
        }
    }

    private func sendKeepAliveProbe() async {
        // 发送一个零窗口探测包（TCP Keep-Alive）
        // 这是一个包含前一个序列号的 ACK 包
        let probeSeq = serverSequenceNumber &- 1
        let ackNumber = clientSequenceNumber
        
        var tcp = Data(count: 20)
        tcp[0] = UInt8(destPort >> 8); tcp[1] = UInt8(destPort & 0xFF)
        tcp[2] = UInt8(sourcePort >> 8); tcp[3] = UInt8(sourcePort & 0xFF)
        
        withUnsafeBytes(of: probeSeq.bigEndian) { tcp.replaceSubrange(4..<8, with: $0) }
        withUnsafeBytes(of: ackNumber.bigEndian) { tcp.replaceSubrange(8..<12, with: $0) }
        
        tcp[12] = 0x50  // Data offset = 5 (20 bytes)
        tcp[13] = TCPFlags.ack.rawValue
        tcp[14] = UInt8(availableWindow >> 8)
        tcp[15] = UInt8(availableWindow & 0xFF)
        tcp[16] = 0; tcp[17] = 0  // Checksum
        tcp[18] = 0; tcp[19] = 0  // Urgent pointer
        
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

    // 更新活动时间
    private func updateLastActivity() {
        lastActivityTime = Date()
        keepAliveProbesSent = 0  // 重置探测计数
    }
    
    // MARK: - Dynamic Buffer Adjustment
    func adjustBufferSize(_ newSize: Int) {
        let oldSize = recvBufferLimit
        recvBufferLimit = max(4 * 1024, min(newSize, Int(MAX_WINDOW_SIZE)))
        
        if oldSize != recvBufferLimit {
            log("Buffer adjusted: \(oldSize) -> \(recvBufferLimit) bytes")
            updateAdvertisedWindow()
            
            // 如果缓冲区缩小且当前缓存过多，触发清理
            if recvBufferLimit < oldSize && bufferedBytesForWindow() > recvBufferLimit {
                trimBuffers()
            }
        }
    }
    
    // 清理过多的缓冲数据
    private func trimBuffers() {
        // 清理乱序包缓冲区
        while outOfOrderPackets.count > MAX_BUFFERED_PACKETS / 2 {
            outOfOrderPackets.removeFirst()
        }
        
        // 清理待发送数据
        if pendingBytesTotal > recvBufferLimit {
            trimPendingBuffer()
        }
        
        log("Buffers trimmed due to size reduction")
    }
    
    // 添加一个公开方法供 ConnectionManager 调用
    public func handleMemoryPressure(targetBufferSize: Int) async {
        // 调整缓冲区大小
        adjustBufferSize(targetBufferSize)
        
        // 如果缓冲区使用超过新限制，主动清理
        if bufferedBytesForWindow() > recvBufferLimit {
            trimBuffers()
            log("Memory pressure: trimmed buffers to \(recvBufferLimit) bytes")
        }
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


// Client half-close (FIN from client)

}

extension Data {
    fileprivate func hexEncodedString() -> String {
        map { String(format: "%02hhx", $0) }.joined()
    }
}
