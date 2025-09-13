import Foundation
import Network
import os

enum L {
    static func lm(_ s: @autoclosure () -> String) { print("[LayerMinusBridge] \(s())") }
    static func sc(_ s: @autoclosure () -> String) { print("[ServerConnection] \(s())") }
}

public final class LayerMinusBridge {

	// —— 当前连接是否使用 AsyncStream 桥接（而非旧的 pump* 循环）
	private var usingBridge = false

	// —— 只在角色切换后的第一轮 receive 打印一次 maxLen 观测日志
	private var justSwitchedFlag = false

	// MARK: - 二层 role：上行/下行主导
	private enum PrimaryRole { case upstreamPrimary, downstreamPrimary }
	// 主导方向（默认为未知时偏保守：下行主导）
	private var primaryRole: PrimaryRole = .downstreamPrimary
	// 用于判定主导方向的累计流量统计
	private var bytesC2U: Int64 = 0   // client -> upstream
	private var bytesD2C: Int64 = 0   // upstream -> client
	// 切换阈值与裕度（防抖）
	private let ROLE_SWITCH_BASE: Int64 = 4 * 1024     // 4KB
	private let ROLE_HYSTERESIS : Int64 = 32 * 1024     // 32KB 方向裕度

	@inline(__always)
	private func observeBytes(direction: String, n: Int) {
		// direction: "C2U" 或 "D2C"
		if direction == "C2U" {
			bytesC2U &+= Int64(n)
		} else {
			bytesD2C &+= Int64(n)
		}
		maybeSwitchPrimaryRole()
	}

	// 根据累计字节判断是否需要切换主导方向
	private func maybeSwitchPrimaryRole() {
		let c = bytesC2U
		let d = bytesD2C
		// 只有累计到一定规模再判断，避免冷启动时抖动
		guard (c + d) >= ROLE_SWITCH_BASE else { return }

		let newRole: PrimaryRole
		if c > d + ROLE_HYSTERESIS {
			newRole = .upstreamPrimary
		} else if d > c + ROLE_HYSTERESIS {
			newRole = .downstreamPrimary
		} else {
			return
		}
		if newRole != primaryRole {
			// —— 切换前先记录当前（旧 role 下）的 maximumLength
			let oldMaxC2U = computeDesiredMax(isC2U: true)
			let oldMaxD2C = computeDesiredMax(isC2U: false)

			// —— 进行切换
			primaryRole = newRole

			// 让下一轮 receive 打一条“计划窗口”观测日志
			justSwitchedFlag = true

			// —— 切换后再计算一次，打印“前 → 后”的对比
			let newMaxC2U = computeDesiredMax(isC2U: true)
			let newMaxD2C = computeDesiredMax(isC2U: false)
			log(
				"ROLE switch -> \(newRole == .upstreamPrimary ? "UPSTREAM_PRIMARY" : "DOWNSTREAM_PRIMARY") " +
				"C2U=\(c)B D2C=\(d)B " +
				"maxLen(C2U): \(oldMaxC2U) -> \(newMaxC2U)  maxLen(D2C): \(oldMaxD2C) -> \(newMaxD2C) " +
				"pausedC2U=\(pausedC2U) pausedD2C=\(pausedD2C) global=\(Self.globalBufferedBytes)B"
			)
			// 主导方向切换时，及时释放“被最小化侧”的内存，化解压力
			releaseMinimizedSideBuffers()
		}
	}

	// 统一计算本次 receive 的 maximumLength（供 role 切换与 receiveStream 复用）
	@inline(__always)
	private func computeDesiredMax(isC2U: Bool) -> Int {
		var base: Int
		switch primaryRole {
		case .upstreamPrimary:   base = isC2U ? 512*1024 : 4*1024
		case .downstreamPrimary: base = isC2U ? 4*1024   : 512*1024
		}
		// 全局预算吃紧：强制把上限收缩到 ≤16KB
		Self.globalLock.lock()
		let gBytes = Self.globalBufferedBytes
		Self.globalLock.unlock()
		if gBytes > Self.GLOBAL_BUFFER_BUDGET { base = min(base, 16*1024) }
		// 本地背压：强制最小化 4KB
		if isC2U, pausedC2U { base = 4*1024 }
		if !isC2U, pausedD2C { base = 4*1024 }
		return max(1024, base)
	}

	// 尝试释放被最小化侧的缓冲（存在则清空，不存在则忽略）
	private func releaseMinimizedSideBuffers() {

		// 下行最小化（UPSTREAM_PRIMARY）：清 D2C 队列与测速合包缓冲
		if primaryRole == .upstreamPrimary {
			let clearedQ  = self.d2cBufferedBytes
			let clearedSt = self.stBuffer.count
			// 真实清空
			var q = self.d2cQueue; q.removeAll(keepingCapacity: false); self.d2cQueue = q
			var st = self.stBuffer; st.removeAll(keepingCapacity: false); self.stBuffer = st
			self.d2cBufferedBytes = 0
			// 全局预算出账
			let total = clearedQ &+ clearedSt
			if total > 0 { self.subGlobalBytes(total) }
			log("ROLE minimized clear (DOWNSTREAM minimized): d2cQueued=\(clearedQ)B st=\(clearedSt)B global=\(Self.globalBufferedBytes)B")
		} else {
			// 上行最小化（DOWNSTREAM_PRIMARY）：清 C2U 微批缓冲
			let clearedCu = self.cuBuffer.count
			var cu = self.cuBuffer; cu.removeAll(keepingCapacity: false); self.cuBuffer = cu
			if clearedCu > 0 { self.subGlobalBytes(clearedCu) }
			log("ROLE minimized clear (UPSTREAM minimized): cu=\(clearedCu)B global=\(Self.globalBufferedBytes)B")
		}
	}

    // MARK: - Global Memory Monitor (500ms)
    private struct MemoryState {
        var isUnderPressure = false
        var lastCheckTime = DispatchTime.now()
        var consecutivePressureCount = 0
    }
    private static var _memState = MemoryState()
    private static var _memTimer: DispatchSourceTimer?
    private static let _memQueue = DispatchQueue(label: "LayerMinus.mem.monitor")
	// 默认软阈值（MB），可按机型/业务自行调小或调大
	private static let _MEM_SOFT_LIMIT_MB: Int = 48
	private static let _MEM_CHECK_INTERVAL: DispatchTimeInterval = .milliseconds(500)



	//		下行管理
	//
	//// —— 下行（u->c）背压与缓冲
	private var d2cQueue = [Data]()              // 顺序队列
	private var d2cBufferedBytes: Int = 0        // 队列累计字节
	private var d2cSending: Bool = false         // 是否有在途 send
	private var pausedD2C: Bool = false          // 是否已对上游施加背压（暂停 receive）

	// 下行水位：高水位触发暂停上游读，低水位解除
	private var D2C_HIGH_WATER: Int = 512 * 1024     // 512KB
	private var D2C_LOW_WATER:  Int = 256 * 1024     // 256KB（hysteresis）
	private let D2C_MAX_CHUNK:  Int = 64 * 1024      // 单次写入客户端最大块

	@inline(__always)
	private func enqueueDownstream(_ d: Data) {
		// 入队与全局预算入账
		d2cQueue.append(d)
		d2cBufferedBytes &+= d.count
		addGlobalBytes(d.count)

		// 若超高水位或全局预算吃紧，则暂停上游读
		var overGlobal = false
		Self.globalLock.lock()
		overGlobal = (Self.globalBufferedBytes > Self.GLOBAL_BUFFER_BUDGET)
		Self.globalLock.unlock()

		if !pausedD2C && (d2cBufferedBytes >= D2C_HIGH_WATER || overGlobal) {
			pausedD2C = true
			vlog("pause u->c receive due to d2c backpressure: queued=\(d2cBufferedBytes)B global=\(Self.globalBufferedBytes)B")
		}

		// 驱动发送
		sendNextToClientIfIdle()
	}

	@inline(__always)
	private func sendNextToClientIfIdle() {
		guard !d2cSending, !d2cQueue.isEmpty, alive() else { return }

		// 合并小块：尽量凑一个不超过 D2C_MAX_CHUNK 的数据包
		var quota = D2C_MAX_CHUNK
		var merged = Data()
		while quota > 0, !d2cQueue.isEmpty {
			var head = d2cQueue[0]
			if head.count <= quota {
				merged.append(head)
				d2cQueue.removeFirst()
				quota &-= head.count
			} else {
				// 切片发送，剩余部分放回队首（不复制大块，避免扩散内存）
				let part = head.prefix(quota)
				merged.append(part)
				head.removeFirst(quota)
				d2cQueue[0] = head
				quota = 0
			}
		}

		d2cSending = true
		let sz = merged.count
		vlog("send u->c(bp) \(sz)B -> client (queued=\(d2cBufferedBytes)B)")
		client.send(content: merged, completion: .contentProcessed({ [weak self] err in
			guard let s = self, s.alive() else { return }
			s.d2cSending = false
			if let err = err {
				s.log("client send err: \(err)")
				s.queue.async { s.cancel(reason: "client send err") }
				return
			}
			// 发送完成：全局/本地出账
			s.d2cBufferedBytes &-= sz
			s.subGlobalBytes(sz)
			s.vlog("sent u->c(bp) ok, remain queued=\(s.d2cBufferedBytes)B")

			// 若此前因背压暂停上游读，且降到低水位，恢复上游读
			if s.pausedD2C && s.d2cBufferedBytes <= s.D2C_LOW_WATER {
				s.pausedD2C = false
				s.vlog("resume u->c receive after d2c drained: queued=\(s.d2cBufferedBytes)B")
				// s.pumpUpstreamToClient()   // 立刻续上一次 receive 循环

				// 桥接模式下由 receiveStream 自驱动，不再唤醒旧的 pump 循环
				if !s.usingBridge {
					s.pumpUpstreamToClient()
				} else {
					s.vlog("bridge mode: upstream receive auto-loop continues (no pump)")
				}

			}

			// 继续冲队列
			s.sendNextToClientIfIdle()
		}))
	}

	@inline(__always)
	private func adjustD2CWatermarks() {
		// 根据全局水位动态收缩/放大下行水位
		Self.globalLock.lock()
		let g = Self.globalBufferedBytes
		Self.globalLock.unlock()

		if g > Self.GLOBAL_BUFFER_BUDGET {
			// 全局吃紧：更保守
			D2C_HIGH_WATER = 256 * 1024
			D2C_LOW_WATER  = 128 * 1024
		} else {
			// 恢复默认
			D2C_HIGH_WATER = 512 * 1024
			D2C_LOW_WATER  = 256 * 1024
		}
	}


    private static let GLOBAL_BUFFER_BUDGET = 8 * 1024 * 1024
    // —— 100-Continue 兼容：观测到上游 100 后，直到客户端真正发出实体前，避免过早 half-close 上游
    private var saw100Continue = false
    private var bodyBytesAfter100: Int = 0

    // —— Speedtest 特征流：直发与“上行卡住”探测
    private var smallC2UEvents = 0
    private var bytesUpAtFirstSend = 0
    private var uploadStuckTimer: DispatchSourceTimer?
    private var uploadStuck = false
    @inline(__always)
    private var isSpeedtestTarget: Bool {
        // 仅对 8080/443 且域名命中测速特征的连接生效
        let h = targetHost.lowercased()
        return (targetPort == 8080 || targetPort == 443) &&
               (h.hasSuffix("ooklaserver.net")
                 || h.hasSuffix("speedtest.net")
                 || h.contains("measurementlab")
                 || h.contains("mlab"))
    }

	private var currentBufferLimit: Int = 4 * 1024 // 初始大小 4KB
	private let maxBufferLimit: Int = 1 * 1024 * 1024 // 最大大小 1MB
	private var backpressureTimer: DispatchSourceTimer?

	    // 新增：在背压状态下，每 50ms 动态调整一次缓冲区大小
    private func scheduleBackpressureTimer() {
        guard pausedC2U else { return }
        
        // 如果定时器已存在，先取消以避免重复
        backpressureTimer?.cancel()

        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(50), repeating: .milliseconds(50))
        t.setEventHandler { [weak self] in
            guard let s = self, s.alive() else { return }
            
            // 如果背压状态已解除，取消定时器
            if !s.pausedC2U {
                s.backpressureTimer?.cancel()
                s.backpressureTimer = nil
                return
            }
            
            s.adjustBufferLimit()
            s.log("Backpressure timer triggered, new buffer limit: \(s.currentBufferLimit)B")
        }
        backpressureTimer = t
        t.resume()
    }


	private func adjustBufferLimit() {
        let oldLimit = currentBufferLimit
    
		// 获取当前全局缓冲区的安全拷贝
		Self.globalLock.lock()
		let currentGlobalBytes = Self.globalBufferedBytes
		Self.globalLock.unlock()
		
		// 只有在全局预算未超支时才允许个人缓冲增长
		let canGrow = currentGlobalBytes <= Self.GLOBAL_BUFFER_BUDGET
		
		if pausedC2U {
			if canGrow {
				// 在有背压且全局预算充足时，快速增长
				currentBufferLimit = min(currentBufferLimit * 4, maxBufferLimit)
			} else {
				// 全局预算超支时，不再增长，保持当前大小
				self.log("Global budget exceeded, not growing currentBufferLimit.")
			}
		} else {
			// 背压消失时，快速减少
			currentBufferLimit = max(currentBufferLimit / 4, 4 * 1024)
		}
		
		self.log("Adjusting buffer limit: \(oldLimit) -> \(self.currentBufferLimit)")

	}
    
    // —— 发送在途(in-flight)预算，用于约束“直发小包”绕过 cuBuffer 的场景
    private var inflightBytes: Int = 0
    private var inflightSizes = [UInt64: Int]()   // seq -> bytes
    private let INFLIGHT_BYTES_BUDGET = 512 * 1024   // 512KB（可按需调大到 768KB/1MB）
    private let INFLIGHT_COUNT_BUDGET = 256          // 在途包数上限，双保险

    private var sendSeq: UInt64 = 0
    private var inflight = Set<UInt64>()
    
    // --- 上行(c->u)微批：64KB 或 5ms 触发 ---
    private var cuBuffer = Data()
    private var cuFlushTimer: DispatchSourceTimer?
    private let CU_FLUSH_BYTES = 48 * 1024
    private let CU_FLUSH_MS = 10
    // 当定时到点但缓冲小于该值时，允许再延一次以攒到更“胖”的报文
    private let CU_MIN_FLUSH_BYTES = 8 * 1024
    private let CU_EXTRA_MS = 10
    
    // —— 全局预算：限制所有桥接实例合计的缓冲上限（比如 8MB）
    
    private static var globalBufferedBytes: Int = 0
    private static let globalLock = NSLock()

    // —— 回压开关：当本连接的缓冲过大时，暂停 c->u 的继续接收
    private var pausedC2U = false
    
    @inline(__always)
    private func addGlobalBytes(_ n: Int) {
        Self.globalLock.lock(); Self.globalBufferedBytes &+= n; Self.globalLock.unlock()
    }
    @inline(__always)
    private func subGlobalBytes(_ n: Int) {
        Self.globalLock.lock(); Self.globalBufferedBytes &-= n; if Self.globalBufferedBytes < 0 { Self.globalBufferedBytes = 0 }; Self.globalLock.unlock()
    }
    
    
    private let connectInfo: String?
    // 路由元信息（用于统计/打点）
    
    public let id: UInt64
    private let client: NWConnection
    private let targetHost: String
    private let targetPort: Int
    private let verbose: Bool
    
    // --- KPI & 半关闭守护 ---
    private var tStart: DispatchTime = .now()
    private var tReady: DispatchTime?
    private var tFirstByte: DispatchTime?
    private var bytesUp: Int = 0
    private var bytesDown: Int = 0
    private var drainTimer: DispatchSourceTimer?
    // 半关闭后的“空闲超时”窗口：只要仍有对向数据活动就续期，空闲 >= 25s 才收尾
    private let drainGrace: TimeInterval = 25.0
    // 记录哪一侧先 EOF，用于判断是否处于 half-close
    private var eofUpstream = false
    private var eofClient = false

    
    @inline(__always)
    private func appendToCUBuffer(_ d: Data) {
        cuBuffer.append(d)
        addGlobalBytes(d.count)

        if cuBuffer.count >= currentBufferLimit {
            pausedC2U = true
            flushCUBuffer()
            // 新增：启动背压定时器
            scheduleBackpressureTimer()
			// 如果是因为本连接缓冲触顶导致暂停，flush 后排个微延时检查是否可恢复
			scheduleMaybeResumeCheck()
            return
        }

        // ★ 全局预算触发：超出就立即flush并暂停读
        Self.globalLock.lock()
        let overBudget = Self.globalBufferedBytes > Self.GLOBAL_BUFFER_BUDGET
        Self.globalLock.unlock()
        if overBudget {
            pausedC2U = true
            flushCUBuffer()
            // 新增：启动背压定时器
            scheduleBackpressureTimer()
			scheduleMaybeResumeCheck()
            return
        }
    }

	// 新增：flush 后 2ms 再查一次 in-flight，用于打破“回压后僵住”
	private func scheduleMaybeResumeCheck() {
	    let t = DispatchSource.makeTimerSource(queue: queue)
	    t.schedule(deadline: .now() + .milliseconds(2))
	    t.setEventHandler { [weak self] in
	        self?.maybeResumeAfterInflightDrained()
	    }
	    t.resume()
	}

    // —— Speedtest 上传微合并缓冲：4KB 或 4ms 触发，仅测速生效
    private var stBuffer = Data()
    private var stTimer: DispatchSourceTimer?
    private let ST_FLUSH_BYTES = 64 * 1024
    private let ST_FLUSH_MS = 1
    
    @inline(__always)
    private func scheduleSTFlush() {
        
        stTimer?.setEventHandler {}
        stTimer?.cancel()
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(ST_FLUSH_MS))
        t.setEventHandler { [weak self] in
            self?.flushSTBuffer()
        }
        stTimer = t
        t.resume()
    }

    @inline(__always)
    private func flushSTBuffer() {
        guard !stBuffer.isEmpty, !closed else { return }
        let payload = stBuffer
        stBuffer.removeAll(keepingCapacity: true)
        subGlobalBytes(payload.count)

        let (B, _) = self.inflightBudget()
        if self.inflightBytes >= (B * 90) / 100 {
            // 1) 关闭旧定时器
            stTimer?.setEventHandler {}
            stTimer?.cancel()

            // 2) 1ms 后再试一次：优先继续“再合包”一轮（flushSTBuffer），
            //    若 stBuffer 尚未积累，则直接发送 payload
            let t = DispatchSource.makeTimerSource(queue: queue)
            t.schedule(deadline: .now() + .milliseconds(1))
            t.setEventHandler { [weak self] in
                guard let s = self, s.alive() else { return }
                if !s.stBuffer.isEmpty {
                    s.flushSTBuffer()
                } else {
                    s.sendToUpstream(payload, remark: "c->u(st)")
                }
            }
            stTimer = t       // 关键：持有引用，避免定时器被释放
            t.resume()
        } else {
            sendToUpstream(payload, remark: "c->u(st)")
        }
        if pausedC2U { scheduleMaybeResumeCheck() }
    }
    
    
    
    private let onClosed: ((UInt64) -> Void)?
    
    private let queue: DispatchQueue
    private var upstream: NWConnection?
    private var closed = false
    
    // —— 生存门闸：所有回调入口先判存活，避免已取消后仍访问资源
    private let stateLock = NSLock()
    @inline(__always) private func alive() -> Bool {
        stateLock.lock(); defer { stateLock.unlock() }
        return !closed
    }
    
    // 数据面日志（仅在 verbose 为 true 时打印）
    @inline(__always)
    private func vlog(_ msg: String) {
        guard verbose else { return }
        log(msg)
    }
    
    
    init(
        id: UInt64,
        client: NWConnection,
        targetHost: String,
        targetPort: Int,
        verbose: Bool = false,
        connectInfo: String? = nil,
        onClosed: ((UInt64) -> Void)? = nil
    ) {
        self.id = id
        self.client = client
        self.targetHost = targetHost
        self.targetPort = targetPort
        self.verbose = verbose
        self.onClosed = onClosed
        self.connectInfo = connectInfo
        self.queue = DispatchQueue(label: "LayerMinusBridge.\(id)", qos: .userInitiated)
        // 简单的生命周期日志
        NSLog("🟢 CREATED LayerMinusBridge #\(id) for \(targetHost):\(targetPort)\(infoTag())")
    }
    
    deinit {
        log("🔴 DESTROYED LayerMinusBridge #\(id)")
        if !closed {
            log("⚠️ WARNING: LayerMinusBridge #\(id) destroyed without proper closing!")
        }
    }
    
    #if DEBUG
        @inline(__always)
        private func log(_ msg: String) {
            NSLog("[LayerMinusBridge \(id), \(infoTag())] %@", msg)
        }
    #else
        @inline(__always)
        private func log(_ msg: @autoclosure () -> String) { }
    #endif
    
    
    
    // --- 追加KPI ---
    private var tHandoff: DispatchTime?
    private var tFirstSend: DispatchTime?
    
    private func infoTag() -> String {
        guard let s = connectInfo, !s.isEmpty else { return "" }
        return " [\(s)]"
    }
    
    public func start(withFirstBody firstBodyBase64: String) {
        queue.async { [weak self] in
            guard let self = self else { return }
            
            guard self.alive() else { return }
            
            // KPI: 记录会话起点，用于计算 hsRTT / TTFB / 总时长
            self.tStart = .now()
            self.log("start -> \(self.targetHost):\(self.targetPort), firstBody(Base64) len=\(firstBodyBase64.count)")
            
            
            
            // KPI: handoff -> start（应用层排队/解析耗时）
            if let th = self.tHandoff {
                let ms = Double(self.tStart.uptimeNanoseconds &- th.uptimeNanoseconds) / 1e6
                self.log(String(format: "KPI handoff_to_start_ms=%.1f", ms))
            }
            
            guard let firstBody = Data(base64Encoded: firstBodyBase64) else {
                
                self.log("firstBody base64 decode failed")
                self.queue.async { self.cancel(reason: "Invalid Base64") }
                return
            }
            
            self.log("firstBody decoded bytes=\(firstBody.count)")
            
            // 打印前几个字节用于调试
            if firstBody.count > 0 {
                let preview = firstBody.prefix(16).map { String(format: "%02x", $0) }.joined(separator: " ")
                self.log("firstBody preview: \(preview)")
            }
            
            // 开始连接上游并转发数据
            self.connectUpstreamAndRun(firstBody: firstBody)
            
        }
    }

    // MARK: - Backpressure-aware receive stream（按角色/背压/全局预算动态调节窗口）
    private func receiveStream(of conn: NWConnection, role: String) -> AsyncStream<Data> {
        AsyncStream<Data>(bufferingPolicy: .unbounded) { [weak self] continuation in
            guard let s = self else { continuation.finish(); return }
            let q = s.queue

            func loop() {
                // 背压时暂停发起新的 receive（真正对端施压），8ms 后重试
                if (role == "client" && s.pausedC2U) || (role == "upstream" && s.pausedD2C) {
                    q.asyncAfter(deadline: .now() + .milliseconds(8)) { loop() }
                    return
                }
                // 统一用类方法计算当次 maximumLength，确保与切换日志一致
                let maxLen = s.computeDesiredMax(isC2U: (role == "client"))
                // 仅在角色切换后的第一轮打印一次计划窗口，避免刷屏
                if s.verbose, s.justSwitchedFlag {
                    s.vlog("recv plan after role switch: role=\(s.primaryRole) isC2U=\(role == "client") maxLen=\(maxLen) global=\(Self.globalBufferedBytes)B pausedC2U=\(s.pausedC2U) pausedD2C=\(s.pausedD2C)")
                    s.justSwitchedFlag = false
                }
                conn.receive(minimumIncompleteLength: 1, maximumLength: maxLen) { (data, _, isComplete, err) in
                    if let err = err {
                        L.lm("\(role) recv error: \(err)")
                        continuation.finish()
                        return
                    }
                    if let d = data, !d.isEmpty {
                        // 统计方向以驱动 role 切换
                        s.observeBytes(direction: (role == "client") ? "C2U" : "D2C", n: d.count)
                        continuation.yield(d)
                    }
                    if isComplete {
                        L.lm("\(role) EOF")
                        continuation.finish()
                        return
                    }
                    loop()
                }
            }
            loop()
        }
    }

    // Bridge both directions concurrently. No queues, no watermarks, no local buffers.
    private func bridgeConnections(client: NWConnection, remote: NWConnection) async {
        await withTaskGroup(of: Void.self) { group in
            // client -> remote
            group.addTask {
                for await chunk in self.receiveStream(of: client, role: "client") {
                    remote.send(content: chunk, completion: .contentProcessed { _ in })
                }
                // Half-close remote write when client finished sending
                remote.send(content: nil, completion: .contentProcessed { _ in })
            }
            // remote -> client
            group.addTask {
               for await chunk in self.receiveStream(of: remote, role: "upstream") {
                    client.send(content: chunk, completion: .contentProcessed { _ in })
                }
                // Half-close client write when remote finished sending
                client.send(content: nil, completion: .contentProcessed { _ in })
            }
            // Wait for both directions to complete
            for await _ in group { }
        }
        // Tear down after bridge completes
        self.cancel(reason: "bridge completed")
    }
    
    
    public func cancel(reason: String) {

		log("CANCEL trigger id=\(id) reason=\(reason) inflightBytes=\(inflightBytes) inflightCount=\(inflight.count) cu=\(cuBuffer.count)B st=\(stBuffer.count)B global=\(Self.globalBufferedBytes)B pausedC2U=\(pausedC2U)")
        // 幂等关闭，防止重入/竞态
        stateLock.lock()
        if closed { stateLock.unlock(); return }
        closed = true
        stateLock.unlock()
        
        // 停止所有守护/缓冲（置空 handler 防循环引用）
        firstByteWatchdog?.setEventHandler {}

        firstByteWatchdog?.cancel()
        firstByteWatchdog = nil

        drainTimer?.setEventHandler {}

        drainTimer?.cancel()
        drainTimer = nil

        uploadStuckTimer?.setEventHandler {}

        uploadStuckTimer?.cancel()
        uploadStuckTimer = nil

        cuFlushTimer?.setEventHandler {}

        cuFlushTimer?.cancel()
        cuFlushTimer = nil

        let leftover = cuBuffer.count
        if leftover > 0 { subGlobalBytes(leftover) }
        
        
        stTimer?.setEventHandler {}
        
        stTimer?.cancel()
        stTimer = nil

        let stLeft = stBuffer.count
        if stLeft > 0 { subGlobalBytes(stLeft) }
        
        inflightSizes.removeAll(keepingCapacity: false)
        inflight.removeAll(keepingCapacity: false)
        inflightBytes = 0
        
        cuBuffer.removeAll(keepingCapacity: false)
        
        kpiLog(reason: reason)
        log("cancel: \(reason)")
        
        // 关闭上游连接
        upstream?.cancel()
        upstream = nil
        
        // 关闭客户端连接
        client.cancel()
        
        // 通知 ServerConnection
        onClosed?(id)
    }
    
    private func connectUpstreamAndRun(firstBody: Data) {
        guard let port = NWEndpoint.Port(rawValue: UInt16(targetPort)) else {
            log("invalid port \(targetPort)")
            cancel(reason: "invalid port")
            return
        }
        
        let host = NWEndpoint.Host(targetHost)
        let params = NWParameters.tcp
        
        // // 配置 TCP 参数（更稳妥的默认：443/80 保持 noDelay；keepalive 稍放宽）
        if let tcp = params.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
            // 非测速保持原来的偏好；测速目标关闭 noDelay，降低发送队列压力
            let preferNoDelay = true  // 测速与非测速都开；若只想对测速开：self.isSpeedtestTarget ? true : (targetPort == 443 || targetPort == 80 || targetPort == 8080)
            tcp.noDelay = preferNoDelay
            tcp.enableKeepalive = true
            tcp.keepaliveIdle = 30
        }
        
        
        log("Connecting to upstream \(targetHost):\(targetPort)")
        
        let up = NWConnection(host: host, port: port, using: params)
        upstream = up
        
        up.stateUpdateHandler = { [weak self] st in
            guard let self = self else { return }
            
            guard self.alive() else { return }
            
            
            switch st {
            case .ready:
                self.log("upstream ready to \(self.targetHost):\(self.targetPort)")
                // KPI: 记录上游就绪时刻（握手完成）
                self.tReady = .now()
                // 发送首包到上游
                if !firstBody.isEmpty {
                    
                    // 记录首包发送完成时刻（用于 firstSend -> firstRecv）
                    self.sendToUpstream(firstBody, remark: "firstBody")
                }
                
                // 启动双向数据泵
                // self.pumpClientToUpstream()
                // self.pumpUpstreamToClient()

                // 启动简单桥接（AsyncStream/Task，去掉内存管理）

				self.usingBridge = true

                Task { [weak self] in
                    guard let s = self else { return }
                    await s.bridgeConnections(client: s.client, remote: up)
                }

            case .waiting(let error):
                self.log("upstream waiting: \(error)")
                
            case .failed(let error):
                self.log("upstream failed: \(error)")
                self.queue.async { self.cancel(reason: "upstream failed") }
                
            case .cancelled:
                self.log("upstream cancelled")
                self.queue.async { self.cancel(reason: "upstream cancelled") }
                
            default:
                self.log("upstream state: \(st)")
            }
        }
        
        up.start(queue: queue)
    }

	// 来自 ServerConnection 的 handoff 瞬间标记
	public func markHandoffNow() {
		queue.async { [weak self] in
			self?.tHandoff = .now()
		}
	}
    
    @inline(__always)
    private func maybeResumeAfterInflightDrained() {
        let (B, C) = self.inflightBudget()
        if self.pausedC2U &&
           self.inflightBytes <= (B * 90) / 100 &&      // 原来 75%
           self.inflight.count <= (C * 90) / 100 {
            self.pausedC2U = false
            self.vlog("resume c->u after inflight drained: bytes=\(self.inflightBytes) count=\(self.inflight.count)")
            // 桥接模式下由 receiveStream 自驱动，不再唤醒旧的 pump 循环
            if !self.usingBridge {
                self.pumpClientToUpstream()
            } else {
                self.vlog("bridge mode: client receive auto-loop continues (no pump)")
            }
        }
    }
    
    private func scheduleCUFlush(allowExtend: Bool = true) {
        cuFlushTimer?.setEventHandler {}   // 新增：先清 handler
        cuFlushTimer?.cancel()
        
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(CU_FLUSH_MS))
        
        t.setEventHandler { [weak self] in
            guard let s = self, s.alive() else { return }
            
            // 若到点仍很小且允许再延一次，则小幅延时后再冲刷
            if !s.closed, allowExtend, s.cuBuffer.count > 0, s.cuBuffer.count < s.CU_MIN_FLUSH_BYTES {
                s.cuFlushTimer?.cancel()
                let t2 = DispatchSource.makeTimerSource(queue: s.queue)
                t2.schedule(deadline: .now() + .milliseconds(s.CU_EXTRA_MS))
                
                t2.setEventHandler { [weak s] in
                    guard let s = s, s.alive() else { return }
                    s.flushCUBuffer()
                }
                
                
                s.cuFlushTimer = t2
                t2.resume()
            } else {
                s.flushCUBuffer()
            }
        }
        cuFlushTimer = t
        t.resume()
    }
    
    @inline(__always)
    private func inflightBudget() -> (bytes: Int, count: Int) {
        return isSpeedtestTarget ? (3_000_000, 1200) : (768 * 1024, 320)
    }

    private func flushCUBuffer() {
        guard !cuBuffer.isEmpty, !closed else { return }
        let payload = cuBuffer
        cuBuffer.removeAll(keepingCapacity: true)

        // —— 扣减全局水位
        subGlobalBytes(payload.count)

        self.sendToUpstream(payload, remark: "c->u")

        // —— 如果是回压态，且缓冲已清空，则恢复继续读客户端
        if pausedC2U && cuBuffer.isEmpty {
            self.maybeResumeAfterInflightDrained()
        }
    }
    
    private func pumpClientToUpstream() {
        if closed { return }
        
        client.receive(minimumIncompleteLength: 1, maximumLength: 256 * 1024) { [weak self] (data, _, isComplete, err) in
            
            
            guard let self = self else { return }
            if !self.alive() { return }
            
            if let err = err {
                self.log("client recv err: \(err)")
                
                // 避免在 receive 回调栈内同步取消，引发重入/竞态
                self.queue.async { self.cancel(reason: "client recv err") }
                
                
                return
            }
            
            if let d = data, !d.isEmpty {
                
                self.vlog("recv from client: \(d.count)B")

                // —— 仅对测速流的 30–100B 小块“直发”，其余仍按微批策略处理
                if self.isSpeedtestTarget && (1...300).contains(d.count) {
                    self.smallC2UEvents &+= 1

                    // 🔸 改为：测速上传微合并（首包已发出后才启动，避免影响握手）
                    if self.tFirstSend != nil {
                        self.stBuffer.append(d)
                        self.addGlobalBytes(d.count)

                        // 全局预算触发（与 cuBuffer 一致的防线）：立即 flush 并暂停读
                        Self.globalLock.lock()
                        let overBudget = Self.globalBufferedBytes > Self.GLOBAL_BUFFER_BUDGET
                        Self.globalLock.unlock()

                        if self.pausedC2U && self.cuBuffer.count < Int(Double(self.currentBufferLimit) * 0.5) { // 设定一个阈值来解除背压
							self.pausedC2U = false
							self.adjustBufferLimit()
							self.backpressureTimer?.cancel()
							self.backpressureTimer = nil
						}
						
                        if overBudget || self.stBuffer.count >= self.currentBufferLimit {

							if self.pausedC2U == false {
								self.pausedC2U = true
								self.adjustBufferLimit() // 动态调整
								scheduleBackpressureTimer()
							}

                        } else if self.stBuffer.count >= self.ST_FLUSH_BYTES {
                            self.flushSTBuffer()
                        } else {
                            self.scheduleSTFlush()
                        }
                    } else {
                        // 首包未完成前仍保留原直发（避免影响握手）
                        self.sendToUpstream(d, remark: "c->u")
                    }
                } else {
                    // 其它：累积到缓冲，达到阈值立即冲刷，否则启动短定时器
                    self.appendToCUBuffer(d)
                }


                // 若已收到上游的 100-Continue，则统计 100 之后客户端是否真的发了实体
                if self.saw100Continue {
                    self.bodyBytesAfter100 &+= d.count
                }


//                if !self.isSpeedtestTarget {
//                    if self.cuBuffer.count >= self.CU_FLUSH_BYTES { self.flushCUBuffer() }
//                    else { self.scheduleCUFlush() }
//                }

                // 仅当本次不是“测速小块直发”时，才参与微批触发判断
                if !(self.isSpeedtestTarget && (1...300).contains(d.count)) {
                    if self.cuBuffer.count >= self.CU_FLUSH_BYTES { self.flushCUBuffer() }
                    else { self.scheduleCUFlush() }
                }
            }
            
            
            if isComplete {
                self.log("client EOF")
                
                self.eofClient = true

                // 先把缓冲冲刷出去，再进入排水期/或酌情 half-close
                self.flushCUBuffer()
                
                self.flushSTBuffer()   // 把测速合包缓冲也冲掉，释放全局预算

                // ★ 若已观测到上游发过 100-Continue，但客户端尚未发送任何实体，
                //   暂不 half-close 上游（避免把请求体“宣告写完”）；仅进入空闲计时，等待自然收尾
                if (self.saw100Continue && self.bodyBytesAfter100 == 0) || self.uploadStuck {
                    self.scheduleDrainCancel(hint: "client EOF (deferred half-close due to 100-Continue)")
                } else {
                    // 常规路径：half-close 上游写端，再进入排水
                    self.upstream?.send(content: nil, completion: .contentProcessed({ _ in }))
                    self.scheduleDrainCancel(hint: "client EOF")
                }
                
                return
            }
            
            // 继续接收
            if !self.pausedC2U {
                self.pumpClientToUpstream()
            } else {
                self.vlog("pause c->u receive due to backpressure")
            }
        }
    }
    
    
    
    private func scheduleDrainCancel(hint: String) {
        // half-close 空闲计时器：每次调用都会重置，确保有活动就不收尾
        drainTimer?.setEventHandler {}   // 新增
        drainTimer?.cancel()
        
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + drainGrace)
        timer.setEventHandler { [weak self] in
            guard let s = self, s.alive() else { return }
            s.cancel(reason: "drain timeout after \(hint)")
        }
        drainTimer = timer
        timer.resume()
    }
    
    private func pumpUpstreamToClient() {
        if closed { return }
        
        upstream?.receive(minimumIncompleteLength: 1, maximumLength: 128 * 1024) { [weak self] (data, _, isComplete, err) in
            
            guard let self = self else { return }
            if !self.alive() { return }
            
            if self.closed { return }
            
            if let err = err {
                self.log("upstream recv err: \(err)")
                self.queue.async { self.cancel(reason: "upstream recv err") }
                return
            }
            
            if let d = data, !d.isEmpty {
                self.vlog("recv from upstream: \(d.count)B")
                if self.tFirstByte == nil {
                    self.tFirstByte = .now()
                    
                    self.firstByteWatchdog?.setEventHandler {}
                    self.firstByteWatchdog?.cancel()
                    self.firstByteWatchdog = nil
                    
                
                    // KPI: 即时打印首字节到达延迟 (TTFB)
                    let ttfbMs = Double(self.tFirstByte!.uptimeNanoseconds &- self.tStart.uptimeNanoseconds) / 1e6
                    self.log(String(format: "KPI immediate TTFB_ms=%.1f", ttfbMs))
                    // KPI: 首包发送完成 -> 首字节回流（纯传输/排队段）
                    if let ts = self.tFirstSend {
                        let segMs = Double(self.tFirstByte!.uptimeNanoseconds &- ts.uptimeNanoseconds) / 1e6
                        self.log(String(format: "KPI firstSend_to_firstRecv_ms=%.1f", segMs))
                    }
                }
                
                // ★ 轻量识别 100-Continue（只看明文前缀；TLS 情况下这里拿到的是解密后的应用数据）
                if !self.saw100Continue {
                    if let s = String(data: d.prefix(16), encoding: .ascii),
                       s.hasPrefix("HTTP/1.1 100") || s.hasPrefix("HTTP/1.0 100") {
                        self.saw100Continue = true
                        self.log("observed upstream HTTP 100-Continue")
                    }
                }
                
				adjustD2CWatermarks()
                self.bytesDown &+= d.count
                self.enqueueDownstream(d)

                // 若客户端已 EOF（half-close），但上游仍在下发数据，则刷新空闲计时器，避免早收尾
                if self.eofClient {
                    self.scheduleDrainCancel(hint: "client EOF (upstream->client activity)")
                }
                
            }
            
            if isComplete {
                self.log("upstream EOF")
                self.eofUpstream = true
                // 半关闭客户端写入，进入排水期
                self.client.send(content: nil, completion: .contentProcessed({ _ in }))
                self.scheduleDrainCancel(hint: "upstream EOF")
                return
            }
            
            // 继续接收
            // 继续接收：若因下行背压暂停，则暂不继续读上游
            if !self.pausedD2C {
                self.pumpUpstreamToClient()
            } else {
                self.vlog("pause u->c receive loop due to d2c backpressure (queued=\(self.d2cBufferedBytes)B)")
            }
        }
    }
    
    // 首包回包看门狗（避免黑洞 60–100s 挂死；测速上传场景按策略放宽/禁用）
    private var firstByteWatchdog: DispatchSourceTimer?
    
    private func sendToUpstream(_ data: Data, remark: String) {
        
        guard let up = upstream, alive() else {
            log("Cannot send to upstream: upstream=\(upstream != nil), closed=\(closed)")
            return
        }
        
        let seq = { sendSeq &+= 1; return sendSeq }()
        inflight.insert(seq)
        
        let (B, C) = self.inflightBudget()

        // —— 入账在途体积，并在超预算时暂停 c->u 读取
        let sz = data.count
        inflightSizes[seq] = sz
        inflightBytes &+= sz
        
        if inflightBytes >= B || inflight.count >= C {
            if !pausedC2U {
                pausedC2U = true
                vlog("pause c->u due to inflight budget: bytes=\(inflightBytes) count=\(inflight.count)")
            }
        }
        

        vlog("send \(remark) \(data.count)B -> upstream #\(seq)")
        
        
        
        up.send(content: data, completion: .contentProcessed({ [weak self] err in
            
            guard let self = self, self.alive() else { return }

			// —— ★★ 无论如何，先做一次出账（只会在第一次回调时成功扣减）
			let debited: Int = {
				if let n = self.inflightSizes.removeValue(forKey: seq) {
					self.inflightBytes &-= n
					if self.inflightBytes < 0 { self.inflightBytes = 0 }
					return n
				}
				return 0
			}()
            
            // 去重：只处理一次完成回调

			let firstCompletion = self.inflight.remove(seq) != nil
			if !firstCompletion {
				// 已经处理过：只补个日志并尝试恢复读
				self.log("WARN dup completion for #\(seq), ignore (debited=\(debited))")
				self.maybeResumeAfterInflightDrained()
				return
			}

			if let err = err {
				self.log("upstream send err: \(err)")
				self.queue.async { self.cancel(reason: "upstream send err") }
				return
			}
			


            // 发送成功日志 + 计数
            if remark == "firstBody" && self.tFirstSend == nil {
                self.tFirstSend = .now()
                self.log("sent firstBody successfully (mark tFirstSend)")

                // —— Speedtest “上行卡住”探测：首包发出后启动 8s 观察窗口
                if self.isSpeedtestTarget {
                    self.bytesUpAtFirstSend = self.bytesUp
                    self.uploadStuckTimer?.cancel()
                    let t = DispatchSource.makeTimerSource(queue: self.queue)

                    t.schedule(deadline: .now() + .seconds(10))
                    t.setEventHandler { [weak self] in
                        guard let s = self, s.alive() else { return }
                        let upDelta = max(0, s.bytesUp - s.bytesUpAtFirstSend)
                        if upDelta < 32 * 1024 && s.smallC2UEvents >= 10 && !s.uploadStuck {
                            s.uploadStuck = true
                            s.log("UPLOAD_STUCK origin=\(s.targetHost):\(s.targetPort) up=\(upDelta)B down=\(s.bytesDown)B small_events=\(s.smallC2UEvents)")
                            // 仅延后收尾由 drain idle 控制（不主动 half-close）
                            s.scheduleDrainCancel(hint: "UPLOAD_STUCK")
                        }
                    }
                    
                    self.uploadStuckTimer = t
                    t.resume()
                }


            } else {
                self.vlog("sent \(remark) successfully")
            }

            self.maybeResumeAfterInflightDrained()

            self.bytesUp &+= data.count
            // 仅在首包时设置 TTFB 看门狗
            if remark == "firstBody" {
                
				// 测速流（无论 443 或 8080）禁用/放宽 watchdog，HTTPS 再放宽一些
				let disableWatchdog = self.isSpeedtestTarget
				let isHTTPS = (self.targetPort == 443)
				let watchdogDelay: TimeInterval = disableWatchdog ? 20.0 : (isHTTPS ? 15.0 : 8.0)

				let wd = DispatchSource.makeTimerSource(queue: self.queue)
                wd.schedule(deadline: .now() + watchdogDelay)
                wd.setEventHandler { [weak self] in
                    
                    guard let s = self, s.alive() else { return }
                    
                    s.log("KPI watchdog: no first byte within \(Int(watchdogDelay*1000))ms after firstBody; fast-fail")
                    s.cancel(reason: "first_byte_timeout")
                }
                self.firstByteWatchdog = wd
                wd.resume()
            }
        }))
    }
    
    private func kpiLog(reason: String) {
        let now = DispatchTime.now()
        let durMs = Double(now.uptimeNanoseconds &- tStart.uptimeNanoseconds) / 1e6
        let hsMs: Double? = tReady.map { diffMs(start: tStart, end: $0) }
        let fbMs: Double? = tFirstByte.map { diffMs(start: tStart, end: $0) }
        func f(_ x: Double?) -> String { x.map { String(format: "%.1f", $0) } ?? "-" }
        log("KPI host=\(targetHost):\(targetPort) reason=\(reason) hsRTT_ms=\(f(hsMs)) ttfb_ms=\(f(fbMs)) up_bytes=\(bytesUp) down_bytes=\(bytesDown) dur_ms=\(String(format: "%.1f", durMs))")
    }
    

    private func sendToClient(_ data: Data, remark: String) {
        guard alive() else {
            log("Cannot send to client: closed=\(closed)")
            return
        }
        
        vlog("send \(remark) \(data.count)B -> client")
        
        client.send(content: data, completion: .contentProcessed({ [weak self] err in
            guard let self = self, self.alive() else { return }
            if let err = err {
                self.log("client send err: \(err)")
                self.queue.async { self.cancel(reason: "client send err") }
            } else {
                self.vlog("sent \(remark) successfully")
            }
        }))
    }
    
    private func diffMs(start: DispatchTime, end: DispatchTime) -> Double {
        return Double(end.uptimeNanoseconds &- start.uptimeNanoseconds) / 1e6
    }
}
