import Foundation
import Network
import os

#if DEBUG
import Darwin.Mach // for task_info / mach_task_basic_info
#endif

#if DEBUG
enum L {
    static func lm(_ s: @autoclosure () -> String) { print("[LayerMinusBridge] \(s())") }
    static func sc(_ s: @autoclosure () -> String) { print("[ServerConnection] \(s())") }
}
#else
enum L {
    @inline(__always) static func lm(_ s: @autoclosure () -> String) { /* no-op in Release */ }
    @inline(__always) static func sc(_ s: @autoclosure () -> String) { /* no-op in Release */ }
}
#endif


private extension LayerMinusBridge {
    // 内存管理常量
    static let ABSOLUTE_MAX_BUFFER = 512 * 1024  // 512KB 绝对上限
    static let MEMORY_PRESSURE_BUFFER = 64 * 1024  // 内存压力时缓冲区
    static let MEMORY_WARNING_THRESHOLD = 50 * 1024 * 1024  // 50MB 警告阈值

}


public final class LayerMinusBridge {

	// MARK: - RTT 自适应调参状态（新增）
	private enum RttBand { case low, mid, high }
	private var rttBand: RttBand = .mid
	private var lastTuneAt: CFAbsoluteTime = 0
	private let tuneCooldownMs: Int = 2000   // 2s 冷却，避免抖动
	private let lowEnterMs = 20              // 进入低 RTT 阈值
	private let lowExitMs  = 30              // 退出低 RTT 的滞回
	private let highEnterMs = 100            // 进入高 RTT 阈值
	private let highExitMs  = 80             // 退出高 RTT 的滞回


	// 添加这些新属性
    private var hasStarted = false
    private var startTime: DispatchTime?
    private static let MIN_LIFETIME_MS: Double = 100.0
    
	 // 用于防止过早释放的引用池
    private static var activeBridges = [UInt64: LayerMinusBridge]()
    private static let bridgesLock = NSLock()
	
	// [DYNAMIC-ROLE] role election
	private var roleFinalized = false          // 是否已决定上下行
	private var electionStarted = false        // 是否已启动竞速监听
	private var connReq: NWConnection?         // 固定指向 reqHost:reqPort 那条连接
	private var connRes: NWConnection?         // 固定指向 resHost:resPort 那条连接

	@inline(__always)
	private func swapUpDown() {
		guard let up = upstream, let dn = downstream, up !== dn else { return }
		let oldUp = up
		upstream = dn
		downstream = oldUp
		log("dynamic-role: swapped roles (up<->down)")
	}

	// [DYNAMIC-ROLE] 首次下行数据快速路径（用于竞速监听的胜者首包）
	@inline(__always)
	private func handleFirstDownstreamBytes(_ d: Data, source: String) {
		var data = d
		// 先扣掉我们自己发出的 RES-FIRST 回显，避免误计入 TTFB/KPI
		if downIgnoreBytes > 0 {
			let drop = min(downIgnoreBytes, data.count)
			downIgnoreBytes -= drop
			if drop == data.count {
				return // 本批全是回显字节，直接丢弃，不触发任何 KPI
			} else {
				data.removeFirst(drop)
			}
		}

		// KPI 首字节
		if tFirstByte == nil {
			tFirstByte = .now()
			if let dr = tDownReady {
				let ms = diffMs(start: dr, end: tFirstByte!)
				log(String(format:"KPI downReady_to_firstRecv_ms=%.1f", ms))
			}
			firstByteWatchdog?.setEventHandler {}
			firstByteWatchdog?.cancel()
			firstByteWatchdog = nil

			let ttfbMs = Double(tFirstByte!.uptimeNanoseconds &- tStart.uptimeNanoseconds) / 1e6
			log(String(format: "KPI immediate TTFB_ms=%.1f (via %@)", ttfbMs, source))
			if let ts = tFirstSend {
				let segMs = Double(tFirstByte!.uptimeNanoseconds &- ts.uptimeNanoseconds) / 1e6
				log(String(format: "KPI firstSend_to_firstRecv_ms=%.1f", segMs))
			}
		}

		// 100-Continue 观察（与正常下行一致）
		if !saw100Continue {
			if let s = String(data: d.prefix(16), encoding: .ascii),
			s.hasPrefix("HTTP/1.1 100") || s.hasPrefix("HTTP/1.0 100") {
				saw100Continue = true
				log("observed downstream HTTP 100-Continue (race-first)")
			}
		}

		bytesDown &+= data.count
		sendToClient(data, remark: "down->client(race-first)")
	}

	private struct MemoryState {
        var isUnderPressure = false
        var lastCheckTime = DispatchTime.now()
        var consecutivePressureCount = 0
    }

	private var memoryState = MemoryState()

	private func checkAndHandleMemoryPressure() -> Bool {
        // 限制检查频率
        let now = DispatchTime.now()
        let elapsed = diffMs(start: memoryState.lastCheckTime, end: now)
        guard elapsed >= 1000 else { // 最多每秒检查一次
            return memoryState.isUnderPressure
        }
        memoryState.lastCheckTime = now
        
        // 获取内存信息
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout.size(ofValue: info) / MemoryLayout<natural_t>.size)
        
        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count)
            }
        }
        
        guard result == KERN_SUCCESS else { return false }
        
        let residentMB = Double(info.resident_size) / (1024.0 * 1024.0)
        let physicalMB = Double(ProcessInfo.processInfo.physicalMemory) / (1024.0 * 1024.0)
        let ratio = residentMB / physicalMB
        
        // 动态阈值：根据设备内存调整
        let threshold: Double = physicalMB > 4096 ? 0.05 : 0.03  // 4GB以上设备5%，否则3%
        
        if ratio > threshold || residentMB > 100 {  // 超过阈值或绝对值超过100MB
            memoryState.consecutivePressureCount += 1
            
            if memoryState.consecutivePressureCount >= 2 && !memoryState.isUnderPressure {
                log("Memory pressure ON: \(Int(residentMB))MB (\(Int(ratio*100))%)")
                memoryState.isUnderPressure = true
                handleMemoryPressure()
            }
            return true
        } else {
            if memoryState.isUnderPressure && ratio < threshold * 0.8 {
                log("Memory pressure OFF: \(Int(residentMB))MB")
                memoryState.isUnderPressure = false
            }
            memoryState.consecutivePressureCount = 0
            return false
        }
    }
    
    private func handleMemoryPressure() {
        queue.async { [weak self] in
            guard let self = self else { return }
            
            autoreleasepool {
                // 立即清理缓冲区
                if !self.cuBuffer.isEmpty {
                    self.flushCUBuffer()
                }
                if !self.stBuffer.isEmpty {
                    self.flushSTBuffer()
                }
                
                // 降低缓冲区限制
                self.currentBufferLimit = min(self.currentBufferLimit, Self.MEMORY_PRESSURE_BUFFER)
                self.downMaxRead = min(self.downMaxRead, 16 * 1024)
                
                // 暂停非关键定时器
                self.safeStopTimer(&self.roleTimer)
                self.safeStopTimer(&self.backpressureTimer)
            }
        }
    }

	private static let qKey = DispatchSpecificKey<UInt8>()

    @inline(__always)
    private func sendToDownstream(_ data: Data, remark: String) {
        guard let dn = downstream, alive() else {
            log("Cannot send to downstream: downstream=\(downstream != nil), closed=\(closed)")
            return
          }
          dn.send(content: data, completion: .contentProcessed({ [weak self] err in
				autoreleasepool {
					guard let self = self, self.alive() else { return }
					if let err = err {
						self.log("downstream send err: \(err)")
						self.queue.async { self.cancel(reason: "downstream send err") }
					} else {
						self.vlog("sent \(remark) \(data.count)B -> downstream ok")
					}
				}
          }))
    }


    // 动态全局缓冲水位：≥4GB 设备放宽到 12MiB，否则维持 8MiB
    private static var GLOBAL_BUFFER_BUDGET: Int = {
        let physicalMB = Double(ProcessInfo.processInfo.physicalMemory) / (1024.0 * 1024.0)
        return (physicalMB > 4096 ? 12 : 8) * 1024 * 1024
    }()

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
		let h = resHost.lowercased()
		let p = resPort
		if (p == 443 || p == 8080) &&
			(h.hasSuffix("speedtest.net")
				|| h.hasSuffix("ooklaserver.net")
				|| h.contains("measurementlab")
				|| h.contains("mlab")
				|| h.hasSuffix("fast.com")) {
			return true
		}
		return false
    }

		/// 僅當“像是上傳測速”時才開啟 1ms 上行微合併與關閉 TTFB 看門狗
	/// 啟發式：目標為測速站，且自 UP ready 起超過 1500ms 仍未見任何下行首字節
	@inline(__always)
	private var isSpeedtestUploadMode: Bool {
		guard isSpeedtestTarget else { return false }
		if let r = tReady {
			let ageMs = diffMs(start: r, end: .now())
			if tFirstByte == nil && ageMs >= 1500 { return true }
		}
		return false
	}
    
    @inline(__always)
    private func safeStopTimer(_ t: inout DispatchSourceTimer?) {
		 guard let timer = t else { return }
			t = nil
			
			timer.setEventHandler {}  // 断电，避免尾随触发
			// 同队列：不等待，直接 cancel，防止死锁
			if DispatchQueue.getSpecific(key: Self.qKey) != nil {
				timer.cancel()
				return
			}
			let sem = DispatchSemaphore(value: 0)
			timer.setCancelHandler { sem.signal() }
			timer.cancel()
			_ = sem.wait(timeout: .now() + .milliseconds(50))
    }
    
    #if DEBUG
    private var memSummaryTimer: DispatchSourceTimer?
    #endif
    

    @inline(__always)
    public func processResidentSizeMB() -> Double? {
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout.size(ofValue: info) / MemoryLayout<natural_t>.size)
        let kr: kern_return_t = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count)
            }
        }
        if kr == KERN_SUCCESS {
            return Double(info.resident_size) / (1024.0 * 1024.0)
        }
        return nil
    }
    

    

    private var currentBufferLimit: Int = 4 * 1024 // 初始大小 4KB
    private let maxBufferLimit: Int = 256 * 1024 // 最大大小 256KB
    private var backpressureTimer: DispatchSourceTimer?

        // 新增：在背压状态下，每 50ms 动态调整一次缓冲区大小
    private func scheduleBackpressureTimer() {
        guard pausedC2U, backpressureTimer == nil else { return } // 只排一次
		let t = DispatchSource.makeTimerSource(queue: queue)
		t.schedule(deadline: .now() + .milliseconds(100))         // 轻推 100ms
		t.setEventHandler { [weak self] in
			autoreleasepool {
				guard let s = self, s.alive() else { return }
				if s.pausedC2U {
					let old = s.currentBufferLimit
					s.adjustBufferLimit()
					if old != s.currentBufferLimit { s.log("Backpressure: \(old)->\(s.currentBufferLimit)B") }
				}
				s.backpressureTimer?.setEventHandler {}
				s.backpressureTimer?.cancel()
				s.backpressureTimer = nil
			}
		}
		backpressureTimer = t
		t.resume()
    }
	// === Flow primary detection & caps ===
	private enum FlowPrimary { case upstream, downstream, balanced }
	private var primaryRole: FlowPrimary = .balanced


	// 最近窗口（每 300ms 累計）的上下行量
	private var winUpBytes = 0
	private var winDownBytes = 0
	private var roleTimer: DispatchSourceTimer?
	private let ROLE_INTERVAL_MS = 300

	// 抖動抑制：連續 N 個窗口一致才切換
	private var pendingRole: FlowPrimary = .balanced
	private var pendingStreak = 0
	private let ROLE_HYSTERESIS = 2

	// “硬上限”鉤子（nil 表示不施加）
	private var downReadHardCap: Int? = nil           // 次要方向為下行時：4KB
	private var cuFlushBytesOverride: Int? = nil      // 次要方向為上行時：4KB
	private var cuFlushMsOverride: Int? = nil         // 次要方向為上行時：4ms

	private let LOCAL_BUFFER_HARD_CAP = 1_500_000 // ~1.5MB，可按需调

    
    // ==== 下行(d->c)背压/BBR ====
    private var pausedD2C = false
    private var downInflightBytes = 0        // 已送入 client、尚未完成的字节数
    private var downInflightCount = 0
    private var downMaxRead = 32 * 1024      // 动态读窗口
    private let DOWN_MIN_READ = 4 * 1024
    private let DOWN_MAX_READ = 32 * 1024

    // 下行最小版 BBR（以真正“交付给 client”的速率为准）
    private var d_bbrBw_bps: Double = 0
    private var d_bbrBwMax_bps: Double = 0
    private var d_bbrPrevDelivered = 0
    private var d_bbrSampleTs: DispatchTime = .now()
    private var d_bbrMinRtt_ms: Double = 50.0
    private var d_bbrMinRttStamp: DispatchTime = .now()
    private var downDeliveredBytes = 0

    // 预算夹紧/回退
    private let D_BBR_MIN_BUDGET_BYTES = 128 * 1024
    private let D_BBR_MAX_BUDGET_BYTES = 2 * 1024 * 1024
    private let D_BBR_FALLBACK_BUDGET_BYTES = 1 * 1024 * 1024


    private func adjustBufferLimit() {
        // ===== 上行 (client -> upstream) =====
        let oldUploadLimit = currentBufferLimit

        // 全局水位
        Self.globalLock.lock()
        let globalBytes = Self.globalBufferedBytes
        Self.globalLock.unlock()

        // 结合（上行）BBR 的动态 in-flight 预算，决定是否允许扩张
        let (B, C) = inflightBudget()
        let canGrowUpload = globalBytes <= Self.GLOBAL_BUFFER_BUDGET
                        && inflightBytes < (B * 90) / 100
                        && inflight.count < (C * 90) / 100

        if pausedC2U {
            if canGrowUpload {
                currentBufferLimit = min(currentBufferLimit * 2, maxBufferLimit)  // 温和扩张
            }
            // else: 不具备扩张条件，维持现状，避免峰值继续放大
        } else {
            currentBufferLimit = max(currentBufferLimit / 3, 4 * 1024)            // 快速收敛
        }

        if currentBufferLimit != oldUploadLimit {
            log("Adjust upload buffer: \(oldUploadLimit) -> \(currentBufferLimit)")
        }

        // ===== 下行 (downstream -> client) =====
        let targetDown = downInflightBudgetBytes()

        // 根据占用调整读取粒度（抑峰/提吞吐）
        let oldRead = downMaxRead

        // ★ 平滑起步：TTFB 后 300ms 内或交付 <128KB，不要节流，窗口拉满
        let smoothStart: Bool = {
            if let tfb = tFirstByte {
                let ageMs = diffMs(start: tfb, end: .now())
                return ageMs < 300 || downDeliveredBytes < 128 * 1024
            }
            return true // 还没拿到首字节，也认为在平滑期
        }()



        if smoothStart {
            if downMaxRead != DOWN_MAX_READ {
                downMaxRead = DOWN_MAX_READ
                vlog("smooth-start: force read=\(downMaxRead)")
            }
            if pausedD2C {
                pausedD2C = false
                vlog("smooth-start: force resume d->c")
                pumpDownstreamToClient()
            }
        } else {
            if pausedD2C || downInflightBytes >= (targetDown * 90) / 100 {
                downMaxRead = max(downMaxRead / 2, DOWN_MIN_READ)     // 压力大：减半
            } else if downInflightBytes <= (targetDown * 40) / 100 {
                downMaxRead = min(downMaxRead * 2, DOWN_MAX_READ)     // 压力小：翻倍
            }

            if downMaxRead != oldRead {
                log("Adjust down read: \(oldRead) -> \(downMaxRead)")
            }

			// 角色導致的硬上限（僅作天花板，BBR 在此之內動態）
			if let cap = self.downReadHardCap, downMaxRead > cap {
				let o = downMaxRead
				downMaxRead = cap
				log("down read cap: \(o) -> \(downMaxRead)")
			}

            // 命中/脱离目标触发暂停/恢复
            if downInflightBytes >= targetDown {
                if !pausedD2C {
                    pausedD2C = true
                    vlog("pause d->c: inflight=\(downInflightBytes) >= target=\(targetDown), read=\(downMaxRead)")
                }
            } else if pausedD2C, downInflightBytes <= (targetDown * 90) / 100 {
                pausedD2C = false
                vlog("resume d->c: inflight=\(downInflightBytes) <= 0.9*target, read=\(downMaxRead)")
                pumpDownstreamToClient()
            }
        }
    }


    // ====== Minimal BBR (upload) ======
    private enum BBRState { case startup, drain, probeBW }
    private var bbrState: BBRState = .startup

    // 上行带宽估计：bytesUp 增量 / 采样间隔
    private var bbrBwUp_bps: Double = 0          // 当前瞬时估计（bits/s）
    private var bbrBwMax_bps: Double = 0         // 窗口内最大值（近似 max filter）
    private var bbrPrevBytesUp: Int = 0
    private var bbrSampleTs: DispatchTime = .now()

    // RTT 估计：以 TTFB 为 minRTT 初值（ms）
    private var bbrMinRtt_ms: Double = 100.0
    private var bbrMinRttStamp: DispatchTime = .now()

    // pacing/cwnd gain（极简）
    private var bbrPacingGain: Double = 2.0
    private var bbrCwndGain: Double = 2.0
    private var bbrProbeCycle: [Double] = [1.25, 0.75, 1, 1, 1, 1, 1, 1]
    private var bbrProbeIndex: Int = 0

    // 采样定时器
    private var bbrTimer: DispatchSourceTimer?

    // 预算夹紧与回退
    private let BBR_MIN_BUDGET_BYTES: Int = 128 * 1024
    private let BBR_MAX_BUDGET_BYTES: Int = 2 * 1024 * 1024
    private let BBR_FALLBACK_BUDGET_BYTES: Int = 768 * 1024
    private let BBR_FALLBACK_COUNT: Int = 320

    // —— 发送在途(in-flight)预算，用于约束“直发小包”绕过 cuBuffer 的场景
    private var inflightBytes: Int = 0

    private let INFLIGHT_BYTES_BUDGET = 384 * 1024   // 512KB（可按需调大到 768KB/1MB）
    private let INFLIGHT_COUNT_BUDGET = 96          // 在途包数上限，双保险

    private var sendSeq: UInt64 = 0
    private var inflight = Set<UInt64>()
    
    // --- 上行(c->u)微批：64KB 或 5ms 触发 ---
    private var cuBuffer = Data()
    private var cuFlushTimer: DispatchSourceTimer?
    private let CU_FLUSH_BYTES = 32 * 1024
    private let CU_FLUSH_MS = 8
    // 当定时到点但缓冲小于该值时，允许再延一次以攒到更“胖”的报文
    private let CU_MIN_FLUSH_BYTES = 8 * 1024
    private let CU_EXTRA_MS = 10

    // 启动最小版 BBR 采样器（200ms）
     private func startBBRSampler() {
		// 保守停旧（兼容旧版本）
		bbrTimer?.setEventHandler {}
		bbrTimer?.cancel()
		bbrTimer = nil
		// 初始化一次状态
		bbrPrevBytesUp = bytesUp
		bbrSampleTs = .now()
		bbrState = .startup
		bbrPacingGain = 2.0
		bbrCwndGain = 2.0
		bbrProbeIndex = 0
		bbrBwMax_bps = 0
     }

     @inline(__always)
    private func downInflightBudgetBytes() -> Int {
        if d_bbrBwMax_bps <= 0 || d_bbrMinRtt_ms <= 0 {
            return D_BBR_FALLBACK_BUDGET_BYTES
        }
        let bdp = Int((d_bbrBwMax_bps / 8.0) * (d_bbrMinRtt_ms / 1000.0)) // bytes
        return min(max(bdp, D_BBR_MIN_BUDGET_BYTES), D_BBR_MAX_BUDGET_BYTES)
    }

    // —— 全局预算：限制所有桥接实例合计的缓冲上限（比如 8MB）
    
    fileprivate static var globalBufferedBytes: Int = 0
    fileprivate static let globalLock = NSLock()


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
    
    private let reqHost: String
    private let reqPort: Int
    
    private let resHost: String
    private let resPort: Int
    
    private let verbose: Bool
    
    // --- KPI & 半关闭守护 ---
    private var tStart: DispatchTime = .now()
    private var tReady: DispatchTime?
    private var tFirstByte: DispatchTime?
    private var tDownReady: DispatchTime?      // ← 新增：下行 ready 的时间戳




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

		// 添加紧急内存检查
        if cuBuffer.count + d.count > Self.ABSOLUTE_MAX_BUFFER {
            log("Emergency flush: buffer would exceed absolute max")
            flushCUBuffer()
        }
		// 使用 autoreleasepool 包装
        autoreleasepool {
            cuBuffer.append(d)
            addGlobalBytes(d.count)
            
            // 检查缓冲区限制
            if cuBuffer.count >= currentBufferLimit {
                pausedC2U = true
                flushCUBuffer()
                scheduleBackpressureTimer()
                scheduleMaybeResumeCheck()
                return
            }
            
            // 全局预算检查
            Self.globalLock.lock()
            let overBudget = Self.globalBufferedBytes > Self.GLOBAL_BUFFER_BUDGET
            Self.globalLock.unlock()
            
            if overBudget {
                pausedC2U = true
                flushCUBuffer()
                scheduleBackpressureTimer()
                scheduleMaybeResumeCheck()
            }
        }
    }

    // 新增：flush 后 2ms 再查一次 in-flight，用于打破“回压后僵住”
    private func scheduleMaybeResumeCheck() {
        // 先取消旧的
        resumeCheckTimer?.setEventHandler {}
        resumeCheckTimer?.cancel()
        resumeCheckTimer = nil

        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(2))
        t.setEventHandler { [weak self] in
			autoreleasepool {
				guard let s = self, s.alive() else { return }
				s.maybeResumeAfterInflightDrained()
				// 触发一次即释放
				s.resumeCheckTimer?.setEventHandler {}
				s.resumeCheckTimer?.cancel()
				s.resumeCheckTimer = nil
			}
        }
        resumeCheckTimer = t
        t.resume()
    }

    // —— Speedtest 上传微合并缓冲：4KB 或 4ms 触发，仅测速生效
    private var stBuffer = Data()
    private var stTimer: DispatchSourceTimer?
    private let ST_FLUSH_BYTES = 64 * 1024
    private let ST_FLUSH_MS = 1
    
    @inline(__always)
    private func scheduleSTFlush() {
        
        // 先停止旧定时器
		safeStopTimer(&stTimer)
		
		let t = DispatchSource.makeTimerSource(queue: queue)
		t.schedule(deadline: .now() + .milliseconds(ST_FLUSH_MS))
		t.setEventHandler { [weak self] in  // 使用 weak self
			autoreleasepool {
				self?.flushSTBuffer()
			}
		}
		stTimer = t
		t.resume()
    }

    @inline(__always)
    private func flushSTBuffer() {
        guard !stBuffer.isEmpty, !closed else { return }
        let payload = stBuffer
        stBuffer.removeAll(keepingCapacity: false)
        subGlobalBytes(payload.count)

        let (B, _) = self.inflightBudget()
        if self.inflightBytes >= (B * 90) / 100 {
            // 1) 关闭旧定时器

			safeStopTimer(&stTimer)

           

            // 2) 1ms 后再试一次：优先继续“再合包”一轮（flushSTBuffer），
            //    若 stBuffer 尚未积累，则直接发送 payload
            let t = DispatchSource.makeTimerSource(queue: queue)
            t.schedule(deadline: .now() + .milliseconds(1))
            t.setEventHandler { [weak self] in
				autoreleasepool {
					guard let s = self, s.alive() else { return }
					if !s.stBuffer.isEmpty {
						s.flushSTBuffer()
					} else {
						s.sendToUpstream(payload, remark: "c->u(st)")
					}
				}
            }
            stTimer = t       // 关键：持有引用，避免定时器被释放
            t.resume()
        } else {
            sendToUpstream(payload, remark: "c->u(st)")
        }
        if pausedC2U { scheduleMaybeResumeCheck() }
    }

    private func setupMemoryMonitoring() {
		// 每5秒检查一次内存
		queue.asyncAfter(deadline: .now() + 5.0) { [weak self] in
			guard let self = self, self.alive() else { return }
			self.checkAndHandleMemoryPressure()
			self.setupMemoryMonitoring() // 递归调用
		}
	}
    
    
    private let onClosed: ((UInt64) -> Void)?
    
    fileprivate let queue: DispatchQueue
    private var upstream: NWConnection?
    private var downstream: NWConnection?
    private var closed = false
    
    // —— 生存门闸：所有回调入口先判存活，避免已取消后仍访问资源
    private let stateLock = NSLock()
    @inline(__always) fileprivate func alive() -> Bool {
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
        reqHost: String,
        reqPort: Int,
        resHost: String,
        resPort: Int,
        verbose: Bool = false,
        connectInfo: String? = nil,
        onClosed: ((UInt64) -> Void)? = nil
    ) {
        self.id = id
        self.client = client
        
        self.reqHost = reqHost
        self.reqPort = reqPort
        
        self.resHost = resHost
        self.resPort = resPort
        
        self.verbose = verbose
        self.onClosed = onClosed
        self.connectInfo = connectInfo
        self.queue = DispatchQueue(label: "LayerMinusBridge.\(id)", qos: .userInitiated)
        self.queue.setSpecific(key: Self.qKey, value: 1)
        // 简单的生命周期日志（仅 DEBUG）
        #if DEBUG
        	NSLog("🟢 CREATED LayerMinusBridge #\(id) for reqHost \(reqHost):\(reqPort) resHost \(resHost):\(resPort) \(infoTag())")
        #endif
        setupMemoryMonitoring()

    }
    
    deinit {
        let lifetime = diffMs(start: tStart, end: .now())
		if lifetime < MIN_LIFETIME_MS {
			log("⚠️ WARNING: Bridge #\(id) destroyed too quickly: \(lifetime)ms")
		}
			
			// 先标记为已关闭，防止任何新操作
			stateLock.lock()
			let wasAlreadyClosed = closed
			closed = true
			stateLock.unlock()
			
			if !wasAlreadyClosed {
				log("⚠️ WARNING: LayerMinusBridge #\(id) destroyed without proper closing!")
				cleanupAllResources()
				
			}

		log("🔴 DESTROYED LayerMinusBridge #\(id)")
		
    }
    
    #if DEBUG
        @inline(__always)
        public func log(_ msg: String) {
            NSLog("[LayerMinusBridge \(id), \(infoTag())] %@", msg)
        }
    #else
        @inline(__always)
        private func log(_ msg: @autoclosure () -> String) { }
    #endif

	// MARK: - Interactive traffic detection (轻量启发式)
	private var lastInteractiveMark: TimeInterval = 0
	private let interactiveStickMs: Int = 1500   // 1.5s 粘性直送，避免抖动

	@inline(__always)
	private func isInteractiveTraffic(_ data: Data) -> Bool {
		// 1) 端口启发：SSH/Telnet/RDP/VNC 常见端口
		if reqPort == 22   || reqPort == 23   || reqPort == 3389 ||
		reqPort == 5900 || reqPort == 5901 || reqPort == 5902 {
			lastInteractiveMark = CFAbsoluteTimeGetCurrent()
			return true
		}
		// 2) 内容启发：极轻前缀匹配（不做重解析）
		if data.count >= 4 {
			// SSH banner: "SSH-"
			if data.starts(with: [0x53, 0x53, 0x48, 0x2D]) { // "SSH-"
				lastInteractiveMark = CFAbsoluteTimeGetCurrent()
				return true
			}
			// Telnet IAC: 0xFF 开头
			if data[0] == 0xFF {
				lastInteractiveMark = CFAbsoluteTimeGetCurrent()
				return true
			}
			// RDP(TPKT) 常见前导: 0x03 0x00 ...
			if data[0] == 0x03 && data[1] == 0x00 {
				lastInteractiveMark = CFAbsoluteTimeGetCurrent()
				return true
			}
		}
		// 3) 粘性窗口：最近 1.5s 判过交互 → 继续直送
		let now = CFAbsoluteTimeGetCurrent()
		if (now - lastInteractiveMark) * 1000.0 < Double(interactiveStickMs) {
			return true
		}
		return false
	}
    
    private func cleanupAllResources() {
        // 立即断开所有handler
		upstream?.stateUpdateHandler = nil
		downstream?.stateUpdateHandler = nil
		
		// 停止所有定时器
		stopAllTimersImmediate()
		
		// 清理缓冲区
		if !cuBuffer.isEmpty {
			subGlobalBytes(cuBuffer.count)
			cuBuffer = Data()
		}
		if !stBuffer.isEmpty {
			subGlobalBytes(stBuffer.count)
			stBuffer = Data()
		}
		
		// 从协调器移除
		BridgeCoordinator.shared.remove(self)
		
		// 取消连接
		upstream?.cancel()
		downstream?.cancel()
		client.cancel()
		
		// 清空引用
		upstream = nil
		downstream = nil
    }

	private let MIN_LIFETIME_MS: Double = 100  // 至少存活100ms

	private func stopAllTimersImmediate() {
        // 辅助函数来停止单个定时器
		func stopTimer(_ timer: inout DispatchSourceTimer?) {
			if let t = timer {
				t.setEventHandler {}
				t.cancel()
				timer = nil
			}
		}
		
		// 停止所有定时器
		stopTimer(&firstByteWatchdog)
		stopTimer(&drainTimer)
		stopTimer(&uploadStuckTimer)
		stopTimer(&cuFlushTimer)
		stopTimer(&stTimer)
		stopTimer(&resumeCheckTimer)
		stopTimer(&backpressureTimer)
		stopTimer(&bbrTimer)
		stopTimer(&roleTimer)
		
		#if DEBUG
		stopTimer(&memSummaryTimer)
		#endif
    }
    
    // --- 追加KPI ---
    private var tHandoff: DispatchTime?
    private var tFirstSend: DispatchTime?
    
    private func infoTag() -> String {
        guard let s = connectInfo, !s.isEmpty else { return "" }
        return " [\(s)]"
    }
    
    public func start(withFirstBody firstBody: Data) {
		BridgeCoordinator.shared.add(self)
        BridgeCoordinator.shared.ensure200ms()
        #if DEBUG
        BridgeCoordinator.shared.ensure5sMemSummary()
        #endif

		// 保持自身引用一段时间，防止立即销毁
		let selfRef = self
        
        // 防止过早释放
		Self.bridgesLock.lock()
		Self.activeBridges[self.id] = self
		Self.bridgesLock.unlock()
        
        // 100ms 后释放临时引用
        queue.asyncAfter(deadline: .now() + .milliseconds(100)) { [weak self, id = self.id] in
            Self.bridgesLock.lock()
            Self.activeBridges.removeValue(forKey: id)
            Self.bridgesLock.unlock()
        }

        queue.async { [weak self] in
            guard let self = self else { return }
            guard self.alive() else { return }
            
            // KPI: 记录会话起点，用于计算 hsRTT / TTFB / 总时长
            self.tStart = .now()
            self.startBBRSampler()
            self.log("✅ start -> \(self.reqHost):\(self.reqPort) <-- \(self.resHost):\(self.resPort), firstBody bytes=\(firstBody.count)")

            // KPI: handoff -> start（应用层排队/解析耗时）
            if let th = self.tHandoff {
                let ms = Double(self.tStart.uptimeNanoseconds &- th.uptimeNanoseconds) / 1e6
                self.log(String(format: "KPI handoff_to_start_ms=%.1f", ms))
            }
            
            
        	self.log("firstBody bytes=\(firstBody.count)")

			// 修复：使用弱引用包装器
            let domain = self.etld1(of: self.resHost)
            let weakSelf = Weak(self)
            
            DomainGate.shared.acquire(domain: domain) {
                guard let strongSelf = weakSelf.value, strongSelf.alive() else { return }
				strongSelf.connectUpstreamAndRun(reqFirstBody: firstBody, resFirstBody: nil)
            }
            
            
        }
    }
    
    private final class Weak<T: AnyObject> {
		weak var value: T?
		init(_ value: T) { self.value = value }
	}


    @inline(__always)
    private func gracefulCloseConnection(_ c: inout NWConnection?, label: String) {
        guard let conn = c else { return }
		c = nil  // 立即置空
		
		// 标记操作开始
		pendingOperations.incrementAndGet()
		
		// 清除handler防止回调
		conn.stateUpdateHandler = nil
		
		conn.send(content: nil, completion: .contentProcessed { [weak self] _ in
			autoreleasepool {
				self?.pendingOperations.decrementAndGet()
				// 延迟取消给内核时间
				self?.queue.asyncAfter(deadline: .now() + .milliseconds(10)) {
					conn.cancel()
				}
			}
		})
    }
    
    private var isCancelling = false
    public func cancel(reason: String) {

		log("CANCEL trigger id=\(id) reason=\(reason) ...")

		if reason.contains("LayerMinus cutover") {
			// 热切换：标为“交接收尾”，避免误判为 KILL
			log("🔴CLOSE_CLASS=HANDOFF note=cutover handoff🔴")
			// 直接进入 drain 路径：给对向 25s（你已设置 drainGrace=25s）
			// 如果你已有任一侧 EOF，可改短到 3~5s
			scheduleDrainCancel(hint: "handoff")
			return
		}

        // —— 保留并提前打印原有诊断日志（不动）
            log("CANCEL trigger id=\(id) reason=\(reason) inflightBytes=\(inflightBytes) inflightCount=\(inflight.count) cu=\(cuBuffer.count)B st=\(stBuffer.count)B global=\(Self.globalBufferedBytes)B pausedC2U=\(pausedC2U)")
            switch reason {
            case let s where s.contains("first_byte_timeout"):
                log("KILL_CLASS=TTFB_TIMEOUT note=watchdog fired after firstBody")
            case let s where s.contains("drain timeout"):
                log("KILL_CLASS=DRAIN_IDLE note=no opposite activity within \(Int(drainGrace))s")
            case let s where s.contains("recv err") || s.contains("failed"):
                log("KILL_CLASS=NETWORK_ERR note=peer/network failure")
            case let s where s.contains("downstream recv err") && Self.markAndBurstTunnelDown():
                log("KILL_CLASS=TUNNEL_DOWN note=burst downstream errs in window")
            default:
                log("KILL_CLASS=MISC note=\(reason)")
            }
    
        // 使用更强的同步机制
        var shouldProceed = false
        stateLock.lock()
        if !closed && !isCancelling {
            closed = true
            isCancelling = true
            shouldProceed = true
        }
        stateLock.unlock()
        
        guard shouldProceed else { return }
        
        log("CANCEL: \(reason)")
        
        // 立即停止所有活动
        pausedC2U = true
        pausedD2C = true
        
        
        // 在当前队列执行清理，确保同步
        queue.async { [weak self] in
            guard let self = self else { return }
            
            autoreleasepool {
                // 停止所有定时器
                self.stopAllTimersImmediate()
                
                // 清理缓冲区
                self.cleanupBuffers()
                
                // 断开连接
                self.closeConnectionsImmediate()
                
                // 最后的回调
                if let cb = self.onClosed {
                    cb(self.id)
                }

				// 从协调器移除
    			BridgeCoordinator.shared.remove(self)
                
                // 释放域名锁
                DomainGate.shared.release(domain: self.etld1(of: self.resHost))
                
                self.stateLock.lock()
                self.isCancelling = false
                self.stateLock.unlock()
            }
        }
		
    }
    
    private func closeConnectionsImmediate() {
        // 立即清除所有 handler 并断开连接
        if let up = upstream {
            up.stateUpdateHandler = nil
            up.cancel()
            upstream = nil
        }
        
        if let down = downstream {
            down.stateUpdateHandler = nil
            down.cancel()
            downstream = nil
        }
        
        client.cancel()
    }
    
    private func cleanupBuffers() {
        if !cuBuffer.isEmpty {
            subGlobalBytes(cuBuffer.count)
            cuBuffer = Data()
        }
        if !stBuffer.isEmpty {
            subGlobalBytes(stBuffer.count)
            stBuffer = Data()
        }
        inflightBytes = 0
        inflight.removeAll()
    }

	// 每 200ms 被 Coordinator 派发一次，跑在“本连接的 queue”上
	func onTick200() {
		guard alive() else { return }

		// (a) 原 startBBRSampler() 里的采样/状态机逻辑，整体搬到这里
		bbrOnTick()

		// (b) 原 roleTimer 每 300ms 的角色判定/护栏（你已有 recomputePrimaryRole）
		// 以 200ms tick 代替 300ms，效果更实时
		recomputePrimaryRole()
		applyPressureCapsIfNeeded()
		adaptiveBufferTuning()
		// (c) 替代 scheduleBackpressureTimer() 的循环：仅在 pausedC2U 时调整
		if pausedC2U {
			let old = currentBufferLimit
			adjustBufferLimit()
			if old != currentBufferLimit { log("Backpressure: \(old)->\(currentBufferLimit)B") }
		}
	}

	// MARK: - 根据 RTT 动态调整缓冲参数（新增）
	/// 使用 BBR/路径测得的最小 RTT（毫秒）来做轻量自适应：
	/// - 低 RTT（<20ms）：更短刷新、更小批量（降低交互延迟）
	/// - 高 RTT（>100ms）：更长刷新、更大批量（提高吞吐/包效率）
	/// 中间区间恢复默认（取消 override）
	private func adaptiveBufferTuning() {
		// 使用 Double 进行统一比较
		let rtt: Double = self.bbrMinRtt_ms

		// 若没有可用 RTT，或仍处于“竞速未定/测速上传模式”，则不调整
		if rtt <= 0.0 || (!self.roleFinalized && !self.isSpeedtestUploadMode) {
			return
		}

		// 冷却：避免 200ms tick 频繁改动
		let now = CFAbsoluteTimeGetCurrent()
		if (now - lastTuneAt) * 1000.0 < Double(tuneCooldownMs) {
			return
		}

		var newBand: RttBand = rttBand
		switch rttBand {
		case .mid:
			if rtt < Double(lowEnterMs) { newBand = .low }
			else if rtt > Double(highEnterMs) { newBand = .high }
		case .low:
			if rtt > Double(lowExitMs) { newBand = .mid }
		case .high:
			if rtt < Double(highExitMs) { newBand = .mid }
		}

		guard newBand != rttBand else { return }
		rttBand = newBand
		lastTuneAt = now

		// 应用覆盖值（不直接改默认常量）
		switch newBand {
		case .low:
			// 低延迟网络：更短刷新、更小批量（极致跟手）
			self.cuFlushMsOverride = 2
			self.cuFlushBytesOverride = 16 * 1024
			log("RTT tune => LOW (\(rtt)ms) flush=\(self.cuFlushMsOverride!)ms/\(self.cuFlushBytesOverride!)B")
		case .high:
			// 高延迟网络：更长刷新、更大批量（更稳吞吐）
			self.cuFlushMsOverride = 10
			self.cuFlushBytesOverride = 64 * 1024
			log("RTT tune => HIGH (\(rtt)ms) flush=\(self.cuFlushMsOverride!)ms/\(self.cuFlushBytesOverride!)B")
		case .mid:
			// 恢复默认：取消 override，让原有逻辑接管
			self.cuFlushMsOverride = nil
			self.cuFlushBytesOverride = nil
			log("RTT tune => MID (\(rtt)ms) flush=default")
		}
	}


	private var pendingOperations = AtomicInteger()

	// 添加 AtomicInteger 实现
	private class AtomicInteger {
		private var value: Int = 0
		private let lock = NSLock()
		
		init() {}
		
		func get() -> Int {
			lock.lock()
			defer { lock.unlock() }
			return value
		}
		
		func incrementAndGet() -> Int {
			lock.lock()
			defer { lock.unlock() }
			value += 1
			return value
		}
		
		func decrementAndGet() -> Int {
			lock.lock()
			defer { lock.unlock() }
			value -= 1
			return value
		}
	}

    
    private var UUID: String?
    
    @inline(__always)
    private func gracefulCloseImmutableConnection(_ conn: NWConnection, label: String) {
		pendingOperations.incrementAndGet()

		conn.stateUpdateHandler = nil
		conn.send(content: nil, completion: .contentProcessed { [weak self] _ in
			autoreleasepool {
				self?.pendingOperations.decrementAndGet()
				self?.queue.asyncAfter(deadline: .now() + .milliseconds(10)) {
					conn.cancel()
				}
			}
		})
    }
    
    public func start(
        reqFirstBody: String,
        resFirstBody: String,
        UUID: String
    ) {

		BridgeCoordinator.shared.add(self)
		BridgeCoordinator.shared.ensure200ms()
		#if DEBUG
			BridgeCoordinator.shared.ensure5sMemSummary()
		#endif

		// 保持引用
		let selfRef = self

        
		// 防止过早释放
			Self.bridgesLock.lock()
			Self.activeBridges[self.id] = self
			Self.bridgesLock.unlock()
        
        // 100ms 后释放临时引用
        queue.asyncAfter(deadline: .now() + .milliseconds(100)) { [weak self, id = self.id] in
            Self.bridgesLock.lock()
            Self.activeBridges.removeValue(forKey: id)
            Self.bridgesLock.unlock()
        }

        self.UUID = UUID
        queue.async { [weak self] in
            guard let self = self, self.alive() else { return }

            self.tStart = .now()

            // 启动 BBR 采样器
            self.startBBRSampler()

            self.log("start (dual-first-body) -> req=\(self.reqHost):\(self.reqPort)  res=\(self.resHost):\(self.resPort)  reqLen=\(reqFirstBody.count)  resLen=\(resFirstBody.count)")

            let reqFirst = Data(reqFirstBody.utf8)
        	let resFirst = Data(resFirstBody.utf8)
            
            self.log("start -> req.bytes=\(reqFirst.count) res.bytes=\(resFirst.count) req=\(self.reqHost):\(self.reqPort) res=\(self.resHost):\(self.resPort)")



            // 開始連線上游並轉發資料（以 eTLD+1 做併發閘門）
            let domain = etld1(of: self.resHost)
            DomainGate.shared.acquire(domain: domain) { [weak self] in
				guard let self = self, self.alive() else { return }
				self.connectUpstreamAndRun(reqFirstBody: reqFirst, resFirstBody: resFirst)
			}

            
        }
    }
    
    @inline(__always)
    private func sendFirstBodyToUpstream(_ data: Data) {
        guard let up = upstream, alive() else {
            log("REQ-FIRST send aborted: upstream=\(upstream != nil), closed=\(closed)")
            return
        }
        up.send(content: data, completion: .contentProcessed({ [weak self] err in
			autoreleasepool {
				guard let self = self, self.alive() else { return }
				if let err = err {
					self.log("REQ-FIRST upstream send err: \(err)")
					self.queue.async { self.cancel(reason: "upstream send err (firstBody)") }
					return
				}
				if self.tFirstSend == nil {    // 仅首包标记
					self.tFirstSend = .now()
					self.log("REQ-FIRST sent successfully (mark tFirstSend)")

					// 首頁不要關 watchdog；只有上傳測速才關
					if !self.isSpeedtestUploadMode {
						let delay: TimeInterval = (self.reqPort == 443) ? 15.0 : 8.0
						let wd = DispatchSource.makeTimerSource(queue: self.queue)
						wd.schedule(deadline: .now() + delay)
						wd.setEventHandler { [weak self] in
							guard let s = self, s.alive() else { return }
							s.log("KPI watchdog fired: no first byte within \(Int(delay*1000))ms after REQ-FIRST")
							s.cancel(reason: "first_byte_timeout")
						}
						self.firstByteWatchdog = wd
						wd.resume()
					} else {
						self.vlog("speedtest upload-mode: watchdog disabled")
					}
				}
			}
        }))
    }

	// [DYNAMIC-ROLE] 两条连接各收一次，谁先到就定为下行；另一条为上行
	private func beginDynamicRoleElectionIfNeeded() {
		guard !electionStarted, !roleFinalized, let a = connReq, let b = connRes else { return }
		// DIRECT 由 maybeKickPumps 已处理
		if a === b { return }

		electionStarted = true
		log("dynamic-role: begin race listening on both connections")

		// 封装一次性监听
		func arm(_ conn: NWConnection, tag: String) {
			conn.receive(minimumIncompleteLength: 1, maximumLength: raceMaxRead) { [weak self] (data, _, isComplete, err) in
				autoreleasepool {
					guard let s = self, s.alive() else { return }

					// 如果这时角色已确定：上行侧直接丢弃，不重臂 => 等价于释放监听
					if s.roleFinalized {
						if conn === s.upstream {
							s.vlog("dynamic-role: upstream recv after finalize -> drop, no re-arm")
							return // 不再 re-arm
						}
						// 少见并发情形：下行侧又来了字节，直接转给客户端
						if let d = data, !d.isEmpty {
							s.sendToClient(d, remark: "down->client(race-late)")
						}
						return
					}

					if let err = err {
						s.log("dynamic-role: \(tag) race recv err: \(err) -> recheck in 50ms")
						// 二次确认：给另一条连接 50ms 的机会赢下竞速，避免偶发抖动误杀
						if !s.roleFinalized {
							s.queue.asyncAfter(deadline: .now() + .milliseconds(50)) {
								if !s.roleFinalized { s.cancel(reason: "\(tag) race recv err (rechecked)") }
							}
						}
						return
					}

					if let d = data, !d.isEmpty {
						if !s.roleFinalized {
							s.roleFinalized = true
							// 当前 down/up 预设：downstream == connRes
							let winnerIsUp = (conn === s.upstream)
							if winnerIsUp {
								s.log("dynamic-role: winner=\(tag) (pre-set upstream) -> swap roles")
								s.swapUpDown()
							} else {
								s.log("dynamic-role: winner=\(tag) (pre-set downstream)")
							}
							s.handleFirstDownstreamBytes(d, source: "race:\(tag)")
							s.pumpDownstreamToClient()
						} else {
							// 已定角色，若是上行侧收到了数据，直接忽略（按设计它不应有下行数据）
							if conn === s.upstream {
								s.vlog("dynamic-role: ignore late bytes on upstream after finalize")
							} else {
								// 极少数同时到达的情况，再次喂给 client
								s.sendToClient(d, remark: "down->client(race-late)")
							}
						}
						return
					}

					if isComplete {
						// 竞速中的 EOF：若另一侧稍后有数据，会被选定；否则走空闲收尾
						s.vlog("dynamic-role: \(tag) race EOF")
						if !s.roleFinalized {
							s.scheduleDrainCancel(hint: "race EOF on \(tag)")
						}
					}
				}
			}
		}

		arm(a, tag: "UP")
		arm(b, tag: "DOWN")
	}

	// 竞速读取尽量少：避免在失败者上吃多余数据
	private let raceMaxRead = 32  // 或 16/32


    @inline(__always)
    private func sendFirstBodyToDownstream(_ data: Data, onOK: (() -> Void)? = nil) {
        guard let dn = downstream, alive() else {
            log("RES-FIRST send aborted: downstream=\(downstream != nil), closed=\(closed)")
            return
        }
        // 这些字节若被对端回流，不应触发 TTFB
        downIgnoreBytes &+= data.count
        dn.send(content: data, completion: .contentProcessed({ [weak self] err in
			autoreleasepool {
				guard let self = self, self.alive() else { return }
				if let err = err {
					self.log("RES-FIRST downstream send err: \(err)")
					self.queue.async { self.cancel(reason: "downstream send err (firstBody)") }
				} else {
					self.log("RES-FIRST sent successfully")
					onOK?()  // ★ 真正发出去后再开始“下行监听”
				}
			}
        }))
    }
    
    private var downIgnoreBytes: Int = 0
    // 在 connectUpstreamAndRun(...) 开头保存 res 首包
    
    // 类属性（私有）
    private var pendingResFirstBody: Data?

	// MARK: - C2U 直送总开关 & 背压护栏
	private let immediateUpload = true                // 默认开启“零阈值直送”
	private let maxInFlightC2U = 1 << 20              // 1MB 在途上限（可按机型微调）

	// 运行时状态
	private var inFlightC2U: Int = 0                  // 当前在途上行字节
	private var c2uQueue: [Data] = []                 // 背压/竞速排队数据（保持顺序）
	private var upstreamPausedByBackpressure = false  // 已暂停从 APP 读取

    
    private func connectUpstreamAndRun(reqFirstBody: Data, resFirstBody: Data?) {
        // 端口合法性
        guard let reqNWPort = NWEndpoint.Port(rawValue: UInt16(self.reqPort)) else {
            log("invalid reqPort \(self.reqPort)"); cancel(reason: "invalid reqPort"); return
        }
        guard let resNWPort = NWEndpoint.Port(rawValue: UInt16(self.resPort)) else {
            log("invalid resPort \(self.resPort)"); cancel(reason: "invalid resPort"); return
        }

        // TCP 参数
        let params = NWParameters.tcp
        if let tcp = params.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
            tcp.noDelay = true
            tcp.enableKeepalive = true
            tcp.keepaliveIdle = 30
        }
        var upReady = false
        var downReady = false
        var reqFirstSent = false
        var resFirstSent = false
        var pumpsStarted = false   // ← 新增：防止双泵重复启动

        // 在最前面保存 res 首包，再判定 DIRECT
        self.pendingResFirstBody = resFirstBody
        let isDIRECT = (resFirstBody == nil)

		// 双首包模式（非 DIRECT）要求两边都有实际首包，否则节点等不到
		if !isDIRECT, let rb = resFirstBody, rb.isEmpty {
			log("FATAL: dual-conn requires non-empty resFirstBody, but got EMPTY")
			cancel(reason: "empty resFirstBody in dual-conn workflow")
			return
		}

        // 初始化连接：DIRECT 只建一条并复用；非 DIRECT 建两条
        let up = NWConnection(host: NWEndpoint.Host(self.reqHost), port: reqNWPort, using: params)
		self.upstream = up
		self.connReq = up

		if isDIRECT {
			self.downstream = up
			self.connRes = up
		} else {
			let dn = NWConnection(host: NWEndpoint.Host(self.resHost), port: resNWPort, using: params)
			self.downstream = dn
			self.connRes = dn
		}

        func maybeKickPumps() {
			guard alive() else { return }
				vlog("maybeKickPumps check: upReady=\(upReady) downReady=\(downReady) reqFirstSent=\(reqFirstSent) resFirstSent=\(resFirstSent) isDIRECT=\(isDIRECT)")

				// 上行（client->node）可先启动，后续如需会因角色翻转自动生效
				if upReady, !reqFirstSent {
					reqFirstSent = true
					if !reqFirstBody.isEmpty {
						log("REQ-FIRST ready; sending \(reqFirstBody.count)B to \(reqHost):\(reqPort)")
						sendFirstBodyToUpstream(reqFirstBody)
					} else {
						vlog("REQ-FIRST is empty; skip")
					}
					pumpClientToUpstream()
				}

				// 下行首包（若有）只需在下行连接 ready 后发，DIRECT 没有 resFirst
				if downReady, !resFirstSent {
					if !isDIRECT, let rb = pendingResFirstBody, !rb.isEmpty {
						log("RES-FIRST ready; sending \(rb.count)B to \(resHost):\(resPort)")
						sendFirstBodyToDownstream(rb) { [weak self] in
							guard let s = self, s.alive() else { return }
							resFirstSent = true
							// 不立刻 pumpDownstreamToClient —— 交给竞速决定
							s.beginDynamicRoleElectionIfNeeded()
						}
						return
					} else {
						resFirstSent = true
						// 直接进入竞速（DIRECT / 无 res-first）
						beginDynamicRoleElectionIfNeeded()
					}
				}

				// 如果 DIRECT：两端等同，只要 up ready 就可以认为 role 已定
				if isDIRECT, upReady, !electionStarted {
					roleFinalized = true
					electionStarted = true
					vlog("DIRECT mode: role finalized implicitly (single conn)")
					pumpDownstreamToClient()
				}
			}
        // 监听状态变化

        up.stateUpdateHandler = { [weak self] st in
			autoreleasepool {
				guard let s = self, s.alive() else { return }
				switch st {
				case .ready:
					s.log("UP ready UUID:\(s.UUID ?? "") \(s.reqHost):\(s.reqPort)")
					s.tReady = .now()
					upReady = true
					if isDIRECT {
					// 先就緒再觸發 maybeKickPumps；並補上 tDownReady 供 KPI 使用
						s.tDownReady = .now()
						downReady = true
					}
					maybeKickPumps()
					// ★ 新增：就绪后尝试排空直送队列（弱网/短暂阻塞后的恢复）
					s.drainC2UQueueIfPossible()
				case .waiting(let e):
					s.log("UP waiting: UUID:\(s.UUID ?? "") \(e)")
				case .failed(let e):
					s.log("UP failed: UUID:\(s.UUID ?? "") \(e)"); s.queue.async { s.cancel(reason: "upstream failed") }
				case .cancelled:
					s.log("UP cancelled UUID:\(s.UUID ?? "") "); s.queue.async { s.cancel(reason: "upstream cancelled") }
				default:
					s.log("UP UUID:\(s.UUID ?? "") state: \(st)")
				}
			}
        }

        // 非 DIRECT 才单独监听 downstream 的状态；DIRECT 避免对同一连接重复设置 handler
        if !isDIRECT, let down = self.downstream {
            down.stateUpdateHandler = { [weak self] st in
				autoreleasepool {
					guard let s = self, s.alive() else { return }
					switch st {
					case .ready:
						s.log("DOWN ready UUID:\(s.UUID ?? "") \(s.resHost):\(s.resPort)")
						s.tDownReady = .now()
						downReady = true
						maybeKickPumps()
					case .waiting(let e):
						s.log("DOWN waiting: UUID:\(s.UUID ?? "") \(e)")
					case .failed(let e):
						s.log("DOWN failed: UUID:\(s.UUID ?? "")  \(e)"); s.queue.async { s.cancel(reason: "downstream failed") }
					case .cancelled:
						s.log("DOWN cancelled UUID:\(s.UUID ?? "")"); s.queue.async { s.cancel(reason: "downstream cancelled") }
					default:
						s.log("DOWN state:  UUID:\(s.UUID ?? "") \(st)")
					}
				}
            }
        }

        log("Connecting UUID:\(self.UUID ?? ""): UP \(reqHost):\(reqPort)\(isDIRECT ? "  |  DOWN (DIRECT=UP)" : "  |  DOWN \(resHost):\(resPort)")")
        up.start(queue: queue)
        if !isDIRECT, let down = self.downstream, down !== up {
            down.start(queue: queue)
        }
    }

	private func recomputePrimaryRole() {
		let up = self.winUpBytes
		let down = self.winDownBytes
		self.winUpBytes = 0
		self.winDownBytes = 0

		// 阈值：至少 16KB，且一方是另一方的 3 倍
		let MIN_BYTES = 16 * 1024
		let ratio: Double = (down == 0) ? (up > 0 ? .infinity : 1) : Double(up) / Double(down)

		let newRole: FlowPrimary
		if up >= MIN_BYTES && ratio >= 3.0 {
			newRole = .upstream
		} else if down >= MIN_BYTES && (1.0/ratio) >= 3.0 {
			newRole = .downstream
		} else {
			newRole = .balanced
		}

		if newRole == pendingRole {
			pendingStreak += 1
		} else {
			pendingRole = newRole
			pendingStreak = 1
		}
		if pendingStreak >= ROLE_HYSTERESIS && newRole != primaryRole {
			primaryRole = newRole
			applyPrimaryCaps()
		}
	}

	private func applyPrimaryCaps() {
		switch primaryRole {
		case .upstream:
		// 上行為主：下行讀窗硬上限 4KB
		downReadHardCap = 4 * 1024
		cuFlushBytesOverride = nil
		cuFlushMsOverride = nil
		log("ROLE -> upstream-primary; cap downRead to 4KB")
		case .downstream:
		// 下行為主：上行微批硬上限 4KB/4ms
			downReadHardCap = nil
			cuFlushBytesOverride = 4 * 1024
			cuFlushMsOverride = 4
		log("ROLE -> downstream-primary; cap up flush to 4KB/4ms")
		case .balanced:
			// 解除硬上限
			downReadHardCap = nil
			cuFlushBytesOverride = nil
			cuFlushMsOverride = nil
			log("ROLE -> balanced; remove hard caps")
		}
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
           self.inflightBytes <= (B * 95) / 100 &&      // 原来 75%
           self.inflight.count <= (C * 95) / 100 {
            self.pausedC2U = false
            self.vlog("resume c->u after inflight drained: bytes=\(self.inflightBytes) count=\(self.inflight.count)")
            self.pumpClientToUpstream()
        }
    }

	@inline(__always)
	private func handleAppToUpstream(_ data: Data) {
		guard !data.isEmpty, alive() else { return }
		if immediateUpload {
			writeUpstreamImmediately(data)
		} else {
			appendToCUBuffer(data) // 兼容旧路径
		}
	}

	// MARK: - 直送核心（含竞速保护）
	private func writeUpstreamImmediately(_ data: Data) {
		guard alive() else { return }

		// —— 连接初期“角色未定”保护：避免把可能成为下行的线路被大流量占满
		if !self.roleFinalized && !self.isSpeedtestUploadMode {
			let head = min(data.count, 4 * 1024) // 直送小头 4KB，降低交互时延
			if head > 0 {
				let first = data.prefix(head)
				self._writeUpstreamSingleChunk(first, remark: "c->u(pre-race small)")
			}
			if data.count > head {
				// 余量入队，等角色确定或上游可写后顺序发送（由 _writeUpstreamSingleChunk/队列负责）
				self.c2uQueue.append(data.suffix(from: head))
				self._maybePauseReadFromApp() // 你已有或占位的方法，避免内存膨胀
			}
			return
		}

		// —— 1) 小包（<1KB）直接发送，跳过缓冲
		if data.count < 1024 {
			self._writeUpstreamSingleChunk(data, remark: "c->u(small-direct)")
			return
		}

		// —— 2) 交互式流量（SSH/Telnet/RDP/VNC 等）直接发送
		if isInteractiveTraffic(data) {
			self._writeUpstreamSingleChunk(data, remark: "c->u(interactive)")
			return
		}

		// —— 3) 其他走原有逻辑（含微批/BBR），更利于大流稳定性
		appendToCUBuffer(data)
	}

	@inline(__always)
	private func _maybePauseReadFromApp() {
		guard !upstreamPausedByBackpressure else { return }
		upstreamPausedByBackpressure = true
		// TODO: 若你已有暂停读取的 API，请替换这里（例如在 receive 回调用标志直接 return）
	}

	@inline(__always)
	private func _resumeReadFromAppIfPaused() {
		guard upstreamPausedByBackpressure else { return }
		upstreamPausedByBackpressure = false
		// TODO: 若你已有恢复读取的 API，请替换这里（例如重新调用 pumpClientToUpstream()）
	}

	@inline(__always)
	private func drainC2UQueueIfPossible() {
		guard alive(), !upstreamPausedByBackpressure, !c2uQueue.isEmpty else { return }
		let next = c2uQueue.removeFirst()
		_writeUpstreamSingleChunk(next, remark: "c->u(drain)")
	}


	@inline(__always)
	private func _writeUpstreamSingleChunk(_ data: Data, remark: String) {
		guard let up = self.upstream, alive() else { return }

		// 背压护栏：在途过大则入队 & 暂停读 APP
		if self.inFlightC2U >= self.maxInFlightC2U || self.upstreamPausedByBackpressure {
			self.c2uQueue.append(data)
			self._maybePauseReadFromApp()
			return
		}

		self.inFlightC2U &+= data.count
		up.send(content: data, completion: .contentProcessed { [weak self] err in
			guard let s = self, s.alive() else { return }
			if let err = err {
				s.log("C2U write error (\(remark)): \(err)")
				s.cancel(reason: "C2U write failed")
				return
			}
			s.inFlightC2U &-= data.count

			// 队列顺序排空；若为空则恢复读取
			if !s.c2uQueue.isEmpty {
				let next = s.c2uQueue.removeFirst()
				s.upstreamPausedByBackpressure = false
				s._writeUpstreamSingleChunk(next, remark: "c->u(queued)")
			} else {
				s.upstreamPausedByBackpressure = false
				s._resumeReadFromAppIfPaused()
			}
		})
	}
    
    private func scheduleCUFlush(allowExtend: Bool = true) {
        cuFlushTimer?.setEventHandler {}   // 新增：先清 handler
        cuFlushTimer?.cancel()
        
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(CU_FLUSH_MS))
        
        t.setEventHandler { [weak self] in

			autoreleasepool {
				guard let s = self, s.alive() else { return }
				
				// 若到点仍很小且允许再延一次，则小幅延时后再冲刷
				if !s.closed, allowExtend, s.cuBuffer.count > 0, s.cuBuffer.count < s.CU_MIN_FLUSH_BYTES {
					s.cuFlushTimer?.cancel()
					let t2 = DispatchSource.makeTimerSource(queue: s.queue)
					t2.schedule(deadline: .now() + .milliseconds(s.CU_EXTRA_MS))
					
					t2.setEventHandler { [weak s] in
						autoreleasepool {
							guard let s = s, s.alive() else { return }
							s.flushCUBuffer()
						}
					}
					
					
					s.cuFlushTimer = t2
					t2.resume()
				} else {
					s.flushCUBuffer()
				}
			}
        }
        cuFlushTimer = t
        t.resume()
    }
    
    @inline(__always)
    private func inflightBudget() -> (bytes: Int, count: Int) {
        // —— 动态预算：BDP × gain（回退到固定值）
        if bbrBwMax_bps <= 0 || bbrMinRtt_ms <= 0 {
            return (BBR_FALLBACK_BUDGET_BYTES, BBR_FALLBACK_COUNT)
        }
        let bdpBytes = Int((bbrBwMax_bps / 8.0) * (bbrMinRtt_ms / 1000.0))
        if bdpBytes <= 0 {
            return (BBR_FALLBACK_BUDGET_BYTES, BBR_FALLBACK_COUNT)
        }
        var target = Int(Double(bdpBytes) * bbrCwndGain)
        if bbrState == .drain {
            target = Int(Double(target) * min(1.0, bbrPacingGain))
        }
        target = min(max(target, BBR_MIN_BUDGET_BYTES), BBR_MAX_BUDGET_BYTES)
        return (target, BBR_FALLBACK_COUNT)
    }

    private func flushCUBuffer() {
        guard !cuBuffer.isEmpty, !closed else { return }
        let payload = cuBuffer
        cuBuffer.removeAll(keepingCapacity: false)

        // —— 扣减全局水位
        subGlobalBytes(payload.count)

        self.sendToUpstream(payload, remark: "c->u")

        // —— 如果是回压态，且缓冲已清空，则恢复继续读客户端
        if pausedC2U && cuBuffer.isEmpty {
            self.maybeResumeAfterInflightDrained()
        }
    }

	private func ensureRoleTimer() { /* shared tick via BridgeCoordinator; no-op */ }
    
    private func pumpClientToUpstream() {
        if closed { return }
        
		 pendingOperations.incrementAndGet()  // 进入操作
			client.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in

				defer {
					self?.pendingOperations.decrementAndGet()  // 确保退出
				}
				autoreleasepool {
					guard let self = self else { return }
					if !self.alive() { return }
					
					if let err = err {
						self.log("UUID:\(self.UUID ?? "") client recv err: \(err)")
						self.queue.async { self.cancel(reason: "UUID:\(self.UUID ?? "") client recv err") }
						return
					}
					
					if let d = data, !d.isEmpty {

						self.winUpBytes &+= d.count
						
						self.vlog("UUID:\(self.UUID ?? "") recv from client: \(d.count)B")

						// 僅在疑似上傳測速時才走 ST 1ms 微合併；首頁/一般流量走一般微批
						if self.isSpeedtestUploadMode && (1...300).contains(d.count) {
							self.smallC2UEvents &+= 1

							// 🔸 改为：测速上传微合并（首包已发出后才启动，避免影响握手）
							if self.tFirstSend != nil {
								self.stBuffer.append(d)
								self.addGlobalBytes(d.count)

								// ★ 本地硬限（与 appendToCUBuffer 一致）：
								let localBuffered = self.cuBuffer.count + self.stBuffer.count + self.inflightBytes
								if localBuffered >= self.LOCAL_BUFFER_HARD_CAP {
									if !self.pausedC2U { self.pausedC2U = true }
									self.flushSTBuffer()
									self.flushCUBuffer()
									self.scheduleBackpressureTimer()
									self.vlog("local cap hit (st): paused, local=\(localBuffered)")
									return
								}

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
                                        self.scheduleBackpressureTimer()
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
							self.handleAppToUpstream(d)
						}


						// 若已收到上游的 100-Continue，则统计 100 之后客户端是否真的发了实体
						if self.saw100Continue {
							self.bodyBytesAfter100 &+= d.count
						}

						// 仅当本次不是“测速小块直发”时，才参与微批触发判断
						if !(self.isSpeedtestUploadMode && (1...300).contains(d.count)) {
							let effBytes = self.cuFlushBytesOverride ?? self.CU_FLUSH_BYTES
							let effMs    = self.cuFlushMsOverride    ?? self.CU_FLUSH_MS
							if self.cuBuffer.count >= effBytes { self.flushCUBuffer() }
							else { self.scheduleCUFlush(afterMs: effMs) }
						}
					}
					
					
					if isComplete {
						self.log("UUID:\(self.UUID ?? "") client EOF")
						self.eofClient = true
						self.flushCUBuffer()
						self.flushSTBuffer()
						
						if (self.saw100Continue && self.bodyBytesAfter100 == 0) || self.uploadStuck {
							self.scheduleDrainCancel(hint: "UUID:\(self.UUID ?? "") client EOF (deferred)")
						} else {
							self.upstream?.send(content: nil, completion: .contentProcessed({ _ in }))
							self.scheduleDrainCancel(hint: "UUID:\(self.UUID ?? "") client EOF")
						}
						return
					}
					
					// 继续接收
					if !self.pausedC2U {
						self.pumpClientToUpstream()
					} else {
						self.vlog("UUID:\(self.UUID ?? "") pause c->u receive due to backpressure")
					}
				}
			}
							
						
					
				
	}

	private func scheduleCUFlush(afterMs: Int) {
		// 确保先清理旧定时器
		safeStopTimer(&cuFlushTimer)
		
		let t = DispatchSource.makeTimerSource(queue: queue)
		t.schedule(deadline: .now() + .milliseconds(afterMs))
		t.setEventHandler { [weak self] in  // 使用 weak self
			autoreleasepool {
				guard let s = self, s.alive() else { return }
				s.flushCUBuffer()
			}
		}
		cuFlushTimer = t
		t.resume()
	}
    
    
    
    private func scheduleDrainCancel(hint: String) {
        // half-close 空闲计时器：每次调用都会重置，确保有活动就不收尾
        drainTimer?.setEventHandler {}   // 新增
        drainTimer?.cancel()
        
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + drainGrace)
        timer.setEventHandler { [weak self] in
			autoreleasepool {
				guard let s = self, s.alive() else { return }
				s.cancel(reason: "drain timeout after \(hint)")
			}
        }
        drainTimer = timer
        timer.resume()
    }
    

    private static var tunnelErrStamps = [DispatchTime]()
    private static let tunnelErrLock = NSLock()
    private static let tunnelBurstThreshold = 5         // 1.5s 內 >=5 條視為隧道級
    private static let tunnelBurstWindowMs: Double = 1500

    @inline(__always)
    private static func markAndBurstTunnelDown() -> Bool {
        let now = DispatchTime.now()
        tunnelErrLock.lock()
        defer { tunnelErrLock.unlock() }
        tunnelErrStamps.append(now)
        let cutoff = now.uptimeNanoseconds &- UInt64(tunnelBurstWindowMs * 1e6)
        tunnelErrStamps = tunnelErrStamps.filter { $0.uptimeNanoseconds >= cutoff }
        return tunnelErrStamps.count >= tunnelBurstThreshold
    }
    
	// 追加：節流“READY TO LISTEN”的熱路徑日誌
	private var didLogDownListenOnce = false
	private var lastDownListenLog: DispatchTime?
	private let DOWN_LISTEN_LOG_MIN_INTERVAL_MS: Double = 1000

	@inline(__always)
	private func etld1(of host: String) -> String {
		let parts = host.lowercased().split(separator: ".")
		if parts.count >= 2 { return parts.suffix(2).joined(separator: ".") }
		return host.lowercased()
	}

	private func pumpDownstreamToClient() {
        if closed { return }

        if pausedD2C {
            vlog("d->c receive paused inflight=\(downInflightBytes) read=\(downMaxRead)")
            return
        }
        
		// 只在首次或距上次 >= 500ms 時打印，避免刷屏造成 CPU/IO 壓力
		let now = DispatchTime.now()
		var shouldLog = false
		if !didLogDownListenOnce {
			shouldLog = true
		} else if let last = lastDownListenLog {
			let elapsedMs = Double(now.uptimeNanoseconds - last.uptimeNanoseconds) / 1_000_000.0
			if elapsedMs >= DOWN_LISTEN_LOG_MIN_INTERVAL_MS { shouldLog = true }
		}
		if shouldLog {
			log("downstream READY TO LISTEN pausedD2C=\(self.pausedD2C ? 1 : 0) read=\(self.downMaxRead)B")
			didLogDownListenOnce = true
			lastDownListenLog = now
		}

        downstream?.receive(minimumIncompleteLength: 1, maximumLength: downMaxRead) { [weak self] (data, _, isComplete, err) in
			autoreleasepool {
				guard let self = self, self.alive() else { return }

				if let err = err {
					// 1) ENODATA -> 软 EOF（不立刻 cancel）
					if case let .posix(code) = (err as? NWError), code.rawValue == 96 {
						self.log("CLOSE_CLASS=SOFT note=downstream POSIX=96 -> EOF")
						self.eofUpstream = true
						self.client.send(content: nil, completion: .contentProcessed({ _ in }))
						self.scheduleDrainCancel(hint: "downstream ENODATA EOF")
						return
					}
					// 2) 已有下行数据/首字节时，54/50/102 也走软 EOF
					if case let .posix(c) = (err as? NWError),
					[54, 50, 102].contains(c.rawValue),
					(self.tFirstByte != nil || self.bytesDown > 0) {
						self.log("CLOSE_CLASS=SOFT note=downstream POSIX=\(c.rawValue) after data")
						self.eofUpstream = true
						self.client.send(content: nil, completion: .contentProcessed({ _ in }))
						self.scheduleDrainCancel(hint: "downstream soft EOF posix \(c.rawValue)")
						return
					}
					// 3) 其他错误才走硬取消（保留隧道突发判断）
					var mapped = "downstream recv err"
					if case let .posix(c) = (err as? NWError), [54, 50, 102].contains(c.rawValue) {
						if Self.markAndBurstTunnelDown() {
							self.log("KILL_CLASS=TUNNEL_DOWN note=burst \(c.rawValue) on downstream")
						} else {
							self.log("KILL_CLASS=NETWORK_ERR note=downstream POSIX=\(c.rawValue)")
						}
					}
					self.queue.async { self.cancel(reason: mapped) }
					return
				}

				if let d = data, !d.isEmpty {
					self.winDownBytes &+= d.count

					self.vlog("recv from downstream: \(d.count)B")
					self.downLogBytesAccum &+= d.count
					let now2 = DispatchTime.now()
					var shouldAggLog = false
					if let last = self.lastDownLog {
						let elp = Double(now2.uptimeNanoseconds &- last.uptimeNanoseconds) / 1_000_000.0
						if elp >= self.DOWN_LOG_MIN_INTERVAL_MS || self.downLogBytesAccum >= 64*1024 { shouldAggLog = true }
					} else { shouldAggLog = true }

					if shouldAggLog {

						self.vlog("recv from downstream (agg): \(self.downLogBytesAccum)B")

						self.downLogBytesAccum = 0
						self.lastDownLog = now2
					}
					
					
					if self.tFirstByte == nil {
						self.tFirstByte = .now()

						if let dr = self.tDownReady {
							let ms = self.diffMs(start: dr, end: self.tFirstByte!)
							self.log(String(format:"KPI downReady_to_firstRecv_ms=%.1f", ms))
						}



						self.firstByteWatchdog?.setEventHandler {}
						self.firstByteWatchdog?.cancel()
						self.firstByteWatchdog = nil

						let ttfbMs = Double(self.tFirstByte!.uptimeNanoseconds &- self.tStart.uptimeNanoseconds) / 1e6
						self.log(String(format: "KPI immediate TTFB_ms=%.1f", ttfbMs))
						if let ts = self.tFirstSend {
							let segMs = Double(self.tFirstByte!.uptimeNanoseconds &- ts.uptimeNanoseconds) / 1e6
							self.log(String(format: "KPI firstSend_to_firstRecv_ms=%.1f", segMs))
						}
					}

					// 100-Continue 识别必须放在“下行”链路
					if !self.saw100Continue {
						if let s = String(data: d.prefix(16), encoding: .ascii),
						s.hasPrefix("HTTP/1.1 100") || s.hasPrefix("HTTP/1.0 100") {
							self.saw100Continue = true
							self.log("observed downstream HTTP 100-Continue")
						}
					}

					self.bytesDown &+= d.count
					self.sendToClient(d, remark: "down->client")
					
					// 如果“上游寫端 half-close”已發送，但下行仍在流動，也刷新
					if self.eofUpstream {
						self.scheduleDrainCancel(hint: "upstream EOF (still draining to client)")
					}

					if !self.pausedD2C { self.pumpDownstreamToClient() }
				}

				if isComplete {
					self.log("downstream EOF")
					self.eofUpstream = true     // 下行方向等价于“上游写完”
					self.client.send(content: nil, completion: .contentProcessed({ _ in }))
					self.scheduleDrainCancel(hint: "downstream EOF")
					return
				}
			}
        }
    }

	// 非阻塞地等待 pendingOperations 清零后做最终回收
	private func finalizeCancelWhenIdle(_ retries: Int = 0) {
		if self.pendingOperations.get() == 0 || retries >= 10 {
			// 执行最终清理
			if let cb = self.onClosed { 
				let bid = self.id
				// 使用全局队列而不是自己的队列，避免循环引用
				DispatchQueue.global(qos: .utility).async { cb(bid) }
			}
			DomainGate.shared.release(domain: etld1(of: self.resHost))
			self.isCancelling = false
			return
		}

		DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + .milliseconds(5)) { [weak self] in
			self?.finalizeCancelWhenIdle(retries + 1)
		}
	}

	private func applyPressureCapsIfNeeded() {
		Self.globalLock.lock()
		let g = Self.globalBufferedBytes
		let budget = Self.GLOBAL_BUFFER_BUDGET
		Self.globalLock.unlock()

		let util = Double(g) / Double(budget)
		if util >= 0.90 {
			// 壓力很大：兩邊都收緊（保命）
			if downReadHardCap != 4*1024 || cuFlushBytesOverride != 4*1024 || cuFlushMsOverride != 4 {
				downReadHardCap = 4 * 1024
				cuFlushBytesOverride = 4 * 1024
				cuFlushMsOverride = 4
				log(String(format:"PRESSURE cap ON (util=%.0f%%): downRead=4KB, upFlush=4KB/4ms", util*100))
			}
		} else if util <= 0.50 {
			// 壓力低：解除壓力護欄（保留角色導向的 caps）
			if cuFlushBytesOverride == 4*1024 || cuFlushMsOverride == 4 || downReadHardCap == 4*1024 {
				cuFlushBytesOverride = nil
				cuFlushMsOverride = nil
				// 若當前角色不是 upstream-primary，就解除 downRead 硬上限
				if primaryRole != .upstream { downReadHardCap = nil }
				log(String(format:"PRESSURE cap OFF (util=%.0f%%)", util*100))
			}
		}
	}
    
    // ==== BBR periodic sampler (called by onTick200) ====
    @inline(__always)
    private func bbrOnTick() {
        let now = DispatchTime.now()
        // 采样窗口：>=50ms 避免抖动
        let dtNs = now.uptimeNanoseconds &- bbrSampleTs.uptimeNanoseconds
        if dtNs >= 50_000_000 {
            let delta = max(0, bytesUp - bbrPrevBytesUp)
            let bps = Double(delta) * 8.0 / (Double(dtNs) / 1e9)   // bits/s
            bbrBwUp_bps = bps
            // 简单衰减的 max filter
            bbrBwMax_bps = max(bbrBwMax_bps * 0.90, bps)
            bbrPrevBytesUp = bytesUp
            bbrSampleTs = now
        }
        // 启动后 3s 进入 probeBW
        if bbrState == .startup {
            let ageMs = diffMs(start: tStart, end: now)
            if ageMs >= 3000 {
                bbrState = .probeBW
                bbrPacingGain = 1.0
                bbrCwndGain  = 2.0
            }
        }
    }

    
    // 首包回包看门狗（避免黑洞 60–100s 挂死；测速上传场景按策略放宽/禁用）
    private var firstByteWatchdog: DispatchSourceTimer?
    private var resumeCheckTimer: DispatchSourceTimer?

	private var downLogBytesAccum = 0
	private var lastDownLog: DispatchTime?
	private let DOWN_LOG_MIN_INTERVAL_MS: Double = 400
    
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

        inflightBytes &+= sz
        
        if inflightBytes >= B || inflight.count >= C {
            if !pausedC2U {
                pausedC2U = true
                vlog("pause c->u due to inflight budget: bytes=\(inflightBytes) count=\(inflight.count)")
                scheduleMaybeResumeCheck()   // 新增：确保很快检查一次是否可以恢复
            }
        }
        

        vlog("send \(remark) \(data.count)B -> upstream #\(seq)")
        
        pendingOperations.incrementAndGet()  // 标记操作开始
        
        up.send(content: data, completion: .contentProcessed({ [weak self] err in
            
			defer {
				self?.pendingOperations.decrementAndGet()
			}

			autoreleasepool {
				guard let self = self, self.alive() else { return }

				

				// —— ★★ 无论如何，先做一次出账（只会在第一次回调时成功扣减）
				let firstCompletion = self.inflight.remove(seq) != nil

				if firstCompletion {
					self.inflightBytes &-= sz
					if self.inflightBytes < 0 { self.inflightBytes = 0 }
					self.maybeResumeAfterInflightDrained()  // ← 新增：成功出账后立刻尝试恢复
				} else {
					self.vlog("dup completion for #\(seq), ignore")
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
					if self.isSpeedtestUploadMode {
						self.bytesUpAtFirstSend = self.bytesUp
						self.uploadStuckTimer?.cancel()
						let t = DispatchSource.makeTimerSource(queue: self.queue)

						t.schedule(deadline: .now() + .seconds(10))
						t.setEventHandler { [weak self] in
							autoreleasepool {
								guard let s = self, s.alive() else { return }
								let upDelta = max(0, s.bytesUp - s.bytesUpAtFirstSend)
								if upDelta < 32 * 1024 && s.smallC2UEvents >= 10 && !s.uploadStuck {
									s.uploadStuck = true
									s.log("UPLOAD_STUCK req=\(s.reqHost):\(s.reqPort) res=\(s.resHost):\(s.resPort) up=\(upDelta)B down=\(s.bytesDown)B small_events=\(s.smallC2UEvents)")
									// 仅延后收尾由 drain idle 控制（不主动 half-close）
									s.scheduleDrainCancel(hint: "UPLOAD_STUCK")
								}
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
					
					let isHTTPS = (self.reqPort == 443)
					if !self.isSpeedtestUploadMode {
						let watchdogDelay: TimeInterval = isHTTPS ? 15.0 : 8.0
						let wd = DispatchSource.makeTimerSource(queue: self.queue)
						wd.schedule(deadline: .now() + watchdogDelay)
						wd.setEventHandler { [weak self] in
							guard let s = self, s.alive() else { return }
							s.log("KPI watchdog: no first byte within \(Int(watchdogDelay*1000))ms after firstBody; fast-fail")
							s.cancel(reason: "first_byte_timeout")
						}
						self.firstByteWatchdog = wd
						wd.resume()
					} else {
						self.vlog("speedtest upload-mode: watchdog disabled")
					}
				}
				self.maybeResumeAfterInflightDrained()
			}

        }))
    }
    
    private func kpiLog(reason: String) {
        let now = DispatchTime.now()
            let durMs = Double(now.uptimeNanoseconds &- tStart.uptimeNanoseconds) / 1e6
            let hsMs: Double? = tReady.map { diffMs(start: tStart, end: $0) }
            let fbMs: Double? = tFirstByte.map { diffMs(start: tStart, end: $0) }

            func f(_ x: Double?) -> String { x.map { String(format: "%.1f", $0) } ?? "-" }
            let durStr = String(format: "%.1f", durMs)

            log("KPI req=\(reqHost):\(reqPort) res=\(resHost):\(resPort) reason=\(reason) hsRTT_ms=\(f(hsMs)) ttfb_ms=\(f(fbMs)) up_bytes=\(bytesUp) down_bytes=\(bytesDown) dur_ms=\(durStr)")
    }
    

    private func sendToClient(_ data: Data, remark: String) {
        guard alive() else { log("Cannot send to client: closed=\(closed)"); return }

        // 下行 in-flight 入账
        downInflightBytes &+= data.count
        downInflightCount &+= 1
        if downInflightBytes >= downInflightBudgetBytes() && !pausedD2C {
            pausedD2C = true
            vlog("pause d->c receive due to inflight: \(downInflightBytes)B")
        }

        let sendStart = DispatchTime.now()
        client.send(content: data, completion: .contentProcessed({ [weak self] err in

			autoreleasepool {
				guard let self = self, self.alive() else { return }

				// 下行 in-flight 出账（兜底）
				self.downInflightBytes &-= data.count
				if self.downInflightBytes < 0 { self.downInflightBytes = 0 }
				self.downInflightCount &-= 1
				if self.downInflightCount < 0 { self.downInflightCount = 0 }

				if let err = err {
					self.log("client send err: \(err)")
					self.queue.async { self.cancel(reason: "client send err") }
				} else {
					self.vlog("sent \(remark) successfully")
					// 下行 BBR：统计交付字节 + 单块“发送完成时延”作为 RTT 样本
					self.downDeliveredBytes &+= data.count
					let dtNs = DispatchTime.now().uptimeNanoseconds &- sendStart.uptimeNanoseconds
					let ms = Double(dtNs) / 1e6
					if ms > 0 {
						let ageMs = self.diffMs(start: self.d_bbrMinRttStamp, end: DispatchTime.now())
						if ms < self.d_bbrMinRtt_ms || ageMs > 10_000 {
							self.d_bbrMinRtt_ms = ms
							self.d_bbrMinRttStamp = DispatchTime.now()
						}
					}
				}

				// in-flight 降到阈值以下时尝试恢复下行接收
				self.maybeResumeDownAfterInflightDrained()
			}
        }))
    }

    @inline(__always)
    private func maybeResumeDownAfterInflightDrained() {
        let target = downInflightBudgetBytes()
        if pausedD2C && downInflightBytes <= (target * 90) / 100 {
            pausedD2C = false
            vlog("resume d->c after inflight drained: \(downInflightBytes)B <= 0.9*target")
            pumpDownstreamToClient()
        }
    }
    
    private func diffMs(start: DispatchTime, end: DispatchTime) -> Double {
        return Double(end.uptimeNanoseconds &- start.uptimeNanoseconds) / 1e6
    }

	#if DEBUG

	/// 每 5s 打一次“水位/内存”摘要（仅 DEBUG）
	private func startMemSummary() {
		memSummaryTimer?.setEventHandler {}
		memSummaryTimer?.cancel()
		memSummaryTimer = nil

		let t = DispatchSource.makeTimerSource(queue: queue)
		t.schedule(deadline: .now() + .seconds(5), repeating: .seconds(5))
		t.setEventHandler { [weak self] in
			autoreleasepool {
				guard let s = self, s.alive() else { return }
				Self.globalLock.lock()
				let globalBytes = Self.globalBufferedBytes
				Self.globalLock.unlock()

				let inflightB = s.inflightBytes
				let inflightN = s.inflight.count
				let cuLen = s.cuBuffer.count
				let stLen = s.stBuffer.count
				let paused = s.pausedC2U ? 1 : 0
				let limit  = s.currentBufferLimit
				let up     = s.bytesUp
				let down   = s.bytesDown

				if let rss = s.processResidentSizeMB() {
					s.log(String(format: "MEM summary: rss=%.1fMB global=%dB inflight=%dB(#%d) cu=%dB st=%dB paused=%d limit=%dB up=%d down=%d",
								rss, globalBytes, inflightB, inflightN, cuLen, stLen, paused, limit, up, down))
				} else {
					s.log(String(format: "MEM summary: rss=NA global=%dB inflight=%dB(#%d) cu=%dB st=%dB paused=%d limit=%dB up=%d down=%d",
								globalBytes, inflightB, inflightN, cuLen, stLen, paused, limit, up, down))
				}
			}
		}
		memSummaryTimer = t
		t.resume()
	}
	#endif

}

final class DomainGate {
    static let shared = DomainGate(limitPerDomain: 16, globalLimit: 64)
        private let q = DispatchQueue(label: "domain.gate", qos: .userInitiated)
        private let perLimit: Int
        private let globalLimit: Int
        private var per: [String:Int] = [:]
        private var global = 0
        private var pend: [String:[() -> Void]] = [:]
        
        init(limitPerDomain: Int, globalLimit: Int) {
            self.perLimit = limitPerDomain
            self.globalLimit = globalLimit
        }
        
        func acquire(domain: String, run: @escaping () -> Void) {
            q.async { [weak self] in
                guard let self = self else { return }
                
                if self.global >= self.globalLimit || (self.per[domain] ?? 0) >= self.perLimit {
                    // 存储闭包时使用 autoreleasepool
                    autoreleasepool {
                        self.pend[domain, default: []].append(run)
                    }
                } else {
                    self.global += 1
                    self.per[domain, default: 0] += 1
                    // 在新的队列中执行，避免阻塞 gate 队列
                    DispatchQueue.global(qos: .userInitiated).async {
                        autoreleasepool { run() }
                    }
                }
            }
        }
        
        func release(domain: String) {
            q.async { [weak self] in
                guard let self = self else { return }
                
                if self.global > 0 { self.global -= 1 }
                if let c = self.per[domain], c > 0 { self.per[domain] = c - 1 }
                
                if var queue = self.pend[domain], !queue.isEmpty {
                    let next = queue.removeFirst()
                    self.pend[domain] = queue.isEmpty ? nil : queue  // 清理空数组
                    self.global += 1
                    self.per[domain, default: 0] += 1
                    // 在新队列执行
                    DispatchQueue.global(qos: .userInitiated).async {
                        autoreleasepool { next() }
                    }
                }
            }
        }
}

// 放在同文件底部或新文件中
final class BridgeCoordinator {
    static let shared = BridgeCoordinator()
    private let q = DispatchQueue(label: "LayerMinusBridge.Coordinator", qos: .userInitiated)

    private struct WeakBox { weak var v: LayerMinusBridge? }
    private var conns: [WeakBox] = []

    private var tick200: DispatchSourceTimer?
    private var tick5s: DispatchSourceTimer?

    func add(_ b: LayerMinusBridge) {
        q.async { self.compact(); self.conns.append(.init(v: b)) }
    }
    func remove(_ b: LayerMinusBridge) {
        q.async { self.conns.removeAll { $0.v == nil || $0.v === b } }
    }
    private func compact() { conns.removeAll { $0.v == nil } }

    // 200ms 共享 tick：给每个连接派发「BBR/role/backpressure」周期工作
    func ensure200ms() {
        q.async {
            guard self.tick200 == nil else { return }
            let t = DispatchSource.makeTimerSource(queue: self.q)
            t.schedule(deadline: .now() + .milliseconds(200), repeating: .milliseconds(200))
            t.setEventHandler { [weak self] in
                guard let self = self else { return }
                var alive: [WeakBox] = []
                for box in self.conns {
                    if let b = box.v, b.alive() {
                        alive.append(box)
                        b.queue.async { b.onTick200() }   // ← 派发到连接自己的队列
                    }
                }
                self.conns = alive   // 压缩空槽
            }
            self.tick200 = t; t.resume()
        }
    }

    #if DEBUG
    // 5s 共享内存摘要（只打一条全局线）
    func ensure5sMemSummary() {
        q.async {
            guard self.tick5s == nil else { return }
            let t = DispatchSource.makeTimerSource(queue: self.q)
            t.schedule(deadline: .now() + .seconds(5), repeating: .seconds(5))
            t.setEventHandler { [weak self] in
                guard let self = self else { return }
                // 统计活动连接数
                let active = self.conns.compactMap { $0.v?.alive() == true ? $0.v : nil }
                // 利用已有的全局水位
                LayerMinusBridge.globalLock.lock()
                let g = LayerMinusBridge.globalBufferedBytes
                LayerMinusBridge.globalLock.unlock()
                if let any = active.first, let rss = any.processResidentSizeMB() {
                    any.log(String(format:"GLOBAL summary: rss=%.1fMB global=%dB conns=%d", rss, g, active.count))
                } else if let any = active.first {
                    any.log("GLOBAL summary: rss=NA global=\(g)B conns=\(active.count)")
                }
            }
            self.tick5s = t; t.resume()
        }
    }
    #endif
}


extension LayerMinusBridge {
    private func checkMemoryPressure() {
        let info = ProcessInfo.processInfo
        let physicalMemory = info.physicalMemory
        let threshold = physicalMemory / 10  // 10% 阈值
        
        if let rss = processResidentSizeMB(), 
           rss > Double(threshold) / (1024 * 1024) {
            // 触发紧急清理
            emergencyCleanup()
        }
    }
    
    private func emergencyCleanup() {
        // 1. 清理缓冲区
        cuBuffer = Data()
        stBuffer = Data()
        
        // 2. 取消非关键定时器
        safeStopTimer(&roleTimer)
        safeStopTimer(&backpressureTimer)
        
        // 3. 强制 GC
        autoreleasepool { }
        
        log("Emergency cleanup triggered")
    }
}
