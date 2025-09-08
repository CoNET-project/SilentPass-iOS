import Foundation
import Network
import os

#if DEBUG
import Darwin.Mach // for task_info / mach_task_basic_info
#endif


enum L {
    static func lm(_ s: @autoclosure () -> String) { print("[LayerMinusBridge] \(s())") }
    static func sc(_ s: @autoclosure () -> String) { print("[ServerConnection] \(s())") }
}

public final class LayerMinusBridge {
    @inline(__always)
    private func sendToDownstream(_ data: Data, remark: String) {
        guard let dn = downstream, alive() else {
            log("Cannot send to downstream: downstream=\(downstream != nil), closed=\(closed)")
            return
        }
        vlog("send \(remark) \(data.count)B -> downstream")
        dn.send(content: data, completion: .contentProcessed({ [weak self] err in
            guard let self = self, self.alive() else { return }
            if let err = err {
                self.log("downstream send err: \(err)")
                self.queue.async { self.cancel(reason: "downstream send err") }
            } else {
                self.vlog("sent \(remark) successfully (downstream)")
            }
        }))
    }
    
    private static let GLOBAL_BUFFER_BUDGET = 5 * 1024 * 1024
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
//        let h = targetHost.lowercased()
//        return (targetPort == 8080 || targetPort == 443) &&
//               (h.hasSuffix("ooklaserver.net")
//                 || h.hasSuffix("speedtest.net")
//                 || h.contains("measurementlab")
//                 || h.contains("mlab"))
        return true
    }
    
    #if DEBUG
    private var memSummaryTimer: DispatchSourceTimer?
    #endif
    
    #if DEBUG
    @inline(__always)
    private func processResidentSizeMB() -> Double? {
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
    
    /// 每 2s 打一次“水位/内存”摘要（仅 DEBUG）
    private func startMemSummary() {
        // 先停旧的（防重入）
        memSummaryTimer?.setEventHandler {}
        memSummaryTimer?.cancel()
        memSummaryTimer = nil

        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .seconds(2), repeating: .seconds(2))
        t.setEventHandler { [weak self] in
            guard let s = self, s.alive() else { return }

            // 读取全局与本连接水位
            Self.globalLock.lock()
            let globalBytes = Self.globalBufferedBytes
            Self.globalLock.unlock()

            let inflightB = s.inflightBytes
            let inflightN = s.inflight.count
            let cuLen = s.cuBuffer.count
            let stLen = s.stBuffer.count
            let paused = s.pausedC2U ? 1 : 0
            let limit = s.currentBufferLimit
            let up = s.bytesUp
            let down = s.bytesDown

            // 进程 RSS（可失败时就不打印）
            if let rss = s.processResidentSizeMB() {
                s.log(String(format:
                    "MEM summary: rss=%.1fMB global=%dB inflight=%dB(#%d) cu=%dB st=%dB paused=%d limit=%dB up=%d down=%d",
                    rss, globalBytes, inflightB, inflightN, cuLen, stLen, paused, limit, up, down))
            } else {
                s.log(String(format:
                    "MEM summary: rss=NA global=%dB inflight=%dB(#%d) cu=%dB st=%dB paused=%d limit=%dB up=%d down=%d",
                    globalBytes, inflightB, inflightN, cuLen, stLen, paused, limit, up, down))
            }
        }
        memSummaryTimer = t
        t.resume()
    }
    #endif
    

	private var currentBufferLimit: Int = 4 * 1024 // 初始大小 4KB
	private let maxBufferLimit: Int = 256 * 1024 // 最大大小 1MB
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
				currentBufferLimit = min(currentBufferLimit * 2, maxBufferLimit)
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
    
    private let reqHost: String
    private let reqPort: Int
    
    private let resHost: String
    private let resPort: Int
    
    private let verbose: Bool
    
    // --- KPI & 半关闭守护 ---
    private var tStart: DispatchTime = .now()
    private var tReady: DispatchTime?
    private var tFirstByte: DispatchTime?
    private var bytesUp: Int = 0
    private var bytesDown: Int = 0
    private var drainTimer: DispatchSourceTimer?
    // 半关闭后的“空闲超时”窗口：只要仍有对向数据活动就续期，空闲 >= 25s 才收尾
    private let drainGrace: TimeInterval = 60.0
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
        // 先取消旧的
        resumeCheckTimer?.setEventHandler {}
        resumeCheckTimer?.cancel()
        resumeCheckTimer = nil

        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(2))
        t.setEventHandler { [weak self] in
            guard let s = self, s.alive() else { return }
            s.maybeResumeAfterInflightDrained()
            // 触发一次即释放
            s.resumeCheckTimer?.setEventHandler {}
            s.resumeCheckTimer?.cancel()
            s.resumeCheckTimer = nil
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
    private var downstream: NWConnection?
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
        // 简单的生命周期日志
        NSLog("🟢 CREATED LayerMinusBridge #\(id) for reqHost \(reqHost):\(reqPort) resHost \(resHost):\(resPort) \(infoTag())")
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
            
            #if DEBUG
            self.startMemSummary()
            #endif
            
            self.log("start -> \(self.reqHost):\(self.reqPort) <-- \(self.resHost):\(self.resPort), firstBody(Base64) len=\(firstBodyBase64.count)")
            
            
            
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
            self.connectUpstreamAndRun(reqFirstBody: firstBody,resFirstBody: nil)
            
        }
    }
    
    
    @inline(__always)
    private func safeStopTimer(_ t: inout DispatchSourceTimer?) {
        guard let x = t else { return }
        x.setEventHandler {}      // 先断电
        x.cancel()                // 再取消
        t = nil                   // 置空
    }

    @inline(__always)
    private func gracefulCloseConnection(_ c: inout NWConnection?, label: String) {
        guard let conn = c else { return }
        // 断开回调，避免 cancel 时 stateUpdateHandler 再次访问 self
        conn.stateUpdateHandler = { _ in }
        // 尝试 half-close：这一步即便失败也不影响后续 cancel
        conn.send(content: nil, completion: .contentProcessed { [weak self] _ in
            guard let s = self, s.alive() else { return }
            // 给内核一点点时间把 FIN/队列送出去，避免即刻 cancel 触发底层断言
            s.queue.asyncAfter(deadline: .now() + .milliseconds(2)) { [weak s] in
                guard s?.alive() ?? false else { return }
                conn.cancel()
            }
        })
        c = nil
    }
    
    private var isCancelling = false
    public func cancel(reason: String) {

        // —— 保留并提前打印原有诊断日志（不动）
            log("CANCEL trigger id=\(id) reason=\(reason) inflightBytes=\(inflightBytes) inflightCount=\(inflight.count) cu=\(cuBuffer.count)B st=\(stBuffer.count)B global=\(Self.globalBufferedBytes)B pausedC2U=\(pausedC2U)")
            switch reason {
            case let s where s.contains("first_byte_timeout"):
                log("KILL_CLASS=TTFB_TIMEOUT note=watchdog fired after firstBody")
            case let s where s.contains("drain timeout"):
                log("KILL_CLASS=DRAIN_IDLE note=no opposite activity within \(Int(drainGrace))s")
            case let s where s.contains("recv err") || s.contains("failed"):
                log("KILL_CLASS=NETWORK_ERR note=peer/network failure")
            default:
                log("KILL_CLASS=MISC note=\(reason)")
            }
        
#if DEBUG
safeStopTimer(&memSummaryTimer)
#endif

            // —— 可重入防抖 + 标记关闭（保持你现有的 stateLock 语义）
            stateLock.lock()
            if closed || isCancelling { stateLock.unlock(); return }
            closed = true
            isCancelling = true
            stateLock.unlock()

            // 停止数据面：立即暂停继续收
            pausedC2U = true

            // —— 安全停表：所有计时器先“断电”再 cancel，避免尾随触发
            safeStopTimer(&firstByteWatchdog)
            safeStopTimer(&drainTimer)
            safeStopTimer(&uploadStuckTimer)
            safeStopTimer(&cuFlushTimer)
            safeStopTimer(&stTimer)
            safeStopTimer(&resumeCheckTimer)
            safeStopTimer(&backpressureTimer)   // ← 新增：把背压定时器也停掉

            // —— 结清在途 & 缓冲、释放全局预算（保持你原有逻辑）
            if !cuBuffer.isEmpty { subGlobalBytes(cuBuffer.count) }
            if !stBuffer.isEmpty { subGlobalBytes(stBuffer.count) }
            inflightSizes.removeAll(keepingCapacity: false)
            inflight.removeAll(keepingCapacity: false)
            inflightBytes = 0
            cuBuffer.removeAll(keepingCapacity: false)
            stBuffer.removeAll(keepingCapacity: false)

            // —— KPI 在“硬关闭”前打印（保留）
            kpiLog(reason: reason)
            log("cancel: \(reason)")


            // —— 软关闭三条连接
            gracefulCloseConnection(&upstream,   label: "UP")      // 可变可置空
            gracefulCloseConnection(&downstream, label: "DOWN")    // 可变可置空
            gracefulCloseImmutableConnection(client, label: "CLIENT") // 不可变，只做优雅收尾

            // —— onClosed 放最后，且异步回调，避免回调里再同步重入
            if let cb = onClosed {
                let bid = id
                queue.async { cb(bid) }
            }

            isCancelling = false
    }
    
    @inline(__always)
    private func gracefulCloseImmutableConnection(_ conn: NWConnection, label: String) {
        // 切断回调，避免 cancel 时回调里再访问 self
        conn.stateUpdateHandler = { _ in }
        // half-close（发送 FIN），然后稍后再 cancel，避免底层断言
        conn.send(content: nil, completion: .contentProcessed { [weak self] _ in
            guard let s = self, s.alive() else { return }
            s.queue.asyncAfter(deadline: .now() + .milliseconds(2)) {
                conn.cancel()
            }
        })
    }
    
    public func start(
        reqFirstBodyBase64: String,
        resFirstBodyBase64: String
    ) {
        queue.async { [weak self] in
            guard let self = self, self.alive() else { return }

            self.tStart = .now()
            
#if DEBUG
self.startMemSummary()
#endif
            self.log("start (dual-first-body) -> req=\(self.reqHost):\(self.reqPort)  res=\(self.resHost):\(self.resPort)  reqLen=\(reqFirstBodyBase64.count)  resLen=\(resFirstBodyBase64.count)")

            if let th = self.tHandoff {
                let ms = Double(self.tStart.uptimeNanoseconds &- th.uptimeNanoseconds) / 1e6
                self.log(String(format: "KPI handoff_to_start_ms=%.1f", ms))
            }

            guard let reqFirst = Data(base64Encoded: reqFirstBodyBase64), let resFirst = Data(base64Encoded: resFirstBodyBase64) else {
                self.log("reqFirstBody base64 decode failed")
                self.queue.async { self.cancel(reason: "Invalid Base64 (req)") }
                return
            }
            self.log("start dual-first-body -> req.len=\(reqFirst.count) res.len=\(resFirst.count) req=\(self.reqHost):\(self.reqPort) res=\(self.resHost):\(self.resPort)")
            self.connectUpstreamAndRun(reqFirstBody: reqFirst, resFirstBody: resFirst)
        }
    }
    
    private var downIgnoreBytes: Int = 0
    
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

        // 两条连接并发启动
        let up = NWConnection(host: NWEndpoint.Host(self.reqHost), port: reqNWPort, using: params)
        let down = NWConnection(host: NWEndpoint.Host(self.resHost), port: resNWPort, using: params)
        self.upstream = up
        self.downstream = down

        var upReady = false
        var downReady = false
        var reqFirstSent = false
        var resFirstSent = false
        var pumpsStarted = false   // ← 新增：防止双泵重复启动

        func maybeKickPumps() {
            guard alive() else { return }

            // 1) 上行 ready 就把请求首包发出去（必须用 "firstBody" 触发 watchdog 与 tFirstSend）
            if upReady, !reqFirstSent {
                reqFirstSent = true
                if !reqFirstBody.isEmpty {
                    self.sendToUpstream(reqFirstBody, remark: "firstBody")  // ← 关键修正（原来是 "firstBody(req)"）
                }
            }

            // 2) 下行 ready 且提供了下行首包时，直接注入给客户端（不影响 watchdog）
            if downReady, !resFirstSent, let rb = resFirstBody, !rb.isEmpty {
                self.bytesDown &+= rb.count
                    self.log("DOWN ready; sending resFirstBody \(rb.count)B to \(self.resHost):\(self.resPort)")
                    self.downIgnoreBytes &+= rb.count      // 关键：标记这些字节若被回流，不算“首字节”
                    self.sendToDownstream(rb, remark: "firstBody(res)")
                    self.log("resFirstBody sent to downstream")
                    resFirstSent = true
            }

            // 3) 两端都 ready 后，只启动一次双泵
            if upReady && downReady && !pumpsStarted {
                pumpsStarted = true
                self.pumpClientToUpstream()
                self.pumpDownstreamToClient()
            }
        }

        up.stateUpdateHandler = { [weak self] st in
            guard let s = self, s.alive() else { return }
            switch st {
            case .ready:
                s.log("UP ready \(s.reqHost):\(s.reqPort)")
                s.tReady = .now()
                upReady = true
                maybeKickPumps()
            case .waiting(let e):
                s.log("UP waiting: \(e)")
            case .failed(let e):
                s.log("UP failed: \(e)"); s.queue.async { s.cancel(reason: "upstream failed") }
            case .cancelled:
                s.log("UP cancelled"); s.queue.async { s.cancel(reason: "upstream cancelled") }
            default:
                s.log("UP state: \(st)")
            }
        }

        down.stateUpdateHandler = { [weak self] st in
            guard let s = self, s.alive() else { return }
            switch st {
            case .ready:
                s.log("DOWN ready \(s.resHost):\(s.resPort)")
                downReady = true
                maybeKickPumps()
            case .waiting(let e):
                s.log("DOWN waiting: \(e)")
            case .failed(let e):
                s.log("DOWN failed: \(e)"); s.queue.async { s.cancel(reason: "downstream failed") }
            case .cancelled:
                s.log("DOWN cancelled"); s.queue.async { s.cancel(reason: "downstream cancelled") }
            default:
                s.log("DOWN state: \(st)")
            }
        }

        log("Connecting: UP \(reqHost):\(reqPort)  |  DOWN \(resHost):\(resPort)")
        up.start(queue: queue)
        down.start(queue: queue)
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
        return (768 * 1024, 320)
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
    
    private func pumpDownstreamToClient() {
        if closed { return }
        downstream?.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in
            guard let self = self, self.alive() else { return }

            if let err = err {
                self.log("downstream recv err: \(err)")
                self.queue.async { self.cancel(reason: "downstream recv err") }
                return
            }

            if let d = data, !d.isEmpty {
                self.vlog("recv from downstream: \(d.count)B")

                var effectiveCount = d.count
                if self.downIgnoreBytes > 0 {
                    let ignored = min(self.downIgnoreBytes, d.count)
                    self.downIgnoreBytes -= ignored
                    effectiveCount -= ignored
                    if ignored > 0 {
                        self.vlog("ignored \(ignored)B echoed-resFirstBody; remain downIgnoreBytes=\(self.downIgnoreBytes)")
                    }
                }
                
                
                if self.tFirstByte == nil && effectiveCount > 0 {
                    self.tFirstByte = .now()
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
                
                if self.eofClient { self.scheduleDrainCancel(hint: "client EOF (downstream->client activity)") }  // 你已有
                // 再补：如果“上游写端 half-close”已发送，但下行仍在流动，也刷新
                if self.eofUpstream {
                    self.scheduleDrainCancel(hint: "upstream EOF (still draining to client)")
                }

                if self.eofClient {
                    self.scheduleDrainCancel(hint: "client EOF (downstream->client activity)")
                }
            }

            if isComplete {
                self.log("downstream EOF")
                self.eofUpstream = true     // 下行方向等价于“上游写完”
                self.client.send(content: nil, completion: .contentProcessed({ _ in }))
                self.scheduleDrainCancel(hint: "downstream EOF")
                return
            }

            self.pumpDownstreamToClient()
        }
    }
    
    // 首包回包看门狗（避免黑洞 60–100s 挂死；测速上传场景按策略放宽/禁用）
    private var firstByteWatchdog: DispatchSourceTimer?
    private var resumeCheckTimer: DispatchSourceTimer?
    
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
                scheduleMaybeResumeCheck()   // 新增：确保很快检查一次是否可以恢复
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
                            s.log("UPLOAD_STUCK req=\(s.reqHost):\(s.reqPort) res=\(s.resHost):\(s.resPort) up=\(upDelta)B down=\(s.bytesDown)B small_events=\(s.smallC2UEvents)")
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
                
                let isHTTPS = (self.reqPort == 443)
                if !self.isSpeedtestTarget {
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
                    self.vlog("speedtest target -> watchdog disabled")
                }
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
