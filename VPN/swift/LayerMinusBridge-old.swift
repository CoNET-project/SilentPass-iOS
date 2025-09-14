import Foundation
import Network
import os

enum L {
    static func lm(_ s: @autoclosure () -> String) { print("[LayerMinusBridge] \(s())") }
    static func sc(_ s: @autoclosure () -> String) { print("[ServerConnection] \(s())") }
}

public final class LayerMinusBridge_old {

	// MARK: - 一次性角色判定（仅在 TLS .appData 后按早期流量决定，之后不再反转）
	private enum FlowRole { case upstreamPrimary, downstreamPrimary }
	private var flowRole: FlowRole = .downstreamPrimary     // 默认偏保守：下行优先
	private var roleDecided = false
	private var bytesC2U: Int64 = 0    // client -> upstream
	private var bytesD2C: Int64 = 0    // upstream -> client
	private let ROLE_DECIDE_BASE: Int64 = 16 * 1024   // 累计总字节达到 16KB 才开始判定
	private let ROLE_DECIDE_MARGIN: Int64 = 32 * 1024 // 方向滞回 32KB，避免误判

	@inline(__always)
	private func observeBytesForOneShotRole(isC2U: Bool, n: Int) {
		// 仅在 appData 相位且未决策前统计
		guard tlsPhase == .appData, !roleDecided else { return }
		if isC2U { bytesC2U &+= Int64(n) } else { bytesD2C &+= Int64(n) }
		let c = bytesC2U, d = bytesD2C
		guard (c + d) >= ROLE_DECIDE_BASE else { return }
		// 一次性判定：明显一侧领先才定角色
		if c > d + ROLE_DECIDE_MARGIN {
			flowRole = .upstreamPrimary
			roleDecided = true
			log("ROLE decided -> UPSTREAM_PRIMARY (c2u=\(c)B, d2c=\(d)B)")
		} else if d > c + ROLE_DECIDE_MARGIN {
			flowRole = .downstreamPrimary
			roleDecided = true
			log("ROLE decided -> DOWNSTREAM_PRIMARY (c2u=\(c)B, d2c=\(d)B)")
		}
		// 若差距不足以决策，则继续累计，直到满足 margin
	}


	private enum TLSPhase {
		case handshake    // 仅握手流量（TLS record type 0x16）
		case appData      // 已出现应用数据（TLS record type 0x17）
	}

	private var tlsPhase: TLSPhase = .handshake

	// 握手期的中性窗口（避免首期被 4KB 限死）
	private let HANDSHAKE_MAXLEN: Int = 32 * 1024

	// 统计握手字节，仅用于观测
	private var hsBytesC2U: Int = 0
	private var hsBytesD2C: Int = 0

	@inline(__always)
	private func tlsRecordType(_ data: Data) -> UInt8? {
		// TLS 记录：ContentType(1) + Version(2) + Length(2) ...
		// ContentType: 0x16=Handshake, 0x17=ApplicationData
		guard data.count >= 5 else { return nil }
		let t = data[data.startIndex]
		// 粗判断版本 0x03 0x01..0x04；不严格卡版本以兼容
		let v1 = data[data.startIndex &+ 1]
		if v1 == 0x03 { return t }
		return nil
	}

	// —— 当前连接是否使用 AsyncStream 桥接（而非旧的 pump* 循环）
	private var usingBridge = false



    // MARK: - Global Memory Monitor (500ms)
    private struct MemoryState {
        var isUnderPressure = false
        var lastCheckTime = DispatchTime.now()
        var consecutivePressureCount = 0
    }

	// 默认软阈值（MB），可按机型/业务自行调小或调大
	private static let _MEM_SOFT_LIMIT_MB: Int = 48



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



	private func adjustBufferLimit() {
        let oldLimit = currentBufferLimit

		// 读取全局预算
		Self.globalLock.lock()
		let currentGlobalBytes = Self.globalBufferedBytes
		Self.globalLock.unlock()

		let canGrow = currentGlobalBytes <= Self.GLOBAL_BUFFER_BUDGET

		if pausedC2U {
			if canGrow {
				currentBufferLimit = min(currentBufferLimit * 4, maxBufferLimit)
			}
			// 全局超支时不增长：静默处理即可（不必每次打日志）
		} else {
			currentBufferLimit = max(currentBufferLimit / 4, 4 * 1024)
		}

		// ✅ 仅在变化时、且 verbose 时打印，避免刷屏
		if verbose, oldLimit != currentBufferLimit {
			vlog("adjust buffer limit: \(oldLimit) -> \(currentBufferLimit)")
		}

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
			if tlsPhase == .appData {
				pausedC2U = true
			}
			flushCUBuffer()  // 无论哪个阶段都要发送
			
			return
        }

        // ★ 全局预算触发：超出就立即flush并暂停读
        Self.globalLock.lock()
        let overBudget = Self.globalBufferedBytes > Self.GLOBAL_BUFFER_BUDGET
        Self.globalLock.unlock()
		if tlsPhase == .appData && overBudget {
			pausedC2U = true
			flushCUBuffer()
			
			return
		} else if overBudget {
			// 握手期也要flush，只是不设置背压
			flushCUBuffer()
			return
		}
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
        if pausedC2U {
			maybeResumeAfterInflightDrained()
		}
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
       
		if !closed {
			log("⚠️ WARNING: destroyed without proper closing!")
			// 同步清理，不异步
			client.cancel()
			upstream?.cancel()
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

	public func connectUpstreamAndRun(reqFirstBody: Data, resFirstBody: Data?) {
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

					// 新增：拆解 TTFB 的第一段——连接/握手耗时
					let connectMs = s.diffMs(start: s.tStart, end: s.tReady!)
					s.log(String(format: "KPI connectRTT_ms=%.1f", connectMs))

					upReady = true
					if isDIRECT {
					// 先就緒再觸發 maybeKickPumps；並補上 tDownReady 供 KPI 使用
						s.tDownReady = .now()
						downReady = true
					}
					maybeKickPumps()
					// ★ 新增：就绪后尝试排空直送队列（弱网/短暂阻塞后的恢复）
					s.drainC2UQueueIfPossible()

					// ★ 新增：可达/更优路径回调里也做一次排空，避免换路后卡半包
					if #available(iOS 13.0, *) {
						up.viabilityUpdateHandler = { [weak s] viable in
							guard let s = s, s.alive(), viable else { return }
							s.drainC2UQueueIfPossible()
						}
						up.betterPathUpdateHandler = { [weak s] _ in
							guard let s = s, s.alive() else { return }
							s.drainC2UQueueIfPossible()
						}
					}


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

		// 清理下行队列并出账（避免全局预算残留）
		if d2cBufferedBytes > 0 {
			subGlobalBytes(d2cBufferedBytes)
		}
		d2cQueue.removeAll(keepingCapacity: false)
		d2cBufferedBytes = 0
        
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
    

	// 来自 ServerConnection 的 handoff 瞬间标记
	public func markHandoffNow() {
		queue.async { [weak self] in
			self?.tHandoff = .now()
		}
	}
    


	private var d2cUnpauseTimer: DispatchSourceTimer?
    
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

		// —— 在途入账
		let sz = data.count
		inflightSizes[seq] = sz
		inflightBytes &+= sz

		// ★ 修正：握手期也要发送；仅“回压判定”在 appData 相位启用
		if tlsPhase == .appData {
			if inflightBytes >= B || inflight.count >= C {
				if !pausedC2U {
					pausedC2U = true
					vlog("pause c->u due to inflight budget: bytes=\(inflightBytes) count=\(inflight.count)")
				}
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

			// 发送完成（无论是否首包）
			if self.tlsPhase == .appData {
				self.adjustBufferLimit()            // ✅ 事件驱动调整
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

	private func bridgeConnections(client: NWConnection, remote: NWConnection) async {
		do {
			try await withThrowingTaskGroup(of: Void.self) { group in
				// C -> S（上行）
				group.addTask { [weak self] in
					guard let s = self else { return }
                    try await s.pipeOneDirection(
						from: client,
						to: remote,
						chunkSize: 256 * 1024,
						onBytes: { n in
							s.observeBytesForOneShotRole(isC2U: true, n: n)
							s.bytesUp += n
						}
					)
				}

				// S -> C（下行）
				group.addTask { [weak self] in
					guard let s = self else { return }
                    try await s.pipeOneDirection(
						from: remote,
						to: client,
						chunkSize: 512 * 1024,
						onBytes: { n in
							s.observeBytesForOneShotRole(isC2U: false, n: n)
							s.bytesDown += n
						},
						onFirstByte: {
							// 首字节 KPI 与看门狗取消
							if s.tFirstByte == nil {
								s.tFirstByte = .now()
								s.firstByteWatchdog?.setEventHandler {}
								s.firstByteWatchdog?.cancel()
								s.firstByteWatchdog = nil

								let ttfbMs = Double(s.tFirstByte!.uptimeNanoseconds &- s.tStart.uptimeNanoseconds) / 1e6
								s.log(String(format: "KPI immediate TTFB_ms=%.1f", ttfbMs))
								if let ts = s.tFirstSend {
									let segMs = Double(s.tFirstByte!.uptimeNanoseconds &- ts.uptimeNanoseconds) / 1e6
									s.log(String(format: "KPI firstSend_to_firstRecv_ms=%.1f", segMs))
								}
							}
						}
					)
				}

				// 任一方向抛错会使 group 抛错；否则等双向自然结束
				try await group.waitForAll()
			}
		} catch {
			log("bridge error: \(error)")
		}

		// 双向完成或出错后统一收尾
		client.cancel()
		remote.cancel()
		cancel(reason: "bridge completed")
	}
    
    private func pipeOneDirection(
        from source: NWConnection,
        to destination: NWConnection,
        chunkSize: Int,
        onBytes: ((Int) -> Void)? = nil,
        onFirstByte: (() -> Void)? = nil
    ) async throws {
        var seenFirst = false
        for try await data in source.receiveStream(maxLength: chunkSize) {
            if !seenFirst {
                seenFirst = true
                onFirstByte?()
            }
            onBytes?(data.count)
            try await destination.sendAsync(data)     // 等待发送完成 → 不再预读 → 背压生效
        }
        // 上游 EOF -> 下游半关闭写端（FIN）
        try? await destination.sendAsync(nil, final: true)
    }

}

extension NWConnection {
    
    func sendAsync(_ data: Data?, final: Bool = false) async throws {
        try await withCheckedThrowingContinuation { continuation in
            if let data = data {
                self.send(content: data, completion: .contentProcessed { error in
                    if let error = error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                })
            } else if final {
                // 发送 EOF
                self.send(content: nil, completion: .contentProcessed { error in
                    if let error = error {
                        continuation.resume(throwing: error)
                    } else {
                        continuation.resume()
                    }
                })
            } else {
                continuation.resume()
            }
        }
    }
}


// MARK: - Pull-driven AsyncSequence：每次 next() 才真正调用一次 receive（无预取）
struct NWReceiveSequence: AsyncSequence {
    typealias Element = Data
    struct Iterator: AsyncIteratorProtocol {
        let conn: NWConnection
        let max: Int
        mutating func next() async throws -> Data? {
            guard let d = try await conn.recv(max: max) else { return nil }  // EOF
            return d.isEmpty ? try await next() : d  // 跳过空块
        }
    }
    let conn: NWConnection
    let max: Int
    func makeAsyncIterator() -> Iterator { Iterator(conn: conn, max: max) }
}

extension NWConnection {
    func receiveStream(maxLength: Int) -> NWReceiveSequence {
        NWReceiveSequence(conn: self, max: maxLength)
    }
	    /// 返回 Data；返回 nil 表示 EOF
    func recv(max: Int) async throws -> Data? {
        try await withCheckedThrowingContinuation { cont in
            self.receive(minimumIncompleteLength: 1, maximumLength: max) { data, _, isComplete, error in
                if let error = error {
                    cont.resume(throwing: error)
                } else if isComplete {
                    cont.resume(returning: nil)        // EOF
                } else {
                    cont.resume(returning: data ?? Data())
                }
            }
        }
    }
}
