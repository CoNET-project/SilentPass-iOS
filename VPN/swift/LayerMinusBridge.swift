import Foundation
import Network
import os

enum L {
    static func lm(_ s: @autoclosure () -> String) { print("[LayerMinusBridge] \(s())") }
    static func sc(_ s: @autoclosure () -> String) { print("[ServerConnection] \(s())") }
}

public final class LayerMinusBridge {
    
    private var sendSeq: UInt64 = 0
    private var inflight = Set<UInt64>()
    
    // --- 上行(c->u)微批：64KB 或 5ms 触发 ---
    private var cuBuffer = Data()
    private var cuFlushTimer: DispatchSourceTimer?
    private let CU_FLUSH_BYTES = 64 * 1024
    private let CU_FLUSH_MS = 12
    // 当定时到点但缓冲小于该值时，允许再延一次以攒到更“胖”的报文
    private let CU_MIN_FLUSH_BYTES = 4 * 1024
    private let CU_EXTRA_MS = 6
    
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
    private let drainGrace: TimeInterval = 1.0  // 半关闭后等待对端残留数据的守护时间（秒）
    
    
    private let onClosed: ((UInt64) -> Void)?
    
    private let queue: DispatchQueue
    private var upstream: NWConnection?
    private var closed = false
    
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
            print("⚠️ WARNING: LayerMinusBridge #\(id) destroyed without proper closing!")
        }
    }
    
    @inline(__always)
    private func log(_ msg: String) {
        NSLog("[LayerMinusBridge \(id), \(infoTag())] %@", msg)
    }
    
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
                self.cancel(reason: "Invalid Base64")
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
    
    
    
    
    public func cancel(reason: String) {
        guard !closed else { return }
        closed = true
        
        // 停止所有守护/缓冲
        
        // 统一在真正取消前清理所有定时器，避免回调晚到再次触发
        firstByteWatchdog?.cancel(); firstByteWatchdog = nil
        drainTimer?.cancel(); drainTimer = nil
        
        
        cuFlushTimer?.cancel(); cuFlushTimer = nil
        drainTimer?.cancel(); drainTimer = nil
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
            let preferNoDelay = (targetPort == 443 || targetPort == 80 || targetPort == 8080)
            tcp.noDelay = preferNoDelay
            tcp.enableKeepalive = true
            tcp.keepaliveIdle = 30
        }
        
        log("Connecting to upstream \(targetHost):\(targetPort)")
        
        let up = NWConnection(host: host, port: port, using: params)
        upstream = up
        
        up.stateUpdateHandler = { [weak self] st in
            guard let self = self else { return }
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
                self.pumpClientToUpstream()
                self.pumpUpstreamToClient()
                
            case .waiting(let error):
                self.log("upstream waiting: \(error)")
                
            case .failed(let error):
                self.log("upstream failed: \(error)")
                self.cancel(reason: "upstream failed")
                
            case .cancelled:
                self.log("upstream cancelled")
                self.cancel(reason: "upstream cancelled")
                
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
    
    private func scheduleCUFlush(allowExtend: Bool = true) {
        cuFlushTimer?.cancel()
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .milliseconds(CU_FLUSH_MS))
        t.setEventHandler { [weak self] in
            guard let s = self else { return }
            // 若到点仍然很小且允许再延一次，则小幅延时后再冲刷
            if !s.closed, allowExtend, s.cuBuffer.count > 0, s.cuBuffer.count < s.CU_MIN_FLUSH_BYTES {
                s.cuFlushTimer?.cancel()
                let t2 = DispatchSource.makeTimerSource(queue: s.queue)
                t2.schedule(deadline: .now() + .milliseconds(s.CU_EXTRA_MS))
                t2.setEventHandler { [weak self] in self?.flushCUBuffer() }
                s.cuFlushTimer = t2
                t2.resume()
            } else {
                s.flushCUBuffer()
            }
        }
        cuFlushTimer = t
        t.resume()
    }

    private func flushCUBuffer() {
        guard !cuBuffer.isEmpty, !closed else { return }
        let payload = cuBuffer
        cuBuffer.removeAll(keepingCapacity: true)
        self.sendToUpstream(payload, remark: "c->u")
    }
    
    private func pumpClientToUpstream() {
        if closed { return }
        
        client.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in
            guard let self = self else { return }
            if self.closed { return }
            
            if let err = err {
                self.log("client recv err: \(err)")
                
                // 避免在 receive 回调栈内同步取消，引发重入/竞态
                self.queue.async { self.cancel(reason: "client recv err") }
                
                
                
                
                return
            }
            
            if let d = data, !d.isEmpty {
                self.vlog("recv from client: \(d.count)B")
                // 上行微批：累积到缓冲，达到阈值立即冲刷，否则启动一个很短的定时器
                self.cuBuffer.append(d)
                
                
                if self.cuBuffer.count >= self.CU_FLUSH_BYTES {
                    self.flushCUBuffer()
                } else {
                    self.scheduleCUFlush()
                }
            }
            
            
            if isComplete {
                self.log("client EOF")
                // 先把缓冲冲刷出去，再半关闭上游写入，进入排水期
                self.flushCUBuffer()
                self.upstream?.send(content: nil, completion: .contentProcessed({ _ in }))
                self.scheduleDrainCancel(hint: "client EOF")
                return
            }
            
            // 继续接收
            self.pumpClientToUpstream()
        }
    }
    
    private func scheduleDrainCancel(hint: String) {
        // 如果对向也尽快 EOF，会先走到另一个分支触发；否则在 grace 到时统一取消
        drainTimer?.cancel()
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + drainGrace)
        timer.setEventHandler { [weak self] in
            self?.cancel(reason: "drain timeout after \(hint)")
        }
        drainTimer = timer
        timer.resume()
    }
    
    private func pumpUpstreamToClient() {
        if closed { return }
        
        upstream?.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in
            guard let self = self else { return }
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
                    
                    
                    self.firstByteWatchdog?.cancel(); self.firstByteWatchdog = nil
                    // KPI: 即时打印首字节到达延迟 (TTFB)
                    let ttfbMs = Double(self.tFirstByte!.uptimeNanoseconds &- self.tStart.uptimeNanoseconds) / 1e6
                    self.log(String(format: "KPI immediate TTFB_ms=%.1f", ttfbMs))
                    // KPI: 首包发送完成 -> 首字节回流（纯传输/排队段）
                    if let ts = self.tFirstSend {
                        let segMs = Double(self.tFirstByte!.uptimeNanoseconds &- ts.uptimeNanoseconds) / 1e6
                        self.log(String(format: "KPI firstSend_to_firstRecv_ms=%.1f", segMs))
                    }
                }
                

                self.bytesDown &+= d.count
                self.sendToClient(d, remark: "u->c")
            }
            
            if isComplete {
                self.log("upstream EOF")
                // 半关闭客户端写入，进入排水期
                self.client.send(content: nil, completion: .contentProcessed({ _ in }))
                self.scheduleDrainCancel(hint: "upstream EOF")
                return
            }
            
            // 继续接收
            self.pumpUpstreamToClient()
        }
    }
    
    // 首包回包看门狗（避免黑洞 60–100s 挂死；测速上传场景按策略放宽/禁用）
    private var firstByteWatchdog: DispatchSourceTimer?
    
    private func sendToUpstream(_ data: Data, remark: String) {
        guard let up = upstream, !closed else {
            log("Cannot send to upstream: upstream=\(upstream != nil), closed=\(closed)")
            return
        }
        
        let seq = { sendSeq &+= 1; return sendSeq }()
        
        inflight.insert(seq)
        
        vlog("send \(remark) \(data.count)B -> upstream #\(seq)")
        
        up.send(content: data, completion: .contentProcessed({ [weak self] err in
            guard let self = self else { return }
            // 去重：只处理一次完成回调
            guard self.inflight.remove(seq) != nil else {
                self.log("WARN dup completion for #\(seq), ignore")
                return
            }
            if let err = err {
                self.log("upstream send err: \(err)")
                self.queue.async { self.cancel(reason: "upstream send err") }
                return
            }
            // 发送成功日志 + 计数
            if remark == "firstBody", self.tFirstSend == nil {
                self.tFirstSend = .now()
                self.log("sent firstBody successfully (mark tFirstSend)")
            } else {
                self.vlog("sent \(remark) successfully")
            }
            self.bytesUp &+= data.count
            // 仅在首包时设置 TTFB 看门狗
            if remark == "firstBody" {
                let isSpeedtestHost =
                    self.targetHost.hasSuffix("ooklaserver.net") ||
                    self.targetHost.hasSuffix("speedtest.net") ||
                    self.targetHost.contains("measurementlab")
                let disableWatchdog = (self.targetPort == 8080) && isSpeedtestHost
                let watchdogDelay: TimeInterval = disableWatchdog ? 15.0 : 2.5
                
                let wd = DispatchSource.makeTimerSource(queue: self.queue)
                wd.schedule(deadline: .now() + watchdogDelay)
                wd.setEventHandler { [weak self] in
                    guard let s = self, !s.closed else { return }
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
        guard !closed else {
            log("Cannot send to client: closed=\(closed)")
            return
        }
        
        log("send \(remark) \(data.count)B -> client")
        
        client.send(content: data, completion: .contentProcessed({ [weak self] err in
            
            if let err = err {
                self?.log("client send err: \(err)")
                self?.queue.async { self?.cancel(reason: "client send err") }
            } else {
                self?.vlog("sent \(remark) successfully")
            }
        }))
    }
    
    private func diffMs(start: DispatchTime, end: DispatchTime) -> Double {
        return Double(end.uptimeNanoseconds &- start.uptimeNanoseconds) / 1e6
    }
}
