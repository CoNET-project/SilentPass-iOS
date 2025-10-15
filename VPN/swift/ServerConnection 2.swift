import Foundation
import Network
import os
import Darwin
import os.log



//		ServerConnection LayerMinusBridge
public final class ServerConnection {

	// Header、Body 都必须小于 32KB（严格小于 ⇒ 31KB）
	private let HTTP_HDR_MAX  = 31 * 1024
	private let HTTP_BODY_MAX = 31 * 1024

	private func blockHTTPAndClose(statusLine: String, reason: String) {
		let body = "Blocked: \(reason)\n"
		let header =
			"HTTP/1.1 \(statusLine)\r\n" +
			"Connection: close\r\n" +
			"Content-Type: text/plain; charset=utf-8\r\n" +
			"Content-Length: \(body.utf8.count)\r\n\r\n"
		let payload = Data((header + body).utf8)
		client.send(content: payload, completion: .contentProcessed { _ in
			self.recvBuffer.removeAll(keepingCapacity: false)
			self.close(reason: "http_block: \(reason)")
		})
	}
    
    // 客户端活性/可写状态（由 stateUpdateHandler 维护）
    private var clientIsReady: Bool = false

    // MARK: - Static→Instance 日志桥
    private static weak var _logTarget: ServerConnection?
	private static var _memTick: Int = 0   // 用于周期性心跳打印
    @inline(__always)
    private static func _log(_ msg: String) {
        if let t = _logTarget {
            t.log(msg)                 // 使用实例的 log(_:)
        } else {
            #if DEBUG
            print(msg)                 // 兜底：尚未注册时不丢日志
            #else
            NSLog("%@", msg)
            #endif
        }
    }

	// MARK: - Global Memory Monitor (500ms)
	private struct MemoryState {
		var isUnderPressure = false
		var lastCheckTime = DispatchTime.now()
		var consecutivePressureCount = 0
	}
    private static var _memTimer: DispatchSourceTimer?
	private static var _memState = MemoryState()

	private static let _memQueue = DispatchQueue(label: "ServerConnection.mem.monitor")
	// 可按机型调整软阈值（MB）
	private static let _MEM_SOFT_LIMIT_MB: Int = 48
	private static let _MEM_CHECK_INTERVAL: DispatchTimeInterval = .milliseconds(500)

	/// 全局内存压力只读开关（其他模块可读取）
	public static var isUnderMemoryPressure: Bool { _memState.isUnderPressure }

    /// 改为：仅在被调用时**采样并打印一次**（无定时器）
    private static func startGlobalMemoryMonitorIfNeeded(event: String, logger: (String) -> Void) {
        _memState.lastCheckTime = .now()
        guard let rss = currentRSSMB() else {
            logger("⚠️ [MEM] \(event)  rss=unavailable  limit=\(_MEM_SOFT_LIMIT_MB)MB")
            return
        }
        let was = _memState.isUnderPressure
        let isPressure = (rss >= _MEM_SOFT_LIMIT_MB)
        _memState.isUnderPressure = isPressure
        _memState.consecutivePressureCount = isPressure ? (_memState.consecutivePressureCount + 1) : 0
        logger("⚠️ [MEM] \(event)  pressure=\(isPressure ? "ON" : "OFF")  rss=\(rss)MB  limit=\(_MEM_SOFT_LIMIT_MB)MB  consec=\(_memState.consecutivePressureCount)")
        if was != isPressure {
            logger("⚠️ [MEM] pressure state changed: \(was ? "ON→OFF" : "OFF→ON")")
        }
    }


	/// 获取当前进程物理占用（MB），优先使用 task_vm_info.phys_footprint
	public static func currentRSSMB() -> Int? {
		#if canImport(Darwin)
		var info = task_vm_info_data_t()
		var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size) / 4
		let kr: kern_return_t = withUnsafeMutablePointer(to: &info) {
			$0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
				task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count)
			}
		}
		guard kr == KERN_SUCCESS else { return nil }
		let footprint = info.phys_footprint // bytes
		return Int(footprint) / (1024 * 1024)
		#else
		return nil
		#endif
	}

	// 命中黑名单 → 立即废止（HTTP 返回 403；SOCKS5 返回 0x02），统一在 ServerConnection 的 queue 上执行
	@inline(__always)
    private func shouldBlock(host: String) -> Bool {
        return AdBlacklist.matches(host)
    }
    private func blockHTTPForbiddenAndClose(_ reason: String) {
        let resp = "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        client.send(content: resp.data(using: .utf8), completion: .contentProcessed({ [weak self] _ in
            self?.close(reason: "blocked by blacklist (\(reason))")
        }))
    }
    
    private func blockSocksAndClose(_ reason: String) {
        // 0x02: connection not allowed by ruleset
        let reply = Data([0x05, 0x02, 0x00, 0x01, 0,0,0,0, 0,0])
        client.send(content: reply, completion: .contentProcessed({ [weak self] _ in
            self?.close(reason: "blocked by blacklist (\(reason))")
        }))
    }

	@inline(__always)
	private func resolveFirstIPv4(_ host: String) -> String? {
		// 仅解析 IPv4，避免 IPv6 干扰 CIDR
		var hints = addrinfo(
			ai_flags: AI_ADDRCONFIG, ai_family: AF_INET,
			ai_socktype: SOCK_STREAM, ai_protocol: IPPROTO_TCP,
			ai_addrlen: 0, ai_canonname: nil, ai_addr: nil, ai_next: nil
		)
		var res: UnsafeMutablePointer<addrinfo>?
		let rc = getaddrinfo(host, nil, &hints, &res)
		guard rc == 0, let first = res else { return nil }
		defer { freeaddrinfo(res) }

		var addr = first.pointee.ai_addr.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { $0.pointee }
		var buf = [CChar](repeating: 0, count: Int(INET_ADDRSTRLEN))
		inet_ntop(AF_INET, &addr.sin_addr, &buf, socklen_t(INET_ADDRSTRLEN))
		return String(cString: buf)
	}

    // 命中白名单 → 直连（由 ServerConnection 决策，不走 LM 打包）
    @inline(__always)
    private func shouldDirect(host: String) -> Bool {
        // 1) 先按域名规则（保持现有语义）
		if Allowlist.matches(host) { return true }
		
		return false
    }

    public let id: UInt64
    public let client: NWConnection
    private let onClosed: ((UInt64) -> Void)?
    var httpConnect = true

    private let logger: Logger
    private let queue: DispatchQueue
    private let verbose: Bool

    private var recvBuffer = Data()
    private enum Phase {
        case methodSelect
        case requestHead
        case requestAddr(ver: UInt8, cmd: UInt8, atyp: UInt8)
        case connected(host: String, port: Int)
        case bridged
        case closed
    }
    
    private let RECV_BUFFER_SOFT_LIMIT = 32 * 1024  // 32KB：降低首部稍大的场景的误伤
    
    
    /// 该连接是否已切到 LayerMinus 通道（由业务分支显式标记）
    public private(set) var isLayerMinusRouted: Bool = false

    /// 当确定此连接将经由 LayerMinusBridge 转发时调用
    public func markAsLayerMinusRouted() {
        self.isLayerMinusRouted = true
    }

	// MARK: IP 字面量检测（IPv4 / IPv6）
    @inline(__always)
    private func isIPAddress(_ s: String) -> Bool {
        // 允许形如 "[2001:db8::1]" 的 Host，先去掉方括号
        let t = s.trimmingCharacters(in: CharacterSet(charactersIn: "[]"))
        var v4 = in_addr()
        if t.withCString({ inet_pton(AF_INET, $0, &v4) }) == 1 { return true }
        var v6 = in6_addr()
        if t.withCString({ inet_pton(AF_INET6, $0, &v6) }) == 1 { return true }
        return false
    }
    
    public var onRoutingDecided: ((ServerConnection) -> Void)?
    
    private var phase: Phase = .methodSelect
    private var closed = false
    private var handedOff = false
    private var bridge: LayerMinusBridge?
    private var layerMinus: LayerMinus

    // 路由决策：是否使用 LayerMinus 打包（默认 true）
    private var useLayerMinus: Bool = true

    init(
        id: UInt64,
        connection: NWConnection,
        logger: Logger = Logger(subsystem: "VPN", category: "SOCKS5"),
        verbose: Bool = true,
        layerMinus: LayerMinus,
        onClosed: ((UInt64) -> Void)? = nil
    ) {
        self.id = id
        self.client = connection
        self.logger = logger
        self.verbose = verbose
        self.onClosed = onClosed
        self.queue = DispatchQueue(label: "ServerConnection.\(id)", qos: .userInitiated)
        self.layerMinus = layerMinus
        // 简单的生命周期日志
        log("🟢 CREATED ServerConnection #\(id)")
    }

    #if DEBUG
    private let vpnLog = OSLog(subsystem: "com.silentpass.vpn", category: "ServerConnection")
    @inline(__always)
    private func log(_ msg: @autoclosure () -> String, type: OSLogType = .info) {
        os_log("%{public}@", log: vpnLog, type: type, msg())
    }
    #else
    @inline(__always)
    private func log(_ msg: @autoclosure () -> String, type: OSLogType = .info) { }
    #endif

    public func start() {
        // 始终以“最新实例”作为日志目标（弱引用，实例释放后自动失效）
        Self._logTarget = self
        client.stateUpdateHandler = { [weak self] state in
            guard let self = self else { return }
            switch state {
            case .ready:
                self.clientIsReady = true
                self.log("client ready; enter recv loop")
                self.recvLoop()
            case .failed(let e):
                self.clientIsReady = false
                self.log("client failed: \(e)")
                self.close(reason: "client failed")
            case .cancelled:
                self.clientIsReady = false
                self.log("client cancelled")
                self.close(reason: "client cancelled")
            default:
                break
            }
        }
        client.start(queue: queue)
        log("will start")
    }
    
    // MARK: - Stop origin (logging-only)
    private static var _stopOriginTag: String = "unknown"
    private static var _stopNote: String = ""

    public func close(reason: String) {
        guard !closed else { return }
        closed = true
        phase = .closed
		
        if reason.lowercased().contains("server stop") {
			log("close: \(reason) | origin=\(Self._stopOriginTag) note=\(Self._stopNote)")
		} else {
			log("close: \(reason)")
		}
        
        // 取消客户端连接
        client.cancel()
        
        // 如果有 bridge，也要关闭它
        if let b = bridge {
            Task { await b.cancel(reason: "ServerConnection closed: \(reason)") }
        }
        bridge = nil
        
        // 通知 Server 移除此连接
        onClosed?(id)
    }
    
    // 外部调用的关闭方法
    func shutdown(reason: String) {
        close(reason: reason)
    }
    
    deinit {
        log("🔴 DESTROYED ServerConnection #\(id)")
        if !closed {
            log("⚠️ WARNING: ServerConnection #\(id) destroyed without proper closing!")
        }
    }

    private func recvLoop() {
        if handedOff || closed { return }

        client.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in
            guard let self = self else { return }
            if self.handedOff || self.closed { return }

            if let err = err {
                self.log("recv err: \(err)")
                self.close(reason: "recv err")
                return
            }
            
            if let chunk = data, !chunk.isEmpty {
                //self.log("recv \(chunk.count)B, buffer before: \(self.recvBuffer.count)B, phase: \(self.phase)")
                self.recvBuffer.append(chunk)
                
                if self.recvBuffer.count > RECV_BUFFER_SOFT_LIMIT {
                    // 选择：要么丢弃老数据、要么直接 413 关闭，这里先保守地直接收尾，避免 OOM
                    self.log("recvBuffer exceeded soft limit (\(self.recvBuffer.count)B) -> close to protect memory")

                    // 仅保留最新 1MB，避免 OOM 同时不中断连接
                    let KEEP = 64 * 1024
                    if self.recvBuffer.count > KEEP {
                        self.recvBuffer = self.recvBuffer.suffix(KEEP)
                    }

                    // self.close(reason: "recvBuffer overflow")
                    // return
                }
                
                
                //self.log("buffer after append: \(self.recvBuffer.count)B")
                
                // 打印接收到的数据的前几个字节（用于调试）
                if chunk.count > 0 && self.verbose {
                    let preview = chunk.prefix(16).map { String(format: "%02x", $0) }.joined(separator: " ")
                    self.log("recv data preview: \(preview)")
                }
                
                self.parseBuffer()
            }
            
            if isComplete {
                self.log("client EOF")
                self.close(reason: "client EOF")
                return
            }

            if self.handedOff || self.closed { return }
            
            self.recvLoop()
        }
    }

	private func parseContentLength(_ header: String) -> Int? {
		for line in header.split(separator: "\r\n", omittingEmptySubsequences: false) {
			if line.lowercased().hasPrefix("content-length:") {
				let v = line.drop { $0 != ":" }.dropFirst().trimmingCharacters(in: .whitespaces)
				return Int(v)
			}
		}
		return nil
	}
    
    private var socksVer: Int? = nil  // 4 or 5

    private func parseBuffer() {
        // 安全检查：确保 buffer 不为空
        guard !recvBuffer.isEmpty else {
            log("parseBuffer called with empty buffer")
            return
        }
        
        //log("parseBuffer: phase=\(phase), buffer size=\(recvBuffer.count)")
        
        var advanced = true
        while advanced, !closed, !handedOff {
            advanced = false
            
            // 记录当前处理的阶段
            let bufferSizeBefore = recvBuffer.count
            
            switch phase {
            case .methodSelect:
                if let first = recvBuffer.first {
                    if first == 0x05 {
                        // SOCKS5
                        socksVer = 5
                        httpConnect = false
                        advanced = parseMethodSelect()
                    } else if first == 0x04 {
                        // SOCKS4/4a
                        socksVer = 4
                        httpConnect = false
                        advanced = parseSocks4Request()
                    } else {
                        // 尝试当作 HTTP 代理
                        advanced = tryParseHTTPProxyRequest()
                        if !advanced { log("methodSelect: waiting (HTTP/SOCKS?)") }
                    }
                }
            case .requestHead:
                advanced = parseRequestHead()
                if advanced {
                    log("parseBuffer: requestHead consumed \(bufferSizeBefore - recvBuffer.count) bytes")
                }
            case .requestAddr(let ver, let cmd, let atyp):
                advanced = parseRequestAddr(ver: ver, cmd: cmd, atyp: atyp)
                if advanced {
                    log("parseBuffer: requestAddr consumed \(bufferSizeBefore - recvBuffer.count) bytes")
                }
            case .connected(let host, let port):
                if !recvBuffer.isEmpty {
                    
                    
                    
                    let first = recvBuffer
                    recvBuffer.removeAll(keepingCapacity: false)
                    processFirstBody(host: host, port: port, firstBody: first)
                    advanced = true
                }
            case .bridged, .closed:
                log("parseBuffer: already bridged or closed, returning")
                return
            }
        }
        
        log("parseBuffer: done, remaining buffer=\(recvBuffer.count) bytes")
    }
    
    private func parseSocks4Request() -> Bool {
        // 格式: VN(0x04) CD(0x01=CONNECT) DSTPORT(2) DSTIP(4) USERID(zero-terminated) [DOMAIN(zero-terminated) if 4a]
        // 先检查最小头 8 字节
        guard recvBuffer.count >= 8 else { return false }
        // 不破坏缓冲，先窥视
        let head = Array(recvBuffer.prefix(8))
        let vn = head[0], cd = head[1]
        guard vn == 0x04 else { return false }
        guard cd == 0x01 else {
            // 仅支持 CONNECT
            // 返回 0x5b（拒绝）
            _ = sendSocks4Reply(granted: false, host: "0.0.0.0", port: 0)
            close(reason: "SOCKS4 unsupported cmd \(cd)")
            return true
        }
        let dstPort = (Int(head[2]) << 8) | Int(head[3])
        let ipBytes = [head[4], head[5], head[6], head[7]]
        let isSocks4a = (ipBytes[0] == 0 && ipBytes[1] == 0 && ipBytes[2] == 0 && ipBytes[3] != 0)

        // 找 USERID 结尾的 \0
        // 起始位置从第 8 字节开始
        guard let uidEnd = recvBuffer[8...].firstIndex(of: 0x00) else { return false } // 等更多数据
        let afterUID = recvBuffer.index(after: uidEnd)

        var host = ""
        if isSocks4a {
            // 需要再找一个 \0 作为域名结尾
            guard let domainEnd = recvBuffer[afterUID...].firstIndex(of: 0x00) else { return false }
            let domainData = recvBuffer[afterUID..<domainEnd]
            host = String(data: domainData, encoding: .utf8) ?? ""
            // 消费：头(8) + userid + \0 + domain + \0
            recvBuffer.removeSubrange(recvBuffer.startIndex..<recvBuffer.index(after: domainEnd))
        } else {
            // 直接用 IPv4 字面量
            host = "\(ipBytes[0]).\(ipBytes[1]).\(ipBytes[2]).\(ipBytes[3])"
            // 消费：头(8) + userid + \0
            recvBuffer.removeSubrange(recvBuffer.startIndex..<afterUID)
        }

        // --- 白名单：直连（与 SOCKS5 逻辑保持一致） ---
        if shouldDirect(host: host) {
            useLayerMinus = false
            log("SOCKS4 CONNECT \(host):\(dstPort) matched allowlist -> DIRECT")
        } else {
            useLayerMinus = true
        }

        // --- 黑名单：直接拒绝 ---
        if shouldBlock(host: host) {
            log("SOCKS4 CONNECT \(host):\(dstPort) blocked by blacklist")
            _ = sendSocks4Reply(granted: false, host: "0.0.0.0", port: 0)
            close(reason: "blocked by blacklist (SOCKS4 \(host))")
            return true
        }

        // 发送 0x5a 同意，并进入 .connected
        return didGetTargetSocks4(host: host, port: dstPort)
    }

    @discardableResult
    private func sendSocks4Reply(granted: Bool, host: String, port: Int) -> Bool {
        // 规范：VN=0x00, REP=0x5a(成功)/0x5b(失败)，然后回填 BINDPORT/BINDIP（这里用 0）
        let rep: UInt8 = granted ? 0x5a : 0x5b
        let pHi = UInt8((port >> 8) & 0xff), pLo = UInt8(port & 0xff)
        let ipBytes: [UInt8]
        if let ipv4 = IPv4Address(host) {
            ipBytes = Array(ipv4.rawValue)
        } else {
            ipBytes = [0,0,0,0]
        }
        var reply = Data()
        reply.append(contentsOf: [0x00, rep, pHi, pLo])
        reply.append(contentsOf: ipBytes)
        client.send(content: reply, completion: .contentProcessed { [weak self] err in
            if let err = err { self?.log("send SOCKS4 reply err: \(err)") }
        })
        return granted
    }

    private func didGetTargetSocks4(host: String, port: Int) -> Bool {
        log("SOCKS4 CONNECT \(host):\(port) -> reply OK, then wait first-body")
        _ = sendSocks4Reply(granted: true, host: "0.0.0.0", port: 0)
        self.httpConnect = false
        phase = .connected(host: host, port: port)
        // 若缓冲里已经有首包，立刻处理
        parseBuffer()
        return true
    }
    
    // MARK: HTTP/HTTPS Proxy 解析与改写（绝对URI → origin-form）
    private func tryParseHTTPProxyRequest() -> Bool {
        // 我们至少需要一行（\r\n）来判断方法，且处理非 CONNECT 时需要首部结束（\r\n\r\n）
        let CRLF = Data([0x0d, 0x0a])
        let CRLFCRLF = Data([0x0d, 0x0a, 0x0d, 0x0a])

		if recvBuffer.count > HTTP_HDR_MAX, recvBuffer.range(of: CRLFCRLF) == nil {
			log("❌ HTTP header too large or malformed: \(recvBuffer.count) bytes, no CRLFCRLF")
			blockHTTPAndClose(statusLine: "431 Request Header Fields Too Large", reason: "Header > 31KB or malformed")
			return true
		}

		// —— 2) 如果已拿到完整 Header，进一步做 Header 长度与 Body 长度限制
		if let sep = recvBuffer.range(of: CRLFCRLF) {
			let headerLen = recvBuffer.distance(from: recvBuffer.startIndex, to: sep.lowerBound)
			if headerLen > HTTP_HDR_MAX {
				log("❌ HTTP header too large: \(headerLen) > \(HTTP_HDR_MAX)")
				blockHTTPAndClose(statusLine: "431 Request Header Fields Too Large", reason: "Header > 31KB")
				return true
			}

			// CONNECT 一般无 Body；非 CONNECT 则检查 Body 上限
			let methodIsCONNECT: Bool = {
				// 只读前几个字节判断是否以 "CONNECT " 开头，避免大规模复制
				let prefixLen = min(7, headerLen)
				let prefixData = recvBuffer[recvBuffer.startIndex..<recvBuffer.index(recvBuffer.startIndex, offsetBy: prefixLen)]
				return String(data: prefixData, encoding: .utf8)?.uppercased().hasPrefix("CONNECT") == true
			}()

			if !methodIsCONNECT {
				// 已经缓冲到的 Body 长度（可能为 0）
				let bodyStart = sep.upperBound
				let bufferedBodyLen = recvBuffer.distance(from: bodyStart, to: recvBuffer.endIndex)
				if bufferedBodyLen > HTTP_BODY_MAX {
					log("❌ HTTP body buffered too large: \(bufferedBodyLen) > \(HTTP_BODY_MAX)")
					blockHTTPAndClose(statusLine: "413 Payload Too Large", reason: "Body > 31KB (buffered)")
					return true
				}

				// 解析 Content-Length；若声明超限也直接拒绝
				if let headerStr = String(data: recvBuffer[recvBuffer.startIndex..<sep.lowerBound], encoding: .utf8) {
					if headerStr.range(of: "transfer-encoding:", options: .caseInsensitive) != nil,
					headerStr.range(of: "chunked", options: .caseInsensitive) != nil {
						// 为了满足“Body < 32KB”的硬约束，这里直接拒绝 chunked
						log("❌ chunked body not allowed")
						blockHTTPAndClose(statusLine: "413 Payload Too Large", reason: "Chunked not allowed (>31KB)")
						return true
					}
					if let cl = parseContentLength(headerStr), cl > HTTP_BODY_MAX {
						log("❌ Content-Length too large: \(cl) > \(HTTP_BODY_MAX)")
						blockHTTPAndClose(statusLine: "413 Payload Too Large", reason: "Content-Length > 31KB")
						return true
					}
				}
			}
		}

        guard let firstLineEnd = recvBuffer.range(of: CRLF) else { return false }

        
        let firstLineData = recvBuffer.subdata(in: recvBuffer.startIndex..<firstLineEnd.lowerBound)
        guard let firstLine = String(data: firstLineData, encoding: .utf8) else { return false }

        
        // 支持的方法（大小写不敏感）：CONNECT / GET / POST / PUT / DELETE / HEAD / OPTIONS / PATCH / TRACE
        let upper = firstLine.uppercased()
        let httpMethods = ["CONNECT", "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH", "TRACE"]
        guard httpMethods.first(where: { upper.hasPrefix($0 + " ") }) != nil else { return false }

        
        // CONNECT 单独处理（只需第一行 + 可选首部）
        if upper.hasPrefix("CONNECT ") {
            // CONNECT host:port HTTP/x.y
            let parts = firstLine.split(separator: " ")
            guard parts.count >= 2 else { return false }
            let hostPort = String(parts[1])
            let hp = splitHostPort(hostPort, defaultPort: 443)
            // 等待到首部结束后再消费（更稳妥）
            guard let headerEnd = recvBuffer.range(of: CRLFCRLF) else { return false }

            // --- 白名单：直连，不走 LayerMinus ---
            if shouldDirect(host: hp.host) {
                useLayerMinus = false
                log("HTTP CONNECT \(hp.host):\(hp.port) matched allowlist -> DIRECT")
            } else {
                useLayerMinus = true
            }

            // --- 黑名单：直接 403 并关闭 ---
            if shouldBlock(host: hp.host) {
                // 丢弃首部以免后续误处理
                recvBuffer.removeSubrange(recvBuffer.startIndex..<headerEnd.upperBound)
                log("HTTP CONNECT \(hp.host):\(hp.port) blocked by blacklist")
                blockHTTPForbiddenAndClose("HTTP CONNECT \(hp.host)")
                return true
            }
            
            // 丢弃 CONNECT 请求首部
            recvBuffer.removeSubrange(recvBuffer.startIndex..<headerEnd.upperBound)

            
            // 发送 200 Established
            let established = "HTTP/1.1 200 Connection Established\r\nProxy-Agent: vpn2socks\r\n\r\n"
            
            
            client.send(content: established.data(using: .utf8), completion: .contentProcessed({ [weak self] err in
                if let err = err { self?.log("send CONNECT 200 err: \(err)") }
            }))

            
            // 进入 connected，等待 TLS 首包进入再统一走 processFirstBody → LayerMinusBridge
            self.phase = .connected(host: hp.host, port: hp.port)
            return true

        }

	
		// 其它明文 HTTP：需至少拿到完整首部（避免误改正文）
		guard let headerEnd = recvBuffer.range(of: CRLFCRLF) else { return false }

	
		// 解析第一行：METHOD SP PATH SP HTTP/x.y
		let lineParts = firstLine.split(separator: " ", maxSplits: 2)
		guard lineParts.count == 3 else { return false }
		let method = String(lineParts[0])
		let rawPath = String(lineParts[1]) // 可能是绝对URI
		var version = String(lineParts[2]) // HTTP/1.1
		if version.hasPrefix("HTTP/") { version.removeFirst(5) }

		// 解析 Host 首部（用于 origin-form 与默认端口判断）
		let headerData = recvBuffer.subdata(in: firstLineEnd.upperBound..<headerEnd.lowerBound)
		guard let headerText = String(data: headerData, encoding: .utf8) else { return false }
		var hostHeader = ""
		for line in headerText.split(separator: "\r\n") {
			let t = line.trimmingCharacters(in: .whitespaces)
			if t.lowercased().hasPrefix("host:") {
				hostHeader = t.dropFirst("host:".count).trimmingCharacters(in: .whitespaces)
				break
			}
		}

	
		// 目标主机/端口与改写后的 PATH
		let (targetHost, targetPort, originPath) = normalizeAbsoluteOrOriginPath(
			rawPath: rawPath,
			hostHeader: hostHeader
		)

		if method.uppercased() == "GET",
			(targetPort == 8888), (originPath == "/pac") {

			// 回送 PAC
            let body = PACBuilder.buildPAC(proxyHost: targetHost)
			var headers = "HTTP/1.1 200 OK\r\n"
			headers += "Content-Type: application/x-ns-proxy-autoconfig; charset=utf-8\r\n"
			headers += "Cache-Control: no-store, max-age=0\r\n"
			headers += "Content-Length: \(body.count)\r\n"
			headers += "Connection: close\r\n\r\n"

			var resp = Data(headers.utf8)
			resp.append(body)

			// 清空缓冲，直接回发并关连接
			recvBuffer.removeAll(keepingCapacity: false)
			client.send(content: resp, completion: .contentProcessed({ [weak self] _ in
				self?.close(reason: "served PAC")
			}))
			return true
		}

		// --- 白名单：命中则本地直连，不走 LM ---
		if shouldDirect(host: targetHost) {
			useLayerMinus = false
			log("HTTP \(method) \(targetHost):\(targetPort) matched allowlist -> DIRECT")
		} else {
			useLayerMinus = true
		}

		// --- 黑名单：明文 HTTP 直接 403 并关闭 ---
		if shouldBlock(host: targetHost) {
			// 消费缓冲，避免遗留
			recvBuffer.removeAll(keepingCapacity: false)
			log("HTTP \(method) \(targetHost):\(targetPort) blocked by blacklist")
			blockHTTPForbiddenAndClose("HTTP \(method) \(targetHost)")
			return true
		}

		// 重写第一行：METHOD SP originPath SP HTTP/version
		let newFirstLine = "\(method) \(originPath) HTTP/\(version)"
		guard let newFirstLineData = (newFirstLine + "\r\n").data(using: .utf8) else { return false }

		// 将首行替换为改写后的内容，其余首部与（可能存在的）正文原样透传
		// 原数据 = [firstLine + CRLF] + [headers.. + CRLFCRLF] + [body...]
		let restData = recvBuffer.subdata(in: firstLineEnd.upperBound..<recvBuffer.endIndex)
		var rewritten = Data()
		rewritten.append(newFirstLineData)
		rewritten.append(restData)

		// 消费缓冲并移交给 LayerMinusBridge
		recvBuffer.removeAll(keepingCapacity: false)
	
	
		
		handoffToBridge(host: targetHost, port: targetPort, firstBody: rewritten)
		return true
	}

	private func splitHostPort(_ hostPort: String, defaultPort: Int) -> (host: String, port: Int) {
		if let idx = hostPort.lastIndex(of: ":"), idx < hostPort.endIndex {
			let h = String(hostPort[..<idx])
			let pStr = String(hostPort[hostPort.index(after: idx)...])
			if let p = Int(pStr), p > 0 && p < 65536 { return (h, p) }
		}
		return (hostPort, defaultPort)
	}

	/// 将绝对URI（http://h[:p]/x）改写为 origin-form（/x），并返回目标 host/port
	private func normalizeAbsoluteOrOriginPath(rawPath: String, hostHeader: String) -> (String, Int, String) {
		var host = hostHeader
		var port = 80
		var path = rawPath

		
		if rawPath.hasPrefix("http://") || rawPath.hasPrefix("https://") {
			// 绝对URI：解析 scheme://host[:port]/path?query
			let isHTTPS = rawPath.hasPrefix("https://")
			port = isHTTPS ? 443 : 80
			let schemeEnd = rawPath.index(rawPath.startIndex, offsetBy: isHTTPS ? 8 : 7)
			let afterScheme = rawPath[schemeEnd...]            // host[:port]/path...
			if let slash = afterScheme.firstIndex(of: "/") {
				let hp = String(afterScheme[..<slash])
				let tail = String(afterScheme[slash...])      // /path?query
				let sp = splitHostPort(hp, defaultPort: port)
				host = sp.host
				port = sp.port
				path = tail.isEmpty ? "/" : tail
			} else {
				// 没有路径，按根路径处理
				let hp = String(afterScheme)
				let sp = splitHostPort(hp, defaultPort: port)
				host = sp.host
				port = sp.port
				path = "/"
			}
		} else {
			// origin-form：需要从 Host 首部补全目标
			let sp = splitHostPort(hostHeader, defaultPort: 80)
			host = sp.host
			port = sp.port
		}
		if path.isEmpty { path = "/" }
		return (host, port, path)
	}

	private func handoffToBridge(host: String, port: Int, firstBody: Data) {
		if self.httpConnect {
			log("🟢 HTTP/HTTPS proxy #\(id) \(host):\(port) ")
		} else {
			log("🟢 SOCKS v5 proxy #\(id) \(host):\(port) ")
		}
		
		processFirstBody(host: host, port: port, firstBody: firstBody)
	}
    
    

    // MARK: Method Select
    private func parseMethodSelect() -> Bool {
        guard recvBuffer.count >= 2 else { return false }
        
        // 使用安全的方式访问 Data
        let bytes = Array(recvBuffer.prefix(2))
        guard bytes.count == 2 else { return false }
        
        let ver = bytes[0]
        let n = Int(bytes[1])

        guard ver == 0x05 else {
            // 非 SOCKS5：交由 HTTP 解析流程（上层已调用），这里不再关闭连接
            return false
        }
        
        guard recvBuffer.count >= 2 + n else { return false }

        // 提取方法列表用于日志
        var methods: [UInt8] = []
        let methodBytes = Array(recvBuffer.dropFirst(2).prefix(n))
        methods = methodBytes

        recvBuffer.removeFirst(2 + n)
        
        // 先更改状态，再发送响应
        phase = .requestHead
        log("mselect parsed: ver=5 n=\(n) methods=\(methods)")
        
        // 异步发送响应，避免阻塞解析
        let reply = Data([0x05, 0x00]) // NO-AUTH
        client.send(content: reply, completion: .contentProcessed { [weak self] err in
            guard let self = self else { return }
            if let err = err {
                self.log("send mselect err: \(err)")
                self.close(reason: "send mselect err")
                return
            }
            self.log("mselect reply sent (NO-AUTH)")
        })
        
        return true
    }

    // MARK: Request Head
    private func parseRequestHead() -> Bool {
        // 安全检查
        guard recvBuffer.count >= 4 else {
            log("parseRequestHead: need 4 bytes, have \(recvBuffer.count)")
            return false
        }
        
        // 使用 Data 的安全访问方式
        let bytes = Array(recvBuffer.prefix(4))
        guard bytes.count == 4 else {
            log("parseRequestHead: failed to extract 4 bytes")
            return false
        }
        
        let ver = bytes[0]
        let cmd = bytes[1]
        let rsv = bytes[2]
        let atyp = bytes[3]
        
        log("parseRequestHead: ver=\(ver) cmd=\(cmd) rsv=\(rsv) atyp=\(atyp)")
        
        guard ver == 0x05, cmd == 0x01 else {
            sendReply(socksReply: 0x07) // Command not supported
            close(reason: "unsupported cmd/ver (ver=\(ver) cmd=\(cmd))")
            return false
        }
        
        recvBuffer.removeFirst(4)
        phase = .requestAddr(ver: ver, cmd: cmd, atyp: atyp)
        log("req head parsed: ver=5 cmd=CONNECT atyp=\(String(format:"0x%02x", atyp))")
        return true
    }

    // MARK: Request Address
    private func parseRequestAddr(ver: UInt8, cmd: UInt8, atyp: UInt8) -> Bool {
        switch atyp {
        case 0x01: // IPv4: 4 + 2
            guard recvBuffer.count >= 6 else { return false }
            let bytes = Array(recvBuffer.prefix(6))
            guard bytes.count == 6 else { return false }
            
            let host = "\(bytes[0]).\(bytes[1]).\(bytes[2]).\(bytes[3])"
            let port = (Int(bytes[4]) << 8) | Int(bytes[5])
            recvBuffer.removeFirst(6)
            
            // --- 白名单：命中则直连（不走 LayerMinus） ---
            if shouldDirect(host: host) {
                useLayerMinus = false
                log("SOCKS5 CONNECT \(host):\(port) matched allowlist -> DIRECT")
            } else {
                useLayerMinus = true
            }
            
            
            return didGetTarget(host: host, port: port)

        case 0x03: // DOMAIN: 1(len) + len + 2
            guard recvBuffer.count >= 1 else { return false }
            let lenByte = Array(recvBuffer.prefix(1))
            guard lenByte.count == 1 else { return false }
            
            let n = Int(lenByte[0])
            guard recvBuffer.count >= 1 + n + 2 else { return false }
            
            let nameData = recvBuffer.dropFirst(1).prefix(n)
            let host = String(data: nameData, encoding: .utf8) ?? ""
            
            let portBytes = Array(recvBuffer.dropFirst(1 + n).prefix(2))
            guard portBytes.count == 2 else { return false }
            let port = (Int(portBytes[0]) << 8) | Int(portBytes[1])
            
            recvBuffer.removeFirst(1 + n + 2)

            // --- 白名单：命中则直连 ---

            if shouldDirect(host: host) {
                useLayerMinus = false
                log("SOCKS5 CONNECT \(host):\(port) matched allowlist -> DIRECT")
            } else {
                useLayerMinus = true
            }

            // --- 黑名单：SOCKS5 直接按规则禁止 ---
            if shouldBlock(host: host) {
                log("SOCKS5 CONNECT \(host):\(port) blocked by blacklist")
                blockSocksAndClose("SOCKS5 \(host)")
                return true
            }
            
            
            return didGetTarget(host: host, port: port)

        case 0x04: // IPv6: 16 + 2
            guard recvBuffer.count >= 18 else { return false }
            let bytes = Array(recvBuffer.prefix(18))
            guard bytes.count == 18 else { return false }
            
            var s = ""
            for i in stride(from: 0, to: 16, by: 2) {
                s += String(format: "%02x%02x", bytes[i], bytes[i+1])
                if i < 14 { s += ":" }
            }
            let port = (Int(bytes[16]) << 8) | Int(bytes[17])
            recvBuffer.removeFirst(18)
            return didGetTarget(host: s, port: port)

        default:
            sendReply(socksReply: 0x08) // Address type not supported
            close(reason: "bad atyp \(atyp)")
            return false
        }
    }

    private func didGetTarget(host: String, port: Int) -> Bool {
        log("CONNECT \(host):\(port) -> reply OK, then wait first-body")
        // 发送 SOCKS5 成功响应
        let reply = Data([0x05, 0x00, 0x00, 0x01, 0,0,0,0, 0,0])
        client.send(content: reply, completion: .contentProcessed { [weak self] err in
            guard let self = self else { return }
            if let err = err {
                self.log("send CONNECT OK err: \(err)")
                self.close(reason: "send CONNECT OK err")
                return
            }
            self.log("CONNECT OK sent")
        })
        phase = .connected(host: host, port: port)
        // 若缓冲里已经有首包，立刻处理
        parseBuffer()
        return true
    }
    
    private func isClientAliveAndWritable() async -> Bool {
        if closed || !clientIsReady { return false }
        // 避免在 @Sendable 闭包里捕获 self
        let c = client
        return await withCheckedContinuation { cont in
            c.send(content: Data(), completion: .contentProcessed { err in
                cont.resume(returning: (err == nil))
            })
        }
    }

    // MARK: 首包处理（智能区分 SSL / 非 SSL）
    private func processFirstBody(host: String, port: Int, firstBody: Data) {
        guard !handedOff else { return }
        
        let fb = firstBody   // 捕获值，供异步块使用
        
        Task { [weak self] in
            guard let self = self else { return }
            // ⛳️ 移交/打包 LM 之前先探测客户端是否还活着/可写
            if !(await self.isClientAliveAndWritable()) {
                self.log("drop before LM: client already gone or not writable")
                self.close(reason: "client gone (pre-LM probe)")
                return
            }
        }
        
        var detectedInfo = ""
        var isSSL = false
        
        // 智能检测：检查是否为 TLS/SSL 握手
        if isTLSClientHello(firstBody) {
            // SSL/TLS 加密连接
            isSSL = true
            detectedInfo = "TLS/SSL ClientHello detected"
            log("Detected SSL/TLS connection (ClientHello) to \(host):\(port), bytes=\(firstBody.count)")
            
        } else if let httpInfo = parseHttpFirstLineAndHost(firstBody) {
            // HTTP 明文连接
            isSSL = false
            detectedInfo = "HTTP \(httpInfo.method) \(httpInfo.path) HTTP/\(httpInfo.version)"
            if !httpInfo.host.isEmpty {
                detectedInfo += ", Host: \(httpInfo.host)"
            }
            log("Detected HTTP connection: \(detectedInfo)")
            
            // 对于 HTTP CONNECT 方法，通常表示隧道代理（可能后续会升级为 SSL）
            if httpInfo.method.uppercased() == "CONNECT" {
                log("HTTP CONNECT method detected - tunnel proxy request")
            }
            
        } else if isLikelyHTTP(firstBody) {
            // 可能是 HTTP 但解析失败
            isSSL = false
            detectedInfo = "Likely HTTP but parse failed"
            log("Possible HTTP connection but couldn't parse, bytes=\(firstBody.count)")
            
        } else {
            // 无法识别的协议，根据端口猜测
            if port == 443 || port == 8443 || port == 465 || port == 993 || port == 995 {
                isSSL = true
                detectedInfo = "Unknown protocol on SSL port \(port), treating as SSL"
                log("Unknown protocol on common SSL port \(port), treating as encrypted")
            } else {
                isSSL = false
                detectedInfo = "Unknown protocol on port \(port)"
                log("Unknown protocol, treating as plain text, bytes=\(firstBody.count)")
            }
        }
        
        // 将首包转换为 Base64
        let b64 = firstBody.base64EncodedString()
        //log("Converting first body to Base64: \(b64.prefix(100))... (total: \(b64.count) chars)")
        //log("Protocol detection: \(detectedInfo), isSSL=\(isSSL)")
        
        // 标记已移交，停止接收
        handedOff = true
        phase = .bridged
        
        if isIPAddress(host) {
            if isTelegramIP(host) {
                useLayerMinus = true
                log("🔵 TELEGRAM IP detected: \(host):\(port) -> force LayerMinus")
            } else {
                useLayerMinus = false
                log("🟢🟢 DIRECT (IP literal): \(host):\(port) -> bypass LayerMinus")
            }
        }
            
        Task { [weak self] in
            guard let self = self else { return }
            // ⛳️ 移交/打包 LM 之前先探测客户端是否还活着/可写
            if !(await self.isClientAliveAndWritable()) {
                self.log("drop before LM: client already gone or not writable")
                self.close(reason: "client gone (pre-LM probe)")
                return
            }
        }
        
        useLayerMinus = false

        guard useLayerMinus, let egressNode = self.layerMinus.getRandomEgressNodes(),
            !egressNode.isEmpty else {
		// guard useLayerMinus, let egressNode = self.layerMinus.getRandomEgressNodes(),
        //     egressNode.isEmpty else {
            let connectInfo = "origin=\(host):\(port) \(useLayerMinus) httpConnect \(httpConnect) socksVer \((socksVer != nil) ? String(socksVer!) : "nil") useLayerMinus=\(useLayerMinus), layerMinus entryNodes = \(self.layerMinus.entryNodes.count) egressNode = \(self.layerMinus.egressNodes.count) using DIRECT CONNECT"
            
            // 创建并启动 LayerMinusBridge，保存引用
            
            log("🟢  \(connectInfo)")
            let newBridge = LayerMinusBridge(
                id: self.id,
                client: self.client,
                targetHost: host,
                targetPort: port,
                verbose: self.verbose,
                connectInfo: connectInfo,
                onClosed: { [weak self] bridgeId in
                    // 当 bridge 关闭时，先采样打印内存，再关闭连接
                    if let strong = self {
                        Self.startGlobalMemoryMonitorIfNeeded(event: "LayerMinusBridge #\(bridgeId) DESTROYED", logger: { strong.log($0) })
                        strong.log("Bridge #\(bridgeId) closed, closing ServerConnection")
                        strong.close(reason: "Bridge closed")
                    }

                }
            )
            
            self.bridge = newBridge
            self.onRoutingDecided?(self)
            
            // KPI：标记 handoff 时刻（与 Bridge.start 的 tStart 对齐，用于 handoff->start）
            self.log("KPI handoff -> LM host=\(host):\(port) ")
            Task { await newBridge.markHandoffNow() }

			Self.startGlobalMemoryMonitorIfNeeded(event: "LayerMinusBridge #\(self.id) CREATED", logger: { [weak self] in self?.log($0) })
            // 传递 Base64 编码的首包给 bridge
            Task { await newBridge.start(withFirstBody: b64) }
            return
        }
        
        let entryInfo = self.layerMinus.getRandomEntryNodes()?.ip_addr ?? "NONE"
        
        
        if self.httpConnect {
            self.log("Layer Minus start by HTTP/HTTPS PROXY 🟢 \(self.id) \(host):\(port) with entry  \(entryInfo), egress \(egressNode.ip_addr)")
        } else {
            self.log("Layer Minus start by SOCKS 5 PROXY 🟢 \(self.id) \(host):\(port) with entry  \(entryInfo), egress \(egressNode.ip_addr)")
        }


        
        let message = self.layerMinus.makeSocksRequest(host: host, port: port, body: b64, command: "CONNECT")
        let messageData = message.data(using: .utf8)!
        let account = self.layerMinus.keystoreManager.addresses![0]

        Task{
            // 极端情况下，在 LM 消息签名/组包前也再探测一次
            if !(await self.isClientAliveAndWritable()) {
                self.log("drop before LM pack: client not writable (third probe)")
                self.close(reason: "client gone (pre-pack)")
                return
            }
        }
            
        Task{
            
            let signMessage = try await self.layerMinus.web3.personal.signPersonalMessage(message: messageData, from: account, password: "")
            if let callFun2 = self.layerMinus.javascriptContext.objectForKeyedSubscript("json_sign_message") {
                if let ret2 = callFun2.call(withArguments: [message, "0x\(signMessage.toHexString())"]) {
                    let cmd = ret2.toString()!
                    let pre_request = self.layerMinus.createValidatorData(node: egressNode, responseData: cmd)
                    let request = self.layerMinus.makeRequest(host: entryInfo == "NONE" ? egressNode.ip_addr: entryInfo, data: pre_request)
                    
                    self.log("KPI handoff -> LM host=\(host):\(port) entry=\(entryInfo == "NONE" ? egressNode.ip_addr: entryInfo) egress=\(egressNode.ip_addr)")
                    let connectInfo = "origin=\(host):\(port) entry=\(entryInfo == "NONE" ? egressNode.ip_addr: entryInfo) egress=\(egressNode.ip_addr)"
                    log("🟢🟢🟢  \(connectInfo)")
                    
                    let newBridge = LayerMinusBridge(
                        id: self.id,
                        client: self.client,
                        targetHost: entryInfo == "NONE" ? egressNode.ip_addr: entryInfo,
                        targetPort: 80,
                        verbose: self.verbose,
                        connectInfo: connectInfo,
                        onClosed: { [weak self] bridgeId in
                            // 当 bridge 关闭时，关闭 ServerConnection
                            self?.log("Bridge #\(bridgeId) closed, closing ServerConnection")
                            self?.close(reason: "Bridge closed")
                        }
                    )
                    self.isLayerMinusRouted = true
                    self.bridge = newBridge
                    self.onRoutingDecided?(self)
					Self.startGlobalMemoryMonitorIfNeeded(event: "LayerMinusBridge #\(self.id) CREATED", logger: { [weak self] in self?.log($0) })
                    
                    // 传递 Base64 编码的首包给 bridge（actor 方法需 await）
                    Task {
                        await newBridge.start(withFirstBody: request.data(using: .utf8)!.base64EncodedString())
                    }
                }
            }
        }
        
        
        
        
    }

    // MARK: TLS/SSL 检测
    private func isTLSClientHello(_ data: Data) -> Bool {
        // TLS record: 0x16 (Handshake) 0x03 0x01/02/03... (TLS version), length(2)
        guard data.count >= 5 else { return false }
        let bytes = Array(data.prefix(2))
        guard bytes.count == 2 else { return false }
        
        // 0x16 = TLS Handshake, 0x03 = TLS/SSL 3.x
        return bytes[0] == 0x16 && bytes[1] == 0x03
    }

    // MARK: HTTP 解析
    private func parseHttpFirstLineAndHost(_ data: Data) -> (method: String, path: String, version: String, host: String)? {
        guard let text = String(data: data, encoding: .utf8) else { return nil }
        
        // 查找第一个 \r\n
        guard let rnRange = text.range(of: "\r\n") else { return nil }
        let firstLine = String(text[..<rnRange.lowerBound])
        
        // 解析 HTTP 请求行: METHOD PATH HTTP/VERSION
        let parts = firstLine.split(separator: " ", maxSplits: 2)
        guard parts.count >= 3 else { return nil }
        
        let method = String(parts[0])
        let path = String(parts[1])
        var version = String(parts[2])
        
        // 验证 HTTP 方法
        let httpMethods = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "CONNECT", "PATCH", "TRACE"]
        guard httpMethods.contains(method.uppercased()) else { return nil }
        
        // 提取版本号
        if version.hasPrefix("HTTP/") {
            version.removeFirst(5)
        }
        
        // 查找 Host 头
        var hostHeader = ""
        let remainingText = String(text[rnRange.upperBound...])
        for line in remainingText.split(separator: "\r\n") {
            let trimmedLine = line.trimmingCharacters(in: .whitespaces)
            if trimmedLine.lowercased().hasPrefix("host:") {
                let hostValue = trimmedLine.dropFirst("host:".count)
                hostHeader = hostValue.trimmingCharacters(in: .whitespaces)
                break
            }
        }
        
        return (method, path, version, hostHeader)
    }
    
    // 判断是否为 Telegram 的 IP 段
    private func isTelegramIP(_ ip: String) -> Bool {
        let telegramCIDRs = [
            "149.154.160.0/20",
            "91.108.4.0/22"
        ]
        for cidr in telegramCIDRs {
            if ipInCIDR(ip: ip, cidr: cidr) {
                return true
            }
        }
        return false
    }

    private func ipInCIDR(ip: String, cidr: String) -> Bool {
        let parts = cidr.split(separator: "/")
        guard parts.count == 2,
              let baseAddr = IPv4Address(String(parts[0])),
              let ipAddr = IPv4Address(ip),
              let prefix = Int(parts[1]) else { return false }

        return ipAddr.inRange(of: baseAddr, prefix: prefix)
    }

    // MARK: HTTP 启发式检测
    private func isLikelyHTTP(_ data: Data) -> Bool {
        guard data.count >= 4 else { return false }
        guard let text = String(data: data.prefix(16), encoding: .utf8) else { return false }
        
        // 检查是否以常见 HTTP 方法开头
        let httpMethods = ["GET ", "POST ", "PUT ", "DELETE ", "HEAD ", "OPTIONS ", "CONNECT ", "PATCH ", "TRACE "]
        for method in httpMethods {
            if text.hasPrefix(method) {
                return true
            }
        }
        
        return false
    }

    
    // MARK: Reply helper
    private func sendReply(socksReply rep: UInt8) {
        let reply = Data([0x05, rep, 0x00, 0x01, 0,0,0,0, 0,0])
        client.send(content: reply, completion: .contentProcessed({ [weak self] err in
            if let err = err {
                self?.log("send reply err: \(err)")
            }
        }))
    }
}

extension IPv4Address {
    func inRange(of network: IPv4Address, prefix: Int) -> Bool {
        let mask: UInt32 = prefix == 0 ? 0 : ~UInt32(0) << (32 - prefix)
        let selfInt = self.rawValue.withUnsafeBytes { $0.load(as: UInt32.self).bigEndian }
        let netInt = network.rawValue.withUnsafeBytes { $0.load(as: UInt32.self).bigEndian }
        return (selfInt & mask) == (netInt & mask)
    }
}
