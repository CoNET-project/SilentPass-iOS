import Foundation
import Network
import os


public final class ServerConnection {
    
    // 增强的入口节点选择策略
    private func selectBestEntryNode() -> Node? {
        // 获取所有可用的入口节点
        guard let allEntryNodes = self.layerMinus.getAllEntryNodes(),
              !allEntryNodes.isEmpty else {
            log("No entry nodes available")
            return nil
        }
        
        // 计算每个节点的评分
        var nodeScores: [(node: Node, score: Double)] = []
        
        for node in allEntryNodes {
            if let score = NodeQoS.shared.getNodeScore(ip: node.ip_addr) {
                nodeScores.append((node, score))
            }
        }
        
        // 如果没有可用节点，尝试使用随机节点探索
        if nodeScores.isEmpty {
            log("All nodes filtered by QoS, attempting random exploration")
            return allEntryNodes.randomElement()
        }
        
        // 使用加权随机选择策略
        return weightedRandomSelection(from: nodeScores)
    }
    
    private let cleanupTimer = NodeQoSCleanupTimer()
    private var statsTimer: Timer?
    
    // 加权随机选择
    private func weightedRandomSelection(from nodeScores: [(node: Node, score: Double)]) -> Node? {
        guard !nodeScores.isEmpty else { return nil }
        
        // 如果只有一个节点，直接返回
        if nodeScores.count == 1 {
            return nodeScores[0].node
        }
        
        // 计算总分
        let totalScore = nodeScores.reduce(0.0) { $0 + $1.score }
        guard totalScore > 0 else {
            // 如果所有分数都是0，随机选择
            return nodeScores.randomElement()?.node
        }
        
        // 生成随机数进行加权选择
        let random = Double.random(in: 0..<totalScore)
        var cumulative = 0.0
        
        for (node, score) in nodeScores {
            cumulative += score
            if random < cumulative {
                return node
            }
        }
        
        // 兜底返回最后一个
        return nodeScores.last?.node
    }
    
    // 辅助方法：创建直连 Bridge
     private func createDirectBridge(host: String, port: Int, firstBodyBase64: String) {
         let connectInfo = "origin=\(host):\(port) DIRECT CONNECT"
         let newBridge = LayerMinusBridge(
             id: self.id,
             client: self.client,
             targetHost: host,
             targetPort: port,
             verbose: self.verbose,
             connectInfo: connectInfo,
             onClosed: { [weak self] bridgeId in
                 self?.log("Bridge #\(bridgeId) closed, closing ServerConnection")
                 self?.close(reason: "Bridge closed")
             }
         )
         
         self.bridge = newBridge
         self.onRoutingDecided?(self)
         
         log("KPI handoff -> DIRECT CONNECT host=\(host):\(port)")
         newBridge.markHandoffNow()
         newBridge.start(withFirstBody: firstBodyBase64)
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

    // 命中白名单 → 直连（由 ServerConnection 决策，不走 LM 打包）
    @inline(__always)
    private func shouldDirect(host: String) -> Bool {
        return Allowlist.matches(host)
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
    
    /// 该连接是否已切到 LayerMinus 通道（由业务分支显式标记）
    public private(set) var isLayerMinusRouted: Bool = false

    /// 当确定此连接将经由 LayerMinusBridge 转发时调用
    public func markAsLayerMinusRouted() {
        self.isLayerMinusRouted = true
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

    @inline(__always)
    private func log(_ msg: String) {
        //NSLog("[ServerConnection] #\(id) %@", msg)
    }

    public func start() {
        client.stateUpdateHandler = { [weak self] state in
            guard let self = self else { return }
            switch state {
            case .ready:
                self.log("client ready; enter recv loop")
                self.recvLoop()
            case .failed(let e):
                self.log("client failed: \(e)")
                self.close(reason: "client failed")
            case .cancelled:
                self.log("client cancelled")
                self.close(reason: "client cancelled")
            default:
                break
            }
        }
        // 启动清理定时器
         cleanupTimer.start()
         
         // 启动统计定时器（每5分钟输出一次统计）
         statsTimer = Timer.scheduledTimer(withTimeInterval: 300, repeats: true) { _ in
             self.logNodeStatistics()
         }
        
        client.start(queue: queue)
        log("will start")
    }
    
    private func logNodeStatistics() {
        let stats = NodeQoS.shared.getDetailedStatistics()
        NSLog("[Server] Node Statistics: \(stats)")
    }

    public func close(reason: String) {
        guard !closed else { return }
        closed = true
        phase = .closed
        log("close: \(reason)")
        
        // 取消客户端连接
        client.cancel()
        
        // 如果有 bridge，也要关闭它
        bridge?.cancel(reason: "ServerConnection closed: \(reason)")
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
                // HTTP/HTTPS proxy support added
                
                // 先尝试 SOCKS5；若不是，则尝试 HTTP 代理首包解析
                if let first = recvBuffer.first, first == 0x05 {
                   advanced = parseMethodSelect()
                    self.httpConnect = false
               } else {
                    // 可能是 HTTP/HTTPS 显式代理（GET/POST/CONNECT ...）
                    advanced = tryParseHTTPProxyRequest()
                    if !advanced {
                        // 还不足以解析 HTTP 首部，继续等待更多数据
                        // 避免误关连接
                        log("methodSelect: waiting for more bytes (maybe HTTP proxy)")
                    }
                }
                
                if advanced {
                    log("parseBuffer: methodSelect consumed \(bufferSizeBefore - recvBuffer.count) bytes")
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
    
    // MARK: HTTP/HTTPS Proxy 解析与改写（绝对URI → origin-form）
    private func tryParseHTTPProxyRequest() -> Bool {
        
        
        // 首先检查是否为PAC请求
        if isPACRequest(recvBuffer) {
            handlePACRequest()
            return true
        }
        
        // 我们至少需要一行（\r\n）来判断方法，且处理非 CONNECT 时需要首部结束（\r\n\r\n）
        let CRLF = Data([0x0d, 0x0a])
        let CRLFCRLF = Data([0x0d, 0x0a, 0x0d, 0x0a])

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

    // MARK: 首包处理（智能区分 SSL / 非 SSL）
    private func processFirstBody(host: String, port: Int, firstBody: Data) {
        guard !handedOff else { return }
        
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
        
        // —— 选择 egress：保持随机；选择 entry：应用 QoS 过滤（排除慢的一半 & 禁用 5 分钟的节点）
        guard useLayerMinus, let egressNode = self.layerMinus.getRandomEgressNodes() else {
            createDirectBridge(host: host, port: port, firstBodyBase64: b64)
            return
        }
        
        // 使用增强的入口节点选择策略
        guard let entryNode = selectBestEntryNode() else {
            log("No suitable entry node found, falling back to direct connection")
            createDirectBridge(host: host, port: port, firstBodyBase64: b64)
            return
        }
        
        
        if self.httpConnect {
            self.log("Layer Minus start by HTTP/HTTPS PROXY 🟢 \(self.id) \(host):\(port) with entry  \(entryNode.ip_addr), egress \(egressNode.ip_addr)")
        } else {
            self.log("Layer Minus start by SOCKS 5 PROXY 🟢 \(self.id) \(host):\(port) with entry  \(entryNode.ip_addr), egress \(egressNode.ip_addr)")
        }


        
        let message = self.layerMinus.makeSocksRequest(host: host, port: port, body: b64, command: "CONNECT")
        let messageData = message.data(using: .utf8)!
        let account = self.layerMinus.keystoreManager.addresses![0]


        Task{
            let signMessage = try await self.layerMinus.web3.personal.signPersonalMessage(message: messageData, from: account, password: "")
            if let callFun2 = self.layerMinus.javascriptContext.objectForKeyedSubscript("json_sign_message") {
                if let ret2 = callFun2.call(withArguments: [message, "0x\(signMessage.toHexString())"]) {
                    let cmd = ret2.toString()!
                    let pre_request = self.layerMinus.createValidatorData(node: egressNode, responseData: cmd)
                    let request = self.layerMinus.makeRequest(host: entryNode.ip_addr, data: pre_request)
                    
                    self.log("KPI handoff -> LM host=\(host):\(port) entry=\(entryNode.ip_addr) egress=\(egressNode.ip_addr)")
                    let connectInfo = "origin=\(host):\(port) entry=\(entryNode.ip_addr) egress=\(egressNode.ip_addr)"
                    let newBridge = LayerMinusBridge(
                        id: self.id,
                        client: self.client,
                        targetHost: entryNode.ip_addr,
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
                    
                    // 传递 Base64 编码的首包给 bridge
                    newBridge.start(withFirstBody: request.data(using: .utf8)!.base64EncodedString())
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

extension ServerConnection {
    
    // 1. 添加PAC请求检测方法
    private func isPACRequest(_ data: Data) -> Bool {
        guard let text = String(data: data, encoding: .utf8) else { return false }
        return text.hasPrefix("GET /pac HTTP/") || text.hasPrefix("GET /proxy.pac HTTP/")
    }
    
    // 2. 添加PAC脚本生成方法
    private func generatePACScript() -> String {
        // 将白名单和黑名单转换为JavaScript数组
        let whitelistPatterns = Allowlist.patterns.map { "\"\($0)\"" }.joined(separator: ",\n        ")
        let blacklistPatterns = AdBlacklist.patterns.map { "\"\($0)\"" }.joined(separator: ",\n        ")
        
        let pacScript = """
        function FindProxyForURL(url, host) {
            // 规范化主机名
            host = host.toLowerCase();
            
            // 黑名单域名 - 返回PROXY但会被服务器拦截
            var blacklist = [
                \(blacklistPatterns)
            ];
            
            // 白名单域名 - 直接连接
            var whitelist = [
                \(whitelistPatterns)
            ];
            
            // 检查黑名单
            for (var i = 0; i < blacklist.length; i++) {
                var pattern = blacklist[i].toLowerCase();
                if (pattern.indexOf("*.") === 0) {
                    var suffix = pattern.substring(1);
                    if (host.endsWith(suffix)) {
                        return "PROXY 127.0.0.1:8888";  // 强制走代理以便拦截
                    }
                } else if (host === pattern || host.endsWith("." + pattern)) {
                    return "PROXY 127.0.0.1:8888";  // 强制走代理以便拦截
                }
            }
            
            // 检查白名单
            for (var i = 0; i < whitelist.length; i++) {
                var pattern = whitelist[i].toLowerCase();
                if (pattern.indexOf("*.") === 0) {
                    var suffix = pattern.substring(1);
                    if (host.endsWith(suffix)) {
                        return "DIRECT";  // 直接连接
                    }
                } else if (host === pattern || host.endsWith("." + pattern)) {
                    return "DIRECT";  // 直接连接
                }
            }
            
            // 本地地址直接连接
            if (isPlainHostName(host) ||
                shExpMatch(host, "*.local") ||
                host === "localhost" ||
                host === "127.0.0.1" ||
                host === "::1") {
                return "DIRECT";
            }
            
            // 内网IP检测
            if (host.match(/^\\d+\\.\\d+\\.\\d+\\.\\d+$/)) {
                if (isInNet(host, "10.0.0.0", "255.0.0.0") ||
                    isInNet(host, "172.16.0.0", "255.240.0.0") ||
                    isInNet(host, "192.168.0.0", "255.255.0.0") ||
                    isInNet(host, "127.0.0.0", "255.0.0.0")) {
                    return "DIRECT";
                }
            }
            
            // 默认使用代理
            return "PROXY 127.0.0.1:8888; SOCKS5 127.0.0.1:8888";
        }
        """
        
        return pacScript
    }
    
    // 3. 添加PAC响应处理方法
    private func handlePACRequest() {
        log("Handling PAC request")
        
        let pacContent = generatePACScript()
        let contentData = pacContent.data(using: .utf8) ?? Data()
        
        // 构建HTTP响应
        let httpResponse = """
        HTTP/1.1 200 OK\r
        Content-Type: application/x-ns-proxy-autoconfig\r
        Content-Length: \(contentData.count)\r
        Cache-Control: no-cache, no-store, must-revalidate\r
        Pragma: no-cache\r
        Expires: 0\r
        Connection: close\r
        Access-Control-Allow-Origin: *\r
        Access-Control-Allow-Methods: GET, OPTIONS\r
        Access-Control-Allow-Headers: *\r
        \r
        \(pacContent)
        """
        
        // 发送响应并关闭连接
        client.send(content: httpResponse.data(using: .utf8), completion: .contentProcessed { [weak self] err in
            guard let self = self else { return }
            if let err = err {
                self.log("Failed to send PAC response: \(err)")
            } else {
                self.log("PAC script sent successfully")
            }
            self.close(reason: "PAC request completed")
        })
        
        // 标记为已处理
        recvBuffer.removeAll(keepingCapacity: false)
        handedOff = true
    }
    
    /*
    4. 修改tryParseHTTPProxyRequest方法（最小修改）
    在tryParseHTTPProxyRequest方法的开始处添加以下代码：
    
    private func tryParseHTTPProxyRequest() -> Bool {
        // === 添加这部分代码 ===
        // 首先检查是否为PAC请求
        if isPACRequest(recvBuffer) {
            handlePACRequest()
            return true
        }
        // === 添加结束 ===
        
        // 以下是原有代码...
        let CRLF = Data([0x0d, 0x0a])
        let CRLFCRLF = Data([0x0d, 0x0a, 0x0d, 0x0a])
        ...
    }
    */
}
