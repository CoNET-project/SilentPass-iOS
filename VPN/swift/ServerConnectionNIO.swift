//
//  ServerConnectionNIO.swift
//  CoNETVPN1
//
//  Created by peter on 2025-09-17.
//

// ServerConnectionNIO.swift
import Foundation
import NIO
import NIOTransportServices

/// NIO 版本的本地代理连接：解析 SOCKS5 / HTTP 代理首包，决定直连/LayerMinus，
/// 然后把下游 Channel 交给 LayerMinusBridgeNIO 承载自动背压与双向转发。
public final class ServerConnectionNIO {

    public let id: UInt64
    public let channel: Channel
    public let verbose: Bool
    public let layerMinus: LayerMinus
    public var onClosed: ((UInt64) -> Void)?

    public init(id: UInt64,
                channel: Channel,
                layerMinus: LayerMinus,
                verbose: Bool = true,
                onClosed: ((UInt64) -> Void)? = nil) {
        self.id = id
        self.channel = channel
        self.layerMinus = layerMinus
        self.verbose = verbose
        self.onClosed = onClosed
    }

    /// 安装下游解析处理器，启动读循环
    public func start() {
        channel.pipeline.addHandler(ServerConnInboundHandler(
            id: id,
            layerMinus: layerMinus,
            verbose: verbose,
            onClosed: onClosed
        ), position: .last).whenFailure { error in
            #if DEBUG
            NSLog("[ServerConnectionNIO #\(self.id)] add handler failed: %@", "\(error)")
            #endif
            _ = self.channel.close()
            self.onClosed?(self.id)
        }
    }
}

// MARK: - Inbound Handler：移植自 NW 版 ServerConnection 的解析 & handoff 语义
final class ServerConnInboundHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn   = ByteBuffer
    typealias OutboundIn  = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let id: UInt64
    private let layerMinus: LayerMinus
    private let verbose: Bool
    private var onClosed: ((UInt64) -> Void)?

    // —— 解析状态（照搬旧类）
    private enum Phase {
        case methodSelect
        case requestHead
        case requestAddr(ver: UInt8, cmd: UInt8, atyp: UInt8)
        case connected(host: String, port: Int)
        case bridged
        case closed
    }
    private var phase: Phase = .methodSelect
    private var httpConnect: Bool = true
    private var useLayerMinus: Bool = true
    private var closed = false
    private var handedOff = false

    // —— 缓冲与阈值（与旧类一致）
    private let HTTP_HDR_MAX  = 31 * 1024
    private let HTTP_BODY_MAX = 31 * 1024
    private let RECV_BUFFER_SOFT_LIMIT = 32 * 1024

    private var recvBuffer = Data()

    init(id: UInt64,
         layerMinus: LayerMinus,
         verbose: Bool,
         onClosed: ((UInt64) -> Void)?) {
        self.id = id
        self.layerMinus = layerMinus
        self.verbose = verbose
        self.onClosed = onClosed
        log("🟢 CREATED ServerConnectionNIO #\(id)")
    }

    deinit {
        log("🔴 DESTROYED ServerConnectionNIO #\(id)")
    }

    // MARK: - Channel events

    func channelActive(context: ChannelHandlerContext) {
        log("downstream active; start reading")
        context.read()
    }

    func channelInactive(context: ChannelHandlerContext) {
        if !closed { close(context: context, reason: "downstream inactive") }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !closed, !handedOff else { return }
        var buf = unwrapInboundIn(data)
        if let bytes = buf.readBytes(length: buf.readableBytes) {
            recvBuffer.append(contentsOf: bytes)
        }

        if recvBuffer.count > RECV_BUFFER_SOFT_LIMIT {
            // 限制缓冲，避免 OOM
            let KEEP = 64 * 1024
            if recvBuffer.count > KEEP {
                recvBuffer = recvBuffer.suffix(KEEP)
            }
            log("recvBuffer exceeded soft limit (\(recvBuffer.count)B), trimmed to \(KEEP)B")
        }

        parseBuffer(context)
        if !closed, !handedOff { context.read() }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        log("error: \(error)")
        close(context: context, reason: "error \(error)")
    }

    // MARK: - Close
    private func close(context: ChannelHandlerContext, reason: String) {
        guard !closed else { return }
        closed = true
        phase = .closed
        log("close: \(reason)")
        context.close(promise: nil)
        onClosed?(id)
    }

    // MARK: - Buffer parser（按旧版逻辑）
    private func parseBuffer(_ context: ChannelHandlerContext) {
        var advanced = true
        while advanced, !closed, !handedOff {
            advanced = false
            switch phase {
            case .methodSelect:
                if let first = recvBuffer.first, first == 0x05 {
                    advanced = parseMethodSelect(context)
                    httpConnect = false
                } else {
                    advanced = tryParseHTTPProxyRequest(context)
                    if !advanced { log("methodSelect: waiting (HTTP?)") }
                }
            case .requestHead:
                advanced = parseRequestHead(context)
            case .requestAddr(let ver, let cmd, let atyp):
                advanced = parseRequestAddr(context, ver: ver, cmd: cmd, atyp: atyp)
            case .connected(let host, let port):
                if !recvBuffer.isEmpty {
                    let first = recvBuffer
                    recvBuffer.removeAll(keepingCapacity: false)
                    processFirstBody(context, host: host, port: port, firstBody: first)
                    advanced = true
                }
            case .bridged, .closed:
                return
            }
        }
    }

    // MARK: - SOCKS5: method select
    private func parseMethodSelect(_ context: ChannelHandlerContext) -> Bool {
        guard recvBuffer.count >= 2 else { return false }
        let bytes = Array(recvBuffer.prefix(2))
        guard bytes.count == 2 else { return false }
        let ver = bytes[0], n = Int(bytes[1])
        guard ver == 0x05, recvBuffer.count >= 2 + n else { return false }

        let methods = Array(recvBuffer.dropFirst(2).prefix(n))
        recvBuffer.removeFirst(2 + n)
        phase = .requestHead
        log("mselect parsed: ver=5 n=\(n) methods=\(methods)")

        // 回复 NO-AUTH
        var out = context.channel.allocator.buffer(capacity: 2)
        out.writeBytes([0x05, 0x00])
        // 直接写 ByteBuffer，避免 NIOAny 的弃用告警
        context.writeAndFlush(out, promise: nil)
        return true
    }

    // MARK: - SOCKS5: request head
    private func parseRequestHead(_ context: ChannelHandlerContext) -> Bool {
        guard recvBuffer.count >= 4 else { return false }
        let b = Array(recvBuffer.prefix(4))
        let ver = b[0], cmd = b[1], rsv = b[2], atyp = b[3]
        log("reqHead ver=\(ver) cmd=\(cmd) rsv=\(rsv) atyp=\(String(format:"0x%02x", atyp))")
        guard ver == 0x05, cmd == 0x01 else {
            sendSocksReply(context, 0x07); close(context: context, reason: "unsupported cmd/ver"); return false
        }
        recvBuffer.removeFirst(4)
        phase = .requestAddr(ver: ver, cmd: cmd, atyp: atyp)
        return true
    }

    // MARK: - SOCKS5: request addr
    private func parseRequestAddr(_ ctx: ChannelHandlerContext, ver: UInt8, cmd: UInt8, atyp: UInt8) -> Bool {
        switch atyp {
        case 0x01: // IPv4
            guard recvBuffer.count >= 6 else { return false }
            let b = Array(recvBuffer.prefix(6))
            let host = "\(b[0]).\(b[1]).\(b[2]).\(b[3])"
            let port = (Int(b[4]) << 8) | Int(b[5])
            recvBuffer.removeFirst(6)
            if shouldDirect(host: host) { useLayerMinus = false; log("SOCKS5 \(host):\(port) allowlist -> DIRECT") }
            if shouldBlock(host: host) { log("SOCKS5 \(host) blocked"); blockSocksAndClose(ctx, reason: "SOCKS5 \(host)"); return true }
            return didGetTarget(ctx, host: host, port: port)

        case 0x03: // DOMAIN
            guard recvBuffer.count >= 1 else { return false }
            let n = Int(recvBuffer[recvBuffer.startIndex])
            guard recvBuffer.count >= 1 + n + 2 else { return false }
            let name = recvBuffer.dropFirst(1).prefix(n)
            let host = String(data: name, encoding: .utf8) ?? ""
            let pb = Array(recvBuffer.dropFirst(1+n).prefix(2))
            let port = (Int(pb[0]) << 8) | Int(pb[1])
            recvBuffer.removeFirst(1 + n + 2)
            if shouldDirect(host: host) { useLayerMinus = false; log("SOCKS5 \(host):\(port) allowlist -> DIRECT") }
            if shouldBlock(host: host) { log("SOCKS5 \(host) blocked"); blockSocksAndClose(ctx, reason: "SOCKS5 \(host)"); return true }
            return didGetTarget(ctx, host: host, port: port)

        case 0x04: // IPv6（简单透传）
            guard recvBuffer.count >= 18 else { return false }
            let pb = Array(recvBuffer.prefix(18))
            var s = ""
            for i in stride(from: 0, to: 16, by: 2) {
                s += String(format:"%02x%02x", pb[i], pb[i+1]); if i < 14 { s += ":" }
            }
            let port = (Int(pb[16]) << 8) | Int(pb[17])
            recvBuffer.removeFirst(18)
            return didGetTarget(ctx, host: s, port: port)

        default:
            sendSocksReply(ctx, 0x08); close(context: ctx, reason: "bad atyp \(atyp)"); return false
        }
    }

    private func didGetTarget(_ ctx: ChannelHandlerContext, host: String, port: Int) -> Bool {
        // 发送成功
        var out = ctx.channel.allocator.buffer(capacity: 10)
        out.writeBytes([0x05,0x00,0x00,0x01,0,0,0,0,0,0])
        ctx.writeAndFlush(out, promise: nil)
        phase = .connected(host: host, port: port)
        parseBuffer(ctx) // 若已有首包立即处理
        return true
    }

    // MARK: - HTTP/HTTPS 显式代理
    private func tryParseHTTPProxyRequest(_ ctx: ChannelHandlerContext) -> Bool {
        let CRLF = Data([0x0d,0x0a]), CRLFCRLF = Data([0x0d,0x0a,0x0d,0x0a])

        if recvBuffer.count > HTTP_HDR_MAX, recvBuffer.range(of: CRLFCRLF) == nil {
            blockHTTPAndClose(ctx, status: "431 Request Header Fields Too Large", reason: "Header > 31KB or malformed")
            return true
        }
        // 需要至少拿到首部结尾
        guard let firstLineEnd = recvBuffer.range(of: CRLF) else { return false }
        guard let firstLine = String(data: recvBuffer[..<firstLineEnd.lowerBound], encoding: .utf8) else { return false }
        let upper = firstLine.uppercased()
        let httpMethods = ["CONNECT","GET","POST","PUT","DELETE","HEAD","OPTIONS","PATCH","TRACE"]
        guard httpMethods.first(where: { upper.hasPrefix($0 + " ") }) != nil else { return false }

        // CONNECT：等到 \r\n\r\n
        if upper.hasPrefix("CONNECT ") {
            guard let headerEnd = recvBuffer.range(of: CRLFCRLF) else { return false }
            let parts = firstLine.split(separator: " "); guard parts.count >= 2 else { return false }
            let hostPort = String(parts[1])
            let tgt = splitHostPort(hostPort, defaultPort: 443)
            if shouldDirect(host: tgt.host) { useLayerMinus = false; log("HTTP CONNECT \(tgt.host):\(tgt.port) allowlist -> DIRECT") }
            if shouldBlock(host: tgt.host) {
                recvBuffer.removeSubrange(..<headerEnd.upperBound)
                blockHTTPForbiddenAndClose(ctx, "HTTP CONNECT \(tgt.host)")
                return true
            }
            // 丢首部，回 200 Established
            recvBuffer.removeSubrange(..<headerEnd.upperBound)
            writeString(ctx, "HTTP/1.1 200 Connection Established\r\nProxy-Agent: vpn2socks\r\n\r\n")
            phase = .connected(host: tgt.host, port: tgt.port)
            return true
        }

        // 非 CONNECT：需要 \r\n\r\n
        guard let headerEnd = recvBuffer.range(of: CRLFCRLF) else { return false }
        let firstParts = firstLine.split(separator: " ", maxSplits: 2)
        guard firstParts.count == 3 else { return false }
        let method = String(firstParts[0])
        let rawPath = String(firstParts[1])
        var version = String(firstParts[2]); if version.hasPrefix("HTTP/") { version.removeFirst(5) }

        // Host 头
        let headerData = recvBuffer[firstLineEnd.upperBound..<headerEnd.lowerBound]
        let headerText = String(data: headerData, encoding: .utf8) ?? ""
        var hostHeader = ""
        for line in headerText.split(separator: "\r\n") {
            let t = line.trimmingCharacters(in: .whitespaces)
            if t.lowercased().hasPrefix("host:") {
                hostHeader = t.dropFirst("host:".count).trimmingCharacters(in: .whitespaces)
                break
            }
        }

        let (targetHost, targetPort, originPath) = normalizeAbsoluteOrOriginPath(rawPath: rawPath, hostHeader: hostHeader)

        if method.uppercased() == "GET",
           (targetHost == "127.0.0.1" || targetHost == "localhost"),
           targetPort == 8888,
           (originPath == "/pac" || originPath == "/pac.js") {
            // 返回 PAC
            let body = PACBuilder.buildPAC()
            var headers = "HTTP/1.1 200 OK\r\n"
            headers += "Content-Type: application/x-ns-proxy-autoconfig; charset=utf-8\r\n"
            headers += "Cache-Control: no-store, max-age=0\r\n"
            headers += "Content-Length: \(body.count)\r\n"
            headers += "Connection: close\r\n\r\n"
            var all = Data(headers.utf8); all.append(body)
            recvBuffer.removeAll(keepingCapacity: false)
            writeData(ctx, all)
            close(context: ctx, reason: "served PAC")
            return true
        }

        if shouldDirect(host: targetHost) { useLayerMinus = false; log("HTTP \(method) \(targetHost):\(targetPort) allowlist -> DIRECT") }
        if shouldBlock(host: targetHost) {
            recvBuffer.removeAll(keepingCapacity: false)
            blockHTTPForbiddenAndClose(ctx, "HTTP \(method) \(targetHost)")
            return true
        }

        // 绝对URI → origin-form
        let newFirst = "\(method) \(originPath) HTTP/\(version)\r\n"
        let rest = recvBuffer[firstLineEnd.upperBound...]
        var rewritten = Data(newFirst.utf8); rewritten.append(rest)
        recvBuffer.removeAll(keepingCapacity: false)
        handoffToBridge(ctx, host: targetHost, port: targetPort, firstBody: rewritten)
        return true
    }

    // MARK: - 一步移交到 NIO Bridge
    private func handoffToBridge(_ ctx: ChannelHandlerContext, host: String, port: Int, firstBody: Data) {
        if httpConnect { log("🟢 HTTP/HTTPS proxy #\(id) \(host):\(port)") }
        else           { log("🟢 SOCKS v5 proxy #\(id) \(host):\(port)") }
        processFirstBody(ctx, host: host, port: port, firstBody: firstBody)
    }

    private func processFirstBody(_ ctx: ChannelHandlerContext, host: String, port: Int, firstBody: Data) {
        guard !handedOff else { return }
        handedOff = true
        phase = .bridged

        // —— 直连（或失败时）路径：直接把下游交给 Bridge，目标 host:port
        func startDirectBridge(connectInfo: String, targetHost: String, targetPort: Int, firstBody: Data) {
            let b64 = firstBody.base64EncodedString()
            let bridge = LayerMinusBridgeNIO(
                id: id,
                targetHost: targetHost,
                targetPort: targetPort,
                verbose: verbose,
                connectInfo: connectInfo,
                onClosed: { [weak self] _ in
                    self?.onClosed?(self?.id ?? 0)
                }
            )
            bridge.attachDownstream(ctx.channel)
            bridge.markHandoffNow()
            bridge.start(withFirstBody: b64)
            // 移除自己，让 bridge 接管 pipeline
            ctx.pipeline.removeHandler(self, promise: nil)
        }

        // 如果不走 LayerMinus，或者拿不到 egress/entry 节点，就直接连接目标
        guard useLayerMinus, let egress = layerMinus.getRandomEgressNodes(), !egress.isEmpty else {
            let info = "origin=\(host):\(port) DIRECT (LM disabled or no node)"
            startDirectBridge(connectInfo: info, targetHost: host, targetPort: port, firstBody: firstBody)
            return
        }

        // —— LayerMinus 路径：构造签名请求，目的连 entry(或 egress) 80 端口，首包为 JSON 请求
        let entryHost = layerMinus.getRandomEntryNodes()?.ip_addr ?? egress.ip_addr
        let message = layerMinus.makeSocksRequest(host: host, port: port, body: firstBody.base64EncodedString(), command: "CONNECT")
        let messageData = message.data(using: .utf8)!
        let account = layerMinus.keystoreManager.addresses![0]

        Task.detached { [weak self] in
            guard let self = self else { return }
            do {
                let sig = try await self.layerMinus.web3.personal.signPersonalMessage(message: messageData, from: account, password: "")
                if let fn = self.layerMinus.javascriptContext.objectForKeyedSubscript("json_sign_message"),
                   let ret = fn.call(withArguments: [message, "0x\(sig.toHexString())"]) {
                    let cmd = ret.toString()!
                    let pre = self.layerMinus.createValidatorData(node: egress, responseData: cmd)
                    let request = self.layerMinus.makeRequest(host: entryHost, data: pre)
                    let b64 = request.data(using: .utf8)!.base64EncodedString()

                    let connectInfo = "origin=\(host):\(port) entry=\(entryHost) egress=\(egress.ip_addr)"
                    let bridge = LayerMinusBridgeNIO(
                        id: self.id,
                        targetHost: entryHost,
                        targetPort: 80,
                        verbose: self.verbose,
                        connectInfo: connectInfo,
                        onClosed: { [weak self] _ in self?.onClosed?(self?.id ?? 0) }
                    )
                    bridge.attachDownstream(ctx.channel)
                    bridge.markHandoffNow()
                    bridge.start(withFirstBody: b64)

                    // 交给 Bridge 管道后移除自身
                    ctx.pipeline.removeHandler(self, promise: nil)
                } else {
                    self.log("LM sign js bridge failed, fallback DIRECT")
                    startDirectBridge(connectInfo: "LM sign/js failed -> DIRECT", targetHost: host, targetPort: port, firstBody: firstBody)
                }
            } catch {
                self.log("LM sign error: \(error), fallback DIRECT")
                startDirectBridge(connectInfo: "LM sign error -> DIRECT", targetHost: host, targetPort: port, firstBody: firstBody)
            }
        }
    }

    // MARK: - 小工具（HTTP/SOCKS 回复、规则、URL 处理）

    private func writeString(_ ctx: ChannelHandlerContext, _ s: String) {
        var b = ctx.channel.allocator.buffer(capacity: s.utf8.count)
        b.writeString(s)
        ctx.writeAndFlush(b, promise: nil)
    }
    private func writeData(_ ctx: ChannelHandlerContext, _ d: Data) {
        var b = ctx.channel.allocator.buffer(capacity: d.count)
        b.writeBytes(d)
        ctx.writeAndFlush(b, promise: nil)
    }

    private func sendSocksReply(_ ctx: ChannelHandlerContext, _ rep: UInt8) {
        var b = ctx.channel.allocator.buffer(capacity: 10)
        b.writeBytes([0x05, rep, 0x00, 0x01, 0,0,0,0, 0,0])
        ctx.writeAndFlush(b, promise: nil)
    }

    private func blockHTTPAndClose(_ ctx: ChannelHandlerContext, status: String, reason: String) {
        let body = "Blocked: \(reason)\n"
        let hdr = "HTTP/1.1 \(status)\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: \(body.utf8.count)\r\n\r\n"
        writeData(ctx, Data((hdr + body).utf8))
        close(context: ctx, reason: "http_block: \(reason)")
    }

    private func blockHTTPForbiddenAndClose(_ ctx: ChannelHandlerContext, _ reason: String) {
        writeString(ctx, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        close(context: ctx, reason: "blocked: \(reason)")
    }

    private func blockSocksAndClose(_ ctx: ChannelHandlerContext, reason: String) {
        var b = ctx.channel.allocator.buffer(capacity: 10)
        b.writeBytes([0x05, 0x02, 0x00, 0x01, 0,0,0,0, 0,0])
        ctx.writeAndFlush(b, promise: nil)
        close(context: ctx, reason: "blocked: \(reason)")
    }

    private func splitHostPort(_ hostPort: String, defaultPort: Int) -> (host: String, port: Int) {
        if let idx = hostPort.lastIndex(of: ":"), idx < hostPort.endIndex {
            let h = String(hostPort[..<idx])
            let pStr = String(hostPort[hostPort.index(after: idx)...])
            if let p = Int(pStr), p > 0 && p < 65536 { return (h, p) }
        }
        return (hostPort, defaultPort)
    }

    private func normalizeAbsoluteOrOriginPath(rawPath: String, hostHeader: String) -> (String, Int, String) {
        var host = hostHeader, port = 80, path = rawPath
        if rawPath.hasPrefix("http://") || rawPath.hasPrefix("https://") {
            let isHTTPS = rawPath.hasPrefix("https://"); port = isHTTPS ? 443 : 80
            let schemeEnd = rawPath.index(rawPath.startIndex, offsetBy: isHTTPS ? 8 : 7)
            let after = rawPath[schemeEnd...]
            if let slash = after.firstIndex(of: "/") {
                let hp = String(after[..<slash])
                let tail = String(after[slash...])
                let sp = splitHostPort(hp, defaultPort: port)
                host = sp.host; port = sp.port; path = tail.isEmpty ? "/" : tail
            } else {
                let hp = String(after)
                let sp = splitHostPort(hp, defaultPort: port)
                host = sp.host; port = sp.port; path = "/"
            }
        } else {
            let sp = splitHostPort(hostHeader, defaultPort: 80)
            host = sp.host; port = sp.port
        }
        if path.isEmpty { path = "/" }
        return (host, port, path)
    }

    // —— 规则判定：与旧类保持一致（这里示例沿用你项目里的 Allowlist / AdBlacklist）
    @inline(__always)
    private func shouldBlock(host: String) -> Bool { AdBlacklist.matches(host) }

    @inline(__always)
    private func shouldDirect(host: String) -> Bool {
        if Allowlist.matches(host) { return true }
        if let ip = resolveFirstIPv4(host), Allowlist.matches(ip) { return true }
        return false
    }

    private func resolveFirstIPv4(_ host: String) -> String? {
        var hints = addrinfo(ai_flags: AI_ADDRCONFIG, ai_family: AF_INET,
                             ai_socktype: SOCK_STREAM, ai_protocol: IPPROTO_TCP,
                             ai_addrlen: 0, ai_canonname: nil, ai_addr: nil, ai_next: nil)
        var res: UnsafeMutablePointer<addrinfo>?
        let rc = getaddrinfo(host, nil, &hints, &res)
        guard rc == 0, let first = res else { return nil }
        defer { freeaddrinfo(res) }
        var addr = first.pointee.ai_addr.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { $0.pointee }
        var buf = [CChar](repeating: 0, count: Int(INET_ADDRSTRLEN))
        inet_ntop(AF_INET, &addr.sin_addr, &buf, socklen_t(INET_ADDRSTRLEN))
        return String(cString: buf)
    }

    // MARK: - Log
    private func log(_ s: @autoclosure () -> String) {
        #if DEBUG
        NSLog("[ServerConnectionNIO] #\(id) %@", s())
        #endif
    }
}
