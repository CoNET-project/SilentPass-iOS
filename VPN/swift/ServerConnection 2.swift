import Foundation
import Network

/// 单个 SOCKS5 会话（直连上游）
final class ServerConnection {

    // MARK: - 基本属性
    private let client: NWConnection
    private let listenPort: UInt16
    private let id: UInt64
    private let onClose: ((ServerConnection) -> Void)?

    private let queue: DispatchQueue
    private var closed = false

    // 上游直连
    private var upstream: NWConnection?

    // 握手/转发状态
    private var clientReadEOF = false
    private var upstreamReadEOF = false
    private var didReplySuccess = false

    // MARK: - 超时
    private let connectTimeoutSec: TimeInterval = 10
    private let idleTimeoutSec: TimeInterval = 60
    private var connectDeadline: DispatchSourceTimer?
    private var idleTimer: DispatchSourceTimer?

    // 目的端口白名单
    private let allowedPorts: Set<UInt16>? = nil

    // MARK: - DNS 直连解析器（过滤 198.18/15 与私网）
    enum DirectDNSError: Error { case timeout, badResponse }
    
    
    var connectTitle = ""
    struct DirectDNSResolver {
        /// 解析首个可路由的 IPv4 地址
        static func resolveIPv4(
            host: String,
            serverIPv4: String = "1.1.1.1",
            timeout: TimeInterval = 2.0
        ) async throws -> IPv4Address {

            // 构造 DNS 报文: Header(12) + Question
            let txid = UInt16.random(in: 0...UInt16.max)
            var q = Data()
            q.append(contentsOf: [UInt8(txid >> 8), UInt8(txid & 0xFF)]) // ID
            q.append(0x01); q.append(0x00) // RD=1
            q.append(0x00); q.append(0x01) // QDCOUNT=1
            q.append(0x00); q.append(0x00) // ANCOUNT=0
            q.append(0x00); q.append(0x00) // NSCOUNT=0
            q.append(0x00); q.append(0x00) // ARCOUNT=0
            for label in host.split(separator: ".") {
                guard let lb = label.data(using: .utf8), lb.count > 0, lb.count < 64 else {
                    throw DirectDNSError.badResponse
                }
                q.append(UInt8(lb.count)); q.append(lb)
            }
            q.append(0x00)
            q.append(0x00); q.append(0x01) // QTYPE=A
            q.append(0x00); q.append(0x01) // QCLASS=IN

            let params = makeDirectUDPParameters()
            let conn = NWConnection(host: NWEndpoint.Host(serverIPv4), port: 53, using: params)

            // ready
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                conn.stateUpdateHandler = { st in
                    switch st {
                    case .ready: cont.resume()
                    case .failed(let e): cont.resume(throwing: e)
                    case .waiting(let e): cont.resume(throwing: e)
                    default: break
                    }
                }
                conn.start(queue: .global(qos: .userInitiated))
            }

            // 发送
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                conn.send(content: q, completion: .contentProcessed { e in
                    if let e { cont.resume(throwing: e) } else { cont.resume() }
                })
            }

            // 接收
            let data = try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Data, Error>) in
                conn.receiveMessage { data, _, _, error in
                    if let error { cont.resume(throwing: error); return }
                    guard let data else { cont.resume(throwing: DirectDNSError.timeout); return }
                    cont.resume(returning: data)
                }
            }
            conn.cancel()

            // 解析应答
            guard data.count >= 12 else { throw DirectDNSError.badResponse }
            let rxid = (UInt16(data[0]) << 8) | UInt16(data[1])
            guard rxid == txid else { throw DirectDNSError.badResponse }
            let rcode = data[3] & 0x0F
            guard rcode == 0 else { throw DirectDNSError.badResponse }

            let qd = (Int(data[4]) << 8) | Int(data[5])
            var an = (Int(data[6]) << 8) | Int(data[7])
            var i = 12

            // 跳过 Question
            for _ in 0..<qd {
                guard i < data.count else { throw DirectDNSError.badResponse }
                while i < data.count, data[i] != 0 {
                    let l = Int(data[i]); i += 1 + l
                }
                i += 1 /*root*/ + 4 /*QTYPE/QCLASS*/
            }

            // 读取 Answers
            while an > 0, i + 12 <= data.count {
                let type = (Int(data[i+2]) << 8) | Int(data[i+3])
                let cls  = (Int(data[i+4]) << 8) | Int(data[i+5])
                let rdlen = (Int(data[i+10]) << 8) | Int(data[i+11])
                i += 12
                guard i + rdlen <= data.count else { throw DirectDNSError.badResponse }
                if type == 1, cls == 1, rdlen == 4 {
                    let a = IPv4Address(data[i..<(i+4)])!
                    if isRoutablePublicIPv4(a) { return a }
                }
                i += rdlen
                an -= 1
            }
            throw DirectDNSError.badResponse
        }

        private static func isRoutablePublicIPv4(_ ip: IPv4Address) -> Bool {
            let b = [UInt8](ip.rawValue)
            let isPrivate =
                b[0] == 10 || (b[0] == 172 && (b[1] >= 16 && b[1] <= 31)) ||
                (b[0] == 192 && b[1] == 168)
            let isLoopback = b[0] == 127
            let isLinkLocal = (b[0] == 169 && b[1] == 254)
            let isBenchmark = (b[0] == 198 && (b[1] & 0xFE) == 18) // 198.18/15
            let isTestNet =
                (b[0] == 192 && b[1] == 0 && b[2] == 2) ||
                (b[0] == 198 && b[1] == 51 && b[2] == 100) ||
                (b[0] == 203 && b[1] == 0 && b[2] == 113)
            let isMulticast = (b[0] >= 224 && b[0] <= 239)
            let isReservedHi = (b[0] >= 240)
            return !(isPrivate || isLoopback || isLinkLocal || isBenchmark || isTestNet || isMulticast || isReservedHi)
        }

        private static func makeDirectUDPParameters() -> NWParameters {
            let p = NWParameters.udp
            // 允许所有接口类型，让系统选择最佳路径
            // 不设置 prohibitedInterfaceTypes
            if #available(iOS 15.0, macOS 12.0, *) {
                p.preferNoProxies = true
            }
            return p
        }
    }

    // MARK: - 生命周期
    init(client: NWConnection, listenPort: UInt16, id: UInt64, onClose: ((ServerConnection) -> Void)? = nil) {
        self.client = client
        self.listenPort = listenPort
        self.id = id
        self.onClose = onClose
        self.queue = DispatchQueue(label: "socks5.connection.\(id)", qos: .userInitiated)
    }

    deinit { NSLog("Socks5Connection deinit") }

    // MARK: - 启动
    func start() {
        NSLog("SOCKS5 \(listenPort) connection \(id) will start")
        client.stateUpdateHandler = { [weak self] state in
            guard let self else { return }
            switch state {
            case .ready:
                NSLog("SOCKS5 \(self.listenPort) connection \(self.id) ready")
                self.bumpIdle()
                self.readHandshake()
            case .failed(let e):
                self.logNWError(prefix: "client failed", e)
                self.close()
            case .cancelled:
                self.close()
            default:
                break
            }
        }
        client.start(queue: queue)
    }

    /// 并发超限时用于快速拒绝
    func startQuickReject() {
        client.start(queue: queue)
        client.receive(minimumIncompleteLength: 2, maximumLength: 2) { [weak self] _, _, _, _ in
            guard let self else { return }
            self.client.send(content: Data([0x05, 0xFF]), completion: .contentProcessed { _ in
                self.close()
            })
        }
    }

    // MARK: - SOCKS 握手
    private func readHandshake() {
        // 读 VER + NMETHODS
        client.receive(minimumIncompleteLength: 2, maximumLength: 2) { [weak self] head, _, _, err in
            guard let self else { return }
            self.bumpIdle()
            if let err { self.fail(err); return }
            guard let h = head, h.count == 2, h[0] == 0x05 else { self.close(); return }
            let n = Int(h[1])

            // 读 METHODS
            self.client.receive(minimumIncompleteLength: n, maximumLength: n) { [weak self] methods, _, _, err in
                guard let self else { return }
                self.bumpIdle()
                if let err { self.fail(err); return }
                guard methods?.count == n else { self.close(); return }

                // 选择无认证
                self.client.send(content: Data([0x05, 0x00]), completion: .contentProcessed { sendErr in
                    if let sendErr { self.fail(sendErr); return }
                    NSLog("SOCKS5 handshake ok, no-auth selected")
                    self.readRequest()
                })
            }
        }
    }

    // MARK: - 解析请求
    private func readRequest() {
        // VER/CMD/RSV/ATYP
        client.receive(minimumIncompleteLength: 4, maximumLength: 4) { [weak self] header, _, _, err in
            guard let self else { return }
            self.bumpIdle()
            if let err { self.fail(err); return }
            guard let h = header, h.count == 4, h[0] == 0x05 else { self.close(); return }

            let cmd = h[1]
            let atyp = h[3]

            guard cmd == 0x01 else { // CONNECT only
                self.reply(rep: 0x07)
                self.close()
                return
            }
            self.readDst(atyp: atyp)
        }
    }

    private func readDst(atyp: UInt8) {
        switch atyp {
        case 0x01: // IPv4 + PORT
            client.receive(minimumIncompleteLength: 6, maximumLength: 6) { [weak self] d, _, _, err in
                guard let self else { return }
                self.bumpIdle()
                if let err { self.fail(err); return }
                guard let d, d.count == 6 else { self.close(); return }
                let host = d[0...3].map { String($0) }.joined(separator: ".")
                let port = UInt16(d[4]) << 8 | UInt16(d[5])
                self.connectTitle = "Connect to \(host):\(port)"
                self.handleConnect(host: host, port: port, isDomain: false)
            }

        case 0x03: // DOMAIN: LEN + NAME + PORT
            client.receive(minimumIncompleteLength: 1, maximumLength: 1) { [weak self] lenD, _, _, err in
                guard let self else { return }
                self.bumpIdle()
                if let err { self.fail(err); return }
                guard let lenD, lenD.count == 1 else { self.close(); return }
                let n = Int(lenD[0])

                self.client.receive(minimumIncompleteLength: n + 2, maximumLength: n + 2) { [weak self] rest, _, _, err in
                    guard let self else { return }
                    self.bumpIdle()
                    if let err { self.fail(err); return }
                    guard let rest, rest.count == n + 2 else { self.close(); return }
                    let name = String(decoding: rest[0..<n], as: UTF8.self)
                    let port = UInt16(rest[n]) << 8 | UInt16(rest[n+1])
                    self.connectTitle = "Connect to \(name):\(port)"
                    self.handleConnect(host: name, port: port, isDomain: true)
                }
            }

        case 0x04: // IPv6 + PORT
            client.receive(minimumIncompleteLength: 18, maximumLength: 18) { [weak self] d, _, _, err in
                guard let self else { return }
                self.bumpIdle()
                if let err { self.fail(err); return }
                guard let d, d.count == 18 else { self.close(); return }
                let ip6 = Self.ipv6String(from: Array(d[0..<16]))
                let port = UInt16(d[16]) << 8 | UInt16(d[17])
                self.handleConnect(host: ip6, port: port, isDomain: false)
            }

        default:
            reply(rep: 0x08) // Address type not supported
            close()
        }
    }

    private func handleConnect(host: String, port: UInt16, isDomain: Bool) {
        NSLog("SOCKS5 CONNECT to \(host):\(port)")
        guardPortOrReject(port) {
            // Simply use system resolver for all connections
            // This avoids potential issues with custom DNS resolution
            connectUpstream(host: NWEndpoint.Host(host), port: NWEndpoint.Port(rawValue: port)!)
        }
    }

    private func guardPortOrReject(_ port: UInt16, _ cont: () -> Void) {
        if let allow = allowedPorts {
            if allow.contains(port) { cont() } else { reply(rep: 0x07); close() }
        } else {
            cont() // 不限端口
        }
    }

    // MARK: - 建立上游连接（先回 0x00 再开始转发）
    private func connectUpstream(host: NWEndpoint.Host, port: NWEndpoint.Port) {
        let params = Self.makeDirectTCPParameters()
        let up = NWConnection(host: host, port: port, using: params)
        self.upstream = up

        startConnectDeadline()

        up.stateUpdateHandler = { [weak self] st in
            guard let self = self else { return }
            switch st {
            case .ready:
                self.cancelConnectDeadline()
                NSLog("SOCKS5 \(self.connectTitle) Upstream ready -> reply 0x00 then begin relay")
                self.replyOKAndStartRelay()   // 先回成功
            case .failed(let err):
                self.fail(err, at: "up.state.failed")
            case .cancelled:
                self.close(reason: "up.cancelled")
            default:
                break
            }
        }
        // 与 client 使用同队列，简化串行化
        up.start(queue: self.queue)
    }

    private func replyOKAndStartRelay() {
        // VER=5, REP=0, RSV=0, ATYP=IPv4, BND=0.0.0.0:0
        let resp = Data([0x05, 0x00, 0x00, 0x01, 0,0,0,0, 0,0])
        didReplySuccess = true
        client.send(content: resp, completion: .contentProcessed { [weak self] _ in
            self?.beginRelay()
        })
    }

    private static func makeDirectTCPParameters() -> NWParameters {
        let p = NWParameters.tcp
        if let tcp = p.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
            tcp.connectionTimeout = 10
            tcp.enableKeepalive = true
            tcp.keepaliveIdle = 60
        }
        // 重要：不设置任何接口限制，让系统选择最佳路径
        // 不设置 prohibitedInterfaceTypes
        // 不设置 requiredInterfaceType
        
        // 在 macOS 12+ 和 iOS 15+ 上，确保不走系统代理
        if #available(iOS 15.0, macOS 12.0, *) {
            p.preferNoProxies = true
        }
        return p
    }

    // MARK: - 开始双向转发
    private func beginRelay() {
        readFromClient()
        readFromUpstream()
    }

    // 上游 -> 客户端
    private func readFromUpstream() {
        guard let up = upstream else { close(reason: "no-upstream"); return }

        up.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] data, _, isComplete, error in
            guard let self = self else { return }
            if let error = error {
                self.fail(error, at: "up.receive")
                return
            }
            if let data, !data.isEmpty {
                self.bumpIdle()
                self.client.send(content: data, completion: .contentProcessed { [weak self] sendErr in
                    if let sendErr { self?.fail(sendErr, at: "client.send(up->client)") }
                })
            }
            if isComplete {
                self.upstreamReadEOF = true
                // 告知客户端写端结束
                self.client.send(content: nil, contentContext: .finalMessage, isComplete: true, completion: .idempotent)
                self.maybeClose()
                return
            }
            self.readFromUpstream()
        }
    }

    // 客户端 -> 上游
    private func readFromClient() {
        guard let up = upstream else { close(reason: "no-upstream"); return }

        client.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] data, _, isComplete, error in
            guard let self = self else { return }
            if let error = error {
                self.fail(error, at: "client.receive")
                return
            }
            if let data, !data.isEmpty {
                self.bumpIdle()
                up.send(content: data, completion: .contentProcessed { [weak self] sendErr in
                    if let sendErr { self?.fail(sendErr, at: "up.send(client->up)") }
                })
            }
            if isComplete {
                self.clientReadEOF = true
                up.send(content: nil, contentContext: .finalMessage, isComplete: true, completion: .idempotent)
                self.maybeClose()
                return
            }
            self.readFromClient()
        }
    }

    // MARK: - 定时器
    private func startConnectDeadline() {
        cancelConnectDeadline()
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + connectTimeoutSec)
        t.setEventHandler { [weak self] in
            guard let self else { return }
            NSLog("SOCKS5 \(self.connectTitle) connect timeout")
            if !self.didReplySuccess { self.reply(rep: 0x05) }
            self.close()
        }
        t.resume()
        connectDeadline = t
    }

    private func cancelConnectDeadline() {
        connectDeadline?.cancel()
        connectDeadline = nil
    }

    private func bumpIdle() {
        idleTimer?.cancel()
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + idleTimeoutSec)
        t.setEventHandler { [weak self] in self?.close() }
        t.resume()
        idleTimer = t
    }

    // MARK: - 工具
    private func reply(rep: UInt8) {
        var resp = Data([0x05, rep, 0x00, 0x01])
        resp.append(contentsOf: [0,0,0,0, 0,0])
        client.send(content: resp, completion: .contentProcessed { _ in })
    }

    private func fail(_ err: Error) {
        NSLog("SOCKS5 \(self.connectTitle) error: \(err)")
        if !didReplySuccess { reply(rep: 0x05) }
        close()
    }

    private func fail(_ err: Error, at site: String) {
        NSLog("SOCKS5 \(self.connectTitle) error @\(site): \(err)")
        if !didReplySuccess { reply(rep: 0x05) }
        close(reason: "fail:\(site)")
    }

    private func logNWError(prefix: String, _ err: NWError) {
        switch err {
        case .posix(let code) where code == .ECANCELED:
            NSLog("SOCKS5 \(self.connectTitle) \(prefix): ECANCELED")
        case .posix(let code) where code.rawValue == 54:
            NSLog("SOCKS5 \(self.connectTitle) \(prefix): Connection reset by peer")
        default:
            NSLog("SOCKS5 \(self.connectTitle) \(prefix): \(err)")
        }
    }

    private static func ipv6String(from bytes: [UInt8]) -> String {
        var s = ""
        for i in stride(from: 0, to: 16, by: 2) {
            let part = UInt16(bytes[i]) << 8 | UInt16(bytes[i+1])
            s += String(format: "%x", part)
            if i < 14 { s += ":" }
        }
        return s
    }

    private func maybeClose() {
        if clientReadEOF && upstreamReadEOF {
            close(reason: "both-EOF")
        }
    }

    private func close() {
        close(reason: "normal")
    }

    private func close(reason: String) {
        if closed { return }
        closed = true
        NSLog("SOCKS5 \(self.connectTitle) close: \(reason)")
        idleTimer?.cancel(); idleTimer = nil
        connectDeadline?.cancel(); connectDeadline = nil
        upstream?.cancel(); upstream = nil
        client.cancel()
        onClose?(self)
    }
}
