import Foundation
import Network

/// SOCKS5 服务器监听器（线程安全：所有状态都在 self.queue 上操作）
final class Server {

    // MARK: - 配置
    let port: UInt16
    let maxConcurrent: Int
    let localOnly: Bool  // 仅允许从本机回环地址连入

    // MARK: - 内部状态（仅在 queue 上访问）
    private var listener: NWListener?
    private let queue = DispatchQueue(label: "socks5.server.listener", qos: .userInitiated)
    private var nextID: UInt64 = 0
    private var connections: [UInt64: ServerConnection] = [:]

    // MARK: - 生命周期
    init(port: UInt16, maxConcurrent: Int = 64, localOnly: Bool = true) {
        self.port = port
        self.maxConcurrent = maxConcurrent
        self.localOnly = localOnly
    }

    deinit { stop() }

    // MARK: - 启动/停止
    func start() {
        queue.async { [weak self] in
            guard let self, self.listener == nil else { return }

            do {
                let tcp = NWParameters.tcp
                // 只使用回环接口，避免路径被系统挑错
                tcp.requiredInterfaceType = .loopback

                if let tcpOpt = tcp.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
                    tcpOpt.connectionTimeout = 10
                    tcpOpt.enableKeepalive = true
                    tcpOpt.keepaliveIdle = 60
                }

                let portObj = NWEndpoint.Port(rawValue: self.port)!
                let l = try NWListener(using: tcp, on: portObj)
                self.listener = l

                NSLog("SOCKS5 Server starting on 127.0.0.1:\(self.port)")

                l.stateUpdateHandler = { [weak self] state in
                    guard let self else { return }
                    switch state {
                    case .ready:
                        NSLog("SOCKS5 Server ready on 127.0.0.1:\(self.port)")
                    case .failed(let e):
                        NSLog("SOCKS5 Server failed: \(e)")
                        self.stop()
                    case .cancelled:
                        break
                    default:
                        break
                    }
                }

                l.newConnectionHandler = { [weak self] nw in
                    guard let self else { return }
                    self.queue.async {
                        if self.localOnly, !Self.isLoopbackPeer(nw.endpoint) {
                            nw.cancel()
                            return
                        }
                        if self.connections.count >= self.maxConcurrent {
                            NSLog("SOCKS5 reach maxConcurrent=\(self.maxConcurrent), quick reject")
                            let id = self.nextAndIncID()
                            let tmp = ServerConnection(
                                client: nw, listenPort: self.port, id: id,
                                onClose: { [weak self] _ in
                                    self?.queue.async {
                                        NSLog("SOCKS5 conn closed, active=\(self?.connections.count ?? 0)")
                                    }
                                }
                            )
                            tmp.startQuickReject()
                            return
                        }
                        let id = self.nextAndIncID()
                        let conn = ServerConnection(
                            client: nw, listenPort: self.port, id: id,
                            onClose: { [weak self] _ in
                                guard let self else { return }
                                self.queue.async {
                                    self.connections[id] = nil
                                    NSLog("SOCKS5 conn closed, active=\(self.connections.count)")
                                }
                            }
                        )
                        self.connections[id] = conn
                        NSLog("SOCKS5 new conn, active=\(self.connections.count)")
                        conn.start()
                    }
                }

                l.start(queue: self.queue)

            } catch {
                NSLog("SOCKS5 Server create listener failed: \(error)")
                self.stop()
            }
        }
    }

    func stop() {
        queue.async { [weak self] in
            guard let self else { return }
            self.listener?.cancel()
            self.listener = nil

            // 拷贝一份连接列表，先清空表，再让连接各自收尾
            let conns = Array(self.connections.values)
            self.connections.removeAll(keepingCapacity: false)

            // 让连接自己在各自的队列里关闭（避免在本队列里直接 re-enter）
            conns.forEach { _ = $0 } // 如需外部关闭，可在 ServerConnection 增加公共 shutdown() 并在此调用
        }
    }

    // MARK: - 工具（仅在 queue 上调用）
    private func nextAndIncID() -> UInt64 {
        let v = nextID
        nextID &+= 1
        return v
    }

    /// 判断入站连接是否来自回环地址
    private static func isLoopbackPeer(_ ep: NWEndpoint) -> Bool {
        switch ep {
        case .hostPort(let host, _):
            let s = host.debugDescription.lowercased()
            if s == "localhost" { return true }
            if s == "::1" { return true }
            if s == "127.0.0.1" { return true }
            if s.hasPrefix("127.") { return true }
            if s.contains("::ffff:127.0.0.1") { return true }
            return false
        default:
            // 其他类型（如 service）通常来自本机，保守接受
            return true
        }
    }
    

}
