import Foundation
import Network
import NetworkExtension
import os.log

final class ServerBridge {

    // === 入参（与现有保持一致） ===
    let sendData: Data
    let host: String
    let port: Int
    unowned let proxyConnect: ServerConnection
    let remoteHost: String
    let remotePort: Int
    let header: String
    let base64Body: String
    let directConnect: Bool

    // === 运行时 ===
    private var uplink: NWTCPConnection?
    private let mtu = 64 * 1024
    private var isStopped = false

    // 由 PacketTunnelProvider 注入
    static weak var packetProvider: NEPacketTunnelProvider?

    init(sendData: Data,
         host: String, port: Int,
         proxyConnect: ServerConnection,
         remoteHost: String, remotePort: Int,
         header: String, base64Body: String,
         directConnect: Bool)
    {
        self.sendData = sendData
        self.host = host
        self.port = port
        self.proxyConnect = proxyConnect
        self.remoteHost = remoteHost
        self.remotePort = remotePort
        self.header = header
        self.base64Body = base64Body
        self.directConnect = directConnect
    }

    deinit { stop(error: nil) }

    // 由 PacketTunnelProvider 注入
    static func configure(with provider: NEPacketTunnelProvider) {
        ServerBridge.packetProvider = provider
        NSLog("ServerBridge configured provider = \(provider)")
    }

    func start() {
        if directConnect {
            startDirect()
        } else {
            startViaEntryNode()
        }
    }

    // MARK: - 直连：用于调试
    private func startDirect() {
        NSLog("ServerBridge call start() DIRECT \(remoteHost):\(remotePort)")

        guard let provider = ServerBridge.packetProvider else {
            NSLog("ServerBridge DIRECT error: packetProvider is nil")
            stop(error: nil)
            return
        }

        let ep = NWHostEndpoint(hostname: remoteHost, port: "\(remotePort)")
        // App 侧已经 TLS，因此这里必须 enableTLS=false
        let conn = provider.createTCPConnection(to: ep, enableTLS: false, tlsParameters: nil, delegate: nil)
        self.uplink = conn

        // 1) 先把“首包”(如 TLS ClientHello) 排队写出；失败会回调错误
        if let first = Data(base64Encoded: base64Body, options: [.ignoreUnknownCharacters]), !first.isEmpty {
            conn.write(first) { [weak self] err in
                if let err = err {
                    NSLog("ServerBridge DIRECT write-first error: \(err)")
                    self?.stop(error: err)
                } else {
                    NSLog("ServerBridge DIRECT wrote first: \(first.count) bytes")
                }
            }
        }

        // 2) 立刻启动“双向泵”；连接尚未建立时系统会排队，连上后回调触发
        pumpDownToUp()
        pumpUpToDown()
    }

    // MARK: - 入口节点（预留给 LayerMinus；当前直接发 sendData）
    private func startViaEntryNode() {
        NSLog("ServerBridge call start() VIA ENTRY \(host):\(port)")

        guard let provider = ServerBridge.packetProvider else {
            NSLog("ServerBridge ENTRY error: packetProvider is nil")
            stop(error: nil)
            return
        }

        let ep = NWHostEndpoint(hostname: host, port: "\(port)")
        let conn = provider.createTCPConnection(to: ep, enableTLS: false, tlsParameters: nil, delegate: nil)
        self.uplink = conn

        if !sendData.isEmpty {
            conn.write(sendData) { [weak self] err in
                if let err = err {
                    NSLog("ServerBridge ENTRY write-first error: \(err)")
                    self?.stop(error: err)
                } else {
                    NSLog("ServerBridge ENTRY wrote first: \(self?.sendData.count ?? 0) bytes")
                }
            }
        }

        pumpDownToUp()
        pumpUpToDown()
    }

    // MARK: - 泵：远端 -> 客户端
    private func pumpUpToDown() {
        guard let up = uplink, !isStopped else { return }
        up.readMinimumLength(1, maximumLength: mtu) { [weak self] data, err in
            guard let self = self else { return }
            if let err = err {
                NSLog("ServerBridge up→down read error: \(err)")
                self.stop(error: err)
                return
            }
            guard let d = data, !d.isEmpty else {
                // EOF
                self.stop(error: nil)
                return
            }
            self.proxyConnect.connection.send(content: d, completion: .contentProcessed { _ in })
            self.pumpUpToDown()
        }
    }

    // MARK: - 泵：客户端 -> 远端
    private func pumpDownToUp() {
        guard !isStopped else { return }
        proxyConnect.connection.receive(minimumIncompleteLength: 1, maximumLength: mtu) { [weak self] data, _, isComplete, err in
            guard let self = self else { return }
            if let err = err {
                NSLog("ServerBridge down→up recv error: \(err)")
                self.stop(error: err)
                return
            }
            if isComplete {
                self.stop(error: nil)
                return
            }
            guard let d = data, !d.isEmpty else {
                self.pumpDownToUp()
                return
            }
            self.uplink?.write(d) { [weak self] werr in
                if let werr = werr {
                    NSLog("ServerBridge down→up write error: \(werr)")
                    self?.stop(error: werr)
                    return
                }
                self?.pumpDownToUp()
            }
        }
    }

    // MARK: - 停止
    func stop(error: Error?) {
        guard !isStopped else { return }
        isStopped = true
        if let err = error {
            NSLog("ServerBridge stop with error: \(err)")
        } else {
            NSLog("ServerBridge stop")
        }
        uplink?.cancel()
        uplink = nil
        // 不主动关闭 proxyConnect.connection；交给上层 ServerConnection
    }
}
