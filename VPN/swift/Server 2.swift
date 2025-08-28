import Foundation
import Network

final class Server {
    // Public config
    let bindHost: String
    let port: NWEndpoint.Port
    let portLabel: String

    let maxActiveConnections: Int
    let verbose: Bool

    private(set) var listener: NWListener?
    fileprivate let queue: DispatchQueue
    fileprivate var conns: [UInt64: ServerConnection] = [:]
    private var nextID: UInt64 = 0

    private var totalUp = 0
    private var totalDown = 0
    private var tickUp = 0
    private var tickDown = 0
    private var statTimer: DispatchSourceTimer?
    private let layerMinus = LayerMinus()
    
    private var didCutoverToLayerMinus = false
    
    /// 关闭所有非 LayerMinus 的旧连接，只执行一次
    private func cutoverToLayerMinusIfNeeded(triggeringConn: ServerConnection) {
        guard !didCutoverToLayerMinus else { return }
        guard triggeringConn.isLayerMinusRouted else { return } // 只有当触发连接是 LM 才执行

        // LayerMinus 已就绪才切换
        guard let eg = self.layerMinus.getRandomEgressNodes(),
              let en = self.layerMinus.getRandomEntryNodes(),
              !eg.isEmpty, !en.isEmpty else { return }

        didCutoverToLayerMinus = true
        self.log("⚡️ LayerMinus cutover: closing non-LM connections, keeping LM connections")

        let victims = self.conns.filter { !$0.value.isLayerMinusRouted }
        for (id, conn) in victims {
            self.log("Cutover: shutting down non-LM connection #\(id)")
            conn.shutdown(reason: "LayerMinus cutover")
        }
        self.log("Cutover done: victims=\(victims.count), now conns=\(self.conns.count)")
    }
    
    
    init(host: String = "127.0.0.1",
         port: UInt16 = 8888,
         portLabel: String? = nil,
         queue: DispatchQueue = DispatchQueue(label: "socks5.server.queue"),
         maxActiveConnections: Int = 1024,
         verbose: Bool = true) {
        self.bindHost = host
        self.port = NWEndpoint.Port(rawValue: port)!
        self.portLabel = portLabel ?? "\(port)"
        self.queue = queue
        self.maxActiveConnections = maxActiveConnections
        self.verbose = verbose
    }

    deinit { stop() }

    func start() throws {
        // 幂等
        if listener != nil {
            log("SOCKS5 Server already running on \(bindHost):\(portLabel)")
            return
        }

        //log("SOCKS5 Server starting on \(bindHost):\(portLabel) \(entryNodes.count) \(egressNodes.count)")

        let tcp = NWProtocolTCP.Options()
        tcp.enableKeepalive = true
        tcp.noDelay = true
        let params = NWParameters(tls: nil, tcp: tcp)

        let lst = try NWListener(using: params, on: port)
        self.listener = lst

        lst.stateUpdateHandler = { [weak self] st in
            guard let self = self else { return }
            self.queue.async {
                switch st {
                case .ready:
                    self.log("SOCKS5 Server ready on \(self.bindHost):\(self.portLabel)")
                case .failed(let e):
                    self.log("Server failed: \(Self.describe(e))")
                case .cancelled:
                    self.log("Server cancelled")
                default:
                    break
                }
            }
        }

        lst.newConnectionHandler = { [weak self] nw in
            guard let self = self else { return }
            self.queue.async {
                if self.conns.count >= self.maxActiveConnections {
                    self.log("SOCKS5 refuse new conn: over limit \(self.maxActiveConnections)")
                    nw.cancel()
                    return
                }
                
                // 为每个新连接创建新的 ServerConnection 实例
                self.nextID &+= 1
                let id = self.nextID
                
               
                    
                    let conn = ServerConnection(
                        id: id,
                        connection: nw,
                        layerMinus: self.layerMinus,
                        onClosed: { [weak self] connectionId in
                            // 当 ServerConnection 关闭时，从字典中移除
                            self?.onConnectionClosed(id: connectionId)
                        }
                    )
                    
                    self.conns[id] = conn
                    
                    if self.verbose {
                        //self.log("SOCKS5 new conn #\(id), active=\(self.conns.count)")
                    }
                        
                        
                    conn.onRoutingDecided = { [weak self] conn in
                        self?.cutoverToLayerMinusIfNeeded(triggeringConn: conn)
                    }

                    conn.start()
                
                
                
            }
        }

        lst.start(queue: queue)
        startStatsTimer()
    }
    
    
    
    func layerMinusInit (
        privateKey: String, entryNodes: [Node], egressNodes: [Node]
    ) {
        self.layerMinus.startInVPN(privateKey: privateKey,
                                   entryNodes: entryNodes,
                                   egressNodes: egressNodes,
                                   port: 8888)
        self.log("layerMinusInit success privateKey = \(privateKey) entryNodes = \(entryNodes.count) egressNodes = \(egressNodes.count)")
    }

    func stop() {
        queue.async {
            self.statTimer?.cancel()
            self.statTimer = nil
            self.listener?.cancel()
            self.listener = nil
            
            // 关闭所有活动连接
            for (id, conn) in self.conns {
                self.log("Shutting down connection #\(id)")
                conn.shutdown(reason: "server stop")
            }
            self.conns.removeAll()
            
            self.log("SOCKS5 Server stopped")
        }
    }

    // 连接关闭回调
    func onConnectionClosed(id: UInt64) {
        queue.async { [weak self] in
            guard let self = self else { return }
            
            if self.conns.removeValue(forKey: id) != nil {
                self.log("Connection #\(id) closed and removed, active=\(self.conns.count)")
            }
        }
    }

    // 供 LayerMinus/ServerConnection 计流量（可按需调用）
    func addBytes(up: Int, down: Int) {
        totalUp &+= up; totalDown &+= down
        tickUp &+= up;  tickDown &+= down
    }

    // Logging & Error
    func log(_ s: String) {
        //NSLog("[ServerConnection] %@", s)
    }
    
    static func describe(_ err: Error) -> String {
        if let ne = err as? NWError {
            switch ne {
            case .posix(let c): return "POSIXErrorCode(rawValue: \(c.rawValue)): \(c)"
            case .dns(let c):   return "dns: \(c)"
            case .tls(let os):  return "tls: \(os)"
            @unknown default:   return String(describing: ne)
            }
        }
        return String(describing: err)
    }

    // Stats
    private func startStatsTimer() {
        let t = DispatchSource.makeTimerSource(queue: queue)
        t.schedule(deadline: .now() + .seconds(10), repeating: .seconds(10))
        t.setEventHandler { [weak self] in
            guard let self = self else { return }
            let up = self.tickUp, down = self.tickDown
            self.tickUp = 0; self.tickDown = 0
            let upKBps = String(format: "%.1f", Double(up) / 1024.0 / 10.0)
            let downKBps = String(format: "%.1f", Double(down) / 1024.0 / 10.0)
            self.log("SOCKS5 \(self.portLabel) stats: active=\(self.conns.count), 10s up=\(up)B(\(upKBps) KB/s), down=\(down)B(\(downKBps) KB/s), total up=\(self.totalUp)B, down=\(self.totalDown)B")
        }
        t.resume()
        self.statTimer = t
    }
}
