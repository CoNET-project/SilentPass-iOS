//
//  优化的SOCKS服务器配置
//  Server.swift
//

import Foundation
import Network

final class Server {
    let port: UInt16
    let maxConcurrent: Int
    let localOnly: Bool

    private var listener: NWListener?
    private let queue = DispatchQueue(label: "socks5.server.listener", qos: .userInitiated)
    private var nextID: UInt64 = 0
    private var connections: [UInt64: ServerConnection] = [:]
    
    // NEW: 增强的统计和监控
    private var connectionStats = ConnectionStats()
    private var statsTimer: DispatchSourceTimer?
    private let statsInterval: TimeInterval = 30.0
    
    private struct ConnectionStats {
        var totalConnections = 0
        var activeConnections = 0
        var rejectedConnections = 0
        var completedConnections = 0
        var startTime = Date()
        
        mutating func connectionStarted() {
            totalConnections += 1
            activeConnections += 1
        }
        
        mutating func connectionEnded() {
            activeConnections = max(0, activeConnections - 1)
            completedConnections += 1
        }
        
        mutating func connectionRejected() {
            rejectedConnections += 1
        }
    }

    // 优化的默认参数 - 配合VPN客户端的50并发限制
    init(port: UInt16, maxConcurrent: Int = 80, localOnly: Bool = true) {  // 增加到80
        self.port = port
        self.maxConcurrent = maxConcurrent
        self.localOnly = localOnly
    }

    deinit {
        stop()
    }

    func start() {
        queue.async { [weak self] in
            guard let self, self.listener == nil else { return }

            do {
                let tcp = NWParameters.tcp
                tcp.requiredInterfaceType = .loopback
                
                // 优化TCP参数
                if let tcpOpt = tcp.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
                    tcpOpt.connectionTimeout = 5        // 减少连接超时
                    tcpOpt.enableKeepalive = true
                    tcpOpt.keepaliveIdle = 30           // 减少keepalive间隔
                    tcpOpt.keepaliveInterval = 10
                    tcpOpt.noDelay = true               // 启用TCP_NODELAY
                }

                let portObj = NWEndpoint.Port(rawValue: self.port)!
                let l = try NWListener(using: tcp, on: portObj)
                self.listener = l

                NSLog("SOCKS5 Server starting on 127.0.0.1:\(self.port) (maxConcurrent: \(self.maxConcurrent))")
                
                // 启动统计定时器
                self.startStatsTimer()

                l.stateUpdateHandler = { [weak self] state in
                    guard let self else { return }
                    switch state {
                    case .ready:
                        NSLog("SOCKS5 Server ready on 127.0.0.1:\(self.port)")
                    case .failed(let e):
                        NSLog("SOCKS5 Server failed: \(e)")
                        self.stop()
                    case .cancelled:
                        NSLog("SOCKS5 Server cancelled")
                    default:
                        break
                    }
                }

                l.newConnectionHandler = { [weak self] nw in
                    guard let self else { return }
                    self.queue.async {
                        if self.localOnly, !Self.isLoopbackPeer(nw.endpoint) {
                            nw.cancel()
                            self.connectionStats.connectionRejected()
                            return
                        }
                        
                        if self.connections.count >= self.maxConcurrent {
                            // 软拒绝：快速回复错误而不是直接关闭
                            let id = self.nextAndIncID()
                            self.connectionStats.connectionRejected()
                            let tmp = ServerConnection(
                                client: nw, listenPort: self.port, id: id,
                                onClose: { [weak self] _ in
                                    // 不计入active连接数
                                }
                            )
                            tmp.startQuickReject()
                            
                            // 每100个拒绝记录一次日志
                            if self.connectionStats.rejectedConnections % 100 == 0 {
                                NSLog("SOCKS5 rejected \(self.connectionStats.rejectedConnections) connections due to limit")
                            }
                            return
                        }
                        
                        let id = self.nextAndIncID()
                        self.connectionStats.connectionStarted()
                        
                        let conn = ServerConnection(
                            client: nw, listenPort: self.port, id: id,
                            onClose: { [weak self] _ in
                                guard let self else { return }
                                self.queue.async {
                                    self.connections[id] = nil
                                    self.connectionStats.connectionEnded()
                                    
                                    // 只在连接数变化显著时记录日志
                                    if self.connections.count % 10 == 0 || self.connections.count < 10 {
                                        NSLog("SOCKS5 conn closed, active=\(self.connections.count)")
                                    }
                                }
                            }
                        )
                        self.connections[id] = conn
                        
                        // 减少日志频率
                        if self.connections.count % 10 == 0 || self.connections.count < 10 {
                            NSLog("SOCKS5 new conn, active=\(self.connections.count)")
                        }
                        
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
    
    // NEW: 统计定时器
    private func startStatsTimer() {
        statsTimer?.cancel()
        let timer = DispatchSource.makeTimerSource(queue: queue)
        timer.schedule(deadline: .now() + statsInterval, repeating: statsInterval)
        timer.setEventHandler { [weak self] in
            self?.printStats()
        }
        timer.resume()
        statsTimer = timer
    }
    
    private func printStats() {
        let uptime = Date().timeIntervalSince(connectionStats.startTime)
        let uptimeStr = String(format: "%02d:%02d:%02d",
                              Int(uptime) / 3600,
                              (Int(uptime) % 3600) / 60,
                              Int(uptime) % 60)
        
        let successRate = connectionStats.totalConnections > 0
            ? Double(connectionStats.completedConnections) / Double(connectionStats.totalConnections) * 100
            : 0
            
        NSLog("""
        === SOCKS5 Server Stats ===
        Uptime: \(uptimeStr)
        Connections: total=\(connectionStats.totalConnections) active=\(connectionStats.activeConnections)/\(maxConcurrent) completed=\(connectionStats.completedConnections) rejected=\(connectionStats.rejectedConnections)
        Success Rate: \(String(format: "%.1f", successRate))%
        =========================
        """)
    }

    func stop() {
        queue.async { [weak self] in
            guard let self else { return }
            
            NSLog("SOCKS5 Server stopping...")
            
            self.statsTimer?.cancel()
            self.statsTimer = nil
            
            self.listener?.cancel()
            self.listener = nil

            let conns = Array(self.connections.values)
            self.connections.removeAll(keepingCapacity: false)

            // 通知所有连接关闭
            conns.forEach { conn in
                // 如果有公共关闭方法，在此调用
            }
            
            NSLog("SOCKS5 Server stopped")
        }
    }

    private func nextAndIncID() -> UInt64 {
        let v = nextID
        nextID &+= 1
        return v
    }

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
            return true
        }
    }
}

// MARK: - 使用建议

/*
使用优化的SOCKS服务器：

1. 启动服务器时使用更高的并发限制：
```swift
let server = Server(port: 8888, maxConcurrent: 80, localOnly: true)
server.start()
```

2. 监控日志中的统计信息，了解：
   - 连接成功率
   - 拒绝连接数量
   - 活跃连接数

3. 根据实际情况调整参数：
   - 如果经常看到拒绝连接，可以增加maxConcurrent
   - 如果系统资源紧张，可以减少maxConcurrent
   - 监控VPN客户端的SOCKS连接池使用情况

4. 系统级优化：
```bash
# 增加文件描述符限制
ulimit -n 4096

# 检查端口使用情况
lsof -i :8888 | wc -l
```

期望效果：
- SOCKS服务器能处理更多并发连接
- VPN客户端的连接池和队列机制避免超载
- 更好的错误处理和统计监控
- 减少连接超时和失败
*/
