//
//  ServerNIO.swift
//  CoNETVPN1
//
//  Created by peter on 2025-09-17.
//

// ServerNIO.swift
import Foundation
import NIO
import NIOCore
import NIOTransportServices

/// 纯 NIO 版本的本地代理服务器：
/// - 127.0.0.1:port 上监听
/// - 每个下游 Channel -> 交给 ServerConnectionNIO
/// - ServerConnectionNIO 再对接 LayerMinusBridgeNIO 完成自动背压的上下游 pipe
final class ServerNIO {

    // MARK: Config
    private let bindHost: String
    private let port: Int
    private let maxActiveConnections: Int
    private let verbose: Bool

    // 你现有的 LayerMinus（保持 internal，避免 public 暴露冲突）
    public let layerMinus = LayerMinus()

    // MARK: Runtime
    private var group: NIOTSEventLoopGroup?
    private var serverChannel: Channel?
    private var nextID: UInt64 = 0

    // 活跃连接
    private var conns: [UInt64: ServerConnectionNIO] = [:]
    private let lock = NSLock()

    // 统计定时器（可选）
    private var statTimer: DispatchSourceTimer?

    // MARK: Init
    init(bindHost: String = "127.0.0.1",
         port: Int = 8888,
         maxActiveConnections: Int = 1024,
         verbose: Bool = true) {
        self.bindHost = bindHost
        self.port = port
        self.maxActiveConnections = maxActiveConnections
        self.verbose = verbose
    }

    deinit {
        stop()
    }

    // MARK: Public
    func start() {
        // 幂等防护
        if serverChannel != nil { log("NIO server already running on \(bindHost):\(port)"); return }

        let g = NIOTSEventLoopGroup()
        self.group = g

        let bootstrap = NIOTSListenerBootstrap(group: g)
            .childChannelOption(ChannelOptions.allowRemoteHalfClosure, value: true)
            .childChannelInitializer { [weak self] ch in
                guard let self = self else { return ch.eventLoop.makeSucceededFuture(()) }

                // 连接数上限
                self.lock.lock()
                let activeCount = self.conns.count
                self.lock.unlock()
                if activeCount >= self.maxActiveConnections {
                    self.log("refuse new conn: over limit \(self.maxActiveConnections)")
                    return ch.close()
                }

                // 分配连接 ID
                self.lock.lock()
                self.nextID &+= 1
                let id = self.nextID
                self.lock.unlock()

                // 创建 NIO 版 ServerConnection
                let conn = ServerConnectionNIO(
                    id: id,
                    channel: ch,
                    layerMinus: self.layerMinus,
                    verbose: self.verbose,
                    onClosed: { [weak self] cid in
                        self?.onConnectionClosed(id: cid)
                    }
                )

                self.lock.lock()
                self.conns[id] = conn
                self.lock.unlock()

                if self.verbose {
                    self.log("new conn #\(id), active=\(activeCount + 1)")
                }

                conn.start()
                return ch.eventLoop.makeSucceededFuture(())
            }

        do {
            self.serverChannel = try bootstrap.bind(host: bindHost, port: port).wait()
            log("NIO server ready on \(bindHost):\(port)")
            startStatsTimer()
        } catch {
            log("NIO server failed to bind: \(error)")
            stop() // 清理资源
        }
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
        // 停止统计
        statTimer?.cancel()
        statTimer = nil

        // 关闭监听
        if let ch = serverChannel {
            _ = ch.close()
            serverChannel = nil
        }

        // 清空连接引用（真实关闭在各自 ChannelInactive 内完成）
        lock.lock()
        conns.removeAll()
        lock.unlock()

        // 关闭事件循环组
        if let g = group {
            try? g.syncShutdownGracefully()
            group = nil
        }

        log("NIO server stopped")
    }

    // MARK: Private
    private func onConnectionClosed(id: UInt64) {
        lock.lock()
        let removed = conns.removeValue(forKey: id)
        let active = conns.count
        lock.unlock()

        if removed != nil, verbose {
            log("conn #\(id) closed, active=\(active)")
        }
    }

    private func startStatsTimer() {
        let t = DispatchSource.makeTimerSource(queue: .global(qos: .utility))
        t.schedule(deadline: .now() + .seconds(10), repeating: .seconds(10))
        t.setEventHandler { [weak self] in
            guard let self = self else { return }
            self.lock.lock()
            let active = self.conns.count
            self.lock.unlock()
            if self.verbose { self.log("stats: active=\(active)") }
        }
        t.resume()
        self.statTimer = t
    }

    @inline(__always)
    private func log(_ s: @autoclosure () -> String) {
        #if DEBUG
        NSLog("[ServerNIO] %@", s())
        #endif
    }
}
