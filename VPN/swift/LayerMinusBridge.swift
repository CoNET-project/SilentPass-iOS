import Foundation
import Network
import os

enum L {
    static func lm(_ s: @autoclosure () -> String) { print("[LayerMinusBridge] \(s())") }
    static func sc(_ s: @autoclosure () -> String) { print("[ServerConnection] \(s())") }
}

public final class LayerMinusBridge {

    public let id: UInt64
    private let client: NWConnection
    private let targetHost: String
    private let targetPort: Int
    private let verbose: Bool
    private let onClosed: ((UInt64) -> Void)?

    private let queue: DispatchQueue
    private var upstream: NWConnection?
    private var closed = false

    public init(
        id: UInt64,
        client: NWConnection,
        targetHost: String,
        targetPort: Int,
        verbose: Bool = true,
        onClosed: ((UInt64) -> Void)? = nil
    ) {
        self.id = id
        self.client = client
        self.targetHost = targetHost
        self.targetPort = targetPort
        self.verbose = verbose
        self.onClosed = onClosed
        self.queue = DispatchQueue(label: "LayerMinusBridge.\(id)", qos: .userInitiated)
        
        // 简单的生命周期日志
        log("🟢 CREATED LayerMinusBridge #\(id) for \(targetHost):\(targetPort)")
    }
    
    deinit {
        log("🔴 DESTROYED LayerMinusBridge #\(id)")
        if !closed {
            print("⚠️ WARNING: LayerMinusBridge #\(id) destroyed without proper closing!")
        }
    }

    @inline(__always)
    private func log(_ msg: String) {
        //NSLog("[LayerMinusBridge \(id)] %@", msg)
    }

    public func start(withFirstBody firstBodyBase64: String) {
        queue.async { [weak self] in
            guard let self = self else { return }
            
            self.log("start -> \(self.targetHost):\(self.targetPort), firstBody(Base64) len=\(firstBodyBase64.count)")
            
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
        
        // 配置 TCP 参数
        if let tcp = params.defaultProtocolStack.transportProtocol as? NWProtocolTCP.Options {
            tcp.noDelay = true
            tcp.enableKeepalive = true
            tcp.keepaliveIdle = 10
        }

        log("Connecting to upstream \(targetHost):\(targetPort)")
        
        let up = NWConnection(host: host, port: port, using: params)
        upstream = up

        up.stateUpdateHandler = { [weak self] st in
            guard let self = self else { return }
            switch st {
            case .ready:
                self.log("upstream ready to \(self.targetHost):\(self.targetPort)")
                
                // 发送首包到上游
                if !firstBody.isEmpty {
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

    private func pumpClientToUpstream() {
        if closed { return }
        
        client.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in
            guard let self = self else { return }
            if self.closed { return }

            if let err = err {
                self.log("client recv err: \(err)")
                self.cancel(reason: "client recv err")
                return
            }
            
            if let d = data, !d.isEmpty {
                self.log("recv from client: \(d.count)B")
                self.sendToUpstream(d, remark: "c->u")
            }
            
            if isComplete {
                self.log("client EOF")
                // 关闭上游的写入
                self.upstream?.send(content: nil, completion: .contentProcessed({ _ in }))
                // 重要：也要取消整个连接
                self.cancel(reason: "client EOF")
                return
            }
            
            // 继续接收
            self.pumpClientToUpstream()
        }
    }

    private func pumpUpstreamToClient() {
        if closed { return }
        
        upstream?.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) { [weak self] (data, _, isComplete, err) in
            guard let self = self else { return }
            if self.closed { return }

            if let err = err {
                self.log("upstream recv err: \(err)")
                self.cancel(reason: "upstream recv err")
                return
            }
            
            if let d = data, !d.isEmpty {
                self.log("recv from upstream: \(d.count)B")
                self.sendToClient(d, remark: "u->c")
            }
            
            if isComplete {
                self.log("upstream EOF")
                // 关闭客户端的写入
                self.client.send(content: nil, completion: .contentProcessed({ _ in }))
                // 重要：也要取消整个连接
                self.cancel(reason: "upstream EOF")
                return
            }
            
            // 继续接收
            self.pumpUpstreamToClient()
        }
    }

    private func sendToUpstream(_ data: Data, remark: String) {
        guard let up = upstream, !closed else {
            log("Cannot send to upstream: upstream=\(upstream != nil), closed=\(closed)")
            return
        }
        
        log("send \(remark) \(data.count)B -> upstream")
        
        up.send(content: data, completion: .contentProcessed({ [weak self] err in
            if let err = err {
                self?.log("upstream send err: \(err)")
                self?.cancel(reason: "upstream send err")
            } else {
                self?.log("sent \(remark) successfully")
            }
        }))
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
                self?.cancel(reason: "client send err")
            } else {
                self?.log("sent \(remark) successfully")
            }
        }))
    }
}
