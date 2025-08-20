//
//  socks.swift
//  CoNETVPN
//
//  Created by peter on 2024-12-27.
//
import Network
import Foundation


class Socks5 {
    let client: ServerConnection
    var rfc1928: Rfc1928!
    var buffer: Data!
    var serverBridge : ServerBridge!
    var host = ""

    init(client: ServerConnection) {
        self.client = client

        // 先收首个报文（方法协商或直接是请求，由你的 Rfc1928 解析）
        self.client.connection.receive(minimumIncompleteLength: 1, maximumLength: self.client.MTU) { (data1, _, _, error) in
            if let error {
                self.client.stop(error: error)
                return print("Local Proxy Socks5 [\(client.id)] \(client.port) error: \(error)")
            }
            if let data = data1, !data.isEmpty {
                self.buffer = data
                self.rfc1928 = Rfc1928(data: data)
                let req = data.hexString
                NSLog("Local Proxy Socks5 got first request: \(req) CMD \(String(describing: self.rfc1928.CMD()))")

                switch self.rfc1928.CMD() {
                case .BIND:
                    self.stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR()
                    NSLog("Local Proxy Socks5 \(client.port) got BIND request: \(req)")

                case .CONNECT:
                    self.CONNECT()   // 进入 CONNECT 处理

                case .UDP_ASSOCIATE:
                    self.stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR()
                    NSLog("Local Proxy Socks5 \(client.port) got UDP_ASSOCIATE request: \(req)")

                default:
                    self.stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR()
                    NSLog("Local Proxy Socks5 \(client.port) got invalid request: \(req)")
                }
            }
        }

        // 方法协商：无认证
        self.send(data: Data([0x05, 0x00]))
    }

    func send(data: Data) {
        self.client.connection.send(content: data, completion: .contentProcessed({ error in
            if let error = error { self.client.stop(error: error) }
        }))
    }

    func stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR() {
        self.rfc1928.REP(self.rfc1928.COMMAND_NOT_SUPPORTED_or_PROTOCOL_ERROR)
        self.send(data: self.rfc1928.toData())
        self.client.stop(error: nil)
    }

    func stop_stage2_WithREQUEST_FAILED() {
        self.rfc1928.STATUS(.NOT_ALLOWED)
        self.send(data: self.rfc1928.toData())
        self.client.stop(error: nil)
    }

    // ⚠️ 如果未来要做 UDP_ASSOCIATE，这里不要再对 self.client.connection 读取业务数据，
    // 否则会和桥接冲突。当前先保持占位。
    func UDP_ASSOCIATE () { /* ...与原来一致，去掉内部对 connection.receive 的那段调试读取... */ }

    func CONNECT() {
        // 解析目标
        switch self.rfc1928.ATYP() {
        case .IP_V4:
            self.host = self.rfc1928.IPv4()
        case .DOMAINNAME:
            self.host = self.rfc1928.domain()
        case .IP_V6:
            NSLog("Local Proxy Socks5 \(client.port) got CONNECT with IPv6 STOP")
            return self.stop_stage2_WithREQUEST_FAILED()
        default:
            self.stop_stage2_WithREQUEST_FAILED()
            return NSLog("Local Proxy Socks5 \(client.port) got CONNECT with invalid ATYP: \(String(describing: self.rfc1928.ATYP()))")
        }
        let dstPort = self.rfc1928.port()
        NSLog("Local Proxy Socks5 \(client.port) CONNECT to \(host):\(dstPort)")

        // 先回复成功，允许 App 发 TLS
        self.rfc1928.REP(0)
        self.send(data: self.rfc1928.toData())

        // 从现在起，只读取一次“首包”交给 ServerBridge；之后的收发都由 ServerBridge 接管
        func waitFirstPacketAndStart() {
            self.client.connection.receive(minimumIncompleteLength: 1, maximumLength: self.client.MTU) { [weak self] (data, _, _, error) in
                guard let self = self else { return }
                if let error = error {
                    NSLog("Local Proxy Socks5 \(self.client.port) read first packet error: \(error)")
                    self.stop_stage2_WithREQUEST_FAILED()
                    return
                }
                guard let firstData = data, !firstData.isEmpty else {
                    // 还没到首包，继续等
                    waitFirstPacketAndStart()
                    return
                }

                let firstB64 = firstData.base64EncodedString()

                // === 根据你的策略选择直连或 LayerMinus ===
                // 建议做成可配置，这里示例：LayerMinus 需要首包 => 设为 false；直连设为 true
                let directConnect = true   // TODO: 按你的配置切换

                if directConnect {
                    // 直连：remoteHost/remotePort 生效，把首包交给 ServerBridge（它会在 uplink ready 后先发出去）
                    self.serverBridge = ServerBridge(
                        sendData: Data(),          // 直连不用
                        host: "", port: 0,         // 直连不用
                        proxyConnect: self.client,
                        remoteHost: self.host,
                        remotePort: dstPort,
                        header: "",                // TLS 无 header，就留空
                        base64Body: firstB64,      // ★ 关键：首包交给 Bridge
                        directConnect: true
                    )
                    NSLog("Local Proxy Socks5 CONNECT direct to \(self.host):\(dstPort) -> start()")
                    self.serverBridge.start()
                } else {
                    // LayerMinus：把首包作为 body 打进签名/打包
                    let _egressNode = self.client.layerMinus.getRandomEgressNodes()
                    let _entryNode  = self.client.layerMinus.getRandomEntryNodes()
                    let entryNode   = _entryNode.ip_addr

                    // 1) 构造消息（把首包打进去）
                    let message = self.client.layerMinus.makeSocksRequest(
                        host: self.host,
                        port: dstPort,
                        body: firstB64,            // ★ 关键：首包
                        command: "CONNECT"
                    )
                    let messageData = message.data(using: .utf8)!
                    let account = self.client.layerMinus.keystoreManager.addresses![0]

                    // 2) 异步签名 -> 生成入口请求 -> 建桥并启动
                    Task {
                        do {
                            let sign = try await self.client.layerMinus.web3.personal.signPersonalMessage(
                                message: messageData, from: account, password: ""
                            )
                            guard
                                let callFun2 = self.client.layerMinus.javascriptContext.objectForKeyedSubscript("json_sign_message"),
                                let ret2 = callFun2.call(withArguments: [message, "0x\(sign.toHexString())"])?.toString()
                            else {
                                NSLog("Local Proxy Socks5 LayerMinus sign/js pack failed")
                                self.stop_stage2_WithREQUEST_FAILED()
                                return
                            }

                            let pre_request = self.client.layerMinus.createValidatorData(node: _egressNode, responseData: ret2)
                            let request     = self.client.layerMinus.makeRequest(host: entryNode, data: pre_request)

                            self.serverBridge = ServerBridge(
                                sendData: request.data(using: .utf8)!, // ★ 入口节点要发的首个包（包含上面 body）
                                host: entryNode,
                                port: 80,
                                proxyConnect: self.client,
                                remoteHost: self.host,
                                remotePort: dstPort,
                                header: "",                // 如后续有 HTTP header 需求再填
                                base64Body: firstB64,      // 冗余给 Bridge，如你的实现需要
                                directConnect: false
                            )
                            NSLog("Local Proxy Socks5 CONNECT via entry \(entryNode) egress \(_egressNode.ip_addr) -> start()")
                            self.serverBridge.start()
                        } catch {
                            NSLog("Local Proxy Socks5 LayerMinus sign error: \(error)")
                            self.stop_stage2_WithREQUEST_FAILED()
                        }
                    }
                }
            }
        }

        waitFirstPacketAndStart()
    }
}

class Socks4 {
    let client: ServerConnection
    var rfc1928: Rfc1928!
    var buffer: Data!
    var serverBridge : ServerBridge!
    
    init(client: ServerConnection, data: Data){
        
        self.client = client
        self.buffer = data
        if !data.isEmpty {
            
            self.rfc1928 = Rfc1928(data: data)
            let req = data.hexString
            switch self.rfc1928.CMD() {
                
                case .BIND:
                    NSLog("Proxy Socks4 \(client.port) got BIND request: \(req)")
                    self.stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR()
                    return
                    
                case .CONNECT:
                    NSLog("Proxy Socks4 \(client.port) got CONNECT request: \(req)")
                    self.CONNECT()
                    return
                case .UDP_ASSOCIATE:
                    NSLog("Proxy Socks4 \(client.port) got UDP_ASSOCIATE request: \(req)")
                    self.stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR()
                    return
                    
                default :
                    NSLog("Proxy Socks4 \(client.port) got invalid request: \(req)")
                    self.stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR()
                    return
            }
        }
        
    }
    
    func CONNECT() {
        
        self.client.connection.receive(minimumIncompleteLength: 1, maximumLength: self.client.MTU) {(data, _, isComplete, error) in
            if let data = data, !data.isEmpty {
                let body = data.base64EncodedString()
                NSLog("Proxy Socks4 \(self.client.id) \(self.client.port) got CONNECT \(self.rfc1928.IPv4()):\(self.rfc1928.port()) body length = \(data.count)")
                let message = self.client.layerMinus.makeSocksRequest(host: self.rfc1928.IPv4(), port: self.rfc1928.port(), body: body, command: "CONNECT")
                let messageData = message.data(using: .utf8)!
                let account = self.client.layerMinus.keystoreManager.addresses![0]
                let _egressNode = self.client.layerMinus.getRandomEgressNodes()
                let _entryNode = self.client.layerMinus.getRandomEntryNodes()
                let entryNode = _entryNode.ip_addr
                Task{
                    let signMessage = try await self.client.layerMinus.web3.personal.signPersonalMessage(message: messageData, from: account, password: "")
                    if let callFun2 = self.client.layerMinus.javascriptContext.objectForKeyedSubscript("json_sign_message") {
                        if let ret2 = callFun2.call(withArguments: [message, "0x\(signMessage.toHexString())"]) {
                            let cmd = ret2.toString()!
                            let pre_request = self.client.layerMinus.createValidatorData(node: _egressNode, responseData: cmd)
                            let request = self.client.layerMinus.makeRequest(host: entryNode, data: pre_request)
                            let port = NWEndpoint.Port(rawValue: 80)!
                            let host = NWEndpoint.Host(entryNode)
                            let header = String(data: data, encoding: .utf8) ?? data.hexString
                            self.serverBridge = ServerBridge(sendData: request.data(using: .utf8)!, host: entryNode, port: 80, proxyConnect: self.client, remoteHost: self.rfc1928.IPv4(), remotePort: self.rfc1928.port(), header: "", base64Body: body, directConnect: true)
                            
                            return self.serverBridge.start()
                        }
                    }
                    
                }
            }
        }
        
        let ret:[UInt8] = [0x0,0x5a,0x0,0x0,0x0,0x0,0x0,0x0]
        self.send(data: Data(ret))
    }
    
    func send(data: Data) {
        self.client.connection.send(content: data, completion: .contentProcessed({ error in
            if let error = error {
                return self.client.stop(error: error)
            }
        }))
    }
    
    func stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR() {
        let ret:[UInt8] = [0x0,0x5b,0x0,0x0]
        self.send(data: Data(ret))
        self.client.stop(error: nil)
    }
}
