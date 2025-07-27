//
//  ServerConnection.swift
//  tq-proxy-ios
//
//  Created by peter on 2024-11-14.
//

import Foundation
import Network


class ServerConnection {
    //The TCP maximum package size is 64K 65536
    let MTU = 65536
    
    private static var nextID: Int = 0
    let  connection: NWConnection
    let id: Int
    let layerMinus: LayerMinus
    let port: UInt16
    var serverBridge: ServerBridge!
    var excludeIP: [String]
    init(nwConnection: NWConnection, _layerMinus: LayerMinus, port: UInt16) {

        connection = nwConnection
        id = ServerConnection.nextID
        ServerConnection.nextID += 1
        layerMinus = _layerMinus
        self.port = port
        self.excludeIP = layerMinus.entryNodes.map{$0.ip_addr}
        self.excludeIP.append(contentsOf: layerMinus.egressNodes.map{$0.ip_addr})
        
    }

    var didStopCallback: ((Error?) -> Void)? = nil

    func start() {
        NSLog("Local Proxy \(self.port) connection \(id) will start")
        connection.stateUpdateHandler = self.stateDidChange(to:)
        setupReceive()
        connection.start(queue: .main)
    }


    private func stateDidChange(to state: NWConnection.State) {
        switch state {
        case .waiting(let error):
            connectionDidFail(error: error)
        case .ready:
            print("Local Proxy \(self.port) connection \(id) ready")
        case .failed(let error):
            connectionDidFail(error: error)
        default:
            break
        }
    }
    
    let proxyServerFirstResponse = "HTTP/1.1 200 Connection Established\r\n\r\n"
    let proxyServerFirstResponse_Error = "HTTP/1.1 503 no server was available\r\n\r\n"

    func retPac (socks5: Bool, excludeIP: [String]) -> String {
        var resIpaddress = "127.0.0.1"
//        switch(self.connection.endpoint) {
//            case .hostPort(let host, _):
//                let remoteHost = "\(host)"
//                if remoteHost != "127.0.0.1" {
//                    resIpaddress = self.layerMinus.localIpaddress
//                }
//                
//            default:
//                break
//        }
        var sockString = socks5 ? "socks5" : "socks"
        var _excludeIP = ""
        for ip in excludeIP {
            _excludeIP += "   isInNet( dnsResolve( host ), \"\(ip)\", \"255.255.255.255\" ) ||\n"
        }
        var ret = "function FindProxyForURL ( url, host ) {\n"
                + "if (isInNet ( dnsResolve( host ), \"0.0.0.0\", \"255.0.0.0\") ||\n"
                + "   isInNet( dnsResolve( host ), \"172.16.0.0\", \"255.240.255.0\") ||\n"
                + "   isInNet( dnsResolve( host ), \"127.0.0.0\", \"255.255.255.0\") ||\n"
                + "   isInNet ( dnsResolve( host ), \"192.168.0.0\", \"255.255.0.0\" ) ||\n"
                + "   isInNet ( dnsResolve( host ), \"10.0.0.0\", \"255.0.0.0\" ) ||\n"
                + "   \(_excludeIP)"
                //+ "   dnsDomainIs( host, \"conet.network\") ||\n"
                + "   dnsDomainIs( host, \".apple-mapkit.com\") ||\n"
                + "   dnsDomainIs( host, \".firefox.com\") ||\n"
                + "   dnsDomainIs( host, \".mozilla.com\") ||\n"
                + "   dnsDomainIs( host, \".icloud.com\") ||\n"
                + "   dnsDomainIs( host, \".icloud-content.com\") ||\n"
                + "   dnsDomainIs( host, \".apple.com\") ||\n"
                + "   dnsDomainIs( host, \".aplle.com\")) {\n"
                + "   dnsDomainIs( host, \".cn\")) {\n"
                + "   dnsDomainIs( host, \".qq.com\")) {\n"
                + "   dnsDomainIs( host, \".cdn-apple.com\") ||\n"
                + "   dnsDomainIs( host, \".apple.news\") ||\n"
        
                + "   dnsDomainIs( host, \".local\")) {\n"
                //+ "   dnsDomainIs( host, \".openpgp.online\")) {\n"
                + "       return \"DIRECT\";\n"
                + "};\n"
        + "return \"\(sockString) \(resIpaddress):\(self.port)\";\n}"
        return ret
    }
    
    func responseHttp(body: String) -> String {
        let response = "HTTP/1.1 200 OK\r\nContent-Length:\(body.count)\r\n\r\n\(body)"
        return response
    }
    
    private func setupReceive() {
        connection.receive(minimumIncompleteLength: 1, maximumLength: MTU) {(data, _, _, error) in
            if let data = data, !data.isEmpty {
                //      Try http & https Proxy Protocol
                let header = String(data: data, encoding: .utf8) ?? ""
                NSLog(header)
                if header.hasPrefix("CONNECT ") {
                    self.connection.receive(minimumIncompleteLength: 1, maximumLength: self.MTU) {(data1, _, isComplete, error) in
                        if let data1 = data1, !data1.isEmpty {
                            let body = data1.base64EncodedString()
                            return self.makeHttpProxyConnect(header: header, body: body)
                        }
                    }
                    return self.send(data: self.proxyServerFirstResponse)
                }
                
                if header.hasPrefix("GET /pac HTTP/") {
                    var splitLine = header.components(separatedBy: "\r")
                    NSLog("Local Proxy /pac" + splitLine.joined(separator: "\n"))
                    return self.send(data: self.responseHttp(body: self.retPac(socks5: false, excludeIP: self.excludeIP)))
                }
                
                
                //      Try Socks Protocol
                let hexStr = data.hexString
                if hexStr.hasPrefix("04") {
                    let connect = Socks4(client: self, data: data)
                    return NSLog("Local Proxy \(self.port) received Socks v4 data \(hexStr)")
                }
                
                if hexStr.hasPrefix("05") {
                    let connect = Socks5(client: self)
                    return self.serverBridge = connect.serverBridge
                }
                
                
                var newData = data
                var newDataString = ""
                var targetHost: String?      // 声明一个变量来存储目标主机
                var targetPort: Int?         // ✅ 1. 声明一个变量来存储目标端口

                if var text = String(data: data, encoding: .utf8),
                   let range = text.range(of: "\r\n") {
                    let requestLine = String(text[..<range.lowerBound])      // 第一行
                    let remaining = String(text[range.upperBound...])        // 剩余部分

                    let parts = requestLine.components(separatedBy: " ")
                    if parts.count == 3, let url = URL(string: parts[1]) {
                        
                        // 从解析出的 URL 对象中获取 host 和 port
                        targetHost = url.host
                        targetPort = url.port // ✅ 2. 从 URL 对象中获取 port
                        
                        // --- 以下为路径替换逻辑，保持不变 ---
                        let path = url.path.isEmpty ? "/" : url.path
                        let query = url.query.map { "?\($0)" } ?? ""
                        let newRequestLine = "\(parts[0]) \(path + query) \(parts[2])"
                        let finalRequest = newRequestLine + "\r\n" + remaining
                        
                        if let finalData = finalRequest.data(using: .utf8) {
                            newData = finalData
                            newDataString = finalRequest
                        }
                    }
                }

                // 打印日志部分保持不变...
                if let host = targetHost {
                    if let port = targetPort {
                        NSLog("Local Proxy makeHttpProxyConnect ✅ 成功提取到目标主机: \(host)，端口: \(port)")
                    } else {
                        NSLog("Local Proxy makeHttpProxyConnect ✅ 成功提取到目标主机: \(host)，端口: 未指定 (将使用默认端口)")
                    }
                } else {
                    NSLog("Local Proxy makeHttpProxyConnect ⚠️ 未能从请求中提取到主机名。")
                }


                // 用修改后的 newData 生成 base64 body
                // ✅ 记录 header 和修改后的 newData 内容
                // NSLog("Local Proxy makeHttpProxyConnect header:\n%@", header) // header变量在此上下文中未定义
                NSLog("Local Proxy makeHttpProxyConnect newData:\n%@", newDataString)


                if let host = targetHost, host.isIPAddress {
                    let port = UInt16(targetPort ?? 80)
                    NSLog("Local Proxy makeHttpProxyConnect ✅ 目标是IP地址，执行直接隧道连接...\(host):\(port)")

                    self.directTunnel(to: host, port: port, initialData: newData) { remoteConnection in
                        guard let remote = remoteConnection else {
                            // 连接失败处理，例如关闭当前客户端连接
                            self.connection.cancel()
                            return
                        }

                        // 建立 pipe 双向传输
                        self.pipe(from: self.connection, to: remote, label: "\(host):\(port)")
                    }

                    return  // ✅ 不再继续同步处理，等待异步完成
                }


                // 如果上面的 if 条件不满足（不是IP或主机为nil），则执行以下逻辑
                let body = newData.base64EncodedString()
                return self.makeHttpProxyConnect(header: header, body: body)
            }
            
            
            if let error = error {
                NSLog("Local Proxy \(self.port) connection \(self.id) error")
                self.connectionDidFail(error: error)
            }
        }
        
    }
    
    
    func directTunnel(to host: String, port: UInt16, initialData: Data? = nil, completion: @escaping (NWConnection?) -> Void) {
        let nwEndpoint = NWEndpoint.Host(host)
        let nwPort = NWEndpoint.Port(rawValue: port)!

        let connection = NWConnection(host: nwEndpoint, port: nwPort, using: .tcp)

        connection.stateUpdateHandler = { state in
            switch state {
            case .ready:
                NSLog("✅ directTunnel connected to \(host):\(port)")
                if let data = initialData {
                    connection.send(content: data, completion: .contentProcessed({ _ in }))
                }
                completion(connection)
            case .failed(let error):
                NSLog("❌ directTunnel connection failed: \(error)")
                connection.cancel()
                completion(nil)
            default:
                break
            }
        }

        connection.start(queue: .global())
    }
    
    func pipe(from src: NWConnection, to dst: NWConnection, label: String) {
        func forward(from source: NWConnection, to destination: NWConnection, direction: String) {
            source.receive(minimumIncompleteLength: 1, maximumLength: 8192) { data, _, isComplete, error in
                if let data = data, !data.isEmpty {
                    destination.send(content: data, completion: .contentProcessed({ _ in }))
                    forward(from: source, to: destination, direction: direction)
                } else if let error = error {
                    NSLog("⚠️ Local Proxy makeHttpProxyConnect \(label) pipe error (\(direction)): \(error)")
                    source.cancel()
                    destination.cancel()
                } else if isComplete {
                    NSLog("⛔️ Local Proxy makeHttpProxyConnect\(label) pipe completed (\(direction))")
                    source.cancel()
                    destination.cancel()
                }
            }
        }

        forward(from: src, to: dst, direction: "src→dst")
        forward(from: dst, to: src, direction: "dst→src")
    }
    
    func makeHttpProxyConnect(header: String, body: String) {
        let _egressNode = self.layerMinus.getRandomEgressNodes()
        let _entryNode = self.layerMinus.getRandomEntryNodes()

        if (_egressNode.ip_addr == "" || _entryNode.ip_addr == "") {
            return self.proxyServerError()
        }
        
        let entryNode = _entryNode.ip_addr
        
        if let callFun1 = self.layerMinus.javascriptContext.objectForKeyedSubscript("makeRequest") {
            
            if let ret1 = callFun1.call(withArguments: [header,body,self.layerMinus.walletAddress]) {
                let message = ret1.toString()!
                print(message)
                let messageData = message.data(using: .utf8)!
                let account = self.layerMinus.keystoreManager.addresses![0]
                Task {
                    let signMessage = try await self.layerMinus.web3.personal.signPersonalMessage(message: messageData, from: account, password: "")
                    if let callFun2 = self.layerMinus.javascriptContext.objectForKeyedSubscript("json_sign_message") {
                        if let ret2 = callFun2.call(withArguments: [message, "0x\(signMessage.toHexString())"]) {
                            let cmd = ret2.toString()!
                            let pre_request = self.layerMinus.createValidatorData(node: _egressNode, responseData: cmd)
                            let request = self.layerMinus.makeRequest(host: entryNode, data: pre_request)
                            let port = NWEndpoint.Port(rawValue: 80)!
                            let host = NWEndpoint.Host(entryNode)
                            NSLog("Local Proxy \(self.id) http protocol Target :[\(message)] ")
                            self.serverBridge = ServerBridge(sendData: request.data(using: .utf8)!, host: host, port: port, proxyConnect: self)
//                            NSLog("Proxy connect started entry node:[ \(entryNode):\(_entryNode.ip_addr) ] egress node:[ \(egressNode):\(_egressNode.ip_addr) ] request:[ \(request) ]")
                            return self.serverBridge.start()
                        }
                    }
                }
            }
        }
    }
    
    func proxyServerError() {
        let sendData = proxyServerFirstResponse_Error.data(using: .utf8)!
        self.connection.send(content: sendData, completion: .contentProcessed( { error in
            if let _ = error {
                return
            }
            let userInfo: [String: Any] = ["当前通知类型": "网络连接失败","重试": "需重试"]
            NotificationCenter.default.post(name: .didUpdateConnectionNodes, object: nil, userInfo:userInfo)
            NSLog("Local Proxy \(self.port) hasn't EgressNodes yet Error!")
            self.stop(error: nil)
        }))
    }


    func send(data: String) {
        let sendData = data.data(using: .utf8)!
        self.connection.send(content: sendData, completion: .contentProcessed( { error in
            if let error = error {
                return self.stop(error: error)
            }
            NSLog("Local Proxy \(self.port) connection \(self.id) did send, data: \(data)")
        }))
    }


    func connectionDidFail(error: Error) {
        NSLog("Local Proxy \(self.port) connection \(id) did fail, error: \(error)")
        stop(error: error)
    }

    private func connectionDidEnd() {
        NSLog("Local Proxy \(self.port) connection \(id) did end")
        stop(error: nil)
    }

    func stop(error: Error?) {
        if self.connection.stateUpdateHandler != nil {
            self.connection.stateUpdateHandler = nil
            
        }
        
        
        if self.serverBridge != nil {
            self.serverBridge.stop(error: error)
            self.serverBridge = nil
        }
        self.connection.cancel()
        if let didStopCallback = didStopCallback {
            self.didStopCallback = nil
            didStopCallback(error)
        }
    }
    

}

extension Data {
    var hexString : String {
        return self.reduce("") { (a : String, v : UInt8) -> String in
            return a + String(format: "%02x", v)
        }
    }
}

extension String {
    /// 判断字符串是否为有效的IPv4或IPv6地址
    var isIPAddress: Bool {
        // 尝试初始化为IPv4地址，如果不为nil，则是有效的IPv4地址
        if IPv4Address(self) != nil {
            return true
        }
        // 尝试初始化为IPv6地址，如果不为nil，则是有效的IPv6地址
        if IPv6Address(self) != nil {
            return true
        }
        return false
    }
}
