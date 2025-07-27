//
//  ServerConnection.swift
//  tq-proxy-ios
//
//  Created by peter on 2024-11-14.
//

import Foundation
import Network


let AVAILABLE_INTERFACES = [
    "en0",
    "en2",
    "en3",
    "en4",
    "pdp_ip0",
    "pdp_ip1",
    "pdp_ip2",
    "pdp_ip3",
]

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
        print("Local Proxy \(self.port) connection \(id) will start")
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
        switch(self.connection.endpoint) {
            case .hostPort(let host, _):
                let remoteHost = "\(host)"
                if remoteHost != "127.0.0.1" {
                    resIpaddress = self.layerMinus.localIpaddress
                }
                
            default:
                break
        }
        var sockString = socks5 ? "socks5" : "socks"

        var ret = "function FindProxyForURL ( url, host ) {\n"
                + "if (isInNet ( dnsResolve( host ), \"0.0.0.0\", \"255.0.0.0\") ||\n"
                + "   isInNet( dnsResolve( host ), \"172.16.0.0\", \"255.240.255.0\") ||\n"
                + "   isInNet( dnsResolve( host ), \"127.0.0.0\", \"255.255.255.0\") ||\n"
                + "   isInNet ( dnsResolve( host ), \"192.168.0.0\", \"255.255.0.0\" ) ||\n"
                + "   isInNet ( dnsResolve( host ), \"10.0.0.0\", \"255.0.0.0\" ) ||\n"
                //+ "   dnsDomainIs( host, \"conet.network\") ||\n"
                + "   dnsDomainIs( host, \".apple-mapkit.com\") ||\n"
                + "   dnsDomainIs( host, \".firefox.com\") ||\n"
                + "   dnsDomainIs( host, \".mozilla.com\") ||\n"
                + "   dnsDomainIs( host, \".icloud.com\") ||\n"
                + "   dnsDomainIs( host, \".icloud-content.com\") ||\n"
                + "   dnsDomainIs( host, \".apple.com\") ||\n"
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
                print(header)
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
                    print(splitLine)
                    return self.send(data: self.responseHttp(body: self.retPac(socks5: true, excludeIP: self.excludeIP)))
                }
                
                if header.hasPrefix("GET ") {
                    let body = data.base64EncodedString()
                    return self.makeHttpProxyConnect(header: header, body: body)
                }
                //      Try Socks Protocol
                let hexStr = data.hexString
                if hexStr.hasPrefix("04") {
                    let connect = Socks4(client: self, data: data)
                    return print("Local Proxy \(self.port) received Socks v4 data \(hexStr)")
                }
                
                if hexStr.hasPrefix("05") {
                    let connect = Socks5(client: self)
                    return self.serverBridge = connect.serverBridge
                }
                
                print("Local Proxy \(self.port) received unknow Protocol \(hexStr) ")
                print("Local Proxy \(self.port) \(data.base64EncodedString())")
                return self.stop(error: nil)
            }
            
            
            if let error = error {
                print("Local Proxy \(self.port) connection \(self.id) error")
                self.connectionDidFail(error: error)
            }
        }
        
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
                            self.serverBridge = ServerBridge(sendData: request.data(using: .utf8)!, host: host, port: port, proxyConnect: self)
//                            print("Proxy connect started entry node:[ \(entryNode):\(_entryNode.ip_addr) ] egress node:[ \(egressNode):\(_egressNode.ip_addr) ] request:[ \(request) ]")
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
            print("Local Proxy \(self.port) hasn't EgressNodes yet Error!")
            self.stop(error: nil)
        }))
    }


    func send(data: String) {
        let sendData = data.data(using: .utf8)!
        self.connection.send(content: sendData, completion: .contentProcessed( { error in
            if let error = error {
                return self.stop(error: error)
            }
            print("Local Proxy \(self.port) connection \(self.id) did send, data: \(data)")
        }))
    }


    func connectionDidFail(error: Error) {
        print("Local Proxy \(self.port) connection \(id) did fail, error: \(error)")
        stop(error: error)
    }

    private func connectionDidEnd() {
        print("Local Proxy \(self.port) connection \(id) did end")
        stop(error: nil)
    }

    func stop(error: Error?) {
        if self.connection.stateUpdateHandler != nil {
            self.connection.stateUpdateHandler = nil
            self.connection.cancel()
        }
        
        if self.serverBridge != nil {
            self.serverBridge.stop(error: error)
            self.serverBridge = nil
        }
        
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
