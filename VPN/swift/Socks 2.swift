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
        self.client.connection.receive(minimumIncompleteLength: 1, maximumLength: self.client.MTU) {(data1, _, _, error) in
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
                        return NSLog("Local Proxy Socks5 \(client.port) got BIND request: \(req)")
                        
                    case .CONNECT:
                        return self.CONNECT()
                        
                    case .UDP_ASSOCIATE:
                        self.stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR()
                        return NSLog("Local Proxy Socks5 \(client.port) got UDP_ASSOCIATE request: \(req)")
                        
                    default :
                        self.stop_stage1_withSUPPORTED_or_PROTOCOL_ERROR()
                        return NSLog("Local Proxy Socks5 \(client.port) got invalid request: \(req)")
                }
                
                
            }
        }
        self.send(data: Data([0x05, 0x00]))
    }
    
    func send(data: Data) {
        self.client.connection.send(content: data, completion: .contentProcessed({ error in
            if let error = error {
                return self.client.stop(error: error)
            }
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
    
    func UDP_ASSOCIATE () {
        print("Local Proxy Socks5 \(client.port) UDP_ASSOCIATE")
        switch self.rfc1928.ATYP() {
            case .IP_V4:
                host = self.rfc1928.IPv4()
            case .DOMAINNAME:
                host = self.rfc1928.domain()
            case .IP_V6:
            NSLog("Local Proxy Socks5 UDP_ASSOCIATE \(client.port) got CONNECT with IPv6 STOP")
                return self.stop_stage2_WithREQUEST_FAILED()
            default:
                self.stop_stage2_WithREQUEST_FAILED()
                return NSLog("Local Proxy Socks5 UDP_ASSOCIATE \(client.port) got CONNECT with invalid ATYP: \(String(describing: self.rfc1928.ATYP()))")
        }
        NSLog ("Local Proxy Socks5 \(client.port) got UDP_ASSOCIATE \(String(describing: self.rfc1928.ATYP())) with host: \(host):\(self.rfc1928.port()) buffer length = \(self.rfc1928.dataArray.count)")
        print (self.buffer.hexString)
        self.client.connection.receive(minimumIncompleteLength: 1, maximumLength: self.client.MTU) {(data, _, isComplete, error) in
            if let data = data, !data.isEmpty {
                let header = String(data: data, encoding: .utf8) ?? data.hexString
                print (header)
                let body = data.base64EncodedString()
            }
        }
        self.rfc1928.REP(0)
        self.send(data: self.rfc1928.toData())
    }
   
    
    func CONNECT() {
//        print("Local Proxy Socks5 \(client.port) got CONNECT")
        switch self.rfc1928.ATYP() {
            case .IP_V4:
                host = self.rfc1928.IPv4()
            case .DOMAINNAME:
                host = self.rfc1928.domain()
            case .IP_V6:
            NSLog("Local Proxy Socks5 \(client.port) got CONNECT with IPv6 STOP")
                return self.stop_stage2_WithREQUEST_FAILED()
            default:
                self.stop_stage2_WithREQUEST_FAILED()
                return NSLog("Local Proxy Socks5 \(client.port) got CONNECT with invalid ATYP: \(String(describing: self.rfc1928.ATYP()))")
        }
        NSLog ("Local Proxy Socks5 \(client.port) got CONNECT \(String(describing: self.rfc1928.ATYP())) with host: \(host):\(self.rfc1928.port()) buffer length = \(self.rfc1928.dataArray.count)")
        print (self.buffer.hexString)
        
        self.client.connection.receive(minimumIncompleteLength: 1, maximumLength: self.client.MTU) {(data, _, isComplete, error) in
            if let data = data, !data.isEmpty {
                let header = String(data: data, encoding: .utf8) ?? data.hexString
//                print (header)
                let body = data.base64EncodedString()
                let message = self.client.layerMinus.makeSocksRequest(host: self.host, port: self.rfc1928.port(), body: body, command: "CONNECT")
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
                            self.serverBridge = ServerBridge(sendData: request.data(using: .utf8)!, host: host, port: port, proxyConnect: self.client)
                            
                            return self.serverBridge.start()
                        }
                    }
                    
                }
            }
        }
        
        self.rfc1928.REP(0)
        self.send(data: self.rfc1928.toData())
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
                            self.serverBridge = ServerBridge(sendData: request.data(using: .utf8)!, host: host, port: port, proxyConnect: self.client)
                            
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
