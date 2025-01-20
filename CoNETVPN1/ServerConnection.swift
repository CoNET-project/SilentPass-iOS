//
//  ServerConnection.swift
//  tq-proxy-ios
//
//  Created by peter on 2024-11-14.
//

import Foundation
import Network

@available(macOS 10.14, *)


class ServerConnection {
    //The TCP maximum package size is 64K 65536
    let MTU = 65536
    
    private static var nextID: Int = 0
    let  connection: NWConnection
    let id: Int
    var webView: WebViewManager
    var server: Server
    
    init(nwConnection: NWConnection, _webView: WebViewManager, _server: Server) {
        connection = nwConnection
        id = ServerConnection.nextID
        ServerConnection.nextID += 1
        webView = _webView
        server = _server
    }

    var didStopCallback: ((Error?) -> Void)? = nil

    func start() {
        print("connection \(id) will start")
        connection.stateUpdateHandler = self.stateDidChange(to:)
        setupReceive()
        connection.start(queue: .main)
    }


    private func stateDidChange(to state: NWConnection.State) {
        switch state {
        case .waiting(let error):
            connectionDidFail(error: error)
        case .ready:
            print("connection \(id) ready")
        case .failed(let error):
            connectionDidFail(error: error)
        default:
            break
        }
    }
    
    let proxyServerFirstResponse = "HTTP/1.1 200 Connection Established\r\n\r\n"
    

    private func setupReceive() {
        connection.receive(minimumIncompleteLength: 1, maximumLength: MTU) {(data, _, isComplete, error) in
            if let data = data, !data.isEmpty {
                let message = String(data: data, encoding: .utf8)
                if ((message?.hasPrefix("CONNECT ")) != nil) {
                    print("connection \(self.id)  had a CONNECT request \n \(message ?? "-")")
                    
                    self.connection.receive(minimumIncompleteLength: 1, maximumLength: self.MTU) {(data1, _, isComplete, error) in
                        if let data1 = data1, !data1.isEmpty {
                            
                            print("connection \(self.id) second request")
                            let str1 = data.base64EncodedString()
                            let str2 = data1.base64EncodedString()
                            let node = self.server.getRandomEgressNodes()
                            let args = String("\(str1)===oooo===\(str2)===oooo===\(node)")
                            print("\(args)")
                            let jsFunc = "connectSaaS"
                            
                    
//                            试一下 这里有没有拿到节点
//                            print("Egress Nodes: \(self.server.egressNodes)")
//                            print("Entry Nodes: \(self.server.entryNodes)")
                            
                            self.webView.nativeBridge.callJavaScriptFunction(functionName: jsFunc, arguments: args) { response in
                                let str = response as! String
                                let _data = str.data(using: .utf8)!
                                if let responseJSON = try? JSONSerialization.jsonObject(with: _data, options: []) as? [String: Any] {
                                    if let data = responseJSON["data"] {
                                        let data1 = data as! String
                                        let entryNode = self.server.getRandomEntryNode()
                                        let sendDate = self.server.makeRequest(host: node, data: data1)
                                        print("ServerBridge send proxy request to \(node)")
                                        let port = NWEndpoint.Port(rawValue: 80)!
                                        let host = NWEndpoint.Host(entryNode)
                                        let conBri = ServerBridge(sendData: sendDate.data(using: .utf8)!, host: host, port: port, proxyConnect: self)
                                        return conBri.start()
                                    }
                                    print("error \(responseJSON)")
                                }
                                
                            }
                            
                        }
                    }
                    return self.send(data: self.proxyServerFirstResponse)
                }

            }
            
            if isComplete {
                print("ServerConnection \(self.id) receive did isComplete")
            }
            
            if let error = error {
                print("connection \(self.id) error")
                self.connectionDidFail(error: error)
            }
        
        }
    }


    func send(data: String) {
        let sendData = data.data(using: .utf8)!
        self.connection.send(content: sendData, completion: .contentProcessed( { error in
            if let error = error {
                self.connectionDidFail(error: error)
                return
            }
            print("connection \(self.id) did send, data: \(data)")
        }))
    }

    func stop() {
        print("connection \(id) will stop")
    }

    func connectionDidFail(error: Error) {
        print("connection \(id) did fail, error: \(error)")
        stop(error: error)
    }

    private func connectionDidEnd() {
        print("connection \(id) did end")
        stop(error: nil)
    }

    func stop(error: Error?) {
        connection.stateUpdateHandler = nil
        connection.cancel()
        if let didStopCallback = didStopCallback {
            self.didStopCallback = nil
            didStopCallback(error)
        }
    }
    
    
}
