//
//  mining.swift
//  CoNETVPN
//
//  Created by peter on 2024-12-04.
//
import Network
import Foundation

class MiningProcess {
    let MTU = 65536
    var server: Server
    var miningNode: NWConnection
    var host: String
    var firstSend: Data = "".data(using: .utf8)!
    var privateKey: String
    init (_server: Server, _privateKey: String) {
        server = _server
        host = _server.getRandomEntryNode()
        privateKey = _privateKey
        let _host = NWEndpoint.Host(host)
        let _port = NWEndpoint.Port(80)
        miningNode = NWConnection(host: _host, port: _port, using: .tcp)
    }
    
    func start() {
        let jsFunc = "connectMining"
        let args = "\(host)=====\(privateKey)"
        miningNode.stateUpdateHandler = self.stateDidChange(to:)
        server.webView.nativeBridge.callJavaScriptFunction(functionName: jsFunc, arguments: args) { response in
            if let response = response {
                let str = response as! String
                let data = str.data(using: .utf8)!
                if let responseJSON = try? JSONSerialization.jsonObject(with: data, options: []) as? [String: Any] {
                    let _sendDate = responseJSON["data"]
                    if let __sendDate = _sendDate as? [String: Any] {
                        if let _data = __sendDate["requestData"] as? [String] {
                            let __data = _data[0]
                            let sendDate = self.server.makeRequest(host: self.host, data: __data)
                            self.firstSend = sendDate.data(using: .utf8)!
                            self.miningNode.start(queue: .main)
                        }
                        
                    }
                        
                   
                }
                
            }
        }
    }
    private func stateDidChange(to state: NWConnection.State) {
        switch state {
        case .waiting(let error):
            connectionDidFail(error: error)
        case .ready:
            ready()
        case .failed(let error):
            connectionDidFail(error: error)
        default:
            break
        }
    }
    
    
    private func ready () {
        print("connectMining ready")
        self.miningNode.send(content: self.firstSend, completion: .contentProcessed( { error in
            if let error = error {
                self.connectionDidFail(error: error)
                return
            }
            print("connectMining did first")
            self.nextStep()
        }))
    }
    
    private func makeValidatorJSON (responseJSON: [String: Any]) -> String {
        let ret = "{\"nodeWallet\": \"\(responseJSON["nodeWallet"] ?? "-")\", \"epoch\": \"\(responseJSON["epoch"] ?? "-")\", \"hash\": \"\(responseJSON["hash"] ?? "-")\", \"nodeDomain\": \"\(responseJSON["nodeDomain"] ?? "-")\"}"
        let retBase = ret.data(using: .utf8)!.base64EncodedString()
        return retBase
    }
    
    
    private func nextStep () {
        self.miningNode.receive(minimumIncompleteLength: 1, maximumLength: MTU) {(data, _, isComplete, error) in
            if let data = data, !data.isEmpty {
                if let responseJSON = try? JSONSerialization.jsonObject(with: data, options: []) as? [String: Any] {
                    if let _ = responseJSON["epoch"] as? Int {
                        let _args = self.makeValidatorJSON(responseJSON: responseJSON)
                        let jsFunc = "makeMiningPostForSaasNode"
                        self.server.egressNodes.forEach { node in
                            let args = "\(node)==00==\(_args)==00==\(self.privateKey)"
                            self.server.webView.nativeBridge.callJavaScriptFunction(functionName: jsFunc, arguments: args) { response in
                                let str = response as! String
                                let _ = ValidatorPost(postData: str, server: self.server, node: node )
                            }
                        }
                    }
                }
                return self.nextStep()
            }
        }
    }
    
    func stop (error: Error?) {
        miningNode.stateUpdateHandler = nil
        miningNode.cancel()
        if let didStopCallback = didStopCallback {
            self.didStopCallback = nil
            didStopCallback(error)
        }
    }
    var didStopCallback: ((Error?) -> Void)? = nil
    private func connectionDidComplete(error: Error?) {
        print("ServerBridge connection did complete, error: \(String(describing: error))")
        stop(error: error)
    }
    
    private func connectionDidFail(error: Error) {
        print("ServerBridge connection did fail, error: \(error)")
        stop(error: error)
    }
    
    
    
}
