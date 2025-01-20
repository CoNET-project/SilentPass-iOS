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
    var _layerMinus: LayerMinus
    var miningNodeObj: NWConnection!
    var miningNode: Node!
    var firstSend: Data = "".data(using: .utf8)!
    var body = Data()
    var first = true
    var port: Int
    init (layerMinus: LayerMinus, post: Int) {
        self._layerMinus = layerMinus
        self.port = post
    }
    
    
    func start() {
        self.first = true
        
        Task {
            self.miningNode = self._layerMinus.getRandomEntryNodes()
           
            if (self.miningNode.ip_addr == "") {
                return print("MiningProcess \(self.port) Error! No mining node can found from getRandomNodeFromEntryNodes")
            }
            
            NSLog("MiningProcess \(self.port) 挖礦開始，聆聽節點\(self.miningNode.ip_addr) 本地IP address \(self._layerMinus.localIpaddress)")
            let host = NWEndpoint.Host(self.miningNode.ip_addr)
            let post = NWEndpoint.Port(80)
            self.miningNodeObj = NWConnection(host: host, port: post, using: .tcp)
            self.miningNodeObj.stateUpdateHandler = self.stateDidChange(to:)
            let _data =  try await self._layerMinus.createConnectCmd(node: self.miningNode)
            
            let sendDate = self._layerMinus.makeRequest(host: self.miningNode.ip_addr, data: _data)
            NSLog("MiningProcess \(self.port) \(sendDate) ")
            self.firstSend = sendDate.data(using: .utf8)!
            self.miningNodeObj.start(queue: .main)
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
        NSLog("MiningProcess connectMining ready")
        self.miningNodeObj.send(content: self.firstSend, completion: .contentProcessed( { error in
            if let error = error {
                self.connectionDidFail(error: error)
                return
            }
            NSLog("MiningProcess \(self.port) connectMining did first")
            self.nextStep()
        }))
    }
    
    private func nextStep () {
        self.miningNodeObj.receive(minimumIncompleteLength: 1, maximumLength: MTU) {(data, _, _, error) in
            if (error != nil) {
                return self.stop(keep: true)
            }
            
            if let data = data, !data.isEmpty {
                self.body += data
                if let responseJSON = try? JSONSerialization.jsonObject(with: self.body, options: []) as? [String: Any] {
                    if let epoch = responseJSON["epoch"] as? String {
                        let nodeHash = responseJSON["hash"] as! String
                        Task {
                            let minerResponseHash = await self._layerMinus.signEphch(hash: nodeHash)
                            NSLog("MiningProcess \(self.port) epoch \(epoch) \(self._layerMinus.walletAddress)\nhash \(minerResponseHash)")
                            let nodeWallet = responseJSON["nodeWallet"] as! String
                            let nodeDomain = responseJSON["nodeDomain"] as! String
                            
                            if let callFun1 = self._layerMinus.javascriptContext.objectForKeyedSubscript("json_mining_response") {
                                if let ret1 = callFun1.call(withArguments: [epoch, self._layerMinus.walletAddress, nodeWallet, nodeHash, nodeDomain, minerResponseHash]) {
                                    let message = ret1.toString()!
                                    let messageData = message.data(using: .utf8)!
                                    let account = self._layerMinus.keystoreManager.addresses![0]
                                    let signMessage = try await self._layerMinus.web3.personal.signPersonalMessage(message: messageData, from: account, password: "")
                                    if let callFun2 = self._layerMinus.javascriptContext.objectForKeyedSubscript("json_sign_message") {
                                        if let ret2 = callFun2.call(withArguments: [message, "0x\(signMessage.toHexString())"]) {
                                            let cmd = ret2.toString()!
//                                            print("Mining epoch \(epoch) \(self._layerMinus.privateKeyAromed)\nhash \(minerResponseHash)\n\(message)")

                                            self._layerMinus.egressNodes.forEach { _node in
                                                let response = self._layerMinus.createValidatorData(node: _node, responseData: cmd)
                                                let submitNode = self._layerMinus.getRandomEntryNodes().ip_addr
                                                if !response.isEmpty {
                                                    print("MiningProcess \(self.port) [\(self._layerMinus.walletAddress)] eposh \(epoch) 挖礦信息，送往出口節點 Send Validator to Node [\(_node.ip_addr)] via submitNode 通過入口 \(submitNode) 轉發")
                                                    NSLog("MiningProcess  \(self.port) [\(self._layerMinus.walletAddress)] eposh \(epoch) 挖礦信息，送往出口節點 Send Validator to Node [\(_node.ip_addr)] via submitNode 通過入口 \(submitNode) 轉發")
                                                    let validNode = ValidatorPost(postData: response, node: submitNode, layerMinus: self._layerMinus )
                                                    validNode.start()
                                                }
                                            }
                                        }
                                    }
                                    
                                }
                            }
                            
                        }
                        self.body = "".data(using: .utf8)!
                    }
                    
                } else {
                    if (self.first) {
                        self.body = "".data(using: .utf8)!
                        self.first = false
                    }
                }
                
            } else {
                NSLog("MiningProcess \(self.port) nextStep data.isEmpty")
                return self.stop(keep: true)
            }
            self.nextStep()
        }
        
    }
    
    func stop(keep: Bool) {
        NSLog("MiningProcess \(self.port) 挖礦停止")
        
        guard let miningNode = miningNodeObj else {
            NSLog("MiningProcess \(self.port) Error: miningNodeObj is nil")
            return // or handle error accordingly
        }
        
        miningNode.stateUpdateHandler = nil
        miningNode.cancel()
        
        if let didStopCallback = didStopCallback {
            self.didStopCallback = nil
            didStopCallback(nil)
            
            if keep {
                return self.start()
            }
        }
    }
    var didStopCallback: ((Error?) -> Void)? = nil
    
    private func connectionDidComplete(error: Error?) {
        NSLog("MiningProcess \(self.port) ServerBridge connection did complete, error: \(String(describing: error))")
        stop(keep: true)
    }
    
    private func connectionDidFail(error: Error) {
        
        let userInfo: [String: Any] = ["当前通知类型": "网络连接失败"]
        NotificationCenter.default.post(name: .didUpdateConnectionNodes, object: nil, userInfo:userInfo)
        
        NSLog("MiningProcess \(self.port) ServerBridge connection did fail, error: \(error)")
        stop(keep: true)
    }
    
}
