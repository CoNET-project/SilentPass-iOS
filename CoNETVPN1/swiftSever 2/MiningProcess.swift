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
    var _layerMinus: LayerMinus!
    var miningNodeObj: NWConnection!
    var miningNode: Node!
    var firstSend: Data = "".data(using: .utf8)!
    var body = Data()
    var firstResponse = true
    var processing = false
    var port: Int
    var startMessage: String
    var keep = true
    var listeningCount = 0
    init (layerMinus: LayerMinus, post: Int, message: String) {
        self._layerMinus = layerMinus
        self.port = post
        self.startMessage = message
    }
    
    
    func start() {
        NSLog("MiningProcess \(self.port) \(self.startMessage) start() ")
        if self.processing == true {
            return NSLog("MiningProcess \(self.port) \(self.startMessage) start Error! already other process started")
        }
        self.firstResponse = true
        self.processing  = true
        
        Task {
            self.miningNode = self._layerMinus.getRandomEntryNodes()
           
            if (self.miningNode.ip_addr == "") {
                processing = false
                return print("MiningProcess \(self.port) \(self.startMessage)  Error! No mining node can found from getRandomNodeFromEntryNodes")
            }
            
            NSLog("MiningProcess \(self.port) \(self.startMessage) 挖礦開始，聆聽節點\(self.miningNode.ip_addr)")
            let host = NWEndpoint.Host(self.miningNode.ip_addr)
            let post = NWEndpoint.Port(80)
            self.miningNodeObj = NWConnection(host: host, port: post, using: .tcp)
            self.miningNodeObj.stateUpdateHandler = self.stateDidChange(to:)
            let _data =  try await self._layerMinus.createConnectCmd(node: self.miningNode)
            
            let sendDate = self._layerMinus.makeRequest(host: self.miningNode.ip_addr, data: _data)
            NSLog("MiningProcess \(self.port) \(self.startMessage) \(sendDate) ")
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
        NSLog("MiningProcess connectMining \(self.startMessage) ready")
        self.miningNodeObj.send(content: self.firstSend, completion: .contentProcessed( { error in
            if let error = error {
                self.connectionDidFail(error: error)
                return
            }
            NSLog("MiningProcess \(self.port) \(self.startMessage) connectMining did first")
            self.nextStep()
        }))
    }
    
    private func nextStep () {
        self.miningNodeObj.receive(minimumIncompleteLength: 1, maximumLength: MTU) {(data, _, _, error) in
            
            if let data = data, !data.isEmpty {
                self.body += data
                
                
                if let responseJSON = try? JSONSerialization.jsonObject(with: self.body, options: []) as? [String: Any] {
                    if let epoch = responseJSON["epoch"] as? String {
                        let nodeHash = responseJSON["hash"] as! String
                        let before = Date()
                        
                        Task {
                            let minerResponseHash = await self._layerMinus.signEphch(hash: nodeHash)
                            NSLog("MiningProcess \(self.port) \(self.startMessage) epoch \(epoch) \(self._layerMinus.walletAddress)\nhash \(minerResponseHash)")
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
                                                    let after = Date()
                                                    let timeInterval = after.timeIntervalSince(before)
                                                    if timeInterval > 48 {
                                                        return NSLog("NSLog MiningProcess timeInterval = \(timeInterval) > 48 STOP 送往出口節點")
                                                        
                                                    }
                                                    NSLog("NSLog MiningProcess timeInterval = [\(timeInterval)] \(self.startMessage) \(self.port) listeningCount \(self.listeningCount) [\(self._layerMinus.walletAddress)] eposh \(epoch) 挖礦信息，送往出口節點 Send Validator to Node [\(_node.ip_addr)] via submitNode 通過入口 \(submitNode) 轉發")
                                                    let validNode = ValidatorPost(postData: response, node: submitNode, layerMinus: self._layerMinus)
                                                    validNode.start()
                                                }
                                            }
                                        }
                                    }
                                    
                                }
                            }
                            
                        }
                        
                    }
                    self.stop(true)
                }
                
                if self.firstResponse == true {
                    self.firstResponse = false
                    self.body = "".data(using: .utf8)!
                    
                }
                
            }
            
            self.nextStep()
            
        }
        
    }
    
    func stop(_ keep: Bool = false) {
        NSLog("MiningProcess \(self.port) 挖礦停止 with keep \(keep)")
        processing = false
        guard let miningNode = miningNodeObj else {
            NSLog("MiningProcess \(self.port) Error: miningNodeObj is nil")
            return // or handle error accordingly
        }
        
        miningNode.stateUpdateHandler = nil
        miningNode.cancel()
        
        if let didStopCallback = didStopCallback {
            self.didStopCallback = nil
            didStopCallback(nil)
            
        }
        
        if keep == true {
            let delay = DispatchTime.now() + 120
            DispatchQueue.main.asyncAfter(deadline: delay) {
                if self.keep == true {
                    self.start()
                }
            }
        }
    }
    
    var didStopCallback: ((Error?) -> Void)? = nil
    
    private func connectionDidComplete(error: Error?) {
        NSLog("MiningProcess \(self.port) ServerBridge connection did complete, error: \(String(describing: error))")
        
    }
    
    private func connectionDidFail(error: Error) {
        
        let userInfo: [String: Any] = ["当前通知类型": "网络连接失败"]
        NotificationCenter.default.post(name: .didUpdateConnectionNodes, object: nil, userInfo:userInfo)
        
        NSLog("MiningProcess \(self.port) ServerBridge connection did fail, error: \(error)")
        
    }
    
}
