//
//  VPost.swift
//  CoNETVPN
//
//  Created by peter on 2024-12-06.
//
import Network
import Foundation

class ValidatorPost {
    let MTU = 65536
    var firstSend = ""
    var validatorEntryNode: NWConnection
    var host: String
    init (postData: String, node: String, layerMinus: LayerMinus) {
        
        self.firstSend = layerMinus.makeRequest(host: node, data: postData)
        self.host = node
        let port = NWEndpoint.Port(rawValue: 80)!
        let _host = NWEndpoint.Host(node)
        self.validatorEntryNode = NWConnection(host: _host, port: port, using: .tcp)
        
        
    }
    
    func start () {
        self.validatorEntryNode.stateUpdateHandler = self.stateDidChange(to:)
        self.validatorEntryNode.start(queue: .main)
    }
    
    private func ready () {
        
        let sendDate = self.firstSend.data(using: .utf8)!
        self.validatorEntryNode.send(content: sendDate, completion: .contentProcessed( { error in
            if let error = error {
                self.connectionDidFail(error: error)
                return
            }
            self.nextStep()
        }))
    }
    
    private func nextStep() {
        self.validatorEntryNode.receive(minimumIncompleteLength: 1, maximumLength: MTU) {(data, _, isComplete, error) in
            self.stop(error: nil)
            
            if let data = data, !data.isEmpty {
                let header = String(data: data, encoding: .utf8) ?? ""
                if header.hasPrefix("HTTP/1.1 200 OK") {
                    NSLog("PacketTunnelProvider receive data \(header)")
                    let userInfo: [String: Any] = ["当前通知类型": "允许上网"]
                    NotificationCenter.default.post(name: .didUpdateConnectionNodes, object: nil, userInfo:userInfo)
                    
                }
                
            }
            
            
            if let error = error {
//                let userInfo: [String: Any] = ["当前通知类型": "网络连接失败"]
//                NotificationCenter.default.post(name: .didUpdateConnectionNodes, object: nil, userInfo:userInfo)
                NSLog("PacketTunnelProvider receive data ERROR! \(error)")
                
            }
            
        }
    }
    
    func stop (error: Error?) {
        NSLog("PacketTunnelProvider  ValidatorPost STOP!")
        validatorEntryNode.stateUpdateHandler = nil
        validatorEntryNode.cancel()
        if let didStopCallback = didStopCallback {
            self.didStopCallback = nil
            didStopCallback(error)
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
    var didStopCallback: ((Error?) -> Void)? = nil
    private func connectionDidComplete(error: Error?) {
        NSLog("PacketTunnelProvider ServerBridge connection did complete, error: \(String(describing: error))")
        stop(error: error)
    }
    
    private func connectionDidFail(error: Error) {
        let userInfo: [String: Any] = ["当前通知类型": "网络连接失败"]
        NotificationCenter.default.post(name: .didUpdateConnectionNodes, object: nil, userInfo:userInfo)
        NSLog("PacketTunnelProvider ServerBridge connection did fail, error: \(error)")
        stop(error: error)
    }
}
