//
//  ProxyBridge.swift
//  CoNETVPN
//
//  Created by peter on 2024-12-03.
//

import Network
import Foundation

class ServerBridge {
    var sendData: Data
    var tcpClient: NWConnection
    var proxyConnect: ServerConnection
    //The TCP maximum package size is 64K 65536
    let MTU = 65536
    
    private func stateDidChange(to state: NWConnection.State) {
        switch state {
            
        case .setup:
            print("输出测试启动前的初始状态情况下设置")
        case .preparing:
            print("输出测试准备连接正在积极建立连接准备")
        case .cancelled:
            print("输出测试被取消的连接已被客户端确认为无效，将不再发送事件情况下取消")
      
            
        case .waiting(let error):
            connectionDidFail(error: error)
            print("输出测试等待连接尚未启动，或者没有可用的网络 例等待(NWError) \(error)")
            
        case .ready:
            print("输出测试链接成功")
            firstSend()
        case .failed(let error):
            print("输出测试链接失败")
            connectionDidFail(error: error)
        default:
            print("输出测试其他状态")
            break
        }
    }
    
    init(sendData: Data, host: NWEndpoint.Host, port: NWEndpoint.Port, proxyConnect: ServerConnection) {
        self.proxyConnect = proxyConnect
        self.sendData = sendData
        self.tcpClient = NWConnection(host: host, port: port, using: .tcp)
    }
    
    func start() {
        self.tcpClient.stateUpdateHandler = stateDidChange(to:)
        self.tcpClient.start(queue: .main)
    }
    
    private func tcpClientStartReceive() {
        self.tcpClient.receive(minimumIncompleteLength: 1, maximumLength: MTU) {(data, _, isComplete, error) in
            if let data = data, !data.isEmpty {
                self.proxyConnect.connection.send(content: data, completion: .contentProcessed ({ error in
                    if let error = error {
                        print("ServerBridge Node \(data.count) ---> APP Error!")
                        self.proxyConnect.connectionDidFail(error: error)
                        return self.stop(error: error)
                    }
                    print("ServerBridge send Node \(data.count) --->  APP SUCCESS!")
                }))
            }
            
            if let error = error {
                self.stop(error: error)
                return print("ServerBridge receive Node data ERROR \(error)!")
            }
            
            
            print("ServerBridge receive Node data \(data?.count ?? 0) isComplete ")
            self.tcpClientStartReceive ()
            
        }
        
    }
    
    private func proxyConnectStartReceive () {
        self.proxyConnect.connection.receive(minimumIncompleteLength: 1, maximumLength: self.MTU) {(data, _, isComplete, error) in
            if let data = data, !data.isEmpty {
                
                self.tcpClient.send(content: data, completion: .contentProcessed ({ error in
                    if let error = error {
                        print("ServerBridge send APP \(data.count) --> Node Error!")
                        return self.connectionDidFail(error: error)
                    }
                    print("ServerBridge send APP \(data.count) --> Node success!")
                }))
            }
            
            if let error = error {
                self.proxyConnect.stop(error: error)
                return print("ServerBridge receive APP data ERROR \(error)!")
            }
            
            print("ServerBridge receive APP data \(data?.count ?? 0) isComplete ")
            self.proxyConnectStartReceive ()
        }
    }
    
    func firstSend() {
        
        proxyConnectStartReceive()
        tcpClientStartReceive()
        
        self.tcpClient.send(content: self.sendData, completion: .contentProcessed ({ error in
            if let error = error {
                print("ServerBridge --> Node Access ERROR!")
                return self.stop(error: error)
            }
            
            print("ServerBridge firstSend --> Node Access SUCCESS!")
            
        }))
    }
    
    private func connectionDidComplete(error: Error?) {
        print("ServerBridge connection did complete, error: \(String(describing: error))")
        stop(error: error)
    }
    
    private func connectionDidFail(error: Error) {
        print("ServerBridge connection did fail, error: \(error)")
        stop(error: error)
    }
    
    private func stop(error: Error?) {
        tcpClient.stateUpdateHandler = nil
        tcpClient.cancel()
        if let didStopCallback = didStopCallback {
            self.didStopCallback = nil
            didStopCallback(error)
        }
    }
    
    var didStopCallback: ((Error?) -> Void)? = nil
}
