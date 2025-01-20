//
//  Server.swift
//  tq-proxy-ios
//
//  Created by peter on 2024-11-14.
//

import Foundation
import Network

import ObjectivePGP

//import Web3Core
//import web3swift


@available(macOS 10.14, *)

class Server {
    let port: NWEndpoint.Port
    let listener: NWListener
    let layerMinus: LayerMinus
    let portNumber: UInt16
    private var connectionsByID: [Int: ServerConnection] = [:]
    
    init(port: UInt16, layerMinus: LayerMinus) {
        self.port = NWEndpoint.Port(rawValue: port)!
        listener = try! NWListener(using: .tcp, on: self.port)
        self.layerMinus = layerMinus
        self.portNumber = port
    }
    
    

    func start() {
        print("Local Proxy Server starting on port \(self.port) ...")
        listener.stateUpdateHandler = self.stateDidChange(to:)
        listener.newConnectionHandler = self.didAccept(nwConnection:)
        listener.start(queue: .main)
    
    }

    func stateDidChange(to newState: NWListener.State) {
        switch newState {
        case .ready:
          print("Server on ready.")
        case .failed(let error):
            print("Server failure, error: \(error.localizedDescription)")
            exit(EXIT_FAILURE)
        default:
            print("Server newState unknown, \(newState)")
            break
        }
    }

    private func didAccept(nwConnection: NWConnection) {
        let connection = ServerConnection(nwConnection: nwConnection, _layerMinus: self.layerMinus, post: self.portNumber)
        self.connectionsByID[connection.id] = connection
        connection.didStopCallback = { _ in
            self.connectionDidStop(connection)
        }
        
        connection.start()
//        Server.swift
        
        
        print("server did open connection \(connection.id)")
    }

    private func connectionDidStop(_ connection: ServerConnection) {
        self.connectionsByID.removeValue(forKey: connection.id)
        print("server did close connection \(connection.id)")
    }

    func stop() {
        self.listener.stateUpdateHandler = nil
        self.listener.newConnectionHandler = nil
        self.listener.cancel()
        for connection in self.connectionsByID.values {
            connection.didStopCallback = nil
            connection.stop(error: nil)
        }
        self.connectionsByID.removeAll()
    }
    
    func getLastConnection() -> ServerConnection? {
        return connectionsByID.values.map { $0 }.last // 转换为数组以获取最后一个元素
    }

    
}
extension Notification.Name {
    static let didUpdateConnectionNodes = Notification.Name("didUpdateConnectionNodes")
}
