//
//  UpdaterV2.swift
//  CoNETVPN1
//
//  Created by peter on 2025-08-25.
//

import Foundation
import ZIPFoundation // 导入第三方解压库
import Web3Core
import web3swift
import BigInt
import XCTest

final class UpdaterV2 {
    static let ReadAllDataRequest = Data(hex: "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003e8")
    static let NodeInfoAddr = EthereumAddress("0x2DF3302d0c9aC19BE01Ee08ce3DDA841BdcF6F03")
    static let ConetRPC = "https://mainnet-rpc.conet.network"
    static let ReadAllDataRequestABIName = "CONET_Guardian_NodeInfo_ABI"
    let tempKeystore: EthereumKeystoreV3!
    var keystoreManager:KeystoreManager! // 先给空管理器
    var web3: Web3?                      // 异步拿到后再赋值
    init (){
        self.tempKeystore = try! EthereumKeystoreV3(password: "")
        initClass()
    }
    
    private func initClass() {
        Task {
            do {
                // 1) web3
                let rpcUrl = URL(string: UpdaterV2.ConetRPC)!
                self.web3 = try await Web3.new(rpcUrl)
                
                // 2) keystore
                self.keystoreManager = KeystoreManager([self.tempKeystore])
                self.web3?.addKeystoreManager(self.keystoreManager)
                let account = self.keystoreManager.addresses?[0]
                // 3) 读取 ABI 文本
                if let jsSourcePath = Bundle.main.path(forResource: UpdaterV2.ReadAllDataRequestABIName, ofType: "text") {
                    let abi = try String(contentsOfFile: jsSourcePath)
                    
                    guard let contract = web3?.contract(abi, at: UpdaterV2.NodeInfoAddr, abiVersion: 2)! else {
                        return XCTFail()
                    }
                    
                    let start = BigInt("0")
                    let end = BigInt("1000")
                    let readTX = contract.createReadOperation(
                        "getAllNodes",
                        parameters: [start, end]
                    )
                    let tokenBalanceResponse = try await readTX?.callContractMethod()
                    let temp = tokenBalanceResponse as! [[String: Any]]
                }
                
            } catch {
                print("initClass error:", error)
            }
        }
    }
}
