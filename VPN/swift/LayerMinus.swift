//
//  LayerMinus.swift
//  CoNETVPN
//
//  Created by peter on 2024-12-09.
//


struct NodeRegion {
    var node: Node
    var time: Double
}

import Foundation
import Web3Core
import web3swift
import ObjectivePGP
import JavaScriptCore

func getIPAddress() -> String? {
    var address: String?

    // 获取本地接口列表
    var ifaddr: UnsafeMutablePointer<ifaddrs>?
    guard getifaddrs(&ifaddr) == 0 else { return nil }
    guard let firstAddr = ifaddr else { return nil }

    // 遍历所有接口
    for ifptr in sequence(first: firstAddr, next: { $0.pointee.ifa_next }) {
        let interface = ifptr.pointee

        // 检查地址类型（IPv4）
        guard let ifaAddr = interface.ifa_addr else { continue }
        let addrFamily = ifaAddr.pointee.sa_family
        if addrFamily == UInt8(AF_INET) { // 仅需要IPv4地址

            // 检查接口名称
            let name = String(cString: interface.ifa_name)
            if name == "en0" || name == "en1" || name == "en2" || name == "en3" || name == "en4" || name.hasPrefix("pdp_ip") {

                // 转换地址为可读字符串
                var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
                let result = getnameinfo(ifaAddr,
                                         socklen_t(ifaAddr.pointee.sa_len),
                                         &hostname,
                                         socklen_t(hostname.count),
                                         nil,
                                         socklen_t(0),
                                         NI_NUMERICHOST)
                if result == 0 {
                    address = String(cString: hostname)
                }
            }
        }
    }
    freeifaddrs(ifaddr)

    return address
}

class LayerMinus {
    static let maxRegionNodes = 5
    var maxEgressNodes = 1
    static let rpc = "https://rpc.conet.network"
    static let rpcUrl = URL(string: LayerMinus.rpc)!
    static var allNodes: [Node] = []
    static var nearbyCountryTestNodes: [NodeRegion] = []
    static var nearbyCountry = ""
    
    var tempKeystore = try! EthereumKeystoreV3(password: "")
    var keystoreManager: KeystoreManager
    var walletAddress = ""
    var privateKeyAromed = ""
    var privateKey: Data!
    let keyring = Keyring()
    let pgpKey = KeyGenerator().generate(for: "user@conet.network", passphrase: "")
    
    static var currentScanNode = 100
    static var country = Set<String>()
    var entryNodes: [Node] = []
    var egressNodes: [Node] = []
    
    let javascriptContext: JSContext = JSContext()

    var web3: Web3!
    var CONET_Guardian_NodeInfo_ABI: String!
    var CONET_Guardian_NodeInfo_Contract: EthereumContract!
    var localIpaddress = ""
    
    func initializeJS() {
        self.javascriptContext.exceptionHandler = { context, exception in
            if let exc = exception {
                print("JS Exception:", exc.toString() as Any)
            }
        }
        let jsSourceContents = "let _makeRequest=(header,body) => {var headers = header.split('\\r\\n')[0]; var commandLine = headers.split(' '); var hostUrl = commandLine[1]; var host = /http/.test(hostUrl)?hostUrl.split('http://')[1].split('/')[0]:hostUrl.split(':')[0]; var port = parseInt(hostUrl.split(':')[1])||80; return { host, buffer: body||header, cmd: body.length ? 'CONNECT' : 'GET', port, order: 0 };};\nvar json_string=(data)=>JSON.stringify({data});var json_command=(data)=>JSON.stringify({command: 'mining', algorithm: 'aes-256-cbc', Securitykey: '', walletAddress: data});var json_sign_message=(message,signMessage)=>JSON.stringify({message,signMessage});var json_mining_response=(eposh,walletAddress,nodeWallet,hash,nodeDomain,minerResponseHash )=>JSON.stringify({Securitykey:'',walletAddress,algorithm:'aes-256-cbc',command:'mining_validator',requestData:{epoch:parseInt(eposh),nodeWallet,hash,nodeDomain,minerResponseHash,isUser:true}});var makeRequest=(header,body,walletAddress)=>JSON.stringify({command:'SaaS_Sock5',algorithm:'aes-256-cbc',Securitykey:'',requestData:[_makeRequest(header,body)],walletAddress});var getResult=(res)=> JSON.parse(res)[1].result;var makeSocksRequest = (host, port, buffer, walletAddress, cmd) =>JSON.stringify({command:'SaaS_Sock5',algorithm:'aes-256-cbc',Securitykey:'',requestData:[{host, cmd, port, order: 0, buffer}],walletAddress});"
        self.javascriptContext.evaluateScript(jsSourceContents)
    }
    
    func nodeJSON (nodeJsonStr: String) -> [Node] {
        let decoder = JSONDecoder()
        do {
            let nodes = try decoder.decode([Node].self, from: nodeJsonStr.data(using: .utf8)!)
            return nodes
        } catch {
            return []
        }
        
    }
    
    func entryNodes_JSON () -> String {
        let jsonStr = self.entryNodes.toJSONString()
        return jsonStr
    }
    
    func egressNodes_JSON () -> String {
        let jsonStr = self.egressNodes.toJSONString()
        return jsonStr
    }
    
    
    func createValidatorData (node: Node, responseData: String) -> String {
        let nodePGP = node.armoredPublicKey
        let cmdData = responseData.data(using: .utf8)!.base64EncodedString().data(using: .utf8)!
        do {
            let keys = try ObjectivePGP.readKeys(from: nodePGP.data(using: .utf8)!)
            let encrypted = try ObjectivePGP.encrypt(cmdData, addSignature: false, using: keys)
            let armoredRet = Armor.armored(encrypted, as: .message)
            if let functionFullname = self.javascriptContext.objectForKeyedSubscript("json_string") {
                if let fullname = functionFullname.call(withArguments: [armoredRet]) {
                    return fullname.toString()
                }
            }
        } catch {
            
        }
        return ""
        
    }
    
    func makeSocksRequest (host:String, port: Int, body: String, command: String) -> String {
        if let callFun1 = self.javascriptContext.objectForKeyedSubscript("makeSocksRequest") {
            if let ret1 = callFun1.call(withArguments: [host, port, body, self.walletAddress, command]) {
                let message = ret1.toString()!
//                print(message)
                let messageData = message.data(using: .utf8)!
                return message
            }
        }
        return ""
    }

    
    func createConnectCmd (node: Node) async throws -> String {
        
        if let callFun1 = self.javascriptContext.objectForKeyedSubscript("json_command") {
            if let ret1 = callFun1.call(withArguments: [self.walletAddress]) {
                let message = ret1.toString()!
                let messageData = message.data(using: .utf8)!
//                let nodePGP = node.armoredPublicKey
                let account = self.keystoreManager.addresses![0]
                let signMessage = try await self.web3.personal.signPersonalMessage(message: messageData, from: account, password: "")
//                print(self.privateKeyAromed, signMessage.toHexString(), message)
                
                if let callFun2 = self.javascriptContext.objectForKeyedSubscript("json_sign_message") {
                    if let ret2 = callFun2.call(withArguments: [message, "0x\(signMessage.toHexString())"]) {
                        let cmd = ret2.toString()!
                        return self.createValidatorData(node: node, responseData: cmd)
                    }
                }
            }
        }
        return ""
    }
    
    init (port: Int) {
        
        self.keystoreManager = KeystoreManager([self.tempKeystore!])
        let account = self.keystoreManager.addresses![0]
        self.walletAddress = account.address.lowercased()
        self.initializeJS()
        self.readCoNET_nodeInfoABI()
        
        do {
            self.privateKey = try self.keystoreManager.UNSAFE_getPrivateKeyData(password: "", account: account)
            self.privateKeyAromed = self.privateKey.toHexString()
            
            UserDefaults.standard.set(self.privateKey, forKey: "privateKey")
            UserDefaults.standard.synchronize()
        } catch {
            print ("LayerMinus Error getting private key!")
            return
        }
        Task{
            self.web3 = try await Web3.new(LayerMinus.rpcUrl)
            web3.addKeystoreManager(self.keystoreManager)
        }
        //self.localIpaddress =  getIPAddress()!
    }
    
    func makeRequest(host: String, data: String) -> String {
        var ret = "POST /post HTTP/1.1\r\n"
        ret += "Host: \(host)\r\n"
        ret += "Content-Length: \(data.count)\r\n\r\n"
        ret += data
        ret += "\r\n"
        
        return ret
    }
    
    func getRandomEntryNodes () -> Node {
        if entryNodes.isEmpty {
            return Node(country: "", ip_addr: "", region: "", armoredPublicKey: "", nftNumber: "")
        }
        let randomIndex = Int.random(in: 0..<self.entryNodes.count)
        return entryNodes[randomIndex]
    }
    
    func getRandomEgressNodes() -> Node {
        if self.egressNodes.isEmpty {
            return Node(country: "", ip_addr: "", region: "", armoredPublicKey: "", nftNumber: "")
        }
        let randomIndex = Int.random(in: 0..<self.egressNodes.count)
        return self.egressNodes[randomIndex]
    }
    
    func signEphch (hash: String) async -> String {
        let account = self.keystoreManager.addresses![0]
        do {
            let signMessage = try await self.web3.personal.signPersonalMessage(message: hash.data(using: .utf8)!, from: account, password: "")
            return "0x\(signMessage.toHexString())"
        } catch {
            return ""
        }
       
    }
    
    func readCoNET_nodeInfoABI () {
        if let jsSourcePath = Bundle.main.path(forResource: "CONET_Guardian_NodeInfo_ABI", ofType: "text") {
            do {
                self.CONET_Guardian_NodeInfo_ABI = try String(contentsOfFile: jsSourcePath)
                self.CONET_Guardian_NodeInfo_Contract = try EthereumContract(self.CONET_Guardian_NodeInfo_ABI, at: nil)
            } catch {
                print("readCoNET_nodeInfoABI Error")
            }
        }
    }
    
    func startInVPN (privateKey: String, entryNodes: [Node], egressNodes: [Node], port: Int) {
        let privateKeyData = Data.fromHex(privateKey)!
        
        self.tempKeystore = try! EthereumKeystoreV3(privateKey: privateKeyData, password: "")
        self.keystoreManager = KeystoreManager([self.tempKeystore!])
        
        if (self.tempKeystore == nil) {
            return NSLog("PacketTunnelProvider startInVPN privateKey Error!")
        }
        
        self.entryNodes = entryNodes
        self.egressNodes = egressNodes
        
        let account = self.keystoreManager.addresses![0]
        self.walletAddress = account.address.lowercased()
        do {
            self.privateKey = try self.keystoreManager.UNSAFE_getPrivateKeyData(password: "", account: account)
            self.privateKeyAromed = self.privateKey.toHexString()
           
        } catch {
            return NSLog ("PacketTunnelProvider startInVPN private key ERROR!")
        }
        
        NSLog("PacketTunnelProvider startInVPN privateKey \(self.privateKeyAromed) entryNodes [\(self.entryNodes.count)] egressNodes [\(self.egressNodes.count)]")
        
        Task{
            self.web3 = try await Web3.new(LayerMinus.rpcUrl)
            web3.addKeystoreManager(self.keystoreManager)
        }
        
//        if self.miningProcess != nil {
//            self.miningProcess.keep = false
//            self.miningProcess.stop(false)
//            self.miningProcess = nil
//        }
//        self.miningProcess = MiningProcess(layerMinus: self, post: port, message: "PacketTunnelProvider startInVPN")
//        self.miningProcess.start()
    }


    
    
    static func getRandomNodeWithRegion(country: String) -> Node {
        let allNodes = LayerMinus.allNodes.filter { $0.country == country }
        let randomIndex = Int.random(in: 0..<allNodes.count)
        return allNodes[randomIndex]
    }

    
    
    
    static func getDocumentsDirectory() -> URL {
        let paths = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
        return paths[0]
    }
}

extension FixedWidthInteger {
    var data: Data {
        let data = withUnsafeBytes(of: self) { Data($0) }
        return data
    }
}


extension Encodable {
    
    func toJSONString() -> String {
        let jsonData = try! JSONEncoder().encode(self)
        return String(data: jsonData, encoding: .utf8)!
    }
    
}

func instantiate<T: Decodable>(jsonString: String) -> T? {
    return try? JSONDecoder().decode(T.self, from: jsonString.data(using: .utf8)!)
}
