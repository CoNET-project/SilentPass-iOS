import Foundation
import Network
import Web3Core
import web3swift
import BigInt

// 你原有的 Node
struct Node: Codable {
    var country: String
    var ip_addr: String
    var region: String
    var armoredPublicKey: String
    var nftNumber: String
    var domain: String?
    
    enum CodingKeys: String, CodingKey {
        case country, ip_addr, region, armoredPublicKey, nftNumber
    }
}

enum NodeFetchError: Error { case badURL, badAddress, decode }

// ✅ 用于承接链上返回的 nodeInfo
struct OnchainNode: Codable {
    let id: UInt64
    let PGP: String
    let PGPKey: String
    let ip_addr: String
    let regionName: String
}


// ⛏️ 把合约返回的数组解成 OnchainNode[]
private func decodeOnchainNodes(_ arr: [Any]) throws -> [OnchainNode] {
    var out: [OnchainNode] = []
    out.reserveCapacity(arr.count)

    for el in arr {
        // 1) 字典（带字段名）
        if let dict = el as? [String: Any] {
            let id = try asUInt64(dict["id"])
            let PGP = dict["PGP"] as? String ?? ""
            let PGPKey = dict["PGPKey"] as? String ?? ""
            let ip = dict["ip_addr"] as? String ?? ""
            let region = dict["regionName"] as? String ?? ""
            out.append(.init(id: id, PGP: PGP, PGPKey: PGPKey, ip_addr: ip, regionName: region))
            continue
        }
        // 2) 无名 tuple（按顺序 5 项）
        if let tup = el as? [Any], tup.count == 5 {
            let id = try asUInt64(tup[0])
            let PGP = tup[1] as? String ?? ""
            let PGPKey = tup[2] as? String ?? ""
            let ip = tup[3] as? String ?? ""
            let region = tup[4] as? String ?? ""
            out.append(.init(id: id, PGP: PGP, PGPKey: PGPKey, ip_addr: ip, regionName: region))
            continue
        }
        // 3) 反射兜底（ABIv2.Tuple5）
        if let mirrored = reflectTuple5ToOnchain(el) {
            out.append(mirrored)
            continue
        }
        throw NodeFetchError.decode
    }
    return out
}

private func reflectTuple5ToOnchain(_ any: Any) -> OnchainNode? {
    let m = Mirror(reflecting: any)
    var vals: [Any] = []
    for c in m.children { vals.append(c.value) }
    guard vals.count == 5,
          let id = try? asUInt64(vals[0]),
          let PGP = vals[1] as? String,
          let PGPKey = vals[2] as? String,
          let ip = vals[3] as? String,
          let region = vals[4] as? String
    else { return nil }
    return .init(id: id, PGP: PGP, PGPKey: PGPKey, ip_addr: ip, regionName: region)
}

private func asUInt64(_ any: Any?) throws -> UInt64 {
    if let u = any as? UInt64 { return u }
    if let i = any as? Int { return UInt64(i) }
    if let bi = any as? BigUInt { return UInt64(bi) }
    if let s = any as? String, let v = UInt64(s) { return v }
    throw NodeFetchError.decode
}

// ✅ 你的 Node 去重逻辑（按 ip）
private func dedupeNodes(_ input: [Node]) -> [Node] {
    var seenIP = Set<String>()
    var out: [Node] = []
    for n in input.reversed() {
        if seenIP.insert(n.ip_addr).inserted {
            out.append(n)
        }
    }
    return out.reversed()
}

// ✅ 从 regionName 推断 country（例： "PA.US" -> "US"；不含点则原样返回）
private func deriveCountry(fromRegionName region: String) -> String {
    if let suffix = region.split(separator: ".").last {
        return String(suffix)
    }
    return region
}

// ====================== NodeStore ======================

struct NodeStore {
    // ⛏️ 去掉对 jsonString 的依赖，默认空数组，等你拉链上数据再填充
    static var allNodes: [Node] = []
    static var allNodes_domain: [OnchainNode] = []
    static var allRegion: [String] = []
    static var allRegionTested: [String] = []
    /// 每个 Region 抽样出来用于测速/展示的代表节点
    static var regionSampleNode: [String: Node] = [:]
    /// 每个 Region 的延迟（ms）。失败/超时会记成 Int.max
    static var regionLatencyMs: [String: Int] = [:]
    
    static var entryNodes:[Node] = []
    
    static func getRandomEntryNode() -> Node? {
        guard !entryNodes.isEmpty else { return nil }
        return entryNodes.randomElement()
    }
    
    // ✅ 初始化：自动拉链上节点、填充 allNodes, allNodes_domain, allRegion
    static func initialize() async {
        do {
            let nodes = try await fetchAllNodesViaWeb3swift()
            allNodes = nodes
            
            // 先基于 nodes 生成国家级 allRegion（唯一且保持顺序）
            var seen = Set<String>()
            allRegion = nodes.compactMap { n in
                let c = n.country
                return seen.insert(c).inserted ? c : nil
            }

            // —— 使用可复用函数：仅测 DE / ES / US / GB ——
            let results = await sampleRegionsLatency(
                countries: allRegion.filter { ["DE", "ES", "US", "GB"].contains($0) },
                timeout: 3.0
            )
            
            // 写回缓存 & 让 allRegion 变为按延迟从快到慢的顺序
            regionSampleNode = Dictionary(uniqueKeysWithValues: results.map { ($0.region, $0.node) })
            regionLatencyMs  = Dictionary(uniqueKeysWithValues: results.map { ($0.region, $0.delay >= 0 ? $0.delay : Int.max) })
            allRegionTested  = results.map { $0.region }
            
            // 可选：简单打印观测到的次序
            #if DEBUG
                let preview = results.prefix(8).map { "\($0.region)=\($0.delay)ms" }.joined(separator: ", ")
                print("⚡️ Region latency order (top): \(preview)")
            #endif
            

            print("✅ NodeStore initialized: \(allNodes.count) nodes, \(allRegion.count) regions")
        } catch {
            print("❌ NodeStore initialization failed:", error)
        }
    }

    /// 随机选一个可达节点
    static func getRandom(region: String? = nil,
                          timeout: TimeInterval = 3.0,
                          maxProbe: Int = 8) async -> Node? {
        let candidates = (region == nil)
        ? allNodes
        : allNodes.filter { $0.region == region }

        guard !candidates.isEmpty else { return nil }

        let shuffled = candidates.shuffled()
        let firstBatchCount = min(maxProbe, shuffled.count)

        for node in shuffled.prefix(firstBatchCount) {
            let delay = await getNodeDelay(node, timeout: timeout)
            if delay >= 0 { return node }
        }
        for node in shuffled.dropFirst(firstBatchCount) {
            let delay = await getNodeDelay(node, timeout: timeout)
            if delay >= 0 { return node }
        }
        return nil
    }
    
    /// 按国家列表抽样：每个国家随机 1 节点，并发测延迟，返回 (country, node, delay) 从快到慢
    static func sampleRegionsLatency(
        countries: [String],
        timeout: TimeInterval = 3.0
    ) async -> [(region: String, node: Node, delay: Int)] {
        let regionsSnapshot = countries
        var results: [(region: String, node: Node, delay: Int)] = []
        results.reserveCapacity(regionsSnapshot.count)
    
        await withTaskGroup(of: (String, Node?, Int).self) { group in
            for r in regionsSnapshot {
                // 候选以 Node.country 匹配（因为 allRegion 里现在只保留国家码）
                let candidates = allNodes.filter { $0.country == r }
                if let pick = candidates.randomElement() {
                    group.addTask {
                        let d = await getNodeDelay(pick, timeout: timeout)
                        return (r, pick, d)
                    }
                } else {
                    group.addTask { (r, nil, Int.max) }
                }
            }
            for await (r, nodeOpt, delay) in group {
                if let n = nodeOpt { results.append((r, n, delay)) }
            }
        }
        // -1 视为最慢
        results.sort {
            let a = $0.delay >= 0 ? $0.delay : Int.max
            let b = $1.delay >= 0 ? $1.delay : Int.max
            return a < b
        }
        
        // —— 结束前：在“最快的国家”的所有节点中，随机挑 20 个 getNodeDelay 可用的作为 entryNodes ——
        if let fastest = results.first?.region {
            // 按国家（country）筛候选；为避免阻塞，分批并发探测
            let candidates = allNodes.filter { $0.country == fastest }
            var picked: [Node] = []
            picked.reserveCapacity(20)
        
            // 随机顺序，限制并发批大小，逐批收集成功者
            let shuffled = candidates.shuffled()
            let batch = 8
            var idx = 0
            while idx < shuffled.count && picked.count < 20 {
                let end = min(idx + batch, shuffled.count)
                await withTaskGroup(of: (Node, Int).self) { group in
                    for n in shuffled[idx..<end] {
                        group.addTask {
                            let d = await getNodeDelay(n, timeout: timeout)
                            return (n, d)
                        }
                    }
                    for await (n, d) in group {
                        if d >= 0 && picked.count < 20 {
                            picked.append(n)
                        }
                    }
                }
                idx = end
            }
            entryNodes = picked
        } else {
            entryNodes = []
        }
        
        
        return results
    }
    
    
    

    static func getNodeDelay(_ node: Node, timeout: TimeInterval = 3.0) async -> Int {
        await withCheckedContinuation { continuation in
            let host = NWEndpoint.Host(node.ip_addr)
            guard let port = NWEndpoint.Port(rawValue: 80) else {
                continuation.resume(returning: -1); return
            }
            
            print("getNodeDelay start for \(node)")

            let conn = NWConnection(host: host, port: port, using: .tcp)
            let start = Date()

            final class FinishFlag {
                var finished = false
                let lock = NSLock()
                func markFinished() -> Bool {
                    lock.lock(); defer { lock.unlock() }
                    if finished { return false }
                    finished = true
                    return true
                }
            }
            let flag = FinishFlag()

            @Sendable func finish(_ value: Int) {
                if flag.markFinished() {
                    conn.cancel()
                    continuation.resume(returning: value)
                }
            }

            conn.stateUpdateHandler = { state in
                switch state {
                case .ready:
                    let ms = Int(Date().timeIntervalSince(start) * 1000)
                    finish(ms)
                case .failed(_), .cancelled:
                    finish(-1)
                default: break
                }
            }

            conn.start(queue: .global())
            DispatchQueue.global().asyncAfter(deadline: .now() + timeout) { finish(-1) }
        }
    }
    
    // ✅ 将 fetchAllNodesViaWeb3swift 放入 NodeStore 内部
    static func fetchAllNodesViaWeb3swift(
        rpc: String = "https://mainnet-rpc.conet.network",
        contractHex: String = "0x2DF3302d0c9aC19BE01Ee08ce3DDA841BdcF6F03",
        pageSize: UInt = 200,
        maxPages: Int = 10_000
    ) async throws -> [Node] {
        guard let url = URL(string: rpc) else { throw NodeFetchError.badURL }
        let web3 = try await Web3.new(url)
        guard let caddr = EthereumAddress(contractHex) else { throw NodeFetchError.badAddress }

        let abi = nodeInfoABI
        let contract = web3.contract(abi, at: caddr)!
        var start = BigUInt(0)
        let length = BigUInt(pageSize)
        var allOnchain: [OnchainNode] = []
        var page = 0

        while page < maxPages {
            let result = try await contract.createReadOperation(
                "getAllNodes", parameters: [start, length]
            )!.callContractMethod()

            let arrAny: [Any]
            if let named = result["allNodes"] as? [Any] {
                arrAny = named
            } else if let first = result.values.first as? [Any] {
                arrAny = first
            } else { throw NodeFetchError.decode }

            let pageNodes = try decodeOnchainNodes(arrAny)
            allOnchain.append(contentsOf: pageNodes)
            if pageNodes.count < Int(pageSize) { break }
            start += length
            page += 1
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        let mapped: [Node] = allOnchain.map { oc in
            Node(
                country: deriveCountry(fromRegionName: oc.regionName),
                ip_addr: oc.ip_addr,
                region: oc.regionName,
                armoredPublicKey: oc.PGP,
                nftNumber: String(oc.id),
                domain: oc.PGPKey
            )
        }
        
        allNodes_domain = allOnchain.map { oc in
            OnchainNode(id: oc.id, PGP: oc.PGP, PGPKey: oc.PGPKey, ip_addr: oc.ip_addr, regionName: oc.regionName)
        }
        

        // 去重并更新 allRegion
        let deduped = dedupeNodes(mapped)
        var seen = Set<String>()
        var regions: [String] = []
        for n in deduped where seen.insert(n.region).inserted {
            regions.append(n.region)
        }
        allRegion = regions
        return deduped
    }
}

// ⭐️ 提供一个入口把链上数据拉下来并写到 NodeStore.allNodes
//@MainActor
//func reloadNodesFromChain(pageSize: UInt = 200) async {
//    do {
//        let nodes = try await NodeStore.initialize()
//
//        print("✅ fetched \(nodes.count) nodes")
//        
//    } catch {
//        print("❌ fetch nodes failed:", error)
//    }
//}

let nodeInfoABI = """
[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"ipAddr","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"regin","type":"bytes32"}],"name":"deleteIPAddr","type":"event"},{"inputs":[{"internalType":"string","name":"ipaddress","type":"string"}],"name":"IP2PGP","outputs":[{"internalType":"string","name":"pgp","type":"string"},{"internalType":"string","name":"pgpKey","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"ipaddress","type":"string"},{"internalType":"string","name":"regionName","type":"string"},{"internalType":"string","name":"pgp","type":"string"},{"internalType":"string","name":"pgpKey","type":"string"},{"internalType":"address","name":"owner","type":"address"}],"name":"addNode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"adminList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"addr","type":"address"},{"internalType":"bool","name":"status","type":"bool"}],"name":"changeAddressInAdminlist","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"start","type":"uint256"},{"internalType":"uint256","name":"length","type":"uint256"}],"name":"getAllNodes","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"PGP","type":"string"},{"internalType":"string","name":"PGPKey","type":"string"},{"internalType":"string","name":"ip_addr","type":"string"},{"internalType":"string","name":"regionName","type":"string"}],"internalType":"struct nodeInfo[]","name":"allNodes","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getAllRegions","outputs":[{"internalType":"string[]","name":"Regions","type":"string[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"getOwnerIPs","outputs":[{"internalType":"string[]","name":"ips","type":"string[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"pgpKey","type":"string"}],"name":"getPGPKeyIPaddress","outputs":[{"internalType":"string","name":"ipaddress","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"regionName","type":"string"}],"name":"getReginNodes","outputs":[{"internalType":"string[]","name":"nodes","type":"string[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"regionName","type":"string"}],"name":"getRegionNodes","outputs":[{"internalType":"string[]","name":"nodes","type":"string[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"text","type":"string"}],"name":"hashString","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"id2ip","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"ipaddress","type":"string"}],"name":"id2node","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"idOwner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ip2id","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddress2PGP","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddress2owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddress2pgpKey","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddressExisting","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddressToRegion","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"pgpKey2ipaddress","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"pgpKeyToPGP","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"pgp","type":"string"},{"internalType":"string","name":"pgpKey","type":"string"},{"internalType":"string","name":"ipaddress","type":"string"}],"name":"pgpUpdate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"regionExisting","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"regionList","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"ipaddress","type":"string"}],"name":"removeNode","outputs":[],"stateMutability":"nonpayable","type":"function"}]
"""
