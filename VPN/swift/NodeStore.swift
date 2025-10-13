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
    
    enum CodingKeys: String, CodingKey {
        case country, ip_addr, region, armoredPublicKey, nftNumber
    }
}

enum NodeFetchError: Error { case badURL, badAddress, decode }

// ✅ 用于承接链上返回的 nodeInfo
private struct OnchainNode {
    let id: UInt64
    let PGP: String
    let PGPKey: String
    let ip_addr: String
    let regionName: String
}

// ✅ 从合约分页抓取，并**映射**为你项目的 Node
func fetchAllNodesViaWeb3swift(
    rpc: String = "https://mainnet-rpc.conet.network",
    contractHex: String = "0x2DF3302d0c9aC19BE01Ee08ce3DDA841BdcF6F03",
    pageSize: UInt = 200,
    maxPages: Int = 10_000
) async throws -> [Node] {

    guard let url = URL(string: rpc) else { throw NodeFetchError.badURL }
    let web3 = try await Web3.new(url)
    guard let caddr = EthereumAddress(contractHex) else { throw NodeFetchError.badAddress }

    let abi = nodeInfoABI

    // ⛏️ 修正：不要从 web3.eth 调，用 web3.contract(...)
    let contract = web3.contract(abi, at: caddr)!
    var start = BigUInt(0)
    let length = BigUInt(pageSize)
    var allOnchain: [OnchainNode] = []
    var page = 0

    while page < maxPages {
        let result = try await contract.createReadOperation(
            "getAllNodes",
            parameters: [start, length]
        )!.callContractMethod()


        // 兼容两种返回形态：命名输出/未命名输出
        let arrAny: [Any]
        if let named = result["allNodes"] as? [Any] {
            arrAny = named
        } else if let first = result.values.first as? [Any] {
            arrAny = first
        } else {
            throw NodeFetchError.decode
        }

        let pageNodes = try decodeOnchainNodes(arrAny)
        allOnchain.append(contentsOf: pageNodes)

        if pageNodes.count < Int(pageSize) { break }
        start += length
        page += 1

        // 轻微节流（可选）
        try await Task.sleep(nanoseconds: 50_000_000)
    }

    // 把 OnchainNode -> 你的 Node
    let mapped: [Node] = allOnchain.map { oc in
        let country = deriveCountry(fromRegionName: oc.regionName)
        return Node(
            country: country,
            ip_addr: oc.ip_addr,
            region: oc.regionName,
            armoredPublicKey: oc.PGP,          // 映射到你的 armoredPublicKey
            nftNumber: String(oc.id)           // 把合约里的 id 转成字符串
        )
    }

    return dedupeNodes(mapped)
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

    static func getNodeDelay(_ node: Node, timeout: TimeInterval = 3.0) async -> Int {
        await withCheckedContinuation { continuation in
            let host = NWEndpoint.Host(node.ip_addr)
            guard let port = NWEndpoint.Port(rawValue: 80) else {
                continuation.resume(returning: -1); return
            }

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
}

// ⭐️ 提供一个入口把链上数据拉下来并写到 NodeStore.allNodes
@MainActor
func reloadNodesFromChain(pageSize: UInt = 200) async {
    do {
        let nodes = try await fetchAllNodesViaWeb3swift(pageSize: pageSize)
        NodeStore.allNodes = nodes
        print("✅ fetched \(nodes.count) nodes")
        
    } catch {
        print("❌ fetch nodes failed:", error)
    }
}

let nodeInfoABI = """
[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"bytes32","name":"ipAddr","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"regin","type":"bytes32"}],"name":"deleteIPAddr","type":"event"},{"inputs":[{"internalType":"string","name":"ipaddress","type":"string"}],"name":"IP2PGP","outputs":[{"internalType":"string","name":"pgp","type":"string"},{"internalType":"string","name":"pgpKey","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"ipaddress","type":"string"},{"internalType":"string","name":"regionName","type":"string"},{"internalType":"string","name":"pgp","type":"string"},{"internalType":"string","name":"pgpKey","type":"string"},{"internalType":"address","name":"owner","type":"address"}],"name":"addNode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"adminList","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"addr","type":"address"},{"internalType":"bool","name":"status","type":"bool"}],"name":"changeAddressInAdminlist","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"start","type":"uint256"},{"internalType":"uint256","name":"length","type":"uint256"}],"name":"getAllNodes","outputs":[{"components":[{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"PGP","type":"string"},{"internalType":"string","name":"PGPKey","type":"string"},{"internalType":"string","name":"ip_addr","type":"string"},{"internalType":"string","name":"regionName","type":"string"}],"internalType":"struct nodeInfo[]","name":"allNodes","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getAllRegions","outputs":[{"internalType":"string[]","name":"Regions","type":"string[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"getOwnerIPs","outputs":[{"internalType":"string[]","name":"ips","type":"string[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"pgpKey","type":"string"}],"name":"getPGPKeyIPaddress","outputs":[{"internalType":"string","name":"ipaddress","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"regionName","type":"string"}],"name":"getReginNodes","outputs":[{"internalType":"string[]","name":"nodes","type":"string[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"regionName","type":"string"}],"name":"getRegionNodes","outputs":[{"internalType":"string[]","name":"nodes","type":"string[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"text","type":"string"}],"name":"hashString","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"id2ip","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"string","name":"ipaddress","type":"string"}],"name":"id2node","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"idOwner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ip2id","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddress2PGP","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddress2owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddress2pgpKey","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddressExisting","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"ipaddressToRegion","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"pgpKey2ipaddress","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"pgpKeyToPGP","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"pgp","type":"string"},{"internalType":"string","name":"pgpKey","type":"string"},{"internalType":"string","name":"ipaddress","type":"string"}],"name":"pgpUpdate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"","type":"string"}],"name":"regionExisting","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"regionList","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"ipaddress","type":"string"}],"name":"removeNode","outputs":[],"stateMutability":"nonpayable","type":"function"}]
"""
