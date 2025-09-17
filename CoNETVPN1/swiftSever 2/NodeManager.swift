import Foundation

struct Node: Codable {
    var country: String
    var ip_addr: String
    var region: String
    var armoredPublicKey: String
    var nftNumber: String
    
    enum CodingKeys: String, CodingKey {
        case country = "country"
        case ip_addr = "ip_addr"
        case region = "region"
        case armoredPublicKey = "armoredPublicKey"
        case nftNumber = "nftNumber"
    }
}

class NodeManager {
    static var allNodes: [Node] = []
    
    // 用于保存节点数组到 UserDefaults
    static func saveNodes() {
        let encoder = JSONEncoder()
        do {
            let data = try encoder.encode(allNodes) // 将 Node 数组编码为 JSON 数据
            UserDefaults.standard.set(data, forKey: "allNodes") // 存储到 UserDefaults
            UserDefaults.standard.synchronize() // 
            print("Nodes successfully saved!")
        } catch {
            print("Failed to save nodes: \(error.localizedDescription)")
        }
    }
    
    // 用于从 UserDefaults 中读取节点数组
    static func loadNodes() {
        let decoder = JSONDecoder()
        guard let data = UserDefaults.standard.data(forKey: "allNodes") else {
            print("No nodes data found in UserDefaults")
            return
        }
        
        do {
            allNodes = try decoder.decode([Node].self, from: data) // 将 JSON 数据解码为 Node 数组
            print("Nodes successfully loaded!")
        } catch {
            print("Failed to load nodes: \(error.localizedDescription)")
        }
    }
    
    
    
    static func newsaveNodes(_ nodes: [Node], withKey key: String) {
            let encoder = JSONEncoder()
            do {
                let data = try encoder.encode(nodes) // 将 Node 数组编码为 JSON 数据
                UserDefaults.standard.set(data, forKey: key) // 存储到 UserDefaults
                UserDefaults.standard.synchronize()
                print("Nodes successfully saved!")
            } catch {
                print("Failed to save nodes: \(error.localizedDescription)")
            }
        }
        
    static func newloadNodes(forKey key: String) -> [Node] {
            let decoder = JSONDecoder()
            guard let data = UserDefaults.standard.data(forKey: key) else {
                print("No nodes data found in UserDefaults")
                return []
            }
            
            do {
                let nodes = try decoder.decode([Node].self, from: data) // 将 JSON 数据解码为 Node 数组
                print("Nodes successfully loaded!")
                return nodes
            } catch {
                print("Failed to load nodes: \(error.localizedDescription)")
                return []
            }
        }
    
    
}







