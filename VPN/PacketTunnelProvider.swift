//  PacketTunnelProvider.swift
//  vpn-tunnel
//
//  Created by peter xie on 2021-10-18.
//


import NetworkExtension
import os.log
import vpn2socks


// —— Helpers ——

// 轻量 IPv4 校验
private func isValidIPv4(_ s: String) -> Bool {
    let parts = s.split(separator: ".")
    guard parts.count == 4 else { return false }
    for p in parts {
        guard let v = Int(p), (0...255).contains(v) else { return false }
        // 允许 "0"；不做严格前导零限制，真实环境更宽容
    }
    return true
}



// 为了 JSON 解码写个局部 Node（避免与你项目里已有 Node 重名冲突）
private struct _Node: Codable {
    let ip_addr: String
}

// 同时兼容三种放法：
// 1) JSON 字符串（[Node]）或 Data
// 2) NSArray<[NSDictionary]>（键含 "ip_addr"）
// 3) NSArray<[String]>（元素可能是 "IP" 或 "IP:port"）
private func decodeNodeIPs(from any: NSObject?) -> [String] {
    guard let any else { return [] }

    // NSData（JSON）
    if let data = any as? NSData {
        if let nodes = try? JSONDecoder().decode([_Node].self, from: data as Data) {
            return nodes.map(\.ip_addr)
        }
    }

    // NSString（JSON 文本 或 单个 "IP[:port]"）
    if let s = any as? NSString {
        let str = s.trimmingCharacters(in: .whitespacesAndNewlines)
        if let data = str.data(using: .utf8),
           let nodes = try? JSONDecoder().decode([_Node].self, from: data) {
            return nodes.map(\.ip_addr)
        }
        // 非 JSON：当作 "IP[:port]"
        let head = str.split(separator: ":").first.map(String.init) ?? str
        return [head]
    }

    // NSArray
    if let arr = any as? NSArray {
        var out: [String] = []
        for e in arr {
            if let d = e as? NSDictionary, let ip = d["ip_addr"] as? String {
                out.append(ip)
            } else if let s = e as? NSString {
                let head = s.components(separatedBy: [":","/"]).first ?? (s as String)
                out.append(head)
            }
        }
        return out
    }

    return []
}

// 从 options 里抓两组节点的 IPv4，转成 /32
private func collectNodeCIDRs(options: [String: NSObject]?) -> [String] {
    var ips = Set<String>()
    for key in ["entryNodes", "egressNodes"] {
        for ip in decodeNodeIPs(from: options?[key]) where isValidIPv4(ip) {
            ips.insert(ip)
        }
    }
    return ips.map { "\($0)/32" }
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

// --- 有序收集节点 /32 CIDR：entry 在前，egress 在后 ---
func collectNodeCIDRsInOrder(_ options: [String: NSObject]?) -> [String] {
    // 你之前已经有 decodeNodeIPs/isValidIPv4 等工具；这里复用
    func cidrs(from any: NSObject?) -> [String] {
        let ips = decodeNodeIPs(from: any)  // -> [String]，提取 ip_addr
        return ips.filter { isValidIPv4($0) }.map { "\($0)/32" }
    }
    let entry = cidrs(from: options?["entryNodes"])
    let egress = cidrs(from: options?["egressNodes"])
    // 按 entry→egress 的顺序返回（此处不去重，统一在下面做“有序去重拼接”）
    return entry + egress
}

@inline(__always)
func orderedUniqueConcat(front: [String], back: [String]) -> [String] {
    var seen = Set<String>()
    var out: [String] = []
    out.reserveCapacity(front.count + back.count)

    for s in front {
        let t = s.trimmingCharacters(in: .whitespacesAndNewlines)
        if !t.isEmpty, seen.insert(t).inserted { out.append(t) }
    }
    for s in back {
        let t = s.trimmingCharacters(in: .whitespacesAndNewlines)
        if !t.isEmpty, seen.insert(t).inserted { out.append(t) }
    }
    return out
}


class PacketTunnelProvider: vpn2socks.PacketTunnelProvider {
    private var socksServer: ServerNIO?
    let port = 8888
    

    override init() {
        super.init()
        let s = ServerNIO(port: 8888)
        self.socksServer = s
        do {
            try self.socksServer?.start()
            NSLog("PacketTunnelProvider SOCKS server started.")
        } catch {
            NSLog("Failed to start SOCKS server: \(error)")
        }
    }

    override func startTunnel(options: [String : NSObject]?, completionHandler: @escaping (Error?) -> Void) {
        
        // 先拷贝一份可变 options
        var opts: [String: NSObject] = options ?? [:]
        let nodeCIDRs = collectNodeCIDRsInOrder(options)
        let allowlistCIDRs = Allowlist.ipv4CIDRsRaw
        let merged = orderedUniqueConcat(front: nodeCIDRs, back: allowlistCIDRs)

        // 传给 vpn2socks 的扩展键（保持既有 Key）
        opts["LM.extraExcludedCIDRs"] = (merged as NSArray)

        // （可选）打印前几项确认顺序：节点 /32 应该出现在最前面
        NSLog("[PTP] LM.extraExcludedCIDRs (head) %@", Array(merged.prefix(30)) as NSArray)

        
        super.startTunnel(options: opts) { error in
            // 5. 在核心逻辑完成后，你可以执行后续的自定义操作
            if let _ = error {
                NSLog("PacketTunnelProvider Target: Core logic failed. Cleaning up.")
                // 处理错误
            } else {
                NSLog("PacketTunnelProvider Target: Core logic succeeded. Tunnel is up.")
                guard let options = options else {
                    completionHandler(NSError(domain: "NEPacketTunnelProviderError", code: -1,
                                              userInfo: [NSLocalizedDescriptionKey: "No options provided"]))
                    return
                }
                
                let entryNodesStr = options["entryNodes"] as? String ?? ""
                let egressNodesStr = options["egressNodes"] as? String ?? ""
                let privateKey = options["privateKey"] as? String ?? ""
                let entryNodes = nodeJSON(nodeJsonStr: entryNodesStr)
                let egressNodes = nodeJSON(nodeJsonStr: egressNodesStr)
                
                
                do {
                    try self.socksServer?.start()
                    self.socksServer?.layerMinusInit(privateKey: privateKey, entryNodes: entryNodes, egressNodes: egressNodes)
                    NSLog("PacketTunnelProvider SOCKS server started.")
                } catch {
                    NSLog("Failed to start SOCKS server: \(error)")
                }
                
                // 最后，调用 completionHandler 通知系统
                completionHandler(error)
                
            }
            
            
        }
        
    }


    override func stopTunnel(with reason: NEProviderStopReason, completionHandler: @escaping () -> Void) {
        NSLog("🛑 PacketTunnelProvider.stopTunnel called, reason: \(reason.rawValue)")
        socksServer?.stop()
        socksServer = nil
        super.stopTunnel(with: reason) {
            NSLog("PacketTunnelProvider: Core tunnel stopped. Finalizing cleanup.")
            // 核心隧道停止后的最终清理
            completionHandler()
        }
    }

    override func handleAppMessage(_ messageData: Data, completionHandler: ((Data?) -> Void)?) {
        NSLog("📩 PacketTunnelProvider.handleAppMessage called")
        completionHandler?(messageData)
    }

    override func sleep(completionHandler: @escaping () -> Void) {
        NSLog("💤 PacketTunnelProvider.sleep called")
        completionHandler()
    }

    override func wake() {
        NSLog("🔔 PacketTunnelProvider.wake called")
    }
}
extension Notification.Name {
    static let didUpdateConnectionNodes = Notification.Name("didUpdateConnectionNodes")
}
