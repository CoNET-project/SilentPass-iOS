//  PacketTunnelProvider.swift
//  vpn-tunnel
//
//  Created by peter xie on 2021-10-18.
//

import NetworkExtension
import os.log


func nodeJSON (nodeJsonStr: String) -> [Node] {
    let decoder = JSONDecoder()
    do {
        let nodes = try decoder.decode([Node].self, from: nodeJsonStr.data(using: .utf8)!)
        return nodes
    } catch {
        return []
    }
    
}


class PacketTunnelProvider: NEPacketTunnelProvider {
    var socksServer: Server
    let port = 8888
    let localhost = "127.0.0.1"
    let VirtualIP = "10.222.222.222"

    override init() {
        self.socksServer = Server(port:8888)
        do {
            try socksServer.start()
            NSLog("PacketTunnelProvider SOCKS server started.")
        } catch {
            NSLog("Failed to start SOCKS server: \(error)")
        }
        
    }
    
    public static func createPACSettings() -> NEProxySettings {
        let proxySettings = NEProxySettings()
        
        // 启用PAC自动配置
        proxySettings.autoProxyConfigurationEnabled = true
        proxySettings.proxyAutoConfigurationURL = URL(string: "http://127.0.0.1:8888/pac")
        proxySettings.httpEnabled = false
        proxySettings.httpsEnabled = false
        // 排除简单主机名
        proxySettings.excludeSimpleHostnames = true
        
        // 设置例外列表（直接连接）
        proxySettings.exceptionList = [
            "localhost",
            "127.0.0.1",
            "::1",
            "*.local",
            "169.254/16",
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16"
        ]
        
        
        // 匹配所有域名
        proxySettings.matchDomains = [""]
        
        return proxySettings
    }

    override func startTunnel(options: [String : NSObject]?, completionHandler: @escaping (Error?) -> Void) {
        
        NSLog("[PacketTunnelProvider] Starting tunnel...")
        
        guard let options = options else {
            completionHandler(NSError(domain: "NEPacketTunnelProviderError", code: -1,
                                      userInfo: [NSLocalizedDescriptionKey: "No options provided"]))
            return
        }
        
    
        
        // Configure TUN settings with APNs exclusions
        let settings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: "127.0.0.1")
        let ip = "172.16.0.1"
        let mask = "255.255.255.0"
        
        let v4 = NEIPv4Settings(addresses: [ip], subnetMasks: [mask])
        
        // ✅ 修改路由配置：默认走隧道，后续通过 excludedRoutes 进行精确绕行
        v4.includedRoutes = [
            NEIPv4Route.default()
        ]
        
        // ✅ 新增：排除苹果推送网段和其他本地网络
        v4.excludedRoutes = [
            // 苹果推送服务网段 (17.0.0.0/8) - 核心APNs网段
            NEIPv4Route(destinationAddress: "17.0.0.0", subnetMask: "255.0.0.0"),
            
            // 本地网络
            NEIPv4Route(destinationAddress: "192.168.0.0", subnetMask: "255.255.0.0"),
            NEIPv4Route(destinationAddress: "10.0.0.0", subnetMask: "255.0.0.0"),
            NEIPv4Route(destinationAddress: "127.0.0.0", subnetMask: "255.0.0.0"),
            NEIPv4Route(destinationAddress: "169.254.0.0", subnetMask: "255.255.0.0"),
            // 其他苹果服务网段
            NEIPv4Route(destinationAddress: "23.0.0.0", subnetMask: "255.0.0.0"),        // Apple CDN
            NEIPv4Route(destinationAddress: "143.224.0.0", subnetMask: "255.240.0.0"),   // Apple 服务
            NEIPv4Route(destinationAddress: "144.178.0.0", subnetMask: "255.254.0.0"),   // Apple 服务备用
            NEIPv4Route(destinationAddress: "199.47.192.0", subnetMask: "255.255.224.0"), // Apple 推送备用
            NEIPv4Route(destinationAddress: "38.102.126.50", subnetMask: "255.0.0.0"),
            NEIPv4Route(destinationAddress: "172.67.215.169", subnetMask: "255.255.255.0"),
            NEIPv4Route(destinationAddress: "1.1.1.1", subnetMask: "255.255.255.0"),
            NEIPv4Route(destinationAddress: "8.8.8.8", subnetMask: "255.255.255.0"),
            NEIPv4Route(destinationAddress: "208.67.222.222", subnetMask: "255.255.255.0"),
            // 🔥 腾讯/微信 IP 段
                NEIPv4Route(destinationAddress: "101.32.0.0", subnetMask: "255.255.0.0"),     // 腾讯云
                NEIPv4Route(destinationAddress: "101.33.0.0", subnetMask: "255.255.0.0"),     // 腾讯云
                NEIPv4Route(destinationAddress: "101.89.0.0", subnetMask: "255.255.0.0"),     // 微信
                NEIPv4Route(destinationAddress: "101.91.0.0", subnetMask: "255.255.0.0"),     // 微信
                NEIPv4Route(destinationAddress: "101.226.0.0", subnetMask: "255.255.0.0"),    // 微信
                NEIPv4Route(destinationAddress: "101.227.0.0", subnetMask: "255.255.0.0"),    // 微信
                NEIPv4Route(destinationAddress: "103.7.28.0", subnetMask: "255.255.252.0"),   // 微信海外
                NEIPv4Route(destinationAddress: "109.244.0.0", subnetMask: "255.255.0.0"),    // 腾讯云
                NEIPv4Route(destinationAddress: "110.52.193.0", subnetMask: "255.255.255.0"), // 微信
                NEIPv4Route(destinationAddress: "110.53.0.0", subnetMask: "255.255.0.0"),     // 微信
                NEIPv4Route(destinationAddress: "111.30.0.0", subnetMask: "255.254.0.0"),     // 腾讯
                NEIPv4Route(destinationAddress: "112.53.0.0", subnetMask: "255.255.0.0"),     // 微信
                NEIPv4Route(destinationAddress: "112.60.0.0", subnetMask: "255.252.0.0"),     // 微信
                NEIPv4Route(destinationAddress: "112.64.0.0", subnetMask: "255.192.0.0"),     // 微信
                NEIPv4Route(destinationAddress: "112.90.0.0", subnetMask: "255.254.0.0"),     // 腾讯
                NEIPv4Route(destinationAddress: "113.96.0.0", subnetMask: "255.224.0.0"),     // 腾讯
                NEIPv4Route(destinationAddress: "115.159.0.0", subnetMask: "255.255.0.0"),    // 腾讯云
                NEIPv4Route(destinationAddress: "117.184.0.0", subnetMask: "255.248.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "119.28.0.0", subnetMask: "255.255.0.0"),     // 腾讯云
                NEIPv4Route(destinationAddress: "119.29.0.0", subnetMask: "255.255.0.0"),     // 腾讯云
                NEIPv4Route(destinationAddress: "119.147.0.0", subnetMask: "255.255.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "120.198.0.0", subnetMask: "255.255.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "120.232.0.0", subnetMask: "255.252.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "121.51.0.0", subnetMask: "255.255.0.0"),     // 腾讯
                NEIPv4Route(destinationAddress: "129.226.0.0", subnetMask: "255.255.0.0"),    // 腾讯云国际
                NEIPv4Route(destinationAddress: "140.206.0.0", subnetMask: "255.255.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "140.207.0.0", subnetMask: "255.255.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "150.109.0.0", subnetMask: "255.255.0.0"),    // 腾讯云
                NEIPv4Route(destinationAddress: "162.62.0.0", subnetMask: "255.255.0.0"),     // 腾讯云海外
                NEIPv4Route(destinationAddress: "180.96.0.0", subnetMask: "255.254.0.0"),     // 腾讯
                NEIPv4Route(destinationAddress: "180.163.0.0", subnetMask: "255.255.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "182.254.0.0", subnetMask: "255.255.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "183.192.0.0", subnetMask: "255.192.0.0"),    // 腾讯
                NEIPv4Route(destinationAddress: "203.205.128.0", subnetMask: "255.255.128.0"), // 腾讯
                NEIPv4Route(destinationAddress: "211.95.0.0", subnetMask: "255.255.0.0"),     // 腾讯
                NEIPv4Route(destinationAddress: "220.196.0.0", subnetMask: "255.252.0.0"),    // 腾讯
                
        ]
        
        settings.ipv4Settings = v4
        settings.proxySettings = PacketTunnelProvider.createPACSettings()
        settings.mtu = 1400
    
        

        
        setTunnelNetworkSettings(settings) { error in
            if let error = error {
                NSLog("❌ PacketTunnelProvider.setTunnelNetworkSettings error: \(error)")
                completionHandler(error)
            } else {
                NSLog("✅ PacketTunnelProvider.setTunnelNetworkSettings succeeded, calling completionHandler(nil)")
                
                
                let entryNodesStr = options["entryNodes"] as? String ?? ""
                let egressNodesStr = options["egressNodes"] as? String ?? ""
                let privateKey = options["privateKey"] as? String ?? ""
                let entryNodes = nodeJSON(nodeJsonStr: entryNodesStr)
                let egressNodes = nodeJSON(nodeJsonStr: egressNodesStr)
                
                do {
                    try self.socksServer.start()
                    self.socksServer.layerMinusInit(privateKey: privateKey, entryNodes: entryNodes, egressNodes: egressNodes)
                    NSLog("PacketTunnelProvider SOCKS server started with entryNodes \(entryNodes.count) egressNodes \(egressNodes.count).")
                } catch {
                    NSLog("Failed to start SOCKS server: \(error) entryNodes \(entryNodes.count) egressNodes \(egressNodes.count)")
                }
                
                
                completionHandler(nil)
            }
        }
        


        
        
    }

    private func setup(entryNodes: [String], egressNodes: [String], completionHandler: @escaping (Error?) -> Void) {
    }

    override func stopTunnel(with reason: NEProviderStopReason, completionHandler: @escaping () -> Void) {
        NSLog("🛑 PacketTunnelProvider.stopTunnel called, reason: \(reason.rawValue)")
        socksServer.stop()
        completionHandler()
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
