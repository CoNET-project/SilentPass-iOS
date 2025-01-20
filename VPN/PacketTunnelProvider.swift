//
//  PacketTunnelProvider.swift
//  vpn-tunnel
//
//  Created by peter xie on 2021-10-18.
//

import NetworkExtension
import os.log

class PacketTunnelProvider: NEPacketTunnelProvider {
    var localServer: Server?
    let port = 8888
//    let log = Logger(subsystem: "com.example.myvpnapp", category: "packet-tunnel")
    
    var server: Server!
    var layerMinus:LayerMinus!
    let localhost = "127.0.0.1"
    let VirtualIP = "10.222.222.222"
    
    override init() {
        super.init()
//        self.log.log(level: .debug, "隧道启动")
        self.layerMinus = LayerMinus(port: self.port)
        self.server = Server(port: UInt16(self.port), layerMinus: self.layerMinus)
        self.server.start()
    }
        
    override func startTunnel(options: [String : NSObject]?, completionHandler: @escaping (Error?) -> Void) {
        

        // 确保 options 不为 nil
           guard let options = options else {
               completionHandler(NSError(domain: "NEPacketTunnelProviderError", code: -1, userInfo: [NSLocalizedDescriptionKey: "No options provided"]))
               return
           }

           
        let entryNodesStr = options["entryNodes"] as? String ?? ""
        let egressNodesStr = options["egressNodes"] as? String ?? ""
        let privateKey = options["privateKey"] as? String ?? ""
        
        
        let entryNodes = layerMinus.nodeJSON(nodeJsonStr: entryNodesStr)
        let egressNodes = layerMinus.nodeJSON(nodeJsonStr: egressNodesStr)
        let entryNodesIpAddress = entryNodes.map { $0.ip_addr }
        let egressNodesIpAddress = egressNodes.map { $0.ip_addr }
        NSLog("MiningProcess PacketTunnelProvider VPN 隧道启动 egressNodes [\(egressNodesIpAddress)] entryNodes [\(entryNodesIpAddress)]")
        self.layerMinus.startInVPN(privateKey: privateKey, entryNodes: entryNodes, egressNodes: egressNodes, port: self.port)
        
        
        let delay = DispatchTime.now() + 1
        DispatchQueue.main.asyncAfter(deadline: delay) {
                
            self.setup(entryNodes: entryNodesIpAddress, egressNodes: egressNodesIpAddress, completionHandler: completionHandler)
                
        }
    
        
    }
    
    private func setup(entryNodes: [String], egressNodes: [String], completionHandler: @escaping (Error?) -> Void)
    {
        let settings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: localhost)
        
        
    
//        let settings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: VirtualIP)
        settings.mtu = 1500
//        settings.dnsSettings = NEDNSSettings(servers: ["8.8.8.8", "1.1.1.1"])
//        settings.dnsSettings?.matchDomains = [""]
        //      proxy setup
        
        
        
        let proxySettings = NEProxySettings()
        
        proxySettings.httpEnabled = true
        proxySettings.httpServer = NEProxyServer(address: localhost, port: self.port)
        
        proxySettings.httpsEnabled = true
        proxySettings.httpsServer = NEProxyServer(address: localhost, port: self.port)
        
        
        proxySettings.autoProxyConfigurationEnabled = true
        proxySettings.excludeSimpleHostnames = true
        proxySettings.proxyAutoConfigurationURL = URL(string: "http://\(localhost):\(self.port)/pac")
        
        
        let entryNodesArray = entryNodes
        let egressNodesArray = egressNodes
               
        // 合并数组并强制转换为 [String]
        let combinedArray = (entryNodesArray + egressNodesArray).compactMap { $0 as? String }
        
        NSLog("PacketTunnelProvider setup egressNodesArray \(egressNodesArray) entryNodesArray \(entryNodesArray) ")

        
//                localServer?.putNodes(entryNodes: entryNodesArray1, egressNodes: egressNodesArray1, privateKey: privateKey)
        
        // 添加额外的条目到 exceptionList
        let dns = DomainFilter()
        let addedArray = dns.getIPArray()
        
        let updatedArray = addedArray + combinedArray + [
            "10.0.0.0/8",
            "169.254.0.0/16",
            "172.16.0.0/14",
            "192.168.0.0/16",
            "127.0.0.0/8",
            "1.1.1.1/32",
            "8.8.8.8/32",
            "*.local",
            "conet.network","*.conet.network",
//
//            "*.apple.com","*.push-apple.com.akadns.net",
//            "*.icloud.com",
//            "apple-mapkit.com","*.apple-mapkit.com","*.firefox.com","*.mozilla.com","*.icloud-content.com",
//            "*.cdn-apple.com","*.aplle.com","*.cdn-apple.com","*.apple.news","*.apple.com.edgecast.net",
//            "*.cn","qq.com","*.qq.com",
            
        ]
         
        
        print(updatedArray) // 验证合并结果
        // 更新 proxySettings 的 exceptionList
        proxySettings.exceptionList = updatedArray
        
        settings.proxySettings = proxySettings
        
        //指定用于 VPN 隧道的 IP 地址列表。这些地址将被分配给用户空间中的设备，以便网络流量可以通过这些地址路由。
//        var ips:[String] = [settings.tunnelRemoteAddress]
//
//        if ip != nil {
//            ips.append(ip!)
//        }
        /* ipv4 settings */
            let ipv4Settings = NEIPv4Settings(
                addresses: [settings.tunnelRemoteAddress],
                subnetMasks: ["255.255.255.255"]
            )
        //        定义了所有能够发送到此 VPN 隧道的目的地地址范围。这些地址会通过 VPN 隧道进行转发，处理包括数据包传输等功能。
        ipv4Settings.includedRoutes = [NEIPv4Route.default()]
//        ipv4Settings.includedRoutes = [ip]
//        定义了不通过 VPN 隧道发送的地址范围。这些地址通常指向本地网络或其他在隧道之外的资源。
        
        
        // 遍历 combinedArray 数组
        var nodeArray = [NEIPv4Route]()
        for node in combinedArray {
            let route = NEIPv4Route(destinationAddress: node, subnetMask: "255.255.255.255")
            nodeArray.append(route) // 将路由添加到可变数组
        }
        for ip in dns.getAll_IpAddr() {
            let mask = dns.getMask(ip)
            nodeArray.append(NEIPv4Route(destinationAddress: ip, subnetMask: mask))
        }

        nodeArray.append(NEIPv4Route(destinationAddress: "10.0.0.0", subnetMask: "255.0.0.0"))
        nodeArray.append(NEIPv4Route(destinationAddress: "169.254.0.0", subnetMask: "255.255.0.0"))
        nodeArray.append(NEIPv4Route(destinationAddress: "172.16.0.0", subnetMask: "255.240.255.0"))
        nodeArray.append(NEIPv4Route(destinationAddress: "192.168.0.0", subnetMask: "255.255.0.0"))
        nodeArray.append(NEIPv4Route(destinationAddress: "127.0.0.0", subnetMask: "255.0.0.0"))
        nodeArray.append(NEIPv4Route(destinationAddress: "1.1.1.1", subnetMask: "255.255.255.255"))
        nodeArray.append(NEIPv4Route(destinationAddress: "8.8.8.8", subnetMask: "255.255.255.255"))
        ipv4Settings.excludedRoutes = nodeArray
        
        
            
        settings.ipv4Settings = ipv4Settings
//        settings.ipv4Settings = NEIPv4Settings(addresses: ["10.0.0.252"], subnetMasks: ["255.255.255.255"])
        setTunnelNetworkSettings(settings) { error in
            
            if let error = error {
                NSLog("Did setup tunnel error: \(String(describing: error))")
            }
            
            completionHandler(error)
           
        }
        
    }
    
    func getIPAddress() -> String? {
        var address : String?

        // Get list of all interfaces on the local machine:
        var ifaddr : UnsafeMutablePointer<ifaddrs>?
        guard getifaddrs(&ifaddr) == 0 else { return nil }
        guard let firstAddr = ifaddr else { return nil }

        // For each interface ...
        for ifptr in sequence(first: firstAddr, next: { $0.pointee.ifa_next }) {
            let interface = ifptr.pointee

            // Check for IPv4 or IPv6 interface:
            let addrFamily = interface.ifa_addr.pointee.sa_family
            if addrFamily == UInt8(AF_INET) || addrFamily == UInt8(AF_INET6) {

                // Check interface name:
                // wifi = ["en0"]
                // wired = ["en2", "en3", "en4"]
                // cellular = ["pdp_ip0","pdp_ip1","pdp_ip2","pdp_ip3"]
                
                let name = String(cString: interface.ifa_name)
                if  name == "en1" || name == "en0" || name == "en2" || name == "en3" || name == "en4" || name == "pdp_ip0" || name == "pdp_ip1" || name == "pdp_ip2" || name == "pdp_ip3" {

                    // Convert interface address to a human readable string:
                    var hostname = [CChar](repeating: 0, count: Int(NI_MAXHOST))
                    getnameinfo(interface.ifa_addr, socklen_t(interface.ifa_addr.pointee.sa_len),
                                &hostname, socklen_t(hostname.count),
                                nil, socklen_t(0), NI_NUMERICHOST)
                    address = String(cString: hostname)
                }
            }
        }
        freeifaddrs(ifaddr)

        return address
    }
    
    override func stopTunnel(with reason: NEProviderStopReason, completionHandler: @escaping () -> Void) {
        print("停止启动")
        localServer?.stop()
        localServer?.layerMinus.miningProcess.keep = false
        localServer?.layerMinus.miningProcess?.stop(false)
        server.stop()
        // Add code here to start the process of stopping the tunnel.
        completionHandler()
    }
    
    override func handleAppMessage(_ messageData: Data, completionHandler: ((Data?) -> Void)?) {
        // Add code here to handle the message.
        if let handler = completionHandler {
            
            print("发送数据到app")
            handler(messageData)
        }
    }
    
    override func sleep(completionHandler: @escaping () -> Void) {
        // Add code here to get ready to sleep.
        print("隧道睡眠了")
        completionHandler()
    }
    
    override func wake() {
        // Add code here to wake up.
    }
}
