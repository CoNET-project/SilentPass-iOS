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
    var server: Server!
    var layerMinus: LayerMinus!
    let localhost = "127.0.0.1"
    let VirtualIP = "10.222.222.222"

    override init() {
        super.init()
        self.layerMinus = LayerMinus(port: self.port)
        self.server = Server(port: UInt16(self.port), layerMinus: self.layerMinus)
        self.server.start()
    }

    override func startTunnel(options: [String : NSObject]?, completionHandler: @escaping (Error?) -> Void) {
        NSLog("🚀 PacketTunnelProvider.startTunnel called, options: \(options ?? [:])")
        guard let options = options else {
            completionHandler(NSError(domain: "NEPacketTunnelProviderError", code: -1,
                                      userInfo: [NSLocalizedDescriptionKey: "No options provided"]))
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
        self.layerMinus.startInVPN(privateKey: privateKey,
                                    entryNodes: entryNodes,
                                    egressNodes: egressNodes,
                                    port: self.port)

        self.setup(entryNodes: entryNodesIpAddress,
                   egressNodes: egressNodesIpAddress,
                   completionHandler: completionHandler)
    }

    private func setup(entryNodes: [String], egressNodes: [String], completionHandler: @escaping (Error?) -> Void) {
        let settings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: localhost)
        settings.mtu = 1500

        let proxySettings = NEProxySettings()
        proxySettings.httpEnabled = true
        proxySettings.httpServer = NEProxyServer(address: localhost, port: self.port)
        proxySettings.httpsEnabled = true
        proxySettings.httpsServer = NEProxyServer(address: localhost, port: self.port)

        let dns = DomainFilter()
        let combinedArray = dns.getIPArray() + entryNodes + egressNodes + [
            "10.0.0.0/8", "169.254.0.0/16", "172.16.0.0/14",
            "192.168.0.0/16", "127.0.0.0/8", "*.local"
        ]
        proxySettings.exceptionList = combinedArray
        settings.proxySettings = proxySettings

        let ipv4Settings = NEIPv4Settings(addresses: [localhost], subnetMasks: ["255.255.255.255"])
        ipv4Settings.includedRoutes = [NEIPv4Route.default()]
        var nodeArray = [NEIPv4Route]()
        for node in combinedArray {
            let route = NEIPv4Route(destinationAddress: node, subnetMask: "255.255.255.255")
            nodeArray.append(route)
        }
        ipv4Settings.excludedRoutes = nodeArray
        settings.ipv4Settings = ipv4Settings

        setTunnelNetworkSettings(settings) { error in
            if let error = error {
                NSLog("❌ PacketTunnelProvider.setTunnelNetworkSettings error: \(error)")
                completionHandler(error)
            } else {
                NSLog("✅ PacketTunnelProvider.setTunnelNetworkSettings succeeded, calling completionHandler(nil)")
                completionHandler(nil)
            }
        }
    }

    override func stopTunnel(with reason: NEProviderStopReason, completionHandler: @escaping () -> Void) {
        NSLog("🛑 PacketTunnelProvider.stopTunnel called, reason: \(reason.rawValue)")
        localServer?.stop()
        server.stop()
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
