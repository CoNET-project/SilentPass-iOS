//
//  PacketTunnelProvider.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Updated with advanced DNS Interception and concurrency safety fixes.
//
import NetworkExtension
import Network

open class PacketTunnelProvider: NEPacketTunnelProvider {

    private var connectionManager: ConnectionManager?

    public override init() { super.init() }

    nonisolated
    open override func startTunnel(options: [String : NSObject]?,
                                   completionHandler: @escaping (Error?) -> Void) {
        NSLog("[PacketTunnelProvider] Starting tunnel...")

        // 1) Configure TUN
        let settings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: "127.0.0.1")
        let ip = "172.16.0.1"
        let mask = "255.255.255.0"
        let fakeDNS = "172.16.0.2"
        
        let v4 = NEIPv4Settings(addresses: [ip], subnetMasks: [mask])
        
        // Include routes for our fake IP range (RFC 2544 test net)
        // Route 198.18.0.0/15 so packets to fake IPs go into the tunnel
        v4.includedRoutes = [
            .default(), // full-tunnel; remove this if you only want to hijack fake IPs
            NEIPv4Route(destinationAddress: "198.18.0.0", subnetMask: "255.254.0.0")
        ]

        NSLog("[PacketTunnelProvider] Routing includes 198.18.0.0/15 for fake IPs")

        
        // Exclude local networks to avoid conflicts
        v4.excludedRoutes = [
            NEIPv4Route(destinationAddress: "192.168.0.0", subnetMask: "255.255.0.0"),
            NEIPv4Route(destinationAddress: "10.0.0.0", subnetMask: "255.0.0.0"),
//            NEIPv4Route(destinationAddress: "172.16.0.0", subnetMask: "255.240.0.0"),
            NEIPv4Route(destinationAddress: "127.0.0.0", subnetMask: "255.0.0.0"),
            NEIPv4Route(destinationAddress: "169.254.0.0", subnetMask: "255.255.0.0")
        ]
        
        settings.ipv4Settings = v4
        
//        let v6 = NEIPv6Settings(addresses: ["fd00::1"], networkPrefixLengths: [64])
//        v6.includedRoutes = [ NEIPv6Route.default() ]   // 把所有 IPv6 也进隧道
//        settings.ipv6Settings = v6

        settings.dnsSettings = NEDNSSettings(servers: [fakeDNS])
        settings.mtu = 1400  // Set MTU to avoid fragmentation issues
        
        NSLog("[PacketTunnelProvider] DNS trap set for \(fakeDNS)")
        NSLog("[PacketTunnelProvider] Routing includes 10.8.0.0/16 for fake IPs")

        // 2) Apply settings
        setTunnelNetworkSettings(settings) { [weak self] err in
            guard let self = self else { return }

            if let err = err {
                NSLog("[PacketTunnelProvider] setTunnelNetworkSettings failed: \(err)")
                completionHandler(err)
                return
            }

            // 3) Create Sendable wrapper and start ConnectionManager
            let flowWrapper = SendablePacketFlow(packetFlow: self.packetFlow)
            let mgr = ConnectionManager(packetFlow: flowWrapper, fakeDNSServer: fakeDNS)
            self.connectionManager = mgr
            mgr.start()

            NSLog("[PacketTunnelProvider] Tunnel started successfully.")
            completionHandler(nil)
        }
    }

    nonisolated
    open override func stopTunnel(with reason: NEProviderStopReason,
                                  completionHandler: @escaping () -> Void) {
        NSLog("[PacketTunnelProvider] Stopping tunnel.")
        self.connectionManager = nil
        completionHandler()
    }
}
