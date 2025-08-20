//
//  PacketTunnelProvider.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Updated with advanced DNS Interception.
//
import NetworkExtension
import Network




// MARK: - Main Provider Class

class PacketTunnelProvider: NEPacketTunnelProvider {

    private var connectionManager: ConnectionManager?

    override func startTunnel(options: [String : NSObject]?, completionHandler: @escaping (Error?) -> Void) {
        NSLog("[PacketTunnelProvider] Starting tunnel...")
        
        // 1. Configure the virtual network interface using a safe private range.
        let tunnelNetworkSettings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: "8.8.8.8")
        
        let tunnelIPAddress = "172.16.0.1"
        let tunnelSubnetMask = "255.255.255.0"
        let fakeDNSServer = "172.16.0.2"
        
        tunnelNetworkSettings.ipv4Settings = NEIPv4Settings(addresses: [tunnelIPAddress], subnetMasks: [tunnelSubnetMask])
        tunnelNetworkSettings.ipv4Settings?.includedRoutes = [NEIPv4Route.default()]
        
        // 2. CRITICAL: Set up the DNS trap. All DNS queries will be sent to our fake server.
        tunnelNetworkSettings.dnsSettings = NEDNSSettings(servers: [fakeDNSServer])
        NSLog("[PacketTunnelProvider] DNS trap set for \(fakeDNSServer)")

        // 3. Apply the settings
        setTunnelNetworkSettings(tunnelNetworkSettings) { [weak self] error in
            guard let self = self else { return }

            if let error = error {
                NSLog("[PacketTunnelProvider] Failed to set tunnel network settings: \(error)")
                completionHandler(error)
                return
            }
            
            // 4. Initialize the new ConnectionManager
            self.connectionManager = ConnectionManager(packetFlow: self.packetFlow, fakeDNSServer: fakeDNSServer)
            
            Task {
                await self.connectionManager?.start()
            }
            
            NSLog("[PacketTunnelProvider] Tunnel started successfully.")
            completionHandler(nil)
        }
    }
    
    override func stopTunnel(with reason: NEProviderStopReason, completionHandler: @escaping () -> Void) {
        NSLog("[PacketTunnelProvider] Stopping tunnel.")
        connectionManager = nil
        completionHandler()
    }
}
