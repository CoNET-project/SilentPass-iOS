import NetworkExtension

class VPNManager {
    

    
    // 刷新配置，启动 VPN
    func refresh() {
        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
            if let error = error {
                NSLog("Failed to load preferences: \(error.localizedDescription)")
                return
            }
            
            guard let self = self else { return }
            guard let tunnels = managers else {
                self.createTunnel()
                return
            }
            
            NSLog("Loaded preferences successfully, tunnels.count = \(tunnels.count)")
            
            // 如果已有配置且有效，直接激活
            if let existingTunnel = tunnels.first(where: { $0.localizedDescription == "fx168" }) {
                self.activateTunnel(existingTunnel)
                return
            }
            
            // 否则删除所有旧配置并重新创建
            let group = DispatchGroup()
            for tunnel in tunnels {
                group.enter()
                tunnel.removeFromPreferences { error in
                    if let error = error {
                        NSLog("Failed to remove old tunnel: \(error.localizedDescription)")
                    }
                    group.leave()
                }
            }
            
            group.notify(queue: .main) {
                NSLog("All old tunnels removed. Creating a new tunnel.")
                self.createTunnel()
            }
        }
    }
    
    // 创建新 VPN 配置
    private func createTunnel() {
        let tunnel = makeManager()
        tunnel.saveToPreferences { error in
            if let error = error {
                NSLog("Failed to save new tunnel: \(error.localizedDescription)")
                return
            }
            
            tunnel.loadFromPreferences { error in
                if let error = error {
                    NSLog("Failed to load new tunnel: \(error.localizedDescription)")
                    return
                }
                
                self.activateTunnel(tunnel)
            }
        }
    }
    
    // 激活 VPN
    private func activateTunnel(_ manager: NETunnelProviderManager) {
        

    }
    
    // 关闭 VPN
    func stopVPN() {
        NETunnelProviderManager.loadAllFromPreferences { managers, error in
            if let error = error {
                NSLog("Failed to load preferences: \(error.localizedDescription)")
                return
            }
            
            guard let managers = managers, let activeManager = managers.first(where: {
                $0.connection.status == .connected || $0.connection.status == .connecting
            }) else {
                NSLog("No active VPN to stop.")
                return
            }
            
            activeManager.connection.stopVPNTunnel()
            NSLog("VPN tunnel stopped successfully.")
        }
    }
    
    // 创建 NETunnelProviderManager 实例
    private func makeManager() -> NETunnelProviderManager {
        let manager = NETunnelProviderManager()
        manager.localizedDescription = "CoNET"
        let VirtualIP = "10.222.222.222"
        let proto = NETunnelProviderProtocol()
        proto.serverAddress = VirtualIP
        if #available(iOS 14.2, *) {
            proto.includeAllNetworks = false
            proto.excludeLocalNetworks = true
        }
        
        proto.providerBundleIdentifier = "com.fx168.maxVPN.CoNETVPN.macVPN" // 替换为实际 Bundle ID
        manager.protocolConfiguration = proto
        manager.isEnabled = true
        
        return manager
    }
    
    @objc private func vpnStatusDidChange() {
//        guard let connection = manager?.connection else {
//            print("Failed to access VPN connection.")
//            return
//        }
//
//        let statusDescription = statusToString(connection.status)
//        print("VPN status changed: \(statusDescription)")
        
        // Notify the app about status changes
        
        
        
        getVPNConfigurationStatus()
        
    }
    
    private func statusToString(_ status: NEVPNStatus) -> String {
            switch status {
            case .invalid:
                return "我的vpn Invalid (VPN configuration is invalid)"
            case .disconnected:
                return "我的vpnDisconnected (VPN is not active)"
            case .connecting:
                return "我的vpnConnecting (VPN is in the process of connecting)"
            case .connected:
                return "我的vpnConnected (VPN is active)"
            case .reasserting:
                return "我的vpnReasserting (VPN connection is being re-established)"
            case .disconnecting:
                return "我的vpnDisconnecting (VPN is in the process of disconnecting)"
            @unknown default:
                return "我的vpnUnknown status (\(status.rawValue))"
            }
        }
    
    
    func getVPNConfigurationStatus() {
        NETunnelProviderManager.loadAllFromPreferences { managers, error in
            if let error = error {
                print("Failed to load VPN configurations: \(error.localizedDescription)")
                return
            }

            guard let managers = managers else {
                print("No VPN configurations found")
                return
            }

            for manager in managers {
                print("VPN configuration: \(manager.localizedDescription ?? "Unknown")")
                print("Status: \(manager.connection.status)")
                if manager.localizedDescription == "CoNET"
                {
                    if manager.connection.status.rawValue == 1 ||   manager.connection.status.rawValue == 3
                    {
                        NotificationCenter.default.post(
                            name: Notification.Name("VPNStatusChanged"),
                            object: manager.connection.status
                        )
                    }
                    
                }
                
            }
        }
    }
}
