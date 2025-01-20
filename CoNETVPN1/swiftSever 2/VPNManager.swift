import NetworkExtension

class VPNManager {
    
    let layerMinus: LayerMinus!
//    private var manager: NEVPNManager?
    init(layerMinus: LayerMinus) {
        self.layerMinus = layerMinus
//        manager = NEVPNManager.shared()
//                NotificationCenter.default.addObserver(
//                    self,
//                    selector: #selector(vpnStatusDidChange),
//                    name: .NEVPNStatusDidChange,
//                    object: nil
//                )
    }
    
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
        
        let egressNodes = self.layerMinus.egressNodes_JSON()
        let entryNodes = self.layerMinus.entryNodes_JSON()
        let privateKeyData = self.layerMinus.privateKeyAromed
        
       
        // 构造 options 字典
            let options: [String: NSObject] = [
                "entryNodes": entryNodes as NSObject,
                "egressNodes": egressNodes as NSObject,
                "privateKey": privateKeyData as NSObject
            ]

        // 启动 VPN 并传递参数
        do {
            try manager.connection.startVPNTunnel(options: options)
            NSLog("MiningProcess VPN tunnel started successfully with egressNodes \(self.layerMinus.egressNodes[0].ip_addr)")
//            self.layerMinus.miningProcess.stop(keep: false)
        } catch {
            NSLog("Failed to start VPN tunnel: \(error.localizedDescription)")
        }
        
        
        
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
        manager.localizedDescription = "CoNET VPN"
        let VirtualIP = "127.0.0.1:8888"
        let proto = NETunnelProviderProtocol()
        proto.serverAddress = VirtualIP // 替换为实际服务器地址
//        proto.includeAllNetworks = true
        if #available(iOS 14.2, *) {
            proto.includeAllNetworks = false
            proto.excludeLocalNetworks = true
        }
        
        proto.providerBundleIdentifier = "com.fx168.CoNETVPN1.CoNETVPN1.VPN" // 替换为实际 Bundle ID
        manager.protocolConfiguration = proto
        manager.isEnabled = true
        
        return manager
    }
    
 
}
