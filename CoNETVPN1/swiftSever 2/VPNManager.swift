import NetworkExtension

class VPNManager {

    let layerMinus: LayerMinus
    private let vpnIdentifier = "com.fx168.CoNETVPN1.CoNETVPN1.VPN1"

    init(layerMinus: LayerMinus) {
        self.layerMinus = layerMinus
    }

    func refresh() {
        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
            guard let self = self else { return }

            if let error = error {
                NSLog("❌ Failed to load preferences: \(error.localizedDescription)")
                return
            }

            let existing = managers?.first(where: {
                ($0.protocolConfiguration as? NETunnelProviderProtocol)?.providerBundleIdentifier == self.vpnIdentifier
            })

            if let existing = existing {
                NSLog("✅ 已存在 VPN 配置，尝试激活")
                self.activateTunnel(existing)
            } else {
                NSLog("🆕 没找到 VPN 配置，准备创建")
                self.createTunnelWithRetry()
            }
        }
    }

    private func createTunnelWithRetry(retryCount: Int = 2, delay: TimeInterval = 1.5) {
        let manager = makeManager()

        func saveAndLoad(retriesLeft: Int) {
            manager.saveToPreferences { error in
                if let error = error {
                    NSLog("❌ Failed to save VPN config: \(error.localizedDescription)")
                    
                    if retriesLeft > 0 {
                        DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
                            saveAndLoad(retriesLeft: retriesLeft - 1)
                        }
                        return
                    }
                    print("当前 VPN 状态 为 Failed to save VPN config 发送关闭状态！")
                    
                    return
                }

                manager.loadFromPreferences { error in
                    if let error = error {
                        
                        NSLog("❌ Failed to load VPN config after save: \(error.localizedDescription)")
                        return
                    }

                    NSLog("✅ VPN 配置创建成功，准备激活")
                    self.activateTunnel(manager)
                }
            }
        }

        saveAndLoad(retriesLeft: retryCount)
    }

    private func activateTunnel(_ manager: NETunnelProviderManager) {
        let egressNodes = self.layerMinus.egressNodes_JSON()
        let entryNodes = self.layerMinus.entryNodes_JSON()
        let privateKeyData = self.layerMinus.privateKeyAromed

        let options: [String: NSObject] = [
            "entryNodes": entryNodes as NSObject,
            "egressNodes": egressNodes as NSObject,
            "privateKey": privateKeyData as NSObject
        ]
        startAndCleanOtherVPNs(manager: manager, options: options)
//        do {
//            try manager.connection.startVPNTunnel(options: options)
//            
//            startAndCleanOtherVPNs()
//            
//            NSLog("✅ VPN tunnel started successfully with egressNodes \(self.layerMinus.egressNodes.first?.ip_addr ?? "")")
//        } catch {
//            NSLog("❌ Failed to start VPN tunnel: \(error.localizedDescription)")
//            
//          
//            
//            
//            self.createTunnelWithRetry()
//        }
    }
    
    
    func startAndCleanOtherVPNs(manager: NETunnelProviderManager, options: [String: NSObject]? = nil) {
        do {
            try manager.connection.startVPNTunnel(options: options)

            
                print("VPN 连接成功，开始清理其他 VPN 配置")

                // 加载所有配置（iOS 17+ 需要权限配置）
                NETunnelProviderManager.loadAllFromPreferences { managers, error in
                    guard error == nil else {
                        print("加载所有配置失败: \(error!.localizedDescription)")
                        return
                    }

                    for item in managers ?? [] {
                        if item != manager && item.connection.status == .disconnected {
                            print("删除未连接的 VPN 配置: \(item.localizedDescription ?? "未知")")
                            item.removeFromPreferences { error in
                                if let error = error {
                                    print("删除失败: \(error.localizedDescription)")
                                } else {
                                    print("删除成功")
                                }
                            }
                        }
                    }
                }
            

        } catch {
            print("启动 VPN 失败: \(error.localizedDescription)")
            self.createTunnelWithRetry()
        }
    }

    func stopVPN() {
        NETunnelProviderManager.loadAllFromPreferences { managers, error in
            if let error = error {
                NSLog("❌ Failed to load preferences: \(error.localizedDescription)")
                return
            }

            guard let managers = managers else {
                NSLog("ℹ️ No VPN configurations found.")
                return
            }

            for manager in managers where
                (manager.protocolConfiguration as? NETunnelProviderProtocol)?.providerBundleIdentifier == self.vpnIdentifier {
                manager.connection.stopVPNTunnel()
                NSLog("🛑 VPN tunnel stopped successfully.")
            }
        }
    }

    private func makeManager() -> NETunnelProviderManager {
        let manager = NETunnelProviderManager()

        let ruleEvaluate = NEOnDemandRuleEvaluateConnection()
        let excludedDomains = [
            "wechat.com", "weixin.qq.com", "wx.qq.com", "qpic.cn", "qlogo.cn",
            "tenpay.com", "pay.weixin.qq.com", "short.weixin.qq.com",
            "szextshort.weixin.qq.com", "res.wx.qq.com", "wx.qlogo.cn",
            "cdn.weixin.qq.com", "game.weixin.qq.com", "wxsnsdy.wxs.qq.com",
            "open.weixin.qq.com", "api.weixin.qq.com",
            "work.weixin.qq.com", "qy.weixin.qq.com", "wecom.qq.com",
            "alipay.com", "alipayobjects.com", "m.alipay.com", "api.alipay.com"
        ]
        ruleEvaluate.connectionRules = [
            NEEvaluateConnectionRule(matchDomains: excludedDomains, andAction: .neverConnect)
        ]
        manager.onDemandRules = [ruleEvaluate]
        manager.isOnDemandEnabled = true

        manager.localizedDescription = "CoNET VPN"
        let proto = NETunnelProviderProtocol()
        proto.serverAddress = "127.0.0.1:8888"
        proto.providerBundleIdentifier = vpnIdentifier
        if #available(iOS 14.2, *) {
            proto.includeAllNetworks = false
            proto.excludeLocalNetworks = true
        }
        manager.protocolConfiguration = proto
        manager.isEnabled = true

        return manager
    }
}
