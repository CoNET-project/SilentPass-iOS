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

    // 🔥 激进版stopVPN - 无差别停止所有VPN
    func stopVPN() {
        print("🔥 启动激进VPN停止模式 - 停止所有VPN隧道")
        
        // 第一步：通过NETunnelProviderManager停止所有隧道提供商VPN
        self.stopAllTunnelProviderVPNs()
        
        // 第二步：通过NEVPNManager停止系统级VPN
        self.stopSystemVPN()
        
        // 第三步：额外的清理工作
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
            self.performAdditionalCleanup()
        }
    }
    
    // 停止所有隧道提供商VPN
    private func stopAllTunnelProviderVPNs() {
        NETunnelProviderManager.loadAllFromPreferences { managers, error in
            if let error = error {
                NSLog("❌ 加载VPN配置失败: \(error.localizedDescription)")
                // 即使加载失败也要继续其他停止方法
                return
            }
            
            guard let managers = managers, !managers.isEmpty else {
                print("📋 没有找到隧道提供商VPN配置")
                return
            }
            
            print("📋 找到 \(managers.count) 个VPN配置，开始无差别停止")
            
            for (index, manager) in managers.enumerated() {
                let bundleId = (manager.protocolConfiguration as? NETunnelProviderProtocol)?.providerBundleIdentifier ?? "unknown"
                let status = manager.connection.status
                let description = manager.localizedDescription ?? "无名VPN"
                
                print("🔍 处理VPN \(index+1): \(description)")
                print("   Bundle ID: \(bundleId)")
                print("   状态: \(self.vpnStatusString(status))")
                
                // 更稳顺序：先禁用 On-Demand 并保存 → 再 stop（主线程）
                let turnOffOnDemand = {
                    if manager.isOnDemandEnabled {
                        manager.isOnDemandEnabled = false
                            print("   🔧 已禁用按需连接")
                    }
                }
                turnOffOnDemand()
                manager.saveToPreferences { err in
                    if let err = err {
                        print("   ⚠️ 保存设置失败: \(err.localizedDescription)，仍尝试停止")
                    } else {
                        print("   ✅ 设置已保存")
                    }
                    manager.loadFromPreferences { _ in
                        DispatchQueue.main.async {
                            print("🛑 强制停止VPN: \(description)")
                            manager.connection.stopVPNTunnel()
                            print("   ✅ 已在主线程发送停止信号")
                        }
                    }
                }
            }
        }
    }
    
    // 停止系统VPN
    private func stopSystemVPN() {
        print("🔄 检查并停止系统VPN...")
        
        NEVPNManager.shared().loadFromPreferences { error in
            // 更稳顺序：先禁用 On-Demand 并保存 → 再 stop（主线程）
            if systemVPN.isOnDemandEnabled {
                systemVPN.isOnDemandEnabled = false
                print("🔧 已禁用系统VPN按需连接")
            }
            systemVPN.saveToPreferences { err in
                if let err = err {
                    print("⚠️ 保存系统VPN设置失败: \(err.localizedDescription)，仍尝试停止")
                } else {
                    print("✅ 系统VPN设置已保存")
                }
                NEVPNManager.shared().loadFromPreferences { _ in
                    DispatchQueue.main.async {
                        NEVPNManager.shared().connection.stopVPNTunnel()
                            print("🛑 已在主线程发送系统VPN停止信号")
                    }
                }
            }
        }
    }
    
    // 执行额外的清理工作
    private func performAdditionalCleanup() {
        print("🧹 执行额外清理工作...")
        
        // 再次检查是否还有VPN在运行
        NETunnelProviderManager.loadAllFromPreferences { managers, error in
            guard let managers = managers else { return }
            
            var stillRunning = 0
            for manager in managers {
                let status = manager.connection.status
                if status == .connected || status == .connecting || status == .reasserting {
                    stillRunning += 1
                    // 对仍在运行的VPN再次「保存关闭 On-Demand → 主线程 stop」
                    if manager.isOnDemandEnabled {
                        manager.isOnDemandEnabled = false
                    }
                    manager.saveToPreferences { _ in
                        manager.loadFromPreferences { _ in
                            DispatchQueue.main.async {
                                manager.connection.stopVPNTunnel()
                                    print("🔁 再次停止仍在运行的VPN（主线程）")
                            }
                        }
                    }
                }
            }
            
            if stillRunning > 0 {
                print("⚠️ 检测到 \(stillRunning) 个VPN仍在运行，已发送额外停止信号")
            } else {
                print("✅ 所有VPN已停止")
            }
        }
        
        // 检查系统VPN
        NEVPNManager.shared().loadFromPreferences { _ in
            let status = NEVPNManager.shared().connection.status
            if status != .disconnected {
                print("🔁 系统VPN仍在运行，再次停止（保存→主线程）")
                let mgr = NEVPNManager.shared()
                if mgr.isOnDemandEnabled { mgr.isOnDemandEnabled = false }
                mgr.saveToPreferences { _ in
                    NEVPNManager.shared().loadFromPreferences { _ in
                        DispatchQueue.main.async {
                            NEVPNManager.shared().connection.stopVPNTunnel()
                        }
                    }
                }
            }
        }
    }
    
    // VPN状态转换为可读字符串
    private func vpnStatusString(_ status: NEVPNStatus) -> String {
        switch status {
        case .invalid: return "无效"
        case .disconnected: return "已断开"
        case .connecting: return "连接中"
        case .connected: return "已连接"
        case .reasserting: return "重新连接中"
        case .disconnecting: return "断开连接中"
        @unknown default: return "未知状态(\(status.rawValue))"
        }
    }
    
    // 获取当前所有VPN状态（调试用）
    func getAllVPNStatus(completion: @escaping (String) -> Void) {
        var statusReport = "🔍 当前系统中所有VPN状态:\n\n"
        
        // 检查隧道提供商VPN
        NETunnelProviderManager.loadAllFromPreferences { managers, error in
            if let error = error {
                statusReport += "❌ 获取隧道VPN失败: \(error.localizedDescription)\n\n"
            } else if let managers = managers, !managers.isEmpty {
                statusReport += "📱 隧道提供商VPN (\(managers.count) 个):\n"
                for (index, manager) in managers.enumerated() {
                    let bundleId = (manager.protocolConfiguration as? NETunnelProviderProtocol)?.providerBundleIdentifier ?? "unknown"
                    let status = self.vpnStatusString(manager.connection.status)
                    let description = manager.localizedDescription ?? "无名VPN"
                    let isEnabled = manager.isEnabled ? "启用" : "禁用"
                    let isOnDemand = manager.isOnDemandEnabled ? "启用" : "禁用"
                    
                    statusReport += "\(index + 1). \(description)\n"
                    statusReport += "   状态: \(status)\n"
                    statusReport += "   Bundle ID: \(bundleId)\n"
                    statusReport += "   配置启用: \(isEnabled)\n"
                    statusReport += "   按需连接: \(isOnDemand)\n\n"
                }
            } else {
                statusReport += "📱 没有找到隧道提供商VPN\n\n"
            }
            
            // 检查系统VPN
            NEVPNManager.shared().loadFromPreferences { error in
                if let error = error {
                    statusReport += "❌ 获取系统VPN失败: \(error.localizedDescription)\n"
                } else {
                    let systemVPN = NEVPNManager.shared()
                    let status = self.vpnStatusString(systemVPN.connection.status)
                    let isEnabled = systemVPN.isEnabled ? "启用" : "禁用"
                    let isOnDemand = systemVPN.isOnDemandEnabled ? "启用" : "禁用"
                    
                    statusReport += "⚙️ 系统VPN:\n"
                    statusReport += "   状态: \(status)\n"
                    statusReport += "   配置启用: \(isEnabled)\n"
                    statusReport += "   按需连接: \(isOnDemand)\n"
                }
                
                completion(statusReport)
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