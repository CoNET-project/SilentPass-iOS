import NetworkExtension

class VPNManager {
#if DEBUG
@inline(__always)
    private func log(_ msg: @autoclosure () -> String) {
        NSLog("[VPNManager] %@", msg())
    }
#else
    @inline(__always)
    private func log(_ msg: @autoclosure () -> String) { }
#endif
    let layerMinus: LayerMinus
    private let vpnIdentifier = "com.fx168.CoNETVPN1.CoNETVPN1.VPN1"

    init(layerMinus: LayerMinus) {
        self.layerMinus = layerMinus
    }

    func refresh() {
        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
            guard let self = self else { return }

            if let error = error {
                self.log("❌ Failed to load preferences: \(error.localizedDescription)")
                return
            }

            let existing = managers?.first(where: {
                ($0.protocolConfiguration as? NETunnelProviderProtocol)?.providerBundleIdentifier == self.vpnIdentifier
            })

            if let existing = existing {
                self.log("✅ 已存在 VPN 配置，尝试激活")
                self.activateTunnel(existing)
            } else {
                self.log("🆕 没找到 VPN 配置，准备创建")
                self.createTunnelWithRetry()
            }
        }
    }

    private func createTunnelWithRetry(retryCount: Int = 2, delay: TimeInterval = 1.5) {
        let manager = makeManager()

        func saveAndLoad(retriesLeft: Int) {
            manager.saveToPreferences { [weak self] error in
                guard let self = self else { return }
                
                if let error = error {
                    self.log("❌ Failed to save VPN config: \(error.localizedDescription)")
                    
                    if retriesLeft > 0 {
                        DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
                            saveAndLoad(retriesLeft: retriesLeft - 1)
                        }
                        return
                    }
                    log("当前 VPN 状态 为 Failed to save VPN config 发送关闭状态！")
                    
                    return
                }

                manager.loadFromPreferences { [weak self] error in
                    guard let self = self else { return }
                    
                    if let error = error {
                        
                        self.log("❌ Failed to load VPN config after save: \(error.localizedDescription)")
                        return
                    }

                    self.log("✅ VPN 配置创建成功，准备激活")
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

            
                log("VPN 连接成功，开始清理其他 VPN 配置")

                // 加载所有配置（iOS 17+ 需要权限配置）
                NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
                    guard let self = self else { return }
                    
                    guard error == nil else {
                        self.log("加载所有配置失败: \(error!.localizedDescription)")
                        return
                    }

                    for item in managers ?? [] {
                        if item != manager && item.connection.status == .disconnected {
                            self.log("删除未连接的 VPN 配置: \(item.localizedDescription ?? "未知")")
                            item.removeFromPreferences { [weak self] error in
                                guard let self = self else { return }
                                
                                if let error = error {
                                    self.log("删除失败: \(error.localizedDescription)")
                                } else {
                                    self.log("删除成功")
                                }
                            }
                        }
                    }
                }
            

        } catch {
            self.log("启动 VPN 失败: \(error.localizedDescription)")
            self.createTunnelWithRetry()
        }
    }

    // 🔥 终极版stopVPN - 立即同步停止所有VPN
    func stopVPN() {
        self.log("🔥 启动终极VPN停止模式 - 立即停止所有VPN隧道")
        
        // 🚨 关键修复：使用信号量确保同步执行
        let semaphore = DispatchSemaphore(value: 0)
        var stoppedCount = 0

        // 若被主线程调用，立刻切到后台，避免与内部的信号量形成互锁
        if Thread.isMainThread {
            self.log("⚠️ stopVPN called on main thread, offloading to background queue")
            DispatchQueue.global(qos: .userInitiated).async { [weak self] in
                self?.stopVPN()
            }
            return
        }
        
        // 第一步：停止所有隧道提供商VPN（同步执行）
        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
            guard let self = self else { 
                semaphore.signal()
                return 
            }
            defer { semaphore.signal() } // 确保信号量被释放
            
            if let error = error {
                self.log("❌ 加载VPN配置失败: \(error.localizedDescription)")
                return
            }
            
            guard let managers = managers, !managers.isEmpty else {
                self.log("📋 没有找到隧道提供商VPN配置")
                return
            }
            
            self.log("📋 找到 \(managers.count) 个VPN配置，开始立即停止")
            
            for (index, manager) in managers.enumerated() {
                let bundleId = (manager.protocolConfiguration as? NETunnelProviderProtocol)?.providerBundleIdentifier ?? "unknown"
                let status = manager.connection.status
                let description = manager.localizedDescription ?? "无名VPN"
                
                self.log("🔍 处理VPN \(index+1): \(description)")
                self.log("   Bundle ID: \(bundleId)")
                self.log("   状态: \(self.vpnStatusString(status))")
                
                // 🚨 关键修复：无论什么状态都立即停止
                self.log("🛑 立即强制停止VPN: \(description)")
                
                // 立即禁用On-Demand
                manager.isOnDemandEnabled = false
                
                // 立即发送停止信号
                manager.connection.stopVPNTunnel()
                stoppedCount += 1
                self.log("   ✅ 已立即发送停止信号")
                
                // 🚨 关键修复：立即保存设置，不等待回调
                manager.saveToPreferences { [weak self] error in
                    guard let self = self else { return }
                    
                    if let error = error {
                        self.log("   ⚠️ 保存设置失败: \(error.localizedDescription)")
                    } else {
                        self.log("   ✅ 设置已保存")
                    }
                }
            }
            
            self.log("✅ 已向 \(stoppedCount) 个VPN发送立即停止信号")
        }
        
        // 等待第一步完成
        _ = semaphore.wait(timeout: .now() + 3.0) // 最多等待3秒
        
        // 第二步：同时停止系统级VPN
        self.log("🔄 立即检查并停止系统VPN...")
        let systemSemaphore = DispatchSemaphore(value: 0)
        
        NEVPNManager.shared().loadFromPreferences { [weak self] error in
            guard let self = self else {
                systemSemaphore.signal()
                return
            }
            defer { systemSemaphore.signal() }
            
            if let error = error {
                self.log("❌ 加载系统VPN配置失败: \(error.localizedDescription)")
                return
            }
            
            let systemVPN = NEVPNManager.shared()
            let status = systemVPN.connection.status
            
            self.log("🔍 系统VPN状态: \(self.vpnStatusString(status))")
            
            // 立即停止系统VPN
            systemVPN.isOnDemandEnabled = false
            systemVPN.connection.stopVPNTunnel()
            self.log("🛑 已立即发送系统VPN停止信号")
            
            // 立即保存系统VPN设置
            systemVPN.saveToPreferences { [weak self] error in
                guard let self = self else { return }
                
                if let error = error {
                    self.log("⚠️ 保存系统VPN设置失败: \(error.localizedDescription)")
                } else {
                    self.log("✅ 系统VPN设置已保存")
                }
            }
        }
        
        // 等待系统VPN停止完成
        _ = systemSemaphore.wait(timeout: .now() + 2.0)
        
        // 第三步：立即执行额外清理（不等待延时）
        self.performImmediateCleanup()
        
        self.log("🔥 终极VPN停止流程已完成")
    }
    
    // 🚨 立即执行清理，不等待延时
    private func performImmediateCleanup() {
        self.log("🧹 执行立即清理工作...")
        
        // 立即再次检查并停止所有VPN
        let cleanupSemaphore = DispatchSemaphore(value: 0)
        
        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
            guard let self = self else {
                cleanupSemaphore.signal()
                return
            }
            defer { cleanupSemaphore.signal() }
            
            guard let managers = managers else { return }
            
            var stillRunning = 0
            for manager in managers {
                let status = manager.connection.status
                if status != .disconnected && status != .invalid {
                    stillRunning += 1
                    // 对所有非断开状态的VPN强制再次停止
                    manager.connection.stopVPNTunnel()
                    self.log("🔁 强制再次停止VPN，状态: \(self.vpnStatusString(status))")
                }
            }
            
            if stillRunning > 0 {
                self.log("⚠️ 检测到 \(stillRunning) 个VPN可能仍在运行，已发送额外停止信号")
            } else {
                self.log("✅ 所有VPN都已处于断开状态")
            }
        }
        
        // 等待清理完成
        _ = cleanupSemaphore.wait(timeout: .now() + 2.0)
        
        // 最后检查系统VPN
        NEVPNManager.shared().loadFromPreferences { [weak self] _ in
            guard let self = self else { return }
            
            let status = NEVPNManager.shared().connection.status
            if status != .disconnected && status != .invalid {
                self.log("🔁 系统VPN可能仍在运行，强制再次停止")
                NEVPNManager.shared().connection.stopVPNTunnel()
            } else {
                self.log("✅ 系统VPN已断开")
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
    
    // 🆕 新增：强制终止所有VPN进程的方法（核弹级）
    func nuclearStopVPN() {
        self.log("☢️ 启动核弹级VPN停止 - 强制终止所有可能的VPN连接")
        
        // 1. 停止所有已知的VPN配置
        self.stopVPN()
        
        // 2. 尝试通过不同的API停止VPN
        DispatchQueue.global(qos: .userInitiated).async {
            // 方法1：通过所有可能的Manager类型
            self.stopAllPossibleVPNTypes()
            
            // 方法2：清理所有VPN相关配置
            self.cleanupAllVPNConfigurations()
        }
    }
    
    private func stopAllPossibleVPNTypes() {
        self.log("🔍 尝试停止所有可能的VPN类型...")
        
        // 停止Personal VPN
        NEVPNManager.shared().connection.stopVPNTunnel()
        
        // 尝试停止所有网络扩展
        if #available(iOS 9.0, *) {
            // 加载所有可能的网络扩展配置
            NEVPNManager.shared().loadFromPreferences { _ in
                NEVPNManager.shared().connection.stopVPNTunnel()
            }
        }
    }
    
    private func cleanupAllVPNConfigurations() {
        self.log("🧹 清理所有VPN配置...")
        
        // 禁用所有VPN配置的自动连接
        NETunnelProviderManager.loadAllFromPreferences { managers, _ in
            managers?.forEach { manager in
                manager.isEnabled = false
                manager.isOnDemandEnabled = false
                manager.saveToPreferences(completionHandler: nil)
            }
        }
        
        // 禁用系统VPN的自动连接
        NEVPNManager.shared().loadFromPreferences { _ in
            let systemVPN = NEVPNManager.shared()
            systemVPN.isEnabled = false
            systemVPN.isOnDemandEnabled = false
            systemVPN.saveToPreferences(completionHandler: nil)
        }
    }
    
    // 获取当前所有VPN状态（调试用）
    func getAllVPNStatus(completion: @escaping (String) -> Void) {
        var statusReport = "🔍 当前系统中所有VPN状态:\n\n"
        
        // 检查隧道提供商VPN
        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
            guard let self = self else { return }
            
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
            NEVPNManager.shared().loadFromPreferences { [weak self] error in
                guard let self = self else { return }
                
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
        proto.disconnectOnSleep = false   // ← 禁用“睡眠断开”
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
