//
//  PacketTunnelProvider.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Updated with complete concurrency safety for Swift 6 + APNs direct routing
//
import NetworkExtension
import Network
import os.log


private enum OptKey {
    static let extraExcludedCIDRs = "LM.extraExcludedCIDRs"
}

private let logger = Logger(subsystem: "com.vpn2socks", category: "PacketTunnel")

// PacketTunnelProvider with proper concurrency
open class PacketTunnelProvider: NEPacketTunnelProvider {
    
    // Thread-safe storage using actors
    private let stateManager = TunnelStateManager()
    private let connectionStore = ConnectionStore()
    
    public override init() {
        super.init()
    }
    
    open override func startTunnel(options: [String : NSObject]?,
                                   completionHandler: @escaping (Error?) -> Void) {
        NSLog("[PacketTunnelProvider] Starting tunnel...")
        
        // Capture what we need before async context
        let packetFlow = self.packetFlow
        let flowWrapper = SendablePacketFlow(packetFlow: packetFlow)
        let stateManager = self.stateManager
        let connectionStore = self.connectionStore
        
        // Lower thread priority
        Thread.current.qualityOfService = .utility
        
        // Configure TUN settings with APNs exclusions
        let settings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: "127.0.0.1")
        let ip = "172.16.0.1"
        let mask = "255.255.255.0"
        let fakeDNS = "172.16.0.2"
        
        let v4 = NEIPv4Settings(addresses: [ip], subnetMasks: [mask])
        
        // ✅ 修改路由配置：默认走隧道，后续通过 excludedRoutes 进行精确绕行
        v4.includedRoutes = [
            NEIPv4Route.default(),
//			NEIPv4Route(destinationAddress: "172.16.0.2", subnetMask: "255.255.255.255"),
//             NEIPv4Route(destinationAddress: "198.18.0.0", subnetMask: "255.254.0.0"), // 仅 FakeIP /15
//             NEIPv4Route(destinationAddress: "149.154.160.0", subnetMask: "255.255.240.0"), // 149.154.160.0/20
//
//             NEIPv4Route(destinationAddress: "91.108.4.0",   subnetMask: "255.255.252.0"),  // 91.108.4.0/22
//             NEIPv4Route(destinationAddress: "91.108.8.0",   subnetMask: "255.255.252.0"),  // 91.108.8.0/22
//             NEIPv4Route(destinationAddress: "91.108.12.0",  subnetMask: "255.255.252.0"),  // 91.108.12.0/22
//             NEIPv4Route(destinationAddress: "91.108.16.0",  subnetMask: "255.255.252.0"),  // 91.108.16.0/22
//             NEIPv4Route(destinationAddress: "91.108.20.0",  subnetMask: "255.255.252.0"),  // 91.108.20.0/22
//             NEIPv4Route(destinationAddress: "91.108.56.0",  subnetMask: "255.255.252.0")   // 91.108.56.0/22
            
        ]
        NSLog("[PacketTunnelProvider] Routing includes 198.18.0.0/15 for fake IPs")
        
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
        
        
        let extraCIDRs: [String] = {
            if let a = options?["LM.extraExcludedCIDRs"] as? [String] { return a }
            if let a = options?["LM.extraExcludedCIDRs"] as? NSArray { return a.compactMap { $0 as? String } }
            return []
        }()
        
        
        settings.ipv4Settings = v4
        
        settings.proxySettings = PacketTunnelProvider.createPACSettings()
        
        
        if !extraCIDRs.isEmpty {

            
            // ✅ 一次性得到路由对象和可读文本
                let (dynamicRoutes, dynamicPretty) = Self.buildRoutesAndPretty(fromCIDRs: extraCIDRs)

                if v4.excludedRoutes == nil { v4.excludedRoutes = [] }
                v4.excludedRoutes?.append(contentsOf: dynamicRoutes)
                settings.ipv4Settings = v4
            
            // ② 系统代理绕行：把 CIDR 文本追加到 exceptionList
            if let proxy = settings.proxySettings {
                var ex = proxy.exceptionList ?? []
                    ex.append(contentsOf: extraCIDRs)
                    proxy.exceptionList = Array(Set(ex))
                    settings.proxySettings = proxy
            }
            
            NSLog("[PacketTunnelProvider] dynamicRoutes: %@", dynamicPretty.joined(separator: ", ") as NSString)
            NSLog("[PacketTunnelProvider] dynamicRoutes count = %ld", dynamicRoutes.count)
        }
        
        
        
        let dns = NEDNSSettings(servers: [fakeDNS])
        dns.matchDomains = [""] // 关键：让所有域名查询都走fakeDNS
        settings.dnsSettings = dns
        
        settings.mtu = 1400
        
        NSLog("[PacketTunnelProvider] DNS trap set for \(fakeDNS)")
        NSLog("[PacketTunnelProvider] APNs traffic (17.0.0.0/8) will bypass tunnel")
        
        let startBox = StartCompletionBox(completionHandler)

        // 用中介对象承接非 Sendable 的 provider/settings
        let applier = NetworkSettingsApplier(provider: self, settings: settings)

        Task {
            let setError = await applier.apply()
            if let error = setError {
                await startBox.call(error)
                return
            }

            await TunnelSetupHelper.setupTunnel(
                flowWrapper: flowWrapper,
                fakeDNS: fakeDNS,
                stateManager: stateManager,
                connectionStore: connectionStore
            )
            await startBox.call(nil)
        }
    }
    
    // 创建PAC自动配置的NEProxySettings
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
    
    open override func stopTunnel(with reason: NEProviderStopReason,
                                  completionHandler: @escaping () -> Void) {
        NSLog("[PacketTunnelProvider] Stopping tunnel (reason=\(reason.rawValue)).")
        
        let stateManager = self.stateManager
        let connectionStore = self.connectionStore
        
        let stopBox = VoidCompletionBox(completionHandler)

        Task {
            await TunnelSetupHelper.stopTunnel(
                stateManager: stateManager,
                connectionStore: connectionStore
            )
            await stopBox.call()
        }
    }
    
    open override func handleAppMessage(_ messageData: Data,
                                       completionHandler: ((Data?) -> Void)?) {
        guard let message = String(data: messageData, encoding: .utf8) else {
            completionHandler?(nil)
            return
        }
        
        NSLog("[App Message] \(message)")
        
        let stateManager = self.stateManager

        // 只有在存在回调时才创建盒子；避免额外捕获
        let dataBox = completionHandler.map { DataCompletionBox($0) }

        Task {
            let data = await TunnelSetupHelper.handleAppMessage(
                message: message,
                stateManager: stateManager
            )
            if let box = dataBox {
                await box.call(data)
            }
        }
    }
    
    open override func sleep(completionHandler: @escaping () -> Void) {
        NSLog("[Sleep] Device going to sleep")
        
        let stateManager = self.stateManager
        let sleepBox = VoidCompletionBox(completionHandler)

        Task {
            await TunnelSetupHelper.handleSleep(stateManager: stateManager)
            await sleepBox.call()
        }
    }
    
    open override func wake() {
        NSLog("[Wake] Device waking up")
        
        let stateManager = self.stateManager
        
        // Use static method
        TunnelSetupHelper.handleWake(stateManager: stateManager)
    }
}

// MARK: - Static Helper to avoid capturing self

enum TunnelSetupHelper {

    static func setupTunnel(
        flowWrapper: SendablePacketFlow,
        fakeDNS: String,
        stateManager: TunnelStateManager,
        connectionStore: ConnectionStore
    ) async {
        await stateManager.setupInitial()
        await stateManager.startMemoryMonitoring()

        let manager = ConnectionManager(packetFlow: flowWrapper, fakeDNSServer: fakeDNS)

        await connectionStore.setManager(manager)
        await stateManager.setConnectionManager(manager)
        manager.start()

        let memoryMB = await stateManager.getCurrentMemoryUsageMB()
        NSLog("[PacketTunnelProvider] Tunnel started. Initial memory: \(memoryMB)MB")
    }

    static func stopTunnel(
        stateManager: TunnelStateManager,
        connectionStore: ConnectionStore
    ) async {
        await stateManager.stopMemoryMonitoring()
        if let manager = await connectionStore.getManager() {
            manager.prepareForStop()
        }
        await connectionStore.clearManager()
        await stateManager.cleanup()
    }

    static func handleAppMessage(
        message: String,
        stateManager: TunnelStateManager
    ) async -> Data? {
        switch message {
        case "memory_status":
            let memoryMB = await stateManager.getCurrentMemoryUsageMB()
            return "Memory: \(memoryMB)MB".data(using: .utf8)
        case "force_cleanup":
            await stateManager.handleMemoryWarning()
            return "Cleanup done".data(using: .utf8)
        default:
            return nil
        }
    }

    static func handleSleep(
        stateManager: TunnelStateManager
    ) async {
        await stateManager.handleMemoryWarning()
    }

    static func handleWake(stateManager: TunnelStateManager) {
        Task.detached {
            await stateManager.checkMemoryUsage()
        }
    }
}

// MARK: - Connection Store Actor

actor ConnectionStore {
    private var manager: ConnectionManager?
    
    func setManager(_ mgr: ConnectionManager) {
        self.manager = mgr
    }
    
    func getManager() -> ConnectionManager? {
        return manager
    }
    
    func clearManager() {
        self.manager = nil
    }
}

// MARK: - State Manager Actor

actor TunnelStateManager {
    private var memoryMonitorTask: Task<Void, Never>?
    private var memoryPressureSource: DispatchSourceMemoryPressure?
    private let memoryWarningThreshold: UInt64 = 40  // MB
    private let memoryCriticalThreshold: UInt64 = 48  // MB
    private weak var connectionManager: ConnectionManager?
    
    func setupInitial() {
        setupMemoryManagement()
        setMemoryLimits()
    }
    
    func setConnectionManager(_ manager: ConnectionManager) {
        self.connectionManager = manager
    }
    
    private func setupMemoryManagement() {
        setupMemoryPressureMonitoring()
    }
    
    private func setMemoryLimits() {
        // Disable URL caching
        URLCache.shared.memoryCapacity = 0
        URLCache.shared.diskCapacity = 0
        URLCache.shared.removeAllCachedResponses()
        
        // Sync UserDefaults
        UserDefaults.standard.synchronize()
        
        #if DEBUG
        NSLog("[Memory] Initial setup complete")
        #endif
    }
    
    private func setupMemoryPressureMonitoring() {
        let source = DispatchSource.makeMemoryPressureSource(
            eventMask: [.warning, .critical],
            queue: .global(qos: .utility)
        )
        
        let handler = MemoryPressureHandler(source: source, owner: self)
        let onPressure: @Sendable () -> Void = { handler.handle() }
        source.setEventHandler(handler: DispatchWorkItem(block: onPressure))
        
        source.resume()
        self.memoryPressureSource = source
    }
    
    func startMemoryMonitoring() {
        memoryMonitorTask?.cancel()
        
        // Create monitoring task
        memoryMonitorTask = Task.detached { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 10_000_000_000) // 10 seconds
                
                guard let sm = self else { break }
                await sm.checkMemoryUsage()
            }
        }
        
        NSLog("[Memory] Monitoring started (every 10s)")
    }
    
    func stopMemoryMonitoring() {
        memoryMonitorTask?.cancel()
        memoryMonitorTask = nil
        NSLog("[Memory] Monitoring stopped")
    }
    
    func checkMemoryUsage() {
        let memoryMB = getCurrentMemoryUsageMB()
        
        #if DEBUG
        if memoryMB > 30 {
            NSLog("[Memory] Current usage: \(memoryMB)MB")
        }
        #endif
        
        if memoryMB >= memoryCriticalThreshold {
            NSLog("💀 CRITICAL: Memory usage \(memoryMB)MB >= \(memoryCriticalThreshold)MB")
            Task.detached { [weak self] in
                guard let sm = self else { return }
                await sm.handleCriticalMemoryPressure()
            }
        } else if memoryMB >= memoryWarningThreshold {
            NSLog("⚠️ WARNING: Memory usage \(memoryMB)MB >= \(memoryWarningThreshold)MB")
            Task.detached { [weak self] in
                guard let sm = self else { return }
                await sm.handleMemoryWarning()
            }
        }
    }
    
    func handleMemoryWarning() async {
        NSLog("[Memory] Initiating cleanup...")
        
        // Ask connection manager to reduce connections
        if let mgr = connectionManager {
            await mgr.performMemoryCleanup(targetCount: 15)
        }
        
        // Clear caches
        URLCache.shared.removeAllCachedResponses()
        
        // Log result
        let newMemory = getCurrentMemoryUsageMB()
        NSLog("[Memory] After cleanup: \(newMemory)MB")
    }
    
    func handleCriticalMemoryPressure() async {
        NSLog("[Memory] EMERGENCY cleanup initiated!")
        
        // Emergency cleanup
        if let mgr = connectionManager {
            await mgr.emergencyCleanup()
        }
        
        // Clear all caches
        URLCache.shared.removeAllCachedResponses()
        UserDefaults.standard.synchronize()
        
        // Log result
        let newMemory = getCurrentMemoryUsageMB()
        NSLog("[Memory] After emergency cleanup: \(newMemory)MB")
    }
    
    func getCurrentMemoryUsageMB() -> UInt64 {
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size) / 4
        
        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: 1) {
                task_info(mach_task_self_,
                         task_flavor_t(MACH_TASK_BASIC_INFO),
                         $0,
                         &count)
            }
        }
        
        if result == KERN_SUCCESS {
            return info.resident_size / (1024 * 1024)
        }
        return 0
    }
    
    func cleanup() {
        memoryPressureSource?.cancel()
        memoryPressureSource = nil
        memoryMonitorTask?.cancel()
        memoryMonitorTask = nil
    }
}

// MARK: - Memory Info

struct MemoryInfo: Sendable {
    let used: UInt64
    let warning: Bool
    let critical: Bool
    
    var description: String {
        if critical {
            return "💀 Critical: \(used)MB"
        } else if warning {
            return "⚠️ Warning: \(used)MB"
        } else {
            return "✅ Normal: \(used)MB"
        }
    }
}

final class StartCompletionBox: @unchecked Sendable {
    private let cb: (Error?) -> Void
    init(_ cb: @escaping (Error?) -> Void) { self.cb = cb }
    @MainActor func call(_ error: Error?) { cb(error) }
}

final class VoidCompletionBox: @unchecked Sendable {
    private let cb: () -> Void
    init(_ cb: @escaping () -> Void) { self.cb = cb }
    @MainActor func call() { cb() }
}

final class DataCompletionBox: @unchecked Sendable {
    private let cb: (Data?) -> Void
    init(_ cb: @escaping (Data?) -> Void) { self.cb = cb }
    @MainActor func call(_ data: Data?) { cb(data) }
}

final class ErrorContinuationBox: @unchecked Sendable {
    private var cont: CheckedContinuation<Error?, Never>?
    init(_ cont: CheckedContinuation<Error?, Never>) { self.cont = cont }
    func resume(_ error: Error?) {
        cont?.resume(returning: error)
        cont = nil
    }
}

final class MemoryPressureHandler: @unchecked Sendable {
    private unowned(unsafe) let source: DispatchSourceMemoryPressure
    private weak var owner: TunnelStateManager?

    init(source: DispatchSourceMemoryPressure, owner: TunnelStateManager) {
        self.source = source
        self.owner = owner
    }

    func handle() {
        let event = source.data
        Task.detached { [weak owner] in
            guard let sm = owner else { return }
            if event.contains(.critical) {
                NSLog("⚠️ CRITICAL memory pressure detected!")
                await sm.handleCriticalMemoryPressure()
            } else if event.contains(.warning) {
                NSLog("⚠️ Memory pressure warning")
                await sm.handleMemoryWarning()
            }
        }
    }
}

final class NetworkSettingsApplier: @unchecked Sendable {
    private unowned(unsafe) let provider: NEPacketTunnelProvider
    private let settings: NEPacketTunnelNetworkSettings
    init(provider: NEPacketTunnelProvider, settings: NEPacketTunnelNetworkSettings) {
        self.provider = provider
        self.settings = settings
    }

    func apply() async -> Error? {
        await withCheckedContinuation { (cont: CheckedContinuation<Error?, Never>) in
            let box = ErrorContinuationBox(cont)
            provider.setTunnelNetworkSettings(settings) { error in
                box.resume(error)
            }
        }
    }
}


private extension PacketTunnelProvider {
    @inline(__always)
    static func maskString(fromPrefix p: Int) -> String {
        let m: UInt32 = (p == 0) ? 0 : (UInt32.max << (32 - p))
        func octet(_ s: Int) -> String { String((m >> s) & 0xff) }
        return [24,16,8,0].map(octet).joined(separator: ".")
    }

    @inline(__always)
    static func ipv4String(_ v: UInt32) -> String {
        func octet(_ s: Int) -> String { String((v >> s) & 0xff) }
        return [24,16,8,0].map(octet).joined(separator: ".")
    }

    // 解析 1 条 CIDR，返回 (路由对象, 可读字符串, 去重key)
    @inline(__always)
    static func parseCIDRPretty(_ cidr: String) -> (route: NEIPv4Route, pretty: String, key: String)? {
        let parts = cidr.split(separator: "/")
        guard parts.count == 2, let prefix = Int(parts[1]), (0...32).contains(prefix) else { return nil }

        let ipStr = String(parts[0])
        let octs = ipStr.split(separator: ".").compactMap { UInt8($0) }
        guard octs.count == 4 else { return nil }

        let ip: UInt32 = octs.reduce(0) { ($0 << 8) | UInt32($1) }
        let mask: UInt32 = (prefix == 0) ? 0 : (UInt32.max << (32 - prefix))
        let net = ip & mask

        let netStr  = ipv4String(net)
        let maskStr = maskString(fromPrefix: prefix)

        let route  = NEIPv4Route(destinationAddress: netStr, subnetMask: maskStr)
        let pretty = "\(netStr)/\(maskStr)"
        let key    = pretty
        return (route, pretty, key)
    }

    // 批量：返回 (路由数组, 可读字符串数组)
    @inline(__always)
    static func buildRoutesAndPretty(fromCIDRs cidrs: [String]) -> (routes: [NEIPv4Route], pretty: [String]) {
        var seen = Set<String>()
        var routes: [NEIPv4Route] = []
        var pretties: [String] = []

        for raw in cidrs {
            guard let parsed = parseCIDRPretty(raw) else { continue }
            if seen.insert(parsed.key).inserted {
                routes.append(parsed.route)
                pretties.append(parsed.pretty)
            }
        }
        return (routes, pretties)
    }
}
