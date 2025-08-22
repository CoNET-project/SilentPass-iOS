//
//  PacketTunnelProvider.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Updated with complete concurrency safety for Swift 6
//
import NetworkExtension
import Network
import os.log

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
        
        // Configure TUN settings
        let settings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: "127.0.0.1")
        let ip = "172.16.0.1"
        let mask = "255.255.255.0"
        let fakeDNS = "172.16.0.2"
        
        let v4 = NEIPv4Settings(addresses: [ip], subnetMasks: [mask])
        
        v4.includedRoutes = [
            NEIPv4Route.default(),
            NEIPv4Route(destinationAddress: "198.18.0.0", subnetMask: "255.254.0.0")
        ]
        NSLog("[PacketTunnelProvider] Routing includes 198.18.0.0/15 for fake IPs")
        
        v4.excludedRoutes = [
            NEIPv4Route(destinationAddress: "192.168.0.0", subnetMask: "255.255.0.0"),
            NEIPv4Route(destinationAddress: "10.0.0.0", subnetMask: "255.0.0.0"),
            NEIPv4Route(destinationAddress: "127.0.0.0", subnetMask: "255.0.0.0"),
            NEIPv4Route(destinationAddress: "169.254.0.0", subnetMask: "255.255.0.0")
        ]
        settings.ipv4Settings = v4
        
        settings.dnsSettings = NEDNSSettings(servers: [fakeDNS])
        settings.mtu = 1400
        
        NSLog("[PacketTunnelProvider] DNS trap set for \(fakeDNS)")
        
        let startBox = StartCompletionBox(completionHandler)

        // ✅ 用中介对象承接非 Sendable 的 provider/settings
        let applier = NetworkSettingsApplier(provider: self, settings: settings)

        Task {
            let setError = await applier.apply()
            if let error = setError {
                await startBox.call(error)
                return
            }

            await TunnelSetupHelper.setupTunnel(
                flowWrapper: flowWrapper,             // ← 传包装，而不是原始 packetFlow
                fakeDNS: fakeDNS,
                stateManager: stateManager,
                connectionStore: connectionStore
            )
            await startBox.call(nil)
        }
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

           // ✅ 只有在存在回调时才创建盒子；避免额外捕获
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
        flowWrapper: SendablePacketFlow,           // ✅ 改签名
        fakeDNS: String,
        stateManager: TunnelStateManager,
        connectionStore: ConnectionStore
    ) async {
        await stateManager.setupInitial()
        await stateManager.startMemoryMonitoring()

        // ✅ 直接使用传入的 flowWrapper，不要再内部二次包装
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
        Task.detached {    // 这里保留 detached 没问题，因为没有把系统回调跨发
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
        // ✅ 显式声明一个 @Sendable thunk，闭包只捕获 handler（Sendable）
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
// 让 withCheckedContinuation 的闭包只捕获本中介（@unchecked Sendable），
// 避免直接在闭包里用非 Sendable 的 settings/provider。

final class NetworkSettingsApplier: @unchecked Sendable {
    private unowned(unsafe) let provider: NEPacketTunnelProvider
    private let settings: NEPacketTunnelNetworkSettings
    init(provider: NEPacketTunnelProvider, settings: NEPacketTunnelNetworkSettings) {
        self.provider = provider
        self.settings = settings
    }

    // ✅ 把 withCheckedContinuation 移到类内部，避免调用处产生 "sending" 闭包
    func apply() async -> Error? {
        await withCheckedContinuation { (cont: CheckedContinuation<Error?, Never>) in
            let box = ErrorContinuationBox(cont)
            provider.setTunnelNetworkSettings(settings) { error in
                box.resume(error)
            }
        }
    }
}
