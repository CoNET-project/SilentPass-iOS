import Foundation
import NetworkExtension
import Network

// MARK: - 简化的热监控
actor ThermalMonitor {
    static let shared = ThermalMonitor()
    
    private var currentState: ProcessInfo.ThermalState = .nominal
    private var lastStateCheck: Date = Date()
    private let stateCheckInterval: TimeInterval = 5.0  // 每5秒检查一次
    
    private init() {
        Task {
            await updateThermalState()
        }
    }
    
    private func updateThermalState() async {
        let now = Date()
        if now.timeIntervalSince(lastStateCheck) >= stateCheckInterval {
            let newState = ProcessInfo.processInfo.thermalState
            if newState != currentState {
                let oldState = currentState
                currentState = newState
                lastStateCheck = now
                
                let stateDescription = thermalStateDescription(newState)
                NSLog("[ThermalMonitor] 🌡️ Thermal state: \(oldState.description) → \(stateDescription)")
            }
        }
    }
    
    private func thermalStateDescription(_ state: ProcessInfo.ThermalState) -> String {
        switch state {
        case .nominal: return "NOMINAL"
        case .fair: return "FAIR"
        case .serious: return "SERIOUS ⚠️"
        case .critical: return "CRITICAL 🔥"
        @unknown default: return "UNKNOWN"
        }
    }
    
    // 公共接口
    func getCurrentState() async -> ProcessInfo.ThermalState {
        await updateThermalState()
        return currentState
    }
    
    func isUnderThermalPressure() async -> Bool {
        await updateThermalState()
        return currentState != .nominal
    }
    
    func shouldReduceConnections() async -> Bool {
        await updateThermalState()
        return currentState == .serious || currentState == .critical
    }
    
    func getOptimizedConfig() async -> ThermalConfig {
        await updateThermalState()
        switch currentState {
        case .nominal:
            return .normal
        case .fair:
            return .reduced
        case .serious:
            return .conservative
        case .critical:
            return .minimal
        @unknown default:
            return .conservative
        }
    }
}

// MARK: - 热优化配置
struct ThermalConfig {
    let maxSocksConnections: Int
    let maxPendingConnections: Int
    let connectionTimeout: TimeInterval
    let maxPacketsPerBatch: Int
    let inFlightSoftLimit: Int
    let inFlightHardLimit: Int
    let statsInterval: TimeInterval
    let cleanupInterval: TimeInterval
    
    static let normal = ThermalConfig(
        maxSocksConnections: 50,
        maxPendingConnections: 8,
        connectionTimeout: 300.0,
        maxPacketsPerBatch: 64,
        inFlightSoftLimit: 8 * 1024 * 1024,
        inFlightHardLimit: 16 * 1024 * 1024,
        statsInterval: 30.0,
        cleanupInterval: 60.0
    )
    
    static let reduced = ThermalConfig(
        maxSocksConnections: 40,
        maxPendingConnections: 6,
        connectionTimeout: 240.0,
        maxPacketsPerBatch: 48,
        inFlightSoftLimit: 6 * 1024 * 1024,
        inFlightHardLimit: 12 * 1024 * 1024,
        statsInterval: 45.0,
        cleanupInterval: 45.0
    )
    
    static let conservative = ThermalConfig(
        maxSocksConnections: 30,
        maxPendingConnections: 4,
        connectionTimeout: 180.0,
        maxPacketsPerBatch: 32,
        inFlightSoftLimit: 4 * 1024 * 1024,
        inFlightHardLimit: 8 * 1024 * 1024,
        statsInterval: 60.0,
        cleanupInterval: 30.0
    )
    
    static let minimal = ThermalConfig(
        maxSocksConnections: 20,
        maxPendingConnections: 2,
        connectionTimeout: 120.0,
        maxPacketsPerBatch: 16,
        inFlightSoftLimit: 2 * 1024 * 1024,
        inFlightHardLimit: 4 * 1024 * 1024,
        statsInterval: 90.0,
        cleanupInterval: 20.0
    )
}

// MARK: - ProcessInfo.ThermalState 扩展
extension ProcessInfo.ThermalState {
    var description: String {
        switch self {
        case .nominal: return "NOMINAL"
        case .fair: return "FAIR"
        case .serious: return "SERIOUS"
        case .critical: return "CRITICAL"
        @unknown default: return "UNKNOWN"
        }
    }
}
