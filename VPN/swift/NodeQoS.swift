import Foundation


struct NodeStatistics {
    var totalNodes: Int = 0
    var activeNodes: Int = 0
    var idleNodes: Int = 0
    var bannedNodes: Int = 0
    var cooldownNodes: Int = 0
    var activeConnections: Int = 0
    var averageLatency: Double = 0
    var sampledNodes: Int = 0
    var bestNode: String = ""
    var bestLatency: Double = Double.infinity
    var worstNode: String = ""
    var worstLatency: Double = 0
}

struct NodeHealth {
    let ip: String
    var successRate: Double = 0
    var latency: Double = 0
    var latencyStatus: LatencyStatus = .unknown
    var activeConnections: Int = 0
    var loadStatus: LoadStatus = .light
    var availability: AvailabilityStatus = .available
    
    enum LatencyStatus {
        case good, fair, poor, unknown
    }
    
    enum LoadStatus {
        case light, moderate, heavy
    }
    
    enum AvailabilityStatus {
        case available
        case cooldown(until: Date)
        case banned(until: Date)
    }
}

enum NodeStatus {
    case available
    case banned(until: Date)
    case cooldown(until: Date)
}

class NodeQoSCleanupTimer {
    private var cleanupTimer: Timer?
    private var compactTimer: Timer?
    
    func start() {
        // 每小时清理过期节点
        cleanupTimer = Timer.scheduledTimer(withTimeInterval: 3600, repeats: true) { _ in
            NodeQoS.shared.cleanup()
            NSLog("[NodeQoS] Cleanup performed")
        }
        
    }
    
    func stop() {
        cleanupTimer?.invalidate()
        cleanupTimer = nil
        compactTimer?.invalidate()
        compactTimer = nil
    }
    
}


final class NodeQoS {
    static let shared = NodeQoS()
    
    private let alpha: Double = 0.30
    private var map: [String: Stat] = [:]
    private let q = DispatchQueue(label: "NodeQoS.lock", qos: .userInitiated)
    
    private struct Stat {
        var ewmaMs: Double
        var samples: Int
        var bannedUntil: Date?
        var cooldownUntil: Date?
        var successCount: Int = 0  // 新增：成功连接计数
        var failureCount: Int = 0  // 新增：失败连接计数
        var lastUsed: Date?        // 新增：最后使用时间
        var activeConnections: Int = 0  // 新增：当前活跃连接数
    }
    
    // 冷却映射参数
    private let cooldownMinTTFBms: Double = 300
    private let cooldownMaxTTFBms: Double = 900
    private let cooldownMinSec: Double = 30
    private let cooldownMaxSec: Double = 60
    
    // 新增：负载均衡参数
    private let maxActivePerNode: Int = 50  // 每个节点最大活跃连接数
    private let loadBalanceWindow: TimeInterval = 60  // 负载均衡时间窗口（秒）
    
    private func cooldownSeconds(for ttfbMs: Double) -> TimeInterval {
        if ttfbMs <= cooldownMinTTFBms { return 0 }
        if ttfbMs >= cooldownMaxTTFBms { return cooldownMaxSec }
        let r = (ttfbMs - cooldownMinTTFBms) / (cooldownMaxTTFBms - cooldownMinTTFBms)
        return cooldownMinSec + r * (cooldownMaxSec - cooldownMinSec)
    }
    
    // 记录成功响应
    func recordSuccess(ip: String, ttfbMs: Double) {
        q.sync {
            var s = map[ip] ?? Stat(ewmaMs: ttfbMs, samples: 0, bannedUntil: nil, cooldownUntil: nil)
            s.ewmaMs = (s.samples == 0) ? ttfbMs : (alpha * ttfbMs + (1 - alpha) * s.ewmaMs)
            s.samples &+= 1
            s.successCount &+= 1
            s.bannedUntil = nil
            s.lastUsed = Date()
            
            let cool = cooldownSeconds(for: ttfbMs)
            s.cooldownUntil = cool > 0 ? Date().addingTimeInterval(cool) : nil
            
            map[ip] = s
            NSLog("[NodeQoS] success ip=\(ip) ttfb=\(Int(ttfbMs))ms ewma=\(Int(s.ewmaMs))ms cooldown=\(Int(cool))s")
        }
    }
    
    // 记录失败
    func recordNoResponse(ip: String) {
        q.sync {
            var s = map[ip] ?? Stat(ewmaMs: 5_000, samples: 0, bannedUntil: nil, cooldownUntil: nil)
            s.failureCount &+= 1
            s.bannedUntil = Date().addingTimeInterval(5 * 60)
            s.lastUsed = Date()
            map[ip] = s
        }
    }
    
    // 新增：记录连接开始
    func recordConnectionStart(ip: String) {
        q.sync {
            var s = map[ip] ?? Stat(ewmaMs: 1000, samples: 0, bannedUntil: nil, cooldownUntil: nil)
            s.activeConnections &+= 1
            s.lastUsed = Date()
            map[ip] = s
        }
    }
    
    // 新增：记录连接结束
    func recordConnectionEnd(ip: String) {
        q.sync {
            if var s = map[ip] {
                s.activeConnections = max(0, s.activeConnections - 1)
                map[ip] = s
            }
        }
    }
    
    // 新增：获取节点评分（用于选择最佳节点）
    func getNodeScore(ip: String) -> Double? {
        return q.sync {
            let now = Date()
            
            // 检查是否被禁用
            if let s = map[ip], let b = s.bannedUntil, b > now {
                return nil
            }
            
            // 检查是否在冷却期
            if let s = map[ip], let c = s.cooldownUntil, c > now {
                return nil
            }
            
            // 检查活跃连接数是否超限
            if let s = map[ip], s.activeConnections >= maxActivePerNode {
                return nil
            }
            
            // 计算节点评分
            if let s = map[ip] {
                let successRate = s.samples > 0 ?
                    Double(s.successCount) / Double(s.successCount + s.failureCount) : 0.5
                let latencyScore = 1000.0 / max(s.ewmaMs, 1.0)  // 延迟越低，分数越高
                let loadScore = 1.0 - (Double(s.activeConnections) / Double(maxActivePerNode))
                
                // 最近使用奖励（避免节点长期闲置）
                let recencyBonus: Double
                if let lastUsed = s.lastUsed {
                    let timeSinceUse = now.timeIntervalSince(lastUsed)
                    recencyBonus = min(timeSinceUse / loadBalanceWindow, 1.0) * 0.1
                } else {
                    recencyBonus = 0.2  // 新节点奖励
                }
                
                // 综合评分：成功率40% + 延迟30% + 负载20% + 近期使用10%
                return successRate * 0.4 + latencyScore * 0.3 + loadScore * 0.2 + recencyBonus
            }
            
            // 未知节点给予探索机会
            return 0.5
        }
    }
    
    // 是否允许使用（保留兼容性）
    func shouldAccept(ip: String) -> Bool {
        return getNodeScore(ip: ip) != nil
    }
}

extension NodeQoS {
    
    func exportNodeData() -> Data? {
            return q.sync {
                let exportData = map.map { (ip, stat) in
                    return [
                        "ip": ip,
                        "ewmaMs": stat.ewmaMs,
                        "samples": stat.samples,
                        "successCount": stat.successCount,
                        "failureCount": stat.failureCount,
                        "activeConnections": stat.activeConnections,
                        "lastUsed": stat.lastUsed?.timeIntervalSince1970 ?? 0
                    ] as [String : Any]
                }
                
                return try? JSONSerialization.data(withJSONObject: exportData, options: .prettyPrinted)
            }
        }
        
        // 重置特定节点的统计
        func resetNodeStats(ip: String) {
            q.async {
                if var stat = self.map[ip] {
                    stat.samples = 0
                    stat.successCount = 0
                    stat.failureCount = 0
                    stat.ewmaMs = 1000
                    stat.bannedUntil = nil
                    stat.cooldownUntil = nil
                    self.map[ip] = stat
                    NSLog("[NodeQoS] Reset stats for node: \(ip)")
                }
            }
        }
        
        // 手动设置节点状态
        func setNodeStatus(ip: String, status: NodeStatus) {
            q.async {
                var stat = self.map[ip] ?? Stat(ewmaMs: 1000, samples: 0, bannedUntil: nil, cooldownUntil: nil)
                
                switch status {
                case .available:
                    stat.bannedUntil = nil
                    stat.cooldownUntil = nil
                case .banned(let until):
                    stat.bannedUntil = until
                case .cooldown(let until):
                    stat.cooldownUntil = until
                }
                
                self.map[ip] = stat
                NSLog("[NodeQoS] Set node \(ip) status to: \(status)")
            }
        }
    
    func getStatistics() -> String {
        return q.sync {
            var totalActive = 0
            var bannedCount = 0
            var cooldownCount = 0
            let now = Date()
            
            for (ip, stat) in map {
                totalActive += stat.activeConnections
                if let b = stat.bannedUntil, b > now {
                    bannedCount += 1
                }
                if let c = stat.cooldownUntil, c > now {
                    cooldownCount += 1
                }
            }
            
            return "Total nodes: \(map.count), Active connections: \(totalActive), Banned: \(bannedCount), Cooldown: \(cooldownCount)"
        }
    }
    
    // 清理过期的节点信息
    func cleanup() {
        q.async {
            let now = Date()
            let cutoff = now.addingTimeInterval(-24 * 60 * 60)  // 24小时前
            
            self.map = self.map.filter { (_, stat) in
                // 保留活跃连接或最近使用的节点
                if stat.activeConnections > 0 { return true }
                if let lastUsed = stat.lastUsed, lastUsed > cutoff { return true }
                return false
            }
        }
    }
    // 批量更新节点状态
        func updateBulkNodeStatus(_ updates: [(ip: String, status: NodeStatus)]) {
            q.async {
                for update in updates {
                    if var stat = self.map[update.ip] {
                        switch update.status {
                        case .available:
                            stat.bannedUntil = nil
                            stat.cooldownUntil = nil
                        case .banned(let until):
                            stat.bannedUntil = until
                        case .cooldown(let until):
                            stat.cooldownUntil = until
                        }
                        self.map[update.ip] = stat
                    }
                }
            }
        }
        
        // 获取详细统计信息
        func getDetailedStatistics() -> NodeStatistics {
            return q.sync {
                var stats = NodeStatistics()
                let now = Date()
                
                for (ip, stat) in map {
                    stats.totalNodes += 1
                    stats.activeConnections += stat.activeConnections
                    
                    if let b = stat.bannedUntil, b > now {
                        stats.bannedNodes += 1
                    } else if let c = stat.cooldownUntil, c > now {
                        stats.cooldownNodes += 1
                    } else if stat.activeConnections > 0 {
                        stats.activeNodes += 1
                    } else {
                        stats.idleNodes += 1
                    }
                    
                    // 计算平均延迟
                    if stat.samples > 0 {
                        stats.averageLatency += stat.ewmaMs
                        stats.sampledNodes += 1
                    }
                    
                    // 记录最佳和最差节点
                    if stat.ewmaMs < stats.bestLatency {
                        stats.bestLatency = stat.ewmaMs
                        stats.bestNode = ip
                    }
                    if stat.ewmaMs > stats.worstLatency {
                        stats.worstLatency = stat.ewmaMs
                        stats.worstNode = ip
                    }
                }
                
                if stats.sampledNodes > 0 {
                    stats.averageLatency /= Double(stats.sampledNodes)
                }
                
                return stats
            }
        }
        
        // 节点健康检查
        func performHealthCheck() -> [String: NodeHealth] {
            return q.sync {
                var healthReport: [String: NodeHealth] = [:]
                let now = Date()
                
                for (ip, stat) in map {
                    var health = NodeHealth(ip: ip)
                    
                    // 计算成功率
                    let totalAttempts = stat.successCount + stat.failureCount
                    health.successRate = totalAttempts > 0 ?
                        Double(stat.successCount) / Double(totalAttempts) : 0
                    
                    // 延迟状态
                    health.latency = stat.ewmaMs
                    health.latencyStatus = stat.ewmaMs < 300 ? .good :
                        (stat.ewmaMs < 900 ? .fair : .poor)
                    
                    // 负载状态
                    health.activeConnections = stat.activeConnections
                    health.loadStatus = stat.activeConnections < 10 ? .light :
                        (stat.activeConnections < 30 ? .moderate : .heavy)
                    
                    // 可用性状态
                    if let b = stat.bannedUntil, b > now {
                        health.availability = .banned(until: b)
                    } else if let c = stat.cooldownUntil, c > now {
                        health.availability = .cooldown(until: c)
                    } else {
                        health.availability = .available
                    }
                    
                    healthReport[ip] = health
                }
                
                return healthReport
            }
        }
}
