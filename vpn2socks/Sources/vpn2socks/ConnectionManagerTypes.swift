//
//  ConnectionManagerTypes.swift
//  vpn2socks
//
//  Created by peter on 2025-08-20.
//

import Foundation
import NetworkExtension

// MARK: - 数据模型定义

/// 连接池快照
public struct PoolSnapshot: Sendable {
    public let activeSocks: Int
    public let pendingSocks: Int
    public let totalTcpConnections: Int
    public let pendingSyns: Int
    public let inFlightBytes: Int
    public let stats: StatsSnapshot
    public let statesStarting: Int
    public let statesActive: Int
    public let statesEnded: Int
    
    public var socksTotal: Int { activeSocks + pendingSocks }
    
    public var socksLoad: Double {
        guard activeSocks > 0 else { return 0 }
        return Double(activeSocks) / Double(50) // 默认最大50
    }
    
    // 添加初始化器
    public init(activeSocks: Int, pendingSocks: Int, totalTcpConnections: Int,
                pendingSyns: Int, inFlightBytes: Int, stats: StatsSnapshot,
                statesStarting: Int, statesActive: Int, statesEnded: Int) {
        self.activeSocks = activeSocks
        self.pendingSocks = pendingSocks
        self.totalTcpConnections = totalTcpConnections
        self.pendingSyns = pendingSyns
        self.inFlightBytes = inFlightBytes
        self.stats = stats
        self.statesStarting = statesStarting
        self.statesActive = statesActive
        self.statesEnded = statesEnded
    }
}

/// 统计快照
public struct StatsSnapshot: Sendable {
    public let total: Int
    public let failed: Int
    public let bytesReceived: Int
    public let bytesSent: Int
    public let startTime: Date
    
    public var successRate: Double {
        total > 0 ? Double(total - failed) / Double(total) * 100 : 0
    }
    
    public var uptime: TimeInterval {
        Date().timeIntervalSince(startTime)
    }
    
    // 添加初始化器
    public init(total: Int, failed: Int, bytesReceived: Int, bytesSent: Int, startTime: Date) {
        self.total = total
        self.failed = failed
        self.bytesReceived = bytesReceived
        self.bytesSent = bytesSent
        self.startTime = startTime
    }
}

/// 连接限制配置
public struct ConnectionLimits: Sendable {
    public let maxActive: Int
    public let maxPending: Int
    public let maxTotalTcp: Int
    public let maxPendingSyns: Int
    
    public init(maxActive: Int, maxPending: Int, maxTotalTcp: Int, maxPendingSyns: Int) {
        self.maxActive = maxActive
        self.maxPending = maxPending
        self.maxTotalTcp = maxTotalTcp
        self.maxPendingSyns = maxPendingSyns
    }
}
