//
//  TCPConnectionTests.swift
//  vpn2socks
//
//  Created by peter on 2025-08-25.
//

// TCPConnectionTests.swift
#if DEBUG

import Foundation

extension TCPConnection {
    // 测试用的性能指标收集
    struct PerformanceMetrics {
        var expansionCount: Int = 0
        var shrinkCount: Int = 0
        var overflowCount: Int = 0
        var averageUtilization: Double = 0
        var peakBufferSize: Int = 0
        var minBufferSize: Int = Int.max
        var connectionDuration: TimeInterval = 0
    }
    
    private static var performanceMetrics: [String: PerformanceMetrics] = [:]
    
    // 记录性能指标
    func recordPerformanceMetrics() {
        // 实现性能记录逻辑
    }
    
    // 导出测试报告
    static func exportTestReport() -> String {
        var report = """
        === VPN2SOCKS Performance Test Report ===
        Generated: \(Date())
        
        """
        
        for (key, metrics) in performanceMetrics {
            report += """
            Connection: \(key)
            - Expansions: \(metrics.expansionCount)
            - Shrinks: \(metrics.shrinkCount)
            - Overflows: \(metrics.overflowCount)
            - Avg Utilization: \(String(format: "%.1f%%", metrics.averageUtilization))
            - Peak Buffer: \(metrics.peakBufferSize) bytes
            - Min Buffer: \(metrics.minBufferSize) bytes
            - Duration: \(String(format: "%.1f", metrics.connectionDuration))s
            
            """
        }
        
        return report
    }
}

#endif
