//
//  ConnectionManager+Extension.swift
//  vpn2socks
//
//  扩展：统计打印、定时心跳日志、友好格式化
//

import Foundation

public extension ConnectionManager {

    func printStats(tag: String? = nil) async {
        let snap = snapshot()
        let rate = String(format: "%.1f", snap.stats.successRate)
        let up = Int(snap.stats.uptime)
        let t = tag ?? ""
        NSLog("""
        [ConnectionManager] \(t)
        Uptime: \(String(format: "%02d:%02d:%02d", up/3600, (up/60)%60, up%60))
        Connections: total=\(snap.stats.total) active=\(snap.totalTcpConnections) success=\(rate) failed=\(snap.stats.failed)
        Traffic: rx=\(formatBytes(snap.stats.bytesReceived)) tx=\(formatBytes(snap.stats.bytesSent))
        SOCKS: active=\(snap.activeSocks)/\(limits.maxActive) pending=\(snap.pendingSocks)
        """)
    }

    func startPeriodicStatsLog(every seconds: TimeInterval = 30) {
        Task.detached { [weak self] in
            while let self {
                try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
                await self.printStats(tag: "=== Periodic Stats ===")
            }
        }
    }

    // MARK: - Utilities

    private func formatBytes(_ n: Int) -> String {
        let f = Double(n)
        if f < 1024 { return "\(n) B" }
        if f < 1024*1024 { return String(format: "%.2f KB", f/1024) }
        if f < 1024*1024*1024 { return String(format: "%.2f MB", f/1024/1024) }
        return String(format: "%.2f GB", f/1024/1024/1024)
    }
}
