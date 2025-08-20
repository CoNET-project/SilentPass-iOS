//
//  ConnectionManager.swift
//  vpn2socks
//
//  完整版：统一 ID 回调、连接池管理、池满拒绝、孤包检测、统计/速率、日志
//

import Foundation
import NetworkExtension
import Network

// 如果工程里已提供 ConnectionManagerTypes.swift（PoolSnapshot/StatsSnapshot/ConnectionLimits），这里直接使用
// 否则可把它们内联进来

public final actor ConnectionManager {

    // MARK: - Public Config
    public var limits = ConnectionLimits(maxActive: 40, maxPending: 10, maxTotalTcp: 2000, maxPendingSyns: 50)

    // MARK: - State
    private let flow: SendablePacketFlow
    private var nextId: UInt64 = 1

    private var connsByKey: [String: TCPConnection] = [:]
    private var connsById: [UInt64: TCPConnection] = [:]

    private var socksActive: Int = 0
    private var socksPending: Int = 0

    private var statTotal: Int = 0
    private var statFailed: Int = 0
    private var statBytesRx: Int = 0
    private var statBytesTx: Int = 0
    private let startTime = Date()

    // 开关：是否打印孤包
    public var logOrphan = true

    // MARK: - Init
    public init(packetFlow: SendablePacketFlow) {
        self.flow = packetFlow
    }

    // MARK: - Connection key
    @inline(__always)
    private func keyFor(srcIP: IPv4Address, srcPort: UInt16, dstIP: IPv4Address, dstPort: UInt16) -> String {
        return "\(srcIP.rawValue.map{String($0)}.joined(separator: ".")):\(srcPort)->\(dstIP.rawValue.map{String($0)}.joined(separator: ".")):\(dstPort)"
    }

    // MARK: - Public API (called by IP/TCP stack)

    /// SYN 到来时创建或拒绝连接
    public func onSyn(
        srcIP: IPv4Address, srcPort: UInt16,
        dstIP: IPv4Address, dstPort: UInt16,
        initialSeq: UInt32,
        hostname: String?
    ) async {
        let key = keyFor(srcIP: srcIP, srcPort: srcPort, dstIP: dstIP, dstPort: dstPort)
        if connsByKey[key] != nil {
            // 已存在（重传 SYN）— 忽略，等待后续数据
            return
        }

        // 连接池限流：只统计 SOCKS active 数，避免 UI 阻塞
        if socksActive >= limits.maxActive {
            log("SOCKS pool full (\(socksActive)/\(limits.maxActive)), rejecting \(key)")
            // 不建立 TCPConnection，从而上层看到拒绝/超时
            return
        }

        let id = nextId; nextId &+= 1
        let conn = TCPConnection(
            key: key,
            connectionId: id,
            packetFlow: flow,
            sourceIP: srcIP,
            sourcePort: srcPort,
            destIP: dstIP,
            destPort: dstPort,
            destinationHost: hostname,
            initialSequenceNumber: initialSeq,
            tunnelMTU: 1400,
            recvBufferLimit: 24 * 1024,
            reportInFlight: { [weak self] id, delta in
                await self?.onInFlightDelta(id: id, delta: delta)
            },
            reportSocksStart: { [weak self] id in
                await self?.onSocksStart(id: id)
            },
            reportSocksSuccess: { [weak self] id in
                await self?.onSocksSuccess(id: id)
            },
            reportSocksFailure: { [weak self] id, timeout in
                await self?.onSocksFail(id: id, timeout: timeout)
            },
            reportSocksEnd: { [weak self] id in
                await self?.onSocksEnd(id: id)
            }
        )

        connsByKey[key] = conn
        connsById[id] = conn
        statTotal &+= 1

        // 立即发回 SYN-ACK（等 SOCKS 成功后再 flush 上行）
        await conn.sendSynAckIfNeeded()
        await conn.startIfNeeded()

        log("[\(key)] Created id=\(id). Active SOCKS: \(socksActive)/\(limits.maxActive)")
    }

    /// 数据包到达（非 SYN）
    public func onPayload(
        srcIP: IPv4Address, srcPort: UInt16,
        dstIP: IPv4Address, dstPort: UInt16,
        seq: UInt32,
        payload: Data
    ) async {
        let key = keyFor(srcIP: srcIP, srcPort: srcPort, dstIP: dstIP, dstPort: dstPort)
        guard let c = connsByKey[key] else {
            if logOrphan {
                log("Orphan packet for \(key) flags: data len=\(payload.count)")
            }
            return
        }
        // 统计：客户端 -> 代理 上行字节
        statBytesTx &+= payload.count
        await c.handlePayload(payload, sequenceNumber: seq)
    }

    /// 主动关闭（来自 RST/FIN 或上层策略）
    public func close(
        srcIP: IPv4Address, srcPort: UInt16,
        dstIP: IPv4Address, dstPort: UInt16
    ) async {
        let key = keyFor(srcIP: srcIP, srcPort: srcPort, dstIP: dstIP, dstPort: dstPort)
        if let c = connsByKey.removeValue(forKey: key) {
            connsById.removeValue(forKey: c.connectionId)
            await c.close()
            log("Connection \(key) removed")
        }
    }

    // MARK: - Callbacks from TCPConnection (带 ID)

    private func onSocksStart(id: UInt64) {
        socksActive &+= 1
        log("SOCKS started, active: \(socksActive)/\(limits.maxActive)")
    }

    private func onSocksSuccess(id: UInt64) {
        // 成功建立 SOCKS 隧道
        log("[\(id)] SOCKS tunnel established")
    }

    private func onSocksFail(id: UInt64, timeout: Bool) {
        statFailed &+= 1
        socksActive = max(0, socksActive - 1)
        log("[\(id)] SOCKS failed (timeout=\(timeout)). Active: \(socksActive)/\(limits.maxActive)")
        // 清理这条连接（如果还在）
        if let c = connsById.removeValue(forKey: id) {
            connsByKey.removeValue(forKey: c.key)
        }
    }

    private func onSocksEnd(id: UInt64) {
        socksActive = max(0, socksActive - 1)
        log("[\(id)] SOCKS ended, active: \(socksActive)/\(limits.maxActive)")
        // 连接可能已在 fail 时从 map 删除；这里再兜底清理
        if let c = connsById.removeValue(forKey: id) {
            connsByKey.removeValue(forKey: c.key)
        }
    }

    private func onInFlightDelta(id: UInt64, delta: Int) {
        // delta>0 表示客户端->SOCKS 推送中；delta<0 表示已发送完成
        // 这里可以结合池压/背压策略做动态限速（留空以保持简单）
        _ = id; _ = delta
    }

    // MARK: - Stats & snapshots

    public func snapshot() -> PoolSnapshot {
        let statesStarting = 0 // 如需细分状态，可从 TCPConnection 暴露
        let statesActive = socksActive
        let statesEnded = 0
        let stats = StatsSnapshot(
            total: statTotal,
            failed: statFailed,
            bytesReceived: statBytesRx,
            bytesSent: statBytesTx,
            startTime: startTime
        )
        return PoolSnapshot(
            activeSocks: socksActive,
            pendingSocks: socksPending,
            totalTcpConnections: connsById.count,
            pendingSyns: 0,
            inFlightBytes: 0,
            stats: stats,
            statesStarting: statesStarting,
            statesActive: statesActive,
            statesEnded: statesEnded
        )
    }

    // 接收从 SOCKS 回来的下行字节统计时可调用
    public func addRxBytes(_ n: Int) {
        statBytesRx &+= n
    }

    // MARK: - Logging

    @inline(__always)
    private func log(_ s: String) {
        NSLog("[ConnectionManager] \(s)")
    }
}
