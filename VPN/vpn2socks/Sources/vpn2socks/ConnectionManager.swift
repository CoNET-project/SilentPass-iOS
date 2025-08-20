//
//  ConnectionManager.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//


import Foundation
import NetworkExtension
import Network

final actor ConnectionManager {

    private let packetFlow: NEPacketTunnelFlow
    private let fakeDNSServer: IPv4Address
    private let dnsInterceptor = DNSInterceptor()
    private var tcpConnections: [String: TCPConnection] = [:]

    init(packetFlow: NEPacketTunnelFlow, fakeDNSServer: String) {
        self.packetFlow = packetFlow
        self.fakeDNSServer = IPv4Address(fakeDNSServer)!
    }

    func start() {
        Task { await readPackets() }
    }

    private func readPackets() async {
        while true {
            let packets = await packetFlow.readPackets()
            for packetData in packets.packets {
                guard let ipPacket = IPv4Packet(data: packetData) else { continue }
                
                switch ipPacket.protocol {
                case 6: // TCP
                    await handleTCPPacket(ipPacket)
                case 17: // UDP
                    await handleUDPPacket(ipPacket)
                default:
                    break
                }
            }
        }
    }
    
    private func handleUDPPacket(_ ipPacket: IPv4Packet) async {
        // ... DNS 拦截、分配假IP、伪造响应的逻辑 ...
        // (此部分代码与我们之前讨论的完全相同)
    }
    
    private func handleTCPPacket(_ ipPacket: IPv4Packet) async {
        guard let tcpSegment = TCPSegment(data: ipPacket.payload) else { return }
        
        if tcpSegment.isSYN {
            // 是一个新连接的开始
            if let domain = await dnsInterceptor.getDomain(forFakeIP: ipPacket.destinationAddress) {
                // 如果目标IP是我们分配的假IP，则创建新的TCPConnection
                let key = "\(ipPacket.sourceAddress):\(tcpSegment.sourcePort)"
                
                guard tcpConnections[key] == nil else { return }

                let newConnection = TCPConnection(
                    key: key,
                    packetFlow: packetFlow,
                    sourceIP: ipPacket.sourceAddress,
                    sourcePort: tcpSegment.sourcePort,
                    destIP: ipPacket.destinationAddress, // 假IP
                    destPort: tcpSegment.destinationPort,
                    destinationHost: domain // 真实的域名
                )
                
                tcpConnections[key] = newConnection
                
                newConnection.start { [weak self] in
                    Task {
                        await self?.dnsInterceptor.releaseIP(for: domain)
                        await self?.removeConnection(key: key)
                    }
                }
            }
        } else if tcpSegment.isFIN || tcpSegment.isRST {
            // 是一个连接的结束
            let key = "\(ipPacket.sourceAddress):\(tcpSegment.sourcePort)"
            if let connection = tcpConnections[key] {
                connection.close()
            }
        } else {
            // 是连接中的数据传输
            let key = "\(ipPacket.sourceAddress):\(tcpSegment.sourcePort)"
            if let connection = tcpConnections[key] {
                // 适配您的 handlePacket 方法，只传递 TCP payload
                connection.handlePacket(payload: tcpSegment.payload)
            }
        }
    }
    
    private func removeConnection(key: String) {
        tcpConnections.removeValue(forKey: key)
        NSLog("[ConnectionManager] Removed connection for key: \(key)")
    }
    
    // ... 其他辅助函数，如 createResponsePacket ...
}
