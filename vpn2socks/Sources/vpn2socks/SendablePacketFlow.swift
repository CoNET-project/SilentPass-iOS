//
//  SendablePacketFlow.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//
import Foundation
import NetworkExtension
import Network

final class SendablePacketFlow: @unchecked Sendable {
    private let packetFlow: NEPacketTunnelFlow
    private let queue = DispatchQueue(label: "com.vpn2socks.packetFlowQueue")

    init(packetFlow: NEPacketTunnelFlow) {
        self.packetFlow = packetFlow
    }

    func readPackets() async -> ([Data], [NSNumber]) {
        await withCheckedContinuation { continuation in
            queue.async {
                Task {
                    let result = await self.packetFlow.readPackets()
                    continuation.resume(returning: result)
                }
            }
        }
    }

    func writePackets(_ packets: [Data], withProtocols protocols: [NSNumber]) {
        queue.async {
            self.packetFlow.writePackets(packets, withProtocols: protocols)
        }
    }
}
