//
//  IPv4Range.swift
//  vpn2socks
//
//  Created by peter on 2025-08-21.
//

import Foundation
import Network

public struct IPv4Range: Codable {
    public let start: UInt32
    public let end: UInt32 // inclusive

    public init?(cidr: String) {
        let parts = cidr.split(separator: "/")
        guard parts.count == 2,
              let prefix = Int(parts[1]),
              let base = IPv4Address(String(parts[0]))?.u32,
              (0...32).contains(prefix) else { return nil }
        let hostBits = 32 - prefix
        let mask: UInt32 = hostBits == 32 ? 0 : ~((1 << hostBits) - 1)
        let network = base & mask
        let broadcast = network | ~mask
        self.start = network
        self.end   = broadcast
    }

    public func contains(_ ip: UInt32) -> Bool { ip >= start && ip <= end }
}
