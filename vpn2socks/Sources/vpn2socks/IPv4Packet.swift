//
//  IPv4Packet.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//
import NetworkExtension
import Network

struct IPv4Packet {
    var sourceAddress: IPv4Address
    var destinationAddress: IPv4Address
    var `protocol`: UInt8
    var payload: Data

    init?(data: Data) {
        guard data.count >= 20 else { return nil }
        let versionAndIHL = data[0]
        guard (versionAndIHL >> 4) == 4 else { return nil }
        let ihl = Int(versionAndIHL & 0x0F) * 4
        guard data.count >= ihl else { return nil }

        self.protocol = data[9]
        self.sourceAddress = IPv4Address(Data(data[12...15]))!
        self.destinationAddress = IPv4Address(Data(data[16...19]))!
        self.payload = data.subdata(in: ihl..<data.count)
    }
}

