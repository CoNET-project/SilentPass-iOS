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
    let rawData: Data   // 原始 IP 数据

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
        self.rawData = data
    }

    /// 返回 IP header 长度 (bytes)
    var headerLength: Int {
        let ihl = Int(rawData[0] & 0x0F)
        return ihl * 4
    }
}

public extension IPv4Address {
    init?(_ value: UInt32) {
        var v = value.bigEndian
        let data = Data(bytes: &v, count: 4)
        self.init(data)
    }

    var u32: UInt32 {
        let b = self.rawValue
        return (UInt32(b[0]) << 24) | (UInt32(b[1]) << 16) | (UInt32(b[2]) << 8) | UInt32(b[3])
    }
}
