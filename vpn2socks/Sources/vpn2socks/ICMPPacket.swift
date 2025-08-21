//
//  ICMPPacket.swift
//  vpn2socks
//
//  Created by peter on 2025-08-20.
//

import Foundation

/// ICMPv4 header: Type(1) + Code(1) + Checksum(2) + RestOfHeader(4)
struct ICMPPacket {
    var type: UInt8
    var code: UInt8
    var checksum: UInt16
    var restOfHeader: UInt32
    var payload: Data

    init(type: UInt8, code: UInt8, payload: Data) {
        self.type = type
        self.code = code
        self.restOfHeader = 0
        self.payload = payload
        self.checksum = 0
        self.checksum = ICMPPacket.checksum(self.data)
    }

    /// 序列化成 Data
    var data: Data {
        var buf = Data()
        buf.append(type)
        buf.append(code)
        buf.append(UInt8((checksum >> 8) & 0xff))
        buf.append(UInt8(checksum & 0xff))
        var ro = restOfHeader.bigEndian
        withUnsafeBytes(of: &ro) { buf.append(contentsOf: $0) }
        buf.append(payload)
        return buf
    }

    /// 构造 ICMP Port Unreachable (Type=3, Code=3)
    static func unreachable(for ip: IPv4Packet) -> ICMPPacket {
        let needed = ip.headerLength + 8
        let headerBytes = ip.rawData.prefix(min(ip.rawData.count, needed))
        return ICMPPacket(type: 3, code: 3, payload: headerBytes)
    }

    /// 计算 ICMP 校验和
    static func checksum(_ data: Data) -> UInt16 {
        var sum: UInt32 = 0
        var i = 0
        while i + 1 < data.count {
            let word = UInt16(data[i]) << 8 | UInt16(data[i+1])
            sum &+= UInt32(word)
            i += 2
        }
        if i < data.count {
            sum &+= UInt32(data[i]) << 8
        }
        while (sum >> 16) > 0 {
            sum = (sum & 0xFFFF) &+ (sum >> 16)
        }
        return ~UInt16(sum & 0xFFFF)
    }
}
