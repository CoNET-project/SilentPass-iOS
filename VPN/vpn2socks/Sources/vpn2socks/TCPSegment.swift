//
//  TCPSegment.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//

// 一个简化的 TCP 段解析器
// A simplified TCP segment parser
struct TCPSegment {
    var sourcePort: UInt16
    var destinationPort: UInt16
    var isSYN: Bool
    var isFIN: Bool
    var isRST: Bool
    var payload: Data

    init?(data: Data) {
        guard data.count >= 20 else { return nil }
        self.sourcePort = (UInt16(data[0]) << 8) | UInt16(data[1])
        self.destinationPort = (UInt16(data[2]) << 8) | UInt16(data[3])
        let flags = data[13]
        self.isFIN = (flags & 0x01) != 0
        self.isSYN = (flags & 0x02) != 0
        self.isRST = (flags & 0x04) != 0
        let dataOffset = Int(data[12] >> 4) * 4
        guard data.count >= dataOffset else { return nil }
        self.payload = data.subdata(in: dataOffset..<data.count)
    }
}
