//
//  TCPSegment.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//
import Foundation

// CORRECTED: Added sequenceNumber property
struct TCPSegment {
    var sourcePort: UInt16
    var destinationPort: UInt16
    var sequenceNumber: UInt32
    var acknowledgementNumber: UInt32
    var isSYN: Bool
    var isACK: Bool
    var isFIN: Bool
    var isRST: Bool
    var payload: Data

    init?(data: Data) {
        guard data.count >= 20 else { return nil }
        self.sourcePort = (UInt16(data[0]) << 8) | UInt16(data[1])
        self.destinationPort = (UInt16(data[2]) << 8) | UInt16(data[3])
        self.sequenceNumber = data.subdata(in: 4..<8).withUnsafeBytes { $0.load(as: UInt32.self).bigEndian }
        self.acknowledgementNumber = data.subdata(in: 8..<12).withUnsafeBytes { $0.load(as: UInt32.self).bigEndian }
        
        let flags = data[13]
        self.isFIN = (flags & 0x01) != 0
        self.isSYN = (flags & 0x02) != 0
        self.isRST = (flags & 0x04) != 0
        self.isACK = (flags & 0x10) != 0
        
        let dataOffset = Int(data[12] >> 4) * 4
        guard data.count >= dataOffset else { return nil }
        self.payload = data.subdata(in: dataOffset..<data.count)
    }
}
