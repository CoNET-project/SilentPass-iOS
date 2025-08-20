//
//  UDPDatagram.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//
import Foundation
// A simplified UDP datagram parser.

struct UDPDatagram {
    var sourcePort: UInt16
    var destinationPort: UInt16
    var payload: Data
    
    init?(data: Data) {
        guard data.count >= 8 else { return nil }
        self.sourcePort = (UInt16(data[0]) << 8) | UInt16(data[1])
        self.destinationPort = (UInt16(data[2]) << 8) | UInt16(data[3])
        self.payload = data.subdata(in: 8..<data.count)
    }
}
