import Foundation

/// Minimal, self-contained TCP segment parser used by ConnectionManager/TCPConnection.
/// This file intentionally **does not** depend on `IPv4Packet` to avoid cross-type
/// issues (e.g. `.totalLength`, address string conversions, etc.).
struct TCPSegment {
    // MARK: Parsed header fields
    let sourcePort: UInt16
    let destinationPort: UInt16
    let sequenceNumber: UInt32
    let acknowledgementNumber: UInt32
    /// TCP data offset (header length) in 32-bit words (RFC 793 §3.1)
    private let dataOffsetWords: UInt8
    let flagsRaw: UInt8
    let windowSize: UInt16
    let checksum: UInt16
    let urgentPointer: UInt16

    // MARK: Options/payload
    let options: Data
    let payload: Data

    // MARK: Flag helpers
    var isFIN: Bool { (flagsRaw & 0x01) != 0 }
    var isSYN: Bool { (flagsRaw & 0x02) != 0 }
    var isRST: Bool { (flagsRaw & 0x04) != 0 }
    var isPSH: Bool { (flagsRaw & 0x08) != 0 }
    var isACK: Bool { (flagsRaw & 0x10) != 0 }
    var isURG: Bool { (flagsRaw & 0x20) != 0 }
    var isECE: Bool { (flagsRaw & 0x40) != 0 }
    var isCWR: Bool { (flagsRaw & 0x80) != 0 }

    /// Header length in bytes.
    var headerLength: Int { Int(dataOffsetWords) * 4 }

    init?(data: Data) {
        // Require at least the fixed header (20 bytes)
        guard data.count >= 20 else { return nil }

        // Fast local helpers to read big-endian integers
        func be16(_ range: Range<Int>) -> UInt16 {
            return data.subdata(in: range).withUnsafeBytes { $0.load(as: UInt16.self).bigEndian }
        }
        func be32(_ range: Range<Int>) -> UInt32 {
            return data.subdata(in: range).withUnsafeBytes { $0.load(as: UInt32.self).bigEndian }
        }

        self.sourcePort = be16(0..<2)
        self.destinationPort = be16(2..<4)
        self.sequenceNumber = be32(4..<8)
        self.acknowledgementNumber = be32(8..<12)

        let offsetAndReserved = data[12]
        self.dataOffsetWords = offsetAndReserved >> 4
        self.flagsRaw = data[13]

        self.windowSize = be16(14..<16)
        self.checksum = be16(16..<18)
        self.urgentPointer = be16(18..<20)

        // Validate header length
        let headerLen = Int(self.dataOffsetWords) * 4
        guard headerLen >= 20, data.count >= headerLen else { return nil }

        if headerLen > 20 {
            self.options = data.subdata(in: 20..<headerLen)
        } else {
            self.options = Data()
        }
        self.payload = data.subdata(in: headerLen..<data.count)
    }
}
