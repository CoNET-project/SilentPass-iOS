//
//  FakeIPAllocator.swift
//  vpn2socks
//
//  Created by peter on 2025-08-21.
//

import Foundation
import Network

/// 198.18.0.0/15 的按需分配器（排除 198.18.0.0 和 198.19.255.255）
struct FakeIPAllocator {
    private var ranges: [IPv4Range]
    private var rangeIndex: Int
    private var nextIP: UInt32 // cursor pointing to candidate in current range
    private var reserved: Set<UInt32> // explicit exclusions (e.g. first/last of /15)
    private var freeList: [UInt32] = [] // optional reuse bucket


    init(ranges: [IPv4Range], reserved: Set<UInt32>) {
        precondition(!ranges.isEmpty, "ranges must not be empty")
        self.ranges = ranges
        self.rangeIndex = 0
        self.nextIP = ranges[0].start
        self.reserved = reserved
        self.advanceToValid()
    }


    mutating func restoreCursor(rangeIndex: Int, nextIP: UInt32) {
        self.rangeIndex = max(0, min(rangeIndex, ranges.count - 1))
        self.nextIP = nextIP
        self.advanceToValid()
    }


    mutating func pushBack(_ ip: IPv4Address) { freeList.append(ip.u32) }


    mutating func allocate() -> IPv4Address? {
        // Prefer reusing from freeList if any
        if let raw = freeList.popLast(), !reserved.contains(raw) {
        return IPv4Address(raw)
    }
    guard rangeIndex < ranges.count else { return nil }
        let candidate = nextIP
        guard let out = IPv4Address(candidate) else { return nil }
        moveCursorForward()
        return out
    }


    // MARK: - Cursor helpers
    private mutating func moveCursorForward() {
        guard rangeIndex < ranges.count else { return }
        let r = ranges[rangeIndex]
        if nextIP < r.end { nextIP &+= 1 } else {
            // move to next range
            rangeIndex &+= 1
            if rangeIndex < ranges.count {
                nextIP = ranges[rangeIndex].start
            }
        }
        advanceToValid()
    }


    private mutating func advanceToValid() {
        while rangeIndex < ranges.count {
            let r = ranges[rangeIndex]
            if nextIP > r.end { rangeIndex &+= 1; continue }
            if reserved.contains(nextIP) { nextIP &+= 1; continue }
            break
        }
    }


    // For persistence
    func snapshotCursor() -> (rangeIndex: Int, nextIP: UInt32) { (rangeIndex, nextIP) }
}
