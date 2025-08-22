// The Swift Programming Language
// https://docs.swift.org/swift-book

enum ConnectionPriority: Int, Comparable {
    case low = 0
    case normal = 1
    case high = 2
    case critical = 3
    
    static func < (lhs: ConnectionPriority, rhs: ConnectionPriority) -> Bool {
        return lhs.rawValue < rhs.rawValue
    }
}
