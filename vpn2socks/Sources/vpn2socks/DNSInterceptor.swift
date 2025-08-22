//  DNSInterceptor.swift
//  vpn2socks
//
//  Integrated with FakeIPAllocator for lazy on-demand fake-IP generation
//  Created by peter on 2025-08-17. Updated by ChatGPT on 2025-08-21
//

import Foundation
import NetworkExtension
import Network


// MARK: - DNSInterceptor (actor)
final actor DNSInterceptor {
    static let shared = DNSInterceptor()   // ✅ Singleton

    // 映射
    private var fakeIPToDomainMap: [IPv4Address: String] = [:]
    private var domainToFakeIPMap: [String: IPv4Address] = [:]

    // 引用计数（不再触发删除）
    private var holds: [IPv4Address: Int] = [:]

    // 可选回收列表（长期映射默认不回收）
    private var freeList: [IPv4Address] = []

    // ✅ Lazy fake-IP allocator
    private var allocator: FakeIPAllocator

    // 持久化
    private static let persistKey = "DNSInterceptor.FakeIPMap.v2" // bump to v2 for allocator state

    private struct PersistBlob: Codable {
        var mapping: [String:String] // ipString -> domain
        var rangeIndex: Int
        var nextIP: UInt32
    }

    // MARK: - 静态读取（供 init 使用）
    private static func loadPersistedBlob() -> PersistBlob? {
        guard let data = UserDefaults.standard.data(forKey: Self.persistKey) else { return nil }
        return try? JSONDecoder().decode(PersistBlob.self, from: data)
    }

    private init() {
        // Build ranges based on your deployment needs.
        // Primary RFC 2544 /15 fake network 198.18.0.0–198.19.255.255
        let rfc2544 = IPv4Range(cidr: "198.18.0.0/15")!
        // (Optional) another pool often used for TUN inner-facing addresses
        // let tunPool = IPv4Range(cidr: "10.8.0.0/16")!
        let ranges = [rfc2544]

        // Reserve network and broadcast of the /15 to match your previous implementation
        // 网络地址: 198.18.0.0, 广播地址: 198.19.255.255
        let reserved: Set<UInt32> = [IPv4Address("198.18.0.0")!.u32,
                                     IPv4Address("198.19.255.255")!.u32]

        var alloc = FakeIPAllocator(ranges: ranges, reserved: reserved)

        // Restore mappings and allocator cursor if available
        if let blob = Self.loadPersistedBlob() {
            var ipToDomain: [IPv4Address:String] = [:]
            var domainToIP: [String:IPv4Address] = [:]
            for (ipStr, dom) in blob.mapping {
                if let ip = IPv4Address(ipStr) {
                    let d = Self.normalize(dom)
                    ipToDomain[ip] = d
                    domainToIP[d] = ip
                }
            }
            self.fakeIPToDomainMap = ipToDomain
            self.domainToFakeIPMap  = domainToIP
            alloc.restoreCursor(rangeIndex: blob.rangeIndex, nextIP: blob.nextIP)
            NSLog("[DNSInterceptor] Restored \(ipToDomain.count) mappings, cursor=(range=\(blob.rangeIndex), nextIP=\(blob.nextIP)))")
        }

        self.allocator = alloc
        NSLog("[DNSInterceptor] Initialized with lazy FakeIPAllocator (RFC2544 /15)")
    }

    // 统一域名归一化：小写、去尾点、尽力 IDNA（优先 punycode，失败则原样）
    @inline(__always)
    private static func normalize(_ domain: String) -> String {
        let trimmed = domain.trimmingCharacters(in: .whitespacesAndNewlines)
        let noDot = trimmed.hasSuffix(".") ? String(trimmed.dropLast()) : trimmed
        if !noDot.isEmpty {
            var comps = URLComponents()
            comps.scheme = "https"
            comps.host = noDot
            if let h1 = comps.percentEncodedHost, !h1.isEmpty { return h1.lowercased() }
            if let h2 = comps.host, !h2.isEmpty { return h2.lowercased() }
        }
        return noDot.lowercased()
    }

    // MARK: - API

    /// 为域名分配或取回假 IP
    func allocOrGet(for domain: String) -> IPv4Address? {
        let d = Self.normalize(domain)
        if let ip = domainToFakeIPMap[d] { return ip }
        return allocateFakeIP(for: d)
    }

    /// 通过假 IP 反查域名
    func lookupDomain(by ip: IPv4Address) -> String? {
        fakeIPToDomainMap[ip]
    }

    /// 判定是否属于 198.18.0.0/16 假 IP（保持与旧代码兼容）
    func contains(_ ip: IPv4Address) -> Bool {
        let oct = ip.rawValue
        return oct.count == 4 && oct[0] == 198 && oct[1] == 18
    }

    /// 引用计数 +1（可选）
    func retain(fakeIP: IPv4Address) { holds[fakeIP, default: 0] += 1 }

    /// 引用计数 -1（不删除映射）
    func release(fakeIP: IPv4Address) {
        if let c = holds[fakeIP], c > 1 {
            holds[fakeIP] = c - 1
        } else {
            holds.removeValue(forKey: fakeIP)
        }
        // 长期映射：不从 map 删除，也不回收到 freeList
    }

    func registerMapping(fakeIP: IPv4Address, domain: String) {
        let d = Self.normalize(domain)
        if let existed = fakeIPToDomainMap[fakeIP], existed != d {
            NSLog("[DNSInterceptor] ⚠️ Overwrite mapping \(fakeIP) : \(existed) -> \(d)")
        }
        fakeIPToDomainMap[fakeIP] = d
        domainToFakeIPMap[d] = fakeIP
        persistNow()
    }

    func getDomain(forFakeIP ip: IPv4Address) -> String? { fakeIPToDomainMap[ip] }

    // MARK: - DNS 处理

    func handleQueryAndCreateResponse(for queryData: Data) -> (response: Data, fakeIP: IPv4Address?)? {
        guard let qname = extractDomainName(from: queryData) else { return nil }
        let domain = Self.normalize(qname)
        let qtype = extractQType(from: queryData) ?? 0

        if qtype == 1 {
            guard let fakeIP = allocOrGet(for: domain) else { return nil }
            let resp = createDNSResponse(for: queryData, ipAddress: fakeIP)
            NSLog("[DNSInterceptor] Created A record response for \(domain) -> \(fakeIP)")
            return (resp, fakeIP)
        } else {
            _ = allocOrGet(for: domain)
            let resp = createNoDataSOAResponse(for: queryData, negativeTTL: 10)
            NSLog("[DNSInterceptor] Non-A query \(domain), qtype=\(qtype) -> NODATA + SOA(10s)")
            return (resp, nil)
        }
    }

    // MARK: - 内部分配/持久化

    private func allocateFakeIP(for domain: String) -> IPv4Address? {
        if let existing = domainToFakeIPMap[domain] { return existing }
        // Prefer returning from DNSInterceptor's freeList if you ever add recycling
        if let reused = freeList.popLast() {
            fakeIPToDomainMap[reused] = domain
            domainToFakeIPMap[domain] = reused
            NSLog("[DNSInterceptor] Reused fake IP \(reused) for domain \(domain)")
            persistNow()
            return reused
        }
        var a = allocator
        guard let ip = a.allocate() else {
            NSLog("[DNSInterceptor] Error: Fake IP pool exhausted!")
            return nil
        }
        allocator = a // write back mutated state
        fakeIPToDomainMap[ip] = domain
        domainToFakeIPMap[domain] = ip
        NSLog("[DNSInterceptor] Allocated fake IP \(ip) for domain \(domain)")
        persistNow()
        return ip
    }

    private func persistNow() {
        var dict: [String:String] = [:]
        dict.reserveCapacity(fakeIPToDomainMap.count)
        for (ip, d) in fakeIPToDomainMap { dict[ip.debugDescription] = d }
        let snap = allocator.snapshotCursor()
        let blob = PersistBlob(mapping: dict, rangeIndex: snap.rangeIndex, nextIP: snap.nextIP)
        if let data = try? JSONEncoder().encode(blob) {
            UserDefaults.standard.set(data, forKey: Self.persistKey)
        }
    }

    // MARK: - DNS response builders & tools (unchanged)

    private func createNoDataSOAResponse(for queryData: Data, negativeTTL: UInt32) -> Data {
        guard queryData.count >= 12 else { return Data() }
        var idx = 12
        while idx < queryData.count, queryData[idx] != 0 {
            let len = Int(queryData[idx]); idx += 1 + len
            if idx > queryData.count { return Data() }
        }
        let qnameEnd = idx
        let qfixed = 5
        guard queryData.count >= qnameEnd + qfixed else { return Data() }

        @inline(__always) func appendU16BE(_ v: UInt16, to d: inout Data) {
            d.append(UInt8(v >> 8)); d.append(UInt8(v & 0xFF))
        }
        @inline(__always) func appendU32BE(_ v: UInt32, to d: inout Data) {
            d.append(UInt8((v >> 24) & 0xFF)); d.append(UInt8((v >> 16) & 0xFF))
            d.append(UInt8((v >> 8) & 0xFF));  d.append(UInt8(v & 0xFF))
        }
        func dnsName(_ s: String) -> [UInt8] {
            var out: [UInt8] = []
            for label in s.split(separator: ".") where !label.isEmpty {
                out.append(UInt8(label.utf8.count))
                out.append(contentsOf: label.utf8)
            }
            out.append(0)
            return out
        }

        var resp = Data(count: 12)
        resp[0] = queryData[0]; resp[1] = queryData[1]
        let rd = queryData[2] & 0x01
        let cd = queryData[3] & 0x10
        resp[2] = 0x80 | rd | 0x04
        resp[3] = 0x80 | cd
        resp[4] = 0x00; resp[5] = 0x01
        resp[6] = 0x00; resp[7] = 0x00
        resp[8] = 0x00; resp[9] = 0x01
        resp[10] = 0x00; resp[11] = 0x00
        resp.append(queryData[12..<(qnameEnd + qfixed)])
        let nsName = dnsName("ns.invalid.")
        let rname  = dnsName("hostmaster.invalid.")
        resp.append(contentsOf: [0xC0, 0x0C])
        appendU16BE(6, to: &resp)
        appendU16BE(1, to: &resp)
        appendU32BE(negativeTTL, to: &resp)
        var rdata = Data()
        rdata.append(contentsOf: nsName)
        rdata.append(contentsOf: rname)
        appendU32BE(1, to: &rdata)
        appendU32BE(0, to: &rdata)
        appendU32BE(0, to: &rdata)
        appendU32BE(0, to: &rdata)
        appendU32BE(negativeTTL, to: &rdata)
        appendU16BE(UInt16(rdata.count), to: &resp)
        resp.append(rdata)
        return resp
    }

    private func createDNSResponse(for queryData: Data, ipAddress: IPv4Address) -> Data {
        guard queryData.count >= 12 else { return Data() }
        var idx = 12
        while idx < queryData.count, queryData[idx] != 0 {
            let len = Int(queryData[idx]); idx += 1 + len
            if idx > queryData.count { return Data() }
        }
        guard idx < queryData.count else { return Data() }
        let qnameEnd = idx
        let qfixed = 5
        let questionEnd = qnameEnd + qfixed
        guard queryData.count >= questionEnd else { return Data() }
        let questionSection = queryData[12..<questionEnd]
        var resp = Data(count: 12)
        resp[0] = queryData[0]; resp[1] = queryData[1]
        let rd = queryData[2] & 0x01
        let cd = queryData[3] & 0x10
        resp[2] = 0x80 | rd
        resp[3] = 0x80 | cd
        resp[4] = 0x00; resp[5] = 0x01
        resp[6] = 0x00; resp[7] = 0x01
        resp[8] = 0x00; resp[9] = 0x00
        resp[10] = 0x00; resp[11] = 0x00
        resp.append(questionSection)
        @inline(__always) func appendU16BE(_ v: UInt16, to d: inout Data) {
            d.append(UInt8(v >> 8)); d.append(UInt8(v & 0xFF))
        }
        @inline(__always) func appendU32BE(_ v: UInt32, to d: inout Data) {
            d.append(UInt8((v >> 24) & 0xFF)); d.append(UInt8((v >> 16) & 0xFF))
            d.append(UInt8((v >> 8) & 0xFF));  d.append(UInt8(v & 0xFF))
        }
        let ttlSeconds: UInt32 = 10
        resp.append(contentsOf: [0xC0, 0x0C])
        appendU16BE(1, to: &resp)
        appendU16BE(1, to: &resp)
        appendU32BE(ttlSeconds, to: &resp)
        appendU16BE(4, to: &resp)
        resp.append(ipAddress.rawValue)
        if let opt = parseQueryOPT(queryData, questionEnd: questionEnd) {
            resp[10] = 0x00; resp[11] = 0x01
            appendOPT(to: &resp, udpSize: opt.udpSize, version: opt.version, doBit: opt.doBit)
        }
        return resp
    }

    private func extractQType(from dnsQuery: Data) -> UInt16? {
        guard dnsQuery.count >= 12 else { return nil }
        var idx = 12
        while idx < dnsQuery.count {
            let len = Int(dnsQuery[idx])
            if len == 0 { break }
            idx += 1 + len
            if idx > dnsQuery.count { return nil }
        }
        guard idx + 2 < dnsQuery.count else { return nil }
        let hi = dnsQuery[idx + 1]
        let lo = dnsQuery[idx + 2]
        return (UInt16(hi) << 8) | UInt16(lo)
    }

    private func parseQueryOPT(_ queryData: Data, questionEnd: Int) -> (udpSize: UInt16, version: UInt8, doBit: Bool)? {
        guard queryData.count >= 12 else { return nil }
        let arcount = (UInt16(queryData[10]) << 8) | UInt16(queryData[11])
        guard arcount > 0 else { return nil }
        let idx = questionEnd
        guard idx + 11 <= queryData.count, queryData[idx] == 0x00 else { return nil }
        let type = (UInt16(queryData[idx+1]) << 8) | UInt16(queryData[idx+2])
        guard type == 41 else { return nil }
        let udpSize = (UInt16(queryData[idx+3]) << 8) | UInt16(queryData[idx+4])
        let version = queryData[idx+6]
        let zHi = queryData[idx+7], zLo = queryData[idx+8]
        let z = (UInt16(zHi) << 8) | UInt16(zLo)
        let doBit = (z & 0x8000) != 0
        let rdlen = (UInt16(queryData[idx+9]) << 8) | UInt16(queryData[idx+10])
        guard idx + 11 + Int(rdlen) <= queryData.count else { return nil }
        return (udpSize: udpSize == 0 ? 1232 : udpSize, version: version, doBit: doBit)
    }

    private func appendOPT(to resp: inout Data, udpSize: UInt16, version: UInt8, doBit: Bool) {
        @inline(__always) func appendU16BE(_ v: UInt16) {
            resp.append(UInt8(v >> 8)); resp.append(UInt8(v & 0xFF))
        }
        @inline(__always) func appendU32BE(_ v: UInt32) {
            resp.append(UInt8((v >> 24) & 0xFF)); resp.append(UInt8((v >> 16) & 0xFF))
            resp.append(UInt8((v >> 8) & 0xFF));  resp.append(UInt8(v & 0xFF))
        }
        resp.append(0x00)
        appendU16BE(41)
        appendU16BE(udpSize)
        var ttl: UInt32 = 0
        ttl |= UInt32(version) << 16
        if doBit { ttl |= 0x0000_8000 }
        appendU32BE(ttl)
        appendU16BE(0)
    }
}

// MARK: - DNS 工具（与原始版本一致）
fileprivate func extractDomainName(from dnsQueryPayload: Data) -> String? {
    guard dnsQueryPayload.count > 12 else { return nil }
    var labels: [String] = []
    var i = 12
    while i < dnsQueryPayload.count {
        let l = Int(dnsQueryPayload[i])
        if l == 0 { break }
        i += 1
        if i + l > dnsQueryPayload.count { return nil }
        if let s = String(bytes: dnsQueryPayload[i..<(i+l)], encoding: .utf8) { labels.append(s) }
        i += l
    }
    return labels.isEmpty ? nil : labels.joined(separator: ".")
}
