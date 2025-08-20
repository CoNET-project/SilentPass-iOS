//
//  DNSInterceptor.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//

import Foundation
import NetworkExtension
import Network

final actor DNSInterceptor {
    static let shared = DNSInterceptor()   // ✅ 全局单例

    // 映射
    private var fakeIPToDomainMap: [IPv4Address: String] = [:]
    private var domainToFakeIPMap: [String: IPv4Address] = [:]

    // 引用计数（不再触发删除）
    private var holds: [IPv4Address: Int] = [:]

    // 预留但默认不用的空闲列表（长期映射不回收）
    private var freeList: [IPv4Address] = []

    // 假 IP 池（198.18.0.0/16，排除 198.18.0.0 与 198.18.255.255）
    private var ipPool: [IPv4Address] = []
    private var nextIPIndex = 0

    // 持久化
    private static let persistKey = "DNSInterceptor.FakeIPMap.v1"

    private struct PersistBlob: Codable {
        var nextIPIndex: Int
        var mapping: [String:String] // ipString -> domain
    }

    // MARK: - 静态读取（供 init 使用）
    private static func loadPersistedBlob() -> PersistBlob? {
        guard let data = UserDefaults.standard.data(forKey: Self.persistKey) else { return nil }
        return try? JSONDecoder().decode(PersistBlob.self, from: data)
    }

    private init() {
        var pool: [IPv4Address] = []
        // 198.18.0.0/15 -> second octet = 18 or 19
        for second in 18...19 {
            for third in 0...255 {
                for fourth in 0...255 {
                    // 按 /15 语义排除网络地址与广播地址：
                    // 网络地址:     198.18.0.0
                    // 广播地址:     198.19.255.255
                    if (second == 18 && third == 0 && fourth == 0) ||
                       (second == 19 && third == 255 && fourth == 255) {
                        continue
                    }
                    let ip = "198.\(second).\(third).\(fourth)"
                    if let a = IPv4Address(ip) { pool.append(a) }
                }
            }
        }
        self.ipPool = pool
        NSLog("[DNSInterceptor] Initialized with RFC2544 fake IP pool /15, size=\(pool.count)")

        // 同步恢复（不会触发 actor 隔离错误）
        if let blob = Self.loadPersistedBlob() {
            // 维持你原有的边界处理方式
            self.nextIPIndex = min(max(0, blob.nextIPIndex), ipPool.count)

            var ipToDomain: [IPv4Address:String] = [:]
            var domainToIP: [String:IPv4Address] = [:]
            for (ipStr, dom) in blob.mapping {
                if let ip = IPv4Address(ipStr) {
                    let d = Self.normalize(dom)     // ✅ 静态归一化
                    ipToDomain[ip] = d
                    domainToIP[d] = ip
                }
            }
            self.fakeIPToDomainMap = ipToDomain
            self.domainToFakeIPMap  = domainToIP
            NSLog("[DNSInterceptor] Restored \(ipToDomain.count) mappings, next=\(self.nextIPIndex)")
        }
    }

    // 统一域名归一化：小写、去尾点、尽力 IDNA（优先 punycode，失败则原样）
    @inline(__always)
    private static func normalize(_ domain: String) -> String {
        let trimmed = domain.trimmingCharacters(in: .whitespacesAndNewlines)
        let noDot = trimmed.hasSuffix(".") ? String(trimmed.dropLast()) : trimmed
        // ✅ 尝试用 URLComponents 获得 ASCII/punycode host（Foundation 通常会做 IDNA）
        if !noDot.isEmpty {
            var comps = URLComponents()
            comps.scheme = "https"
            comps.host = noDot
            // percentEncodedHost 对 host 通常就是 ASCII（含 punycode），优先取它
            if let h1 = comps.percentEncodedHost, !h1.isEmpty {
                return h1.lowercased()
            }
            if let h2 = comps.host, !h2.isEmpty {
                return h2.lowercased()
            }
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

    /// 判定是否属于 198.18.0.0/16 假 IP
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

    func getDomain(forFakeIP ip: IPv4Address) -> String? {
        fakeIPToDomainMap[ip]
    }

    // MARK: - DNS 处理（用于隧道里拦截 DNS 包）

    /// 解析查询并构造应答：
    /// - A 记录：分配/取回假 IP，回复 A；
    /// - 其他类型：**仍然分配/取回假 IP**，但回复 NODATA+SOA(10s)
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
            // 非 A：预建映射，返回 NODATA + SOA(10s)
            _ = allocOrGet(for: domain)
            let resp = createNoDataSOAResponse(for: queryData, negativeTTL: 10)
            NSLog("[DNSInterceptor] Non-A query \(domain), qtype=\(qtype) -> NODATA + SOA(10s)")
            return (resp, nil)
        }
    }

    // MARK: - 内部分配/持久化

    private func allocateFakeIP(for domain: String) -> IPv4Address? {
        if let existing = domainToFakeIPMap[domain] { return existing }
        let fakeIP: IPv4Address
        if let reused = freeList.popLast() {
            fakeIP = reused
        } else {
            guard nextIPIndex < ipPool.count else {
                NSLog("[DNSInterceptor] Error: Fake IP pool exhausted!")
                return nil
            }
            fakeIP = ipPool[nextIPIndex]
            nextIPIndex += 1
        }
        fakeIPToDomainMap[fakeIP] = domain
        domainToFakeIPMap[domain] = fakeIP
        NSLog("[DNSInterceptor] Allocated fake IP \(fakeIP) for domain \(domain)")
        persistNow()
        return fakeIP
    }

    // 写入持久化（实例里写，使用 static key）
    private func persistNow() {
        var dict: [String:String] = [:]
        dict.reserveCapacity(fakeIPToDomainMap.count)
        for (ip, d) in fakeIPToDomainMap {
            dict[ip.debugDescription] = d
        }
        let blob = PersistBlob(nextIPIndex: nextIPIndex, mapping: dict)
        if let data = try? JSONEncoder().encode(blob) {
            UserDefaults.standard.set(data, forKey: Self.persistKey)
        }
    }

    // 可选：供调试时手动加载（调用方需 await）
    private func loadPersisted() async {
        guard let data = UserDefaults.standard.data(forKey: Self.persistKey) else { return }
        do {
            let blob = try JSONDecoder().decode(PersistBlob.self, from: data)
            self.nextIPIndex = min(max(0, blob.nextIPIndex), ipPool.count)
            var ipToDomain: [IPv4Address:String] = [:]
            var domainToIP: [String:IPv4Address] = [:]
            for (ipStr, d) in blob.mapping {
                if let ip = IPv4Address(ipStr) {
                    let nd = Self.normalize(d)
                    ipToDomain[ip] = nd
                    domainToIP[nd] = ip
                }
            }
            self.fakeIPToDomainMap = ipToDomain
            self.domainToFakeIPMap  = domainToIP
            NSLog("[DNSInterceptor] Restored \(ipToDomain.count) mappings, next=\(self.nextIPIndex)")
        } catch {
            NSLog("[DNSInterceptor] Persist load error: \(error.localizedDescription)")
        }
    }

    // MARK: - DNS 响应构造与工具

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
        // ID
        resp[0] = queryData[0]; resp[1] = queryData[1]
        // Flags: QR=1 | AA=1 | RD回显 | RA=1 | NOERROR
        let rd = queryData[2] & 0x01
        let cd = queryData[3] & 0x10
        resp[2] = 0x80 | rd | 0x04
        resp[3] = 0x80 | cd

        // QD=1, AN=0, NS=1, AR=0
        resp[4] = 0x00; resp[5] = 0x01
        resp[6] = 0x00; resp[7] = 0x00
        resp[8] = 0x00; resp[9] = 0x01
        resp[10] = 0x00; resp[11] = 0x00

        // Question echo
        resp.append(queryData[12..<(qnameEnd + qfixed)])

        // Authority: SOA
        let nsName = dnsName("ns.invalid.")
        let rname  = dnsName("hostmaster.invalid.")

        resp.append(contentsOf: [0xC0, 0x0C]) // NAME ptr
        appendU16BE(6, to: &resp)            // TYPE=SOA
        appendU16BE(1, to: &resp)            // CLASS=IN
        appendU32BE(negativeTTL, to: &resp)  // TTL

        var rdata = Data()
        rdata.append(contentsOf: nsName)
        rdata.append(contentsOf: rname)
        appendU32BE(1, to: &rdata)           // SERIAL
        appendU32BE(0, to: &rdata)           // REFRESH
        appendU32BE(0, to: &rdata)           // RETRY
        appendU32BE(0, to: &rdata)           // EXPIRE
        appendU32BE(negativeTTL, to: &rdata) // MINIMUM

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
        // ID
        resp[0] = queryData[0]; resp[1] = queryData[1]
        // Flags: QR=1 | RD回显 | RA=1 | NOERROR
        let rd = queryData[2] & 0x01
        let cd = queryData[3] & 0x10
        resp[2] = 0x80 | rd
        resp[3] = 0x80 | cd

        // QD=1, AN=1, NS=0, AR=0（若带 EDNS 会在下方改 AR）
        resp[4] = 0x00; resp[5] = 0x01
        resp[6] = 0x00; resp[7] = 0x01
        resp[8] = 0x00; resp[9] = 0x00
        resp[10] = 0x00; resp[11] = 0x00

        // Question echo
        resp.append(questionSection)

        @inline(__always) func appendU16BE(_ v: UInt16, to d: inout Data) {
            d.append(UInt8(v >> 8)); d.append(UInt8(v & 0xFF))
        }
        @inline(__always) func appendU32BE(_ v: UInt32, to d: inout Data) {
            d.append(UInt8((v >> 24) & 0xFF)); d.append(UInt8((v >> 16) & 0xFF))
            d.append(UInt8((v >> 8) & 0xFF));  d.append(UInt8(v & 0xFF))
        }

        // Answer: A
        let ttlSeconds: UInt32 = 10
        resp.append(contentsOf: [0xC0, 0x0C])          // NAME -> 指针到 QNAME
        appendU16BE(1, to: &resp)                      // TYPE=A
        appendU16BE(1, to: &resp)                      // CLASS=IN
        appendU32BE(ttlSeconds, to: &resp)             // TTL
        appendU16BE(4, to: &resp)                      // RDLENGTH
        resp.append(ipAddress.rawValue)                // RDATA

        // EDNS(0) 回显（若原查询带了 OPT）
        if let opt = parseQueryOPT(queryData, questionEnd: questionEnd) {
            resp[10] = 0x00; resp[11] = 0x01   // ARCOUNT = 1
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
        guard type == 41 else { return nil } // OPT

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

        resp.append(0x00)                 // NAME=root
        appendU16BE(41)                   // TYPE=OPT
        appendU16BE(udpSize)              // CLASS=udp size
        var ttl: UInt32 = 0               // extRCODE=0 | version | Z
        ttl |= UInt32(version) << 16
        if doBit { ttl |= 0x0000_8000 }   // DO bit
        appendU32BE(ttl)
        appendU16BE(0)                    // RDLEN=0
    }
}

// MARK: - DNS 工具
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
