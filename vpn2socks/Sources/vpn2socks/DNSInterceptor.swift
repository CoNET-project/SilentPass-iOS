//  DNSInterceptor.swift
//  vpn2socks
//
//  Integrated with FakeIPAllocator for lazy on-demand fake-IP generation
//  Added intelligent DNS proxy for APNs domain forwarding
//  Created by peter on 2025-08-17. Updated by ChatGPT on 2025-08-21
//

import Foundation
import NetworkExtension
import Network
import os.log

private let logger = Logger(subsystem: "com.vpn2socks", category: "DNSInterceptor")

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

    // ✅ DNS转发相关
    private let upstreamDNSServers = ["8.8.8.8", "1.1.1.1", "208.67.222.222"] // Google, Cloudflare, OpenDNS
    private var dnsCache: [String: DNSCacheEntry] = [:]
    private let dnsCacheTTL: TimeInterval = 300 // 5分钟缓存
    
    // DNS查询超时和重试
    private let dnsQueryTimeout: TimeInterval = 3.0
    private let maxDNSRetries = 2

    // ✅ APNs 域名列表（支持直连的域名）
    private let apnsDomains: Set<String> = [
            "conet.network",
            "silentpass.io",
            "openpgp.online",
        // Apple Push 相关
            "conet.network",
            "apple.com",
            "push.apple.com",
            "icloud.com",
            "push-apple.com.akadns.net",
            "silentpass.io",
            "courier.push.apple.com",
            "gateway.push.apple.com",
            "gateway.sandbox.push.apple.com",
            "gateway.icloud.com",
            "bag.itunes.apple.com",
            "init.itunes.apple.com",
            "xp.apple.com",
            "gsa.apple.com",
            "gsp-ssl.ls.apple.com",
            "gsp-ssl.ls-apple.com.akadns.net",
            "mesu.apple.com",
            "gdmf.apple.com",
            "deviceenrollment.apple.com",
            "mdmenrollment.apple.com",
            "iprofiles.apple.com",
            "ppq.apple.com",
            
        // 🔥 微信（WeChat）相关域名
            "wechat.com",
            "weixin.qq.com",
            "weixin110.qq.com",
            "tenpay.com",
            "mm.taobao.com",
            "wx.qq.com",
            "web.wechat.com",
            "webpush.weixin.qq.com",
            "qpic.cn",
            "qlogo.cn",
            "wx.gtimg.com",
            "minorshort.weixin.qq.com",
            "log.weixin.qq.com",
            "szshort.weixin.qq.com",
            "szminorshort.weixin.qq.com",
            "szextshort.weixin.qq.com",
            "hkshort.weixin.qq.com",
            "hkminorshort.weixin.qq.com",
            "hkextshort.weixin.qq.com",
            "hklong.weixin.qq.com",
            "sgshort.wechat.com",
            "sgminorshort.wechat.com",
            "sglong.wechat.com",
            "usshort.wechat.com",
            "usminorshort.wechat.com",
            "uslong.wechat.com",
            
            // 微信支付
            "pay.weixin.qq.com",
            "payapp.weixin.qq.com",
            
            // 微信文件传输
            "file.wx.qq.com",
            "support.weixin.qq.com",
            
            // 微信 CDN
            "mmbiz.qpic.cn",
            "mmbiz.qlogo.cn",
            "mmsns.qpic.cn",
            
            // 腾讯推送服务
            "dns.weixin.qq.com",
            "short.weixin.qq.com",
            "long.weixin.qq.com",
            
            
    ]

    // 持久化
    private static let persistKey = "DNSInterceptor.FakeIPMap.v2"

    private struct PersistBlob: Codable {
        var mapping: [String:String] // ipString -> domain
        var rangeIndex: Int
        var nextIP: UInt32
    }
    
    private var lastAPNsResolveAt: Date = .distantPast

    // ✅ DNS缓存条目
    private struct DNSCacheEntry {
        let response: Data
        let timestamp: Date
        let ttl: TimeInterval
        
        var isExpired: Bool {
            Date().timeIntervalSince(timestamp) > ttl
        }
    }

    // MARK: - 静态读取（供 init 使用）
    private static func loadPersistedBlob() -> PersistBlob? {
        guard let data = UserDefaults.standard.data(forKey: Self.persistKey) else { return nil }
        return try? JSONDecoder().decode(PersistBlob.self, from: data)
    }

    private init() {
        // Build ranges based on your deployment needs.
        let rfc2544 = IPv4Range(cidr: "198.18.0.0/15")!
        let ranges = [rfc2544]

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
            logger.info("Restored \(ipToDomain.count) mappings, cursor=(range=\(blob.rangeIndex), nextIP=\(blob.nextIP)))")
        }

        self.allocator = alloc
        logger.info("Initialized with lazy FakeIPAllocator (RFC2544 /15) + intelligent DNS proxy")
        
        // ✅ 修复：使用 Task 包装异步调用
        Task { [weak self] in
            await self?.startCacheCleanupTask()
        }
    }

    // 统一域名归一化
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

    // ✅ 检测域名是否应该直连（APNs 域名）
    private func shouldBypassTunnel(_ domain: String) -> Bool {
        let normalized = Self.normalize(domain)
        
        // 精确匹配
        if apnsDomains.contains(normalized) {
            return true
        }
        
        // 后缀匹配（支持子域名）
        for apnsDomain in apnsDomains {
            if normalized.hasSuffix("." + apnsDomain) {
                return true
            }
        }
        
        // 通配符匹配 Apple 推送相关域名
        if normalized.contains("push.apple.com") ||
           normalized.contains("gateway.apple.com") ||
           normalized.hasSuffix(".push.apple.com") {
            return true
        }
        
        return false
    }

    // MARK: - API

    /// 为域名分配或取回假 IP
    func allocOrGet(for domain: String) -> IPv4Address? {
        let d = Self.normalize(domain)
        
        // ✅ 检查是否应该绕过隧道
        if shouldBypassTunnel(d) {
            logger.info("Bypassing tunnel for APNs domain: \(d)")
            
            // 清理可能存在的旧映射
            if let existingFakeIP = domainToFakeIPMap[d] {
                domainToFakeIPMap.removeValue(forKey: d)
                fakeIPToDomainMap.removeValue(forKey: existingFakeIP)
                logger.info("Removed stale fake IP mapping for APNs domain: \(d)")
                persistNow()
            }
            
            return nil  // 返回 nil 表示不分配假 IP，应该直连
        }
        
        if let ip = domainToFakeIPMap[d] { return ip }
        return allocateFakeIP(for: d)
    }

    /// 通过假 IP 反查域名
    func lookupDomain(by ip: IPv4Address) -> String? {
        fakeIPToDomainMap[ip]
    }

    /// 判定是否属于 198.18.0.0/15 假 IP
    func contains(_ ip: IPv4Address) -> Bool {
        let o = ip.rawValue
        return o.count == 4 && o[0] == 198 && (o[1] == 18 || o[1] == 19)
    }

    /// 引用计数 +1
    func retain(fakeIP: IPv4Address) { holds[fakeIP, default: 0] += 1 }

    /// 引用计数 -1
    func release(fakeIP: IPv4Address) {
        if let c = holds[fakeIP], c > 1 {
            holds[fakeIP] = c - 1
        } else {
            holds.removeValue(forKey: fakeIP)
        }
    }

    func registerMapping(fakeIP: IPv4Address, domain: String) {
        let d = Self.normalize(domain)
        if let existed = fakeIPToDomainMap[fakeIP], existed != d {
            logger.warning("Overwrite mapping \(String(describing: fakeIP)) : \(existed) -> \(d)")
        }
        fakeIPToDomainMap[fakeIP] = d
        domainToFakeIPMap[d] = fakeIP
        persistNow()
    }

    func getDomain(forFakeIP ip: IPv4Address) -> String? { fakeIPToDomainMap[ip] }

    // MARK: - DNS 处理 (改进版本)

    func handleQueryAndCreateResponse(for queryData: Data) async -> (response: Data, fakeIP: IPv4Address?)? {
        guard let qname = extractDomainName(from: queryData) else { return nil }
        let domain = Self.normalize(qname)
        let qtype = extractQType(from: queryData) ?? 0

        // ✅ 检查是否应该绕过隧道 - 使用智能转发
        if shouldBypassTunnel(domain) {
            logger.info("DNS forwarding for APNs domain: \(domain)")
            // 🔑 关键改进：转发到上游DNS而不是返回NXDOMAIN
            return await forwardToUpstreamDNS(queryData: queryData, domain: domain, qtype: qtype)
        }

        // 普通域名的处理逻辑
        if qtype == 1 {
            guard let fakeIP = allocOrGet(for: domain) else { return nil }
            let resp = createDNSResponse(for: queryData, ipAddress: fakeIP)
            logger.debug("Created A record response for \(domain) -> \(String(describing: fakeIP))")
            return (resp, fakeIP)
        } else {
            _ = allocOrGet(for: domain)
            let resp = createNoDataSOAResponse(for: queryData, negativeTTL: 10)
            logger.debug("Non-A query \(domain), qtype=\(qtype) -> NODATA + SOA(10s)")
            return (resp, nil)
        }
    }

    
    // 提供最近一次 APNs 直连域名解析的时间，用于健康检测
    func getLastAPNsResolveAt() -> Date { lastAPNsResolveAt }
    // MARK: - 智能DNS转发实现

    /// ✅ 核心功能：转发DNS查询到上游服务器
    private func forwardToUpstreamDNS(queryData: Data, domain: String, qtype: UInt16) async -> (response: Data, fakeIP: IPv4Address?)? {
        // 检查缓存
        let cacheKey = "\(domain):\(qtype)"
        if let cached = dnsCache[cacheKey], !cached.isExpired {
            logger.debug("DNS cache hit for \(domain)")
            return (cached.response, nil)
        }
        
        // 如果只处理A记录查询
        guard qtype == 1 else {
            logger.debug("Non-A query for APNs domain \(domain), returning NXDOMAIN")
            return (createNXDomainResponse(for: queryData), nil)
        }
        
        // 尝试从上游DNS服务器获取真实IP
        for (index, dnsServer) in upstreamDNSServers.enumerated() {
            logger.debug("Querying upstream DNS \(dnsServer) for \(domain) (attempt \(index + 1))")
            
            if let response = await queryUpstreamDNS(queryData: queryData, serverIP: dnsServer, domain: domain) {
                // 缓存成功的响应
                let cacheEntry = DNSCacheEntry(response: response, timestamp: Date(), ttl: dnsCacheTTL)
                dnsCache[cacheKey] = cacheEntry
                
                logger.info("Successfully resolved \(domain) via upstream DNS \(dnsServer)")
                self.lastAPNsResolveAt = Date()
                return (response, nil)
            }
        }
        
        // 所有上游DNS都失败，返回临时失败
        logger.error("All upstream DNS servers failed for \(domain)")
        return (createTempFailResponse(for: queryData), nil)
    }

    /// 查询上游DNS服务器
    private func queryUpstreamDNS(queryData: Data, serverIP: String, domain: String) async -> Data? {
        return await withCheckedContinuation { continuation in
            // 创建UDP连接到上游DNS
            let endpoint = NWEndpoint.Host(serverIP)
            let port = NWEndpoint.Port(integerLiteral: 53)
            
            let connection = NWConnection(host: endpoint, port: port, using: .udp)
            
            // ✅ 修复：使用 @Sendable 闭包和线程安全的状态管理
            let stateBox = StateBox()
            
            let timeoutTask = Task {
                try? await Task.sleep(nanoseconds: UInt64(dnsQueryTimeout * 1_000_000_000))
                if stateBox.tryComplete() {
                    connection.cancel()
                    continuation.resume(returning: nil)
                }
            }
            
            connection.stateUpdateHandler = { [stateBox] state in
                switch state {
                case .ready:
                    // 发送DNS查询
                    connection.send(content: queryData, completion: .contentProcessed { [stateBox] error in
                        if let error = error {
                            logger.error("Failed to send DNS query to \(serverIP): \(error)")
                            if stateBox.tryComplete() {
                                timeoutTask.cancel()
                                continuation.resume(returning: nil)
                            }
                            return
                        }
                        
                        // 接收响应
                        connection.receive(minimumIncompleteLength: 1, maximumLength: 512) { [stateBox] data, _, _, error in
                            if stateBox.tryComplete() {
                                timeoutTask.cancel()
                                connection.cancel()
                                
                                if let error = error {
                                    logger.error("Failed to receive DNS response from \(serverIP): \(error)")
                                    continuation.resume(returning: nil)
                                } else if let data = data {
                                    logger.debug("Received DNS response from \(serverIP): \(data.count) bytes")
                                    continuation.resume(returning: data)
                                } else {
                                    continuation.resume(returning: nil)
                                }
                            }
                        }
                    })
                    
                case .failed(let error):
                    logger.error("DNS connection to \(serverIP) failed: \(error)")
                    if stateBox.tryComplete() {
                        timeoutTask.cancel()
                        continuation.resume(returning: nil)
                    }
                    
                case .cancelled:
                    if stateBox.tryComplete() {
                        timeoutTask.cancel()
                        continuation.resume(returning: nil)
                    }
                    
                default:
                    break
                }
            }
            
            connection.start(queue: .global(qos: .userInitiated))
        }
    }

    // MARK: - 缓存管理

    /// 启动缓存清理任务
    private func startCacheCleanupTask() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 60_000_000_000) // 每分钟清理一次
                await self?.cleanupExpiredCache()
            }
        }
    }

    /// 清理过期的DNS缓存
    private func cleanupExpiredCache() {
        let beforeCount = dnsCache.count
        dnsCache = dnsCache.filter { !$0.value.isExpired }
        let afterCount = dnsCache.count
        
        if beforeCount != afterCount {
            logger.debug("Cleaned up \(beforeCount - afterCount) expired DNS cache entries")
        }
    }

    // MARK: - 内部分配/持久化

    private func allocateFakeIP(for domain: String) -> IPv4Address? {
        if let existing = domainToFakeIPMap[domain] { return existing }
        
        if let reused = freeList.popLast() {
            fakeIPToDomainMap[reused] = domain
            domainToFakeIPMap[domain] = reused
            logger.debug("Reused fake IP \(String(describing: reused)) for domain \(domain)")
            persistNow()
            return reused
        }
        
        var a = allocator
        guard let ip = a.allocate() else {
            logger.error("Fake IP pool exhausted!")
            return nil
        }
        allocator = a
        fakeIPToDomainMap[ip] = domain
        domainToFakeIPMap[domain] = ip
        logger.debug("Allocated fake IP \(String(describing: ip)) for domain \(domain)")
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

    // MARK: - DNS response builders

    /// 创建临时失败响应 (SERVFAIL)
    private func createTempFailResponse(for queryData: Data) -> Data {
        guard queryData.count >= 12 else { return Data() }
        var resp = Data(queryData.prefix(12))
        
        // 设置响应标志
        resp[2] = 0x80 | (queryData[2] & 0x01) // QR=1, RD保持
        resp[3] = 0x82 // SERVFAIL (RCODE=2)
        
        // 复制问题段
        if queryData.count > 12 {
            resp.append(queryData[12...])
        }
        
        return resp
    }

    /// 创建 NXDOMAIN 响应
    private func createNXDomainResponse(for queryData: Data) -> Data {
        guard queryData.count >= 12 else { return Data() }
        var idx = 12
        while idx < queryData.count, queryData[idx] != 0 {
            let len = Int(queryData[idx]); idx += 1 + len
            if idx > queryData.count { return Data() }
        }
        let qnameEnd = idx
        let qfixed = 5
        guard queryData.count >= qnameEnd + qfixed else { return Data() }

        var resp = Data(count: 12)
        resp[0] = queryData[0]; resp[1] = queryData[1]
        let rd = queryData[2] & 0x01
        let cd = queryData[3] & 0x10
        resp[2] = 0x80 | rd
        resp[3] = 0x83 | cd  // NXDOMAIN (RCODE = 3)
        resp[4] = 0x00; resp[5] = 0x01  // QDCOUNT = 1
        resp[6] = 0x00; resp[7] = 0x00  // ANCOUNT = 0
        resp[8] = 0x00; resp[9] = 0x00  // NSCOUNT = 0
        resp[10] = 0x00; resp[11] = 0x00 // ARCOUNT = 0
        resp.append(queryData[12..<(qnameEnd + qfixed)])
        
        return resp
    }

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

// MARK: - 线程安全状态管理
final class StateBox: @unchecked Sendable {
    private let lock = NSLock()
    private var completed = false
    
    func tryComplete() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if completed {
            return false
        }
        completed = true
        return true
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
