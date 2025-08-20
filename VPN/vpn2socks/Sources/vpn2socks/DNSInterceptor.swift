//
//  DNSInterceptor.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Updated to use a safe, non-conflicting IP address range.
//
import Foundation
import NetworkExtension
import Network

/// 拦截 DNS 查询，为域名分配临时的“假IP”，并提供伪造的DNS响应。
/// Allocates temporary "fake" IPs for domain names and provides spoofed DNS responses.
actor DNSInterceptor {
    // MARK: - Properties
    
    // 缓存: [假IP: 域名]
    private var fakeIPToDomainMap: [IPv4Address: String] = [:]
    // 反向映射，用于快速查找，避免重复分配
    private var domainToFakeIPMap: [String: IPv4Address] = [:]
    
    // “假IP”地址池
    private var ipPool: [IPv4Address]
    private var nextIPIndex = 0

    // MARK: - Initialization

    init() {
        // CRITICAL CHANGE: Initialize the IP pool from the safe Carrier-Grade NAT (CGNAT)
        // address space (100.64.0.0/10) to prevent conflicts with local networks.
        // For simplicity, we use a small subset of this large range.
        self.ipPool = (1...254).map { IPv4Address("100.64.0.\($0)")! }
        NSLog("[DNSInterceptor] Initialized with CGNAT fake IP pool (100.64.0.1 - 100.64.0.254)")
    }
    
    // MARK: - Public Methods
    
    /// 根据DNS查询数据，分配一个假IP并创建伪造的响应。
    /// Handles a query, allocates a fake IP, and creates a spoofed response.
    func handleQueryAndCreateResponse(for queryData: Data) -> (response: Data, fakeIP: IPv4Address)? {
        guard let domain = extractDomainName(from: queryData) else {
            NSLog("[DNSInterceptor] Failed to extract domain from query.")
            return nil
        }
        
        guard let fakeIP = allocateFakeIP(for: domain) else {
            return nil // Error is logged inside allocateFakeIP
        }
        
        let responsePayload = createDNSResponse(for: queryData, ipAddress: fakeIP)
        return (responsePayload, fakeIP)
    }
    
    /// 根据“假IP”查找对应的域名。
    /// Looks up the domain name corresponding to a given fake IP.
    func getDomain(forFakeIP ip: IPv4Address) -> String? {
        return fakeIPToDomainMap[ip]
    }
    
    /// 当连接关闭时，释放IP地址和域名的映射关系。
    /// Releases the mappings for a domain and its fake IP when a connection closes.
    func releaseIP(for domain: String) {
        if let ip = domainToFakeIPMap.removeValue(forKey: domain) {
            fakeIPToDomainMap.removeValue(forKey: ip)
            NSLog("[DNSInterceptor] Released IP \(ip) for domain \(domain)")
            // A more robust implementation would manage recycling of IPs back into the pool,
            // especially if the pool is exhausted.
        }
    }

    // MARK: - Private Helpers

    /// 为一个域名分配一个“假IP”。
    /// Allocates a fake IP for a given domain.
    private func allocateFakeIP(for domain: String) -> IPv4Address? {
        // If an IP has already been allocated for this domain, return it.
        if let existingIP = domainToFakeIPMap[domain] {
            return existingIP
        }
        
        // If the IP pool is exhausted, we cannot allocate a new IP.
        guard nextIPIndex < ipPool.count else {
            NSLog("[DNSInterceptor] Error: Fake IP pool exhausted!")
            return nil
        }
        
        let fakeIP = ipPool[nextIPIndex]
        nextIPIndex += 1
        
        // Store the bidirectional mapping.
        fakeIPToDomainMap[fakeIP] = domain
        domainToFakeIPMap[domain] = fakeIP
        
        NSLog("[DNSInterceptor] Allocated fake IP \(fakeIP) for domain \(domain)")
        return fakeIP
    }
    
    /// 创建一个伪造的DNS响应包。
    /// Creates a spoofed DNS response packet.
    private func createDNSResponse(for queryData: Data, ipAddress: IPv4Address) -> Data {
        // A standard DNS header is 12 bytes. The question section follows.
        let dnsHeaderEndIndex = 12

        // Ensure the data is long enough to contain a header.
        guard queryData.count > dnsHeaderEndIndex else { return Data() }

        // Create a slice of the data starting after the header.
        let querySlice = queryData[dnsHeaderEndIndex...]
        
        // Find the first null byte in that slice, which marks the end of the domain name.
        guard let questionEndIndex = querySlice.firstIndex(of: 0x00) else { return Data() }

        // The question section includes the name, type (2 bytes), and class (2 bytes).
        let questionSection = queryData[0..<(questionEndIndex + 5)]

        var response = Data()
        response.append(questionSection)
        
        // Set response flags (QR=1, Opcode=0, AA=1, TC=0, RD=1, RA=1, Z=0, RCODE=0)
        response[2] = 0x81
        response[3] = 0x80
        
        // Set Answer RRs count to 1
        response[7] = 0x01
        
        // Append the answer section
        // Pointer to question (0xc00c), Type A, Class IN, TTL (60s), Data Length (4 bytes)
        response.append(contentsOf: [0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x04])
        
        // Append the IPv4 address
        response.append(ipAddress.rawValue)
        
        return response
    }
}

/// 辅助函数：从原始DNS查询负载中提取域名。
/// Helper function: Extracts the domain name from a raw DNS query payload.
fileprivate func extractDomainName(from dnsQueryPayload: Data) -> String? {
    // DNS Question format starts after a 12-byte header
    guard dnsQueryPayload.count > 12 else { return nil }
    var domainParts: [String] = []
    var currentIndex = 12
    while currentIndex < dnsQueryPayload.count {
        let length = Int(dnsQueryPayload[currentIndex])
        if length == 0 { break } // End of domain name
        currentIndex += 1
        if currentIndex + length > dnsQueryPayload.count { return nil } // Malformed
        if let label = String(bytes: dnsQueryPayload[currentIndex..<(currentIndex+length)], encoding: .utf8) {
            domainParts.append(label)
        }
        currentIndex += length
    }
    
    guard !domainParts.isEmpty else { return nil }
    
    return domainParts.joined(separator: ".")
}
