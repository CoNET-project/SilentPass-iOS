//
//  PacketTunnelProvider.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Updated with advanced DNS Interception.
//
import NetworkExtension
import Network




// MARK: - Main Provider Class

class PacketTunnelProvider: NEPacketTunnelProvider {

    private var connectionManager: ConnectionManager?

    override func startTunnel(options: [String : NSObject]?, completionHandler: @escaping (Error?) -> Void) {
        NSLog("[PacketTunnelProvider] Starting tunnel...")
        
        // 1. Configure the virtual network interface using a safe private range.
        let tunnelNetworkSettings = NEPacketTunnelNetworkSettings(tunnelRemoteAddress: "8.8.8.8")
        
        let tunnelIPAddress = "172.16.0.1"
        let tunnelSubnetMask = "255.255.255.0"
        let fakeDNSServer = "172.16.0.2"
        
        tunnelNetworkSettings.ipv4Settings = NEIPv4Settings(addresses: [tunnelIPAddress], subnetMasks: [tunnelSubnetMask])
        tunnelNetworkSettings.ipv4Settings?.includedRoutes = [NEIPv4Route.default()]
        
        // 2. CRITICAL: Set up the DNS trap. All DNS queries will be sent to our fake server.
        tunnelNetworkSettings.dnsSettings = NEDNSSettings(servers: [fakeDNSServer])
        NSLog("[PacketTunnelProvider] DNS trap set for \(fakeDNSServer)")

        // 3. Apply the settings
        setTunnelNetworkSettings(tunnelNetworkSettings) { [weak self] error in
            guard let self = self else { return }

            if let error = error {
                NSLog("[PacketTunnelProvider] Failed to set tunnel network settings: \(error)")
                completionHandler(error)
                return
            }
            
            // 4. Initialize the new ConnectionManager
            self.connectionManager = ConnectionManager(packetFlow: self.packetFlow, fakeDNSServer: fakeDNSServer)
            
            Task {
                await self.connectionManager?.start()
            }
            
            NSLog("[PacketTunnelProvider] Tunnel started successfully.")
            completionHandler(nil)
        }
    }
    
    override func stopTunnel(with reason: NEProviderStopReason, completionHandler: @escaping () -> Void) {
        NSLog("[PacketTunnelProvider] Stopping tunnel.")
        connectionManager = nil
        completionHandler()
    }
}

// MARK: - Connection Manager

/// Manages all network traffic, intercepting DNS and forwarding TCP connections.
final actor ConnectionManager {

    private let packetFlow: NEPacketTunnelFlow
    private let fakeDNSServer: IPv4Address
    private let dnsInterceptor = DNSInterceptor()
    private var tcpConnections: [String: TCPConnection] = [:]

    init(packetFlow: NEPacketTunnelFlow, fakeDNSServer: String) {
        self.packetFlow = packetFlow
        self.fakeDNSServer = IPv4Address(fakeDNSServer)!
    }

    func start() {
        Task { await readPackets() }
    }

    private func readPackets() async {
        while true {
            // Destructure the tuple into a named variable for the packet data.
            // Use an underscore (_) to ignore the second element (protocol numbers) since it's not needed here.
            let (packetsData, _) = await packetFlow.readPackets()
            
            // Loop over the correctly named array `packetsData`.
            for packetData in packetsData {
                guard let ipPacket = IPv4Packet(data: packetData) else { continue }
                
                switch ipPacket.protocol {
                case 6: // TCP
                    await handleTCPPacket(ipPacket)
                case 17: // UDP
                    await handleUDPPacket(ipPacket)
                default:
                    break
                }
            }
        }
    }
    
    /// Intercepts DNS queries, allocates a fake IP, and sends a spoofed response.
    private func handleUDPPacket(_ ipPacket: IPv4Packet) async {
        guard ipPacket.destinationAddress == self.fakeDNSServer,
              let udp = UDPDatagram(data: ipPacket.payload),
              udp.destinationPort == 53 else {
            return
        }

        if let result = await dnsInterceptor.handleQueryAndCreateResponse(for: udp.payload) {
            let responsePacket = createResponsePacket(
                sourceAddress: self.fakeDNSServer,
                sourcePort: 53,
                destinationAddress: ipPacket.sourceAddress,
                destinationPort: udp.sourcePort,
                payload: result.response
            )
            packetFlow.writePackets([responsePacket], withProtocols: [AF_INET as NSNumber])
        }
    }
    
    /// Handles TCP packets, creating new connections for SYN packets to our fake IPs.
    private func handleTCPPacket(_ ipPacket: IPv4Packet) async {
        guard let tcpSegment = TCPSegment(data: ipPacket.payload) else { return }
        
        let connectionKey = "\(ipPacket.sourceAddress):\(tcpSegment.sourcePort)"
        
        if tcpSegment.isSYN {
            // New connection attempt. Check if it's for one of our fake IPs.
            if let domain = await dnsInterceptor.getDomain(forFakeIP: ipPacket.destinationAddress) {
                guard tcpConnections[connectionKey] == nil else {
                    NSLog("[ConnectionManager] Ignoring duplicate SYN for \(connectionKey)")
                    return
                }

                NSLog("[ConnectionManager] New connection for \(domain) via fake IP \(ipPacket.destinationAddress)")
                
                let newConnection = TCPConnection(
                    key: connectionKey,
                    packetFlow: packetFlow,
                    sourceIP: ipPacket.sourceAddress,
                    sourcePort: tcpSegment.sourcePort,
                    destIP: ipPacket.destinationAddress, // The fake IP
                    destPort: tcpSegment.destinationPort,
                    destinationHost: domain // The real domain
                )
                
                tcpConnections[connectionKey] = newConnection
                
                newConnection.start { [weak self] in
                    Task {
                        // This closure is called when the TCPConnection is closed.
                        await self?.dnsInterceptor.releaseIP(for: domain)
                        await self?.removeConnection(key: connectionKey)
                    }
                }
            }
        } else if tcpSegment.isFIN || tcpSegment.isRST {
            if let connection = tcpConnections[connectionKey] {
                connection.close() // This will trigger the onClose handler to do the cleanup.
            }
        } else {
            // Data for an existing connection.
            if let connection = tcpConnections[connectionKey] {
                connection.handlePacket(payload: tcpSegment.payload)
            }
        }
    }
    
    private func removeConnection(key: String) {
        if tcpConnections.removeValue(forKey: key) != nil {
            NSLog("[ConnectionManager] Removed connection for key: \(key)")
        }
    }
    
    // MARK: - Packet Creation Helpers
    
    private func createResponsePacket(sourceAddress: IPv4Address, sourcePort: UInt16, destinationAddress: IPv4Address, destinationPort: UInt16, payload: Data) -> Data {
        let udpHeader = createUDPHeader(sourcePort: sourcePort, destPort: destinationPort, payloadLength: payload.count)
        let ipHeader = createIPv4Header(source: sourceAddress, destination: destinationAddress, payloadLength: udpHeader.count + payload.count, protocol: 17)
        return ipHeader + udpHeader + payload
    }

    private func createUDPHeader(sourcePort: UInt16, destPort: UInt16, payloadLength: Int) -> Data {
        var header = Data(count: 8)
        let totalLength = 8 + payloadLength
        header[0] = UInt8(sourcePort >> 8); header[1] = UInt8(sourcePort & 0xFF)
        header[2] = UInt8(destPort >> 8);   header[3] = UInt8(destPort & 0xFF)
        header[4] = UInt8(totalLength >> 8);header[5] = UInt8(totalLength & 0xFF)
        // TODO: A real implementation MUST calculate the UDP checksum.
        header[6] = 0x00; header[7] = 0x00
        return header
    }

    private func createIPv4Header(source: IPv4Address, destination: IPv4Address, payloadLength: Int, protocol: UInt8) -> Data {
        var header = Data(count: 20)
        let totalLength = 20 + payloadLength
        header[0] = 0x45
        header[2] = UInt8(totalLength >> 8); header[3] = UInt8(totalLength & 0xFF)
        header[8] = 64 // TTL
        header[9] = `protocol`
        source.rawValue.copyBytes(to: &header[12], count: 4)
        destination.rawValue.copyBytes(to: &header[16], count: 4)
        // TODO: A real implementation MUST calculate the IP header checksum.
        header[10] = 0x00; header[11] = 0x00
        return header
    }
}

// MARK: - DNS Interceptor

/// Allocates "fake" IPs for domain names and provides spoofed DNS responses.
actor DNSInterceptor {
    private var fakeIPToDomainMap: [IPv4Address: String] = [:]
    private var domainToFakeIPMap: [String: IPv4Address] = [:]
    
    private var ipPool: [IPv4Address]
    private var nextIPIndex = 0

    init() {
        // Use the safe CGNAT address space (100.64.0.0/10) for fake IPs.
        self.ipPool = (1...254).map { IPv4Address("100.64.0.\($0)")! }
        NSLog("[DNSInterceptor] Initialized with CGNAT fake IP pool.")
    }
    
    private func allocateFakeIP(for domain: String) -> IPv4Address? {
        if let existingIP = domainToFakeIPMap[domain] {
            return existingIP
        }
        guard nextIPIndex < ipPool.count else {
            NSLog("[DNSInterceptor] Fake IP pool exhausted!")
            return nil
        }
        let fakeIP = ipPool[nextIPIndex]
        nextIPIndex += 1
        fakeIPToDomainMap[fakeIP] = domain
        domainToFakeIPMap[domain] = fakeIP
        NSLog("[DNSInterceptor] Allocated fake IP \(fakeIP) for domain \(domain)")
        return fakeIP
    }
    
    func handleQueryAndCreateResponse(for queryData: Data) -> (response: Data, fakeIP: IPv4Address)? {
        guard let domain = extractDomainName(from: queryData) else { return nil }
        guard let fakeIP = allocateFakeIP(for: domain) else { return nil }
        let responsePayload = createDNSResponse(for: queryData, ipAddress: fakeIP)
        return (responsePayload, fakeIP)
    }
    
    func getDomain(forFakeIP ip: IPv4Address) -> String? {
        return fakeIPToDomainMap[ip]
    }
    
    func releaseIP(for domain: String) {
        if let ip = domainToFakeIPMap.removeValue(forKey: domain) {
            fakeIPToDomainMap.removeValue(forKey: ip)
            NSLog("[DNSInterceptor] Released IP \(ip) for domain \(domain)")
            // A more robust implementation would manage recycling of IPs.
        }
    }

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

/// Helper to extract domain name from a raw DNS query payload.
func extractDomainName(from dnsQueryPayload: Data) -> String? {
    guard dnsQueryPayload.count > 12 else { return nil }
    var domainParts: [String] = []
    var currentIndex = 12
    while currentIndex < dnsQueryPayload.count {
        let length = Int(dnsQueryPayload[currentIndex])
        if length == 0 { break }
        currentIndex += 1
        if currentIndex + length > dnsQueryPayload.count { return nil }
        if let label = String(bytes: dnsQueryPayload[currentIndex..<(currentIndex+length)], encoding: .utf8) {
            domainParts.append(label)
        }
        currentIndex += length
    }
    return domainParts.joined(separator: ".")
}
