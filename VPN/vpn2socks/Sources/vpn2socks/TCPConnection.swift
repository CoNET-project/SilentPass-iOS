//
//  TCPConnection.swift
//  vpn2socks
//
//  Created by peter on 2025-08-17.
//  Rewritten on 2025-08-17.
//
import NetworkExtension
import Network

// 代表一个从 Packet Tunnel 转发到本地 SOCKS5 服务器的 TCP 连接。
// Assumes a SOCKS5 server is already running locally on 127.0.0.1:8888.
final actor TCPConnection {
    // MARK: - Properties

    // 本地 SOCKS5 代理服务器的固定地址
    private static let socksHost = NWEndpoint.Host("127.0.0.1")
    private static let socksPort = NWEndpoint.Port(integerLiteral: 8888)

    // 唯一标识此连接，通常是 "sourceIP:sourcePort"
    let key: String
    
    // 从 NEPacketTunnelProvider 传入的 packet flow，用于读写 IP 数据包
    private let packetFlow: NEPacketTunnelFlow
    
    // 原始连接的四元组信息
    private let sourceIP: IPv4Address
    private let sourcePort: UInt16
    private let destIP: IPv4Address
    private let destPort: UInt16/Users/peter/Downloads/CoNET-1.1.5 7/vpn2socks/Sources/vpn2socks/PacketTunnelProvider.swift

    // 到本地 SOCKS5 服务器的 NWConnection
    private var socksConnection: NWConnection?
    // 当此连接关闭时，通知外部管理者的回调
    private var closeHandler: (() -> Void)?
    // NEW: 从 DNS 拦截中获取的真实目标域名
    private let destinationHost: String?

    // MARK: - Initialization

    init(key: String, packetFlow: NEPacketTunnelFlow, sourceIP: IPv4Address, sourcePort: UInt16, destIP: IPv4Address, destPort: UInt16, destinationHost: String?) {
        self.key = key
        self.packetFlow = packetFlow
        self.sourceIP = sourceIP
        self.sourcePort = sourcePort
        self.destIP = destIP
        self.destPort = destPort
        self.destinationHost = destinationHost
        
        // 使用 NSLog 保持与您代码风格的一致
        NSLog("[TCPConnection \(key)] Initialized. Destination: \(destinationHost ?? destIP.debugDescription)")

    }

    // MARK: - Connection Lifecycle

    /// 启动到本地 SOCKS5 服务器的连接。
    /// 启动到本地 SOCKS5 服务器的连接。
    func start(onClose: @escaping () -> Void) {
        guard self.socksConnection == nil else {
            log("Warning: Start called on an already active connection.")
            return
        }
        
        self.closeHandler = onClose
        
        let params = NWParameters.tcp
        let connection = NWConnection(host: TCPConnection.socksHost, port: TCPConnection.socksPort, using: params)
        self.socksConnection = connection

        connection.stateUpdateHandler = { [weak self] newState in
            Task { [weak self] in
                await self?.handleStateUpdate(newState)
            }
        }
        
        log("Starting connection to SOCKS proxy...")
        connection.start(queue: .global(qos: .userInitiated))
    }

    /// 关闭并清理连接
       func close() {
           if socksConnection?.state != .cancelled {
               log("Closing connection.")
               socksConnection?.cancel()
               socksConnection = nil
               closeHandler?()
               closeHandler = nil
           }
       }

    // MARK: - State Handling

    private func handleStateUpdate(_ newState: NWConnection.State) {
        switch newState {
        case .ready:
            log("TCP connection to proxy is ready. Starting SOCKS handshake.")
            performSocksHandshake()
            
        case .failed(let error):
            log("Connection failed: \(error.localizedDescription)")
            close()
            
        case .cancelled:
            close()
            
        default:
            break
        }
    }

    // MARK: - SOCKS5 Handshake Logic

    // MARK: - SOCKS5 Handshake Logic

    private func performSocksHandshake() {
        guard let connection = socksConnection else { return }

        let handshake: [UInt8] = [0x05, 0x01, 0x00]
        
        connection.send(content: Data(handshake), completion: .contentProcessed({ [weak self] error in
            Task { [weak self] in
                if let error = error {
                    await self?.log("Handshake send error: \(error)")
                    await self?.close()
                    return
                }
                
                await self?.log("Step 1: Handshake sent. Waiting for server response...")
                await self?.receiveHandshakeResponse()
            }
        }))
    }


    /// SOCKS5 握手第二步: 接收服务器选择的认证方法。
    private func receiveHandshakeResponse() {
        guard let connection = socksConnection else { return }

        connection.receive(minimumIncompleteLength: 2, maximumLength: 2) { [weak self] (data, _, _, error) in
            Task { [weak self] in
                guard let self = self else { return }
                
                if let error = error {
                    await self.log("Handshake receive error: \(error)")
                    await self.close()
                    return
                }
                
                guard let data = data, data.count == 2, data[0] == 0x05, data[1] == 0x00 else {
                    await self.log("Handshake failed. Server response: \(data?.hexEncodedString() ?? "empty")")
                    await self.close()
                    return
                }
                
                await self.log("Step 1: Handshake response received. Server accepted 'No Auth'.")
                await self.sendSocksConnectRequest()
            }
        }
    }
    
    // MODIFIED: 这是核心修改，现在可以智能地选择使用域名或IP
        /// SOCKS5 第三步: 发送 CONNECT 请求，告诉代理我们想连接到哪里。
    private func sendSocksConnectRequest() {
        guard let connection = socksConnection else { return }

        var request = Data()
        request.append(0x05) // Version
        request.append(0x01) // Command: CONNECT
        request.append(0x00) // Reserved
        
        // ✅ Change 'var' to 'let'
        let logMessage: String
        
        // Check for a domain name and use it first
        if let host = self.destinationHost, !host.isEmpty {
            logMessage = "Step 2: Sending connect request for DOMAIN \(host):\(self.destPort)"
            // Address Type: Domain name
            request.append(0x03)
            let domainData = Data(host.utf8)
            request.append(UInt8(domainData.count))
            request.append(domainData)
        } else {
            logMessage = "Step 2: Sending connect request for IP \(self.destIP):\(self.destPort)"
            // Address Type: IPv4
            request.append(0x01)
            request.append(self.destIP.rawValue)
        }

        // Port number
        request.append(contentsOf: [UInt8(self.destPort >> 8), UInt8(self.destPort & 0xFF)])

        connection.send(content: request, completion: .contentProcessed({ [weak self] error in
            Task { [weak self] in
                if let error = error {
                    await self?.log("Connect request send error: \(error)")
                    await self?.close()
                    return
                }
                // Now this is safe because logMessage is a constant
                await self?.log(logMessage)
                await self?.receiveConnectResponse()
            }
        }))
    }
        
    
    /// SOCKS5 第四步: 接收服务器对 CONNECT 请求的回应。
    private func receiveConnectResponse() {
        guard let connection = socksConnection else { return }

        // 响应通常至少有10个字节长（对于IPv4地址）
        connection.receive(minimumIncompleteLength: 4, maximumLength: 256) { [weak self] (data, _, _, error) in
            Task { [weak self] in
                guard let self = self else { return }
                
                if let error = error {
                    await self.log("Connect response receive error: \(error)")
                    await self.close()
                    return
                }
                
                // 检查响应是否成功: [Ver, REP, RSV, ATYP, ...]
                // REP (Reply field): 0x00 = succeeded
                guard let data = data, data.count >= 4, data[0] == 0x05, data[1] == 0x00 else {
                    await self.log("Connect request failed. Server reply: \(data?.hexEncodedString() ?? "empty")")
                    await self.close()
                    return
                }
                
                await self.log("Step 2: Connect response received. SOCKS tunnel established!")
                // SOCKS 隧道完全建立，现在可以开始双向转发数据
                await self.readFromSocks()
            }
        }
    }

    // MARK: - Data Forwarding

    /// 处理从 VPN 隧道传来的 TCP payload 并通过 SOCKS 连接发送出去。
    func handlePacket(payload: Data) {
        guard !payload.isEmpty else { return }
        
        socksConnection?.send(content: payload, completion: .contentProcessed({ [weak self] error in
            Task { [weak self] in
                if let error = error {
                    await self?.log("Error sending data to SOCKS: \(error)")
                    await self?.close()
                }
            }
        }))
    }

    /// 持续从 SOCKS 连接中读取数据。
    private func readFromSocks() {
        socksConnection?.receive(minimumIncompleteLength: 1, maximumLength: 65535) { [weak self] (data, _, isComplete, error) in
            Task { [weak self] in
                guard let self = self else { return }
                
                // 如果有错误或连接已完成，则关闭
                if let error = error {
                    await self.log("Receive error from SOCKS, closing connection: \(error)")
                    await self.close()
                    return
                }
                
                if isComplete {
                    await self.log("SOCKS connection closed by remote. Closing.")
                    await self.close()
                    return
                }

                // 将收到的数据写回 VPN 隧道
                if let data = data, !data.isEmpty {
                    await self.writeToTunnel(payload: data)
                }
                
                // 继续下一次读取
                await self.readFromSocks()
            }
        }
    }
    
    /// 将从 SOCKS 收到的数据打包成 IP 包写回 VPN 隧道。
    private func writeToTunnel(payload: Data) {
        // 警告: 这是一个非常复杂的过程。
        // 正确实现需要:
        // 1. 维护 TCP 序列号 (Seq) 和确认号 (Ack)。
        // 2. 正确设置 TCP 标志位 (Flags)。
        // 3. 重新计算 IP 和 TCP 头的校验和。
        // 以下是一个极度简化的示例，仅用于演示流程。
        let ipHeader = createIPv4Header(payloadLength: payload.count)
        let tcpHeader = createTCPHeader(payload: payload)
        
        let finalPacket = ipHeader + tcpHeader + payload
        packetFlow.writePackets([finalPacket], withProtocols: [AF_INET as NSNumber])
    }
    
    // MARK: - Logging & Helpers

    private func log(_ message: String) {
        NSLog("[TCPConnection \(key)] \(message)")
    }
    
    // --- 以下辅助函数与原版保持一致 (简化版) ---
    
    private func createIPv4Header(payloadLength: Int) -> Data {
        // 这是一个非常简化的版本，省略了校验和计算
        var header = Data(count: 20)
        header[0] = 0x45 // Version 4, IHL 5
        let totalLength = 20 + 20 + payloadLength // IP Header + TCP Header + Payload
        header[2] = UInt8(totalLength >> 8)
        header[3] = UInt8(totalLength & 0xFF)
        header[8] = 64 // TTL
        header[9] = 6  // Protocol TCP
        
        // 地址 (注意：源和目标已交换)
        destIP.rawValue.copyBytes(to: &header[12], count: 4)
        sourceIP.rawValue.copyBytes(to: &header[16], count: 4)
        
        // TODO: 计算并填充IP校验和
        return header
    }

    private func createTCPHeader(payload: Data) -> Data {
        // 同样，这是一个非常简化的版本，省略了Seq/Ack管理和校验和
        var header = Data(count: 20)
        
        // 端口 (已交换)
        header[0] = UInt8(destPort >> 8)
        header[1] = UInt8(destPort & 0xFF)
        header[2] = UInt8(sourcePort >> 8)
        header[3] = UInt8(sourcePort & 0xFF)
        
        // TODO: 管理 TCP 序列号和确认号
        
        header[12] = 0x50 // Data Offset 5
        header[13] = 0x18 // Flags (ACK, PSH)
        
        // TODO: 计算并填充TCP校验和
        return header
    }
}

// 用于调试时打印 Data 内容
extension Data {
    func hexEncodedString() -> String {
        return map { String(format: "%02hhx", $0) }.joined()
    }
}
