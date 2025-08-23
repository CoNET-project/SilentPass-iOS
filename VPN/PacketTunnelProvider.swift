//  PacketTunnelProvider.swift
//  vpn-tunnel
//
//  Created by peter xie on 2021-10-18.
//

import NetworkExtension
import os.log
import vpn2socks

class PacketTunnelProvider: vpn2socks.PacketTunnelProvider {
    private var socksServer: Server?
    let port = 8888
    var server: Server!

    override init() {
        super.init()
        let s = Server(port: 8888)
        self.socksServer = s
        do {
            try s.start()
            NSLog("PacketTunnelProvider SOCKS server started.")
        } catch {
            NSLog("PacketTunnelProvider Failed to start SOCKS server: \(error)")
        }
    }

    override func startTunnel(options: [String : NSObject]?, completionHandler: @escaping (Error?) -> Void) {
        
        super.startTunnel(options: options) { error in
            // 5. 在核心逻辑完成后，你可以执行后续的自定义操作
            if let error = error {
                NSLog("PacketTunnelProvider Target: Core logic failed. Cleaning up.")
                // 处理错误
            } else {
                NSLog("PacketTunnelProvider Target: Core logic succeeded. Tunnel is up.")
                guard let options = options else {
                    completionHandler(NSError(domain: "NEPacketTunnelProviderError", code: -1,
                                              userInfo: [NSLocalizedDescriptionKey: "No options provided"]))
                    return
                }
                let entryNodesStr = options["entryNodes"] as? String ?? ""
                let egressNodesStr = options["egressNodes"] as? String ?? ""
                let privateKey = options["privateKey"] as? String ?? ""

                do {
                    try self.socksServer?.start()
                    NSLog("PacketTunnelProvider SOCKS server started.")
                } catch {
                    NSLog("Failed to start SOCKS server: \(error)")
                }
                
                // 最后，调用 completionHandler 通知系统
                completionHandler(error)
                
            }
            
            
        }
        
    }


    override func stopTunnel(with reason: NEProviderStopReason, completionHandler: @escaping () -> Void) {
        NSLog("🛑 PacketTunnelProvider.stopTunnel called, reason: \(reason.rawValue)")
        socksServer?.stop()
        socksServer = nil
        super.stopTunnel(with: reason) {
            NSLog("PacketTunnelProvider: Core tunnel stopped. Finalizing cleanup.")
            // 核心隧道停止后的最终清理
            completionHandler()
        }
    }

    override func handleAppMessage(_ messageData: Data, completionHandler: ((Data?) -> Void)?) {
        NSLog("📩 PacketTunnelProvider.handleAppMessage called")
        completionHandler?(messageData)
    }

    override func sleep(completionHandler: @escaping () -> Void) {
        NSLog("💤 PacketTunnelProvider.sleep called")
        completionHandler()
    }

    override func wake() {
        NSLog("🔔 PacketTunnelProvider.wake called")
    }
}
