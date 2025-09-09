//
//  AppDelegate.swift
//  CoNETVPN1
//
//  Created by 杨旭的MacBook Pro on 2024/11/11.
//

import UIKit
import AVFoundation
import Firebase
import SwiftyStoreKit
import SVProgressHUD

@main
class AppDelegate: UIResponder, UIApplicationDelegate {
    
    var window: UIWindow?
//    var localServer: LocalServer?
    
    var webServer = LocalWebServer()


    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        // Override point for customization after application launch.
//        let parameters = LocalServer(port: 8888);
        // 初始化服务器，设置端口为8888
//        localServer = LocalServer(port: 8888)
        // 显示 loading 动画
        
        // 启动本地服务器是一个异步任务，所以在一个 Task 中执行
//        Task {
//            await self.webServer.prepareAndStart()
//        }
                
        FirebaseApp.configure()
        // 启动服务器
//               localServer?.start()
        
//        localServer?.stop() // 停止服务器
        SwiftyStoreKit.completeTransactions { purchases in
                for purchase in purchases {
                    switch purchase.transaction.transactionState {
                    case .purchased, .restored:
//                        print("购买成功: \(purchase.productId)")
                        SwiftyStoreKit.finishTransaction(purchase.transaction)
                    default:
                        break
                    }
                }
            }
        
        
//        NotificationCenter.default.addObserver(
//                self,
//                selector: #selector(handleServerStarted(_:)),
//                name: Notification.Name("LocalServerStarted"),
//                object: nil
//            )
        return true
    }
//    @objc func handleServerStarted(_ notification: Notification) {
//        if let status = notification.userInfo?["status"] as? String {
//            print("接收到服务器状态: \(status)")
//            // 更新UI或执行其他操作
//            if self.webServer.server.state == .starting {
//                // 服务器正在启动中的处理逻辑、
////                self.webServer.stop()
//                print("本地服务器启动中")
//            }
//            else if self.webServer.server.state == .running {
//                // 服务器正在启动中的处理逻辑、
//                print("本地服务器运行中")
//            }
//            else if self.webServer.server.state == .stopping {
//                // 服务器正在启动中的处理逻辑、
//                print("本地服务器停止中")
//            }
//            else if self.webServer.server.state == .stopped {
//                // 服务器正在启动中的处理逻辑、
//
//
//                print("本地服务器已停止")
//                Task {
//                    await self.webServer.prepareAndStart()
//                }
//
//
//            }
//
//
//
//        }
//    }

    // MARK: UISceneSession Lifecycle

    func application(_ application: UIApplication, configurationForConnecting connectingSceneSession: UISceneSession, options: UIScene.ConnectionOptions) -> UISceneConfiguration {
        // Called when a new scene session is being created.
        // Use this method to select a configuration to create the new scene with.
        return UISceneConfiguration(name: "Default Configuration", sessionRole: connectingSceneSession.role)
    }

    func application(_ application: UIApplication, didDiscardSceneSessions sceneSessions: Set<UISceneSession>) {
        // Called when the user discards a scene session.
        // If any sessions were discarded while the application was not running, this will be called shortly after application:didFinishLaunchingWithOptions.
        // Use this method to release any resources that were specific to the discarded scenes, as they will not return.
    }
    func applicationWillEnterForeground(_ application: UIApplication) {
        
       
        }

    func applicationDidEnterBackground(_ application: UIApplication) {
            startBackgroundAudio()
        }

        func startBackgroundAudio() {
            let audioSession = AVAudioSession.sharedInstance()
            do {
                try audioSession.setCategory(.playback, mode: .default)
                try audioSession.setActive(true)
                playSilentAudio()
            } catch {
                print("Failed to set up audio session: \(error.localizedDescription)")
            }
        }
        
        func playSilentAudio() {
            guard let url = Bundle.main.url(forResource: "silent", withExtension: "mp3") else {
                print("Silent audio file not found")
                return
            }

            do {
                let audioPlayer = try AVAudioPlayer(contentsOf: url)
                audioPlayer.numberOfLoops = -1 // 循环播放无声音频
                audioPlayer.play()
                print("Silent audio is playing...")
            } catch {
                print("Failed to play silent audio: \(error.localizedDescription)")
            }
        }
}
