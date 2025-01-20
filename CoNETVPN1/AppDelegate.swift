//
//  AppDelegate.swift
//  CoNETVPN1
//
//  Created by 杨旭的MacBook Pro on 2024/11/11.
//

import UIKit
import AVFoundation
@main
class AppDelegate: UIResponder, UIApplicationDelegate {
    
    var window: UIWindow?
//    var localServer: LocalServer?
    
    

    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        // Override point for customization after application launch.
//        let parameters = LocalServer(port: 8888);
        // 初始化服务器，设置端口为8888
//        localServer = LocalServer(port: 8888)
        
        
        // 启动服务器
//               localServer?.start()
        
//        localServer?.stop() // 停止服务器
        
        return true
    }

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

