//
//  SceneDelegate.swift
//  CoNETVPN1
//
//  Created by 杨旭的MacBook Pro on 2024/11/11.
//

import UIKit

class SceneDelegate: UIResponder, UIWindowSceneDelegate {

    var window: UIWindow?
    private var hasEnteredForegroundBefore = false


    func scene(_ scene: UIScene, willConnectTo session: UISceneSession, options connectionOptions: UIScene.ConnectionOptions) {
        
        
        
        
        // Use this method to optionally configure and attach the UIWindow `window` to the provided UIWindowScene `scene`.
        // If using a storyboard, the `window` property will automatically be initialized and attached to the scene.
        // This delegate does not imply the connecting scene or session are new (see `application:configurationForConnectingSceneSession` instead).
        guard let _ = (scene as? UIWindowScene) else { return }
    }

    func sceneDidDisconnect(_ scene: UIScene) {
        // Called as the scene is being released by the system.
        // This occurs shortly after the scene enters the background, or when its session is discarded.
        // Release any resources associated with this scene that can be re-created the next time the scene connects.
        // The scene may re-connect later, as its session was not necessarily discarded (see `application:didDiscardSceneSessions` instead).
        
        print("app将要失去app")
    }

    func sceneDidBecomeActive(_ scene: UIScene) {
        // Called when the scene has moved from an inactive state to an active state.
        // Use this method to restart any tasks that were paused (or not yet started) when the scene was inactive.
        print("app将要进入app")
        if hasEnteredForegroundBefore {
                    NotificationCenter.default.post(
                        name: Notification.Name("LocalServerStarted"),
                        object: nil,
                        userInfo: ["status": "running"]
                        
                    )
            
            print("app已经进入前台1")
                } else {
                    hasEnteredForegroundBefore = true
                    print("app已经进入前台2")
                }
    }

    func sceneWillResignActive(_ scene: UIScene) {
        
        print("app已经进入后台将要被挂起")
        // Called when the scene will move from an active state to an inactive state.
        // This may occur due to temporary interruptions (ex. an incoming phone call).
    }

    func sceneWillEnterForeground(_ scene: UIScene) {
        // Called as the scene transitions from the background to the foreground.
        // Use this method to undo the changes made on entering the background.
        print("app已经进入前台")

        if hasEnteredForegroundBefore {
                    NotificationCenter.default.post(
                        name: Notification.Name("LocalServerStarted"),
                        object: nil,
                        userInfo: ["status": "running"]
                        
                    )
            
            print("app已经进入前台1")
                } else {
                    hasEnteredForegroundBefore = true
                    print("app已经进入前台2")
                }
        
        
        
    }

    func sceneDidEnterBackground(_ scene: UIScene) {
        NotificationCenter.default.post(
            name: Notification.Name("LocalServerStarted1"),
            object: nil,
            userInfo: ["status": "running"]
            
        )
        print("app已经进入前台3333")
        // Called as the scene transitions from the foreground to the background.
        // Use this method to save data, release shared resources, and store enough scene-specific state information
        // to restore the scene back to its current state.
    }


}
