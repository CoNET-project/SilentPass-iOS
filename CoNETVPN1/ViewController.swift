//
//  ViewController.swift
//  CoNETVPN1
//
//  Created by 杨旭的MacBook Pro on 2024/11/11.
//

import UIKit
import Network
import NetworkExtension
import SVProgressHUD
import WebKit
import AVFoundation
import MBProgressHUD
import GCDWebServer
import ZIPFoundation
import Swifter

class ViewController: UIViewController, WKNavigationDelegate {
    var webView: WKWebView!
    var localServer: Server?
    var timer: Timer?
    var egressNodes: [String] = []
    var entryNodes: [String] = []
    var privateKey = ""
    let getRegionButton = UIButton(type:.system)
    let getnodeButton = UIButton(type:.system)
    var backgroundTask: UIBackgroundTaskIdentifier = .invalid
    var layerMinus: LayerMinus!
    var nativeBridge: NativeBridge!
    var hud: MBProgressHUD?
    var vPNManager: VPNManager!
    var port: Int = 8888
    var webServer = LocalWebServer()
    override func viewDidLoad() {
        super.viewDidLoad()
        // 创建按钮
        
        let config22 = WKWebViewConfiguration()
//作用触发中国手机 链接网络
        let  webView1 = WKWebView(frame: .zero, configuration: config22)
        if let url1 = URL(string: "https://www.baidu.com") {
            let request1 = URLRequest(url: url1)
            webView1.load(request1)
        }
        
        
        self.view.backgroundColor = UIColor.black
        
        
        
        
        //git第一次提交
        


        self.layerMinus = LayerMinus(port: self.port)

        self.vPNManager = VPNManager(layerMinus: self.layerMinus)
        
        
   
        
        let config = WKWebViewConfiguration()
        
        
//        config.setValue(true, forKey: "allowUniversalAccessFromFileURLs")
//        config.limitsNavigationsToAppBoundDomains = true
        let userContentController = WKUserContentController()
        let schemeHandler = LocalServerFirstSchemeHandler()
        config.setURLSchemeHandler(schemeHandler, forURLScheme: "local-first")
        
        
        
        
        
        
        
        config.userContentController = userContentController
        config.preferences.javaScriptEnabled = true
        config.preferences.javaScriptCanOpenWindowsAutomatically = true
        config.suppressesIncrementalRendering = true // 是否支持记忆读取

        // ❗️以下是私有 API，不推荐用于生产，调试可用
//        config.preferences.setValue(true, forKey: "allowFileAccessFromFileURLs")
//        if #available(iOS 10.0, *) {
//            config.setValue(true, forKey: "allowUniversalAccessFromFileURLs")
//        }
        
        SVProgressHUD.setDefaultStyle(.dark)
        SVProgressHUD.show(withStatus: "Loding......")
        
        
       
               webView = WKWebView(frame: .zero, configuration: config)
        
        // 禁用缩放功能
            webView.scrollView.bouncesZoom = false // 禁用缩放回弹效果
            webView.scrollView.maximumZoomScale = 1.0 // 最大缩放比例设为 1（不缩放）
            webView.scrollView.minimumZoomScale = 1.0 // 最小缩放比例设为 1（不缩放）
//            webView.allowsMagnification = false // 禁止用户手势缩放（iOS 11 后有效）

               // 设置 WebView 的全屏布局
               webView.translatesAutoresizingMaskIntoConstraints = false
        self.view.addSubview(webView)
//        webView.frame = CGRect(x: 0, y: 0, width: self.view.frame.size.width, height: self.view.frame.size.height)
        
        
        
        NSLayoutConstraint.activate([
            webView.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor), // ✅ 关键修改
            webView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            webView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            webView.trailingAnchor.constraint(equalTo: view.trailingAnchor)
        ])
//        webView.scrollView.contentInsetAdjustmentBehavior = .never
        webView.backgroundColor = UIColor.black
        webView.isOpaque = false
        webView.backgroundColor = .black
        webView.scrollView.backgroundColor = .black
         webView.scrollView.showsHorizontalScrollIndicator = false
         webView.scrollView.showsVerticalScrollIndicator = false
         webView.scrollView.bounces = false
            if #available(iOS 16.4, *) {
                webView.isInspectable = true
            } else {
                // Fallback on earlier versions
            };

        
//        let js = """
//        if (typeof window.webkit === 'undefined') {
//            window.webkit = {};
//        }
//        if (typeof window.webkit.messageHandlers === 'undefined') {
//            window.webkit.messageHandlers = {};
//        }
//        window.webkit.messageHandlers.webviewMessage = {
//            postMessage: function(message) {
//                window.location.href = 'nativebridge://webviewMessage?' + encodeURIComponent(JSON.stringify(message));
//            }
//        };
//        """
//        let userScript = WKUserScript(source: js, injectionTime: .atDocumentStart, forMainFrameOnly: false)
//        webView.configuration.userContentController.addUserScript(userScript)
        
        // 加载网址
        self.nativeBridge = NativeBridge(webView: webView, viewController: self)
        
//        DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
//            if let url = URL(string: Constants.baseURL) {
//                let request = URLRequest(url: url)
//                self.webView.load(request)
//            }
//        }
 
//        let monitor = NWPathMonitor()
//        
//        // 开始监听网络状态
//                monitor.pathUpdateHandler = { path in
//                    if path.status == .satisfied {
//                        DispatchQueue.main.async {
//                            monitor.cancel()
//                            print("网络已恢复，重新加载 WebView")
//                            
//                            
//                            DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
//                                if let url = URL(string: Constants.baseURL) {
//                                    let request = URLRequest(url: url)
//                                    self.webView.load(request)
//                                }
//                            }
//
//                            
//                        }
//                    }
//                }
        
        
        
//        let queue = DispatchQueue.global(qos: .background)
//                monitor.start(queue: queue)
//        DispatchQueue.main.async{
//
//            
//        }
        
        
        
        NotificationCenter.default.addObserver(
            forName: Notification.Name("VPNStatusChanged"),
            object: nil,
            queue: .main
        ) { notification in
            if let status = notification.object as? NEVPNStatus {
                
                let state = String(status.rawValue)
                
                let jsFunction = "appVpnState('\(state)');"
                print(jsFunction)
                if (self.webView != nil)
                {
                    
                  self.nativeBridge.callJavaScriptFunction(functionName: "appVpnState", arguments: state) { result in
                       if let result = result as? Int {
                               print("JavaScript result: \(result)") // 输出 8
                            } else {
                                print("Failed to get a valid result")
                           }
                    }

                }
                
                
                
            }
        }
        
        timer = Timer.scheduledTimer(timeInterval: 2.0, target: self, selector: #selector(getVPNConfigurationStatus), userInfo: nil, repeats: true)
        
        NotificationCenter.default.addObserver(self,
                                                   selector: #selector(handleWebServerStarted(_:)),
                                                   name: .webServerDidStart,
                                                   object: nil)
        
        NotificationCenter.default.addObserver(
                self,
                selector: #selector(handleServerStarted(_:)),
                name: Notification.Name("LocalServerStarted"),
                object: nil
            )
        
        NotificationCenter.default.addObserver(
                self,
                selector: #selector(handleServerStarted1(_:)),
                name: Notification.Name("LocalServerStarted1"),
                object: nil
            )
        
        
        // 启动本地服务器是一个异步任务，所以在一个 Task 中执行
        Task {
            await self.webServer.prepareAndStart()
        }
        
        // 监听应用即将被终止通知
//               NotificationCenter.default.addObserver(
//                   self,
//                   selector: #selector(handleAppWillTerminate),
//                   name: UIApplication.willTerminateNotification,
//                   object: nil
//               )
        
    }
    @objc func handleServerStarted1(_ notification: Notification)   {
        
        self.webServer.server.stop()
        
    }
    @objc func handleServerStarted(_ notification: Notification)   {
        if let status = notification.userInfo?["status"] as? String {
            print("接收到服务器状态: \(status)")
            // 更新UI或执行其他操作
            if self.webServer.server.state == .starting {
                // 服务器正在启动中的处理逻辑、
//                self.webServer.stop()
                print("本地服务器启动中")
            }
            else if self.webServer.server.state == .running {
                // 服务器正在启动中的处理逻辑、
                print("本地服务器运行中")
            }
            else if self.webServer.server.state == .stopping {
                // 服务器正在启动中的处理逻辑、
                print("本地服务器停止中")
                Task {
                    await self.webServer.prepareAndStart()
                }
            }
            else if self.webServer.server.state == .stopped {
                // 服务器正在启动中的处理逻辑、
//                SVProgressHUD.show(withStatus: "服务器正在重新启动启动")
//                SVProgressHUD.dismiss(withDelay: 3)
//                await self.webServer.stop()
                print("本地服务器已停止")
                Task {
                    await self.webServer.prepareAndStart()
                }
                
                
            }
            
            
            
        }
    }
    @objc private func handleWebServerStarted(_ notification: Notification)  {
       
//        SVProgressHUD.show(withStatus: "开始加载本地html")
//         SVProgressHUD.dismiss(withDelay: 3)
        
        if self.webServer.server.state == .starting {
         
        }
        else if self.webServer.server.state == .running {
            
            DispatchQueue.main.async {
                
                
                guard let url_cache = URL(string: "local-first://localhost:3001") else { return }
                    self.webView.load(URLRequest(url: url_cache))
            }
               
     
        }
        else if self.webServer.server.state == .stopping {
            Task {
                await self.webServer.prepareAndStart()
            }
        }
        else if self.webServer.server.state == .stopped {
     
            Task {
                await self.webServer.prepareAndStart()
            }
            
        }
        
        
         
            
        
        return
        let monitor = NWPathMonitor()
        let queue = DispatchQueue(label: "NetworkMonitor")
        monitor.start(queue: queue)
        
        
        let currentPath = monitor.currentPath
        if currentPath.status == .satisfied {
            DispatchQueue.main.async {
                monitor.cancel()
                guard let url_cache = URL(string: "local-first://localhost:3001") else { return }
                
                
                    self.webView.load(URLRequest(url: url_cache))
                
            }
        } else {
            monitor.pathUpdateHandler = { [weak self] path in
                if path.status == .satisfied {
                    DispatchQueue.main.async {
                        monitor.cancel()
                        guard let url_cache = URL(string: "local-first://localhost:3001") else { return }
                        self?.webView.load(URLRequest(url: url_cache))
                        
                    }
                }
            }
        }
        
    }
   
    
    struct ResponseData: Codable {
        let region: [String]
        let nodes: Int
        let nearbyRegion: String
        let account: String
    }
    
    
    
    
    
    @objc func getVPNConfigurationStatus() {
        NETunnelProviderManager.loadAllFromPreferences { managers, error in
            if let error = error {
                print("Failed to load VPN configurations: \(error.localizedDescription)")
                return
            }

            guard let managers = managers else {
                print("No VPN configurations found")
                return
            }

            for manager in managers {
//                print("VPN configuration: \(manager.localizedDescription ?? "Unknown")")
//                print("Status: \(manager.connection.status)")
                if manager.localizedDescription == "CoNET VPN"
                {
                   
                        NotificationCenter.default.post(
                            name: Notification.Name("VPNStatusChanged"),
                            object: manager.connection.status
                        )
                    
                    
                    
                }
                
                
                
            }
        }
    }
    

    deinit {
            // 确保在视图控制器被释放时取消定时器
//            timer?.invalidate()
        NotificationCenter.default.removeObserver(self)
        }
}

