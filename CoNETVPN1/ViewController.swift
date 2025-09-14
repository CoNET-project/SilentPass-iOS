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
	private var didPerformInitialLoad = false
	private let schemeHandler = LocalServerFirstSchemeHandler()

	private var isStartingServer = false // ➕ 新增：去抖
    
    // 1. 在 ViewController 里加一个静态 token，保证它不会跟随 VC 释放
    private static var vpnObserverToken: NSObjectProtocol?
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
        config.setURLSchemeHandler(schemeHandler, forURLScheme: "local-first")
        
        
        
        
        
        
        
        config.userContentController = userContentController
        config.preferences.javaScriptEnabled = true
        config.preferences.javaScriptCanOpenWindowsAutomatically = true

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
        
//        //      设置捕获JavaScript错误
       webView.navigationDelegate = self
        
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
        
       
        let monitor = NWPathMonitor()
        let queue = DispatchQueue.global(qos: .background)
        monitor.start(queue: queue)

        monitor.pathUpdateHandler = { [weak self] path in
            guard let self = self else { return }   // 或：guard let self else { return }
            if path.status == .satisfied {
                print("输出✅ 网络可用")
              
                // ➕ 去抖：避免并发 prepareAndStart
                guard !self.isStartingServer else { return }
                self.isStartingServer = true
                Task {
                    await self.webServer.prepareAndStart()
                    self.isStartingServer = false
                }
                
                
            } else {
                print("输出❌ 网络不可用")
            }
        }
        
        
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
        
        
        
        
        setupVPNStatusListener()
        
        
        // 监听 VPN 状态变化
            NotificationCenter.default.addObserver(
                self,
                selector: #selector(vpnStatusChanged(_:)),
                name: Notification.Name("VPNStatusChanged"),
                object: nil
            )
        
        
        // 监听应用即将被终止通知
//               NotificationCenter.default.addObserver(
//                   self,
//                   selector: #selector(handleAppWillTerminate),
//                   name: UIApplication.willTerminateNotification,
//                   object: nil
//               )
        
    }
    // 2. 把 setupVPNStatusListener 改成「一次性」注册
    func setupVPNStatusListener() {
        // 如果已经注册过就直接返回
        guard Self.vpnObserverToken == nil else { return }

        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
            guard let self = self,
                  error == nil,
                  let managers = managers,
                  let manager = managers.first(where: { $0.localizedDescription == "CoNET VPN" })
            else { return }

            // 初始状态打印一次
            self.handleVPNStatus(manager.connection.status)

            // 用全局 token 挂通知，object 传 nil 保证任何 connection 都能收到
            Self.vpnObserverToken = NotificationCenter.default.addObserver(
                forName: .NEVPNStatusDidChange,
                object: nil,               // ⚠️ 传 nil，避免 connection 对象不匹配
                queue: .main
            ) { note in
                guard let conn = note.object as? NEVPNConnection else { return }
                self.handleVPNStatus(conn.status)
            }
        }
    }

    @objc func vpnStatusDidChange(_ notification: Notification) {
        if let connection = notification.object as? NEVPNConnection {
            handleVPNStatus(connection.status)
        }
    }

    private func handleVPNStatus(_ status: NEVPNStatus) {
        // 这里处理你的状态回传逻辑，比如给 H5 发
        if status.rawValue == 2 {
            return print("VPN 状态变化：\(status.rawValue) 不发送 JS")
        }
        NotificationCenter.default.post(
            name: Notification.Name("VPNStatusChanged"),
            object: status
        )

        
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
                if manager.localizedDescription == "CoNET VPN" {
                    let status = manager.connection.status
                    var sendStatus = status.rawValue
                    if status.rawValue == 2 {
                        sendStatus = 3
                    }
                    // 把状态通知给 H5
                    print("当前 VPN 状态 为 : \(sendStatus) 发送状态！")
                    NotificationCenter.default.post(
                        name: Notification.Name("VPNStatusChanged"),
                        object: sendStatus
                    )
                    break
                }
            }
        }
    }
    
    @objc func handleServerStarted1(_ notification: Notification)   {
        
        // self.webServer.server.stop()
        
    }
    @objc func handleServerStarted(_ notification: Notification)   {
        
        getVPNConfigurationStatus()
        
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


	// ✅ 兜底：如果是本地协议的超时/网络错误，延迟自动重试一次
    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
       let nsErr = error as NSError
        guard nsErr.domain == NSURLErrorDomain else { return }
        if nsErr.code == NSURLErrorTimedOut
            || nsErr.code == NSURLErrorCannotFindHost
            || nsErr.code == NSURLErrorCannotConnectToHost {

            // 关键：失败时允许再次触发首导航
            self.didPerformInitialLoad = false

            DispatchQueue.main.asyncAfter(deadline: .now() + 0.5) {
                if self.webServer.server.state == .running {
                    // 用和首导航一致的 URL
                    if let url = URL(string: "local-first://localhost:3001/index.html") {
                        self.webView.load(URLRequest(url: url, cachePolicy: .reloadIgnoringLocalAndRemoteCacheData, timeoutInterval: 5))
                    }
                }
            }
        }
    }


    @objc private func handleWebServerStarted(_ notification: Notification)  {
       
		getVPNConfigurationStatus()

		switch self.webServer.server.state {
		case .starting:
			print("本地服务器启动中")
		case .running:
			guard !self.didPerformInitialLoad else {
				print("已完成首导航，忽略重复 load")
				return
			}
			DispatchQueue.main.async {
				guard let url = URL(string: "local-first://localhost:3001") else { return }
				self.webView.load(URLRequest(url: url))
			}
		case .stopping:
			Task { await self.webServer.prepareAndStart() }
		case .stopped:
			print("本地服务器已停止")
			Task { await self.webServer.prepareAndStart() }
		default:
			break
		}
        
    }

	// ✅ 首次加载成功后再置位，防止“提前置位后失败不再尝试”的黑屏
    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        if !didPerformInitialLoad { didPerformInitialLoad = true }
    }

    
    @objc func vpnStatusChanged(_ notification: Notification) {
        
        if let status = notification.object as? NEVPNStatus {
            DispatchQueue.main.async{
                let responseDict: [String: Any] = [
                    "event": "native_VPNStatus",
                    "data": ["VPNStatus": status.rawValue],
                    "callbackId": "杨旭发给老杨VPN状态"
                ]
                // 转换为 JSON 字符串并发送回 H5
                if let responseData = try? JSONSerialization.data(withJSONObject: responseDict),
                   let responseString = String(data: responseData, encoding: .utf8) {
                    DispatchQueue.main.async {
                        
//                        let alert = UIAlertController(
//                            title: "\(status.rawValue)",
//                            message: "",
//                            preferredStyle: .alert
//                        )
//                        alert.addAction(UIAlertAction(title: "YES", style: .default, handler: { _ in
//                        }))
//
//                        self.present(alert, animated: true, completion: nil)
                        
                        
                        
                        self.sendToWebView(responseString: responseString)
                    }
                }
            }
        }

        
//        if let status = notification.object as? NEVPNStatus {
//            switch status {
//            case .connected:
//                print("VPN 已连接")
//
//
//
//            case .connecting:
//                print("VPN 正在连接")
//            case .disconnected:
//                print("VPN 已断开")
//            default:
//                print("VPN 状态变化: \(status.rawValue)")
//            }
//        }
    }
    // 封装发送 JavaScript 消息的方法
    private func sendToWebView(responseString: String) {
        let js = """
        window.dispatchEvent(new MessageEvent('message', { data: '\(responseString)' }));
        """
        print("发送的 js 是：\(js)")
        self.webView?.evaluateJavaScript(js, completionHandler: { result, error in
            if let error = error {
                print("✅ JS 执行失败: \(error)")
            } else {
                print("✅ JS 执行成功，返回: \(String(describing: result))")
            }
        })
    }
    
    struct ResponseData: Codable {
        let region: [String]
        let nodes: Int
        let nearbyRegion: String
        let account: String
    }
    
    
    
    

    

    deinit {
            // 确保在视图控制器被释放时取消定时器
//            timer?.invalidate()
        NotificationCenter.default.removeObserver(self)
        }
}
