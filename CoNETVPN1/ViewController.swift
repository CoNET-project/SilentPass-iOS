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

class ViewController: UIViewController, WKNavigationDelegate, WKScriptMessageHandler {
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
    private var isStartingServer = false
    private var networkMonitor: NWPathMonitor?
    private var isLoadingContent = false // Prevent duplicate loads
    private var lastLoadTime: Date? // Track last load time
    // 缓存最近一次的 VPN 原始状态值（NEVPNStatus.rawValue after mapping）
    private var lastVPNStatusRaw: Int?
    
    // VPN observer token
    private static var vpnObserverToken: NSObjectProtocol?
    
    override func viewDidLoad() {
        super.viewDidLoad()
        
        // Set black background first
        self.view.backgroundColor = UIColor.black
        
        // Initialize LayerMinus and VPNManager
        self.layerMinus = LayerMinus(port: self.port)
        self.vPNManager = VPNManager(layerMinus: self.layerMinus)
        
        // Setup WebView
        setupWebView()
        
        // Show loading indicator
        SVProgressHUD.setDefaultStyle(.dark)
        SVProgressHUD.show(withStatus: "Loading...")
        
        // Setup NativeBridge
        self.nativeBridge = NativeBridge(webView: webView, viewController: self)
        
        // Setup network monitoring
        setupNetworkMonitoring()
        
        // Setup notifications
        setupNotifications()
        
        // Setup VPN status listener
        setupVPNStatusListener()
        
        // 将 VPN 状态提供给本地 Web 服务器，以支持 /iOSVPN 返回 { vpn: true/false }
        webServer.vpnStatusProvider = { [weak self] in
            guard let raw = self?.lastVPNStatusRaw else { return false }
                // 3: connected, 4: reasserting ；你代码里把 2(Connecting) 映射成 3
                return raw == 3 || raw == 4
            }
    }
    
    private func setupWebView() {
        let config = WKWebViewConfiguration()
        let userContentController = WKUserContentController()
        
        // Register custom scheme handler
        config.setURLSchemeHandler(schemeHandler, forURLScheme: "local-first")
        
        // Add message handlers - only add if NativeBridge doesn't add them
        userContentController.add(self, name: "rule")
        // Comment out webviewMessage if NativeBridge adds it
        // userContentController.add(self, name: "webviewMessage")
        userContentController.add(self, name: "nativeHandler")
        
        // Inject JavaScript to ensure handlers exist
        let jsSource = """
        // Ensure webkit.messageHandlers exists
        if (typeof window.webkit === 'undefined') {
            window.webkit = {};
        }
        if (typeof window.webkit.messageHandlers === 'undefined') {
            window.webkit.messageHandlers = {};
        }
        
        // Create fallback handler for rule if it doesn't exist
        if (!window.webkit.messageHandlers.rule) {
            window.webkit.messageHandlers.rule = {
                postMessage: function(message) {
                    console.log('Rule message:', message);
                    // Try to use webviewMessage if it exists (from NativeBridge)
                    if (window.webkit.messageHandlers.webviewMessage) {
                        window.webkit.messageHandlers.webviewMessage.postMessage({
                            type: 'rule',
                            data: message
                        });
                    } else if (window.webkit.messageHandlers.nativeHandler) {
                        // Fallback to nativeHandler
                        window.webkit.messageHandlers.nativeHandler.postMessage({
                            type: 'rule',
                            data: message
                        });
                    }
                }
            };
        }
        
        console.log('Message handlers initialized');
        """
        
        let userScript = WKUserScript(
            source: jsSource,
            injectionTime: .atDocumentStart,
            forMainFrameOnly: false
        )
        userContentController.addUserScript(userScript)
        
        config.userContentController = userContentController
        config.preferences.javaScriptEnabled = true
        config.preferences.javaScriptCanOpenWindowsAutomatically = true
        
        // Create WebView
        webView = WKWebView(frame: .zero, configuration: config)
        
        // Disable zoom
        webView.scrollView.bouncesZoom = false
        webView.scrollView.maximumZoomScale = 1.0
        webView.scrollView.minimumZoomScale = 1.0
        
        // Set navigation delegate
        webView.navigationDelegate = self
        
        // Add to view
        self.view.addSubview(webView)
        
        // Setup constraints
        webView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            webView.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor),
            webView.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            webView.leadingAnchor.constraint(equalTo: view.leadingAnchor),
            webView.trailingAnchor.constraint(equalTo: view.trailingAnchor)
        ])
        
        // Configure appearance
        webView.backgroundColor = UIColor.black
        webView.isOpaque = false
        webView.scrollView.backgroundColor = .black
        webView.scrollView.showsHorizontalScrollIndicator = false
        webView.scrollView.showsVerticalScrollIndicator = false
        webView.scrollView.bounces = false
        
        if #available(iOS 16.4, *) {
            webView.isInspectable = true
        }
    }
    
    // MARK: - WKScriptMessageHandler
    
    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        print("📨 Received message from JS: \(message.name)")
        
        switch message.name {
        case "rule":
            handleRuleMessage(message.body)
        case "webviewMessage":
            // This might be handled by NativeBridge
            handleWebViewMessage(message.body)
        case "nativeHandler":
            // Handle messages that would normally go to webviewMessage
            if let dict = message.body as? [String: Any],
               let type = dict["type"] as? String, type == "rule" {
                if let data = dict["data"] {
                    handleRuleMessage(data)
                }
            } else {
                handleNativeMessage(message.body)
            }
        default:
            print("Unknown message handler: \(message.name)")
        }
    }
    
    private func handleRuleMessage(_ body: Any) {
        print("📋 Rule message received: \(body)")
        // Handle rule messages here
    }
    
    private func handleWebViewMessage(_ body: Any) {
        print("📱 WebView message received: \(body)")
        if let dict = body as? [String: Any] {
            // Handle the message directly here or pass to NativeBridge if it has the right method
            // Check if it's a wrapped rule message
            if let type = dict["type"] as? String, type == "rule" {
                if let data = dict["data"] {
                    handleRuleMessage(data)
                }
            } else {
                // Handle other message types directly
                print("Processing WebView message: \(dict)")
                // You can add specific handling based on message type here
                // For example:
                if let action = dict["action"] as? String {
                    switch action {
                    case "vpnConnect":
                        // Handle VPN connect
                        print("VPN connect requested")
                    case "vpnDisconnect":
                        // Handle VPN disconnect
                        print("VPN disconnect requested")
                    default:
                        print("Unknown action: \(action)")
                    }
                }
            }
        }
    }
    
    private func handleNativeMessage(_ body: Any) {
        print("🔧 Native message received: \(body)")
    }
    
    private func setupNetworkMonitoring() {
        networkMonitor = NWPathMonitor()
        let queue = DispatchQueue.global(qos: .background)
        
        networkMonitor?.pathUpdateHandler = { [weak self] path in
            guard let self = self else { return }
            
            if path.status == .satisfied {
                print("✅ Network available")
                
                // Prevent concurrent server starts
                guard !self.isStartingServer else { return }
                self.isStartingServer = true
                
                Task { @MainActor in
                    await self.webServer.prepareAndStart()
                    self.isStartingServer = false
                }
            } else {
                print("⌛ Network unavailable")
            }
        }
        
        networkMonitor?.start(queue: queue)
    }
    
    private func setupNotifications() {
        // Web server started notification
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(handleWebServerStarted(_:)),
            name: .webServerDidStart,
            object: nil
        )
        
        // Local server notifications
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


        
        // VPN status notification
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(vpnStatusChanged(_:)),
            name: Notification.Name("VPNStatusChanged"),
            object: nil
        )

		// App 回到前台（focus）时，主动同步一次 VPN 状态给 WebView
		NotificationCenter.default.addObserver(
			self,
			selector: #selector(appDidBecomeActive(_:)),
			name: UIApplication.didBecomeActiveNotification,
			object: nil
		)
    }

		@objc private func appDidBecomeActive(_ note: Notification) {
			// 确保本地服务器在跑，避免 WebView 还没 ready
			if self.webServer.server.state != .running {
				Task { @MainActor in
					await self.webServer.prepareAndStart()
				}
			}
			// 主动查询并广播一次状态（触发 Sending JS）
			self.getVPNConfigurationStatus()
		}
    
    // MARK: - WebView Navigation Delegate
    
    func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
        print("📱 WebView started loading")
    }
    
    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        print("✅ WebView finished loading")
        isLoadingContent = false
        
        if !didPerformInitialLoad {
            didPerformInitialLoad = true
            SVProgressHUD.dismiss()
        }
        
        // Make WebView interactive
        webView.isUserInteractionEnabled = true
        webView.scrollView.isScrollEnabled = true
        
        // 冷启动或页面重载后，若已有缓存状态，则主动补发给前端（避免首包丢失）
        if let raw = lastVPNStatusRaw {
            let responseDict: [String: Any] = [
                "event": "native_VPNStatus",
                "data": ["VPNStatus": raw],
                "callbackId": "VPNStatusUpdate"
            ]
            if let responseData = try? JSONSerialization.data(withJSONObject: responseDict),
               let responseString = String(data: responseData, encoding: .utf8) {
                self.sendToWebView(responseString: responseString)
            }
        }
    }
    
    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        let nsErr = error as NSError
        
        // Log error details
        print("❌ 网页加载失败 (Provisional Navigation): \(error.localizedDescription)")
        
        // Handle cancelled error specially
        if nsErr.code == NSURLErrorCancelled {
            print("⚠️ 错误代码: .cancelled - 加载被取消。")
            isLoadingContent = false
            return // Don't retry for cancelled requests
        }
        
        // Handle network errors with retry
        if nsErr.domain == NSURLErrorDomain &&
           (nsErr.code == NSURLErrorTimedOut ||
            nsErr.code == NSURLErrorCannotFindHost ||
            nsErr.code == NSURLErrorCannotConnectToHost) {
            
            isLoadingContent = false
            
            // Allow retry
            self.didPerformInitialLoad = false
            
            // Retry after delay
            DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
                guard let self = self else { return }
                
                if self.webServer.server.state == .running {
                    self.loadInitialContent()
                } else {
                    // Try to start server again
                    Task { @MainActor in
                        await self.webServer.prepareAndStart()
                    }
                }
            }
        } else {
            isLoadingContent = false
        }
    }
    
    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        print("❌ WebView navigation failed: \(error.localizedDescription)")
        isLoadingContent = false
    }
    
    // MARK: - Server Management
    
    @objc private func handleWebServerStarted(_ notification: Notification) {
        print("📢 Web server started notification received")
        
        getVPNConfigurationStatus()
        
        switch self.webServer.server.state {
        case .starting:
            print("Server is starting...")
        case .running:
            print("✅ Server is running")
            
            // Only load if not already loaded and not currently loading
            guard !self.didPerformInitialLoad && !self.isLoadingContent else {
                if self.didPerformInitialLoad {
                    print("Already performed initial load, skipping")
                }
                if self.isLoadingContent {
                    print("Already loading content, skipping")
                }
                return
            }
            
            // Load initial content
            DispatchQueue.main.async { [weak self] in
                self?.loadInitialContent()
            }
            
        case .stopping:
            print("Server is stopping, will restart")
            Task { @MainActor in
                await self.webServer.prepareAndStart()
            }
            
        case .stopped:
            print("Server is stopped, will restart")
            Task { @MainActor in
                await self.webServer.prepareAndStart()
            }
            
        default:
            break
        }
    }
    
    private func loadInitialContent() {
        // Prevent duplicate loads
        guard !isLoadingContent else {
            print("⚠️ Already loading content, skipping duplicate load")
            return
        }
        
        // Check if we just loaded recently (within 2 seconds)
        if let lastLoad = lastLoadTime, Date().timeIntervalSince(lastLoad) < 2.0 {
            print("⚠️ Recently loaded content, skipping duplicate load")
            return
        }
        
        // Cancel any pending loads
        if webView.isLoading {
            webView.stopLoading()
            // Wait a bit before loading
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) { [weak self] in
                self?.performLoad()
            }
        } else {
            performLoad()
        }
    }
    
    private func performLoad() {
        // Set loading flag
        isLoadingContent = true
        lastLoadTime = Date()
        
        // Try to load the main page
        if let url = URL(string: "local-first://localhost:3001/index.html") {
            let request = URLRequest(
                url: url,
                cachePolicy: .reloadIgnoringLocalAndRemoteCacheData,
                timeoutInterval: 10.0
            )
            print("📱 Loading initial content: \(url.absoluteString)")
            self.webView.load(request)
        } else {
            isLoadingContent = false
        }
    }
    
    @objc func handleServerStarted(_ notification: Notification) {
        getVPNConfigurationStatus()
        
        if let status = notification.userInfo?["status"] as? String {
            print("Server status received: \(status)")
            
            // Don't reload if we're coming from background
            if status == "running" && didPerformInitialLoad {
                print("App returning from background, not reloading")
                return
            }
            
            switch self.webServer.server.state {
            case .starting:
                print("Local server starting...")
            case .running:
                print("Local server running")
            case .stopping:
                print("Local server stopping, will restart")
                Task { @MainActor in
                    await self.webServer.prepareAndStart()
                }
            case .stopped:
                print("Local server stopped, will restart")
                Task { @MainActor in
                    await self.webServer.prepareAndStart()
                }
            default:
                break
            }
        }
    }
    
    @objc func handleServerStarted1(_ notification: Notification) {
        // Handle server shutdown notification
        print("Server shutdown notification received")
    }
    
    // MARK: - VPN Management
    
    func setupVPNStatusListener() {
        // Only register once
        guard Self.vpnObserverToken == nil else { return }
        
        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in
            guard let self = self,
                  error == nil,
                  let managers = managers,
                  let manager = managers.first(where: { $0.localizedDescription == "CoNET VPN" })
            else { return }
            
            // Handle initial status
            self.handleVPNStatus(manager.connection.status)
            
            // Register for status changes
            Self.vpnObserverToken = NotificationCenter.default.addObserver(
                forName: .NEVPNStatusDidChange,
                object: nil,
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
        // Skip status 2 (connecting)
        if status.rawValue == 2 {
            return print("VPN status change: \(status.rawValue) - not sending to JS")
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
                    
                    print("Current VPN status: \(sendStatus)")
                    
                    // 更新缓存，供 /iOSVPN 与 WebView 首次 didFinish 补发使用
                    self.lastVPNStatusRaw = sendStatus
                    NotificationCenter.default.post(
                        name: Notification.Name("VPNStatusChanged"),
                        object: sendStatus
                    )
                    break
                }
            }
        }
    }
    
	@objc func vpnStatusChanged(_ notification: Notification) {
		var raw: Int?
		if let status = notification.object as? NEVPNStatus {
			raw = status.rawValue
		} else if let rawInt = notification.object as? Int {
			raw = rawInt
		}
		guard let vpnRaw = raw else { return }
        
        // 任何来源的状态变化都刷新缓存，供 /iOSVPN 读取
        self.lastVPNStatusRaw = vpnRaw

		DispatchQueue.main.async {
			let responseDict: [String: Any] = [
				"event": "native_VPNStatus",
				"data": ["VPNStatus": vpnRaw],
				"callbackId": "VPNStatusUpdate"
			]
			if let responseData = try? JSONSerialization.data(withJSONObject: responseDict),
				let responseString = String(data: responseData, encoding: .utf8) {
				self.sendToWebView(responseString: responseString)
			}
		}
	}
    
    private func sendToWebView(responseString: String) {

		// WebView 未就绪时做一次性重试
		guard webView != nil, !webView.isLoading else {
			print("⚠️ WebView not ready, will retry in 1s")
			DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
				self?.sendToWebView(responseString: responseString)
			}
			return
		}
        
        // Escape the JSON string properly
        let escapedString = responseString
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
            .replacingOccurrences(of: "\n", with: "\\n")
            .replacingOccurrences(of: "\r", with: "\\r")
        
        let js = """
        (function() {
            try {
                window.dispatchEvent(new MessageEvent('message', { data: '\(escapedString)' }));
                return 'success';
            } catch (e) {
                return 'error: ' + e.message;
            }
        })();
        """
        
        print("Sending JS: \(js)")
        
        self.webView?.evaluateJavaScript(js) { result, error in
            if let error = error {
                print("❌ JS execution failed: \(error)")
            } else {
                print("✅ JS execution success: \(String(describing: result))")
            }
        }
    }
    
    // MARK: - Cleanup
    
    deinit {
        networkMonitor?.cancel()
        NotificationCenter.default.removeObserver(self)
    }
}

// MARK: - Response Data Structure

struct ResponseData: Codable {
    let region: [String]
    let nodes: Int
    let nearbyRegion: String
    let account: String
}
