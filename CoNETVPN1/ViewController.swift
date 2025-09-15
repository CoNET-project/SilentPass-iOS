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
    private var isStartingServer = false
    private var networkMonitor: NWPathMonitor?
    
    // 1. 在 ViewController 里加一个静态 token，保证它不会跟随 VC 释放
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
    }
    
    private func setupWebView() {
        let config = WKWebViewConfiguration()
        let userContentController = WKUserContentController()
        
        // Register custom scheme handler
        config.setURLSchemeHandler(schemeHandler, forURLScheme: "local-first")
        
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
    }
    
    // MARK: - WebView Navigation Delegate
    
    func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
        print("📱 WebView started loading")
    }
    
    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        print("✅ WebView finished loading")
        if !didPerformInitialLoad {
            didPerformInitialLoad = true
            SVProgressHUD.dismiss()
        }
    }
    
    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        print("❌ WebView failed to load: \(error.localizedDescription)")
        
        let nsErr = error as NSError
        
        // Handle network errors with retry
        if nsErr.domain == NSURLErrorDomain &&
           (nsErr.code == NSURLErrorTimedOut ||
            nsErr.code == NSURLErrorCannotFindHost ||
            nsErr.code == NSURLErrorCannotConnectToHost) {
            
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
        }
    }
    
    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        print("❌ WebView navigation failed: \(error.localizedDescription)")
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
            
            // Only load if not already loaded
            guard !self.didPerformInitialLoad else {
                print("Already performed initial load, skipping")
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
        // Try to load the main page
        if let url = URL(string: "local-first://localhost:3001/index.html") {
            let request = URLRequest(
                url: url,
                cachePolicy: .reloadIgnoringLocalAndRemoteCacheData,
                timeoutInterval: 10.0
            )
            print("📱 Loading initial content: \(url.absoluteString)")
            self.webView.load(request)
        }
    }
    
    @objc func handleServerStarted(_ notification: Notification) {
        getVPNConfigurationStatus()
        
        if let status = notification.userInfo?["status"] as? String {
            print("Server status received: \(status)")
            
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
        if let status = notification.object as? NEVPNStatus {
            DispatchQueue.main.async {
                let responseDict: [String: Any] = [
                    "event": "native_VPNStatus",
                    "data": ["VPNStatus": status.rawValue],
                    "callbackId": "VPNStatusUpdate"
                ]
                
                if let responseData = try? JSONSerialization.data(withJSONObject: responseDict),
                   let responseString = String(data: responseData, encoding: .utf8) {
                    DispatchQueue.main.async {
                        self.sendToWebView(responseString: responseString)
                    }
                }
            }
        }
    }
    
    private func sendToWebView(responseString: String) {
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