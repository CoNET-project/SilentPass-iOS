//
//  WebViewManager.swift
//  tq-proxy-ios
//
//  Created by 杨旭的MacBook Pro on 2024/11/20.
//


import WebKit

class WebViewManager {
    static let shared = WebViewManager()

    var webView: WKWebView
    var nativeBridge: NativeBridge!
    private init() {
        // 定义 WebView 的配置和初始化
        let config = WKWebViewConfiguration()
        webView = WKWebView(frame: .zero, configuration: config)
        webView.isHidden = true
        
           
        
        nativeBridge = NativeBridge(webView: webView)
        // 可以添加其他配置选项，例如 JavaScript 设置等
    }

    func load(url: URL) {
        if let filePath = Bundle.main.path(forResource: "captcha", ofType: "html") {
            let fileURL = URL(fileURLWithPath: filePath)
            webView.loadFileURL(fileURL, allowingReadAccessTo: fileURL)
        }
    }
}
