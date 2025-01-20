import WebKit


class NativeBridge: NSObject, WKScriptMessageHandler {
    
    private weak var webView: WKWebView?
    private var callbacks: [String: (Any?) -> Void] = [:]
    private var ready = false
    var viewController: ViewController!
    init(webView: WKWebView, viewController: ViewController) {
        super.init()
        self.webView = webView
        self.viewController = viewController
        webView.configuration.userContentController.add(self, name: "error")
        webView.configuration.userContentController.add(self, name: "ready")
        webView.configuration.userContentController.add(self, name: "startVPN")
        webView.configuration.userContentController.add(self, name: "stopVPN")
        
    }
    /**
     
    調用Javascript橋
    functionName：String javaScript中的函数名字
     arguments: 需要帶給javaScript函數的數據
     
     uuid:钩子名字    为什么要作为参数 因为有些固定参数的需要穿
        *******************  uuid勾子只在NativeBridge內部管理所使用，所以無需外部提供 ***************
     completion: 調用方等待的回調函數
                    
    示例
     
     
     解释
     */
    func callJavaScriptFunction(functionName: String, arguments: String, completion: @escaping (Any?) -> Void) {
        
        let callID = UUID().uuidString
        
        // 保存回调
        callbacks[callID] = completion
        webView?.configuration.userContentController.add(self, name: callID)
        
        //呼叫js
        
        let javascript = "fromNative('\(callID),\(functionName),\(arguments)')"
//        print("message from JavaScript \(javascript)")
        webView?.evaluateJavaScript(javascript, completionHandler: nil)
            
    }
    
    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        
        //  聆聽 JavaScript  初始化完成信號
        if (message.name == "ready") {
            return print("初始化完成信號 ready \(message.body)")
        }
        
        //      JavaScript控制台輸出
        if (message.name == "error") {
            return print("message from JavaScript \(message.body)")
        }
        
        //      UI JavaScript console
        if (message.name == "startVPN") {
            let country: String = message.body as! String
            self.viewController.layerMinus.setupEgressNodes(country: country)
            self.viewController.vPNManager.refresh()
            return print("VPN 初始化完成 message from UI JavaScript startVPN \(message.body)")
        }
        
        //      UI JavaScript console
        if (message.name == "stopVPN") {
            
            self.viewController.vPNManager.stopVPN()
            return print("message from UI JavaScript stopVPN \(message.body)")
        }
        
        // 查找并执行对应的回调
        if let callback = callbacks[message.name] {
            let data = message.body
            
            callback(data)
            webView?.configuration.userContentController.removeScriptMessageHandler(forName: message.name)
            callbacks.removeValue(forKey: message.name)
            
        }
    }
}


