import WebKit
import StoreKit
import SwiftyStoreKit
import SVProgressHUD

struct startVPNFromUI: Codable {
    var entryNodes: [Node]
    var privateKey: String
    var exitNode: [Node]
}

struct openWebview: Codable {
    var url: String
}

struct pay: Codable {
    var publicKey: String
    var Solana: String
    var transactionId: String
    var productId: String
    var total: String
    var receipt: String?
}

// ✅ 恢复订阅时，前端只会传钱包信息，单独的输入模型即可避免解码失败
private struct RestoreInput: Codable {
    let publicKey: String
    let Solana: String
}

struct postPay: Codable {
    var receipt: String
    var walletAddress: String
    var solanaWallet: String
}

// 🔥 新增：通知名称扩展，用于兜底机制


class NativeBridge: NSObject, WKScriptMessageHandler ,WKNavigationDelegate, URLSessionTaskDelegate{
#if DEBUG
@inline(__always)
    private func log(_ msg: @autoclosure () -> String) {
        NSLog("[NativeBridge] %@", msg())
    }
#else
    @inline(__always)
    private func log(_ msg: @autoclosure () -> String) { }
#endif
    private weak var webView: WKWebView?
    private var callbacks: [String: (Any?) -> Void] = [:]
    
    /// 存放 callbackId -> completion 闭包
    private var callbacksNative: [String: (Any?) -> Void] = [:]
    
    private var ready = false
    private var updater = Updater()
    var viewController: ViewController!

	// 注入的本地服务器引用（可选）
	var localServer: LocalWebServer?

	/// 将 LocalWebServer 注入并绑定 start/stop/vpnStatus Provider。
	/// 最小化修改：复用已有的 startVPNFromUI 解码及 updater.runUpdater / vPNManager.refresh() / vPNManager.stopVPN()
	func attachLocalServer(_ server: LocalWebServer) {
		self.localServer = server
        self.log("[NativeBridge] attachLocalServer: server=\(Unmanaged.passUnretained(server).toOpaque()) VC=\(Unmanaged.passUnretained(self.viewController).toOpaque())")

        // POST /startVPN -> 解码并走现有启动流程
        self.log("[NativeBridge] startVPNHandler invoked")
        server.startVPNHandler = { [weak self] bodyData in
            guard let self = self else { return false }
            do {
                let payload = try JSONDecoder().decode(startVPNFromUI.self, from: bodyData)
                // UI 相关变化放到主线程
                DispatchQueue.main.async {
                    self.viewController.layerMinus.entryNodes = payload.entryNodes
                    self.viewController.layerMinus.egressNodes = payload.exitNode
                    self.viewController.layerMinus.privateKeyAromed = payload.privateKey
                    self.viewController.vPNManager.refresh()
                }
                // 非阻塞地触发 updater（保留已有实现）
                Task { await self.updater.runUpdater(nodes: payload.entryNodes) }
                return true
            } catch {
                self.log("LocalServer startVPN decode error: \(error.localizedDescription)")
                return false
            }
        }

        // 🔥 关键修复：GET /stopVPN -> 直接停止 VPN
        self.log("[NativeBridge] stopVPNHandler invoked -> will call vPNManager.stopVPN()")
        server.stopVPNHandler = { [weak self] in
            guard let self = self else { return false }
            self.log("🔍 NativeBridge: stopVPNHandler called from LocalWebServer")
            // 重要：后台队列调用，避免与 stopVPN 内部的信号量互锁
            DispatchQueue.global(qos: .userInitiated).async {
                self.log("🔍 NativeBridge: On background queue, calling VPNManager.stopVPN()")
                self.viewController.vPNManager.stopVPN()
                self.log("✅ NativeBridge: VPNManager.stopVPN() call completed")
            }
            return true
        }

        // ⛑ 兜底：如果 LocalWebServer 端点未接线或闭包失效，监听通知也能停
        NotificationCenter.default.addObserver(forName: Notification.Name("stopVPNRequested"), object: nil, queue: .main) { [weak self] _ in
            guard let self = self else { return }
            self.log("[NativeBridge] received .stopVPNRequested fallback -> vPNManager.stopVPN()")
            self.viewController.vPNManager.stopVPN()
        }
        
        // 🔥 绑定VPN状态提供器
        server.vpnStatusProvider = { [weak self] in
            guard let self = self else { return false }
            // 这里可以根据实际的VPN状态判断逻辑来实现
            // 暂时返回false，你可以根据实际情况修改
            return false
        }
    }

    
    init(webView: WKWebView, viewController: ViewController) {
        super.init()
        self.webView = webView
        self.viewController = viewController
        webView.configuration.userContentController.add(self, name: "error")
        webView.configuration.userContentController.add(self, name: "ready")
        webView.configuration.userContentController.add(self, name: "startVPN")
        webView.configuration.userContentController.add(self, name: "stopVPN")
        webView.configuration.userContentController.add(self, name: "openUrl")

        webView.configuration.userContentController.add(self, name: "pay")
        webView.configuration.userContentController.add(self, name: "restorePurchases")
        
        webView.configuration.userContentController.add(self, name: "general")
        
        webView.configuration.userContentController.add(self, name: "native_event")
        webView.configuration.userContentController.add(self, name: "ReactNativeWebView")
        webView.configuration.userContentController.add(self, name: "nativeBridge")
        webView.configuration.userContentController.add(self, name: "webviewMessage")
        webView.configuration.userContentController.add(self, name: "updateVPNUI")
        
        webView.configuration.userContentController.add(self, name: "startCheckUpdate")
        
        webView.navigationDelegate = self
        
    }
    /**
     
     調用Javascript橋
     functionName：String javaScript中的函數名字
     arguments: 需要帶給javaScript函數的數據
     
     uuid:鉤子名字    为什么要作为参数 因为有些固定参数的需要穿
     *******************  uuid勾子只在NativeBridge内部管理所使用，所以无需外部提供 ***************
     completion: 調用方等待的回調函數
     
     示例
     
     
     解釋
     */
    func callJavaScriptFunction(functionName: String, arguments: String, completion: @escaping (Any?) -> Void) {
        
        let callID = UUID().uuidString
        
        // 保存回调
        callbacks[callID] = completion
        webView?.configuration.userContentController.add(self, name: callID)
        
        //呼叫js
        
        let javascript = "fromNative('\(callID),\(functionName),\(arguments)')"
        //        self.log("message from JavaScript \(javascript)")
        webView?.evaluateJavaScript(javascript, completionHandler: nil)
        
    }
    
    func userContentController(_ userContentController: WKUserContentController, didReceive message: WKScriptMessage) {
        
    
        // 处理来自 H5 的消息
        if message.name == "webviewMessage" {
            self.log("开始 startVPN");
            if let body = message.body as? String, let data = body.data(using: .utf8) {
                // 解析 JSON 数据
                if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                    
                    self.log("webviewMessage  \(json)");
                    
                    // 如果 payload 包含 `event` 即 H5 调用 Native
                            if let event = json["event"] as? String {
                                
                                self.log("有event  \(event)");
                                
//                                let data = json["data"] as? [String: Any]
                                let cbId = json["callbackId"] as? String

                                if let data = json["data"] as? [String: Any] {
                                    let cbId = json["callbackId"] as? String
                                    handleEvent(event: event, data: data, callbackId: cbId)
                                } else {
                                    self.log("⚠️ data 不是字典类型")
                                }
//                                handleEvent(event: event, data: data, callbackId: cbId)


                            }
                            // 否则当作 H5 回传给 Native 的回调
                            else if let cbId = json["callbackId"] as? String {
                         
                                let response = json["response"]

                                if let callback = callbacksNative[cbId] {
                                    
                                    callback(response)
                                    
                                    callbacksNative.removeValue(forKey: cbId)

                                }
                            }
                   
                    
                    
                }
            }
        }
       
        
        if (message.name == "ReactNativeWebView") {
            self.log("h5掉原生 \(message.body)")
        }
        
        //  膦鹿 JavaScript  初始化完成信號
        if (message.name == "ready") {
            self.log("初始化完成信號 ready \(message.body)")
        }
        
        //      JavaScript控制台輸出
        if (message.name == "error") {
            self.log("message from JavaScript \(message.body)")
        }
        
        if (message.name == "startCheckUpdate") {
            let base64EncodedString: String = message.body as! String
            let base64EncodedData = base64EncodedString.data(using: .utf8)!
            if let jsonText = Data(base64Encoded: base64EncodedData) {
                let clearText = String(data: jsonText, encoding: .utf8)!

                let data = clearText.data(using: .utf8)!
                do {
                    let _data = try JSONDecoder().decode(startVPNFromUI.self, from: data)
                    self.viewController.layerMinus.entryNodes = _data.entryNodes

                    Task {
                        await self.updater.runUpdater(nodes: _data.entryNodes)
                    }
                    
                    
                } catch {
                    self.log("startCheckUpdate error: \(error.localizedDescription)")
                }
                
            }
        }
        
        if (message.name == "updateVPNUI") {
         
            self.viewController.vPNManager.stopVPN()
            
            let alert = UIAlertController(
                title: "升级成功",
                message: "请退出app后重新打开",
                preferredStyle: .alert
            )

            alert.addAction(UIAlertAction(title: "YES", style: .default, handler: { _ in
                exit(0) // ⚠️ 仅限调试或企业应用，App Store 拒审
            }))

            guard let vc = viewController else { return }

            DispatchQueue.main.async {
                vc.present(alert, animated: true, completion: nil)
            }
            
        }
        
        //      UI JavaScript console
        if (message.name == "startVPN") {
            self.log("开始 startVPN");
            
            
            let base64EncodedString: String = message.body as! String
            let base64EncodedData = base64EncodedString.data(using: .utf8)!
            if let jsonText = Data(base64Encoded: base64EncodedData) {
                let clearText = String(data: jsonText, encoding: .utf8)!
//                self.log(clearText)
                let data = clearText.data(using: .utf8)!
                do {
                    let _data = try JSONDecoder().decode(startVPNFromUI.self, from: data)
                    self.viewController.layerMinus.entryNodes = _data.entryNodes
                    self.viewController.layerMinus.egressNodes = _data.exitNode
                    self.viewController.layerMinus.privateKeyAromed = _data.privateKey
                    self.viewController.vPNManager.refresh()
                    Task {
                        await self.updater.runUpdater(nodes: _data.entryNodes)
                    }
                    
                    
                } catch {
                    self.log("startVPN error: \(error.localizedDescription)")
                }
                
            }
            
            self.log("VPN 初始化完成 message from UI JavaScript startVPN")
        }
        
        //      UI JavaScript console
        if (message.name == "pay") {
            
            self.log("开始支付");
            //           return;
            
            let base64EncodedString: String = message.body as! String
            let base64EncodedData = base64EncodedString.data(using: .utf8)!
            if let jsonText = Data(base64Encoded: base64EncodedData) {
                let clearText = String(data: jsonText, encoding: .utf8)!
                self.log(clearText)
                let data = clearText.data(using: .utf8)!
                do {
                    let _data = try JSONDecoder().decode(pay.self, from: data)
                    payWithApplePay(_data)
                } catch {
                    self.log("pay error: \(error.localizedDescription)")
                }
                
            }
            return
        }
        
        
        if (message.name == "restorePurchases") {
            
            self.log("恢复订阅")

            let base64EncodedString: String = message.body as! String
                let base64EncodedData = base64EncodedString.data(using: .utf8)!
                if let jsonText = Data(base64Encoded: base64EncodedData) {
                    let clearText = String(data: jsonText, encoding: .utf8)!
                    self.log(clearText)
                    let data = clearText.data(using: .utf8)!
                    do {
                        let restore = try JSONDecoder().decode(RestoreInput.self, from: data)

                        if #available(iOS 15.0, *) {
                            Task {
                                // 仅在用户点击"恢复"时同步
                                do { try await AppStore.sync() } catch { /* 不阻塞，继续收集 JWS */ }

                                var jwss: [String] = []
                                for await entitlement in Transaction.currentEntitlements {
                                    jwss.append(entitlement.jwsRepresentation)
                                }

                                // 如果没有任何 JWS，就认为恢复失败
                                guard !jwss.isEmpty else {
                                    return self.handleRestoreError(nil)
                                }

                                // 组装并上报到你的后端

								// 使用 pay 结构体，但把恢复拿不到的字段置空字符串（服务器端可忽略）
								let payload = pay(
									publicKey: restore.publicKey,
									Solana: restore.Solana,
									transactionId: "",
									productId: "",
									total: "",
									receipt: jwss.joined(separator: "\n")
								)
								self.postToAPIServerForRecover(payload)
                            }
                        } else {
                            // iOS < 15 无法获取 JWS
                            SVProgressHUD.showInfo(withStatus: "恢复失败：需要 iOS 15 及以上")
                            SVProgressHUD.dismiss(withDelay: 2)
                        }
                    } catch {
                        self.log("restorePurchases error: \(error.localizedDescription)")
                        SVProgressHUD.showInfo(withStatus: "恢复失败：参数错误")
                        SVProgressHUD.dismiss(withDelay: 2)
                    }
                }
                return
        }
        
        
        
        
        //      UI JavaScript console
        if (message.name == "stopVPN") {
            
            self.viewController.vPNManager.stopVPN()
            self.log("message from UI JavaScript stopVPN \(message.body)")
        }
        
        if (message.name == "openUrl") {
            
            let base64EncodedString: String = message.body as! String
          
                    if let url = URL(string: base64EncodedString) {
                        if UIApplication.shared.canOpenURL(url) {
                            UIApplication.shared.open(url, options: [:], completionHandler: { success in
                                if success {
                                    self.log("成功打开 Safari")
                                } else {
                                    self.log("打开失败")
                                }
                            })
                        }
                 
            }
            
        }
        
        
        // 查找并执行对应的回调
        if let callback = callbacks[message.name] {
            let data = message.body
            
            callback(data)
            webView?.configuration.userContentController.removeScriptMessageHandler(forName: message.name)
            callbacks.removeValue(forKey: message.name)
            
        }
    }
    
    
    func postToAPIServer (_ payObj: pay) {
        
        let encoder = JSONEncoder()
        encoder.outputFormatting = .prettyPrinted
        if let postDataString = try? encoder.encode(payObj) {
            let url = URL(string: "https://hooks.conet.network/api/applePayUser")!
            var request = URLRequest(url: url)
            self.log("postToAPIServer payload prepared")
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpMethod = "POST"
            request.httpBody = postDataString
            let task = URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
                guard let self = self else { return }
                let statusCode = (response as! HTTPURLResponse).statusCode
                if statusCode == 200 {
                    self.log("postToAPIServer SUCCESS")
                    
                } else {
                    self.log("postToAPIServer FAILURE")
                    
                }
            }
            task.resume()
        }
        
    }
    
    func postToAPIServerForRecover (_ payObj: pay) {
        
        let encoder = JSONEncoder()
        encoder.outputFormatting = .prettyPrinted
        if let postDataString = try? encoder.encode(payObj) {
            let url = URL(string: "https://hooks.conet.network/api/applePayUserRecover")!
            var request = URLRequest(url: url)
            self.log("postToAPIServerForRecover payload prepared")
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpMethod = "POST"
            request.httpBody = postDataString
            let task = URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
                guard let self = self else { return }
                let statusCode = (response as! HTTPURLResponse).statusCode
                if statusCode == 200 {
                    self.log("SUCCESS")
                    
                    
                } else {
                    self.log("FAILURE")
                
                    
                }
            }
            task.resume()
        }
        
    }
    
    
    // 验证恢复的购买项
    
    // 验证恢复的购买项
    
    func getReceiptData() -> String? {
        // 获取Receipt URL
        guard let receiptURL = Bundle.main.appStoreReceiptURL,
              FileManager.default.fileExists(atPath: receiptURL.path) else {
            self.log("Receipt不存在")
            return nil
        }
        
        do {
            // 读取Receipt数据
            let receiptData = try Data(contentsOf: receiptURL)
            // 转换为Base64字符串
            let receiptString = receiptData.base64EncodedString(options: [])
            return receiptString
        } catch {
            self.log("读取Receipt失败: \(error.localizedDescription)")
            return nil
        }
    }
    
    private func verifySubscription(productIds: Set<String>, payObj: pay) {
        let validator = AppleReceiptValidator(
            service: .production,               // 调试用 .sandbox
            sharedSecret: "4ac82b1e23144df483e4bfab8b419792"  // App Store Connect -> In-App Purchases -> App-Specific Shared Secret
        )

        let verifyBlock = {
            SwiftyStoreKit.verifyReceipt(using: validator) { [weak self] result in
                guard let self = self else { return }
                switch result {
                case .success(let receipt):
                    let status = SwiftyStoreKit.verifySubscriptions(
                        ofType: .autoRenewable,
                        productIds: productIds,
                        inReceipt: receipt
                    )
                    switch status {
                    case .purchased(let expiryDate, let items):
                        // ✅ 订阅有效
                        self.log("Active until: \(expiryDate). Items: \(items.count)")
                        

                        // 3) 所有内购项（一次性购买也在这里）
                        if let rec = receipt["receipt"] as? [String: AnyObject],
                           let inApps = rec["in_app"] as? [[String: AnyObject]], let latestReceiptB64 = receipt["latest_receipt"] as? String  {
                            
                            var postServer = false
                            if latestReceiptB64.count > 0 {
                                postServer = true
                            }
                            for item in inApps {
                                let productId = item["006"] as? String
                                if productId != nil {
                                    postServer = true
                                }
                            }
                            if postServer, let receipt = self.getReceiptData() {
                                if receipt != nil {
                                    var updatedPayObj = payObj
                                    updatedPayObj.receipt = receipt
                                    return self.postToAPIServerForRecover(updatedPayObj)
                                }
                            }
                        }

                    case .expired(let expiryDate, let items):
                        // ⏰ 已过期
                        self.log("Expired at: \(expiryDate). Items: \(items.count)")
                        self.handleRestoreError(nil)

                    case .notPurchased:
                        // 🚫 从未购买（或非当前 Apple ID）
                        self.log("Not purchased")
                        self.handleRestoreError(nil)
                    }

                case .error(let error):
                    self.log("Receipt verify error: \(error.localizedDescription)")
                    // 可能是没有收据 / 网络问题，尝试刷新收据
                    
                }
            }
        }

        // 先试着直接校验（有时系统已下发收据）
        verifyBlock()
    }
        
    
    
    
    
    
    func payWithApplePay(_ payObj: pay)
    {
      
//        var product = payObj.total == "1" ? "001": "002"
        
        var product = payObj.total == "1"
          ? "001"
          : payObj.total == "2"
          ? "002"
          : payObj.total == "3100"
          ? "006"
          : payObj.total;

        SwiftyStoreKit.purchaseProduct(product) { [weak self] result in
            guard let self = self else { return }
            switch result {
                case .success(let purchase):
                        // Purchase was successful
                    
                        
                        // Extract the transactionId as a String
                    if let transactionId = purchase.transaction.transactionIdentifier {
                        DispatchQueue.main.async {
                            if let appStoreReceiptURL = Bundle.main.appStoreReceiptURL,
                               let receiptData = try? Data(contentsOf: appStoreReceiptURL, options: .alwaysMapped) {
                                
                                let receiptString = receiptData.base64EncodedString(options: [])
                                
                                
                                UserDefaults.standard.set(receiptString, forKey: "pendingReceipt")
                                UserDefaults.standard.synchronize()
                                
                                // Set the transactionId in the product
                                var updatedPayObj = payObj
                                updatedPayObj.transactionId = transactionId
                                updatedPayObj.productId = purchase.productId
                                self.log("Purchase successful for product: \(updatedPayObj.productId)")
                                // Now send the data to the server
                                self.postToAPIServer(updatedPayObj)
                            }
                        }
                    }
                
            case .error(let error):
                
                self.handleRestoreError(error)
                
                //if let url = URL(string: Constants.baseURL) {
                //let request = URLRequest(url: url)
                //self.webView?.load(request)
                //}
            }
        }
        
        
        
        //    manager.restoreSubscriptions()
        
    }
    
    /// 统一处理"恢复/购买失败"提示，允许 error 为 nil
    func handleRestoreError(_ error: Error?) {
        // 先生成要显示/打印的文案
        let consoleMsg: String
        let hudMsg: String

        if let error {
            consoleMsg = "❌ 恢复失败: \(error.localizedDescription)"
            hudMsg = humanReadableMessage(for: error)
        } else {
            consoleMsg = "❌ 恢复失败: (error = nil)"
            hudMsg = "恢复失败，请稍后再试"
        }

        // 控制台详细信息
        self.log(consoleMsg)

        // UI 提示在主线程
        DispatchQueue.main.async {
            SVProgressHUD.showInfo(withStatus: hudMsg)
            SVProgressHUD.dismiss(withDelay: 2)
        }
    }


    // --- WKNavigationDelegate 方法 ---

    /// 在网页开始加载但未能完成时（例如，因网络连接或服务器错误）调用
    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        SVProgressHUD.dismiss()
        self.log("❌ 网页加载失败 (Provisional Navigation): \(error.localizedDescription)")
        
        // 将 Error 对象向下转型为 NSError 以获取更多信息
        if let urlError = error as? URLError {
            switch urlError.code {
            case .notConnectedToInternet:
                self.log("⚠️ 错误代码: .notConnectedToInternet - 请检查您的网络连接。")
                // 可以显示一个用户友好的提示
                showAlert(title: "网络错误", message: "无法连接到互联网。请检查您的网络设置。")
            case .timedOut:
                self.log("⚠️ 错误代码: .timedOut - 请求超时。")
                showAlert(title: "连接超时", message: "加载页面超时。请稍后再试。")
            case .cannotFindHost:
                self.log("⚠️ 错误代码: .cannotFindHost - 无法找到服务器。")
                showAlert(title: "服务器错误", message: "无法找到指定服务器。")
            case .cannotConnectToHost:
                self.log("⚠️ 错误代码: .cannotConnectToHost - 无法连接到服务器。")
                showAlert(title: "连接错误", message: "无法连接到服务器。")
            case .badServerResponse:
                self.log("⚠️ 错误代码: .badServerResponse - 服务器响应无效。")
                showAlert(title: "服务器错误", message: "服务器响应无效。")
            case .appTransportSecurityRequiresSecureConnection:
                self.log("⚠️ 错误代码: .appTransportSecurityRequiresSecureConnection - ATS 要求安全连接。")
                showAlert(title: "安全连接错误", message: "此应用需要安全的网络连接。")
            case .cancelled:
                self.log("⚠️ 错误代码: .cancelled - 加载被取消。")
                // 通常发生在用户在页面完全加载前导航到另一个页面时
            default:
                self.log("⚠️ 其他 URLError: \(urlError.code.rawValue) - \(urlError.localizedDescription)")
                showAlert(title: "加载失败", message: "加载页面时发生未知错误: \(urlError.localizedDescription)")
            }
        } else {
            // 处理非 URLError 类型的错误
            showAlert(title: "加载失败", message: "加载页面时发生未知错误: \(error.localizedDescription)")
        }
    }
    
    /// 当导航失败时（例如，在数据加载完成后但内容无法显示时）调用
    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
        SVProgressHUD.dismiss()
        self.log("❌ 网页导航失败: \(error.localizedDescription)")
        // 这个方法通常在 didFailProvisionalNavigation 之后或在其他更深层次的渲染/脚本错误时被调用
        // 你也可以在此处添加类似的错误处理逻辑
    }
    
    // 改成这样
    func showAlert(title: String, message: String) {
//        guard let vc = viewController else { return }
//        let alert = UIAlertController(title: title, message: message, preferredStyle: .alert)
//        alert.addAction(.init(title: "确定", style: .default))
//        DispatchQueue.main.async {
//            vc.present(alert, animated: true, completion: nil)
//        }
    }
    // webView加载结束的方法
    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        
        SVProgressHUD.dismiss()
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) {
            NotificationCenter.default.post(
                name: Notification.Name("LocalServerStarted"),
                object: nil,
                userInfo: ["status": "running"]
                
            )
        }
        
        

                // 注入 JavaScript 禁用网页缩放
//                let jsCode = """
//                // 禁用双指缩放
//                document.documentElement.style.overflow = 'hidden';
//                document.documentElement.addEventListener('touchstart', function(e) {
//                    if (e.touches.length > 1) {
//                        e.preventDefault();
//                    }
//                }, { passive: false });
//                
//                // 禁用双击缩放
//                document.addEventListener('dblclick', function(e) {
//                    e.preventDefault();
//                }, { passive: false });
//                """
//                webView.evaluateJavaScript(jsCode) { _, _ in }
  
    }
    
    
//    示例 let responseDict: [String: Any] = [
//    "event": "native_event",
//    "data": ["a": 33333],
//    "callbackId": "樊旭发给老樊"
//]
    
    func postNativeWebView(event: String ,data: NSDictionary ,completion: ((Any?) -> Void)? = nil)
    {
      
        let callbackId = "cb_\(Int(Date().timeIntervalSince1970 * 1000))_\(UUID().uuidString)"
        self.log( "当前的 callid \(callbackId) ");
        // 2. 如果有闭包则保存
                if let completion = completion {
                    callbacksNative[callbackId] = completion
                }
        
        let responseDict: [String: Any] = [
            "event": event,
            "data": data,
            "callbackId": callbackId
        ]
       
        let responseData = try? JSONSerialization.data(withJSONObject: responseDict)
        let responseString = String(data: responseData!, encoding: .utf8) ?? "{}"
        let js = """
            window.dispatchEvent(new MessageEvent('message', { data: '\(responseString)' }));
            """
        self.webView?.evaluateJavaScript(js, completionHandler: { [weak self] result, error in
            guard let self = self else { return }
            if let error = error {
                self.log("JS执行失败: \(error.localizedDescription)")
            } else {
                self.log("JS执行成功")
            }
        })
    }
    
    // 封装发送 JavaScript 消息的方法
    private func sendToWebView(responseString: String) {
        let js = """
        window.dispatchEvent(new MessageEvent('message', { data: '\(responseString)' }));
        """
        self.log("发送的 js 是：\(js)")
        self.webView?.evaluateJavaScript(js, completionHandler: { [weak self] result, error in
            guard let self = self else { return }
            if let error = error {
                self.log("JS执行失败: \(error.localizedDescription)")
            } else {
                self.log("JS执行成功，返回: \(String(describing: result))")
            }
        })
    }
    
    
    
    /// 根据不同的 event 类型处理业务逻辑
    private func handleEvent(event: String,
                             data: [String: Any],
                             callbackId: String?)
    {

        // 打印 data，查看具体数据内容
        
        // 根据 event 类型执行不同的逻辑
        switch event {
        case "webview_event":
            // 处理 webview_event 事件
            if let cbId = callbackId {
                // 有 callbackId 时，发送回 H5
                let response: [String: Any] = [
                    "callbackId": cbId,
                    "response": ["msg": "我是樊旭我收到了"]
                ]
                // 转换为 JSON 字符串并发送回 H5
                if let responseData = try? JSONSerialization.data(withJSONObject: response),
                   let responseString = String(data: responseData, encoding: .utf8) {
                    DispatchQueue.main.async {
                        self.sendToWebView(responseString: responseString)
                    }
                }
            } else {
                // 没有 callbackId 时，可以执行其他逻辑，或者只记录日志
                self.log("没有 callbackId，跳过回传")
            }
            
     
            
            
            
        

        case "startVPN":
            
            
            let base64EncodedString: String = data["data"] as! String
            let base64EncodedData = base64EncodedString.data(using: .utf8)!
            if let jsonText = Data(base64Encoded: base64EncodedData) {
                let clearText = String(data: jsonText, encoding: .utf8)!
                self.log(clearText)
                let data = clearText.data(using: .utf8)!
                do {
                    let _data = try JSONDecoder().decode(startVPNFromUI.self, from: data)
                    self.viewController.layerMinus.entryNodes = _data.entryNodes
                    self.viewController.layerMinus.egressNodes = _data.exitNode
                    self.viewController.layerMinus.privateKeyAromed = _data.privateKey
                    self.viewController.vPNManager.refresh()
                    Task {
                        await self.updater.runUpdater(nodes: _data.entryNodes)
                    }
                    
                    // 处理 webview_event 事件
                    if let cbId = callbackId {
                        // 有 callbackId 时，发送回 H5
                        let response: [String: Any] = [
                            "callbackId": cbId,
                            "response": ["msg": "VPN已开启"]
                        ]
                        // 转换为 JSON 字符串并发送回 H5
                        if let responseData = try? JSONSerialization.data(withJSONObject: response),
                           let responseString = String(data: responseData, encoding: .utf8) {
                            DispatchQueue.main.async {
                                self.sendToWebView(responseString: responseString)
                            }
                        }
                    } else {
                        // 没有 callbackId 时，可以执行其他逻辑，或者只记录日志
                        self.log("没有 callbackId，跳过回传")
                    }
                    
                } catch {
                    self.log("handleEvent startVPN error: \(error.localizedDescription)")
                }
                
            }
            
        case "stopVPN":
           
                
            self.viewController.vPNManager.stopVPN()
            // 处理 webview_event 事件
            if let cbId = callbackId {
                // 有 callbackId 时，发送回 H5
                let response: [String: Any] = [
                    "callbackId": cbId,
                    "response": ["msg": "VPN已关闭"]
                ]
                // 转换为 JSON 字符串并发送回 H5
                if let responseData = try? JSONSerialization.data(withJSONObject: response),
                   let responseString = String(data: responseData, encoding: .utf8) {
                    DispatchQueue.main.async {
                        self.sendToWebView(responseString: responseString)
                    }
                }
            } else {
                // 没有 callbackId 时，可以执行其他逻辑，或者只记录日志
                self.log("没有 callbackId，跳过回传")
            }
            
        case "openUrl":
            let base64EncodedString: String = data["data"] as! String
            if let url = URL(string: base64EncodedString) {
                if UIApplication.shared.canOpenURL(url) {
                    UIApplication.shared.open(url, options: [:], completionHandler: { success in
                        if success {
                            self.log("成功打开 Safari")
                        } else {
                            self.log("打开失败")
                        }
                    })
                }
                
            }
            

        // 可以继续添加更多 event 的处理
        default:
            self.log("未知事件: \(event)")
            
            if let cbId = callbackId {
                // 有 callbackId 时，发送回 H5
                let response: [String: Any] = [
                    "callbackId": cbId,
                    "response": ["msg": "请升级app"]
                ]
                // 转换为 JSON 字符串并发送回 H5
                if let responseData = try? JSONSerialization.data(withJSONObject: response),
                   let responseString = String(data: responseData, encoding: .utf8) {
                    DispatchQueue.main.async {
                        self.sendToWebView(responseString: responseString)
                    }
                }
            } else {
                // 没有 callbackId 时，可以执行其他逻辑，或者只记录日志
                self.log("没有 callbackId，跳过回传")
            }
            
        }
    }
    
    private func humanReadableMessage(for error: Error) -> String {
        // StoreKit 2 错误：注意没有 verificationFailed 这个 case
        if #available(iOS 15.0, *), let sk2 = error as? StoreKitError {
            switch sk2 {
            case .userCancelled:
                return "已取消"
            case .networkError(_):
                return "网络错误，请检查连接"
            case .notAvailableInStorefront:
                return "该商品在当前地区不可用"
            case .notEntitled:
                return "未获得购买权限"
            case .systemError(_):
                return "系统错误，请稍后再试"
            default:
                break
            }
        }

        // 旧版 StoreKit
        if let sk = error as? SKError {
            switch sk.code {
            case .paymentCancelled:         return "已取消"
            case .storeProductNotAvailable: return "该商品暂不可购买"
            case .paymentNotAllowed:        return "设备不允许购买"
            case .cloudServiceNetworkConnectionFailed:
                return "网络错误，请检查连接"
            default:
                break
            }
        }

        // 网络层
        let nsErr = error as NSError
        if nsErr.domain == NSURLErrorDomain {
            return "网络连接异常（\(nsErr.code)），请稍后再试"
        }

        // 兜底
        return error.localizedDescription.isEmpty ? "发生未知错误，请稍后再试" : error.localizedDescription
    }


}
