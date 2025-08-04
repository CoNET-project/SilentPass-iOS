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
}

struct postPay: Codable {
    var receipt: String
    var walletAddress: String
    var solanaWallet: String
}

class NativeBridge: NSObject, WKScriptMessageHandler ,WKNavigationDelegate, URLSessionTaskDelegate{
    
    private weak var webView: WKWebView?
    private var callbacks: [String: (Any?) -> Void] = [:]
    
    /// 存放 callbackId -> completion 闭包
    private var callbacksNative: [String: (Any?) -> Void] = [:]
    
    private var ready = false
    private var updater = Updater()
    var viewController: ViewController!
    
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
        
        webView.configuration.userContentController.add(self, name: "general")
        
        webView.configuration.userContentController.add(self, name: "native_event")
        webView.configuration.userContentController.add(self, name: "ReactNativeWebView")
        webView.configuration.userContentController.add(self, name: "nativeBridge")
        webView.configuration.userContentController.add(self, name: "webviewMessage")
        webView.configuration.userContentController.add(self, name: "updateVPNUI")
        webView.navigationDelegate = self
        
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
        
    
        // 处理来自 H5 的消息
        if message.name == "webviewMessage" {
            print("开始支startVPN");
            if let body = message.body as? String, let data = body.data(using: .utf8) {
                // 解析 JSON 数据
                if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                    
                    print("webviewMessage  \(json)");
                    
                    // 如果 payload 包含 `event` 即 H5 调用 Native
                            if let event = json["event"] as? String {
                                
                                print("有event  \(event)");
                                
//                                let data = json["data"] as? [String: Any]
                                let cbId = json["callbackId"] as? String

                                if let data = json["data"] as? [String: Any] {
                                    let cbId = json["callbackId"] as? String
                                    handleEvent(event: event, data: data, callbackId: cbId)
                                } else {
                                    print("⚠️ data 不是字典类型")
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
            return print("h5掉原生 \(message.body)")
        }
        
        //  聆聽 JavaScript  初始化完成信號
        if (message.name == "ready") {
            return print("初始化完成信號 ready \(message.body)")
        }
        
        //      JavaScript控制台輸出
        if (message.name == "error") {
            return print("message from JavaScript \(message.body)")
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
            print("开始支startVPN");
            
            
            let base64EncodedString: String = message.body as! String
            let base64EncodedData = base64EncodedString.data(using: .utf8)!
            if let jsonText = Data(base64Encoded: base64EncodedData) {
                let clearText = String(data: jsonText, encoding: .utf8)!
//                print(clearText)
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
                    print(error)
                }
                
            }
            
            return print("VPN 初始化完成 message from UI JavaScript startVPN \(message.body)")
        }
        
        //      UI JavaScript console
        if (message.name == "pay") {
            
            print("开始支付");
            //           return;
            
            let base64EncodedString: String = message.body as! String
            let base64EncodedData = base64EncodedString.data(using: .utf8)!
            if let jsonText = Data(base64Encoded: base64EncodedData) {
                let clearText = String(data: jsonText, encoding: .utf8)!
                print(clearText)
                let data = clearText.data(using: .utf8)!
                do {
                    let _data = try JSONDecoder().decode(pay.self, from: data)
                    payWithApplePay(_data)
                } catch {
                    print(error)
                }
                
            }
            return
        }
        
        
        
        //      UI JavaScript console
        if (message.name == "stopVPN") {
            
            self.viewController.vPNManager.stopVPN()
            return print("message from UI JavaScript stopVPN \(message.body)")
        }
        
        if (message.name == "openUrl") {
            
            let base64EncodedString: String = message.body as! String
          
                    if let url = URL(string: base64EncodedString) {
                        if UIApplication.shared.canOpenURL(url) {
                            UIApplication.shared.open(url, options: [:], completionHandler: { success in
                                if success {
                                    print("成功打开 Safari")
                                } else {
                                    print("打开失败")
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
            print(payObj)
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpMethod = "POST"
            request.httpBody = postDataString
            let task = URLSession.shared.dataTask(with: request) { data, response, error in
                let statusCode = (response as! HTTPURLResponse).statusCode
                if statusCode == 200 {
                    print("SUCCESS")
                    
                    DispatchQueue.main.async{
                        //                        self.webView?.goBack();
                        self.viewController.vPNManager.stopVPN()
                        SVProgressHUD.showInfo(withStatus: "Your purchase was successful and your duration has been increased,Please reconnect to the vpn")
                        SVProgressHUD.dismiss(withDelay: 2)
                        self.webView?.reload();
                        //                        self.webView?.goBack();
                    }
                    
                } else {
                    print("FAILURE")
                    
                    DispatchQueue.main.async{
                        //                        self.webView?.goBack();
                        self.webView?.goBack();
                        //                        self.webView?.goBack();
                    }
                    
                }
            }
            task.resume()
        }
        
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

        SwiftyStoreKit.purchaseProduct(product) { result in
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
                                print("Purchase successful for product: \(updatedPayObj)")
                                // Now send the data to the server
                                self.postToAPIServer(updatedPayObj)
                            }
                        }
                    }
                
            case .error(let error):
                
                DispatchQueue.main.async{
                    print("购买失败: \(error.localizedDescription)")
                    SVProgressHUD.showInfo(withStatus: "Purchase failed, please try again")
                    self.webView?.goBack();
                    
                    SVProgressHUD.dismiss(withDelay: 2)
                }
                
                //if let url = URL(string: Constants.baseURL) {
                //let request = URLRequest(url: url)
                //self.webView?.load(request)
                //}
            }
        }
        
        
        
        //    manager.restoreSubscriptions()
        
    }

    // --- WKNavigationDelegate 方法 ---

    /// 在网页开始加载但未能完成时（例如，因网络连接或服务器错误）调用
    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {
        SVProgressHUD.dismiss()
        print("❌ 网页加载失败 (Provisional Navigation): \(error.localizedDescription)")
        
        // 将 Error 对象向下转型为 NSError 以获取更多信息
        if let urlError = error as? URLError {
            switch urlError.code {
            case .notConnectedToInternet:
                print("⚠️ 错误代码: .notConnectedToInternet - 请检查您的网络连接。")
                // 可以显示一个用户友好的提示
                showAlert(title: "网络错误", message: "无法连接到互联网。请检查您的网络设置。")
            case .timedOut:
                print("⚠️ 错误代码: .timedOut - 请求超时。")
                showAlert(title: "连接超时", message: "加载页面超时。请稍后再试。")
            case .cannotFindHost:
                print("⚠️ 错误代码: .cannotFindHost - 无法找到服务器。")
                showAlert(title: "服务器错误", message: "无法找到指定服务器。")
            case .cannotConnectToHost:
                print("⚠️ 错误代码: .cannotConnectToHost - 无法连接到服务器。")
                showAlert(title: "连接错误", message: "无法连接到服务器。")
            case .badServerResponse:
                print("⚠️ 错误代码: .badServerResponse - 服务器响应无效。")
                showAlert(title: "服务器错误", message: "服务器响应无效。")
            case .appTransportSecurityRequiresSecureConnection:
                print("⚠️ 错误代码: .appTransportSecurityRequiresSecureConnection - ATS 要求安全连接。")
                showAlert(title: "安全连接错误", message: "此应用需要安全的网络连接。")
            case .cancelled:
                print("⚠️ 错误代码: .cancelled - 加载被取消。")
                // 通常发生在用户在页面完全加载前导航到另一个页面时
            default:
                print("⚠️ 其他 URLError: \(urlError.code.rawValue) - \(urlError.localizedDescription)")
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
        print("❌ 网页导航失败: \(error.localizedDescription)")
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
//    "callbackId": "杨旭发给老杨"
//]
    
    func postNativeWebView(event: String ,data: NSDictionary ,completion: ((Any?) -> Void)? = nil)
    {
      
        let callbackId = "cb_\(Int(Date().timeIntervalSince1970 * 1000))_\(UUID().uuidString)"
        print( "当前的 callid \(callbackId) ");
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
        self.webView?.evaluateJavaScript(js, completionHandler: { result, error in
            if let error = error {
                print("错误")
            } else {
                print("成功")
            }
        })
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
                    "response": ["msg": "我是杨旭我收到了"]
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
                print("没有 callbackId，跳过回传")
            }
            
     
            
            
            
        

        case "startVPN":
            
            
            let base64EncodedString: String = data["data"] as! String
            let base64EncodedData = base64EncodedString.data(using: .utf8)!
            if let jsonText = Data(base64Encoded: base64EncodedData) {
                let clearText = String(data: jsonText, encoding: .utf8)!
                print(clearText)
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
                        print("没有 callbackId，跳过回传")
                    }
                    
                } catch {
                    print(error)
                }
                
            }
            
        case "stopVPN":
           
                
            self.viewController.vPNManager.stopVPN()
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
                print("没有 callbackId，跳过回传")
            }
            
        case "openUrl":
            let base64EncodedString: String = data["data"] as! String
            if let url = URL(string: base64EncodedString) {
                if UIApplication.shared.canOpenURL(url) {
                    UIApplication.shared.open(url, options: [:], completionHandler: { success in
                        if success {
                            print("成功打开 Safari")
                        } else {
                            print("打开失败")
                        }
                    })
                }
                
            }
            

        // 可以继续添加更多 event 的处理
        default:
            print("未知事件: \(event)")
            
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
                print("没有 callbackId，跳过回传")
            }
            
        }
    }


}

