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
class ViewController: UIViewController {
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
    override func viewDidLoad() {
        super.viewDidLoad()
        // 创建按钮
       
        // 设置按钮的位置和大小
        getRegionButton.frame = CGRect(x: 100, y: 100, width: 200, height: 100)
        // 设置按钮的标题
        getRegionButton.setTitle("正在获取全部区域，请稍等", for:.normal)
        getRegionButton.titleLabel?.adjustsFontSizeToFitWidth = true
        // 设置按钮标题的颜色
        getRegionButton.setTitleColor(UIColor.blue, for:.normal)
        
        // 设置按钮被按下时的标题颜色
        getRegionButton.setTitleColor(UIColor.gray, for:.highlighted)
        
        // 添加点击事件处理
        getRegionButton.addTarget(self, action: #selector(getRegionButtonClick), for:.touchUpInside)
        // 将按钮添加到视图中
        view.addSubview(getRegionButton)
        
        
        
        let dns = DomainFilter()
        let addedArray = dns.getIPArray()
        print (addedArray)
        for ip in dns.getAll_IpAddr() {
            let mask = dns.getMask(ip)
            print ("Ip = \(ip), Mask = \(mask)")
        }
        print(dns.getSocksDomain())
        
        // 设置按钮的位置和大小
        getnodeButton.frame = CGRect(x: 100, y: 220, width: 200, height: 100)
        // 设置按钮的标题
        getnodeButton.setTitle("点击随机设置区域", for:.normal)
        getnodeButton.titleLabel?.adjustsFontSizeToFitWidth = true
        // 设置按钮标题的颜色
        getnodeButton.setTitleColor(UIColor.blue, for:.normal)
        
        // 设置按钮被按下时的标题颜色
        getnodeButton.setTitleColor(UIColor.gray, for:.highlighted)
        
        // 添加点击事件处理
        getnodeButton.addTarget(self, action: #selector(getnodeButtonClick), for:.touchUpInside)
        // 将按钮添加到视图中
        view.addSubview(getnodeButton)
        
        
        
        let startVPNButton = UIButton(type:.system)
        // 设置按钮的位置和大小
        startVPNButton.frame = CGRect(x: 100, y: 340, width: 200, height: 100)
        // 设置按钮的标题
        startVPNButton.setTitle("拿到节点后启动vpn", for:.normal)
        startVPNButton.titleLabel?.adjustsFontSizeToFitWidth = true
        // 设置按钮标题的颜色
        startVPNButton.setTitleColor(UIColor.blue, for:.normal)
        
        // 设置按钮被按下时的标题颜色
        startVPNButton.setTitleColor(UIColor.gray, for:.highlighted)
        
        // 添加点击事件处理
        startVPNButton.addTarget(self, action: #selector(startVPN), for:.touchUpInside)
        // 将按钮添加到视图中
        view.addSubview(startVPNButton)
        
        
        let closeVPNButton = UIButton(type:.system)
        // 设置按钮的位置和大小
        closeVPNButton.frame = CGRect(x: 100, y: 460, width: 200, height: 100)
        // 设置按钮的标题
        closeVPNButton.setTitle("关闭vpn", for:.normal)
        closeVPNButton.titleLabel?.adjustsFontSizeToFitWidth = true
        // 设置按钮标题的颜色
        closeVPNButton.setTitleColor(UIColor.blue, for:.normal)
        
        // 设置按钮被按下时的标题颜色
        closeVPNButton.setTitleColor(UIColor.gray, for:.highlighted)
        
        // 添加点击事件处理
        closeVPNButton.addTarget(self, action: #selector(closeVPN), for:.touchUpInside)
        // 将按钮添加到视图中
        view.addSubview(closeVPNButton)


        self.layerMinus = LayerMinus(port: self.port)
//        self.layerMinus.scanAll_nodes()
//        self.localServer = Server(port: UInt16(self.port), layerMinus: self.layerMinus )
//        self.localServer?.start()
        self.vPNManager = VPNManager(layerMinus: self.layerMinus)
        
        NotificationCenter.default.addObserver(self, selector: #selector(handleNodeUpdate(notification:)), name: .didUpdateConnectionNodes, object: nil)
        
        
        // 创建 WKWebView 配置
               let webConfiguration = WKWebViewConfiguration()
               webView = WKWebView(frame: .zero, configuration: webConfiguration)

               // 设置 WebView 的全屏布局
               webView.translatesAutoresizingMaskIntoConstraints = false
        self.view.addSubview(webView)
        webView.frame = CGRect(x: 0, y: 0, width: self.view.frame.size.width, height: self.view.frame.size.height)
        // 加载网址
        self.nativeBridge = NativeBridge(webView: webView, viewController: self)
        
                if let url = URL(string: "https://vpn-beta.conet.network/#/") {
                    let request = URLRequest(url: url)
                    webView.load(request)
                }
        
        
        DispatchQueue.main.async{
//            let hud = MBProgressHUD.showAdded(to: self.view, animated: true)
            
//            self.hud = MBProgressHUD.showAdded(to: self.view, animated: true)
//            self.hud?.label.text = "加载中...第一次加载全部节点较慢，请稍等"
//            self.hud.hide(animated: true, afterDelay: 2.0)
            
        }
        
        
        
        NotificationCenter.default.addObserver(
            forName: Notification.Name("VPNStatusChanged"),
            object: nil,
            queue: .main
        ) { notification in
            if let status = notification.object as? NEVPNStatus {
                
                let state = String(status.rawValue)
                
                let jsFunction = "appVpnState('\(state)');"
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
    }
    
   
    
   
    
    @objc func handleNodeUpdate(notification: Notification) {
        
        if let userInfo = notification.userInfo
        {
            if let nodeCount = userInfo["当前通知类型"] as? String {
                if nodeCount == "获取所有节点"
                {
                    DispatchQueue.main.async {
                        
                        self.hud?.label.text = "已经获取到全部节点，请稍等"
                        
                    }
                    
                }
                
                
                if nodeCount == "切换区域成功"
                {
                    
//                    DispatchQueue.main.async {
//                        self.label.stringValue = "当前状态:切换区域成功，等待节点连接"
//                        
//                        self.hud.label.text = "当前状态:切换区域成功，等待节点连接"
//                        
//                    }
                    
                }
                if nodeCount == "允许上网"
                {
                    
                    DispatchQueue.main.async {
                        
                        self.hud?.label.text = "加载完成，您可以开启VPN啦"
                        self.hud?.hide(animated: true, afterDelay: 1)
                       
                        
                    }
                    
                }
                
                if nodeCount == "节点数量"
                {
                    if let nodeCount1 = userInfo["节点"] as? String {
                            DispatchQueue.main.async {
                                
                                self.hud?.label.text = "当前可用的节点数量\(nodeCount1)"
                                
                            }
                    }
                    
                }
                if nodeCount == "网络连接失败"
                {
                    
                    DispatchQueue.main.async {
                        
                        
//                        self.hud.label.text = "遭遇异常，请重新启动app"
                        self.hud?.hide(animated: true, afterDelay: 1)
                        
                        
                    }
                    
                }
                
                
            }
        }
        
       
       
        // 此处可更新 UI 或其他处理
    }
    
    struct ResponseData: Codable {
        let region: [String]
        let nodes: Int
        let nearbyRegion: String
        let account: String
    }
    
    @objc func getRegionButtonClick()
    {
        
//        展示所有区域
        
//        //进入app第一步   本地服务器启动
//        let args = "获取区域信息"
//    
//        let jsFunc = "getRegionV1"
//        
//        SVProgressHUD.show()
//         
//        // 显示文本信息
//        SVProgressHUD.showInfo(withStatus: "正在获取区域信息")
//
//
//        WebViewManager.shared.nativeBridge.callJavaScriptFunction(functionName: jsFunc, arguments: args) { [weak self] response in
//            guard let strongSelf = self else { return } // 确保 self 仍然存在
//            
//        
//            print("让我们来看看JS给我返回了什么内容: \(String(describing: response))")
//            let inputString = response as! String
//            
//            
//            // 将 JSON 字符串转换为 Data
//               if let jsonData = inputString.data(using: .utf8) {
//                   do {
//                       // 使用 JSONDecoder 解码 JSON 数据
//                       let decoder = JSONDecoder()
//                       let responseData = try decoder.decode(ResponseData.self, from: jsonData)
//                       
//                       // 打印解析结果
////                       print("Regions: \(responseData.region)")
////                       print("Nodes count: \(responseData.nodes)")
////                       print("Nearby Region: \(responseData.nearbyRegion)")
////                       print("Account: \(responseData.account)")
//                       
//                       if responseData.region.count == 0
//                       {
//                           print("没有获取到Regions: \(responseData.region) 需要重新启动app")
//                           
//                           SVProgressHUD.showInfo(withStatus: "区域信息异常需要重启app")
//                           
//                           SVProgressHUD.dismiss(withDelay: 2)
//                       }
////                       
//                       if responseData.nearbyRegion != "nearbyRegion not ready!"
//                       {
//                           print("最近节点找到了吗: \(responseData.nearbyRegion)")
//                           
//                           SVProgressHUD.showInfo(withStatus: "已经获取到最近节点信息可以点击获取节点按钮了 \(responseData.nearbyRegion)")
////                           self?.getnodeButtonClick()
//                           SVProgressHUD.dismiss(withDelay: 3)
//                           
//                       }
//                   } catch {
//                       print("Failed to decode JSON: \(error.localizedDescription)")
//                   }
//               } else {
//                   print("Failed to convert JSON string to Data")
//               }
//            
//
//            // 使用正则表达式来匹配国家代码部分
//            if let regex = try? NSRegularExpression(pattern: "=> ([A-Z,]+)", options: []) {
//                let nsString = inputString as NSString
//                let results = regex.matches(in: inputString, options: [], range: NSRange(location: 0, length: nsString.length))
//                
//                if let match = results.first, let range = Range(match.range(at: 1), in: inputString) {
//                    let countriesString = String(inputString[range]) // 提取国家代码的字符串
//                    
//                    // 按逗号分割成数组
//                    let countriesArray = countriesString.split(separator: ",").map { String($0) }
//                    
//                    print(countriesArray) // 打印数组内容
//                    DispatchQueue.main.async {
//                                     
//                        
//                        
//                       
//                    }
//                   
//                    
//                }
//            }
//            
//            
//        }
        
      
        
        
    }
    
    @objc func getnodeButtonClick()
    {
        
        // 声明一个可选字符串以保存随机国家
        var selectedCountry: String?
        if let randomCountry = LayerMinus.country.randomElement() {
            print("随机国家: \(randomCountry)")
            selectedCountry = randomCountry
        } else {
            print("国家集合是空的")
        }
      
        // 在这里你可以使用 selectCountry
        if let contry = selectedCountry { // 确保该值存在
            
            DispatchQueue.main.async {
                self.getnodeButton.setTitle("当前区域\(contry)", for:.normal)
                WebViewManager.shared.layerMinus.setupEgressNodes(country: contry)
            }
            
        } else {
            print("没有选择到国家")
        }
    }
    
    
    
    func updateConnectionNodes(egressNodes: [String], entryNodes: [String], privateKey: String) {
      
            // 调用 putNodes 更新节点
//        localServer?.putNodes(entryNodes: entryNodes, egressNodes: egressNodes, privateKey: privateKey)
        
        // 在主程序中
        let userDefaults = UserDefaults(suiteName: "group.com.fx168.CoNETVPN1.CoNETVPN1")
        userDefaults?.set(entryNodes, forKey: "entryNodes")
        userDefaults?.set(egressNodes, forKey: "egressNodes")
        userDefaults?.set(privateKey, forKey: "privateKey")
        userDefaults?.synchronize()
        
    
        
    }
    
    @objc func closeVPN()
    {
        let manager = NEVPNManager.shared()
        manager.connection.stopVPNTunnel()
    }
    @objc func startVPN()
    {
//        refresh()
        self.vPNManager.refresh()
    }
    private func refresh() {
 
        NETunnelProviderManager.loadAllFromPreferences { [weak self] managers, error in

            // There is only one VPN configuration the app provides
            
            if let error = error {
                return NSLog("NETunnelProviderManager.loadAllFromPreferences ERROR", error.localizedDescription)
            }
            
            guard let strongSelf = self else { return }
            guard let tunnels = managers else { return }
            
            NSLog("refresh NETunnelProviderManager.loadAllFromPreferences success! tunnels.count = ", tunnels.count)
            
            if tunnels.count == 0 {
                return strongSelf.createTunnel()
            }
            
            
            let group = DispatchGroup()

            for tunnel in tunnels {
                group.enter()
                tunnel.removeFromPreferences { error in
                    if let error = error {
                        NSLog("tunnel.removeFromPreferences ERROR", error.localizedDescription)
                    }
                    group.leave()
                }
            }

            group.notify(queue: .main) {
                NSLog("remove all old Preferences finished!")
                strongSelf.createTunnel()
            }
     
        }
    }
    
    private func createTunnel () {
        let tunnel = makeManager()
        
        tunnel.saveToPreferences { error in
           
            if let error = error {
                NSLog("createTunnel ERROR ", error.localizedDescription)
                return
            }
            
            
            tunnel.loadFromPreferences { error in
                if let error = error {
                    NSLog("tunnel.loadFromPreferences ERROR ", error.localizedDescription)
                    return
                }
                
                print("createTunnel success!")
                do {
                    try tunnel.connection.startVPNTunnel()
                } catch {
                    NSLog("Failed to start VPN tunnel:", error.localizedDescription)
                }
                
                NSLog("startVPNTunnel success")
            }
        }
    }
    
    private func makeManager() -> NETunnelProviderManager {
        let manager = NETunnelProviderManager()
        manager.localizedDescription = "fx168"
        self.localServer?.stop()
        self.layerMinus.miningProcess?.keep = false
        self.layerMinus.miningProcess?.stop(false)
        let proto = NETunnelProviderProtocol()
        let VirtualIP = "127.0.0.1"
        // WARNING: This must send the actual VPN server address, for the demo
        // purposes, I'm passing the address of the server in my local network.
        // The address is going to be different in your network.
        proto.serverAddress = VirtualIP

 
        if #available(iOS 14.2, *) {
            proto.includeAllNetworks = false
            
            proto.excludeLocalNetworks = true
        } else {
            // Fallback on earlier versions
        }
        
        // WARNING: This must match the bundle identifier of the app extension
        // containing packet tunnel provider.
        proto.providerBundleIdentifier = "com.fx168.CoNETVPN1.CoNETVPN1.VPN"
        
        
        manager.protocolConfiguration = proto

        manager.isEnabled = true

        return manager
    }
    
    
    
    func startBackgroundTask() {
            backgroundTask = UIApplication.shared.beginBackgroundTask { [weak self] in
                // 这里做清理工作
                self?.endBackgroundTask()
            }
            
            // 执行后台任务
            DispatchQueue.global().async {
                // 模拟耗时操作
                for _ in 0..<10 {
                    print("Doing some work in the background")
                    Thread.sleep(forTimeInterval: 1)
                }
                
                // 完成后结束后台任务
                self.endBackgroundTask()
            }
        }
        
        func endBackgroundTask() {
            UIApplication.shared.endBackgroundTask(backgroundTask)
            backgroundTask = .invalid
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
    
    func startTimer() {
//            timer = Timer.scheduledTimer(timeInterval: 2.0, target: self, selector: #selector(getRegionButtonClick), userInfo: nil, repeats: true)
        }
    deinit {
            // 确保在视图控制器被释放时取消定时器
//            timer?.invalidate()
        NotificationCenter.default.removeObserver(self)
        }
}

