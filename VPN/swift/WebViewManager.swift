//
//  WebViewManager.swift
//  tq-proxy-ios
//
//  Created by 杨旭的MacBook Pro on 2024/11/20.
//


import WebKit

class WebViewManager {
    static let shared = WebViewManager()
    var layerMinus: LayerMinus!
    private init() {
     
        
        // 可以添加其他配置选项，例如 JavaScript 设置等
    }

//    func layerMinusInit() {
//        self.layerMinus = LayerMinus()
//        self.layerMinus.start()
//    }
}
