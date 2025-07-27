//
//  OfflineSchemeHandler.swift
//  CoNETVPN1
//
//  Created by peter on 2025-07-17.
//
import WebKit

class OfflineSchemeHandler: NSObject, WKURLSchemeHandler {

    // 1. 当 WKWebView 开始加载一个自定义 scheme 的 URL 时，此方法被调用
    func webView(_ webView: WKWebView, start urlSchemeTask: WKURLSchemeTask) {
        guard let url = urlSchemeTask.request.url else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }

        // 将自定义 scheme URL 转换回原始的 HTTPS URL
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }
        components.scheme = "https" // 将 "offline" 转换回 "https"

        guard let originalURL = components.url else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }

        // 2. 尝试从本地缓存中加载数据
        if let cachedData = CacheManager.shared.getCachedData(for: originalURL) {
            // a. 如果缓存命中，直接返回本地数据
            print("✅ [Cache HIT]: \(originalURL.absoluteString)")
            urlSchemeTask.didReceive(cachedData.response)
            urlSchemeTask.didReceive(cachedData.data)
            urlSchemeTask.didFinish()
        } else {
            // b. 如果缓存未命中，则从网络加载
            print("❌ [Cache MISS]: \(originalURL.absoluteString)")
            let dataTask = URLSession.shared.dataTask(with: originalURL) { data, response, error in
                if let error = error {
                    urlSchemeTask.didFailWithError(error)
                    return
                }

                guard let data = data, let response = response else {
                    urlSchemeTask.didFailWithError(URLError(.cannotLoadFromNetwork))
                    return
                }

                // 3. 将网络数据存入缓存
                CacheManager.shared.cacheData(data, for: response)

                // 4. 将网络数据返回给 WKWebView
                urlSchemeTask.didReceive(response)
                urlSchemeTask.didReceive(data)
                urlSchemeTask.didFinish()
            }
            dataTask.resume()
        }
    }

    // 当 WKWebView 停止加载时调用
    func webView(_ webView: WKWebView, stop urlSchemeTask: WKURLSchemeTask) {
        // 在这里可以取消正在进行的网络请求（如果需要）
    }
}
