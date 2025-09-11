//
//  LocalServerFirstSchemeHandler.swift
//  CoNETVPN1
//
//  Created by peter on 2025-07-17.
//

import WebKit

class LocalServerFirstSchemeHandler: NSObject, WKURLSchemeHandler {

    func webView(_ webView: WKWebView, start urlSchemeTask: WKURLSchemeTask) {
        guard let requestURL = urlSchemeTask.request.url else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }

        // Convert "local-first://localhost:3001/path" to "http://localhost:3001/path"
        guard var components = URLComponents(url: requestURL, resolvingAgainstBaseURL: false) else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }
        components.scheme = "http" // Or "https" if your local server uses it
		components.host = "127.0.0.1"
		if (components.path.isEmpty) { components.path = "/" } 

        guard let finalURL = components.url else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }

        // --- The Core Logic: Check if the server is online ---
        isServerOnline(url: finalURL) { [weak self] isOnline in
            guard let self = self else { return }

            if isOnline {
                // SERVER IS ONLINE: Fetch from server and update cache
                print("🟢 Server is Online. Fetching from localhost...")
                self.fetchFromServer(url: finalURL, originalURL: requestURL, for: urlSchemeTask)
            } else {
                // SERVER IS OFFLINE: Serve from cache
                print("🔴 Server is Offline. Attempting to serve from cache...")
            }
        }
    }

    func webView(_ webView: WKWebView, stop urlSchemeTask: WKURLSchemeTask) {
        // Here you could cancel any ongoing URLSession tasks if necessary
    }

    // MARK: - Helper Methods

    /// 1. Pings the server with a quick HEAD request to check for availability.
    private func isServerOnline(url: URL, completion: @escaping (Bool) -> Void) {
        var request = URLRequest(url: url)
        request.httpMethod = "HEAD" // A HEAD request is lightweight; we only need status.
        request.timeoutInterval = 1.0 // Use a very short timeout.

        let task = URLSession.shared.dataTask(with: request) { _, response, error in
            // The server is considered "online" if we get any kind of response,
            // even an HTTP error (like 404), and there's no network-level error.
            if let error = error as? URLError, error.code == .cannotConnectToHost || error.code == .timedOut {
                completion(false) // Connection refused or timed out -> Server is offline.
            } else {
                completion(true) // Any other case (success, http error) -> Server is online.
            }
        }
        task.resume()
    }

    /// 2. Fetches from the server, serves the data, and caches it for later.
    private func fetchFromServer(url: URL, originalURL: URL, for urlSchemeTask: WKURLSchemeTask) {
        let task = URLSession.shared.dataTask(with: url) { data, response, error in
        if let error = error {
            // 离线或拉取失败时，返回一段最简 HTML，避免白屏无回调
            let html = "<html><body><h3>Local server fetch failed</h3><p>\(error.localizedDescription)</p></body></html>"
            let d = Data(html.utf8)
            let resp = URLResponse(
                url: originalURL,                   // 关键：用 local-first 原始 URL
                mimeType: "text/html",
                expectedContentLength: d.count,
                textEncodingName: "utf-8"
            )
            urlSchemeTask.didReceive(resp)
            urlSchemeTask.didReceive(d)
            urlSchemeTask.didFinish()
            return
        }
        guard let http = response as? HTTPURLResponse, let data = data else {
            urlSchemeTask.didFailWithError(URLError(.unknown))
            return
        }

        // 用 http 响应的 mimeType/编码，但 URL 必须是 originalURL（local-first）
        let resp = URLResponse(
            url: originalURL,                       // 关键：保持自定义 scheme
            mimeType: http.mimeType ?? "application/octet-stream",
            expectedContentLength: data.count,
            textEncodingName: http.textEncodingName
        )
        urlSchemeTask.didReceive(resp)
        urlSchemeTask.didReceive(data)
        urlSchemeTask.didFinish()
    }
    task.resume()
    }
}
