//
//  LocalServerFirstSchemeHandler.swift
//  CoNETVPN1
//
//  A scheme handler that routes local-first:// requests to the local HTTP server
//

import WebKit
import Foundation

class LocalServerFirstSchemeHandler: NSObject, WKURLSchemeHandler {
    
    // Track active tasks to handle cancellation
    private var activeTasks = [URLRequest: URLSessionDataTask]()
    private let taskQueue = DispatchQueue(label: "com.conet.schemehandler", attributes: .concurrent)
    
    func webView(_ webView: WKWebView, start urlSchemeTask: WKURLSchemeTask) {
        guard let url = urlSchemeTask.request.url else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }
        
        // Convert local-first://localhost:3001/path to http://127.0.0.1:3001/path
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }
        
        // Change scheme to http
        components.scheme = "http"
        
        // Replace localhost with 127.0.0.1 for better reliability
        if components.host == "localhost" {
            components.host = "127.0.0.1"
        }
        
        // Default to port 3001 if not specified
        if components.port == nil {
            components.port = 3001
        }
        
        guard let httpURL = components.url else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }
        
        print("📱 [SchemeHandler] Converting: \(url.absoluteString) -> \(httpURL.absoluteString)")
        
        // Create request with short timeout for local server
        var request = URLRequest(url: httpURL)
        request.cachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        request.timeoutInterval = 5.0 // Short timeout for local server
        
        // Copy headers from original request
        if let headers = urlSchemeTask.request.allHTTPHeaderFields {
            for (key, value) in headers {
                request.setValue(value, forHTTPHeaderField: key)
            }
        }
        
        // Create URLSession task
        let dataTask = URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            guard let self = self else { return }
            
            // Remove from active tasks
            self.taskQueue.async(flags: .barrier) {
                self.activeTasks.removeValue(forKey: urlSchemeTask.request)
            }
            
            // Handle error
            if let error = error {
                print("❌ [SchemeHandler] Failed to load: \(httpURL.absoluteString), error: \(error.localizedDescription)")
                
                // Check if it's a connection error and provide fallback content
                if (error as NSError).code == NSURLErrorCannotConnectToHost ||
                   (error as NSError).code == NSURLErrorTimedOut {
                    // Provide a fallback loading page
                    self.provideFallbackContent(for: urlSchemeTask)
                } else {
                    urlSchemeTask.didFailWithError(error)
                }
                return
            }
            
            // Handle successful response
            guard let data = data, let response = response else {
                urlSchemeTask.didFailWithError(URLError(.cannotLoadFromNetwork))
                return
            }
            
            print("✅ [SchemeHandler] Successfully loaded: \(httpURL.absoluteString)")
            
            // Send response and data to WebView
            urlSchemeTask.didReceive(response)
            urlSchemeTask.didReceive(data)
            urlSchemeTask.didFinish()
        }
        
        // Store task for potential cancellation
        taskQueue.async(flags: .barrier) {
            self.activeTasks[urlSchemeTask.request] = dataTask
        }
        
        // Start the task
        dataTask.resume()
    }
    
    func webView(_ webView: WKWebView, stop urlSchemeTask: WKURLSchemeTask) {
        print("🛑 [SchemeHandler] Stopping task for: \(urlSchemeTask.request.url?.absoluteString ?? "unknown")")
        
        // Cancel the corresponding URLSession task
        taskQueue.async(flags: .barrier) {
            if let task = self.activeTasks.removeValue(forKey: urlSchemeTask.request) {
                task.cancel()
            }
        }
    }
    
    // Provide fallback content when local server is not ready
    private func provideFallbackContent(for urlSchemeTask: WKURLSchemeTask) {
        let html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Loading...</title>
            <style>
                body {
                    background-color: #000;
                    color: #fff;
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                    padding: 0;
                }
                .container {
                    text-align: center;
                }
                .spinner {
                    border: 3px solid rgba(255, 255, 255, 0.3);
                    border-radius: 50%;
                    border-top: 3px solid white;
                    width: 40px;
                    height: 40px;
                    animation: spin 1s linear infinite;
                    margin: 0 auto 20px;
                }
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
                h1 {
                    font-size: 24px;
                    font-weight: normal;
                    margin: 0;
                }
                p {
                    font-size: 14px;
                    color: #888;
                    margin-top: 10px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="spinner"></div>
                <h1>Loading...</h1>
                <p>Initializing local server</p>
            </div>
            <script>
                // Auto-retry after 1 second
                setTimeout(function() {
                    window.location.reload();
                }, 1000);
            </script>
        </body>
        </html>
        """
        
        guard let data = html.data(using: .utf8) else {
            urlSchemeTask.didFailWithError(URLError(.cannotLoadFromNetwork))
            return
        }
        
        let response = HTTPURLResponse(
            url: urlSchemeTask.request.url!,
            statusCode: 200,
            httpVersion: "HTTP/1.1",
            headerFields: [
                "Content-Type": "text/html; charset=utf-8",
                "Content-Length": String(data.count)
            ]
        )!
        
        urlSchemeTask.didReceive(response)
        urlSchemeTask.didReceive(data)
        urlSchemeTask.didFinish()
    }
}
