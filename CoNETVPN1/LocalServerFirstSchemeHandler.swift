//
//  LocalServerFirstSchemeHandler.swift
//  CoNETVPN1
//
//  A scheme handler that routes local-first:// requests to the local HTTP server
//

import WebKit
import Foundation

class LocalServerFirstSchemeHandler: NSObject, WKURLSchemeHandler {
    
    // Track active tasks and their state
    private var activeTasks = [ObjectIdentifier: URLSessionDataTask]()
    private var taskStates = [ObjectIdentifier: TaskState]()
    private let taskQueue = DispatchQueue(label: "com.conet.schemehandler", attributes: .concurrent)
    
    private enum TaskState {
        case running
        case stopping
        case stopped
    }
    
    func webView(_ webView: WKWebView, start urlSchemeTask: WKURLSchemeTask) {
        let taskId = ObjectIdentifier(urlSchemeTask)
        
        // Check if task is already being handled
        var isAlreadyHandled = false
        taskQueue.sync {
            if let state = taskStates[taskId] {
                isAlreadyHandled = (state != .stopped)
            }
        }
        
        if isAlreadyHandled {
            print("⚠️ [SchemeHandler] Task already being handled, ignoring duplicate")
            return
        }
        
        // Mark task as running
        taskQueue.async(flags: .barrier) {
            self.taskStates[taskId] = .running
        }
        
        guard let url = urlSchemeTask.request.url else {
            completeTask(urlSchemeTask, error: URLError(.badURL))
            return
        }
        
        // Convert local-first://localhost:3001/path to http://127.0.0.1:3001/path
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            completeTask(urlSchemeTask, error: URLError(.badURL))
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
            completeTask(urlSchemeTask, error: URLError(.badURL))
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
            
            // Check if task was stopped
            var shouldProcess = false
            self.taskQueue.sync {
                if let state = self.taskStates[taskId] {
                    shouldProcess = (state == .running)
                }
            }
            
            guard shouldProcess else {
                print("⚠️ [SchemeHandler] Task was stopped, not processing response")
                return
            }
            
            // Handle error
            if let error = error {
                print("❌ [SchemeHandler] Failed to load: \(httpURL.absoluteString), error: \(error.localizedDescription)")
                
                // Check if it's a connection error and provide fallback content
                let nsError = error as NSError
                if nsError.code == NSURLErrorCannotConnectToHost ||
                   nsError.code == NSURLErrorTimedOut {
                    // Provide a fallback loading page
                    self.provideFallbackContent(for: urlSchemeTask, taskId: taskId)
                } else if nsError.code != NSURLErrorCancelled {
                    self.completeTask(urlSchemeTask, error: error)
                }
                return
            }
            
            // Handle successful response
            guard let data = data, let response = response else {
                self.completeTask(urlSchemeTask, error: URLError(.cannotLoadFromNetwork))
                return
            }
            
            print("✅ [SchemeHandler] Successfully loaded: \(httpURL.absoluteString)")
            
            // Send response and data to WebView
            self.completeTask(urlSchemeTask, response: response, data: data)
        }
        
        // Store task for potential cancellation
        taskQueue.async(flags: .barrier) {
            self.activeTasks[taskId] = dataTask
        }
        
        // Start the task
        dataTask.resume()
    }
    
    func webView(_ webView: WKWebView, stop urlSchemeTask: WKURLSchemeTask) {
        let taskId = ObjectIdentifier(urlSchemeTask)
        
        print("🛑 [SchemeHandler] Stopping task for: \(urlSchemeTask.request.url?.absoluteString ?? "unknown")")
        
        // Cancel the corresponding URLSession task
        taskQueue.async(flags: .barrier) {
            // Mark as stopping
            self.taskStates[taskId] = .stopping
            
            // Cancel the data task
            if let task = self.activeTasks[taskId] {
                task.cancel()
                self.activeTasks.removeValue(forKey: taskId)
            }
            
            // Mark as stopped
            self.taskStates[taskId] = .stopped
        }
    }
    
    private func completeTask(_ urlSchemeTask: WKURLSchemeTask, response: URLResponse? = nil, data: Data? = nil, error: Error? = nil) {
        let taskId = ObjectIdentifier(urlSchemeTask)
        
        // Check if task is still running
        var shouldComplete = false
        taskQueue.sync {
            if let state = taskStates[taskId] {
                shouldComplete = (state == .running)
            }
        }
        
        guard shouldComplete else {
            print("⚠️ [SchemeHandler] Task already stopped, not completing")
            return
        }
        
        // Complete the task
        DispatchQueue.main.async { [weak self] in
            guard let self = self else { return }
            
            // Double-check state before completing
            var canComplete = false
            self.taskQueue.sync {
                if let state = self.taskStates[taskId] {
                    canComplete = (state == .running)
                }
            }
            
            guard canComplete else { return }
            
            do {
                if let error = error {
                    urlSchemeTask.didFailWithError(error)
                } else if let response = response, let data = data {
                    urlSchemeTask.didReceive(response)
                    urlSchemeTask.didReceive(data)
                    urlSchemeTask.didFinish()
                } else {
                    urlSchemeTask.didFailWithError(URLError(.unknown))
                }
            } catch {
                print("⚠️ [SchemeHandler] Error completing task: \(error)")
            }
            
            // Clean up
            self.taskQueue.async(flags: .barrier) {
                self.taskStates[taskId] = .stopped
                self.activeTasks.removeValue(forKey: taskId)
            }
        }
    }
    
    // Provide fallback content when local server is not ready
    private func provideFallbackContent(for urlSchemeTask: WKURLSchemeTask, taskId: ObjectIdentifier) {
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
            completeTask(urlSchemeTask, error: URLError(.cannotLoadFromNetwork))
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
        
        completeTask(urlSchemeTask, response: response, data: data)
    }
}
