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
                self.fetchFromServer(url: finalURL, for: urlSchemeTask)
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
    private func fetchFromServer(url: URL, for urlSchemeTask: WKURLSchemeTask) {
        let task = URLSession.shared.dataTask(with: url) { [weak self] data, response, error in
            if let error = error {
                urlSchemeTask.didFailWithError(error)
                return
            }
            guard let response = response, let data = data else {
                urlSchemeTask.didFailWithError(URLError(.unknown))
                return
            }

            // Serve the fresh data to the webview
            urlSchemeTask.didReceive(response)
            urlSchemeTask.didReceive(data)
            urlSchemeTask.didFinish()

            // Update the cache in the background
            print("🗄️ Caching response for \(url.absoluteString)")

        }
        task.resume()
    }
}
