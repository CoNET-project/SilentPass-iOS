//
//  LocalServerFirstSchemeHandler.swift
//  CoNETVPN1
//
//  Created by peter on 2025-07-17.
//

import WebKit

private struct WaitPlan {
    static let maxWait: TimeInterval = 10.0   // 调试态子进程慢；可按需调大
    static let firstDelay: TimeInterval = 0.25
}

class LocalServerFirstSchemeHandler: NSObject, WKURLSchemeHandler {


	private let stoppedTasks = NSHashTable<WKURLSchemeTask>.weakObjects()

	private func respondBootstrapRedirect(to task: WKURLSchemeTask, targetURL: URL) {
		// 将目标URL强制为 http://127.0.0.1:port，并保留 path / query / hash
		var comps = URLComponents(url: targetURL, resolvingAgainstBaseURL: false)!
		comps.scheme = "http"                      // 如果你启了 TLS，可换成 "https"
		comps.host = "127.0.0.1"                   // 统一成 127.0.0.1
		let httpURL = comps.url!

		let html = """
		<!doctype html>
		<meta charset="utf-8">
		<title>Loading…</title>
		<style>
		html,body{height:100%;margin:0;background:#000;color:#9aa0a6;
		font:-apple-system,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
		display:flex;align-items:center;justify-content:center}
		</style>
		<p>Local server is ready. Opening…</p>
		<script>
		// 用 replace 不留历史记录；保持 path/search/hash
		location.replace(\(String(reflecting: httpURL.absoluteString)));
		</script>
		"""

		let data = Data(html.utf8)
		let headers = [
			"Content-Type": "text/html; charset=utf-8",
			"Cache-Control": "no-store"
		]
		let resp = HTTPURLResponse(url: task.request.url!, statusCode: 200, httpVersion: "HTTP/1.1", headerFields: headers)!
		task.didReceive(resp)
		task.didReceive(data)
		task.didFinish()
	}

	private func waitUntilOnlineThenFetch(_ url: URL,
                                      for task: WKURLSchemeTask,
                                      deadline: Date,
                                      delay: TimeInterval) {

		if self.stoppedTasks.contains(task) { return }

		isServerOnline(url: url) { [weak self] ok in
			guard let self = self else { return }
			if ok {
				print("🟢 Server online. Respond bootstrap redirect → http")
				self.respondBootstrapRedirect(to: task, targetURL: url)
				return
			}

			// 还没 online：看看是否超时
			// 1) 修复超时分支的语法完整性（在 waitUntilOnlineThenFetch 内）
			if Date() > deadline {
				print("🔴 Server still offline after wait. Failing with .timedOut")
				task.didFailWithError(URLError(.timedOut))
				return
			}


			// 递增回退（最多到 1.5s 左右），减少 busy loop
			let nextDelay = min(delay * 2, 1.5)
			DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
				self.waitUntilOnlineThenFetch(url, for: task, deadline: deadline, delay: nextDelay)
			}
		}
	}

    // NEW: 记录每个 schemeTask 对应的 URLSessionTask，便于 stop 时取消
    private var taskMap = NSMapTable<WKURLSchemeTask, URLSessionTask>(keyOptions: .weakMemory,
                                                                      valueOptions: .strongMemory)

    func webView(_ webView: WKWebView, start urlSchemeTask: WKURLSchemeTask) {
        guard let requestURL = urlSchemeTask.request.url else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }

        // local-first://localhost:3001/path  ->  http://127.0.0.1:3001/path
        guard var components = URLComponents(url: requestURL, resolvingAgainstBaseURL: false) else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }
        components.scheme = "http"
        if components.host == "localhost" { components.host = "127.0.0.1" }

        guard let finalURL = components.url else {
            urlSchemeTask.didFailWithError(URLError(.badURL))
            return
        }


		let deadline = Date().addingTimeInterval(WaitPlan.maxWait)
    	self.waitUntilOnlineThenFetch(finalURL,
                              for: urlSchemeTask,
                              deadline: deadline,
                              delay: WaitPlan.firstDelay)
    }

    func webView(_ webView: WKWebView, stop urlSchemeTask: WKURLSchemeTask) {
        // 标记为已停止，供等待循环及时退出
		self.stoppedTasks.add(urlSchemeTask)

		if let t = taskMap.object(forKey: urlSchemeTask) {
			t.cancel()
			taskMap.removeObject(forKey: urlSchemeTask)
		}
    }

    // MARK: - Helper Methods

    /// 1) HEAD 探活：更宽松的 2.5s
    private func isServerOnline(url: URL, completion: @escaping (Bool) -> Void) {
         // ✅ 固定探活根路径，避免被具体资源状态干扰
		var comps = URLComponents(url: url, resolvingAgainstBaseURL: false)!
		comps.path = "/"
		comps.query = nil
		let healthURL = comps.url!

		var request = URLRequest(url: healthURL)
		request.httpMethod = "HEAD"
		request.timeoutInterval = 0.5
		request.cachePolicy = .reloadIgnoringLocalCacheData
		request.setValue("close", forHTTPHeaderField: "Connection")

		let task = URLSession.shared.dataTask(with: request) { _, response, error in
			if let err = error as? URLError,
			err.code == .cannotConnectToHost || err.code == .timedOut || err.code == .networkConnectionLost {
				completion(false); return
			}
			if let http = response as? HTTPURLResponse, (200...299).contains(http.statusCode) {
				completion(true)
			} else {
				completion(false)
			}
		}
		task.resume()
    }

    /// 2) GET：返回 URLSessionTask，便于 stop 时取消
    @discardableResult
    private func fetchFromServer(url: URL, for urlSchemeTask: WKURLSchemeTask) -> URLSessionTask {
        var request = URLRequest(url: url)
        request.timeoutInterval = 15.0
        request.setValue("close", forHTTPHeaderField: "Connection")

        let task = URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            defer {
                if let self = self { self.taskMap.removeObject(forKey: urlSchemeTask) }
            }

            // 如果是取消，不再回调 WebKit（避免 didFail 后再次回调导致状态错乱）
            if let err = error as NSError?, err.domain == NSURLErrorDomain, err.code == NSURLErrorCancelled {
                return
            }
            if let error = error {
                urlSchemeTask.didFailWithError(error)
                return
            }
            guard let response = response, let data = data else {
                urlSchemeTask.didFailWithError(URLError(.unknown))
                return
            }

            urlSchemeTask.didReceive(response)
            urlSchemeTask.didReceive(data)
            urlSchemeTask.didFinish()
        }
        task.resume()
        return task
    }

}
