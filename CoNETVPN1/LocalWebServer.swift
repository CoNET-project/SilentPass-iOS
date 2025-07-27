import Foundation
import Swifter // 导入 Web 服务器库
import ZIPFoundation // 导入 ZIP 解压库

// MARK: - Codable Structs for JSON


// 用于构建 /var 接口的返回体
fileprivate struct VersionResponse: Codable {
    let ver: String
}

extension Notification.Name {
    static let webServerDidStart = Notification.Name("webServerDidStart")
}

class LocalWebServer {
    // 使用 public 访问控制
     let server = HttpServer()
    private let port: UInt16
    private let fileManager = FileManager.default

    private var rootDir: URL?
    private let workersDir: URL
    var index: Int = 0

    init(port: UInt16 = 3001) {
        self.port = port
        guard let documentsDirectory = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            fatalError("Could not determine documents directory.")
        }
        self.workersDir = documentsDirectory.appendingPathComponent("workers")
    }

    func prepareAndStart() async {
//        stop() // 确保先停止现有服务器

        do {
            print("🚀 准备启动本地服务器...")
            try await prepareRootDirectory()
            configureRoutes()
            try server.start(port, forceIPv4: true)
            print("✅ 本地服务器启动于 http://127.0.0.1:\(port)")
            if let rootPath = rootDir?.path {
                print("📁 当前服务目录: \(rootPath)")
            }
            
            index += 1
            if index == 1
            {
                // 👉 通知其他地方服务器已启动，并传递端口号
                        NotificationCenter.default.post(name: .webServerDidStart,
                                                        object: nil,
                                                        userInfo: ["port": port])
                
                
            }
            
            
        } catch {
            print("❌ 启动服务器失败: \(error.localizedDescription)")
        }
    }

    func stop() {
        server.stop()
        print("🛑 本地服务器已停止。")
    }

    private func prepareRootDirectory() async throws {
        let indexFile = workersDir.appendingPathComponent("index.html")
        if fileManager.fileExists(atPath: indexFile.path) {
            print("ℹ️ 发现有效的工作目录，直接使用: \(workersDir.path)")
            self.rootDir = workersDir
        } else {
            print("ℹ️ 未找到有效的工作目录，从 App Bundle/build3.zip 解压初始内容...")
            if fileManager.fileExists(atPath: workersDir.path) {
                try fileManager.removeItem(at: workersDir)
            }
            guard let zipPath = Bundle.main.url(forResource: "build3", withExtension: "zip") else {
                throw NSError(domain: "LocalWebServerError", code: 404, userInfo: [NSLocalizedDescriptionKey: "build3.zip not found in app bundle."])
            }
            try fileManager.createDirectory(at: workersDir, withIntermediateDirectories: true)
            try fileManager.unzipItem(at: zipPath, to: workersDir)
            self.rootDir = workersDir
            print("✅ 初始内容解压成功到: \(workersDir.path)")
        }
    }

    private func configureRoutes() {
        guard let rootDir = self.rootDir else {
            print("❌ 无法配置路由，因为 rootDir 未设置。")
            return
        }

        // --- 规则 1: 处理 /var GET 请求 (最具体的) ---
        server.get["/ver"] = { [weak self] _ in
            guard let self = self else { return .internalServerError }
            return self.handleVarRequest(rootDir: rootDir)
        }
        
        // --- 规则 2: 使用 notFoundHandler 作为文件服务的 "全捕获" 路由 ---
        // 这是最健壮的方式，可以避免所有路由优先级和正则表达式问题。
        server.notFoundHandler = { [weak self] request in
            guard let self = self else { return .internalServerError }
            
            var path = request.path
            // 如果请求是根路径，则将其映射到 index.html
            if path == "/" {
                path = "/index.html"
            }
            
            // 从路径中移除开头的 "/" 来获取相对路径
            let relativePath = path.hasPrefix("/") ? String(path.dropFirst()) : path
            
            let fileURL = rootDir.appendingPathComponent(relativePath)
            return self.serveFile(at: fileURL)
        }
    }

    // MARK: - Route Handlers

    private func handleVarRequest(rootDir: URL) -> HttpResponse {
        let updateJsonURL = rootDir.appendingPathComponent("update.json")
        guard fileManager.fileExists(atPath: updateJsonURL.path) else {
            return createJsonResponse(statusCode: 404, body: ["error": "update.json not found"])
        }
        do {
            let data = try Data(contentsOf: updateJsonURL)
            let updateInfo = try JSONDecoder().decode(UpdateInfo.self, from: data)
            let versionResponse = VersionResponse(ver: updateInfo.ver)
            return createJsonResponse(statusCode: 200, body: versionResponse)
        } catch {
            return createJsonResponse(statusCode: 500, body: ["error": "Failed to read or parse update.json: \(error.localizedDescription)"])
        }
    }
    
    // --- 统一的文件服务函数 ---
    private func serveFile(at absoluteURL: URL) -> HttpResponse {
        print("--- [File Request] ---")
        print("  尝试提供文件: \(absoluteURL.path)")
        
        // 安全性检查
        guard let rootPath = self.rootDir?.path, absoluteURL.path.hasPrefix(rootPath) else {
            print("  [结果] ❌ 禁止访问 (路径越界)")
            return .forbidden
        }
        
        guard fileManager.fileExists(atPath: absoluteURL.path) else {
            print("  [结果] ❌ 404 Not Found")
            return .notFound
        }
        
        guard let fileHandle = try? FileHandle(forReadingFrom: absoluteURL) else {
            print("  [结果] ❌ 500 Server Error (无法打开文件)")
            return .internalServerError
        }
        
        print("  [结果] ✅ 200 OK (文件找到)")
        
        let mime = mimeType(for: absoluteURL.pathExtension)
        var headers = [String: String]()
        headers["Content-Type"] = mime
        addCorsHeaders(toRawHeaders: &headers)

        return .raw(200, "OK", headers, { writer in
            defer { try? fileHandle.close() }
            do {
                while true {
                    let data = fileHandle.readData(ofLength: 4096)
                    if data.isEmpty { break }
                    try writer.write(data)
                }
            } catch {
                print("❌ 写入文件到响应时出错: \(error)")
            }
        })
    }

    // MARK: - Helper Methods

    private func createJsonResponse<T: Codable>(statusCode: Int, body: T) -> HttpResponse {
        do {
            let data = try JSONEncoder().encode(body)
            var headers = [String: String]()
            headers["Content-Type"] = "application/json"
            addCorsHeaders(toRawHeaders: &headers)
            return .raw(statusCode, "OK", headers, { writer in try? writer.write(data) })
        } catch {
            return .internalServerError
        }
    }
    
    private func addCorsHeaders(toRawHeaders headers: inout [String: String]) {
        headers["Access-Control-Allow-Origin"] = "*"
        headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        headers["Access-Control-Allow-Headers"] = "Origin, Content-Type, Accept"
    }
    
    private func mimeType(for pathExtension: String) -> String {
        switch pathExtension.lowercased() {
            case "html": return "text/html"
            case "css": return "text/css"
            case "js": return "application/javascript"
            case "json": return "application/json"
            case "png": return "image/png"
            case "jpg", "jpeg": return "image/jpeg"
            case "gif": return "image/gif"
            case "svg": return "image/svg+xml"
            case "ico": return "image/x-icon"
            default: return "application/octet-stream"
        }
    }
}
