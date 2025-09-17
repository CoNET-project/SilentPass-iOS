import Foundation
import Swifter
import ZIPFoundation

// MARK: - Custom Notification
extension Notification.Name {
    static let webServerDidStart = Notification.Name("webServerDidStart")
}

// MARK: - Version Response for /ver endpoint
fileprivate struct VersionResponse: Codable {
    let ver: String
}

// MARK: - Local UpdateInfo if not defined elsewhere
// Comment this out if UpdateInfo is already defined in your project
fileprivate struct LocalUpdateInfo: Codable {
    let ver: String
}

class LocalWebServer {
    let server = HttpServer()
    private let port: UInt16
    private let fileManager = FileManager.default
    private var starting = false
    private var rootDir: URL?
    private let workersDir: URL
    var index: Int = 0
    /// 从外部（例如 ViewController）注入：返回当前 VPN 是否“已连通”
    // 建议：NEVPNStatus == .connected 或 .reasserting 视为 true
    var vpnStatusProvider: (() -> Bool)?
    
    init(port: UInt16 = 3001) {
        self.port = port
        guard let documentsDirectory = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            fatalError("Could not determine documents directory.")
        }
        self.workersDir = documentsDirectory.appendingPathComponent("workers")
    }
    
    func prepareAndStart() async {
        // If already running, just broadcast and return
        if server.state == .running {
            print("✅ Server already running on port \(port)")
            broadcastServerStarted()
            return
        }
        
        // Prevent concurrent starts
        guard !starting else {
            print("⏳ Server already starting, skipping duplicate request")
            return
        }
        starting = true
        defer { starting = false }
        
        do {
            print("🚀 Preparing to start local server...")
            
            // Prepare root directory
            try await prepareRootDirectory()
            
            // Configure routes
            configureRoutes()
            
            // Start server
            try server.start(port, forceIPv4: true)
            
            print("✅ Local server started at http://127.0.0.1:\(port)")
            if let rootPath = rootDir?.path {
                print("📁 Serving files from: \(rootPath)")
            }
            
            // Broadcast server started
            broadcastServerStarted()
            
        } catch {
            print("❌ Failed to start server: \(error.localizedDescription)")
            
            // If it's a port binding error, try to stop and restart
            if (error as NSError).code == 48 { // Address already in use
                print("🔄 Port \(port) already in use, attempting restart...")
                server.stop()
                
                // Wait a moment and try again
                try? await Task.sleep(nanoseconds: 500_000_000) // 0.5 seconds
                await prepareAndStart()
            }
        }
    }
    
    func stop() {
        server.stop()
        print("🛑 Local server stopped.")
    }
    
    private func broadcastServerStarted() {
        DispatchQueue.main.async {
            NotificationCenter.default.post(
                name: .webServerDidStart,
                object: nil,
                userInfo: ["port": self.port]
            )
        }
    }
    
    private func prepareRootDirectory() async throws {
        let indexFile = workersDir.appendingPathComponent("index.html")
        
        if fileManager.fileExists(atPath: indexFile.path) {
            print("ℹ️ Found existing workers directory: \(workersDir.path)")
            self.rootDir = workersDir
        } else {
            print("ℹ️ Workers directory not found, extracting from build3.zip...")
            
            // Remove old directory if exists
            if fileManager.fileExists(atPath: workersDir.path) {
                try fileManager.removeItem(at: workersDir)
            }
            
            // Find build3.zip in bundle
            guard let zipPath = Bundle.main.url(forResource: "build3", withExtension: "zip") else {
                throw NSError(
                    domain: "LocalWebServerError",
                    code: 404,
                    userInfo: [NSLocalizedDescriptionKey: "build3.zip not found in app bundle."]
                )
            }
            
            // Create directory and extract
            try fileManager.createDirectory(at: workersDir, withIntermediateDirectories: true)
            try fileManager.unzipItem(at: zipPath, to: workersDir)
            
            self.rootDir = workersDir
            print("✅ Successfully extracted content to: \(workersDir.path)")
        }
    }
    
    private func configureRoutes() {
        guard let rootDir = self.rootDir else {
            print("❌ Cannot configure routes: rootDir not set")
            return
        }
        
        // Handle /ver endpoint
        server.get["/ver"] = { [weak self] _ in
            guard let self = self else { return .internalServerError }
            return self.handleVersionRequest(rootDir: rootDir)
        }
        
        // Handle /iOSVPN endpoint: 回送 { "vpn": true/false }
        server.get["/iOSVPN"] = { [weak self] _ in
            guard let self = self else { return .internalServerError }
                let isOn = self.vpnStatusProvider?() ?? false
                struct VPNResp: Codable { let vpn: Bool }
            print("/iOSVPN \(VPNResp(vpn: isOn))")
                return self.createJsonResponse(statusCode: 200, body: VPNResp(vpn: isOn))
            }
        
        // Handle HEAD requests for root
        server.head["/"] = { _ in
            var headers = [String: String]()
            headers["Cache-Control"] = "no-store"
            headers["Access-Control-Allow-Origin"] = "*"
            return HttpResponse.raw(200, "OK", headers, { _ in })
        }
        
        // Handle OPTIONS requests using a wildcard pattern
        // Swifter doesn't have built-in OPTIONS support, so we use a custom route pattern
        server.handleOPTIONS = { request in
            var headers = [String: String]()
            headers["Access-Control-Allow-Origin"] = "*"
            headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, HEAD"
            headers["Access-Control-Allow-Headers"] = "Origin, Content-Type, Accept, Authorization"
            headers["Access-Control-Max-Age"] = "3600"
            return HttpResponse.raw(200, "OK", headers, { _ in })
        }
        
        // Use notFoundHandler as catch-all for file serving
        server.notFoundHandler = { [weak self] request in
            guard let self = self else { return .internalServerError }
            
            // Handle OPTIONS requests here if handleOPTIONS doesn't work
            if request.method == "OPTIONS" {
                var headers = [String: String]()
                headers["Access-Control-Allow-Origin"] = "*"
                headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, HEAD"
                headers["Access-Control-Allow-Headers"] = "Origin, Content-Type, Accept, Authorization"
                return HttpResponse.raw(200, "OK", headers, { _ in })
            }
            
            var path = request.path
            
            // Default root to index.html
            if path == "/" || path == "" {
                path = "/index.html"
            }
            
            // Remove leading slash for relative path
            let relativePath = path.hasPrefix("/") ? String(path.dropFirst()) : path
            
            // Construct file URL
            let fileURL = rootDir.appendingPathComponent(relativePath)
            
            // Log request
            print("📄 Request: \(request.path) -> \(fileURL.lastPathComponent)")
            
            return self.serveFile(at: fileURL)
        }
    }
    
    private func handleVersionRequest(rootDir: URL) -> HttpResponse {
        let updateJsonURL = rootDir.appendingPathComponent("update.json")
        
        guard fileManager.fileExists(atPath: updateJsonURL.path) else {
            print("❌ update.json not found")
            return createJsonResponse(statusCode: 404, body: ["error": "update.json not found"])
        }
        
        do {
            let data = try Data(contentsOf: updateJsonURL)
            // Try to decode using the local struct
            if let updateInfo = try? JSONDecoder().decode(LocalUpdateInfo.self, from: data) {
                let versionResponse = VersionResponse(ver: updateInfo.ver)
                print("✅ Version request: \(updateInfo.ver)")
                return createJsonResponse(statusCode: 200, body: versionResponse)
            } else {
                // Fallback to a dictionary approach
                if let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let ver = json["ver"] as? String {
                    let versionResponse = VersionResponse(ver: ver)
                    print("✅ Version request: \(ver)")
                    return createJsonResponse(statusCode: 200, body: versionResponse)
                }
                throw NSError(domain: "LocalWebServerError", code: 500, userInfo: [NSLocalizedDescriptionKey: "Invalid update.json format"])
            }
        } catch {
            print("❌ Failed to read update.json: \(error)")
            return createJsonResponse(statusCode: 500, body: ["error": error.localizedDescription])
        }
    }
    
    private func serveFile(at absoluteURL: URL) -> HttpResponse {
        // Security check - ensure file is within rootDir
        guard let rootPath = self.rootDir?.path,
              absoluteURL.path.hasPrefix(rootPath) else {
            print("❌ Security violation: attempting to access file outside root")
            return .forbidden
        }
        
        // Check if file exists
        guard fileManager.fileExists(atPath: absoluteURL.path) else {
            print("❌ File not found: \(absoluteURL.lastPathComponent)")
            return .notFound
        }
        
        // Try to open file
        guard let fileHandle = try? FileHandle(forReadingFrom: absoluteURL) else {
            print("❌ Cannot open file: \(absoluteURL.lastPathComponent)")
            return .internalServerError
        }
        
        print("✅ Serving: \(absoluteURL.lastPathComponent)")
        
        // Determine MIME type
        let mime = mimeType(for: absoluteURL.pathExtension)
        
        // Prepare headers
        var headers = [String: String]()
        headers["Content-Type"] = mime
        headers["Access-Control-Allow-Origin"] = "*"
        headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        
        // Return file content
        return HttpResponse.raw(200, "OK", headers, { writer in
            defer { try? fileHandle.close() }
            
            do {
                // Read and write file in chunks
                while true {
                    let data = fileHandle.readData(ofLength: 65536) // 64KB chunks
                    if data.isEmpty { break }
                    try writer.write(data)
                }
            } catch {
                // Handle broken pipe gracefully
                let nsErr = error as NSError
                if nsErr.domain == NSPOSIXErrorDomain && nsErr.code == EPIPE {
                    // Client disconnected - this is normal
                } else {
                    print("❌ Error writing response: \(error)")
                }
            }
        })
    }
    
    private func createJsonResponse<T: Codable>(statusCode: Int, body: T) -> HttpResponse {
        do {
            let data = try JSONEncoder().encode(body)
            var headers = [String: String]()
            headers["Content-Type"] = "application/json"
            headers["Access-Control-Allow-Origin"] = "*"
            headers["Cache-Control"] = "no-cache"
            
            return HttpResponse.raw(statusCode, "OK", headers, { writer in
                try? writer.write(data)
            })
        } catch {
            return .internalServerError
        }
    }
    
    private func mimeType(for pathExtension: String) -> String {
        switch pathExtension.lowercased() {
        case "html", "htm": return "text/html; charset=utf-8"
        case "css": return "text/css; charset=utf-8"
        case "js", "mjs": return "application/javascript; charset=utf-8"
        case "json": return "application/json; charset=utf-8"
        case "png": return "image/png"
        case "jpg", "jpeg": return "image/jpeg"
        case "gif": return "image/gif"
        case "svg": return "image/svg+xml"
        case "ico": return "image/x-icon"
        case "woff": return "font/woff"
        case "woff2": return "font/woff2"
        case "ttf": return "font/ttf"
        case "otf": return "font/otf"
        case "txt": return "text/plain; charset=utf-8"
        case "xml": return "text/xml; charset=utf-8"
        case "pdf": return "application/pdf"
        case "zip": return "application/zip"
        case "mp3": return "audio/mpeg"
        case "mp4": return "video/mp4"
        case "webm": return "video/webm"
        case "webp": return "image/webp"
        default: return "application/octet-stream"
        }
    }
}

// MARK: - Swifter Extension for OPTIONS if needed
extension HttpServer {
    var handleOPTIONS: ((HttpRequest) -> HttpResponse)? {
        get { return nil }
        set {
            if let handler = newValue {
                // Register a middleware or use the middleware pattern if Swifter supports it
                // Otherwise handle in notFoundHandler
                _ = handler
            }
        }
    }
}