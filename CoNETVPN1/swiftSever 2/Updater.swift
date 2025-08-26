import Foundation
import ZIPFoundation // 导入第三方解压库
import Web3Core
import web3swift

// MARK: - Data Models (Codable)
// 用于解析 update.json
struct UpdateInfo: Codable {
    let ver: String
    let filename: String
}

// 用于解析 asset-manifest.json
struct AssetManifest: Codable {
    let files: [String: String]
}

// 假设 Node 结构体已在别处定义
// struct Node: Codable {
//     let ip_addr: String
// }


class Updater {
    init (){
//        initAllnodeInfoABI()
    }
    func initAllnodeInfoABI () {
        if let jsSourcePath = Bundle.main.path(forResource: "CONET_Guardian_NodeInfo_ABI", ofType: "text") {
            do {
                let _resData = Data(hex:"000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003e8")
                self.CONET_Guardian_NodeInfo_ABI = try String(contentsOfFile: jsSourcePath)
                self.CONET_Guardian_NodeInfo_Contract = try EthereumContract(self.CONET_Guardian_NodeInfo_ABI, at: nil)
                let decodedData = try self.CONET_Guardian_NodeInfo_Contract.decodeReturnData("getNodeInfoById", data: _resData)
                guard let returnData = decodedData["returnData"] as? [[Any]] else {
                    throw Web3Error.dataError
                }
                NSLog("returnData")
            } catch {
                print("readCoNET_nodeInfoABI Error")
            }
        }
    }
    private let fileManager = FileManager.default
    private let jsonDecoder = JSONDecoder()
    private let urlSession = URLSession.shared
    var CONET_Guardian_NodeInfo_ABI: String!
    var CONET_Guardian_NodeInfo_Contract: EthereumContract!

    /// 运行更新程序。
    /// 该方法会自动从本地 'workers/update.json' 读取当前版本，
    /// 然后与远程节点比较，决定是否需要下载和应用更新。
    /// 成功更新后，会将新内容覆盖到 'workers' 目录。
    ///
    /// - Parameter nodes: 节点服务器列表，用于获取更新信息。
    /// - Returns: 如果更新成功，返回 `true`；如果无需更新或更新失败，返回 `false`。
    func runUpdater(nodes: [Node]) async -> Bool {
        // 获取关键目录路径
        guard let documentsDirectory = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            NSLog("❌ Updater Error: Could not find documents directory.")
            return false
        }
        let workersDir = documentsDirectory.appendingPathComponent("workers")
        let tempUpdatePath = fileManager.temporaryDirectory.appendingPathComponent("conet-update-\(Date().timeIntervalSince1970)")

        // 使用 defer 确保临时目录在函数退出时被清理
        defer {
            try? fileManager.removeItem(at: tempUpdatePath)
        }

        do {
            NSLog("🚀 开始执行动态节点更新程序...")

            // --- 1. 从本地文件系统获取当前版本 ---
            let localUpdateJsonURL = workersDir.appendingPathComponent("update.json")
            var currentVer = "0.0.0"
            if fileManager.fileExists(atPath: localUpdateJsonURL.path) {
                do {
                    let data = try Data(contentsOf: localUpdateJsonURL)
                    let localInfo = try jsonDecoder.decode(UpdateInfo.self, from: data)
                    currentVer = localInfo.ver
                } catch {
                    NSLog("⚠️ 读取或解析本地 update.json 失败，将版本视为 0.0.0. Error: \(error)")
                }
            } else {
                NSLog("ℹ️ 本地 update.json 不存在，将版本视为 0.0.0")
            }
            NSLog("✅ 检测到当前本地版本为: \(currentVer)")

            // --- 2. 获取远程版本信息并比较 ---
            guard let selectedNode = nodes.randomElement() else { throw URLError(.badURL, reason: "节点列表为空") }
            NSLog("✅ 节点列表获取成功！已随机选择节点: \(selectedNode.ip_addr)")
            let baseApiUrl = "http://\(selectedNode.ip_addr)/silentpass-rpc/"
            guard let remoteUpdateURL = URL(string: "\(baseApiUrl)update.json") else { throw URLError(.badURL, reason: "无效的远程 update.json URL") }
            
            let remoteInfo = try await fetchUpdateInfo(from: remoteUpdateURL)
            NSLog("✅ 获取远程信息成功！最新版本: \(remoteInfo.ver)")

            if !isNewerVersion(oldVer: currentVer, newVer: remoteInfo.ver) {
                NSLog("ℹ️ 当前已是最新版本 (\(currentVer))，无需更新。")
                return false // 无需更新
            }
            
            NSLog("🆕 发现新版本 \(remoteInfo.ver)，准备更新...")

            // --- 3. 核心下载、解压和验证逻辑 ---
            try fileManager.createDirectory(at: tempUpdatePath, withIntermediateDirectories: true)
            NSLog("创建临时更新目录: \(tempUpdatePath.path)")
            
            guard let downloadUrl = URL(string: "\(baseApiUrl)\(remoteInfo.filename)") else { throw URLError(.badURL, reason: "无效的下载 URL") }
            
            NSLog("⏳ 正在从 \(downloadUrl) 下载并解压到临时目录...")
            try await downloadAndUnzip(from: downloadUrl, to: tempUpdatePath)
            NSLog("🎉 成功解压到临时目录！")

            let isValid = try await validateAndRepairContents(in: tempUpdatePath, nodes: nodes, baseUrl: baseApiUrl)
            if !isValid {
                throw URLError(.cancelled, reason: "下载的内容无效或修复失败，已终止更新。")
            }

            // --- 4. 覆盖更新逻辑 ---
            NSLog("🔄 准备使用新内容覆盖工作目录...")
            // 如果旧目录存在，先移除
            if fileManager.fileExists(atPath: workersDir.path) {
                try fileManager.removeItem(at: workersDir)
            }
            // 将临时目录的内容移动（或复制）到工作目录
            try fileManager.copyItem(at: tempUpdatePath, to: workersDir)
            NSLog("✅ 更新成功！工作目录已全部替换为新版本内容。")

            return true // 更新成功

        } catch {
            NSLog("❌ 更新过程中发生错误: \(error.localizedDescription)")
            // 打印更详细的错误信息
            if let urlError = error as? URLError {
                // --- 修正点: 使用 localizedDescription 代替不存在的 reason 属性 ---
                NSLog("   - URL Error Details: \(urlError.failureURLString ?? "N/A"), Reason: \(urlError.localizedDescription)")
            }
            return false // 更新失败
        }
    }

    // MARK: - Private Helper Methods

    /// 验证并修复文件夹内容。
    private func validateAndRepairContents(in folderPath: URL, nodes: [Node], baseUrl: String) async throws -> Bool {
        NSLog("🔍 开始验证和修复更新内容...")
        let manifestURL = folderPath.appendingPathComponent("asset-manifest.json")

        // 1. 检查并读取 asset-manifest.json，如果不存在则先下载它
        if !fileManager.fileExists(atPath: manifestURL.path) {
            NSLog("⚠️ 关键文件 asset-manifest.json 未找到！尝试下载...")
            guard let downloadURL = URL(string: "\(baseUrl)asset-manifest.json") else {
                throw URLError(.badURL, reason: "无效的 manifest URL")
            }
            try await downloadSingleFile(from: downloadURL, to: manifestURL)
        }

        let manifestData = try Data(contentsOf: manifestURL)
        let manifest = try jsonDecoder.decode(AssetManifest.self, from: manifestData)

        // 2. 使用 TaskGroup 并行检查和修复所有缺失的文件
        return try await withThrowingTaskGroup(of: Bool.self, returning: Bool.self) { group in
            var missingFileCount = 0
            for (_, filePath) in manifest.files {
                // filePath 可能是 "/static/js/main.123.js"，需要移除开头的 "/"
                let relativePath = filePath.hasPrefix("/") ? String(filePath.dropFirst()) : filePath
                let localFileUrl = folderPath.appendingPathComponent(relativePath)
                
                if !fileManager.fileExists(atPath: localFileUrl.path) {
                    missingFileCount += 1
                    NSLog("🟡 文件缺失: \(relativePath)。准备下载...")
                    
                    // 为每个缺失的文件添加一个下载任务到组中
                    group.addTask {
                        guard let randomNode = nodes.randomElement() else { return false }
                        let downloadBaseUrl = "http://\(randomNode.ip_addr)/silentpass-rpc/"
                        guard let downloadURL = URL(string: "\(downloadBaseUrl)\(relativePath)") else { return false }
                        
                        try await self.downloadSingleFile(from: downloadURL, to: localFileUrl)
                        return true
                    }
                }
            }

            if missingFileCount > 0 {
                NSLog("发现 \(missingFileCount) 个缺失文件，开始并行下载修复...")
            }

            // 等待所有下载任务完成。如果任何一个任务抛出错误，整个 group 都会抛出错误。
            for try await _ in group {}
            
            NSLog("✅ 更新内容验证和修复成功！")
            return true
        }
    }
    
    /// 版本号比较
    private func isNewerVersion(oldVer: String, newVer: String) -> Bool {
        let oldParts = oldVer.split(separator: ".").map { Int($0) ?? 0 }
        let newParts = newVer.split(separator: ".").map { Int($0) ?? 0 }
        let maxCount = max(oldParts.count, newParts.count)
        
        for i in 0..<maxCount {
            let oldPart = i < oldParts.count ? oldParts[i] : 0
            let newPart = i < newParts.count ? newParts[i] : 0
            if newPart > oldPart { return true }
            if newPart < oldPart { return false }
        }
        return false
    }

    /// 下载单个文件并保存到指定路径。
    private func downloadSingleFile(from url: URL, to destinationURL: URL) async throws {
        NSLog("  下载中: \(url.lastPathComponent) -> \(destinationURL.lastPathComponent)")
        
        var request = URLRequest(url: url)
        request.setValue("SP-iOS", forHTTPHeaderField: "Referer")
        
        let (tempURL, response) = try await urlSession.download(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            throw URLError(.badServerResponse)
        }
        
        // 确保目标目录存在
        let destinationDir = destinationURL.deletingLastPathComponent()
        try fileManager.createDirectory(at: destinationDir, withIntermediateDirectories: true)
        
        if fileManager.fileExists(atPath: destinationURL.path) {
            try fileManager.removeItem(at: destinationURL)
        }
        
        try fileManager.moveItem(at: tempURL, to: destinationURL)
    }

    /// 获取并解析 update.json
    private func fetchUpdateInfo(from url: URL) async throws -> UpdateInfo {
        var request = URLRequest(url: url)
        request.setValue("SP-iOS", forHTTPHeaderField: "Referer")
        
        let (data, response) = try await urlSession.data(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            throw URLError(.badServerResponse)
        }
        return try jsonDecoder.decode(UpdateInfo.self, from: data)
    }

    /// 下载并解压 ZIP 文件。
    private func downloadAndUnzip(from url: URL, to destinationDir: URL) async throws {
        var request = URLRequest(url: url)
        request.setValue("SP-iOS", forHTTPHeaderField: "Referer")
        
        let (tempZipURL, response) = try await urlSession.download(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) else {
            throw URLError(.badServerResponse)
        }
        
        try fileManager.unzipItem(at: tempZipURL, to: destinationDir)
    }
}

// 扩展 URLError 以便创建自定义错误
extension URLError {
    init(_ code: Code, reason: String) {
        self.init(code, userInfo: [NSLocalizedDescriptionKey: reason])
    }
}
