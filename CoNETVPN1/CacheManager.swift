//
//  CacheManager.swift
//  CoNETVPN1
//
//  Created by peter on 2025-07-17.
//

import Foundation

// This is the object your WKURLSchemeHandler will receive.
// It contains the reconstructed response and the raw data.
struct CachedData {
    let response: URLResponse
    let data: Data
}

class CacheManager {
    static let shared = CacheManager()
    private let fileManager = FileManager.default
    private let cacheDirectory: URL
    // Use PropertyListEncoder/Decoder for efficient binary encoding.
    private let encoder = PropertyListEncoder()
    private let decoder = PropertyListDecoder()


    private init() {
        // Set up the cache directory path.
        let cacheBaseURL = fileManager.urls(for: .cachesDirectory, in: .userDomainMask).first!
        cacheDirectory = cacheBaseURL.appendingPathComponent("WebViewCodableCache")

        // Create the directory if it doesn't exist.
        if !fileManager.fileExists(atPath: cacheDirectory.path) {
            try? fileManager.createDirectory(at: cacheDirectory, withIntermediateDirectories: true, attributes: nil)
        }
    }

    // Generates a safe, unique filename from a URL.
    private func cacheKey(for url: URL) -> String {
        return url.absoluteString.data(using: .utf8)!.base64EncodedString()
    }

    // MARK: - Main Caching Logic

    /// Saves response and data to a file using PropertyListEncoder.
    func cacheData(_ data: Data, for response: URLResponse) {
        // 1. Ensure we have a valid URL and can create a Codable response object.
        guard let url = response.url, let codableResponse = CodableCachedData.CodableURLResponse(from: response) else {
            return
        }
        
        let key = cacheKey(for: url)
        let fileURL = cacheDirectory.appendingPathComponent(key)

        // 2. Create the main Codable object.
        let cacheObject = CodableCachedData(data: data, response: codableResponse)

        // 3. Encode the object to Data and write to disk.
        do {
            let encodedData = try encoder.encode(cacheObject)
            try encodedData.write(to: fileURL, options: .atomic)
            print("🗄️ [Codable Cached]: \(url.absoluteString)")
        } catch {
            print("❌ Error caching data with Codable: \(error)")
        }
    }

    /// Retrieves and decodes cached data from a file.
    func getCachedData(for url: URL) -> CachedData? {
        let key = cacheKey(for: url)
        let fileURL = cacheDirectory.appendingPathComponent(key)

        guard fileManager.fileExists(atPath: fileURL.path) else {
            return nil
        }
        
        // 1. Read the raw data from the file.
        guard let fileData = try? Data(contentsOf: fileURL) else {
            // Attempt to remove corrupted or unreadable file.
            try? fileManager.removeItem(at: fileURL)
            return nil
        }

        // 2. Decode the data back into our CodableCachedData object.
        do {
            let decodedCache = try decoder.decode(CodableCachedData.self, from: fileData)
            
            // 3. Reconstruct the HTTPURLResponse from our stored properties.
            guard let httpResponse = decodedCache.response.toHTTPURLResponse() else {
                return nil
            }
            
            // 4. Return the final CachedData object for the scheme handler to use.
            return CachedData(response: httpResponse, data: decodedCache.data)
        } catch {
            print("❌ Error retrieving Codable cached data: \(error)")
            // If decoding fails, the model might have changed. Remove the old cache file.
            try? fileManager.removeItem(at: fileURL)
            return nil
        }
    }
}
