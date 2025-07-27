//
//  CodableCachedData.swift
//  CoNETVPN1
//
//  Created by peter on 2025-07-17.
//

import Foundation

// A Codable-compliant struct to store the necessary parts of a response and the data.
struct CodableCachedData: Codable {
    let data: Data
    let response: CodableURLResponse

    // A nested struct to store the relevant properties of a URLResponse.
    struct CodableURLResponse: Codable {
        let url: URL?
        let mimeType: String?
        let expectedContentLength: Int64
        let textEncodingName: String?
        let headers: [String: String]
        let statusCode: Int

        // Initializer to capture properties from an HTTPURLResponse.
        init?(from response: URLResponse) {
            guard let httpResponse = response as? HTTPURLResponse else {
                return nil
            }
            self.url = httpResponse.url
            self.mimeType = httpResponse.mimeType
            self.expectedContentLength = httpResponse.expectedContentLength
            self.textEncodingName = httpResponse.textEncodingName
            self.statusCode = httpResponse.statusCode

            var tempHeaders = [String: String]()
            for (key, value) in httpResponse.allHeaderFields {
                if let keyString = key as? String, let valueString = value as? String {
                    tempHeaders[keyString] = valueString
                }
            }
            self.headers = tempHeaders
        }

        // Method to reconstruct an HTTPURLResponse from the stored properties.
        func toHTTPURLResponse() -> HTTPURLResponse? {
            return HTTPURLResponse(
                url: self.url!,
                statusCode: self.statusCode,
                httpVersion: "HTTP/1.1",
                headerFields: self.headers
            )
        }
    }
}
