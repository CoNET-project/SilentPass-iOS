//
//  PACBuilder.swift
//  CoNETVPN1
//
//  Created by peter on 2025-09-05.
//

import Foundation

enum PACBuilder {
    static func buildPAC(proxyHost: String = "127.0.0.1", proxyPort: Int = 8888) -> Data {
        // 把 Swift 数组安全编码为 JS 字面量
        func jsonArray(_ arr: [String]) -> String {
            let data = try! JSONSerialization.data(withJSONObject: arr, options: [])
            return String(data: data, encoding: .utf8)!
        }

        let allowJS  = jsonArray(Allowlist.patterns)
        let adBlkJS  = jsonArray(AdBlacklist.patterns)

        let pac = """
        function _hostMatches(h, list) {
          h = h.toLowerCase();
          if (h.endsWith(".")) h = h.slice(0, -1);
          for (var i = 0; i < list.length; i++) {
            var pat = (list[i] || "").toLowerCase().trim();
            if (!pat) continue;
            if (pat.indexOf("*.") === 0) {
              var suf = pat.slice(1); // ".example.com"
              if (h.endsWith(suf)) return true;
            } else {
              if (h === pat || h.endsWith("." + pat)) return true;
            }
          }
          return false;
        }

        var DIRECT_RULES = \(allowJS);
        var NOPROXY_RULES = \(adBlkJS);

        function FindProxyForURL(url, host) {
          if (!host) return "DIRECT";
          if (isPlainHostName(host)) return "DIRECT";
          var h = host.toLowerCase();
          if (_hostMatches(h, DIRECT_RULES)) return "DIRECT";
          if (_hostMatches(h, NOPROXY_RULES)) return "DIRECT"; // 黑名单：命中即不走代理
          return "PROXY \(proxyHost):\(proxyPort); DIRECT";
        }
        """
        return pac.data(using: .utf8)!
    }
}
