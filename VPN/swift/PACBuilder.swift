//
//  PACBuilder.swift
//  CoNETVPN1
//
//  Created by peter on 2025-09-05.
//

import Foundation

enum PACBuilder {
    static func buildPAC(
        proxyHost: String = "127.0.0.1",
        proxyPort: Int = 8888,
        resolveDomainsToIP: Bool = false   // 如需“域名→IP再判断本地网段”，设为 true
    ) -> Data {
        func jsonArray(_ arr: [String]) -> String {
            let data = try! JSONSerialization.data(withJSONObject: arr, options: [])
            return String(data: data, encoding: .utf8)!
        }

        // —— 你已有的允许/黑名单 —— //
        let allowJS   = jsonArray(Allowlist.patterns)
        let adBlkJS   = jsonArray(AdBlacklist.patterns)
        let cidrsJS   = jsonArray(Allowlist.ipv4CIDRsRaw)
        let resolveJS = resolveDomainsToIP ? "true" : "false"

        // —— 新增：静态本地直连网段 & 单 IP —— //
        let staticLocalCIDRs = ["192.168.0.0/16","10.0.0.0/8","127.0.0.0/8","169.254.0.0/16","172.16.0.0/12","100.64.0.0/10"]
        let staticLocalIPs   = ["172.16.0.1", "172.16.0.2"]
        let staticLocalCIDRsJS = jsonArray(staticLocalCIDRs)
        let staticLocalIPsJS   = jsonArray(staticLocalIPs)

        let pac = """
        // ===== Helpers: domain match =====
        function _hostMatches(h, list) {
          h = (h || "").toLowerCase();
          if (h.endsWith(".")) h = h.slice(0, -1);
          for (var i = 0; i < list.length; i++) {
            var pat = (list[i] || "").toLowerCase().trim();
            if (!pat) continue;
            if (pat.indexOf("*.") === 0) {
              var suf = pat.slice(1);
              if (h.endsWith(suf)) return true;
            } else {
              if (h === pat || h.endsWith("." + pat)) return true;
            }
          }
          return false;
        }

        // ===== Helpers: IPv4 + CIDR =====
        function _isIPv4(s) {
          if (!s) return false;
          var i = s.indexOf(':'); if (i >= 0) s = s.substring(0, i);
          var a = s.split('.');
          if (a.length !== 4) return false;
          for (var k = 0; k < 4; k++) {
            var n = +a[k];
            if (isNaN(n) || n < 0 || n > 255 || String(n) !== String(+a[k])) return false;
          }
          return true;
        }
        function _ipv4ToInt(s) {
          var a = s.split('.');
          return ((+a[0]<<24)>>>0) | ((+a[1]<<16)>>>0) | ((+a[2]<<8)>>>0) | (+a[3]>>>0);
        }
                // ===== Helpers: IPv6 =====
        function _isIPv6(s) {
             if (!s) return false;
             // 去掉方括号形式的 IPv6: "[::1]"
             if (s[0] === "[" && s[s.length-1] === "]") {
               s = s.substring(1, s.length-1);
             }
             // 必须包含冒号，且不能有非法字符
             if (s.indexOf(":") < 0) return false;
             // 简单验证：每个分段 <= 4 个十六进制数
             var parts = s.split(":");
             if (parts.length < 3) return false;
             for (var i = 0; i < parts.length; i++) {
               if (parts[i].length === 0) continue; // "::" 缩写
               if (!/^[0-9a-fA-F]{1,4}$/.test(parts[i])) return false;
             }
             return true;
        }
        
        function _parseCIDR(c) {
          var p = (c||"").split('/');
          if (p.length !== 2) return null;
          var ip = p[0], plen = +p[1];
          if (!_isIPv4(ip) || isNaN(plen) || plen < 0 || plen > 32) return null;
          var mask = (plen === 0) ? 0 : ((0xFFFFFFFF << (32 - plen)) >>> 0);
          var net  = (_ipv4ToInt(ip) & mask) >>> 0;
          return [net, mask];
        }
        function _buildCIDRList(cidrs) {
          var out = [];
          for (var i = 0; i < cidrs.length; i++) {
            var t = _parseCIDR(cidrs[i]);
            if (t) out.push(t);
          }
          return out;
        }
        function _ipInCIDRs(host, cidrPairs, doResolve) {
          var ipStr = null;
          if (_isIPv4(host)) {
            ipStr = host;
          } else if (doResolve) {
            try { ipStr = dnsResolve(host); } catch (e) { ipStr = null; }
          }
          if (!ipStr || !_isIPv4(ipStr)) return false;
          var ip = _ipv4ToInt(ipStr);
          for (var i = 0; i < cidrPairs.length; i++) {
            var net = cidrPairs[i][0], mask = cidrPairs[i][1];
            if (((ip & mask) >>> 0) === net) return true;
          }
          return false;
        }

        // ===== Lists in PAC =====
        var DIRECT_RULES         = \(allowJS);
        var NOPROXY_RULES        = \(adBlkJS);
        var DIRECT_IPV4_RAW      = \(cidrsJS);
        var DIRECT_IPV4_CIDRS    = _buildCIDRList(DIRECT_IPV4_RAW);
        var RESOLVE_DOMAIN       = \(resolveJS);

        // —— 新增：静态本地直连 —— //
        var STATIC_LOCAL_CIDRS_RAW = \(staticLocalCIDRsJS);
        var STATIC_LOCAL_CIDRS     = _buildCIDRList(STATIC_LOCAL_CIDRS_RAW);
        var STATIC_LOCAL_IPS       = \(staticLocalIPsJS);

        function _isStaticLocal(host) {
          var h = (host || "").toLowerCase();
          // 1) 单 IP：172.16.0.1 / 172.16.0.2
          if (_isIPv4(h)) {
            for (var i = 0; i < STATIC_LOCAL_IPS.length; i++) {
              if (h === STATIC_LOCAL_IPS[i]) return true;
            }
          }
          // 2) 本地网段（仅当 host 是字面量 IP；如需域名->IP 判断，打开 RESOLVE_DOMAIN）
          if (_ipInCIDRs(h, STATIC_LOCAL_CIDRS, RESOLVE_DOMAIN)) return true;
          return false;
        }

        function FindProxyForURL(url, host) {
          if (!host) return "DIRECT";
          if (isPlainHostName(host)) return "DIRECT";
          // —— 如果 host 是 IPv4 或 IPv6 地址，强制 DIRECT —— //
          if (_isIPv4(host) || _isIPv6(host)) return "DIRECT";
          var h = host.toLowerCase();

          // —— 静态本地优先：命中即 DIRECT —— //
          if (_isStaticLocal(h)) return "DIRECT";

          // 1) 域名白名单
          if (_hostMatches(h, DIRECT_RULES)) return "DIRECT";

          // 2) Allowlist 中的 IPv4/CIDR
          if (_ipInCIDRs(h, DIRECT_IPV4_CIDRS, RESOLVE_DOMAIN)) return "DIRECT";

          // 3) “NOPROXY”黑名单（与 Swift 语义对齐为直连）
          if (_hostMatches(h, NOPROXY_RULES)) return "DIRECT";

          // 4) 其余交给本地代理；失败兜底直连
          return "PROXY \(proxyHost):\(proxyPort); DIRECT";
        }
        """
        return pac.data(using: .utf8)!
    }
}
