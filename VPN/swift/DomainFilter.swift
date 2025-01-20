//
//  domainFilter.swift
//  CoNETVPN1
//
//  Created by peter on 2025-01-13.
//

struct DomainFilter {
    let domain: [String] = [
        "apps.mzstatic.com",
        "*.apple.com",
        "*.icloud.com",
        "is1-ssl.mzstatic.com",
        "cdn.apple-mapkit.com",
        "c.apple.news",
        "ocsp.digicert.com",
    ]
    
    let ipAddr: [String] = [
        "151.101.195.6","151.101.131.6","151.101.67.6","151.101.3.6",
        "184.85.232.0",
        //  same as apps.mzstatic.com
        "104.107.104.29",
        "17.248.192.0", //"17.248.192.1","17.248.192.2","17.248.192.3","17.248.192.4","17.248.192.8",
        "23.37.16.190",
        //  same as apps.mzstatic.com
        "23.203.217.149",
        //  same as apps.mzstatic.com
        "23.45.136.0",
        "104.107.106.81",
        //  same as setup.icloud.com
        //  same as setup.icloud.com
        //  same as setup.icloud.com
        "23.212.59.0",
        "23.45.137.116",
        "17.253.5.0",
        //  same as gsp-ssl.ls.apple.com
        //  same as is1-ssl.mzstatic.com
        "96.16.55.0",
        "17.57.172.5",
        //  same as gsp-ssl.ls.apple.com
        "23.56.3.144","23.56.3.195",
        "17.253.17.201","17.253.17.203",
        "17.57.144.0",
        //  same as 36-courier.push.apple.com
        //  same as gsp-ssl.ls.apple.com"
        "17.188.170.0","17.188.171.0","17.188.172.0","17.188.168.0",
        //  same as setup.icloud.com
        //  same as setup.icloud.com
        "72.247.234.254","17.111.103.20","184.28.120.91","104.107.105.8","23.195.145.39"
        //  3same as 36-courier.push.apple.com
    ]
    
    let ipAddr_direct: [String] = [
        
    ]
    
    func getIPv432 () -> [String] {
        ipAddr.filter { $0.split(separator: ".")[3] != "0" }
    }
    
    private func isIpv4InRange (_ ip: String) -> Bool {
        let ipV4 = ipAddr.filter { $0.split(separator: ".")[3] == "0" }
        let _ip = ip.split(separator: ".")[0..<3].joined(separator: ".") + ".0"
        return ipV4.contains(_ip)
    }
    
    
    func getArrayMask (_ ip: String) -> String {
        let last = ip.split(separator: ".")[3]
        return last == "0" ? "/24" : "/32"
    }
    
    func getIPArray () -> [String] {
        var ret: [String] = []
        for ip in ipAddr {
            ret.append(ip + getArrayMask(ip))
        }
        return ret
    }
    
    func getMask (_ ip: String) -> String {
        let last = ip.split(separator: ".")[3]
        return last == "0" ? "255.255.255.0" : "255.255.255.255"
    }
    
    func getAll_IpAddr () -> [String] {
        return ipAddr + ipAddr_direct
    }
    
    func getSocksDomain () -> String {
        var ret = ""
        for _domain in domain {
            var __domain = _domain
            let splited = _domain.split(separator: ".")
            if splited[0] == "*" {
                __domain = splited[1..<splited.count].joined(separator: ".")
            }
            ret += " dnsDomainIs( host, \".\(__domain)\" ) ||\n"
        }
        return ret
    }
    
    func isDoaminName (_ domain: String) -> Bool {
        if isIPv4Address(domain) == true  {
            return false
        }
        
        return domain.contains(".")
    }
    
    func isIpv4InArray (_ ip: String) -> Bool {
        let ipArray = getIPv432()
        if ipArray.contains(ip) {
            return true
        }
        return isIpv4InRange(ip)
    }
    
    func isIPv4Address(_ ipAddress: String) -> Bool {
        let parts = ipAddress.components(separatedBy: ".")
        return parts.count == 4 && parts.allSatisfy { Int($0) != nil && Int($0)! >= 0 && Int($0)! <= 255 }
    }
    
    func isInFilter (_ IpOrDomain: String) -> Bool {
        if isIPv4Address(IpOrDomain) == true {
            return isIpv4InArray(IpOrDomain)
        }
        var ret = false
        for _domain in domain {
            let splited = _domain.split(separator: ".")
            if splited[0] == "*" {
                let _domain = splited[1..<splited.count].joined(separator: ".")
                if IpOrDomain == _domain {
                    ret = true
                    break
                }
            }
        }
        return ret
    }
    
}
