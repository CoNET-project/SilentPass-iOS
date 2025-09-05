//
//  Untitled.swift
//  CoNETVPN1
//
//  Created by peter on 2025-09-05.
//

import Foundation


// --- 广告黑名单（支持精确与前缀 *. 通配后缀匹配） ---
struct AdBlacklist {
    // 可继续扩充；保持短小，命中即废止代理
    static let patterns: [String] = [
        "doubleclick.net",
        "googleadservices.com",
        "googlesyndication.com",
        "googletagmanager.com",
        "googletagservices.com",
        "google-analytics.com",
        "googleanalytics.com",
        "adsystem.com",
        "adsrvr.org",
        "onetrust.com",
        "liadm.com",

        // Facebook/Meta
        "facebook-analytics.com",
        "fbcdn.net",

        // Amazon
        "amazontrust.com",

        // Microsoft
        "adsrvr.org",
        "bing.com",
        "msftconnecttest.com",

        // 通用广告网络
        "adsrvr.org",
        "adnxs.com",
        "adzerk.net",
        "pubmatic.com",
        "criteo.com",
        "criteo.net",
        "casalemedia.com",
        "openx.net",
        "rubiconproject.com",
        "serving-sys.com",
        "taboola.com",
        "outbrain.com",
        "media.net",
        "yieldmo.com",
        "3lift.com",
        "indexexchange.com",
        "sovrn.com",
        "sharethrough.com",
        "spotx.tv",
        "springserve.com",
        "tremor.io",
        "tribalfusion.com",
        "undertone.com",
        "yieldlab.net",
        "yieldmanager.com",
        "zedo.com",
        "zemanta.com",

        // 分析和跟踪
        "scorecardresearch.com",
        "quantserve.com",
        "imrworldwide.com",
        "nielsen.com",
        "alexa.com",
        "hotjar.com",
        "mouseflow.com",
        "luckyorange.com",
        "clicktale.com",
        "demdex.net",
        "krxd.net",
        "bluekai.com",
        "exelator.com",
        "mathtag.com",
        "turn.com",
        "acuityplatform.com",
        "adform.net",
        "bidswitch.net",
        "contextweb.com",
        "districtm.io",
        "emxdgt.com",
        "gumgum.com",
        "improve-digital.com",
        "inmobi.com",
        "loopme.com",
        "mobfox.com",
        "nexage.com",
        "rhythmone.com",
        "smaato.com",
        "smartadserver.com",
        "stroeer.io",
        "teads.tv",
        "triplelift.com",
        "verizonmedia.com",
        "vertamedia.com",
        "video.io",
        "viralize.com",
        "weborama.com",
        "widespace.com",

        // 中国广告网络
        "baidu.com",
        "tanx.com",
        "mediav.com",
        "admaster.com.cn",
        "dsp.com",
        "vamaker.com",
        "allyes.com",
        "ipinyou.com",
        "irs01.com",
        "istreamsche.com",
        "jusha.com",
        "knet.cn",
        "madserving.com",
        "miaozhen.com",
        "mmstat.com",
        "moad.cn",
        "mobaders.com",
        "mydas.mobi",
        "n.shifen.com",
        "netease.gg",
        "newrelic.com",
        "nexac.com",
        "ntalker.com",
        "nylalobghyhirgh.com",
        "o2omobi.com",
        "oimagea2.ydstatic.com",
        "optaim.com",
        "optimix.asia",
        "optimizely.com",
        "overture.com",
        "p0y.cn",
        "pagead.l.google.com",
        "pageadimg.l.google.com",
        "pbcdn.com",
        "pingdom.net",
        "pixanalytics.com",
        "ppjia55.com",
        "punchbox.org",
        "qchannel01.cn",
        "qiyou.com",
        "qtmojo.com",
        "quantcount.com",

        // 恶意软件和垃圾邮件
        "2o7.net",
        "omtrdc.net",
        "everesttech.net",
        "everest-tech.net",
        "rubiconproject.com",
        "adsafeprotected.com",
        "adsymptotic.com",
        "adtechjp.com",
        "advertising.com",
        "evidon.com",
        "voicefive.com",
        "buysellads.com",
        "carbonads.com",
        "cdn.ampproject.org",
        "zdbb.net",
        "trackcmp.net",
        

        // 更多跟踪器
        "mixpanel.com",
        "kissmetrics.com",
        "segment.com",
        "segment.io",
        "keen.io",
        "amplitude.com",
        "appsflyer.com",
        "branch.io",
        "adjust.com",
        "kochava.com",
        "tenjin.io",
        "singular.net",
        "apptentive.com",
        "appboy.com",
        "braze.com",
        "customer.io",
        "intercom.io",
        "drift.com",
        "zendesk.com"
    ]
    
    static let regexps: [NSRegularExpression] = {
        let raw = [
        ".*\\.(doubleclick|googleadservices|googlesyndication|google-analytics|adsrvr|adnxs|pubmatic|criteo|casalemedia|openx|rubiconproject|taboola|outbrain|scorecardresearch|quantserve|demdex|krxd)\\..*",
        "^ad[sxvmn]?\\d*[.-].*",
        "^.*[.-]ad[sxvmn]?\\d*[.-].*",
        "^banner[sz]?[.-].*",
        "^.*[.-]banner[sz]?[.-].*",
        "^track(er|ing)?[.-].*",
        "^.*[.-]track(er|ing)?[.-].*",
        "^stat[sz]?[.-].*",
        "^.*[.-]stat[sz]?[.-].*",
        "^analytics?[.-].*",
        "^.*[.-]analytics?[.-].*",
        "^metric[sz]?[.-].*",
        "^.*[.-]metric[sz]?[.-].*",
        "^telemetry[.-].*",
        "^.*[.-]telemetry[.-].*",
        "^pixel[.-].*",
        "^.*[.-]pixel[.-].*",
        "^click[.-].*",
        "^.*[.-]click[.-].*",
        "^counter[.-].*",
        "^.*[.-]counter[.-].*",
        "^beacon[.-].*",
        "^.*[.-]beacon[.-].*"
        ]
        return raw.compactMap { try? NSRegularExpression(pattern: $0, options: [.caseInsensitive]) }
    }()

    @inline(__always)
    static func matches(_ host: String) -> Bool {
        let h = host.lowercased()
        for p in patterns {
            let pat = p.lowercased()
            if pat.hasPrefix("*.") {
                let suf = String(pat.dropFirst(1)) // ".example.com"
                if h.hasSuffix(suf) { return true }
            } else {
                // Match exact domain OR any subdomain
                if h == pat || h.hasSuffix("." + pat) {
                    return true
                }
            }
        }
        // 额外正则匹配
        for re in regexps {
            let range = NSRange(location: 0, length: h.utf16.count)
            if re.firstMatch(in: h, options: [], range: range) != nil {
                return true
            }
        }
        return false
    }
}

// --- 白名单（命中则本地直连，不走 LayerMinus 打包） ---
struct Allowlist {
    // 可按需扩充；示例以常见业务域/必要依赖为主，避免误伤
    static let patterns: [String] = [
        "conet.network",
        "silentpass.io",
        "openpgp.online",
        "comm100vue.com",
        "comm100.io",
        // Apple Push 相关
        "conet.network",
        "apple.com",
        "push.apple.com",
        "cdn-apple.com",
        "cdnst.net",
        "icloud.com",
        "push-apple.com.akadns.net",
        "amazon-adsystem.com",
        "silentpass.io",
        "ziffstatic.com",
        "cdn.ziffstatic.com",
        "courier.push.apple.com",
        "gateway.push.apple.com",
        "gateway.sandbox.push.apple.com",
        "gateway.icloud.com",
        "bag.itunes.apple.com",
        "init.itunes.apple.com",
        "xp.apple.com",
        "gsa.apple.com",
        "gsp-ssl.ls.apple.com",
        "gsp-ssl.ls-apple.com.akadns.net",
        "mesu.apple.com",
        "gdmf.apple.com",
        "deviceenrollment.apple.com",
        "mdmenrollment.apple.com",
        "iprofiles.apple.com",
        "ppq.apple.com",

        // 🔥 微信（WeChat）相关域名
        "wechat.com",
        "weixin.qq.com",
        "weixin110.qq.com",
        "tenpay.com",
        "mm.taobao.com",
        "wx.qq.com",
        "web.wechat.com",
        "webpush.weixin.qq.com",
        "qpic.cn",
        "qlogo.cn",
        "wx.gtimg.com",
        "minorshort.weixin.qq.com",
        "log.weixin.qq.com",
        "szshort.weixin.qq.com",
        "szminorshort.weixin.qq.com",
        "szextshort.weixin.qq.com",
        "hkshort.weixin.qq.com",
        "hkminorshort.weixin.qq.com",
        "hkextshort.weixin.qq.com",
        "hklong.weixin.qq.com",
        "sgshort.wechat.com",
        "sgminorshort.wechat.com",
        "sglong.wechat.com",
        "usshort.wechat.com",
        "usminorshort.wechat.com",
        "uslong.wechat.com",

        // 微信支付
        "pay.weixin.qq.com",
        "payapp.weixin.qq.com",

        // 微信文件传输
        "file.wx.qq.com",
        "support.weixin.qq.com",

        // 微信 CDN
        "mmbiz.qpic.cn",
        "mmbiz.qlogo.cn",
        "mmsns.qpic.cn",
        "sync.com",

        // 腾讯推送服务
        "dns.weixin.qq.com",
        "short.weixin.qq.com",
        "long.weixin.qq.com",

        "doubleclick.net",
        "pubmatic.com",
        "adnxs.com",
        "rubiconproject.com",

        "adsrvr.org",
        "criteo.com",

        "taboola.com",
        "yahoo.com",
        "publicsuffix.org"
    ]
    static let regexps: [NSRegularExpression] = [] // 如需正则白名单可补充
    @inline(__always)
    static func matches(_ host: String) -> Bool {
        @inline(__always)
        func labelSuffixMatch(_ h: String, _ root: String) -> Bool {
            if h == root { return true }
            return h.hasSuffix("." + root)
        }

        let h = host.lowercased()
        for p in patterns {
            var root = p.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
            if root.hasPrefix("*.") {
                root.removeFirst(2)
            }
            guard !root.isEmpty else { continue }
            if labelSuffixMatch(h, root) { return true }
        }
        
        for re in regexps {
            let r = NSRange(location: 0, length: h.utf16.count)
            if re.firstMatch(in: h, options: [], range: r) != nil { return true }
        }
        return false
    }
}
