//
//  Rfc1928.swift
//  CoNETVPN
//
//  Created by peter on 2024-12-27.
//
import Foundation

enum Rfc1928_Command: String {
    case CONNECT, BIND, UDP_ASSOCIATE
}
enum ATYP: String {
    case IP_V4, DOMAINNAME, IP_V6
}
enum STATUS: String {
    case GRANTED, FAILURE, NOT_ALLOWED, NETWORK_UNREACHABLE, HOST_UNREACHABLE, CONNECTION_REFused, TTL_EXPIRED, COMMAND_NOT_SUPPORTED, ADDRESS_TYPE_NOT_SUPPORTED
}


struct Rfc1928 {
    let COMMAND_NOT_SUPPORTED_or_PROTOCOL_ERROR: UInt8 = 0x07
    var dataArray : [UInt8]
    var REP: UInt8 {
        dataArray[1]
    }
    
    mutating func REP (_ REP: UInt8) {
        self.dataArray[1] = REP
    }
    
    mutating func STATUS (_ STATUS: STATUS) {
        switch STATUS {
            case .GRANTED: self.dataArray[1] = 0x00
            case .FAILURE: self.dataArray[1] = 0x01
            case .NOT_ALLOWED: self.dataArray[1] = 0x02
            case .NETWORK_UNREACHABLE: self.dataArray[1] = 0x03
            case .HOST_UNREACHABLE: self.dataArray[1] = 0x04
            case .CONNECTION_REFused: self.dataArray[1] = 0x05
            case .TTL_EXPIRED: self.dataArray[1] = 0x06
            case .COMMAND_NOT_SUPPORTED: self.dataArray[1] = 0x07
            case .ADDRESS_TYPE_NOT_SUPPORTED: self.dataArray[1] = 0x08
        }
    }
    
    init (data: Data){
        dataArray = [UInt8](data)
    }
    
    func ATYP() -> ATYP?{
        switch dataArray[3] {
            case 1: return .IP_V4
            case 3: return .DOMAINNAME
            case 4: return .IP_V6
            default: return nil
        }
    }
    
    func domain_length () -> Int {
        if self.ATYP() != .DOMAINNAME { return 0 }
        return Int(dataArray[4])
    }
    
    func CMD() -> Rfc1928_Command?{
        switch dataArray[1] {
            case 1: return .CONNECT
            case 2: return .BIND
            case 3: return .UDP_ASSOCIATE
            default: return nil
        }
    }
    
    func toData() -> Data {
        return Data(dataArray)
    }
    
    func IPv4() -> String {
        if dataArray[0] == 0x04 {
            if dataArray[4] == 0x0 {
                return String(decoding: dataArray[9...self.dataArray.count - 2], as: UTF8.self)
            }
            let ret = String(dataArray[4]) + "." + String(dataArray[5]) + "." + String(dataArray[6]) + "." + String(dataArray[7])
            return ret
        }
        if self.ATYP() != .IP_V4 { return "" }
        let ret = String(dataArray[4]) + "." + String(dataArray[5]) + "." + String(dataArray[6]) + "." + String(dataArray[7])
        return ret
    }
    
    func domain () -> String {
        if self.ATYP() != .DOMAINNAME { return "" }
        if self.domain_length() != 0, self.domain_length() <= dataArray.count - 5 {
            return String(decoding: dataArray[5...self.domain_length() + 4], as: UTF8.self)
        }
        return ""
    }
    
    func granted () -> Data {
        let ret:[UInt8] = [0x05,0x00,0x00]
        return Data(ret)
    }
    
    func port () -> Int {
        if dataArray[0] == 0x04 {
            let result = (UInt16(dataArray[2]) << 8) + UInt16(dataArray[3])
            return Int(result)
        }
        
        if self.ATYP() == .DOMAINNAME {
            let result = (UInt16(dataArray[self.domain_length() + 5]) << 8)  + UInt16(dataArray[self.domain_length() + 6])
            return Int(result)
        }
        
        if self.ATYP() == .IP_V6 {
            let result = (UInt16(dataArray[20]) << 8) + UInt16(dataArray[19])
            return Int(result)
        }
        
        let result = (UInt16(dataArray[8]) << 8) + UInt16(dataArray[9])
        return Int(result)
        
    }
}

