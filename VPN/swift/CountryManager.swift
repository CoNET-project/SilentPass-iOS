import Foundation

class CountryManager {
    // 使用 UserDefaults 的 key
    private static let countryKey = "savedCountries"

    // 定义一个静态集合
    static var country = Set<String>() {
        didSet {
            saveCountries() // 每次更新集合时自动保存到 UserDefaults
        }
    }
    
    // 保存到 UserDefaults
    static func saveCountries() {
        let countriesArray = Array(country) // 将 Set 转换为 Array
        UserDefaults.standard.set(countriesArray, forKey: countryKey)
        UserDefaults.standard.synchronize() // 
    }
    
    // 从 UserDefaults 加载数据
    static func loadCountries() {
        if let countriesArray = UserDefaults.standard.array(forKey: countryKey) as? [String] {
            country = Set(countriesArray) // 将 Array 转换回 Set
        } else {
            country = [] // 初始化为空 Set
        }
    }
}
