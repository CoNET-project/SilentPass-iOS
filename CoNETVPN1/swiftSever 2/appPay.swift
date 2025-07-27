import SwiftyStoreKit

class SubscriptionManager {
    let productIds = Set(["001", "001"])
    let sharedSecret = "your_shared_secret"

    func retrieveProductInfo() {
        SwiftyStoreKit.retrieveProductsInfo(productIds) { result in
            if let product = result.retrievedProducts.first {
                let priceString = product.localizedPrice!
                print("Product: \(product.localizedTitle), price: \(priceString)")
            } else if let invalidProductId = result.invalidProductIDs.first {
                print("Invalid product identifier: \(invalidProductId)")
            } else {
                let errorString = result.error?.localizedDescription ?? "Unknown error."
                print("Error: \(errorString)")
            }
        }
    }

    func purchaseSubscription(productId: String) {
        SwiftyStoreKit.purchaseProduct(productId, quantity: 1, atomically: true) { result in
            switch result {
            case .success(let purchase):
                print("Purchase Success: \(purchase.productId)")
                self.verifyReceipt()
            case .error(let error):
                switch error.code {
                case .unknown: print("Unknown error. Please contact support")
                case .clientInvalid: print("Not allowed to make the payment")
                case .paymentCancelled: break
                case .paymentInvalid: print("The purchase identifier was invalid")
                case .paymentNotAllowed: print("The device is not allowed to make the payment")
                case .storeProductNotAvailable: print("The product is not available in the current storefront")
                case .cloudServicePermissionDenied: print("Access to cloud service information is not allowed")
                case .cloudServiceNetworkConnectionFailed: print("Could not connect to the network")
                case .cloudServiceRevoked: print("User has revoked permission to use this cloud service")
                default: print((error as NSError).localizedDescription)
                }
            }
        }
    }

    func verifyReceipt() {
        let appleValidator = AppleReceiptValidator(service: .production, sharedSecret: sharedSecret)
        SwiftyStoreKit.verifyReceipt(using: appleValidator) { result in
            switch result {
            case .success(let receipt):
                print("Receipt verification success: \(receipt)")
            case .error(let error):
                print("Receipt verification failed: \(error)")
            }
        }
    }

    func restoreSubscriptions() {
        SwiftyStoreKit.restorePurchases(atomically: true) { results in
            if results.restoreFailedPurchases.count > 0 {
                print("Restore Failed: \(results.restoreFailedPurchases)")
            } else if results.restoredPurchases.count > 0 {
                print("Restore Success: \(results.restoredPurchases)")
            } else {
                print("Nothing to restore")
            }
        }
    }
}

// 使用示例
//let manager = SubscriptionManager()
//manager.retrieveProductInfo()
//manager.purchaseSubscription(productId: "your_product_id_1")
//manager.restoreSubscriptions()
