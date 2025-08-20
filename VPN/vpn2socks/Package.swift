// swift-tools-version: 6.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "vpn2socks",
    platforms: [
        .iOS(.v15)      // Designates iOS 15 as the minimum supported version
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "vpn2socks",
            targets: ["vpn2socks"]),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "vpn2socks"),
        .testTarget(
            name: "vpn2socksTests",
            dependencies: ["vpn2socks"]
        ),
    ]
)
