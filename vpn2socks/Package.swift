// swift-tools-version: 6.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "vpn2socks",
    platforms: [
        .iOS(.v14)
    ],
    products: [
        .library(
            name: "vpn2socks",
            targets: ["vpn2socks"]),
    ],
    targets: [
        .target(
            name: "vpn2socks"),
        .testTarget(
            name: "vpn2socksTests",
            dependencies: ["vpn2socks"]
        ),
    ]
) // <-- The parenthesis should be here
