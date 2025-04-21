// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "Blackbird",
    platforms: [
        .macOS(.v14),
        .iOS(.v17),
        .watchOS(.v10),
        .tvOS(.v17),
    ],
    products: [
        .library(
            name: "Blackbird",
            targets: ["Blackbird"]),
    ],
    dependencies: [
    ],
    targets: [
        .target(
            name: "Blackbird",
            dependencies: [],
            swiftSettings: [
//                .enableExperimentalFeature("StrictConcurrency"),  // Uncomment for Sendable testing
            ]
        ),
        .testTarget(
            name: "BlackbirdTests",
            dependencies: ["Blackbird"]),
    ]
)
