package(default_visibility = ["//tests:__subpackages__"])

load("//tools:node_modules.bzl", "node_modules")

filegroup(
    name = "files",
    srcs = glob([
        "**/*.*",
    ]),
)

node_modules(
    name = "node_modules",
    packages = [
        "protobufjs@6.8.8",
        "@dataform/core@1.0.3",
        "@dataform/protos@1.0.3",
    ],
)
