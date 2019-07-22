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
    deps = [
        "//assertion_utils:package_tar",
        "//core:package_tar",
        "//crossdb:package_tar",
        "//protos:package_tar",
    ],
)
