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
        "//packages/@dataform/assertion_utils:package_tar",
        "//packages/@dataform/core:package_tar",
        "//packages/@dataform/crossdb:package_tar",
    ],
)
