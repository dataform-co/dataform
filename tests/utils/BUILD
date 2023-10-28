load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//tests:__subpackages__", "//examples:__subpackages__"])

ts_library(
    name = "utils",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//testing",
        "@npm//@types/node",
        "@npm//@types/rimraf",
        "@npm//rimraf",
    ],
)
