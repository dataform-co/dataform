load("@npm_bazel_typescript//:index.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "vscode",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@types/vscode",
    ],
)

filegroup(
    name = "extension.js",
    srcs = [":vscode"],
    output_group = "es5_sources",
)

sh_binary(
    name = "bin",
    srcs = ["run.sh"],
    data = [
        ":extension.js",
        ":language-configuration.json",
        ":package.json",
        ":sqlx.grammar.json",
    ],
)
