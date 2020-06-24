load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "vscode",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//api",
        "//protos:ts",
        "@npm//@types/node",
        "@npm//@types/vscode",
        "@npm//vscode-languageclient",
        "@npm//vscode-languageserver",
        "@npm//vscode-languageserver-textdocument",
    ],
)

filegroup(
    name = "vscode-sources",
    srcs = [":vscode"],
    output_group = "es5_sources",
)

sh_binary(
    name = "bin",
    srcs = ["run.sh"],
    data = [
        ":language-configuration.json",
        ":package.json",
        ":sqlx.grammar.json",
        ":vscode-sources",
    ],
)
