load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "vscode",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//cli/api",
        "//core",
        "//protos:ts",
        "//:node_modules/@types/node",
        "//:node_modules/@types/vscode",
        "//:node_modules/vscode-languageclient",
        "//:node_modules/vscode-languageserver",
        "//:node_modules/vscode-languageserver-protocol",
        "//:node_modules/vscode-jsonrpc",
        "//:node_modules/vscode-languageserver-types",
        "//:node_modules/vscode-languageserver-textdocument",
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
        ":workflow_settings_yaml.schema.json",
        ":vscode-sources",
    ],
)

sh_binary(
    name = "packager",
    srcs = ["packager.sh"],
    data = [
        ":language-configuration.json",
        ":dataform_logo.png",
        ":package.json",
        ":README.md",
        ":LICENSE",
        ":sqlx.grammar.json",
        ":workflow_settings_yaml.schema.json",
        ":vscode-sources",
    ],
)
