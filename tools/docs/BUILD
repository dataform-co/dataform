load("@aspect_rules_js//js:defs.bzl", "js_binary")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "core_readme_generator",
    srcs = [
        "core_readme_generator.ts",
        "proto_extractor.ts",
        "ts_extractor.ts",
        "update_core_readme.ts",
    ],
    deps = [
        "//:node_modules/@types/node",
        "//:node_modules/protobufjs",
        "//:node_modules/typescript",
        "//common/markdown_docs",
        "//core",
    ],
)

js_binary(
    name = "update_core_readme",
    data = [
        ":core_readme_generator",
        "//:node_modules/source-map-support",
        "//testing:resolver-patch",
    ],
    entry_point = ":update_core_readme.js",
    node_options = [
        "--require=./testing/resolver-patch.js",
        "--require=source-map-support/register",
    ],
)
