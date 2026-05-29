load("@aspect_rules_js//js:defs.bzl", "js_binary")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "vm",
    srcs = [
        "compile.ts",
        "jit_worker.ts",
    ],
    deps = [
        "//:node_modules/@types/glob",
        "//:node_modules/@types/node",
        "//:node_modules/@types/semver",
        "//:node_modules/glob",
        "//:node_modules/semver",
        "//:node_modules/vm2",
        "//common/protos",
        "//core",
        "//protos:ts",
    ],
)

js_binary(
    name = "compile_loader",
    chdir = "../..",
    data = [
        ":vm",
        "//:node_modules/source-map-support",
    ],
    entry_point = "compile.js",
)

js_binary(
    name = "jit_worker_bin",
    chdir = "../..",
    data = [
        ":vm",
        "//:node_modules/source-map-support",
    ],
    entry_point = "jit_worker.js",
)
