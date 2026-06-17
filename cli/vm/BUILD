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

# This is a bit nuts. As we fork a process when compiling projects, we
# start a new node instance but within bazel, modules will fail to load in the
# forked process as we don't have a loader script available.
# To fix this, we use the binary rules to generate the bazel loader scripts
# and then execute these scripts instead, so module resulution works.

js_binary(
    name = "compile_loader",
    chdir = "../..",
    data = [
        ":vm",
        "//:node_modules/source-map-support",
    ],
    entry_point = "compile.js",
    testonly = True,
)

js_binary(
    name = "jit_worker_bin",
    chdir = "../..",
    data = [
        ":vm",
        "//:node_modules/source-map-support",
    ],
    entry_point = "jit_worker.js",
    testonly = True,
)
