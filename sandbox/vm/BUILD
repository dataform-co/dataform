load("//tools:ts_library.bzl", "ts_library")
load("@aspect_rules_js//js:defs.bzl", "js_binary")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "vm",
    srcs = glob(
        ["**/*.ts"],
        exclude = ["utils/**/*.*"],
    ),
    deps = [
        "//:node_modules/@types/glob",
        "//:node_modules/@types/node",
        "//:node_modules/glob",
        "//:node_modules/vm2",
        "//common/protos",
        "//core",
        "//protos:ts",
    ],
)

ts_library(
    name = "compile_loader",
    srcs = [],
    data = [
        ":compile_loader.js",
    ],
    deps = [
        ":vm",
        "//:node_modules/source-map-support",
    ],
)

# This is a bit nuts. As we fork a process when compiling projects, we
# start a new node instance but within bazel, modules will fail to load in the
# forked process as we don't have a loader script available.
# To fix this, we use the binary rules to generate the bazel loader scripts
# and then execute these scripts instead, so module resulution works.

js_binary(
    name = "compile",
    data = [
        ":vm",
        "//:node_modules/source-map-support",
    ],
    entry_point = ":compile.ts",
    node_options = ["--require=source-map-support/register"],
)
