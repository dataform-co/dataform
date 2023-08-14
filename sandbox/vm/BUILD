package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "compile_loader",
    srcs = [],
    data = [
        ":compile_loader.js",
    ],
    deps = [
        ":vm",
        "@npm//source-map-support",
    ],
)

ts_library(
    name = "vm",
    srcs = glob(
        ["**/*.ts"],
        exclude = ["utils/**/*.*"],
    ),
    deps = [
        "//common/protos",
        "//core",
        "//protos:ts",
        "@npm//@types/glob",
        "@npm//glob",
        "@npm//vm2",
        "@npm//@types/node",
    ],
)

load("//packages:index.bzl", "pkg_bundle", "pkg_npm_tar")

externals = [
    "vm2",
    "glob",
    "protobufjs",
    "protobufjs/minimal",
    # From Node.
    "path",
]

genrule(
    name = "copy_vm2",
    srcs = ["//:node_modules/vm2"],
    outs = ["node_modules/vm2"],
    cmd = "cp -rp $(SRCS) $(OUTS)",
    output_to_bindir = 1,
)

genrule(
    name = "copy_glob",
    srcs = ["//:node_modules/glob"],
    outs = ["node_modules/glob"],
    cmd = "cp -rp $(SRCS) $(OUTS)",
    output_to_bindir = 1,
)

genrule(
    name = "copy_protobufjs",
    srcs = ["//:node_modules/protobufjs"],
    outs = ["node_modules/protobufjs"],
    cmd = "cp -rp $(SRCS) $(OUTS)",
    output_to_bindir = 1,
)

# Note: to make the copy_ rules work, see the root BUILD file exports_files comment for them.
pkg_bundle(
    name = "worker_bundle",
    args = ["--environment=WORKER_BUNDLE"],
    entry_point = "worker.ts",
    externals = externals,
    deps = [
        ":copy_glob",
        ":copy_protobufjs",
        ":copy_vm2",
        ":vm",
    ],
)

pkg_bundle(
    name = "cli_worker_bundle",
    args = [],
    entry_point = "worker.ts",
    externals = externals,
    deps = [
        ":vm",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

# This is a bit nuts. As we fork a process when compiling projects, we
# start a new node instance but within bazel, modules will fail to load in the
# forked process as we don't have a loader script available.
# To fix this, we use the binary rules to generate the bazel loader scripts
# and then execute these scripts instead, so module resulution works.

nodejs_binary(
    name = "compile",
    data = [
        ":vm",
        "@npm//source-map-support",
    ],
    entry_point = ":compile.ts",
    templated_args = ["--node_options=--require=source-map-support/register"],
)
