package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

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
        "@npm//@types/node",
        "@npm//glob",
        "@npm//vm2",
    ],
)

load("//packages:index.bzl", "pkg_bundle", "pkg_npm_tar")

externals = [
    "vm2",
    "glob",
    "protobufjs",
    "protobufjs/minimal",
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
