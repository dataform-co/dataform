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

load("//packages:index.bzl", "pkg_bundle")

externals = [
    "vm2",
    "glob",
    "protobufjs",
    "protobufjs/minimal",
]

pkg_bundle(
    name = "worker_bundle",
    args = ["--environment=FAT_BUNDLE"],
    entry_point = "worker.ts",
    entry_point = ":compile.ts",
    externals = externals,
    templated_args = ["--node_options=--require=source-map-support/register"],
    deps = [
        ":vm",
        "@npm//source-map-support",
    ],
)
