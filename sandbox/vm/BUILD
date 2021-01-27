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

load("//packages:index.bzl", "pkg_bundle", "pkg_json", "pkg_npm_tar")

externals = [
    "vm2",
    "glob",
    "protobufjs",
]

pkg_bundle(
    name = "worker_bundle",
    entry_point = "worker.ts",
    externals = externals,
    deps = [
        ":vm",
    ],
)
