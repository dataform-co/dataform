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

# load("//packages:index.bzl", "pkg_bundle")

# externals = [
#     "vm2",
#     "glob",
#     "protobufjs",
#     "protobufjs/minimal",
# ]

# pkg_bundle(
#     name = "worker_bundle",
#     args = [
#         "--environment=FAT_BUNDLE",
#     ],
#     entry_point = "worker.ts",
#     externals = externals,
#     deps = [
#         ":vm",
#     ],
# )

load("@npm//webpack:index.bzl", "webpack")

webpack(
    name = "bundler",
    data = [
        ":vm",
        ":webpack.config.js",
        "@npm//webpack-cli",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")

npm_package_bin(
    name = "worker_bundle",
    outs = [
        "worker_bundle.js",
    ],
    args = [
        "--config=sandbox/vm/webpack.config.js",
        # "--path=$(location worker_bundle.js)",
    ],
    data = [
        ":webpack.config.js",
    ],
    tool = ":bundler",
)
