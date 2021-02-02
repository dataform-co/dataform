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

load("@npm//webpack:index.bzl", "webpack")

webpack(
    name = "bundler",
    data = [
        ":copy_vm2",
        ":vm",
        ":webpack.config.js",
        "@npm//webpack-cli",
    ],
)

genrule(
    name = "copy_vm2",
    srcs = ["//:node_modules/vm2"],
    outs = ["node_modules/vm2"],
    cmd = "cp -r $(SRCS) $(OUTS)",
    output_to_bindir = 1,
)

load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")

npm_package_bin(
    name = "worker_bundle",
    outs = [
        "worker_bundle.js",
    ],
    args = [
        "--config=sandbox/vm/webpack.config.js",
        "--output=$(location worker_bundle.js)",
    ],
    data = [
        ":copy_vm2",
        ":package.json",
        ":webpack.config.js",
    ],
    tool = ":bundler",
)
