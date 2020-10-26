load("//tools:ts_library.bzl", "ts_library")
load("//tools:expand_template.bzl", "expand_template")
load("//:version.bzl", "DF_VERSION")

package(default_visibility = ["//visibility:public"])

expand_template(
    name = "version",
    out = "version.ts",
    substitutions = {
        "$DF_VERSION": DF_VERSION,
    },
    template = "version.ts.tmpl",
)

filegroup(
    name = "files",
    srcs = glob(["**/*.*"]) + [":version.ts"],
)

ts_library(
    name = "core",
    srcs = glob(["**/*.ts"]) + [":version.ts"],
    deps = [
        "//common/errors",
        "//common/protos",
        "//common/strings",
        "//protos:ts",
        "//sqlx",
        "@npm//@types/node",
        "@npm//@types/semver",
        "@npm//protobufjs",
        "@npm//semver",
        "@npm//tarjan-graph",
    ],
)
