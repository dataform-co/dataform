load("@io_bazel_rules_sass//:defs.bzl", "multi_sass_binary")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

multi_sass_binary(
    name = "sass",
    srcs = glob(["*.scss"]),
    output_style = "expanded",
    sourcemap = False,
)

ts_library(
    name = "inject_global_styles",
    srcs = ["inject_global_styles.ts"],
    data = [
        ":sass",
    ],
    deps = [
        "//:modules-fix",
        "@npm//@types/node",
    ],
)
