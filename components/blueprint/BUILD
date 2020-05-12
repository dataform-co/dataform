package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_sass//:defs.bzl", "multi_sass_binary")

multi_sass_binary(
    name = "sass",
    srcs = glob(["*.scss"]),
    output_style = "expanded",
    sourcemap = False,
)
