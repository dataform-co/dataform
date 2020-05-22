load("@io_bazel_rules_sass//:defs.bzl", "multi_sass_binary")

package(default_visibility = ["//visibility:public"])

multi_sass_binary(
    name = "sass",
    srcs = glob(["*.scss"]),
    output_style = "expanded",
    sourcemap = False,
)
