load("@aspect_bazel_lib//lib:copy_to_bin.bzl", "copy_to_bin")

copy_to_bin(
    name = "package_layer",
    srcs = ["package.layer.json"],
    visibility = ["//visibility:public"],
)

exports_files(["package.layer.json"])
