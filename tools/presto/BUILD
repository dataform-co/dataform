package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_docker//container:image.bzl", "container_image")

# Exists purely to give a clean name to the presto Docker image.
container_image(
    name = "presto_image",
    base = "@presto//image",
)

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "presto",
    srcs = glob(
        ["*.ts"],
    ),
    data = [
        ":presto_image",
    ],
    deps = [
        "//api",
        "//common/promises",
        "//testing",
        "@npm//@types/node",
        "@npm//@types/uuid",
        "@npm//uuid",
    ],
)
