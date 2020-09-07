package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_docker//container:image.bzl", "container_image")

# Exists purely to give a clean name to the spark Docker image.
container_image(
    name = "spark_image",
    base = "@spark//image",
)

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "spark",
    srcs = glob(
        ["*.ts"],
    ),
    data = [
        ":spark_image",
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
