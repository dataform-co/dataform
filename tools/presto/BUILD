package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_layer")

container_image(
    name = "presto_image",
    base = "@presto//image",
    layers = [
        ":presto_config_files",
        ":presto_database_properties",
    ],
)

container_layer(
    name = "presto_config_files",
    directory = "/etc/presto/",
    files = [
        "catalog/memory.properties",
        "config.properties",
        "jvm.config",
    ],
    mode = "0o644",
)

container_layer(
    name = "presto_database_properties",
    directory = "/etc/presto/catalog/",
    files = [
        "catalog/memory.properties",
    ],
    mode = "0o644",
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
        "@npm//presto-client",
        "@npm//uuid",
    ],
)
