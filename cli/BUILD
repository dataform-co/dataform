package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "cli",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//api",
        "//core",
        "//protos:ts",
        "//sqlx",
        "@npm//@types/analytics-node",
        "@npm//@types/glob",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/readline-sync",
        "@npm//@types/uuid",
        "@npm//@types/yargs",
        "@npm//analytics-node",
        "@npm//chokidar",
        "@npm//glob",
        "@npm//presto-client",
        "@npm//readline-sync",
        "@npm//untildify",
        "@npm//uuid",
        "@npm//yargs",
    ],
)

load("//tools/common:copy.bzl", "copy_file")

copy_file(
    name = "readme",
    src = "//:readme.md",
    out = "readme.md",
)

load("//:version.bzl", "DF_VERSION")
load("@io_bazel_rules_docker//docker/util:run.bzl", "container_run_and_commit")

container_run_and_commit(
    name = "image_with_dataform_cli",
    commands = [
        "npm i -g @dataform/cli@{dataform_version}".format(dataform_version = DF_VERSION),
    ],
    image = "@nodejs_slim_base//image",
    tags = [
        "no-remote",
    ],
)

load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_push")

container_image(
    name = "image_with_dataform_entrypoint",
    base = ":image_with_dataform_cli_commit.tar",
    entrypoint = ["dataform"],
    tags = [
        "no-remote",
    ],
)

container_push(
    name = "push",
    format = "Docker",
    image = ":image_with_dataform_entrypoint",
    registry = "docker.io",
    repository = "dataformco/dataform",
    tag = DF_VERSION,
)
