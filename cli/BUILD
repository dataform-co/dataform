load("//tools:ts_library.bzl", "ts_library")
load("//tools/common:copy.bzl", "copy_file")
load("@io_bazel_rules_docker//container:container.bzl", "container_image", "container_push")
load("@io_bazel_rules_docker//docker/util:run.bzl", "container_run_and_commit")
load("//:version.bzl", "DF_VERSION")
load("//testing:index.bzl", "ts_test_suite")
load("//tools:node_modules.bzl", "node_modules")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "cli",
    srcs = [
        "console.ts",
        "credentials.ts",
        "index.ts",
        "util.ts",
        "yargswrapper.ts",
    ],
    deps = [
        "//cli/api",
        "//cli/vm",
        "//core",
        "//protos:ts",
        "//sqlx:format",
        "@npm//@types/glob",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/readline-sync",
        "@npm//@types/yargs",
        "@npm//chokidar",
        "@npm//glob",
        "@npm//parse-duration",
        "@npm//readline-sync",
        "@npm//untildify",
        "@npm//yargs",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/cli:package_tar",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = ["index_test.ts"],
    data = [
        ":node_modules",
        "//test_credentials:bigquery.json",
        "//packages/@dataform/core:package_tar",
        "@nodejs//:node",
        "@nodejs//:npm",
    ],
    deps = [
        "//core",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/js-yaml",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
        "@npm//js-yaml",
    ],
)

copy_file(
    name = "readme",
    src = "//:readme.md",
    out = "readme.md",
)

container_run_and_commit(
    name = "image_with_dataform_cli",
    commands = [
        "npm i -g @dataform/cli@{dataform_version}".format(dataform_version = DF_VERSION),
    ],
    image = "@nodejs_base//image",
    tags = [
        "no-remote",
    ],
)

container_image(
    name = "image_with_dataform_entrypoint",
    base = ":image_with_dataform_cli_commit.tar",
    cmd = [],
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
