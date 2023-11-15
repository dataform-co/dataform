load("//tools:ts_library.bzl", "ts_library")
load("//tools:expand_template.bzl", "expand_template")
load("//:version.bzl", "DF_VERSION")
load("//testing:index.bzl", "ts_test_suite")

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
    srcs = [
        "assertion.ts",
        "column_descriptors.ts",
        "common.ts",
        "compilers.ts",
        # TODO(ekrekr): move this to its own build folder.
        "compilation_sql/index.ts",
        "declaration.ts",
        "gen_index.ts",
        "index.ts",
        "main.ts",
        "operation.ts",
        "session.ts",
        "table.ts",
        "targets.ts",
        "tasks.ts",
        "test.ts",
        "utils.ts",
        "workflow_settings.ts",
        ":version.ts",
    ],
    deps = [
        "//common/errors",
        "//common/protos",
        "//common/strings",
        "//protos:ts",
        "//sqlx:lexer",
        "@npm//@types/node",
        "@npm//@types/semver",
        "@npm//protobufjs",
        "@npm//semver",
        "@npm//tarjan-graph",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = ["workflow_settings_test.ts"],
    deps = [
        ":core",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
    ],
)
