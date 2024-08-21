load("//:version.bzl", "DF_VERSION")
load("//tools:expand_template.bzl", "expand_template")
load("//tools:node_modules.bzl", "node_modules")
load("//tools:ts_library.bzl", "ts_library", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

expand_template(
    name = "version",
    out = "version.ts",
    substitutions = {
        "$DF_VERSION": DF_VERSION,
    },
    template = "version.ts.tmpl",
)

ts_library(
    name = "core",
    srcs = [
        "actions/assertion.ts",
        "actions/data_preparation.ts",
        "actions/declaration.ts",
        "actions/incremental_table.ts",
        "actions/index.ts",
        "actions/notebook.ts",
        "actions/operation.ts",
        "actions/table.ts",
        "actions/test.ts",
        "actions/view.ts",
        "column_descriptors.ts",
        "common.ts",
        "compilers.ts",
        "index.ts",
        "main.ts",
        "path.ts",
        "session.ts",
        "targets.ts",
        "utils.ts",
        "workflow_settings.ts",
        ":version.ts",
    ],
    deps = [
        "//common/errors",
        "//common/protos",
        "//common/strings",
        "//core/compilation_sql",
        "//protos:ts",
        "//sqlx:lexer",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/node",
        "//:node_modules/@types/semver",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/protobufjs",
        "//:node_modules/semver",
        "//:node_modules/tarjan-graph",
    ],
)

ts_test_suite(
    srcs = [
        "main_test.ts",
    ],
    data = [
        ":node_modules",
    ],
    deps = [
        ":core",
        "//common/protos",
        "//protos:ts",
        "//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/vm2",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/core:package_tar",
    ],
)
