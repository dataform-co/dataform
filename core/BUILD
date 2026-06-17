load("//:version.bzl", "DF_VERSION")
load("//testing:index.bzl", "ts_test_suite")
load("//tools:expand_template.bzl", "expand_template")
load("//tools:node_modules.bzl", "node_modules")
load("//tools:ts_library.bzl", "ts_library")

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
        "compilers.ts",
        "contextables.ts",
        "extension.ts",
        "index.ts",
        "jit_compiler.ts",
        "jit_context.ts",
        "main.ts",
        "path.ts",
        "session.ts",
        "targets.ts",
        "utils.ts",
        "workflow_settings.ts",
        ":version.ts",
    ],
    deps = [
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/node",
        "//:node_modules/@types/semver",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/protobufjs",
        "//:node_modules/semver",
        "//:node_modules/tarjan-graph",
        "//common/errors",
        "//common/protos",
        "//common/strings",
        "//core/compilation_sql",
        "//protos:ts",
        "//sqlx:lexer",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "actions/assertion_test.ts",
        "actions/data_preparation_test.ts",
        "actions/declaration_test.ts",
        "actions/filename_override_test.ts",
        "actions/incremental_table_test.ts",
        "actions/index_test.ts",
        "actions/notebook_test.ts",
        "actions/operation_test.ts",
        "actions/table_test.ts",
        "actions/test_test.ts",
        "actions/view_test.ts",
        "compilers_test.ts",
        "jit_compiler_test.ts",
        "jit_context_test.ts",
        "main_test.ts",
        "utils_test.ts",
    ],
    data = [
        ":node_modules",
    ],
    deps = [
        ":core",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/long",
        "//common/protos",
        "//protos:ts",
        "//testing",
        "//testing:run_core",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/core:package_tar",
        "//packages/sample-extension:package_tar",
    ],
)
