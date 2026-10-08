load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")
load("//tools/common:copy.bzl", "copy_file")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "cli",
    srcs = [
        "commands/compile_command.ts",
        "commands/compile_options.ts",
        "commands/format_command.ts",
        "commands/help_command.ts",
        "commands/index.ts",
        "commands/init_command.ts",
        "commands/init_creds_command.ts",
        "commands/install_command.ts",
        "commands/run_command.ts",
        "commands/run_options.ts",
        "commands/test_command.ts",
        "common_options.ts",
        "console.ts",
        "credentials.ts",
        "index.ts",
        "project_config_options.ts",
        "util.ts",
        "yargswrapper.ts",
    ],
    deps = [
        "//:node_modules/@types/glob",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/@types/readline-sync",
        "//:node_modules/@types/yargs",
        "//:node_modules/chokidar",
        "//:node_modules/glob",
        "//:node_modules/parse-duration",
        "//:node_modules/readline-sync",
        "//:node_modules/untildify",
        "//:node_modules/yargs",
        "//cli/api",
        "//cli/vm",
        "//core",
        "//protos:ts",
        "//sqlx:format",
    ],
)

ts_library(
    name = "index_test_base",
    srcs = ["index_test_base.ts"],
    deps = [
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/node",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//cli",
        "//core",
        "//protos:ts",
        "//testing",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "index_compile_test.ts",
        "index_help_test.ts",
        "index_init_test.ts",
        "index_project_test.ts",
        "tests/jit/jit_build_test.ts",
        "tests/jit/jit_run_test.ts",
        "util_test.ts",
    ],
    data = [
        "//packages/@dataform/cli:bundle",
        "//packages/@dataform/cli:json",
        "//packages/@dataform/cli:worker_bundle",
        "//packages/@dataform/core:package_tar",
    ],
    include_npm = True,
    deps = [
        ":index_test_base",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/ts-mockito",
        "//cli",
        "//cli/api",
        "//core",
        "//protos:ts",
        "//testing",
    ],
)

ts_test_suite(
    name = "integration_tests",
    srcs = [
        "index_run_e2e_test.ts",
        "tests/jit/index_jit_advanced_test.ts",
        "tests/jit/index_jit_dependency_test.ts",
        "tests/jit/index_jit_main_test.ts",
        "tests/jit/index_jit_runtime_test.ts",
    ],
    data = [
        "//packages/@dataform/cli:bundle",
        "//packages/@dataform/cli:json",
        "//packages/@dataform/cli:worker_bundle",
        "//packages/@dataform/core:package_tar",
        "//test_credentials:bigquery.json",
    ],
    # index_test_base runs `npm` directly, and the CLI runs `npm i` when dataformCoreVersion is set.
    include_npm = True,
    tags = ["integration"],
    deps = [
        ":index_test_base",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/ts-mockito",
        "//cli",
        "//cli/api",
        "//core",
        "//protos:ts",
        "//testing",
    ],
)

copy_file(
    name = "readme",
    src = "//:readme.md",
    out = "readme.md",
)
