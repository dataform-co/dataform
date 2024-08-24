load("//tools:ts_library.bzl", "ts_test_suite")

ts_test_suite(
    srcs = glob(["**/*.ts"]),
    data = [
        "//tests/api/projects/common_v2:files",
        "//tests/api/projects/common_v2:node_modules",
        "//tests/api/projects/invalid_dataform_json:files",
        "//tests/api/projects/invalid_dataform_json:node_modules",
        "//tests/api/projects/never_finishes_compiling:files",
        "//tests/api/projects/never_finishes_compiling:node_modules",
    ],
    deps = [
        "//cli/api",
        "//cli/api/utils",
        "//common/promises",
        "//common/protos",
        "//common/strings",
        "//core",
        "//protos:ts",
        "//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//:node_modules/rimraf",
        "//:node_modules/ts-mockito",
    ],
)
