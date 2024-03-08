load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
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
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
        "@npm//rimraf",
        "@npm//ts-mockito",
    ],
)
