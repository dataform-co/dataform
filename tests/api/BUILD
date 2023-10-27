load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.ts"]),
    data = [
        "//tests/api/projects/backwards_compatibility:files",
        "//tests/api/projects/backwards_compatibility:node_modules",
        "//tests/api/projects/common_v2:files",
        "//tests/api/projects/common_v2:node_modules",
        "//tests/api/projects/invalid_dataform_json:files",
        "//tests/api/projects/invalid_dataform_json:node_modules",
        "//tests/api/projects/never_finishes_compiling:files",
        "//tests/api/projects/never_finishes_compiling:node_modules",
    ],
    deps = [
        "//api",
        "//api/utils",
        "//common/promises",
        "//common/strings",
        "//core",
        "//protos:ts",
        "//testing",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/stack-trace",
        "@npm//chai",
        "@npm//rimraf",
        "@npm//stack-trace",
        "@npm//ts-mockito",
    ],
)
