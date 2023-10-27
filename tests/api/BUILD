load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.ts"]),
    data = [
        "//tests/integration/backwards_compatibility:files",
        "//tests/integration/backwards_compatibility:node_modules",
        "//tests/integration/common_v2:files",
        "//tests/integration/common_v2:node_modules",
        "//tests/integration/invalid_dataform_json:files",
        "//tests/integration/invalid_dataform_json:node_modules",
        "//tests/integration/never_finishes_compiling:files",
        "//tests/integration/never_finishes_compiling:node_modules",
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
