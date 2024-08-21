load("//tools:ts_library.bzl", "ts_library", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "strings",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
    deps = [
        "//:node_modules/@types/long",
        "//:node_modules/long",
    ],
)

ts_test_suite(
    srcs = glob(["*.spec.ts"]),
    data = [
        "//:node_modules/source-map-support",
    ],
    templated_args = [
        "--node_options=--require=source-map-support/register",
        "--bazel_patch_module_resolver",
    ],
    deps = [
        ":strings",
        "@df//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
    ],
)
