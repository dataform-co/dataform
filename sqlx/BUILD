load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "lexer",
    srcs = [
        "lexer.ts",
    ],
    deps = [
        "//:node_modules/@types/moo",
        "//:node_modules/moo",
    ],
)

ts_library(
    name = "format",
    srcs = [
        "format.ts",
    ],
    deps = [
        ":lexer",
        "//:node_modules/@types/js-beautify",
        "//:node_modules/@types/node",
        "//:node_modules/google-sql-syntax-ts",
        "//:node_modules/js-beautify",
        "//:node_modules/typeid-js",
        "//common/errors",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "format_test.ts",
        "lexer_test.ts",
    ],
    deps = [
        ":format",
        ":lexer",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//testing",
    ],
)
