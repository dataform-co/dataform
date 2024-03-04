load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "lexer",
    srcs = [
        "lexer.ts"
    ],
    deps = [
        "//:modules-fix",
        "@npm//@types/moo",
        "@npm//moo",
    ],
)

ts_library(
    name = "format",
    srcs = [
        "format.ts"
    ],
    deps = [
        ":lexer",
        "//:modules-fix",
        "//common/errors",
        "@npm//@types/js-beautify",
        "@npm//@types/node",
        "@npm//js-beautify",
        "@npm//sql-formatter",
        "@npm//typeid-js",
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
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
