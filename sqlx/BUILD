load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "sqlx",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//:node_modules/@types/js-beautify",
        "//:node_modules/@types/moo",
        "//:node_modules/@types/node",
        "//:node_modules/@types/uuid",
        "//:node_modules/js-beautify",
        "//:node_modules/moo",
        "//:node_modules/sql-formatter",
        "//:node_modules/uuid",
        "//common/errors",
    ],
)
