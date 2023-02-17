load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "markdown-cms",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//:node_modules/@octokit/rest",
        "//:node_modules/@types/node",
        "//:node_modules/front-matter",
    ],
)
