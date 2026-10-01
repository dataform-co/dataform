load("//packages:index.bzl", "pkg_bundle")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "smoke_test_lib",
    srcs = ["smoke_test.ts"],
    deps = [
        "@npm//@types/node",
    ],
)

# Self-contained CommonJS bundle (Node built-ins only) that the Windows CI runner executes with
# plain `node`; it is built on the Linux job and shipped as a workflow artifact.
pkg_bundle(
    name = "smoke_test_bundle",
    allow_node_builtins = True,
    entry_point = "smoke_test.ts",
    externals = [],
    deps = [":smoke_test_lib"],
)
