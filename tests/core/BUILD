package(default_visibility=["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "lib",
    srcs = glob(["**/*.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//@types/long",
      "@npm//@types/mocha",
      "@npm//@types/chai",
      "//core",
      "//protos",
    ]
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "core",
    srcs = [
      ":lib"
    ],
    data = [
      "@npm//mocha",
      "@npm//chai"
    ],
)
