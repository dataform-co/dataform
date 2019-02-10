package(default_visibility=["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "lib",
    module_name = "@dataform/tests",
    srcs = glob(["api.spec.ts", "utils.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//@types/long",
      "@npm//@types/mocha",
      "@npm//@types/chai",
      "@npm//rimraf",
      "//api",
      "//core",
      "//protos",
    ]
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "tests",
    srcs = [
      ":lib"
    ],
    data = [
      "//examples",
      "@npm//mocha",
      "@npm//chai"
    ],
)
