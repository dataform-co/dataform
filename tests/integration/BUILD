package(default_visibility=["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "utils",
    srcs = glob(["utils.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//@types/mocha",
      "@npm//@types/chai",
      "//api",
      "//core",
      "//protos",
      "//tests/utils",
    ]
)

ts_library(
    name = "bigquery_lib",
    srcs = glob(["bigquery.spec.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//@types/mocha",
      ":utils",
    ]
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "bigquery",
    srcs = [
      ":bigquery_lib"
    ],
    data = [
      "@npm//mocha",
      "@npm//chai",
      "//test_profiles",
      "//examples",
    ],
)

ts_library(
    name = "redshift_lib",
    srcs = glob(["redshift.spec.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//@types/mocha",
      ":utils",
    ]
)

mocha_node_test(
    name = "redshift",
    srcs = [
      ":redshift_lib"
    ],
    data = [
      "@npm//mocha",
      "@npm//chai",
      "//test_profiles",
      "//examples",
    ],
)

ts_library(
    name = "snowflake_lib",
    srcs = glob(["snowflake.spec.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//@types/mocha",
      ":utils",
    ]
)

mocha_node_test(
    name = "snowflake",
    srcs = [
      ":snowflake_lib"
    ],
    data = [
      "@npm//mocha",
      "@npm//chai",
      "//test_profiles",
      "//examples",
    ],
)
