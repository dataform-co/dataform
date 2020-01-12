load("@npm_bazel_typescript//:index.bzl", "ts_library")
load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_test")

def ts_test(name, entry_point, **kwargs):
    ts_library(
        name = name + "_library",
        testonly = 1,
        **kwargs
    )
    nodejs_test(
        name = name,
        data = [
            ":{name}_library".format(name = name),
        ],
        entry_point = entry_point
    )
        

def ts_test_suite(srcs, **kwargs):
    ts_library(
        name = "test_suite_library",
        srcs = srcs,
        **kwargs
    )
    for src in srcs:
        if (src[-8:] == ".spec.ts" or src[-8:] == "_test.ts"):
            nodejs_test(
                name = src[:-3],
                data = [
                    ":test_suite_library",
                ],
                entry_point = ":" + src,
            )
