load("@npm_bazel_typescript//:index.bzl", "ts_library")
load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_test")

def ts_test(name, entry_point, data = [], **kwargs):
    ts_library(
        name = name + "_library",
        data = data,
        testonly = 1,
        **kwargs
    )
    nodejs_test(
        name = name,
        data = data + [
            ":{name}_library".format(name = name),
        ],
        entry_point = entry_point,
    )

def ts_test_suite(name, srcs, data = [], **kwargs):
    ts_library(
        name = name,
        data = data,
        srcs = srcs,
        **kwargs
    )
    for src in srcs:
        if (src[-8:] == ".spec.ts" or src[-8:] == "_test.ts"):
            nodejs_test(
                name = src[:-3],
                data = data + [
                    ":{name}".format(name = name),
                ],
                entry_point = ":" + src,
            )
