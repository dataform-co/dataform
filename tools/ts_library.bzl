load("@aspect_rules_ts//ts:defs.bzl", "ts_project")
load("@npm//:mocha/package_json.bzl", mocha = "bin")

def ts_library(**kwargs):
    ts_project(
        tsconfig = "//:tsconfig",
        declaration = True,
        transpiler = "tsc",
        source_map = True,
        **kwargs
    )

def ts_test_suite(srcs, tsconfig = None, **kwargs):
    for src in srcs:
        if not src.endswith("_test.ts"):
            fail("non-test file passed to test suite")
        test_name = src[:-3]
        lib_name = test_name + "_lib"
        ts_library(
            name = lib_name,
            srcs = [src],
            testonly = True,
            **kwargs
        )
        mocha.mocha_test(
            name = test_name,
            data = [
                ":" + lib_name,
                "//:package_json",
            ],
            node_options = [
                "--experimental-specifier-resolution=node",
            ],
            args = [
                "**/" + test_name + ".js",
            ],
        )
