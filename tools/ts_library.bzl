load("@aspect_rules_ts//ts:defs.bzl", "ts_project")
# load("@npm//:mocha/package_json.bzl", mocha = "bin")
load("@aspect_rules_js//js:defs.bzl", "js_test")

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
        is_test_suffix = src.endswith("_test.ts") or src.endswith("spec.ts")
        if not is_test_suffix:
            fail("non-test file passed to test suite")
        test_name = src[:-3]
        lib_name = test_name + "_lib"
        ts_library(
            name = lib_name,
            srcs = [src],
            testonly = True,
            **kwargs
        )
        js_test(
            name = test_name,
            entry_point = "//:node_modules/mocha/bin/mocha",
            data = [
                ":" + lib_name,
                "//:package_json",
                "//:node_modules/chai",
                "//:node_modules/mocha",
            ],
            node_options = [
                "--experimental-specifier-resolution=node",
            ],
            args = [
                "**/" + test_name + ".js",
            ],
        )
