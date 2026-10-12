load("@aspect_rules_js//js:defs.bzl", "js_test")
load("//tools:ts_library.bzl", "ts_library")

def ts_test_suite(name, srcs, args = [], data = [], tags = [], include_npm = False, **kwargs):
    ts_library(
        name = name,
        data = data,
        srcs = srcs,
        testonly = 1,
        **kwargs
    )

    for src in srcs:
        basename = ".".join(src.split(".")[0:-1])
        if (basename[-5:] == ".spec" or basename[-5:] == "_test"):
            js_test(
                name = basename,
                data = [
                    ":" + name,
                    "//testing:resolver-patch",
                    "//:node_modules/source-map-support",
                ],
                entry_point = (":" + src)[:-3] + ".js",
                args = args,
                node_options = [
                    "--async-stack-traces",
                    "--require=./testing/resolver-patch.js",
                    "--require=source-map-support/register",
                ],
                tags = tags,
                include_npm = include_npm,
            )
