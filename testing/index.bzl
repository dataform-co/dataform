load("@aspect_rules_js//js:defs.bzl", "js_test")
load("//tools:ts_library.bzl", "ts_library")

def ts_test(name, entry_point, args = [], templated_args = [], data = [], tags = [], no_copy_to_bin = [], **kwargs):
    ts_library(
        name = name + "_library",
        data = data,
        testonly = 1,
        **kwargs
    )

    js_test_data = []
    for d in data:
        if d not in js_test_data:
            js_test_data.append(d)
    for d in [
        ":{name}_library".format(name = name),
        "//testing:resolver-patch",
        "//:node_modules/source-map-support",
    ]:
        if d not in js_test_data:
            js_test_data.append(d)

    js_test(
        name = name,
        data = js_test_data,
        entry_point = entry_point.replace(".ts", ".js"),
        args = args,
        node_options = [
            "--async-stack-traces",
            "--require=./testing/resolver-patch.js",
            "--require=source-map-support/register",
        ],
        tags = tags,
        no_copy_to_bin = no_copy_to_bin,
    )

def ts_test_suite(name, srcs, args = [], templated_args = [], data = [], tags = [], no_copy_to_bin = [], **kwargs):
    ts_library(
        name = name,
        data = data,
        srcs = srcs,
        testonly = 1,
        **kwargs
    )

    js_test_data = []
    for d in data:
        if d not in js_test_data:
            js_test_data.append(d)
    for d in [
        ":{name}".format(name = name),
        "//testing:resolver-patch",
        "//:node_modules/source-map-support",
    ]:
        if d not in js_test_data:
            js_test_data.append(d)

    for src in srcs:
        basename = ".".join(src.split(".")[0:-1])
        if (basename[-5:] == ".spec" or basename[-5:] == "_test"):
            js_test(
                name = basename,
                data = js_test_data,
                entry_point = (":" + src).replace(".ts", ".js"),
                args = args,
                node_options = [
                    "--async-stack-traces",
                    "--require=./testing/resolver-patch.js",
                    "--require=source-map-support/register",
                ],
                tags = tags,
                no_copy_to_bin = no_copy_to_bin,
            )
