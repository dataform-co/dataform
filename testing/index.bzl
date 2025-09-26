load("//tools:ts_library.bzl", "ts_library")
load("@aspect_rules_js//js:defs.bzl", "js_test")

def ts_test(name, entry_point, args = [], templated_args = [], data = [], tags = [], **kwargs):
    ts_library(
        name = name + "_library",
        data = data,
        testonly = 1,
        **kwargs
    )
    js_test(
        name = name,
        data = data + [
            ":{name}_library".format(name = name),
        ],
        entry_point = entry_point,
        args = [
                "--node_options=--async-stack-traces", 
                "--node_options=--experimental-specifier-resolution=node",
                "--node_options=--loader=module",
                "--bazel_patch_module_resolver",
        ] + templated_args + args,
        tags = tags,
    )

def ts_test_suite(name, srcs, args = [], templated_args = [], data = [], tags = [], **kwargs):
    name = name if name else "tests"
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
                data = data + [
                    ":{name}".format(name = name),
                ],
                entry_point = ":" + basename + ".js",
                args = [
                    "--node_options=--async-stack-traces", 
                    "--node_options=--experimental-specifier-resolution=node",
                    "--node_options=--loader=module",
                    "--bazel_patch_module_resolver",
                ] + templated_args + args,
                tags = tags,
            )
