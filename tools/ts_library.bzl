load("@aspect_rules_js//js:providers.bzl", "JsInfo")
load("@aspect_rules_ts//ts:defs.bzl", "ts_project")

def _ts_library_forwarder_impl(ctx):
    ts_js_info = ctx.attr.ts_project[JsInfo]

    runfiles = ctx.runfiles(
        transitive_files = depset(transitive = [d[DefaultInfo].files for d in ctx.attr.data]),
    ).merge_all(
        [ctx.attr.ts_project[DefaultInfo].default_runfiles] +
        [d[DefaultInfo].default_runfiles for d in ctx.attr.data],
    )

    return [
        DefaultInfo(
            files = ts_js_info.types,
            runfiles = runfiles,
        ),
        ts_js_info,
    ]

_ts_library_forwarder = rule(
    implementation = _ts_library_forwarder_impl,
    attrs = {
        "ts_project": attr.label(mandatory = True, providers = [JsInfo]),
        "data": attr.label_list(allow_files = True),
    },
)

def ts_library(name, srcs = [], data = [], **kwargs):
    ts_target_name = name + "_ts_project"

    ts_project(
        name = ts_target_name,
        tsconfig = "//:tsconfig",
        declaration = True,
        source_map = True,
        transpiler = "tsc",
        srcs = srcs,
        **kwargs
    )

    _ts_library_forwarder(
        name = name,
        ts_project = ":" + ts_target_name,
        data = data,
        testonly = kwargs.get("testonly"),
        # None falls back to the package's default_visibility.
        visibility = kwargs.get("visibility"),
    )
