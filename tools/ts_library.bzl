load("@aspect_rules_js//js:providers.bzl", "JsInfo")
load("@aspect_rules_ts//ts:defs.bzl", "ts_project")

def _ts_library_forwarder_impl(ctx):
    dts_files = []
    for dep in ctx.attr.deps:
        for f in dep[DefaultInfo].files.to_list():
            if f.extension == "ts" or f.path.endswith(".d.ts") or f.path.endswith(".d.ts.map"):
                dts_files.append(f)

    runfiles = ctx.runfiles()
    for dep in ctx.attr.deps:
        runfiles = runfiles.merge(dep[DefaultInfo].default_runfiles)
    for f in ctx.attr.extra_files:
        runfiles = runfiles.merge(f[DefaultInfo].default_runfiles)

    js_info = ctx.attr.deps[0][JsInfo]
    esm_files = []
    for f in ctx.attr.extra_files:
        for file in f[DefaultInfo].files.to_list():
            if file.extension == "mjs" or file.path.endswith(".mjs.map") or file.extension == "js" or file.path.endswith(".js.map") or file.extension == "json":
                esm_files.append(file)

    new_js_info = JsInfo(
        target = js_info.target if hasattr(js_info, "target") else ctx.label,
        sources = js_info.sources if hasattr(js_info, "sources") else depset(),
        types = js_info.types if hasattr(js_info, "types") else depset(),
        transitive_sources = depset(esm_files, transitive = [js_info.transitive_sources]),
        transitive_types = js_info.transitive_types,
        npm_sources = js_info.npm_sources if hasattr(js_info, "npm_sources") else depset(),
        npm_package_store_infos = js_info.npm_package_store_infos if hasattr(js_info, "npm_package_store_infos") else depset(),
    )

    return [
        DefaultInfo(
            files = depset(dts_files),
            runfiles = runfiles,
        ),
        new_js_info,
    ]

_ts_library_forwarder = rule(
    implementation = _ts_library_forwarder_impl,
    attrs = {
        "deps": attr.label_list(mandatory = True, providers = [JsInfo]),
        "extra_files": attr.label_list(allow_files = True),
    },
)

def ts_library(name, srcs = [], **kwargs):
    ts_target_name = name + "_ts_project"
    ts_esm_target_name = name + "_ts_project_esm"

    if "module_name" not in kwargs:
        package = native.package_name()
        kwargs["module_name"] = "df/" + package if package else "df"

    # Pop legacy rules_nodejs-specific attributes that ts_project doesn't accept
    kwargs.pop("devmode_target", None)
    kwargs.pop("prodmode_target", None)
    kwargs.pop("devmode_module", None)
    kwargs.pop("prodmode_module", None)
    kwargs.pop("module_name", None)
    kwargs.pop("module_root", None)

    data = kwargs.pop("data", [])

    testonly = kwargs.get("testonly", 0)

    # 1. CommonJS compilation (produces .js, .d.ts)
    ts_project(
        name = ts_target_name,
        tsconfig = "//:tsconfig",
        declaration = True,
        source_map = True,
        transpiler = "tsc",
        srcs = srcs,
        **kwargs
    )

    # 2. ESM compilation (produces esm/*.js, esm/*.js.map)
    ts_project(
        name = ts_esm_target_name,
        tsconfig = "//:tsconfig_esm",
        extends = "//:tsconfig",
        declaration = False,
        source_map = True,
        out_dir = "esm",
        transpiler = "tsc",
        srcs = srcs,
        **kwargs
    )
    _ts_library_forwarder(
        name = name,
        deps = [
            ":" + ts_target_name,
        ],
        extra_files = [":" + ts_esm_target_name] + data,
        testonly = testonly,
        visibility = ["//visibility:public"],
    )
