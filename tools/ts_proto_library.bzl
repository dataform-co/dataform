load("@aspect_rules_js//js:providers.bzl", "js_info")

def _ts_proto_library_impl(ctx):
    # Collect all plain-text .proto source files from dependencies
    proto_files = []
    for dep in ctx.attr.deps:
        if ProtoInfo not in dep:
            fail("ts_proto_library dependency %s must be a proto_library rule" % dep.label)
        proto_files.extend(dep[ProtoInfo].direct_sources)

    # Declare compiled JS and TypeScript declaration output files
    output_name = ctx.attr.output_name or ctx.label.name
    js_out = ctx.actions.declare_file(output_name + ".js")
    dts_out = ctx.actions.declare_file(output_name + ".d.ts")
    esm_js_out = ctx.actions.declare_file("esm/" + output_name + ".js")

    # Execute the compiled binary inside the execroot sandbox
    ctx.actions.run(
        inputs = proto_files,
        outputs = [js_out, dts_out, esm_js_out],
        executable = ctx.executable._compiler,
        arguments = [
            "--js-out", js_out.path,
            "--esm-js-out", esm_js_out.path,
            "--dts-out", dts_out.path,
        ] + [f.path for f in proto_files],
        env = {
            "BAZEL_BINDIR": ctx.bin_dir.path,
        },
        progress_message = "Compiling Protos to JS and TS typings inside sandbox",
        mnemonic = "ProtoCompile",
    )

    # Return standard Bzlmod JS providers
    return [
        DefaultInfo(files = depset([js_out, dts_out, esm_js_out])),
        js_info(
            target = ctx.label,
            types = depset([dts_out]),
            transitive_types = depset([dts_out]),
            sources = depset([js_out, esm_js_out]),
            transitive_sources = depset([js_out, esm_js_out]),
        ),
    ]

_ts_proto_library_rule = rule(
    implementation = _ts_proto_library_impl,
    attrs = {
        "deps": attr.label_list(
            doc = "proto_library targets to compile",
            mandatory = True,
            providers = [ProtoInfo],
        ),
        "output_name": attr.string(
            doc = "Base name of the generated files",
            mandatory = True,
        ),
        "_compiler": attr.label(
            default = Label("//tools:compile_protos"),
            executable = True,
            cfg = "exec",
        ),
    },
)

# Public Starlark Macro wrapping the rule and stripping legacy rules_nodejs-specific kwargs
def ts_proto_library(name, deps, output_name = None, **kwargs):
    if not output_name:
        output_name = name

    kwargs.pop("module_name", None)
    kwargs.pop("module_root", None)
    kwargs.pop("devmode_target", None)
    kwargs.pop("prodmode_target", None)
    kwargs.pop("devmode_module", None)
    kwargs.pop("prodmode_module", None)

    _ts_proto_library_rule(
        name = name,
        deps = deps,
        output_name = output_name,
        **kwargs
    )
