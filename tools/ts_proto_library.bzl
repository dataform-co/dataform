# Stolen entirely from: https://github.com/bazelbuild/rules_nodejs/blob/master/packages/labs/src/protobufjs/ts_proto_library.bzl
# TODO: Use the rules in the labs package. They are currently broken.

load("@build_bazel_rules_nodejs//:providers.bzl", "DeclarationInfo", "JSEcmaScriptModuleInfo", "JSNamedModuleInfo")

def _run_pbjs(actions, executable, output_name, proto_files, suffix = ".js", wrap = "default", amd_name = ""):
    js_file = actions.declare_file(output_name + suffix)

    # Reference of arguments:
    # https://github.com/dcodeIO/ProtoBuf.js/#pbjs-for-javascript
    args = actions.args()
    args.add_all(["--target", "static-module"])
    args.add_all(["--wrap", wrap])
    args.add("--strict-long")  # Force usage of Long type with int64 fields
    args.add_all(["--out", js_file.path])
    args.add_all(proto_files)

    actions.run(
        executable = executable._pbjs,
        inputs = proto_files,
        outputs = [js_file],
        arguments = [args],
    )
    return js_file

def _run_pbts(actions, executable, js_file):
    ts_file = actions.declare_file(js_file.basename[:-len(".js")] + ".d.ts")

    # Reference of arguments:
    # https://github.com/dcodeIO/ProtoBuf.js/#pbts-for-typescript
    args = actions.args()
    args.add_all(["--out", ts_file.path])
    args.add(js_file.path)

    actions.run(
        executable = executable._pbts,
        progress_message = "Generating typings from %s" % js_file.short_path,
        inputs = [js_file],
        outputs = [ts_file],
        arguments = [args],
    )
    return ts_file

def _ts_proto_library(ctx):
    sources = depset()
    for dep in ctx.attr.deps:
        if ProtoInfo not in dep:
            fail("ts_proto_library dep %s must be a proto_library rule" % dep.label)

        sources = depset(transitive = [sources, dep[ProtoInfo].transitive_sources])

    output_name = ctx.attr.output_name or ctx.label.name

    js_es5 = _run_pbjs(
        ctx.actions,
        ctx.executable,
        output_name,
        sources,
        amd_name = "/".join([p for p in [
            ctx.workspace_name,
            ctx.label.package,
        ] if p]),
    )
    js_es6 = _run_pbjs(
        ctx.actions,
        ctx.executable,
        output_name,
        sources,
        suffix = ".mjs",
        wrap = "es6",
    )

    # pbts doesn't understand '.mjs' extension so give it the es5 file
    dts = _run_pbts(ctx.actions, ctx.executable, js_es5)

    # Return a structure that is compatible with the deps[] of a ts_library.
    declarations = depset([dts])
    es5_sources = depset([js_es5])
    es6_sources = depset([js_es6])

    return struct(
        providers = [
            DefaultInfo(files = declarations),
            DeclarationInfo(
                declarations = declarations,
                transitive_declarations = declarations,
                type_blacklisted_declarations = depset(),
            ),
            JSNamedModuleInfo(
                direct_sources = es5_sources,
                sources = es5_sources,
            ),
            JSEcmaScriptModuleInfo(
                direct_sources = es6_sources,
                sources = es6_sources,
            ),
        ],
        typescript = struct(
            declarations = declarations,
            transitive_declarations = declarations,
            es5_sources = es5_sources,
            es6_sources = es6_sources,
            transitive_es5_sources = es5_sources,
            transitive_es6_sources = es6_sources,
        ),
    )

ts_proto_library = rule(
    implementation = _ts_proto_library,
    attrs = {
        "output_name": attr.string(
            doc = """Name of the resulting module, which you will import from.
            If not specified, the name will match the target's name.""",
        ),
        "deps": attr.label_list(doc = "proto_library targets"),
        "_pbjs": attr.label(
            default = Label("//:pbjs"),
            executable = True,
            cfg = "host",
        ),
        "_pbts": attr.label(
            default = Label("//:pbts"),
            executable = True,
            cfg = "host",
        ),
    },
)
