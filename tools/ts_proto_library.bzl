def _run_pbjs(actions, executable, output_name, proto_files, suffix = ".js", wrap = "commonjs"):
    js_file = actions.declare_file(output_name + suffix)

    # Reference of arguments:
    # https://github.com/dcodeIO/ProtoBuf.js/#pbjs-for-javascript
    args = actions.args()
    args.add(["--target", "static-module"])
    args.add(["--wrap", wrap])
    args.add("--strict-long")  # Force usage of Long type with int64 fields
    args.add(["--out", js_file.path])
    args.add([f.path for f in proto_files])

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
    args.add(["--out", ts_file.path])
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
        print(dep)
        if ProtoInfo not in dep:
            fail("ts_proto_library dep %s must be a proto_library rule" % dep.label)

        sources = depset(transitive = [sources, dep[ProtoInfo].transitive_sources])

    output_name = ctx.attr.output_name or ctx.label.name

    pbjs = _run_pbjs(
        ctx.actions,
        ctx.executable,
        output_name,
        sources,
        wrap = "commonjs",
    )
    dts = _run_pbts(ctx.actions, ctx.executable, pbjs)

    # Return a structure that is compatible with the deps[] of a ts_library.
    return struct(
        files = depset([dts]),
        typescript = struct(
            declarations = depset([dts]),
            transitive_declarations = depset([dts]),
            type_blacklisted_declarations = depset(),
            es5_sources = depset([pbjs]),
            es6_sources = depset([pbjs]),
            transitive_es5_sources = depset(),
            transitive_es6_sources = depset([pbjs]),
        ),
    )

ts_proto_library = rule(
    implementation = _ts_proto_library,
    attrs = {
        "deps": attr.label_list(doc = "proto_library targets"),
        "output_name": attr.string(
            doc = """Name of the resulting module, which you will import from.
            If not specified, the name will match the target's name.""",
        ),
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
