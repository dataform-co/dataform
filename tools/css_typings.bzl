def _impl(ctx):
    outs = []
    for f in ctx.files.srcs:
        # Only create outputs for css files.
        if f.path[-4:] != ".css":
            fail("Only .css file inputs are allowed.")

        out = ctx.actions.declare_file(f.basename.replace(".css", ".css.d.ts"), sibling = f)
        outs.append(out)
        ctx.actions.run(
            inputs = [f],
            outputs = [out],
            executable = ctx.executable._tool,
            arguments = ["-o", out.root.path, "-p", f.path, "--silent"],
            progress_message = "Generating CSS type definitions for %s" % f.path,
        )

    # Return a structure that is compatible with the deps[] of a ts_library.
    return struct(
        files = depset(outs),
        typescript = struct(
            declarations = depset(outs),
            transitive_declarations = depset(outs),
            type_blacklisted_declarations = depset(),
            es5_sources = depset(),
            es6_sources = depset(),
            transitive_es5_sources = depset(),
            transitive_es6_sources = depset(),
        ),
    )

css_typings = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(doc = "css files", allow_files = True),
        "packages": attr.string_list(),
        "_tool": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("//:tcm"),
        ),
    },
)
