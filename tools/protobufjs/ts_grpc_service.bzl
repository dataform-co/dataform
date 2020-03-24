def _impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.name + ".ts")
    args = ctx.actions.args()
    args.add("--protos")
    for src in ctx.files.srcs:
        args.add(src.path)
    if len(ctx.attr.root_paths) > 0:
        args.add("--root-paths")
        for root_path in ctx.attr.root_paths:
            args.add(root_path)
    args.add("--root")
    args.add(ctx.attr.root)
    args.add("--protos-import")
    args.add(ctx.attr.protos_import)
    args.add("--output-path")
    args.add(out.path)
    args.add("--service")
    args.add(ctx.attr.service)
    ctx.actions.run(
        inputs = ctx.files.srcs,
        outputs = [out],
        executable = ctx.executable._tool,
        arguments = [args],
        progress_message = "Generating protobuf service interface for {service}".format(service = ctx.attr.service),
    )

    return struct(
        files = depset([out]),
    )

ts_grpc_service = rule(
    implementation = _impl,
    attrs = {
        "srcs": attr.label_list(doc = "proto files", allow_files = [".proto"]),
        "root_paths": attr.string_list(),
        "protos_import": attr.string(),
        "root": attr.string(),
        "service": attr.string(),
        "_tool": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("@df//tools/protobufjs:generate_service_defs"),
        ),
    },
)
