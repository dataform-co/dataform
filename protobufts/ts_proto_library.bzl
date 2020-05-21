load("@rules_proto//proto:defs.bzl", "ProtoInfo")

def _ts_proto_library_impl(ctx):
    src_proto_files = []
    import_proto_files = []
    for proto in ctx.attr.protos:
        src_proto_files += proto[ProtoInfo].check_deps_sources.to_list()
        import_proto_files += proto[ProtoInfo].transitive_imports.to_list()

    outs = [ctx.actions.declare_file(src_proto_file.basename[:-6] + ".ts") for src_proto_file in src_proto_files]
    ctx.actions.run(
        outputs = outs,
        inputs = src_proto_files + import_proto_files,
        tools = [ctx.executable._protoc_gen_ts],
        executable = ctx.executable._protoc,
        arguments = [proto_file.path for proto_file in src_proto_files] + [
            "--ts_out=%s" % ctx.genfiles_dir.path,
            "--plugin=protoc-gen-ts=%s" % ctx.executable._protoc_gen_ts.path,
        ],
    )
    return [DefaultInfo(files = depset(outs))]

ts_proto_library = rule(
    implementation = _ts_proto_library_impl,
    attrs = {
        "protos": attr.label_list(allow_empty = False, mandatory = True),
        "_protoc": attr.label(default = "@com_google_protobuf//:protoc", executable = True, allow_single_file = None, cfg = "host"),
        "_protoc_gen_ts": attr.label(default = "//protobufts:bin", executable = True, allow_single_file = None, cfg = "host"),
    },
)
