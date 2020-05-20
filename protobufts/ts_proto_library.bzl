load("@rules_proto//proto:defs.bzl", "ProtoInfo")

def _ts_proto_library_impl(ctx):
    proto_files = []
    for proto in ctx.attr.protos:
        proto_files += proto[ProtoInfo].check_deps_sources.to_list()

    out = ctx.actions.declare_file(ctx.attr.name)

    ctx.actions.run(
        outputs = [out],
        inputs = proto_files,
        tools = [ctx.executable._protoc_gen_ts],
        executable = ctx.executable._protoc,
        arguments = [proto_file.path for proto_file in proto_files] + [
            "--ts_out=%s" % ctx.genfiles_dir.path,
            "--plugin=protoc-gen-ts=%s" % ctx.executable._protoc_gen_ts.path,
        ],
    )
    return [DefaultInfo(files = depset([out]))]

ts_proto_library = rule(
    implementation = _ts_proto_library_impl,
    attrs = {
        "protos": attr.label_list(allow_empty = False, mandatory = True),
        "_protoc": attr.label(default = "@com_google_protobuf//:protoc", executable = True, allow_single_file = None, cfg = "host"),
        "_protoc_gen_ts": attr.label(default = "//protobufts:bin", executable = True, allow_single_file = None, cfg = "host"),
    },
)
