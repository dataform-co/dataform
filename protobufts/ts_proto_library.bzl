load("@npm_bazel_typescript//:index.bzl", "ts_library")
load("@rules_proto//proto:defs.bzl", "ProtoInfo")

def _protoc_gen_ts_impl(ctx):
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
            "--ts_out=import_prefix=%s:%s" % (ctx.attr.import_prefix, ctx.genfiles_dir.path),
            "--plugin=protoc-gen-ts=%s" % ctx.executable._protoc_gen_ts.path,
        ],
    )
    return [DefaultInfo(files = depset(outs))]

protoc_gen_ts = rule(
    implementation = _protoc_gen_ts_impl,
    attrs = {
        "protos": attr.label_list(allow_empty = False, mandatory = True),
        "import_prefix": attr.string(default = ""),
        "_protoc": attr.label(default = "@com_google_protobuf//:protoc", executable = True, allow_single_file = None, cfg = "host"),
        "_protoc_gen_ts": attr.label(default = "//protobufts:bin", executable = True, allow_single_file = None, cfg = "host"),
    },
)

def ts_proto_library(name, protos, deps = [], import_prefix = ""):
    protoc_gen_ts(
        name = name + "-gen",
        protos = protos,
        import_prefix = import_prefix,
    )

    ts_library(
        name = name,
        srcs = [":" + name + "-gen"],
        deps = deps + ["//protobufts:runtime-support"],
    )
