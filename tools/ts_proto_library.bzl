"""
Borrowed from https://github.com/aspect-build/rules_js/issues/397#issuecomment-1320977306
"""

load(
    "@rules_proto_grpc//:defs.bzl",
    "ProtoPluginInfo",
    "proto_compile_attrs",
    "proto_compile_impl",
)

def _ts_proto_compile_impl(ctx):
    base_env = {
        # Make up for https://github.com/bazelbuild/bazel/issues/15470.
        "BAZEL_BINDIR": ctx.bin_dir.path,
    }
    return proto_compile_impl(ctx, base_env = base_env)
    # return proto_compile_impl(ctx)

# proto_compile_attrs.extra_protoc_args =

# based on https://github.com/aspect-build/rules_js/issues/397
ts_proto_compile = rule(
    implementation = _ts_proto_compile_impl,
    attrs = dict(
        proto_compile_attrs,
        _plugins = attr.label_list(
            providers = [ProtoPluginInfo],
            default = [
                Label("//tools:ts_proto_compile"),
            ],
            doc = "List of protoc plugins to apply",
        ),
        extra_protoc_args = attr.string_list(default = ["--ts_proto_opt=esModuleInterop=true"]),
    ),
    toolchains = [
        str(Label("@rules_proto_grpc//protobuf:toolchain_type")),
    ],
)
