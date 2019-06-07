load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")
load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

def _export_next_site_impl(ctx):
    export_dir = ctx.actions.declare_directory(ctx.attr.export_path)
    ctx.action(
        inputs = [ctx.executable.binary],
        outputs = [export_dir],
        progress_message = "Building next.js site %s" % ctx.attr.site_path,
        command = """
          SITE_PATH={site_path};
          EXPORT_PATH=$PWD/{export_path};
          BIN_PATH=$PWD/{binary};
          cd {binary}.runfiles/{workspace_name};
          $BIN_PATH build $SITE_PATH;
          $BIN_PATH export $SITE_PATH -o $EXPORT_PATH;
          """
            .format(
            site_path = ctx.attr.site_path,
            export_path = export_dir.path,
            binary = ctx.executable.binary.path,
            workspace_name = ctx.workspace_name,
        ),
    )
    return [DefaultInfo(files = depset([export_dir]))]

_export_next_site = rule(
    implementation = _export_next_site_impl,
    attrs = {
        "site_path": attr.string(),
        "export_path": attr.string(),
        "binary": attr.label(
            executable = True,
            cfg = "host",
        ),
    },
)

# Generates two targets:
# name: Provides a binary that can be run for local development.
# name_pkg: A tarball of the static site output that can be served in prod.

def next_site(name, srcs, data, site_path):
    nodejs_binary(
        name = name,
        data = srcs + data + [
            "@npm//next",
        ],
        args = [site_path],
        entry_point = "next/dist/bin/next",
        templated_args = ["--node_options=--preserve-symlinks"],
        install_source_map_support = False,
    )
    _export_next_site(
        name = name + ".export",
        site_path = site_path,
        export_path = "out",
        binary = ":" + name,
    )
    pkg_tar(
        name = name + "_pkg",
        strip_prefix = "/" + site_path + "/out",
        srcs = [":" + name + ".export"],
        mode = "0755",
    )
