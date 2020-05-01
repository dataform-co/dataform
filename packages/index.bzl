load("@npm_bazel_rollup//:index.bzl", "rollup_bundle")
load("@build_bazel_rules_nodejs//:index.bzl", "pkg_npm")
load(":rollup_bundle_dts.bzl", "rollup_bundle_dts")

def pkg_json(name, package_name, description, version, external_deps = [], layers = [], main = "", types = ""):
    native.genrule(
        name = name,
        srcs = layers,
        tools = ["//packages:gen-package-json-bin"],
        outs = ["package.json"],
        cmd = """$(location //packages:gen-package-json-bin) \\
                --name {name} \\
                --description '{description}' \\
                --package-version {version} \\
                --main {main} \\
                --types {types} \\
                --output-path $(OUTS) \\
                --layer-paths $(SRCS) \\
                --external-dependencies {external_deps}"""
            .format(
            name = package_name,
            description = description,
            version = version,
            main = main,
            types = types,
            external_deps = " ".join(external_deps),
        ),
    )

def pkg_bundle(deps, externals, args = [], **kwargs):
    rollup_bundle(
        config_file = "//packages:rollup.config.js",
        args = ["--external={}".format(",".join(externals))] + args,
        format = "cjs",
        sourcemap = "false",
        srcs = [
            "//:tsconfig.json",
        ],
        deps = [
            "@npm//@rollup/plugin-node-resolve",
            "@npm//rollup-plugin-dts",
        ] + deps,
        **kwargs
    )

def pkg_bundle_dts(deps, externals, args = [], **kwargs):
    rollup_bundle_dts(
        config_file = "//packages:rollup_dts.config.js",
        args = ["--external={}".format(",".join(externals))] + args,
        format = "es",
        sourcemap = "false",
        deps = [
            "@npm//rollup-plugin-dts",
        ] + deps,
        **kwargs
    )

def pkg_npm_tar(name, srcs = [], deps = []):
    pkg_npm(
        name = name,
        srcs = srcs,
        deps = deps,
    )
    native.genrule(
        name = name + "_tar",
        srcs = [":" + name],
        outs = [name + ".tgz"],
        cmd = "tar -cvzf $(location {name}.tgz) -C $(location :{name})/.. --dereference {name}"
            .format(name = name),
    )
