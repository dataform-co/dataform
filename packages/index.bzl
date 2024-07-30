load("@npm//@bazel/rollup:index.bzl", "rollup_bundle")
load(":rollup_bundle_dts.bzl", "rollup_bundle_dts")
load("@build_bazel_rules_nodejs//:index.bzl", "pkg_npm")

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

def pkg_bundle(deps, externals, allow_node_builtins = False, args = [], **kwargs):
    base_args = ["--external={}".format(",".join(externals))]
    if allow_node_builtins:
        base_args.append("--environment=ALLOW_NODE_BUILTINS")
    rollup_bundle(
        config_file = "//packages:rollup.config.js",
        args = base_args + args,
        format = "cjs",
        sourcemap = "false",
        deps = [
            "@npm//@rollup/plugin-node-resolve",
        ] + deps,
        **kwargs
    )

def pkg_bundle_dts(deps, externals, args = [], **kwargs):
    rollup_bundle_dts(
        config_file = "//packages:rollup_dts.config.js",
        args = ["--external={}".format(",".join(externals))] + args,
        format = "cjs",
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
        outs = [name + ".tar.gz"],
        cmd = "tar -cvzf $(location {name}.tar.gz) -C $(location :{name})/.. --dereference {name}"
            .format(name = name),
    )

LICENSE_HEADER = """// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
"""

def add_license_header_to_file(name, from_file, to_file):
    """
    Adds the Apache 2.0 license header to a file. This is not done in-place because Bazel requires
    separate output and input files.
    """
    native.genrule(
        name=name,
        srcs=[from_file],
        outs=[to_file],
        cmd=(
            (
                "echo '{license_header}' | cat - $(location {from_file}) > $(location {to_file})"
            ).format(from_file=from_file, to_file=to_file, license_header=LICENSE_HEADER)
        ),
    )
