load("@aspect_bazel_lib//lib:directory_path.bzl", "directory_path")
load("@aspect_rules_js//js:defs.bzl", "js_binary", "js_run_binary")
load("@aspect_rules_js//npm:defs.bzl", "npm_package")
load("@aspect_rules_rollup//rollup:defs.bzl", "rollup")

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

def pkg_json(name, package_name, description, version, external_deps = [], layers = [], main = "", types = ""):
    native.genrule(
        name = name,
        srcs = layers,
        tools = ["//packages:gen-package-json-bin"],
        outs = ["package.json"],
        cmd = """BAZEL_BINDIR=. $(location //packages:gen-package-json-bin) \\
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

def pkg_bundle(name, deps, externals, entry_point = "index.js", allow_node_builtins = False, args = [], **kwargs):
    base_args = ["--external={}".format(",".join(externals))]
    if allow_node_builtins:
        base_args.append("--environment=ALLOW_NODE_BUILTINS")

    rollup(
        name = name,
        config_file = "//packages:rollup_config",
        args = base_args + args,
        format = "cjs",
        sourcemap = "false",
        entry_point = entry_point,
        node_modules = "//:node_modules",
        deps = deps + [
            "//:node_modules/@rollup/plugin-node-resolve",
        ],
        **kwargs
    )

def pkg_bundle_dts(name, deps, externals, entry_point = "index.d.ts", **kwargs):
    entry_point_label = entry_point if entry_point.startswith(":") or entry_point.startswith("//") else ":" + entry_point
    package = native.package_name()

    rollup_entry_point = "_{}_rollup_entry_point".format(name)
    directory_path(
        name = rollup_entry_point,
        directory = "//:node_modules/rollup/dir",
        path = "dist/bin/rollup",
    )

    rollup_bin = "_{}_rollup_binary".format(name)
    js_binary(
        name = rollup_bin,
        data = [
            "//:node_modules/rollup",
            "//:node_modules/rollup-plugin-dts",
        ],
        entry_point = ":" + rollup_entry_point,
    )

    out_filename = name
    if not out_filename.endswith(".d.ts"):
        if out_filename.endswith(".d"):
            out_filename = out_filename + ".ts"
        else:
            out_filename = out_filename + ".d.ts"

    js_run_binary(
        name = name,
        tool = ":" + rollup_bin,
        srcs = deps + [
            "//packages:rollup_dts_config",
            entry_point_label,
        ],
        outs = [out_filename],
        args = [
            "--config",
            "packages/rollup_dts.config.js",
            "--input",
            package + "/" + entry_point,
            "--file",
            package + "/" + out_filename,
            "--external",
            ",".join(externals),
        ],
        **kwargs
    )

def pkg_npm_tar(name, srcs = [], deps = []):
    npm_package(
        name = name,
        srcs = srcs + deps,
        visibility = ["//visibility:public"],
    )
    native.genrule(
        name = name + "_tar",
        srcs = [":" + name],
        outs = [name + ".tar.gz"],
        cmd = "tar -cvzf $(location {name}.tar.gz) -C $(location :{name})/.. --dereference {name}"
            .format(name = name),
        visibility = ["//visibility:public"],
    )

def add_license_header_to_file(name, from_file, to_file, use_shebang = False, **kwargs):
    header = LICENSE_HEADER
    if use_shebang:
        header = "#!/usr/bin/env node\\n" + header
    native.genrule(
        name = name,
        srcs = [from_file],
        outs = [to_file],
        cmd = "echo -e '{header}' | cat - $(location {from_file}) > $(location {to_file})"
            .format(from_file = from_file, to_file = to_file, header = header),
        **kwargs
    )
