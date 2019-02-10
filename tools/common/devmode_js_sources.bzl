# Copyright 2017 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Definitions for handling path re-mapping, to support short module names.
# See pathMapping doc: https://github.com/Microsoft/TypeScript/issues/5039
#
# This reads the module_root and module_name attributes from typescript rules in
# the transitive closure, rolling these up to provide a mapping to the
# TypeScript compiler and to editors.
#

"""Rule to get devmode js sources from deps.
Outputs a manifest file with the sources listed.
"""

load(":sources_aspect.bzl", "sources_aspect")
load(":expand_into_runfiles.bzl", "expand_path_into_runfiles")

def _devmode_js_sources_impl(ctx):
  files = depset()

  for d in ctx.attr.deps:
    if hasattr(d, "node_sources"):
      files = depset(transitive=[files, d.node_sources])
    elif hasattr(d, "files"):
      files = depset(transitive=[files, d.files])

  ctx.actions.write(ctx.outputs.manifest, "".join([
    expand_path_into_runfiles(ctx, f.path) + "\n" for f in files
  ]))
  return [DefaultInfo(files = files)]

devmode_js_sources = rule(
    implementation = _devmode_js_sources_impl,
    attrs = {
        "deps": attr.label_list(
            allow_files = True,
            aspects = [sources_aspect],
          ),
    },
    outputs = {
        "manifest": "%{name}.MF",
    }
)
