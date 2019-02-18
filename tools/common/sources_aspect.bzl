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

"""Apsect to collect es5 js sources from deps.
"""

def _sources_aspect_impl(target, ctx):
    result = depset()

    # Sources from npm fine grained deps which are tagged with NODE_MODULE_MARKER
    # should not be included
    if hasattr(ctx.rule.attr, "tags") and "NODE_MODULE_MARKER" in ctx.rule.attr.tags:
        return struct(node_sources = result)

    if hasattr(ctx.rule.attr, "deps"):
        for dep in ctx.rule.attr.deps:
            if hasattr(dep, "node_sources"):
                result = depset(transitive = [result, dep.node_sources])

    # Note layering: until we have JS interop providers, this needs to know how to
    # get TypeScript outputs.
    if hasattr(target, "typescript"):
        result = depset(transitive = [result, target.typescript.es5_sources])
    elif hasattr(target, "files"):
        result = depset(
            [f for f in target.files if f.path.endswith(".js")],
            transitive = [result],
        )
    return struct(node_sources = result)

sources_aspect = aspect(
    _sources_aspect_impl,
    attr_aspects = ["deps"],
)
