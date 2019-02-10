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

"""Helper functions to expand paths into runfiles
"""

def expand_location_into_runfiles(ctx, path):
  """Expand a path into runfiles if it contains a $(location).
  If the path has a location expansion, expand it. Otherwise return as-is.
  Args:
    ctx: context
    path: the path to expand
  Returns:
    The expanded path or the original path
  """
  if path.find('$(location') < 0:
    return path
  return expand_path_into_runfiles(ctx, path)

def expand_path_into_runfiles(ctx, path):
  """Expand a path into runfiles.
  Given a file path that might contain a $(location) label expansion,
  provide the path to the file in runfiles.
  See https://docs.bazel.build/versions/master/skylark/lib/ctx.html#expand_location
  Args:
    ctx: context
    path: the path to expand
  Returns:
    The expanded path
  """
  targets = ctx.attr.data if hasattr(ctx.attr, "data") else []
  expanded = ctx.expand_location(path, targets)
  if expanded.startswith('../'):
    return expanded[len('../'):]
  if expanded.startswith(ctx.bin_dir.path):
    expanded = expanded[len(ctx.bin_dir.path + "/"):]
  if expanded.startswith(ctx.genfiles_dir.path):
    expanded = expanded[len(ctx.genfiles_dir.path + "/"):]
  return ctx.workspace_name + "/" + expanded
