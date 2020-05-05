load("@build_bazel_rules_nodejs//:providers.bzl", "DeclarationInfo", "JSEcmaScriptModuleInfo", "NodeContextInfo", "NpmPackageInfo", "node_modules_aspect", "run_node")
load("@build_bazel_rules_nodejs//internal/linker:link_node_modules.bzl", "module_mappings_aspect")

# This is a fork of https://github.com/bazelbuild/rules_nodejs/blob/master/packages/rollup/src/rollup_bundle.bzl
# with a few small changes to support bundling .d.ts files. Instead of collecting es6 sources, it collects
# typescript declarations. It also changes the logic for resolving js inputs to support resolving .d.ts files instead.
# TODO: Move this upstream into rules_nodejs.

_DOC = """Runs the Rollup.js CLI under Bazel.
See https://rollupjs.org/guide/en/#command-line-reference
Typical example:
```python
load("@npm_bazel_rollup//:index.bzl", "rollup_bundle")
rollup_bundle(
    name = "bundle",
    srcs = ["dependency.js"],
    entry_point = "input.js",
    config_file = "rollup.config.js",
)
```
Note that the command-line options set by Bazel override what appears in the rollup config file.
This means that typically a single `rollup.config.js` can contain settings for your whole repo,
and multiple `rollup_bundle` rules can share the configuration.
Thus, setting options that Bazel controls will have no effect, e.g.
```javascript
module.exports = {
    output: { file: 'this_is_ignored.js' },
}
```
You must determine ahead of time whether Rollup needs to produce a directory output.
This is the case if you have dynamic imports which cause code-splitting, or if you
provide multiple entry points. Use the `output_dir` attribute to specify that you want a
directory output.
Rollup's CLI has the same behavior, forcing you to pick `--output.file` or `--output.dir`.
To get multiple output formats, wrap the rule with a macro or list comprehension, e.g.
```python
[
    rollup_bundle(
        name = "bundle.%s" % format,
        entry_point = "foo.js",
        format = format,
    )
    for format in [
        "cjs",
        "umd",
    ]
]
```
This will produce one output per requested format.
"""

_ROLLUP_ATTRS = {
    "srcs": attr.label_list(
        doc = """Non-entry point JavaScript source files from the workspace.
You must not repeat file(s) passed to entry_point/entry_points.
""",
        # Don't try to constrain the filenames, could be json, svg, whatever
        allow_files = True,
    ),
    "args": attr.string_list(
        doc = """Command line arguments to pass to rollup. Can be used to override config file settings.
These argument passed on the command line before all arguments that are always added by the
rule such as `--output.dir` or `--output.file`, `--format`, `--config` and `--preserveSymlinks` and
also those that are optionally added by the rule such as `--sourcemap`.
See rollup CLI docs https://rollupjs.org/guide/en/#command-line-flags for complete list of supported arguments.""",
        default = [],
    ),
    "config_file": attr.label(
        doc = """A rollup.config.js file
Passed to the --config 
See https://rollupjs.org/guide/en/#configuration-files
If not set, a default basic Rollup config is used.
""",
        allow_single_file = True,
        default = "@npm_bazel_rollup//:rollup.config.js",
    ),
    "entry_point": attr.label(
        doc = """The bundle's entry point (e.g. your main.js or app.js or index.js).
This is just a shortcut for the `entry_points` attribute with a single output chunk named the same as the rule.
For example, these are equivalent:
```python
rollup_bundle(
    name = "bundle",
    entry_point = "index.js",
)
```
```python
rollup_bundle(
    name = "bundle",
    entry_points = {
        "index.js": "bundle"
    }
)
```
If `rollup_bundle` is used on a `ts_library`, the `rollup_bundle` rule handles selecting the correct outputs from `ts_library`.
In this case, `entry_point` can be specified as the `.ts` file and `rollup_bundle` will handle the mapping to the `.mjs` output file.
For example:
```python
ts_library(
    name = "foo",
    srcs = [
        "foo.ts",
        "index.ts",
    ],
)
rollup_bundle(
    name = "bundle",
    deps = [ "foo" ],
    entry_point = "index.ts",
)
```
""",
        allow_single_file = True,
    ),
    "entry_points": attr.label_keyed_string_dict(
        doc = """The bundle's entry points (e.g. your main.js or app.js or index.js).
Passed to the [`--input` option](https://github.com/rollup/rollup/blob/master/docs/999-big-list-of-options.md#input) in Rollup.
Keys in this dictionary are labels pointing to .js entry point files.
Values are the name to be given to the corresponding output chunk.
Either this attribute or `entry_point` must be specified, but not both.
""",
        allow_files = True,
    ),
    "format": attr.string(
        doc = """"Specifies the format of the generated bundle. One of the following:
- `amd`: Asynchronous Module Definition, used with module loaders like RequireJS
- `cjs`: CommonJS, suitable for Node and other bundlers
- `esm`: Keep the bundle as an ES module file, suitable for other bundlers and inclusion as a `<script type=module>` tag in modern browsers
- `iife`: A self-executing function, suitable for inclusion as a `<script>` tag. (If you want to create a bundle for your application, you probably want to use this.)
- `umd`: Universal Module Definition, works as amd, cjs and iife all in one
- `system`: Native format of the SystemJS loader
""",
        values = ["amd", "cjs", "esm", "iife", "umd", "system", "es"],
        default = "esm",
    ),
    "node_context_data": attr.label(
        default = "@build_bazel_rules_nodejs//internal:node_context_data",
        providers = [NodeContextInfo],
        doc = "Internal use only",
    ),
    "output_dir": attr.bool(
        doc = """Whether to produce a directory output.
We will use the [`--output.dir` option](https://github.com/rollup/rollup/blob/master/docs/999-big-list-of-options.md#outputdir) in rollup
rather than `--output.file`.
If the program produces multiple chunks, you must specify this attribute.
Otherwise, the outputs are assumed to be a single file.
""",
    ),
    "rollup_bin": attr.label(
        doc = "Target that executes the rollup binary",
        executable = True,
        cfg = "host",
        default = "@npm//rollup/bin:rollup",
    ),
    "rollup_worker_bin": attr.label(
        doc = "Internal use only",
        executable = True,
        cfg = "host",
        # NB: will be substituted with "@npm//@bazel/rollup/bin:rollup-worker" when the pkg_npm target is built
        default = "@npm//@bazel/rollup/bin:rollup-worker",
    ),
    "silent": attr.bool(
        doc = """Whether to execute the rollup binary with the --silent flag, defaults to False.
Using --silent can cause rollup to [ignore errors/warnings](https://github.com/rollup/rollup/blob/master/docs/999-big-list-of-options.md#onwarn) 
which are only surfaced via logging.  Since bazel expects printing nothing on success, setting silent to True
is a more Bazel-idiomatic experience, however could cause rollup to drop important warnings.
""",
    ),
    "sourcemap": attr.string(
        doc = """Whether to produce sourcemaps.
Passed to the [`--sourcemap` option](https://github.com/rollup/rollup/blob/master/docs/999-big-list-of-options.md#outputsourcemap") in Rollup
""",
        default = "inline",
        values = ["inline", "hidden", "true", "false"],
    ),
    "supports_workers": attr.bool(
        doc = """Experimental! Use only with caution.
Allows you to enable the Bazel Worker strategy for this library.
When enabled, this rule invokes the "rollup_worker_bin"
worker aware binary rather than "rollup_bin".""",
        default = False,
    ),
    "deps": attr.label_list(
        aspects = [module_mappings_aspect, node_modules_aspect],
        doc = """Other libraries that are required by the code, or by the rollup.config.js""",
    ),
}

def _desugar_entry_point_names(name, entry_point, entry_points):
    """Users can specify entry_point (sugar) or entry_points (long form).
    This function allows our code to treat it like they always used the long form.
    It also performs validation:
    - exactly one of these attributes should be specified
    """
    if entry_point and entry_points:
        fail("Cannot specify both entry_point and entry_points")
    if not entry_point and not entry_points:
        fail("One of entry_point or entry_points must be specified")
    if entry_point:
        return [name]
    return entry_points.values()

def _desugar_entry_points(name, entry_point, entry_points, inputs):
    """Like above, but used by the implementation function, where the types differ.
    It also performs validation:
    - attr.label_keyed_string_dict doesn't accept allow_single_file
      so we have to do validation now to be sure each key is a label resulting in one file
    It converts from dict[target: string] to dict[file: string]
    """
    names = _desugar_entry_point_names(name, entry_point.label if entry_point else None, entry_points)

    if entry_point:
        return {_resolve_js_input(entry_point.files.to_list()[0], inputs): names[0]}

    result = {}
    for ep in entry_points.items():
        entry_point = ep[0]
        name = ep[1]
        f = entry_point.files.to_list()
        if len(f) != 1:
            fail("keys in rollup_bundle#entry_points must provide one file, but %s has %s" % (entry_point.label, len(f)))
        result[_resolve_js_input(f[0], inputs)] = name
    return result

def _resolve_js_input(f, inputs):
    if f.extension == "js" or f.extension == "mjs":
        return f

    # look for corresponding js file in inputs
    no_ext = _no_ext(f)
    for i in inputs:
        if i.extension == "js" or i.extension == "mjs" or i.extension == "ts":
            if _no_ext(i) == no_ext or _no_ext(i) == (no_ext + ".d"):
                return i

    return f

def _rollup_outs(sourcemap, name, entry_point, entry_points, output_dir):
    """Supply some labelled outputs in the common case of a single entry point"""
    result = {}
    entry_point_outs = _desugar_entry_point_names(name, entry_point, entry_points)
    if output_dir:
        # We can't declare a directory output here, because RBE will be confused, like
        # com.google.devtools.build.lib.remote.ExecutionStatusException:
        # INTERNAL: failed to upload outputs: failed to construct CAS files:
        # failed to calculate file hash:
        # read /b/f/w/bazel-out/k8-fastbuild/bin/packages/rollup/test/multiple_entry_points/chunks: is a directory
        #result["chunks"] = output_dir
        return {}
    else:
        if len(entry_point_outs) > 1:
            fail("Multiple entry points require that output_dir be set")
        out = entry_point_outs[0]
        result[out] = out + ".ts"
        if sourcemap == "true":
            result[out + "_map"] = "%s.map" % result[out]
    return result

def _no_ext(f):
    return f.short_path[:-len(f.extension) - 1]

def _filter_js(files):
    return [f for f in files if f.extension == "js" or f.extension == "mjs"]

def _rollup_bundle(ctx):
    "Generate a rollup config file and run rollup"

    # rollup_bundle supports deps with JS providers. For each dep,
    # JSEcmaScriptModuleInfo is used if found, then JSModuleInfo and finally
    # the DefaultInfo files are used if the former providers are not found.
    deps_depsets = []
    for dep in ctx.attr.deps:
        if DeclarationInfo in dep:
            deps_depsets.append(dep[DeclarationInfo].transitive_declarations)

        # if JSEcmaScriptModuleInfo in dep:
        #     deps_depsets.append(dep[JSEcmaScriptModuleInfo].sources)
        # elif hasattr(dep, "files"):
        #     deps_depsets.append(dep.files)

        # Also include files from npm deps as inputs.
        # These deps are identified by the NpmPackageInfo provider.
        if NpmPackageInfo in dep:
            deps_depsets.append(dep[NpmPackageInfo].sources)
    deps_inputs = depset(transitive = deps_depsets).to_list()

    inputs = _filter_js(ctx.files.entry_point) + _filter_js(ctx.files.entry_points) + ctx.files.srcs + deps_inputs
    outputs = [getattr(ctx.outputs, o) for o in dir(ctx.outputs)]

    # See CLI documentation at https://rollupjs.org/guide/en/#command-line-reference
    args = ctx.actions.args()

    if ctx.attr.supports_workers:
        # Set to use a multiline param-file for worker mode
        args.use_param_file("@%s", use_always = True)
        args.set_param_file_format("multiline")

    # Add user specified arguments *before* rule supplied arguments
    args.add_all(ctx.attr.args)

    # List entry point argument first to save some argv space
    # Rollup doc says
    # When provided as the first options, it is equivalent to not prefix them with --input
    entry_points = _desugar_entry_points(ctx.label.name, ctx.attr.entry_point, ctx.attr.entry_points, inputs).items()

    # If user requests an output_dir, then use output.dir rather than output.file
    if ctx.attr.output_dir:
        outputs.append(ctx.actions.declare_directory(ctx.label.name))
        for entry_point in entry_points:
            args.add_joined([entry_point[1], entry_point[0]], join_with = "=")
        args.add_all(["--output.dir", outputs[0].path])
    else:
        args.add(entry_points[0][0])
        args.add_all(["--output.file", outputs[0].path])

    args.add_all(["--format", ctx.attr.format])

    if ctx.attr.silent:
        # Run the rollup binary with the --silent flag
        args.add("--silent")

    stamp = ctx.attr.node_context_data[NodeContextInfo].stamp

    config = ctx.actions.declare_file("_%s.rollup_config.js" % ctx.label.name)
    ctx.actions.expand_template(
        template = ctx.file.config_file,
        output = config,
        substitutions = {
            "bazel_stamp_file": "\"%s\"" % ctx.version_file.path if stamp else "undefined",
        },
    )

    args.add_all(["--config", config.path])
    inputs.append(config)

    if stamp:
        inputs.append(ctx.version_file)

    # Prevent rollup's module resolver from hopping outside Bazel's sandbox
    # When set to false, symbolic links are followed when resolving a file.
    # When set to true, instead of being followed, symbolic links are treated as if the file is
    # where the link is.
    args.add("--preserveSymlinks")

    if (ctx.attr.sourcemap and ctx.attr.sourcemap != "false"):
        args.add_all(["--sourcemap", ctx.attr.sourcemap])

    executable = "rollup_bin"
    execution_requirements = {}

    if ctx.attr.supports_workers:
        executable = "rollup_worker_bin"
        execution_requirements["supports-workers"] = str(int(ctx.attr.supports_workers))

    run_node(
        ctx,
        progress_message = "Bundling JavaScript %s [rollup]" % outputs[0].short_path,
        executable = executable,
        inputs = inputs,
        outputs = outputs,
        arguments = [args],
        mnemonic = "Rollup",
        execution_requirements = execution_requirements,
        env = {"COMPILATION_MODE": ctx.var["COMPILATION_MODE"]},
    )

    return [
        DefaultInfo(files = depset(outputs)),
    ]

rollup_bundle_dts = rule(
    doc = _DOC,
    implementation = _rollup_bundle,
    attrs = _ROLLUP_ATTRS,
    outputs = _rollup_outs,
)
