def _deploy_nodejs_gcloud_function_impl(ctx):
    function_name = ctx.attr.function_name

    npm_package = ctx.attr.npm_package
    npm_package_path = npm_package.label.package + "/" + npm_package.label.name

    file_content = [
        "gcloud functions deploy %s" % function_name,
        "--source %s" % npm_package_path,
        "--runtime nodejs10",
        "--trigger-http",
    ]
    runfiles = ctx.runfiles(files = ctx.files.npm_package)

    env_vars_file = ctx.attr.env_vars_file
    if (env_vars_file):
        env_vars_file_path = env_vars_file.label.package + "/" + env_vars_file.label.name
        file_content.append("--env-vars-file %s" % env_vars_file_path)
        runfiles = runfiles.merge(ctx.runfiles(transitive_files = env_vars_file.files))

    out_file = ctx.actions.declare_file(ctx.attr.name)
    ctx.actions.write(out_file, " ".join(file_content), is_executable = True)

    return [DefaultInfo(executable = out_file, runfiles = runfiles)]

deploy_nodejs_gcloud_function = rule(
    implementation = _deploy_nodejs_gcloud_function_impl,
    attrs = {
        "function_name": attr.string(default = "", mandatory = True, values = []),
        "npm_package": attr.label(mandatory = True),
        "env_vars_file": attr.label(allow_single_file = True),
    },
    executable = True,
)
