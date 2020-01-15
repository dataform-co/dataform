def _deploy_nodejs_gcloud_function_impl(ctx, trigger_flag):
    function_name = ctx.attr.function_name

    npm_package = ctx.attr.npm_package
    npm_package_path = npm_package.label.package + "/" + npm_package.label.name

    file_content = [
        "gcloud functions deploy %s" % function_name,
        "--source %s" % npm_package_path,
        "--runtime nodejs10",
        trigger_flag,
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

def _deploy_http_nodejs_gcloud_function_impl(ctx):
    return _deploy_nodejs_gcloud_function_impl(ctx, "--trigger-http")

def _deploy_pubsub_nodejs_gcloud_function_impl(ctx):
    topic_name = ctx.attr.topic_name
    return _deploy_nodejs_gcloud_function_impl(ctx, "--trigger-topic %s" % topic_name)

deploy_http_nodejs_gcloud_function = rule(
    implementation = _deploy_http_nodejs_gcloud_function_impl,
    attrs = {
        "function_name": attr.string(default = "", mandatory = True, values = []),
        "pkg_npm": attr.label(mandatory = True),
        "env_vars_file": attr.label(allow_single_file = True),
    },
    executable = True,
)

deploy_pubsub_nodejs_gcloud_function = rule(
    implementation = _deploy_pubsub_nodejs_gcloud_function_impl,
    attrs = {
        "function_name": attr.string(default = "", mandatory = True, values = []),
        "topic_name": attr.string(default = "", mandatory = True, values = []),
        "pkg_npm": attr.label(mandatory = True),
        "env_vars_file": attr.label(allow_single_file = True),
    },
    executable = True,
)
