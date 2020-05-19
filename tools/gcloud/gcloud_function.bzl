def _deploy_nodejs_gcloud_function_impl(ctx, trigger_flag):
    function_name = ctx.attr.function_name

    pkg_npm = ctx.attr.pkg_npm
    pkg_npm_path = pkg_npm.label.package + "/" + pkg_npm.label.name
    if len(pkg_npm.label.workspace_root) > 0:
        pkg_npm_path = pkg_npm.label.workspace_root + "/" + pkg_npm_path

    gcloud = ctx.attr.gcloud.label.workspace_root + ctx.attr.gcloud.label.package + "/" + ctx.attr.gcloud.label.name
    file_content = [
        "%s functions deploy %s" % (gcloud, function_name),
        "--source %s" % pkg_npm_path,
        "--runtime nodejs10",
        trigger_flag,
    ]
    runfiles = ctx.runfiles(files = ctx.files.gcloud + ctx.files.pkg_npm)

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
        "gcloud": attr.label(default = "@gcloud_sdk//:google-cloud-sdk/bin/gcloud", allow_single_file = True),
        "function_name": attr.string(default = "", mandatory = True, values = []),
        "pkg_npm": attr.label(mandatory = True),
        "env_vars_file": attr.label(allow_single_file = True),
    },
    executable = True,
)

deploy_pubsub_nodejs_gcloud_function = rule(
    implementation = _deploy_pubsub_nodejs_gcloud_function_impl,
    attrs = {
        "gcloud": attr.label(default = "@gcloud_sdk//:google-cloud-sdk/bin/gcloud", allow_single_file = True),
        "function_name": attr.string(default = "", mandatory = True, values = []),
        "topic_name": attr.string(default = "", mandatory = True, values = []),
        "pkg_npm": attr.label(mandatory = True),
        "env_vars_file": attr.label(allow_single_file = True),
    },
    executable = True,
)
