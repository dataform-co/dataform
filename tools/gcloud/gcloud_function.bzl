def _fullPath(label):
    path = label.name
    if len(label.package) > 0:
        path = label.package + "/" + path
    if len(label.workspace_root) > 0:
        path = label.workspace_root + "/" + path
    return path

def _deploy_nodejs_gcloud_function_impl(ctx, trigger_flag):
    function_name = ctx.attr.function_name

    file_content = [
        "%s functions deploy %s" % (_fullPath(ctx.attr.gcloud.label), function_name),
        "--project=%s" % ctx.attr.project,
        "--source %s" % ctx.build_file_path[:-6],
        "--runtime nodejs10",
        trigger_flag,
    ]
    runfiles = ctx.runfiles(files = ctx.files.gcloud + ctx.files.srcs)

    env_vars_file = ctx.attr.env_vars_file
    if (env_vars_file):
        env_vars_file_path = _fullPath(env_vars_file.label)
        file_content += ["--env-vars-file", env_vars_file_path]
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
        "gcloud": attr.label(default = "@gcloud_sdk//:bin/gcloud", allow_single_file = True),
        "project": attr.string(default = "", mandatory = True),
        "function_name": attr.string(default = "", mandatory = True, values = []),
        # TODO: This rule currently assumes that all srcs are inside the calling BUILD file's package.
        # We should fix that somehow, possibly by trying to use rollup to obtain a single file, or perhaps
        # by experimenting with uploading a zip file containing all required source files.
        "srcs": attr.label_list(mandatory = True, allow_files = True),
        "env_vars_file": attr.label(allow_single_file = True),
    },
    executable = True,
)

deploy_pubsub_nodejs_gcloud_function = rule(
    implementation = _deploy_pubsub_nodejs_gcloud_function_impl,
    attrs = {
        "gcloud": attr.label(default = "@gcloud_sdk//:bin/gcloud", allow_single_file = True),
        "project": attr.string(default = "", mandatory = True),
        "function_name": attr.string(default = "", mandatory = True, values = []),
        "topic_name": attr.string(default = "", mandatory = True, values = []),
        # TODO: This rule currently assumes that all srcs are inside the calling BUILD file's package.
        # We should fix that somehow, possibly by trying to use rollup to obtain a single file, or perhaps
        # by experimenting with uploading a zip file containing all required source files.
        "srcs": attr.label_list(mandatory = True, allow_files = True),
        "env_vars_file": attr.label(allow_single_file = True),
    },
    executable = True,
)
