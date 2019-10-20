def _generate_deploy_script_impl(ctx):
    function_name = ctx.attr.function_name

    npm_package = ctx.attr.npm_package
    npm_package_path = npm_package.label.package + "/" + npm_package.label.name

    file_content = [
        "gcloud functions deploy %s" % function_name,
        "--source %s" % npm_package_path,
        "--runtime nodejs10",
        "--trigger-http",
    ]

    env_vars_file = ctx.attr.env_vars_file
    if (env_vars_file):
        env_vars_file_path = env_vars_file.label.package + "/" + env_vars_file.label.name
        file_content.append("--env-vars-file %s" % env_vars_file_path)

    out_file = ctx.actions.declare_file(ctx.attr.name)
    ctx.actions.write(out_file, " ".join(file_content), is_executable = True)
    return [DefaultInfo(files = depset([out_file]))]

generate_deploy_script = rule(
    implementation = _generate_deploy_script_impl,
    attrs = {
        "function_name": attr.string(default = "", mandatory = True, values = []),
        "npm_package": attr.label(mandatory = True),
        "env_vars_file": attr.label(allow_single_file = True),
    },
    doc = "",
)

def nodejs_gcloud_function(name, function_name, npm_package, env_vars_file = None):
    generate_deploy_script(
        name = name,
        function_name = function_name,
        npm_package = npm_package,
        env_vars_file = env_vars_file,
    )

    data = [npm_package]
    if (env_vars_file):
        data = data + [env_vars_file]

    native.sh_binary(
        name = name + ".deploy",
        srcs = [name],
        data = data,
    )
