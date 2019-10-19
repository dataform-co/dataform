def _generate_deploy_script_impl(ctx):
    function_name = ctx.attr.function_name
    npm_package = ctx.attr.npm_package

    out_file = ctx.actions.declare_file(ctx.attr.name)
    npm_package_path = npm_package.label.package + "/" + npm_package.label.name

    ctx.actions.write(
        out_file,
        "gcloud functions deploy %s --source=%s --runtime nodejs10 --trigger-http\n" % (function_name, npm_package_path),
        is_executable = True,
    )
    return [DefaultInfo(files = depset([out_file]))]

generate_deploy_script = rule(
    implementation = _generate_deploy_script_impl,
    attrs = {
        "function_name": attr.string(default = "", doc = "", mandatory = True, values = []),
        "npm_package": attr.label(doc = "", mandatory = True),
    },
    doc = "",
)

def nodejs_gcloud_function(name, function_name, npm_package):
    generate_deploy_script(
        name = name,
        function_name = function_name,
        npm_package = npm_package,
    )

    native.sh_binary(
        name = name + ".deploy",
        srcs = [name],
        data = [npm_package],
    )
