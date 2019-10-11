def _deploy_gcloud_function_impl(ctx):
    function_name = ctx.attr.function_name
    package_dir = ctx.attr.source_path

    ctx.actions.run_shell(
        inputs = [ctx.files.deps],
        command = "gcloud functions deploy %s --runtime nodejs10 --trigger-http --source=%s" %
                  (function_name, package_dir),
    )

    # Tell Bazel that the files to build for this target includes
    # `out_file`.
    return [DefaultInfo()]

deploy_gcloud_function = rule(
    implementation = _deploy_gcloud_function_impl,
    attrs = {
        "function_name": attr.string(default = "", doc = "", mandatory = False, values = []),
        "source_path": attr.string(default = "", doc = "", mandatory = False, values = []),
        "deps": attr.label_list(allow_files = True),
    },
    doc = "",
)
