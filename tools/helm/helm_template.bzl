def helm_template_impl(ctx):
    inputs = [ctx.file.chart_tar]
    release_name = ctx.attr.name if ctx.attr.release_name == "" else ctx.attr.release_name
    helm_cmd = "%s template %s %s" % (ctx.executable.helm_tool.path, release_name, ctx.file.chart_tar.path)
    if len(ctx.attr.namespace) > 0:
        helm_cmd = "%s --namespace %s" % (helm_cmd, ctx.attr.namespace)
    for variable, value in ctx.attr.values.items():
        helm_cmd = "%s --set %s=%s" % (helm_cmd, variable, value)
    if ctx.file.values_yaml_file != None:
        inputs += [ctx.file.values_yaml_file]
        helm_cmd = "%s --values %s" % (helm_cmd, ctx.file.values_yaml_file.path)

    out = ctx.actions.declare_file(ctx.attr.name + ".yaml")
    ctx.actions.run_shell(
        tools = [ctx.executable.helm_tool],
        inputs = inputs,
        outputs = [out],
        progress_message = "Rendering Helm chart for %s" % ctx.file.chart_tar.path,
        command = "%s > %s" % (helm_cmd, out.path),
    )
    return [DefaultInfo(files = depset([out]))]

helm_template = rule(
    implementation = helm_template_impl,
    attrs = {
        "chart_tar": attr.label(allow_single_file = True, mandatory = True),
        "release_name": attr.string(),
        "namespace": attr.string(),
        "values": attr.string_dict(),
        "values_yaml_file": attr.label(allow_single_file = True),
        "helm_tool": attr.label(
            executable = True,
            cfg = "host",
            allow_single_file = True,
            default = Label("@helm_tool//:helm"),
        ),
    },
)
