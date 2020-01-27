def helm_template(name, chart_tar, namespace = "", values = {}, helm_tool = "@helm_tool//:helm"):
    helm_cmd = "$(location %s) template $(location %s)" % (helm_tool, chart_tar)
    if len(namespace) > 0:
        helm_cmd = "%s --namespace %s" % (helm_cmd, namespace)
    for variable, value in values.items():
        helm_cmd = "%s --set %s=%s" % (helm_cmd, variable, value)

    native.genrule(
        name = name,
        srcs = [chart_tar],
        tools = [helm_tool],
        outs = [name + ".yaml"],
        cmd = "%s > $@" % (helm_cmd),
    )
