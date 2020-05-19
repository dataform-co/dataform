def _helm_tool_impl(ctx):
    os_build_name = "linux-amd64"
    if ctx.os.name.startswith("mac"):
        os_build_name = "darwin-amd64"
    ctx.download_and_extract(
        "https://get.helm.sh/helm-%s-%s.tar.gz" % (ctx.attr.version, os_build_name),
        stripPrefix = os_build_name,
    )
    ctx.file("BUILD", 'exports_files(["helm"])')

helm_tool = repository_rule(
    implementation = _helm_tool_impl,
    attrs = {
        "version": attr.string(default = "v3.0.2"),
    },
)

def _helm_chart_impl(ctx):
    # Make sure the local repository config starts clean.
    ctx.delete("repositories.yaml")
    ctx.delete("repositories.lock")

    # Run 'helm repo add' to add the helm repository to the repository config.
    reponame = ctx.attr.name
    result = ctx.execute([
        ctx.path(Label("@helm_tool//:helm")),
        "repo",
        "add",
        reponame,
        ctx.attr.repo_url,
        "--repository-config=repositories.yaml",
    ])
    if result.return_code != 0:
        fail("Failed to add Helm repository '%s': %s" % (reponame, result.stderr))

    # Run 'helm pull' to fetch the helm chart.
    result = ctx.execute([
        ctx.path(Label("@helm_tool//:helm")),
        "pull",
        reponame + "/" + ctx.attr.chartname,
        "--version",
        ctx.attr.version,
        "--repository-config=repositories.yaml",
    ], timeout = 600)
    if result.return_code != 0:
        fail("Failed to pull Helm chart '%s': %s" % (ctx.attr.chartname, result.stderr))

    # Delete repository config.
    ctx.delete("repositories.yaml")
    ctx.delete("repositories.lock")

    # Rename the downloaded chart to 'chart.tgz'.
    filename = ctx.execute(["ls"]).stdout.strip()
    ctx.execute(["mv", filename, "chart.tgz"])

    ctx.file("BUILD", 'exports_files(["chart.tgz"])')

helm_chart = repository_rule(
    implementation = _helm_chart_impl,
    attrs = {
        "repo_url": attr.string(mandatory = True),
        "chartname": attr.string(mandatory = True),
        "version": attr.string(mandatory = True),
    },
)
