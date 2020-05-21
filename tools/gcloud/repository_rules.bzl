def _gcloud_sdk_impl(ctx):
    os_build_name = "linux"
    if ctx.os.name.startswith("mac"):
        os_build_name = "darwin"
    ctx.download_and_extract(
        "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-%s-%s-x86_64.tar.gz" % (ctx.attr.version, os_build_name),
        stripPrefix = "google-cloud-sdk",
    )
    ctx.file("BUILD", 'exports_files(glob(["bin/*"]))')

gcloud_sdk = repository_rule(
    implementation = _gcloud_sdk_impl,
    attrs = {
        "version": attr.string(default = "293.0.0"),
    },
)
