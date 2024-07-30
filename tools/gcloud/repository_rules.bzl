def _gcloud_sdk_impl(ctx):
    os_build_name = "linux"
    os_platform = 'x86_64'
    if ctx.os.name.startswith("mac"):
        os_build_name = "darwin"
    if ctx.os.arch.startswith('aarch'):
        os_platform = 'arm'
    ctx.download_and_extract(
        "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-%s-%s-%s.tar.gz" % (ctx.attr.version, os_build_name, os_platform),
        stripPrefix = "google-cloud-sdk",
    )
    ctx.file("BUILD", 'exports_files(glob(["bin/*"]))')

gcloud_sdk = repository_rule(
    implementation = _gcloud_sdk_impl,
    attrs = {
        "version": attr.string(default = "412.0.0"),
    },
)
