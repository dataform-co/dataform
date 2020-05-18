def _gcloud_sdk_impl(ctx):
    ctx.download("https://sdk.cloud.google.com", "install.sh")
    ctx.execute([
        "bash",
        "install.sh",
        "--disable-prompts",
        "--install-dir=.",
    ])
    ctx.file("BUILD", 'exports_files(glob(["google-cloud-sdk/bin/*"]))')

gcloud_sdk = repository_rule(
    implementation = _gcloud_sdk_impl,
)
