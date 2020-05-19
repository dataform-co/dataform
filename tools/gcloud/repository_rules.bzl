def _gcloud_sdk_impl(ctx):
    ctx.download("https://sdk.cloud.google.com", "install.sh")
    result = ctx.execute([
        "bash",
        "install.sh",
        "--disable-prompts",
        "--install-dir=.",
    ])
    if result.return_code != 0:
        fail("Failed to install gcloud: " + result.stderr)
    ctx.file("BUILD", 'exports_files(glob(["google-cloud-sdk/bin/*"]))')

gcloud_sdk = repository_rule(
    implementation = _gcloud_sdk_impl,
)
