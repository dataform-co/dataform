load("//tools/gcloud:repository_rules.bzl", "gcloud_sdk")

def _gcloud_sdk_extension_impl(ctx):
    gcloud_sdk(name = "gcloud_sdk")

gcloud_sdk_extension = module_extension(
    implementation = _gcloud_sdk_extension_impl,
)
