def _gcloud_secret_impl(ctx):
    out = ctx.actions.declare_file(ctx.attr.name)
    ctx.actions.run(
        outputs = [out],
        inputs = [ctx.file.gcloud, ctx.file.ciphertext_file],
        executable = ctx.file.gcloud,
        arguments = [
            "kms",
            "decrypt",
            "--ciphertext-file=%s" % ctx.file.ciphertext_file.path,
            "--plaintext-file=%s" % out.path,
            "--keyring=%s" % ctx.attr.keyring,
            "--key=%s" % ctx.attr.key,
            "--location=%s" % ctx.attr.location,
        ],
        execution_requirements = {
            "local": "1",
        },
    )
    return [DefaultInfo(files = depset([out]))]

gcloud_secret = rule(
    implementation = _gcloud_secret_impl,
    attrs = {
        "gcloud": attr.label(default = "@gcloud_sdk//:bin/gcloud", allow_single_file = True),
        "ciphertext_file": attr.label(allow_single_file = True),
        "keyring": attr.string(default = "", mandatory = True),
        "key": attr.string(default = "", mandatory = True),
        "location": attr.string(default = "global"),
    },
)
