load("@rules_nodejs//nodejs:repositories.bzl", "nodejs_register_toolchains")

def _impl(_module_ctx):
    nodejs_register_toolchains(
        name = "nodejs",
        node_repositories = {
            "24.13.0-darwin_amd64": ("node-v24.13.0-darwin-x64.tar.xz", "node-v24.13.0-darwin-x64", "4ca0a48233f091a2a69ec28dd58e59f394a1b2d4f052b6c6b10f760377fe266f"),
            "24.13.0-darwin_arm64": ("node-v24.13.0-darwin-arm64.tar.xz", "node-v24.13.0-darwin-arm64", "c59a517e9147f25c6167426875a571432f1478c1d7ee7ecc10baa46b0d0e8545"),
            "24.13.0-linux_amd64": ("node-v24.13.0-linux-x64.tar.xz", "node-v24.13.0-linux-x64", "e798599612f4bb71333a3397ab0d095fd62214e115aea45aa858a145fc72d67e"),
            "24.13.0-linux_arm64": ("node-v24.13.0-linux-arm64.tar.xz", "node-v24.13.0-linux-arm64", "e798599612f4bb71333a3397ab0d095fd62214e115aea45aa858a145fc72d67e"),
            "24.13.0-windows_amd64": ("node-v24.13.0-win-x64.zip", "node-v24.13.0-win-x64", "ca2742695be8de44027d71b3f53a4bdb36009b95575fe1ae6f7f0b5ce091cb88"),
        },
        node_version = "24.13.0",
        register = False,
    )

node_ext = module_extension(
    implementation = _impl,
)
