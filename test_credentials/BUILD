load("//tools/gcloud:secrets.bzl", "gcloud_secret_or_local")

package(default_visibility = ["//visibility:public"])

# The credentials from bigquery.json will be used if the file exists; otherwise the default will
# be loaded from bigquery.json.enc.
# You can update the default credentials for testing by:
# * Ensuring you have the required permissions (at time of writing, you need to request a grant).
# * Run the "scripts/update_test_credentials" script.
gcloud_secret_or_local(
    name = "bigquery.json",
    testonly = 1,
    ciphertext_file = ":bigquery.json.enc",
    project = "dataform-open-source",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
)
