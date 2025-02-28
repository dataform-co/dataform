load("//tools/gcloud:secrets.bzl", "gcloud_secret")

package(default_visibility = ["//visibility:public"])

# You can update the credentials for testing by:
# * Ensuring you have the required permissions (at time of writing, you need to request a grant).
# * Run the "scripts/update_test_credentials" script.
gcloud_secret(
    name = "bigquery.json",
    testonly = 1,
    ciphertext_file = ":bigquery.json.enc",
    project = "dataform-open-source",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
)
