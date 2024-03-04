load("//tools/gcloud:secrets.bzl", "gcloud_secret")

package(default_visibility = ["//visibility:public"])

gcloud_secret(
    name = "bigquery.json",
    testonly = 1,
    ciphertext_file = ":bigquery.json.enc",
    project = "dataform-public",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
)
