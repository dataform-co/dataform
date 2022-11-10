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

gcloud_secret(
    name = "redshift.json",
    testonly = 1,
    ciphertext_file = ":redshift.json.enc",
    project = "dataform-public",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
)

gcloud_secret(
    name = "snowflake.json",
    testonly = 1,
    ciphertext_file = ":snowflake.json.enc",
    project = "dataform-public",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
)

gcloud_secret(
    name = "sqldatawarehouse.json",
    testonly = 1,
    ciphertext_file = ":sqldatawarehouse.json.enc",
    project = "dataform-public",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
)
