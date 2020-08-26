load("//tools/gcloud:secrets.bzl", "gcloud_secret")

package(default_visibility = ["//visibility:public"])

gcloud_secret(
    name = "bigquery.json",
    testonly = 1,
    ciphertext_file = ":bigquery.json.enc",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
    project = "dataform-public",
)

gcloud_secret(
    name = "postgres.json",
    testonly = 1,
    ciphertext_file = ":postgres.json.enc",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
    project = "dataform-public",
)

gcloud_secret(
    name = "redshift.json",
    testonly = 1,
    ciphertext_file = ":redshift.json.enc",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
    project = "dataform-public",
)

gcloud_secret(
    name = "snowflake.json",
    testonly = 1,
    ciphertext_file = ":snowflake.json.enc",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
    project = "dataform-public",
)

gcloud_secret(
    name = "sqldatawarehouse.json",
    testonly = 1,
    ciphertext_file = ":sqldatawarehouse.json.enc",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
    project = "dataform-public",
)
