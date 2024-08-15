load("//tools/gcloud:secrets.bzl", "gcloud_secret")

package(default_visibility = ["//visibility:public"])

# You can update the credentials for testing by:
# * Ensuring you have the required permissions (at time of writing, you need to request a grant).
# * Go to the "Service Accounts" page on the "dataform-open-source" GCP project.
# * Expand the "dataform-testing@dataform-open-source.iam.gserviceaccount.com" account.
# * Go to the "Keys" tab, click "Add Key", and select "Create a New Key", in JSON format.
# * Download the key, and run the `dataform init-creds` CLI command on the JSON.
# * Copy the resulting `.df-credentials` file to here as `bigquery.json`.
# * Run the "scripts/create_secret" script.
# TODO(ekrekr): make this process easier, as keys invalidate themselves fairly frequently now.
gcloud_secret(
    name = "bigquery.json",
    testonly = 1,
    ciphertext_file = ":bigquery.json.enc",
    project = "dataform-open-source",
    key = "dataform-builder-key",
    keyring = "dataform-builder-keyring",
)
