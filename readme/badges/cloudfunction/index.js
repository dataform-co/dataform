/**
 * NOTE: This is branched with minor edits from https://github.com/sbsends/cloud-build-badge.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
const { Storage } = require("@google-cloud/storage");

exports.status = (event, callback) => {
  const pubsubMessage = event.data;
  if (pubsubMessage.data) {
    buildResource = JSON.parse(Buffer.from(pubsubMessage.data, "base64").toString());
    if (buildResource.source) {
      if (buildResource.source.repoSource.repoName && buildResource.source.repoSource.branchName) {
        repo = buildResource.source.repoSource.repoName === "github_dataform_co_dataform";
        branch = buildResource.source.repoSource.branchName === "master";
      }
    } else {
      callback();
    }
    if (buildResource.status) {
      status = buildResource.status;
    } else {
      callback();
    }

    const storage = new Storage();
    if (repo && branch && status == "SUCCESS") {
      storage
        .bucket("dataform-cloud-build-badges")
        .file("build/success.svg")
        .copy(storage.bucket("dataform-cloud-build-badges").file("build/status.svg"));
      console.log("switched badge to build success");
    }
    if (repo && branch && status == "FAILURE") {
      storage
        .bucket("dataform-cloud-build-badges")
        .file("build/failure.svg")
        .copy(storage.bucket("dataform-cloud-build-badges").file("build/status.svg"));
      console.log("switched badge to build failure");
    }
  }
  callback();
};
