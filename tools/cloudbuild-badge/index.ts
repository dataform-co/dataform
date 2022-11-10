import { Storage } from "@google-cloud/storage";

// https://cloud.google.com/cloud-build/docs/api/reference/rest/v1/projects.builds
interface IBuild {
  status: "SUCCESS" | "FAILURE";
  source: {
    repoSource: {
      repoName: string;
      branchName: string;
    };
  };
}

exports.updateCloudBuildStatusBadge = async (event: { data: string }) => {
  const build: IBuild = JSON.parse(Buffer.from(event.data, "base64").toString());
  if (build.source.repoSource.repoName !== "github_dataform_co_dataform") {
    return;
  }
  if (build.source.repoSource.branchName !== "master") {
    return;
  }
  const storage = new Storage();
  if (build.status === "SUCCESS") {
    await storage
      .bucket("dataform-cloud-build-badges")
      .file("build/success.svg")
      .copy(storage.bucket("dataform-cloud-build-badges").file("build/status.svg"));
    // tslint:disable-next-line: no-console
    console.log("Success badge image copied.");
  } else if (build.status === "FAILURE") {
    await storage
      .bucket("dataform-cloud-build-badges")
      .file("build/failure.svg")
      .copy(storage.bucket("dataform-cloud-build-badges").file("build/status.svg"));
    // tslint:disable-next-line: no-console
    console.log("Failure badge image copied.");
  }
};
