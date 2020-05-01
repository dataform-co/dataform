import Octokit from "@octokit/rest";
import express from "express";

interface IIncident {
  incident_id: string;
  resource_id: string;
  resource_name: string;
  state: "open" | "closed";
  // Seconds since epoch.
  started_at: number;
  ended_at: number;
  policy_name: string;
  condition_name: string;
  url: string;
  summary: string;
}

exports.handleStackdriverEvent = async (req: express.Request, res: express.Response) => {
  const octokit = new Octokit({ auth: process.env.GITHUB_AUTH_TOKEN });
  const repo = {
    owner: process.env.REPOSITORY_OWNER,
    repo: process.env.REPOSITORY_NAME
  };
  // tslint:disable: no-console
  try {
    const incident: IIncident = req.body.incident;
    switch (incident.state) {
      case "open": {
        const response = await octokit.issues.create({
          ...repo,
          title: issueTitle(incident.incident_id),
          labels: ["stackdriver"],
          body: issueBody(incident)
        });
        console.log(
          `Issue created: https://github.com/${repo.owner}/${repo.repo}/issues/${response.data.number}`
        );
        res.status(200).send();
        return;
      }
      case "closed": {
        const existingIssues = await octokit.issues.listForRepo({
          ...repo,
          state: "open",
          labels: "stackdriver"
        });
        const openIssue = existingIssues.data.find(
          issue => issue.title === issueTitle(incident.incident_id)
        );
        if (!openIssue) {
          throw new Error(`Could not find open issue for incident: ${incident.incident_id}`);
        }
        const response = await octokit.issues.update({
          ...repo,
          issue_number: openIssue.number,
          state: "closed"
        });
        console.log(
          `Issue closed: https://github.com/${repo.owner}/${repo.repo}/issues/${response.data.number}`
        );
        return;
      }
      default:
        throw new Error(`Unrecognized incident state: ${incident.state}`);
    }
  } catch (e) {
    console.error(e);
    res.status(500).send(e.message);
  }
  // tslint:enable
};

function issueTitle(incidentId: string) {
  return `Stackdriver alert: ${incidentId}`;
}

function issueBody(incident: IIncident) {
  return `Condition "${incident.condition_name}": ${incident.url}
Summary:
${incident.summary}`;
}
