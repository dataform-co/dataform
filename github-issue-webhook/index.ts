import * as Octokit from "@octokit/rest";
import * as express from "express";
import * as request from "request";

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

const octokit = new Octokit({ auth: "ed6adab9186efa03f1cfde589fbbb2a38cbfe0c0" });

const repoDetails = {
  owner: "dataform-co",
  repo: "dataform"
};

exports.handleStackdriverEvent = async (req: express.Request, res: express.Response) => {
  try {
    const incident: IIncident = req.body.incident;
    switch (incident.state) {
      case "open": {
        await octokit.issues.create({
          ...repoDetails,
          title: issueTitle(incident.incident_id),
          labels: ["stackdriver"]
        });
        res.status(200).send();
        return;
      }
      case "closed": {
        const existingIssues = await octokit.issues.listForRepo({
          ...repoDetails,
          state: "open",
          labels: "stackdriver"
        });
        const openIssue = existingIssues.data.find(
          issue => issue.title === issueTitle(incident.incident_id)
        );
        if (!openIssue) {
          throw new Error(`Could not find open issue for incident: ${incident.incident_id}`);
        }
        await octokit.issues.update({
          ...repoDetails,
          issue_number: openIssue.number,
          state: "closed"
        });
        return;
      }
      default:
        throw new Error(`Unrecognized incident state: ${incident.state}`);
    }
  } catch (e) {
    console.log(e);
    res.status(500).send(e.message);
  }
};

function issueTitle(incidentId: string) {
  return `Stackdriver alert: ${incidentId}`;
}
