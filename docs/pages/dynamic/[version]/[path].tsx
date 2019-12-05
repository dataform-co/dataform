import * as Octokit from "@octokit/rest";
import * as fs from "fs";
import { NextPageContext } from "next";
import * as React from "react";
import remark from "remark";
import remark2react from "remark-react";
import { promisify } from "util";

const octokit = new Octokit();

interface IProps {
  contents: any;
}

export default class Test extends React.Component<IProps> {
  public static async getInitialProps({ query }: NextPageContext): Promise<IProps> {
    let contents: string;
    if (query.version === "local") {
      contents = await promisify(fs.readFile)(`docs/content/${query.path}.md`, "utf8");
    } else {
      const files = await octokit.repos.getContents({
        owner: "dataform-co",
        repo: "dataform",
        path: `docs/content/${query.path}.md`
      });
    }

    return { contents };
  }
  public render() {
    return (
      <div>
        {
          remark()
            .use(remark2react)
            .processSync(this.props.contents).contents
        }
      </div>
    );
  }
}
