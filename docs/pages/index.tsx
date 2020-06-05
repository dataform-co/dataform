import { Button } from "@blueprintjs/core";
import { NextPageContext } from "next";
import * as React from "react";

import { Card, CardActions, CardGrid } from "df/components/card";
import { getContentTree, IExtraAttributes } from "df/docs/content_tree";
import Documentation from "df/docs/layouts/documentation";
import { ITree } from "df/tools/markdown-cms/tree";

interface IQuery {
  version: string;
}

export interface IProps {
  index: ITree<IExtraAttributes>;
  version: string;
}

export class Docs extends React.Component<IProps> {
  public static async getInitialProps(ctx: NextPageContext & { query: IQuery }): Promise<IProps> {
    // Strip trailing slashes and redirect permanently, preserving search params.
    // If our URLs have trailing slashes, then our relative paths break.
    const url = new URL(ctx.asPath, "https://docs.dataform.co");
    if (url.pathname !== "/" && url.pathname.endsWith("/")) {
      ctx.res.writeHead(301, {
        Location: url.pathname.substring(0, url.pathname.length - 1) + url.search
      });
      ctx.res.end();
    }

    const { query } = ctx;
    const tree = await getContentTree(query.version);

    return { index: tree.index(), version: query.version };
  }

  public render() {
    return (
      <Documentation
        version={this.props.version}
        index={this.props.index}
        current={{
          name: "Documentation",
          path: "",
          content: "",
          attributes: {
            title: "Documentation",
            subtitle:
              "Whether you’re a startup or a global enterprise, learn how to use Dataform to manage data in Snowflake, BigQuery and Redshift."
          }
        }}
      >
        <h2>Getting started with Dataform</h2>
        <p>
          Learn about Dataform, discover where it fits in the modern data stack, learn about
          features, and understand how it works.
        </p>
        <CardActions>
         <a href="introduction"><Button intent="primary">Read the introduction</Button></a>
         <a href="introduction/dataform-in-5-minutes"><Button>Understand Dataform and SQLX in 5 min</Button></a>
        </CardActions>

        <h2>Join our community</h2>
        <p>
          Join hundreds of data professionals on our slack channel to ask for advice and
          troubleshoot problems.
        </p>
        <CardActions>
          <a href="https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A">
            <Button intent="primary">Join dataform-users on slack</Button>
          </a>
        </CardActions>
      </Documentation>
    );
  }
}

export default Docs;
