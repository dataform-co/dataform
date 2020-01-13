import axios from "axios";
import { IFileTree } from "df/docs/cms";
import { Swagger } from "df/docs/components/swagger";
import Documentation from "df/docs/layouts/documentation";
import { contentTree, localCms } from "df/docs/pages/_docs";
import { NextPageContext } from "next";
import * as React from "react";
import { Spec } from "swagger-schema-official";

interface IQuery {
  version: string;
}

interface IProps {
  version: string;
  index: IFileTree;
  current: IFileTree;
  spec: Spec;
  apiHost: string;
}

export default class WebApiReference extends React.Component<IProps> {
  public static async getInitialProps(
    context: NextPageContext & { query: IQuery }
  ): Promise<IProps> {
    const tree = await contentTree(localCms);
    const apiHost = "api.dataform.co";

    return {
      version: context.query.version || "local",
      index: tree.index(),
      current: tree.get("dataform-web/api-reference"),
      spec: (await axios.get(`https://${apiHost}/swagger.json`)).data,
      apiHost
    };
  }

  public render() {
    return (
      <Documentation
        version={this.props.version}
        current={this.props.current}
        index={this.props.index}
        headerLinks={Swagger.getHeaderLinks(this.props)}
      >
        <Swagger spec={this.props.spec} apiHost={this.props.apiHost}>
          <div className="bp3-callout bp3-icon-info-sign bp3-intent-warning">
            The Dataform Web API is currently in Alpha, and breaking changes are likely to happen.
          </div>
        </Swagger>
      </Documentation>
    );
  }
}
