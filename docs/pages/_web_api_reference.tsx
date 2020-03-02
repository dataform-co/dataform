import axios from "axios";
import { Swagger } from "df/docs/components/swagger";
import { getContentTree } from "df/docs/content_tree";
import Documentation from "df/docs/layouts/documentation";
import { ITree } from "df/tools/markdown-cms/tree";
import { NextPageContext } from "next";
import * as React from "react";
import { Spec } from "swagger-schema-official";

interface IQuery {
  version: string;
}

interface IProps {
  version: string;
  index: ITree;
  current: ITree;
  spec: Spec;
  apiHost: string;
}

export default class WebApiReference extends React.Component<IProps> {
  public static async getInitialProps(
    context: NextPageContext & { query: IQuery }
  ): Promise<IProps> {
    const tree = await getContentTree(context.query.version);
    const apiHost = "api.dataform.co";
    return {
      index: tree.index(),
      version: context.query.version,
      current: tree.getChild("dataform-web/api-reference"),
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
