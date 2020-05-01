import { ITypedoc, Typedoc } from "df/docs/components/typedoc";
import { getContentTree, IExtraAttributes } from "df/docs/content_tree";
import Documentation from "df/docs/layouts/documentation";
import { ITree } from "df/tools/markdown-cms/tree";
import { readFile } from "fs";
import { NextPageContext } from "next";
import * as React from "react";
import { promisify } from "util";

interface IQuery {
  version: string;
}

interface IProps {
  version: string;
  index: ITree<IExtraAttributes>;
  current: ITree<IExtraAttributes>;
  typedoc: ITypedoc;
}

export class Reference extends React.Component<IProps> {
  public static async getInitialProps(
    context: NextPageContext & { query: IQuery }
  ): Promise<IProps> {
    const typedocFile = await promisify(readFile)("docs/core.typedoc.json", "utf8");
    const typedoc: ITypedoc = JSON.parse(typedocFile);
    const tree = await getContentTree(context.query.version);
    return {
      typedoc,
      index: tree.index(),
      current: tree.getChild("reference"),
      version: context.query.version
    };
  }

  public render() {
    const typedocProps = {
      docs: this.props.typedoc,
      entry: [] as string[]
    };
    return (
      <Documentation
        version={this.props.version}
        current={this.props.current}
        index={this.props.index}
        headerLinks={Typedoc.getHeaderLinks(typedocProps)}
      >
        <Typedoc {...typedocProps} />
      </Documentation>
    );
  }
}

export default Reference;
