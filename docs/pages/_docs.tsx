import rehypePrism from "@mapbox/rehype-prism";
import { ICms } from "df/docs/cms";
import { GitHubCms } from "df/docs/cms/github";
import { LocalCms } from "df/docs/cms/local";
import { ITree, Tree } from "df/docs/cms/tree";
import { getContentTree } from "df/docs/content_tree";
import Documentation from "df/docs/layouts/documentation";
import { NextPageContext } from "next";
import * as NodeCache from "node-cache";
import * as React from "react";
import rehypeRaw from "rehype-raw";
import rehypeReact from "rehype-react";
import rehypeSlug from "rehype-slug";
import remark from "remark";
import remarkRehype from "remark-rehype";

interface IQuery {
  version: string;
  path0: string;
  path1: string;
  path2: string;
}

interface IProps {
  index: ITree;
  current: ITree;
  version: string;
}

export class Docs extends React.Component<IProps> {
  public static async getInitialProps({
    query
  }: NextPageContext & { query: IQuery }): Promise<IProps> {
    const path = [query.path0, query.path1, query.path2].filter(part => !!part).join("/");
    const tree = await getContentTree(query.version);
    const current = tree.getChild(path);
    return { index: tree.index(), current, version: query.version };
  }

  public render() {
    return (
      <Documentation
        version={this.props.version}
        current={this.props.current}
        index={this.props.index}
      >
        {this.props.current.content &&
          remark()
            .use(remarkRehype, { allowDangerousHTML: true })
            .use(rehypeSlug)
            .use(rehypePrism)
            .use(rehypeRaw)
            .use(rehypeReact, { createElement: React.createElement })
            .processSync(this.props.current.content).contents}
      </Documentation>
    );
  }
}

export default Docs;
