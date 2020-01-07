import rehypePrism from "@mapbox/rehype-prism";
import { ICms, IFileTree } from "df/docs/cms";
import { GitHubCms } from "df/docs/cms/github";
import { LocalCms } from "df/docs/cms/local";
import { Tree } from "df/docs/cms/tree";
import Documentation from "df/docs/layouts/documentation";
import { NextPageContext } from "next";
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
  index: IFileTree;
  current: IFileTree;
  version: string;
}

export const localCms = new LocalCms("content/docs");

const gitHubCms = (ref: string) =>
  new GitHubCms({
    owner: "dataform-co",
    repo: "dataform",
    rootPath: "content/docs",
    ref
  });

export async function contentTree(cms: ICms) {
  const tree = await Tree.create(cms);

  // Add some custom paths to the tree.
  tree.addChild({
    attributes: {
      title: "API Reference",
      priority: 3
    },
    file: {
      path: "reference",
      hasChildren: true
    },
    content: "",
    children: []
  });

  return tree;
}

export class Docs extends React.Component<IProps> {
  public static async getInitialProps({
    query
  }: NextPageContext & { query: IQuery }): Promise<IProps> {
    const version = query.version;
    const effectiveVersion = query.version || "local";

    const cms = effectiveVersion === "local" ? localCms : gitHubCms(version);
    const path = [query.path0, query.path1, query.path2].filter(part => !!part).join("/");
    const tree = await contentTree(cms);

    const current = tree.get(path);

    return { index: tree.index(), current, version };
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
