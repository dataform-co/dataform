import rehypePrism from "@mapbox/rehype-prism";
import { ICms, IFile, IFileTree, IFrontMatter } from "df/docs/cms";
import { GitHubCms } from "df/docs/cms/github";
import { LocalCms } from "df/docs/cms/local";
import { Tree } from "df/docs/cms/tree";
import Documentation from "df/docs/layouts/documentation";
import * as frontMatter from "front-matter";
import { NextPageContext } from "next";
import { basename } from "path";
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

const localCms = new LocalCms("/home/lewis/workspace/dataform/content/docs");

const gitHubCms = (ref: string) =>
  new GitHubCms({
    owner: "dataform-co",
    repo: "dataform",
    rootPath: "content/docs",
    ref
  });

export class DocsPage extends React.Component<IProps> {
  public static async getInitialProps({
    query
  }: NextPageContext & { query: IQuery }): Promise<IProps> {
    const version = query.version || "local";

    const cms = version === "local" ? localCms : gitHubCms(version);
    const path = [query.path0, query.path1, query.path2].filter(part => !!part).join("/");

    const tree = await Tree.create(cms);
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
        <a
          href={`https://github.com/dataform-co/dataform/blob/master/content/docs/${this.props.current.file.path}.md`}
        >
          edit
        </a>
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
