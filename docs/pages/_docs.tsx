import { Button, Callout } from "@blueprintjs/core";
import { Code } from "df/docs/components/code";
import { getContentTree, IExtraAttributes } from "df/docs/content_tree";
import Documentation from "df/docs/layouts/documentation";
import { ITree } from "df/tools/markdown-cms/tree";
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

export interface IProps {
  index: ITree<IExtraAttributes>;
  current: ITree<IExtraAttributes>;
  version: string;
}

function MaybeCode(props: React.PropsWithChildren<{ className: string }>) {
  if (props.className?.startsWith("language")) {
    const content = String(props.children).trim();
    const lines = content.split("\n");
    const firstLine = lines[0].trim();
    const matches = firstLine.match(/(\/\/|\-\-)\s+(\S+\.\w+)/);
    if (matches) {
      const fileName = matches[2];
      return <Code fileName={fileName}>{lines.slice(1).join("\n")}</Code>;
    }
    return <Code>{content}</Code>;
  }
  return <code {...props}>{props.children} </code>;
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
    const path = [query.path0, query.path1, query.path2].filter(part => !!part).join("/");
    const tree = await getContentTree(query.version);
    const current = tree.getChild(path);

    if (!current) {
      ctx.res.writeHead(404);
      ctx.res.end();
      return;
    }

    if (current.attributes.redirect) {
      const redirectedUrl = new URL(current.attributes.redirect, url.href);
      ctx.res.writeHead(301, {
        Location: redirectedUrl.pathname + url.search + url.hash
      });
      ctx.res.end();
    }
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
            .use(rehypeRaw)
            .use(rehypeReact, {
              createElement: React.createElement,
              components: { button: Button, callout: Callout, code: MaybeCode }
            })
            .processSync(this.props.current.content).contents}
        {this.props.children}
      </Documentation>
    );
  }
}

export default Docs;
