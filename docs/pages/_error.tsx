import { NextPageContext } from "next";
import Error from "next/error";
import * as React from "react";

interface IProps {
  statusCode: number;
}

// Resolve issue with Next type defs.
const ErrorAny = Error as any;

export default class ErrorPage extends React.Component<IProps> {
  public static async getInitialProps(ctx: NextPageContext): Promise<IProps> {
    // Strip trailing slashes and redirect permanently, preserving search params.
    const url = new URL(ctx.asPath, "https://docs.dataform.co");
    if (url.pathname !== "/" && url.pathname.endsWith("/")) {
      ctx.res.writeHead(301, {
        Location: url.pathname.substring(0, url.pathname.length - 1) + url.search
      });
      ctx.res.end();
    }

    return { statusCode: ctx.res.statusCode };
  }
  public render() {
    const { statusCode } = this.props;
    return <ErrorAny statusCode={statusCode} />;
  }
}
