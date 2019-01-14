import * as React from "react";
import Document, { Head, Main, NextScript } from "next/document";
import { Navbar, NavbarHeading, NavbarGroup, Button, Alignment } from "@blueprintjs/core";

export default class DefaultDocument extends Document {
  render() {
    return (
      <html>
        <Head>
          <link rel="stylesheet" href="/static/css/blueprint.css" />
          <link rel="stylesheet" href="/static/css/normalize.css" />
          <link rel="stylesheet" href="/static/css/highlight.css" />
          <link rel="stylesheet" href="/static/css/core.css" />
          <link rel="shortcut icon" href="/static/images/logo.png" type="image/png" />
          <script type="text/javascript" src="/static/js/inline.js" />
        </Head>
        <body style={styles.body}>
          <Navbar title="Dataform" fixedToTop className="hideInline">
            <NavbarGroup>
              <img src="/static/images/logo_with_text.svg" style={styles.logo} />
            </NavbarGroup>
            <NavbarGroup align={Alignment.RIGHT}>
              <a style={styles.githubLink} href="https://github.com/tada-science/dataform">
                <img style={styles.githubLogo} src="/static/images/github_logo.png" />
              </a>
            </NavbarGroup>
          </Navbar>
          <div style={styles.contents}>
            <div style={styles.navbarSpacer} className="hideInline" />
            <div >
              <Main />
              <NextScript />
            </div>
          </div>
        </body>
      </html>
    );
  }
}

export const styles: { [className: string]: React.CSSProperties } = {
  body: {
    margin: "0px",
    backgroundColor: "#f8f9fa",
    fontFamily: 'Lato, Roboto, "Helvetica Neue", Arial, sans-serif',
    color: "#495057",
    fontSize: "1rem",
    fontWeight: 400,
    lineHeight: "1.5"
  },
  logo: {
    height: "100%",
    opacity: 0.9
  },
  contents: {
    width: "100%",
    maxWidth: "1200px",
    margin: "auto"
  },
  githubLogo: {
    filter: "invert(100%)",
    height: "100%"
  },
  githubLink: {
    height: "100%",
    padding: "10px"
  },
  navbarSpacer: {
    height: "50px"
  }
};
