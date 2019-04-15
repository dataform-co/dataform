import * as React from "react";

import Head from "next/head";

import { Footer } from "df/fe/components/footer";
import { Header } from "df/fe/components/header";

import * as styles from "df/docs/layouts/base.css";
import * as globalStyles from "df/fe/global.css";
import * as highlightStyles from "df/fe/highlight.css";

export interface IProps {
  title: string;
}

const PAGES = [];

const MAX_WIDTH = "1500px";

export class BaseLayout extends React.Component<IProps, {}> {
  public render() {
    return (
      <div className={`${globalStyles.root} ${highlightStyles.root}`}>
        <Head>
          <title>{this.props.title}</title>
        </Head>
        <Header pages={PAGES} invert={false} maxWidth={MAX_WIDTH} />
        <div className={styles.container} style={{ maxWidth: MAX_WIDTH }}>
          {this.props.children}
        </div>
      </div>
    );
  }
}
