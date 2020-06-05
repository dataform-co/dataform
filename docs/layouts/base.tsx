import * as React from "react";

import { Header } from "df/docs/components/header";
import * as globalStyles from "df/docs/global.css";
import * as styles from "df/docs/layouts/base.css";
import Head from "next/head";

export interface IProps {
  title: string;
}

const MAX_WIDTH = "1600px";

export class BaseLayout extends React.Component<IProps, {}> {
  public render() {
    return (
      <div className={`${globalStyles.root}`}>
        <Head>
          <title>{this.props.title}</title>
        </Head>
        <Header maxWidth={MAX_WIDTH} />
        <div className={styles.leftBackground} />
        <div className={styles.container} style={{ maxWidth: MAX_WIDTH }}>
          {this.props.children}
        </div>
        <div className={styles.rightBackground} />
      </div>
    );
  }
}
