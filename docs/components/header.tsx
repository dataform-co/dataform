import * as styles from "df/docs/components/header.css";
import { Search } from "df/docs/components/search";
import * as React from "react";

const logoImage = require("df/static/images/new_logo_with_text.svg");

export interface IPage {
  name: string;
  path?: string;
}

interface IProps {
  maxWidth?: string;
}

interface IState {}

export class Header extends React.Component<IProps, IState> {
  public state: IState = {};

  public render() {
    const navClasses = [styles.nav];
    const maxWidth = this.props.maxWidth || "1200px";
    return (
      <div>
        <nav className={navClasses.join(" ")}>
          <span className={styles.navContent} style={{ maxWidth }}>
            <span className={styles.navLeft}>
              <a href="https://dataform.co">
                <img src={logoImage} className={styles.logoImage} />
              </a>
              <a href="/">
                <span className={styles.docs_tag}>docs</span>
              </a>
            </span>
            <span className={styles.navRight}>
              <Search />
              <a target="_blank" rel="noopener" href="https://github.com/dataform-co/dataform">
                <img className={styles.githubLogo} src="/static/images/github_logo.png" />
              </a>
            </span>
          </span>
        </nav>
      </div>
    );
  }
}
