import { Button } from "@blueprintjs/core";
import * as styles from "df/docs/components/header.css";
import { Search } from "df/docs/components/search";
import * as React from "react";

// tslint:disable-next-line: no-var-requires
const logoImage = require("df/static/images/new_logo_with_text.svg");

export interface IPage {
  name: string;
  path?: string;
}

interface IProps {
  maxWidth?: string;
}

export class Header extends React.Component<IProps> {
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
            <span className={styles.links}>
              <a href="/build-your-dataform-project">
                <Button minimal={true}>Docs</Button>
              </a>
              <a href="/introduction/dataform-in-5-minutes">
                <Button minimal={true}>Tutorial</Button>
              </a>
              <a href="/examples/projects">
                <Button minimal={true}>Examples</Button>
              </a>
              <a href="https://github.com/dataform-co/dataform">
                <Button minimal={true}>GitHub</Button>
              </a>
            </span>
            <span className={styles.navRight}>
              <Search />{" "}
            </span>
          </span>
        </nav>
      </div>
    );
  }
}
