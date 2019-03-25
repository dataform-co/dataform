import * as React from "react";

import * as styles from "df/fe/components/header.css";

import { Button, Intent } from "@blueprintjs/core";

export interface IPage {
  path?: string;
  name: string;
}

interface IProps {
  pages: Array<IPage>;
  pagePath?: string;
  invert?: boolean;
  maxWidth?: string;
}

interface IState {
  scroll?: number;
  showMobileMenu?: boolean;
}

const getPath = (p: IPage) => p.path || p.name.replace(" ", "_");

const DEFAULT_MAX_WIDTH = "1200px";

export class Header extends React.Component<IProps, IState> {
  public state: IState = {};

  public handleScroll = e => {
    this.setState({ scroll: window.scrollY });
  };

  public componentDidMount() {
    this.handleScroll(null);
    window.addEventListener("scroll", this.handleScroll);
  }

  public componentWillUnmount() {
    window.removeEventListener("scroll", this.handleScroll);
  }

  public handleMobileMenuChange = e => {
    this.setState({ showMobileMenu: !this.state.showMobileMenu });
  };

  public render() {
    const navClasses = [styles.nav];
    if (this.state.scroll > 100) {
      navClasses.push(styles.navScrolled);
    } else if (this.props.invert) {
      navClasses.push(styles.navInverted);
    }
    return (
      <div>
        <div className={styles.hideMobile}>
          <nav className={navClasses.join(" ")}>
            <span
              className={styles.navContent}
              style={{ maxWidth: this.props.maxWidth || DEFAULT_MAX_WIDTH }}
            >
              <span>
                <a href="/">
                  <img className={styles.logoImage} />
                </a>
              </span>
              <span className={styles.pages}>
                {this.props.pages.map(p => (
                  <span
                    id={p.name}
                    className={this.props.pagePath === getPath(p) ? styles.pageSelected : ""}
                  >
                    <a href={`/${getPath(p)}`}>{p.name}</a>
                  </span>
                ))}
              </span>
              <span className={styles.actions}>
                <span>
                  <a href="https://app.eu.dataform.co">
                    <Button text="Log in" intent={Intent.PRIMARY} />
                  </a>
                </span>
                <span>
                  <a href="/signup">
                    <Button text="Sign up" intent={Intent.SUCCESS} />
                  </a>
                </span>
              </span>
            </span>
          </nav>
        </div>
        <div className={styles.showMobile}>
          {!this.state.showMobileMenu && (
            <nav className={navClasses.join(" ")}>
              <span className={styles.navContent}>
                <span>
                  <a href="/">
                    <img className={styles.logoImage} />
                  </a>
                </span>
              </span>
              <span className={styles.actions}>
                <span onClick={this.handleMobileMenuChange}>
                  <img className={styles.menuIcon} />
                </span>
              </span>
            </nav>
          )}
          {this.state.showMobileMenu && (
            <div className={styles.mobileMenuContainer}>
              <nav className={styles.nav}>
                <span className={styles.navContent}>
                  <span>
                    <a href="/">
                      <img className={styles.logoImageWhite} />
                    </a>
                  </span>
                </span>
                <span className={styles.actions}>
                  <span onClick={this.handleMobileMenuChange}>
                    <img className={styles.menuCloseIcon} />
                  </span>
                </span>
              </nav>
              <div className={styles.mobileMenu}>
                {this.props.pages.map(p => (
                  <>
                    <div className={styles.mobileMenuDivider} />
                    <a href={`/${getPath(p)}`} key={getPath(p)}>
                      <div className={styles.mobileMenuItem}>{p.name}</div>
                    </a>
                  </>
                ))}
              </div>
              <div className={styles.mobileMenuActions}>
                <div className={styles.mobileMenuAction}>
                  <a href="https://app.eu.dataform.co">
                    <Button text="Log in" intent={Intent.PRIMARY} />
                  </a>
                </div>
                <div className={styles.mobileMenuAction}>
                  <a href="/signup">
                    <Button text="Sign up" intent={Intent.SUCCESS} />
                  </a>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }
}
