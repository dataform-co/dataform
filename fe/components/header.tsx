import * as React from "react";

import * as styles from "df/fe/components/header.css";

import { Button, Intent } from "@blueprintjs/core";

import MenuCloseIcon from "df/static/images/menu_close.svg";
import MenuIcon from "df/static/images/menu_icon.svg";
import MenuIconWhite from "df/static/images/menu_icon_white.svg";
import LogoImage from "df/static/images/new_logo_with_text.svg";
import LogoImageWhite from "df/static/images/new_logo_with_text_white.svg";

export interface IPage {
  name: string;
  path?: string;
}

interface IProps {
  pages?: IPage[];
  currentPath?: string;
  invert?: boolean;
  maxWidth?: string;
}

interface IState {
  showFixedNav?: boolean;
  showMobileMenu?: boolean;
}

export class Header extends React.Component<IProps, IState> {
  public state: IState = {};

  public handleScroll = e => {
    this.setState({ showFixedNav: window.scrollY > 100 });
  };

  public componentDidMount() {
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
    const { showFixedNav } = this.state;
    const { invert } = this.props;

    if (showFixedNav) {
      navClasses.push(styles.navScrolled);
    } else if (invert) {
      navClasses.push(styles.navInverted);
    }
    const maxWidth = this.props.maxWidth || "1200px";
    return (
      <div>
        <div className={styles.hideMobile}>
          <nav className={navClasses.join(" ")}>
            <span className={styles.navContent} style={{ maxWidth }}>
              <span>
                <a href="/">{this.createColouredLogo(invert, showFixedNav)}</a>
              </span>
              <span className={styles.pages}>
                {this.props.pages.map(page => (
                  <span
                    key={page.name}
                    id={page.name}
                    className={this.props.currentPath === page.path ? styles.pageSelected : ""}
                  >
                    <a href={page.path}>{page.name}</a>
                  </span>
                ))}
              </span>
              <span className={styles.actions}>
                <span>
                  <a href="https://app.eu.dataform.co">
                    <Button text="Log in" intent={Intent.PRIMARY} className={"ghost"} />
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
                  <a href="/">{this.createColouredLogo(invert, showFixedNav)}</a>
                </span>
              </span>
              <span className={styles.actions}>
                <span onClick={this.handleMobileMenuChange}>
                  {this.createColouredMenuLogo(invert, showFixedNav)}
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
                      <img src={LogoImageWhite} className={styles.logoImageWhite} />
                    </a>
                  </span>
                </span>
                <span className={styles.actions}>
                  <span onClick={this.handleMobileMenuChange}>
                    <img src={MenuCloseIcon} className={styles.menuCloseIcon} />
                  </span>
                </span>
              </nav>
              <div className={styles.mobileMenu}>
                {this.props.pages.map(page => (
                  <React.Fragment key={page.path}>
                    <div className={styles.mobileMenuDivider} />
                    <a href={page.path}>
                      <div className={styles.mobileMenuItem} id={page.name}>
                        {page.name}
                      </div>
                    </a>
                  </React.Fragment>
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

  // TODO just use the fill property in the svg to do this
  private createColouredLogo = (invert: boolean, showFixedNav: boolean) => {
    return invert && !showFixedNav ? (
      <img src={LogoImageWhite} className={styles.logoImage} />
    ) : (
      <img src={LogoImage} className={styles.logoImage} />
    );
  };

  private createColouredMenuLogo = (invert: boolean, showFixedNav: boolean) => {
    return invert && !showFixedNav ? <img src={MenuIconWhite} /> : <img src={MenuIcon} />;
  };
}
