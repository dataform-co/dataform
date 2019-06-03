import * as React from "react";

import * as styles from "df/docs/components/header.css";

const logoImageWhite = require("df/static/images/new_logo_with_text_white.svg");
const logoImage = require("df/static/images/new_logo_with_text.svg");

export interface IPage {
  name: string;
  path?: string;
}

interface IProps {
  pages?: IPage[];
  currentPath?: string;
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

    if (showFixedNav) {
      navClasses.push(styles.navScrolled);
    }

    const maxWidth = this.props.maxWidth || "1200px";
    return (
      <div>
        <nav className={navClasses.join(" ")}>
          <span className={styles.navContent} style={{ maxWidth }}>
            <span>
              <a href="https://dataform.co">
                <img src={logoImage} className={styles.logoImage} />
              </a>
              <a href="/">
                <span className={styles.docs_tag}>docs</span>
              </a>
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
                <a target="_blank" href="https://github.com/dataform-co/dataform">
                  <img className={styles.githubLogo} src="/static/images/github_logo.png" />
                </a>
              </span>
            </span>
          </span>
        </nav>
      </div>
    );
  }
}
