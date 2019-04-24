import * as React from "react";

import * as styles from "df/docs/components/footer.css";

const footerLogoImage = require("df/static/images/new_logo_with_text_white.svg");

export interface IProps {
  maxWidth?: string;
}

export class Footer extends React.Component<IProps> {
  public render() {
    const maxWidth = this.props.maxWidth || "1200px";
    return (
      <div className={styles.footer}>
        <div className={styles.footerContent} style={{ maxWidth }}>
          <div className={styles.sitemap}>
            <div className={styles.sitemapContent}>
              <div className={styles.sitemapColumn1}>
                <h4>Product</h4>
                <a href="/platform">
                  <p className={styles.sitemapColumnLink}>Platform</p>
                </a>
                <a href="/developers">
                  <p className={styles.sitemapColumnLink}>Developers</p>
                </a>
                <a href="/pricing">
                  <p className={styles.sitemapColumnLink}>Pricing</p>
                </a>
                <a href="/schedule_demo">
                  <p className={styles.sitemapColumnLink}>Schedule a demo</p>
                </a>
              </div>
              <div className={styles.sitemapColumn2}>
                <h4>About</h4>
                <a rel="noopener" target="_blank" href="https://angel.co/dataform/jobs">
                  <p className={styles.sitemapColumnLink}>Jobs</p>
                </a>
                <a href="/terms">
                  <p className={styles.sitemapColumnLink}>Terms</p>
                </a>
                <a href="/privacy">
                  <p className={styles.sitemapColumnLink}>Privacy</p>
                </a>
                <a href="mailto:team@dataform.co">
                  <p className={styles.sitemapColumnLink}>Contact</p>
                </a>
              </div>
              <div className={styles.sitemapColumn3}>
                <h4>Docs and help</h4>
                <a href="https://docs.dataform.co">
                  <p className={styles.sitemapColumnLink}>Documentation</p>
                </a>
                <a href="mailto:support@dataform.co">
                  <p className={styles.sitemapColumnLink}>Support</p>
                </a>
                <a href="/security">
                  <p className={styles.sitemapColumnLink}>Security</p>
                </a>
              </div>

              {/*<div className={styles.sitemapColumnCta}>
            <h4>Start your free trial</h4>
          </div>
          */}
            </div>
          </div>
        </div>
        <div className={styles.footerLogo} style={{ maxWidth }}>
          <img src={footerLogoImage} className={styles.logo} />
          <div>© 2019 Tada science, Inc.</div>
        </div>
      </div>
    );
  }
}
