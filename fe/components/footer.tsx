import * as React from "react";

import * as styles from "df/fe/components/footer.css";

import { Button, Intent } from "@blueprintjs/core";

export interface IProps {
  maxWidth?: string;
}

const DEFAULT_MAX_WIDTH = "1200px";

export class Footer extends React.Component<IProps> {
  public render() {
    const maxWidthStyle = {
      maxWidth: this.props.maxWidth || DEFAULT_MAX_WIDTH
    };
    return (
      <div className={styles.footer}>
        <div className={styles.footer2} />
        <div className={styles.cta} style={maxWidthStyle}>
          <div className={styles.ctaHeader}>
            <h1>Build a data warehouse you can be proud of</h1>
          </div>
          <div className={styles.ctaDescription}>
            <h4>Connect to your warehouse and publish your first dataset in 5 minutes</h4>
          </div>
          <div className={styles.ctaActions}>
            <span>
              <a href="/signup">
                <Button text="Sign up" intent={Intent.SUCCESS} />
              </a>
            </span>
            <span>
              <a href="/schedule_demo">
                <Button text="Request demo" intent={Intent.PRIMARY} />
              </a>
            </span>
          </div>
          {/*}<div className={styles.ctaFooter}>
            <p>Free 14 day trial, no credit card required</p>
          </div>*/}
        </div>
        <div className={styles.lineBreak} style={maxWidthStyle} />
        <div className={styles.sitemap} style={maxWidthStyle}>
          <div className={styles.sitemapContent}>
          <div className={styles.sitemapColumn1}>
            <h4>Product</h4>
            <a href="/product">
              <p className={styles.sitemapColumnLink}>Product</p>
            </a>
            <a href="/developers">
              <p className={styles.sitemapColumnLink}>Developers</p>
            </a>
            <a href="/pricing">
              <p className={styles.sitemapColumnLink}>Pricing</p>
            </a>
            <a href="https://docs.dataform.co">
              <p className={styles.sitemapColumnLink}>Documentation</p>
            </a>
          </div>
            <div className={styles.sitemapColumn2}>
              <h4>About</h4>
              <a target="_blank" href="https://angel.co/dataform/jobs">
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

            {/*<div className={styles.sitemapColumn3}>
            <h4>Start your free trial</h4>
          </div>
          */}
          </div>
        </div>
      </div>
    );
  }
}
