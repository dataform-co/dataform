import * as React from "react";
import * as styles from "df/docs/components/screenshot_wrapper.css";

export default class ScreenshotWrapper extends React.Component<any, any> {
  public render() {
    return (
      <div className={styles.editor}>
        <div className={`${styles.header}`}>
          <div className={styles.button_group}>
            <span className={`${styles.button} `} />
            <span className={`${styles.button} `} />
            <span className={`${styles.button} `} />
          </div>
        </div>
        <div className={styles.image}>{this.props.children}</div>
      </div>
    );
  }
}
