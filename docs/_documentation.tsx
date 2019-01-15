import * as React from "react";
import { styles as commonStyles } from "./common_styles";
import Navigation from "./components/navigation";

export interface Props {
  title: string;
}
export default class Documentation extends React.Component<Props, any> {
  render() {
    return (
      <div style={commonStyles.flexRow}>
        <div style={styles.sidebar} className="hideInline">
          <Navigation />
        </div>
        <div style={styles.mainContent}>
          <h1>{this.props.title}</h1>
          {this.props.children}
        </div>
      </div>
    );
  }
}

export const styles: { [className: string]: React.CSSProperties } = {
  sidebar: {
    width: "280px",
    minWidth: "280px",
    padding: "10px",
    maxHeight: "100%",
    overflowY: "scroll"
  },
  mainContent: {
    borderLeft: "1px solid rgba(120,134,156,0.2)",
    borderRight: "1px solid rgba(120,134,156,0.2)",
    paddingLeft: "30px",
    paddingRight: "30px",
    paddingTop: "30px",
    backgroundColor: "#fff",
    flexGrow: 1
  }
};
