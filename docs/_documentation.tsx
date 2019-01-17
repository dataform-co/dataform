import * as React from "react";
import { styles as commonStyles } from "./common_styles";
import Navigation from "./components/navigation";
import OnThisPage from "./components/onThisPage";
import Method from "./components/method";

export interface Props {
  title: string;
}

export default class Documentation extends React.Component<Props, any> {
  getMenuItems = (child: React.ReactElement<any>) => {
    if (child && child.props && Array.isArray(child.props.children)) {
      const headers = child.props.children.filter(item => item.props.name === "h2").map(item => ({
        id: item.props.props.id,
        text: item.props.children
      }));

      const methods = child.props.children.filter(item => item.type === Method).map(item => ({
        id: item.props.name,
        text: item.props.name
      }));

      return [...headers, ...methods];
    }

    return [];
  };

  render() {
    const menu = this.getMenuItems(this.props.children as React.ReactElement<any>);

    return (
      <div style={{ ...commonStyles.flexRow, ...styles.container }}>
        <div style={styles.sidebar} className="hideInline">
          <Navigation />
        </div>
        <div style={styles.mainContent}>
          <h1>{this.props.title}</h1>
          {this.props.children}
        </div>
        <div style={{ ...styles.sidebar, ...styles.onThisPage }} className="hideInline">
          <OnThisPage menu={menu} />
        </div>
      </div>
    );
  }
}

export const styles: { [className: string]: React.CSSProperties } = {
  container: {
    alignItems: "baseline"
  },
  sidebar: {
    width: "280px",
    minWidth: "280px",
    padding: "10px",
    maxHeight: "100%",
    overflowY: "scroll"
  },
  onThisPage: {
    position: "sticky",
    top: 0
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
