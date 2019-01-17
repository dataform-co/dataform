import * as React from "react";

export const styles: { [className: string]: React.CSSProperties } = {
  sidebar: {
    width: "280px",
    minWidth: "280px",
    padding: "10px"
  },
  flexColumn: {
    display: "flex",
    flexDirection: "column"
  },

  flexRow: {
    display: "flex",
    flexDirection: "row",
    alignItems: "baseline"
  },

  flexGrow: {
    flexGrow: 1
  }
};
