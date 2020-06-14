import { H6 } from "@blueprintjs/core";
import * as React from "react";

import * as styles from "df/components/breadcrumb.css";

export interface IBreadCrumbProps {
  pathParts: Array<{ label: string; onClick: () => void }>;
}

export function BreadCrumb({ pathParts }: IBreadCrumbProps) {
  return (
    <div className={styles.container}>
      {pathParts.slice(0, -1).map((pathPart, index) => (
        <React.Fragment key={index}>
          <H6>
            <a onClick={pathPart.onClick}>{pathPart.label}</a>
          </H6>
          {"  >  "}
        </React.Fragment>
      ))}
      <H6>{pathParts.slice(-1)[0].label}</H6>
    </div>
  );
}
