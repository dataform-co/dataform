import { H6 } from "@blueprintjs/core";
import * as styles from "df/components/breadcrumb.css";
import * as React from "react";

interface IBreadcrumbProps {
  pathParts: Array<{ label: string; onClick: () => void }>;
}

export function BreadCrumb({ pathParts }: IBreadcrumbProps) {
  return (
    <div className={styles.container}>
      {pathParts.map((pathPart, index) => (
        <React.Fragment key={index}>
          <H6
            onClick={() => {
              if (index < pathParts.length - 1) {
                pathPart.onClick();
              }
            }}
          >
            {pathPart.label}
          </H6>
          {index < pathParts.length - 1 && " > "}
        </React.Fragment>
      ))}
    </div>
  );
}
