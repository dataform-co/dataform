import { Button, Collapse } from "@blueprintjs/core";
import { useState } from "react";
import * as React from "react";

import * as styles from "df/components/collapse.css";

export interface ISimpleCollapseProps {
  message?: React.ReactNode;
  className?: string;
  defaultOpen?: boolean;
  fullWidth?: boolean;
}

export function SimpleCollapse({
  message,
  className,
  children,
  defaultOpen,
  fullWidth = true
}: React.PropsWithChildren<ISimpleCollapseProps>) {
  const [show, updateShow] = useState(!!defaultOpen);
  const cssStyles = [styles.messageContainer];
  if (className) {
    cssStyles.push(className);
  }
  if (fullWidth) {
    cssStyles.push(styles.messageContainerFullWidth);
  }
  return (
    <>
      <div className={cssStyles.join(" ")}>
        {message || <></>}
        {children && (
          <Button
            minimal={true}
            onClick={() => updateShow(!show)}
            icon={show ? "caret-up" : "caret-down"}
            style={{ marginLeft: fullWidth ? "20px" : "10px" }}
          />
        )}
      </div>
      {children && <Collapse isOpen={show}>{children}</Collapse>}
    </>
  );
}
