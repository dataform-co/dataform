import { Button, Collapse } from "@blueprintjs/core";
import * as styles from "df/components/collapse.css";
import { useState } from "react";
import * as React from "react";

interface ISimpleCollapseProps {
  message: React.ReactNode;
  style?: string;
}

export function SimpleCollapse({
  message,
  style,
  children
}: React.PropsWithChildren<ISimpleCollapseProps>) {
  const [show, updateShow] = useState(false);
  const cssStyles = [styles.messageContainer];
  if (style) {
    cssStyles.push(style);
  }
  return (
    <>
      <div className={cssStyles.join(" ")}>
        {message}
        {children && (
          <Button
            minimal={true}
            className={styles.button}
            onClick={() => updateShow(!show)}
            icon={show ? "caret-up" : "caret-down"}
          />
        )}
      </div>
      {children && <Collapse isOpen={show}>{children}</Collapse>}
    </>
  );
}
