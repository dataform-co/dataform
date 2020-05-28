import { Button, Collapse } from "@blueprintjs/core";
import * as styles from "df/components/collapse.css";
import { useState } from "react";
import * as React from "react";

interface ISimpleCollapseProps {
  message: React.ReactNode;
  className?: string;
}

export function SimpleCollapse({
  message,
  className,
  children
}: React.PropsWithChildren<ISimpleCollapseProps>) {
  const [show, updateShow] = useState(false);
  const cssStyles = [styles.messageContainer];
  if (className) {
    cssStyles.push(className);
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
