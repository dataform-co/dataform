import { Button, Collapse } from "@blueprintjs/core";
import { useState } from "react";
import * as React from "react";

import * as styles from "df/components/collapse.css";

interface ISimpleCollapseProps {
  message: React.ReactNode;
  className?: string;
  defaultOpen?: boolean;
}

export function SimpleCollapse({
  message,
  className,
  children,
  defaultOpen
}: React.PropsWithChildren<ISimpleCollapseProps>) {
  const [show, updateShow] = useState(!!defaultOpen);
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
