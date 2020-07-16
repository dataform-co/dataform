import { IIntentProps, Intent, Spinner as BlueprintSpinner } from "@blueprintjs/core";
import * as React from "react";

import * as styles from "df/components/spinner.css";

export type SpinnerSize = "large" | "standard" | "small";

// These props don't just extend blueprint's props because defining size using
// the blueprint props requires using Spinner.SIZE_LARGE which is more verbose.
export interface ISpinnerProps extends IIntentProps {
  size?: SpinnerSize;
  value?: number;
}

export function Spinner({ size, ...rest }: ISpinnerProps) {
  return (
    <div className={styles.centered}>
      <BlueprintSpinner
        intent={Intent.PRIMARY}
        size={
          size === "large"
            ? BlueprintSpinner.SIZE_LARGE
            : size === "small"
            ? BlueprintSpinner.SIZE_SMALL
            : BlueprintSpinner.SIZE_STANDARD
        }
        {...rest}
      />
    </div>
  );
}
