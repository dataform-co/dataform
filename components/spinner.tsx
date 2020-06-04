import {
  Intent,
  ISpinnerProps as IBlueprintSpinnerProps,
  Spinner as BlueprintSpinner
} from "@blueprintjs/core";
import * as styles from "df/components/spinner.css";
import * as React from "react";

interface ISpinnerProps extends IBlueprintSpinnerProps {
  small?: boolean;
  large?: boolean;
}

export function Spinner({ small, large, ...rest }: ISpinnerProps) {
  const size = !!large
    ? BlueprintSpinner.SIZE_LARGE
    : !!small
    ? BlueprintSpinner.SIZE_SMALL
    : BlueprintSpinner.SIZE_STANDARD;
  return (
    <div className={styles.centered}>
      <BlueprintSpinner intent={Intent.PRIMARY} size={size} {...rest} />
    </div>
  );
}
