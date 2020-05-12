import * as bp from "@blueprintjs/core";
import * as styles from "df/components/flex.css";
import Resizable from "re-resizable";
import * as React from "react";

export interface IFlexItemProps extends React.HTMLAttributes<HTMLDivElement> {
  width?: number;
  height?: number;
  resizable?: bp.Position[];
  fill?: boolean;
}

export const FlexItem = (props: IFlexItemProps) => {
  const { width, height, resizable, fill, ...remainingProps } = props;
  const classNames = [styles.item, props.className];
  if (props.resizable && props.resizable.length > 0) {
    return (
      <Resizable
        {...remainingProps}
        className={classNames.join(" ")}
        defaultSize={{ width: props.width, height: props.height }}
        minWidth={props.width}
        minHeight={props.height}
        enable={props.resizable.reduce(
          (accumulator, position) => ({ ...accumulator, [position]: true }),
          {}
        )}
      >
        {props.children}
      </Resizable>
    );
  }
  if (props.fill) {
    classNames.push(styles.fill);
  }
  const style = { ...props.style };
  if (props.width) {
    style.width = props.width + "px";
    style.minWidth = style.width;
    style.maxWidth = style.width;
  }
  if (props.height) {
    style.height = props.height + "px";
    style.minHeight = style.height;
    style.maxHeight = style.height;
  }
  return (
    <div className={classNames.join(" ")} {...remainingProps} style={style}>
      {props.children}
    </div>
  );
};

export interface IFlexProps extends React.HTMLAttributes<HTMLDivElement> {
  direction?: "row" | "column";
  children?: Array<React.ReactElement<typeof FlexItem>> | React.ReactElement<typeof FlexItem>;
}

export const Flex = (props: IFlexProps) => {
  const { direction, ...remainingProps } = props;
  const classNames = [styles.flex, styles[props.direction || "row"], props.className];
  return (
    <div {...remainingProps} className={classNames.join(" ")}>
      {props.children}
    </div>
  );
};
