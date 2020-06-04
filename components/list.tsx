import * as React from "react";

import * as styles from "df/components/list.css";

interface IListProps extends React.HTMLProps<HTMLUListElement> {
  condensed?: boolean;
}

export const List = ({ condensed, ...rest }: IListProps) => (
  <ul
    {...rest}
    className={`${styles.list}  ${condensed ? styles.listCondensed : ""} ${rest.className || ""}`}
  >
    {rest.children}
  </ul>
);

interface IListItemProps {
  left?: React.ReactNode;
  disabled?: boolean;
  right?: React.ReactNode;
  leftFlex?: "vertical" | "horizontal";
}

export const ListItem = ({
  disabled,
  left,
  children,
  className,
  right,
  leftFlex,
  ...rest
}: IListItemProps & Omit<React.HTMLAttributes<HTMLLIElement>, "title">) => {
  const classes = [className, styles.listItem];
  if (disabled) {
    classes.push(styles.listItemDisabled);
  }
  return (
    <li {...rest} className={classes.join(" ")}>
      {leftFlex === "vertical" ? <div className={styles.verticalListItemFlex}>{left}</div> : left}
      {children}
      {right && (
        <>
          <div className={styles.listItemFill} />
          {right}
        </>
      )}
    </li>
  );
};
