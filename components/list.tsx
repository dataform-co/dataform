import * as styles from "df/components/list.css";
import * as React from "react";

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
}

export const ListItem = ({
  disabled,
  left,
  children,
  className,
  right,
  ...rest
}: IListItemProps & Omit<React.HTMLAttributes<HTMLLIElement>, "title">) => {
  const classes = [className, styles.listItem];
  if (disabled) {
    classes.push(styles.listItemDisabled);
  }
  return (
    <li {...rest} className={classes.join(" ")}>
      {left}
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
