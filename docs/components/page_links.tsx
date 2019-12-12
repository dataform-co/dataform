import * as styles from "df/docs/components/page_links.css";
import React from "react";

export interface IHeaderLink {
  id: string;
  text: string;
}

interface IProps {
  links: IHeaderLink[];
}

export const PageLinks = (props: IProps) => (
  <ul className={styles.pageLinks}>
    {props.links.map(link => (
      <li key={link.id}>
        <a href={`#${link.id}`}>{link.text}</a>
      </li>
    ))}
  </ul>
);
