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
  <>
    {props.links?.length > 0 && <h5 className={styles.onThisPage}>On this page</h5>}
    <ul className={styles.pageLinks}>
      {props.links.map(link => (
        <li key={link.id}>
          <a href={`#${link.id}`}>{link.text}</a>
        </li>
      ))}
    </ul>
  </>
);
