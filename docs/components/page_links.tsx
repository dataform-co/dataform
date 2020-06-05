import React from "react";

import * as styles from "df/docs/components/page_links.css";

export interface IHeaderLink {
  id: string;
  text: string;
}

interface IProps {
  links: IHeaderLink[];
}

export const PageLinks = (props: IProps) => {
  if (!(props.links?.length > 0)) {
    return null;
  }
  return (
    <>
      {<h5 className={styles.onThisPage}>On this page</h5>}
      <ul className={styles.pageLinks}>
        {props.links.map(link => (
          <li key={link.id}>
            <a href={`#${link.id}`}>{link.text}</a>
          </li>
        ))}
      </ul>
    </>
  );
};
