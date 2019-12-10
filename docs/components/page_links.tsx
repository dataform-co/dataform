import * as styles from "df/docs/components/page_links.css";
import React from "react";

export interface IHeaderLink {
  id: string;
  text: string;
}

interface IProps {
  links: IHeaderLink[];
}

export class PageLinks extends React.Component<IProps> {
  public render() {
    return (
      <ul className={styles.pageLinks}>
        {this.props.links.map(link => (
          <li>
            <a href={`#${link.id}`}>{link.text}</a>
          </li>
        ))}
      </ul>
    );
  }
}
