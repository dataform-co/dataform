import { Tag as BlueprintTag } from "@blueprintjs/core";
import * as styles from "df/components/tag.css";
import * as React from "react";

export const Tag = BlueprintTag;

export const TagList = ({ children }: React.PropsWithChildren<{}>) => (
  <div className={styles.tagList}>{children}</div>
);
