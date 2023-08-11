import { Tag as BlueprintTag } from "@blueprintjs/core";
import * as React from "react";

import * as styles from "df/components/tag.css";

export const Tag = BlueprintTag;

export const TagList = ({ children }: React.PropsWithChildren<{}>) => (
  <div className={styles.tagList}>{children}</div>
);
