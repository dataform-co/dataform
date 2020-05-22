import * as styles from "df/docs/components/footer.css";
import { IExtraAttributes } from "df/docs/content_tree";
import { ITree } from "df/tools/markdown-cms/tree";

export function Footer({ tree }: { tree: ITree<IExtraAttributes> }) {
  return (
    <div>
      <h2>Sitemap</h2>
      <div className={styles.sitemapMasonry}>
        {tree.children
          .filter(subTree => !!subTree.path)
          .map(subTree => {
            return (
              <div key={subTree.path}>
                <a href={subTree.path}>
                  <b>{subTree.attributes.title}</b>
                </a>

                {(subTree.children || []).map(subSubTree => (
                  <div key={subSubTree.path}>
                    <a href={subSubTree.path}>{subSubTree.attributes.title}</a>
                  </div>
                ))}
              </div>
            );
          })}
      </div>
    </div>
  );
}
