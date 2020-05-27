import { GitHubCms } from "df/tools/markdown-cms/github";
import { LocalCms } from "df/tools/markdown-cms/local";
import { Tree } from "df/tools/markdown-cms/tree";
import NodeCache from "node-cache";

export interface IExtraAttributes {
  icon?: string;
  redirect?: string;
}

export const localCms = new LocalCms("content/docs");

const githubTreeCache = new NodeCache({
  stdTTL: 5 * 60 * 1000 // 5 Minutes.
});

async function getGithubTree(version: string): Promise<Tree<IExtraAttributes>> {
  const cachedTree = githubTreeCache.get(version);
  if (version === "master") {
    if (cachedTree) {
      return cachedTree as Tree<IExtraAttributes>;
    }
  }
  const tree = await new GitHubCms({
    owner: "dataform-co",
    repo: "dataform",
    rootPath: "content/docs",
    ref: version
  }).get();

  if (version === "master") {
    githubTreeCache.set(version, tree);
  }
  return tree;
}

const packageTreesCache = new NodeCache({
  stdTTL: 5 * 60 * 1000 // 5 Minutes.
});
async function getPackageTrees(): Promise<Array<Tree<IExtraAttributes>>> {
  const cachedTree = packageTreesCache.get("packages");
  if (cachedTree) {
    return cachedTree as Array<Tree<IExtraAttributes>>;
  }

  const trees = await Promise.all(
    [
      // Add new package repositories here to add them to the docs.
      {
        owner: "dataform-co",
        repo: "dataform-segment",
        title: "Segment"
      },
      {
        owner: "dataform-co",
        repo: "dataform-bq-audit-logs",
        title: "BigQuery Audit Logs"
      },
      {
        owner: "dataform-co",
        repo: "dataform-scd",
        title: "Slowly changing dimensions"
      }
    ].map(async ({ owner, repo, title }) => {
      const tree = await new GitHubCms<IExtraAttributes>({
        owner,
        repo,
        rootPath: "",
        ref: "master"
      }).get("README.md");

      return new Tree<IExtraAttributes>(
        `packages/${repo}`,
        tree.content,
        {
          title
        },
        tree.editLink
      );
    })
  );

  packageTreesCache.set("packages", trees);

  return trees;
}

export async function getContentTree(version = "local"): Promise<Tree<IExtraAttributes>> {
  const tree = await (version === "local" ? localCms.get() : getGithubTree(version));

  // Add some custom paths to the tree.

  tree.addChild(
    new Tree("", "", {
      title: "Home",
      priority: -1,
      icon: "home"
    })
  );

  tree.addChild(
    new Tree("reference", "", {
      title: "API Reference",
      priority: 10,
      icon: "git-repo"
    })
  );

  tree.getChild("dataform-web").addChild(
    new Tree("dataform-web/api-reference", "", {
      title: "Web API Reference",
      priority: 3
    })
  );

  // Add packages to the tree.

  (await getPackageTrees()).forEach(packageTree => tree.getChild("packages").addChild(packageTree));

  return tree;
}
