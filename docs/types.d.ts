declare module "dfco/landing/pages/blog/*.mdx" {
  export const meta: { title: string; __filename: string };
  let _default: ((props) => JSX.Element);
  export default _default;
}
declare module "*.mdx" {
  const MDXComponent: (props: any) => JSX.Element;
  export default MDXComponent;
}

declare module "*.svg" {
  const _default: string;
  export default _default;
}

declare module "*.png" {
  const _default: string;
  export default _default;
}