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
