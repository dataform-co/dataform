import rehypePrism from "@mapbox/rehype-prism";
import { IHeaderLink } from "df/docs/components/page_links";
import * as styles from "df/docs/components/typedoc/index.css";
import * as React from "react";
import rehypeRaw from "rehype-raw";
import rehypeReact from "rehype-react";
import remark from "remark";
import remarkRehype from "remark-rehype";

interface ITypedocSignature {
  parameters: Array<{
    name: string;
    type: ITypedocType;
  }>;
  type: ITypedocType;
}

interface ITypedocType {
  type: string;
  name: string;
  types: ITypedocType[];
  elementType: ITypedocType;
  value: string;
  declaration: {
    signatures: ITypedocSignature[];
  };
}

interface ITypedocComment {
  shortText: string;
  text: string;
}

export interface ITypedoc {
  kind: number;
  name: string;
  children: ITypedoc[];
  type: ITypedocType;
  comment: ITypedocComment;
  indexSignature: ITypedocSignature[];
}

interface IProps {
  docs: ITypedoc;
  entry: string[];
}

export class Typedoc extends React.Component<IProps> {
  public static getHeaderLinks(props: IProps): IHeaderLink[] {
    // Traverse the whole tree looking for children.
    const links: IHeaderLink[] = [];
    const queue = [props.docs];
    while (queue.length > 0) {
      const next = queue.shift();
      // Top level or module types.
      if (next.kind === 1 || next.kind === 0) {
        (next.children || []).forEach(child => queue.push(child));
      } else {
        links.push({ id: next.name, text: next.name });
      }
    }
    return links;
  }

  public render() {
    const { docs } = this.props;
    return <Module {...docs} />;
  }
}

const Module = (props: ITypedoc) => (
  <>
    {props.children &&
      props.children.map((child, i) =>
        child.kind === 1 ? <Module key={i} {...child} /> : <NonModule key={i} {...child} />
      )}
  </>
);

const NonModule = (props: ITypedoc) => (
  <div>
    <h2 id={props.name}>{props.name}</h2>
    {(props.kind === 256 || props.kind === 128) && <Interface {...(props as any)} />}
    {props.kind === 4194304 && <Type {...props.type} />}
  </div>
);

const Interface = (props: ITypedoc) => (
  <div className={styles.definition}>
    {props.comment && <Comment {...props.comment} />}
    {props.children && props.children.map((child, i) => <Property key={i} {...child} />)}
    {props.indexSignature &&
      props.indexSignature.map((indexSignature, i) => (
        <IndexSignature key={i} {...indexSignature} />
      ))}
  </div>
);

const Property = (props: any) => (
  <div className={styles.property}>
    <div className={styles.propertyName}>
      <code>{props.name}</code>
    </div>
    <div>
      <Type {...props.type} />
      <div className={styles.propertyComment}>
        <Comment {...props.comment} />
      </div>
    </div>
  </div>
);

const Type = (props: ITypedocType) => {
  return (
    <code>
      <SubType {...props} />
    </code>
  );
};

const SubType = (props: ITypedocType) => {
  if (props.type === "union") {
    const subTypes = props.types.map((type: any, i: number) => <SubType key={i} {...type} />);
    return (
      <>{subTypes.reduce((acc, curr) => (acc.length === 0 ? [curr] : [...acc, " | ", curr]), [])}</>
    );
  }
  if (props.type === "stringLiteral") {
    return <>"{props.value}"</>;
  }
  if (props.type === "reflection" && props.declaration && props.declaration.signatures) {
    return (
      <>
        {props.declaration.signatures.map((signature, i) => (
          <React.Fragment key={i}>
            (
            {signature.parameters &&
              signature.parameters
                .map((parameter, i) => (
                  <React.Fragment key={i}>
                    {parameter.name}: <SubType {...parameter.type} />
                  </React.Fragment>
                ))
                .reduce((acc, curr) => (acc.length === 0 ? [curr] : [...acc, ", ", curr]), [])}
            ) => <SubType {...signature.type} />
          </React.Fragment>
        ))}
      </>
    );
  }
  const elementProps = props.type === "array" ? props.elementType : props;
  const typeSuffix = props.type === "array" ? "[]" : "";
  if (elementProps.type === "reference") {
    return (
      <a href={`#${elementProps.name}`}>
        {elementProps.name}
        {typeSuffix}
      </a>
    );
  }
  return (
    <>
      {elementProps.name}
      {typeSuffix}
    </>
  );
};

const IndexSignature = (props: ITypedocSignature) => (
  <code>
    {"{ "}[{props.parameters[0].name}]: <SubType {...props.type} />
    {" }"}
  </code>
);

const Comment = (props: ITypedocComment) => (
  <>
    {props && props.shortText && (
      <div>
        <Markdown content={props.shortText} />
      </div>
    )}
    {props && props.text && (
      <div>
        <Markdown content={props.text} />
      </div>
    )}
  </>
);

const Markdown = (props: { content: string }) =>
  remark()
    .use(remarkRehype, { allowDangerousHTML: true })
    .use(rehypePrism)
    .use(rehypeRaw)
    .use(rehypeReact, { createElement: React.createElement })
    .processSync(props.content).contents;
