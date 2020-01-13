import { IHeaderLink } from "df/docs/components/page_links";
import * as styles from "df/docs/components/swagger.css";
import React from "react";
import { Operation as IOperation, Schema, Spec } from "swagger-schema-official";

interface IProps {
  apiHost: string;
  spec: Spec;
}

interface IOperationWithPath extends IOperation {
  path: string;
  method: string;
}

export class Swagger extends React.Component<IProps> {
  public static cleanOperations(spec: Spec) {
    return Object.keys(spec.paths)
      .map(path => {
        const schema = spec.paths[path];
        return [
          { ...schema.post, method: "post" },
          { ...schema.get, method: "get" },
          { ...schema.put, method: "put" },
          { ...schema.delete, method: "delete" }
        ]
          .filter(operation => !!operation.operationId)
          .map(operation => ({ path, ...operation }));
      })
      .reduce((acc, curr) => [...acc, ...curr], []) as IOperationWithPath[];
  }

  public static getHeaderLinks(props: IProps): IHeaderLink[] {
    const allOperations = Swagger.cleanOperations(props.spec);
    return [
      ...allOperations.map(operation => ({
        id: operation.operationId,
        text: operation.operationId
      })),
      ...Object.keys(props.spec.definitions).map(name => ({
        id: `/definitions/${name}`,
        text: classname(name)
      }))
    ];
  }

  public render() {
    const { spec, apiHost } = this.props;
    const allOperations = Swagger.cleanOperations(spec);

    return (
      <div>
        <h1>Dataform Web API</h1>
        <div>{this.props.children}</div>
        {allOperations.map(operation => (
          <Operation key={operation.operationId} apiHost={apiHost} operation={operation} />
        ))}
        {Object.keys(spec.definitions).map(name => (
          <Definition key={name} name={name} schema={spec.definitions[name]} />
        ))}
      </div>
    );
  }
}

export class Operation extends React.Component<{ apiHost: string; operation: IOperationWithPath }> {
  public render() {
    const { operation, apiHost } = this.props;
    return (
      <div className={styles.definition}>
        <h2 id={operation.operationId}>
          {operation.operationId}
          <code className={styles.methodTag}>{operation.method.toUpperCase()}</code>
        </h2>
        <code>{apiHost + operation.path}</code>
        <p>{operation.summary}</p>
        <h3>Parameters</h3>
        {operation.parameters
          // These types are a nightmare :(.
          .map(parameter => parameter as any)
          .map(parameter => (
            <div key={parameter.name} className={styles.property}>
              <div className={styles.propertyName}>
                <code>{parameter.name}</code>
              </div>
              <div className={styles.propertyDescription}>
                {parameter.schema && (
                  <a href={parameter.schema.$ref}>
                    {classname(parameter.schema.$ref.replace("#/definitions/", ""))}
                  </a>
                )}
                {!parameter.schema && <code>string</code>}
                {parameter.description && (
                  <div className={styles.propertyComment}>{parameter.description}</div>
                )}
              </div>
            </div>
          ))}
        <h3>Responses</h3>
        {Object.keys(operation.responses)
          .map(code => ({ code, ...operation.responses[code] }))
          .map(code => code as any)
          .map(response => (
            <div key={response.code} className={styles.property}>
              <div className={styles.propertyName}>
                <code>{response.code}</code>
              </div>
              <a href={response.schema.$ref}>
                {classname(response.schema.$ref.replace("#/definitions/", ""))}
              </a>
            </div>
          ))}
      </div>
    );
  }
}

export class Definition extends React.Component<{ name: string; schema: Schema }> {
  public render() {
    const { name, schema } = this.props;
    return (
      <div className={styles.definition}>
        <h2 id={`/definitions/${name}`}>{classname(name)}</h2>
        <div>{schema.description}</div>
        {schema.properties && (
          <>
            <h3>Properties</h3>
            <div className={styles.properties}>
              {Object.keys(schema.properties).map(propertyName => (
                <Property
                  key={propertyName}
                  name={propertyName}
                  property={schema.properties[propertyName]}
                />
              ))}
            </div>
          </>
        )}
        {schema.enum &&
          schema.enum.map(enumValue => (
            <li key={String(enumValue)}>
              <code>{enumValue}</code>
            </li>
          ))}
      </div>
    );
  }
}

export class Parameter extends React.Component<{ name: string; property: Schema }> {
  public render() {
    const { name, property } = this.props;
    const isArray = property.type === "array";
    const itemSchema = isArray ? (property.items as Schema) : property;
    const typeTag = (
      <code>
        {itemSchema.type || classname(itemSchema.$ref.replace("#/definitions/", ""))}
        {isArray ? "[]" : ""}
      </code>
    );
    return (
      <div className={styles.property}>
        <div className={styles.propertyName}>
          <code>{name}</code>
        </div>
        <div className={styles.propertyDescription}>
          {itemSchema.$ref ? <a href={itemSchema.$ref}>{typeTag}</a> : typeTag}
          {property.description && (
            <div className={styles.propertyComment}>{property.description}</div>
          )}
        </div>
      </div>
    );
  }
}

export class Property extends React.Component<{ name: string; property: Schema }> {
  public render() {
    const { name, property } = this.props;
    const isArray = property.type === "array";
    const itemSchema = isArray ? (property.items as Schema) : property;
    const typeTag = (
      <code>
        {itemSchema.type || classname(itemSchema.$ref.replace("#/definitions/", ""))}
        {isArray ? "[]" : ""}
      </code>
    );
    return (
      <div className={styles.property}>
        <div className={styles.propertyName}>
          <code>{name}</code>
        </div>
        <div className={styles.propertyDescription}>
          {itemSchema.$ref ? <a href={itemSchema.$ref}>{typeTag}</a> : typeTag}
          {property.description && (
            <div className={styles.propertyComment}>{property.description}</div>
          )}
        </div>
      </div>
    );
  }
}

function classname(definitionName: string): string {
  return definitionName.match(/[A-Z].*/)[0];
}
