import { Tag } from "@blueprintjs/core";
import { PageLinks } from "df/docs/components/page_links";
import * as styles from "df/docs/components/swagger.css";
import React from "react";
import {
  Operation as IOperation,
  Parameter as IParameter,
  Schema,
  Spec
} from "swagger-schema-official";

interface IProps {
  spec: Spec;
}

interface IOperationWithPath extends IOperation {
  path: string;
  method: string;
}

export class Swagger extends React.Component<IProps> {
  public render() {
    const { spec } = this.props;
    const allOperations: IOperationWithPath[] = Object.keys(spec.paths)
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
      .reduce((acc, curr) => [...acc, ...curr], []);

    return (
      <div className={styles.container}>
        <div className={styles.sidebar}></div>
        <div className={styles.mainContent}>
          <div className={styles.titleBlock}>
            <h1>Dataform Web API</h1>
            {allOperations.map(operation => (
              <Operation operation={operation} />
            ))}
            {Object.keys(spec.definitions).map(name => (
              <Definition name={name} schema={spec.definitions[name]} />
            ))}
          </div>
          {this.props.children}
        </div>
        <div className={styles.sidebarRight}>
          <h5>Operations</h5>
          <PageLinks
            links={allOperations.map(operation => ({
              id: operation.operationId,
              text: operation.operationId
            }))}
          />
          <h5>Types</h5>
          <PageLinks
            links={Object.keys(spec.definitions).map(name => ({
              id: `/definitions/${name}`,
              text: classname(name)
            }))}
          />
        </div>
      </div>
    );
  }
}

export class Operation extends React.Component<{ operation: IOperationWithPath }> {
  public render() {
    const { operation } = this.props;
    return (
      <div className={styles.definition}>
        <h2 id={operation.operationId}>
          {operation.operationId}
          <code>{operation.method.toUpperCase()}</code>
        </h2>
        <code>{operation.path}</code>
        <p>{operation.summary}</p>
        <h3>Parameters</h3>
        {operation.parameters
          // These types are a nightmare :(.
          .map(parameter => parameter as any)
          .map(parameter => (
            <div className={styles.property}>
              <div className={styles.propertyName}>
                <code>{parameter.name}</code>
              </div>
              {parameter.schema && (
                <a href={parameter.schema.$ref}>
                  {classname(parameter.schema.$ref.replace("#/definitions/", ""))}
                </a>
              )}
              {!parameter.schema && <code>string</code>}
              {parameter.description && (
                <div className={styles.propertyDescription}>{parameter.description}</div>
              )}
            </div>
          ))}
        <h3>Responses</h3>
        {Object.keys(operation.responses)
          .map(code => ({ code, ...operation.responses[code] }))
          .map(response => (
            <div className={styles.property}>
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
                <Property name={propertyName} property={schema.properties[propertyName]} />
              ))}
            </div>
          </>
        )}
        {schema.enum &&
          schema.enum.map(enumValue => (
            <li>
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
        {itemSchema.$ref ? <a href={itemSchema.$ref}>{typeTag}</a> : typeTag}
        {property.description && (
          <div className={styles.propertyDescription}>{property.description}</div>
        )}
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
        {itemSchema.$ref ? <a href={itemSchema.$ref}>{typeTag}</a> : typeTag}
        {property.description && (
          <div className={styles.propertyDescription}>{property.description}</div>
        )}
      </div>
    );
  }
}

function namespace(definitionName: string): string {
  return definitionName.split(/[A-Z]/)[0];
}

function classname(definitionName: string): string {
  return definitionName.match(/[A-Z].*/)[0];
}
