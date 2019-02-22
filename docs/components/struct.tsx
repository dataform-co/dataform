import * as React from "react";

export interface Props {
  description?: string;
  fields?: {
    name: string;
    type: string;
    typeLink?: string;
    description: string;
  }[];
}

export default class Struct extends React.Component<Props, any> {
  render() {
    return (
      <table className="pt-html-table pt-html-table-striped" style={styles.table}>
        <thead>
          <tr>
            <td>Field</td>
            <td>Type</td>
            <td>Description</td>
          </tr>
        </thead>
        <tbody>
          {this.props.fields.map(field => (
            <tr key={field.name}>
              <td>
                <code>{field.name}</code>
              </td>
              <td>
                <code>{field.type}</code>
              </td>
              <td>{field.description}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  }
}

export const styles: { [className: string]: React.CSSProperties } = {
  table: {
    width: "100%"
  }
};
