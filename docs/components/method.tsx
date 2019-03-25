import * as React from "react";
import remark from "remark";
import remark2react from "remark-react";
import * as typedoc from "typedoc";
import Struct, { Props as StructProps } from "./struct";

export interface Props extends StructProps {
  name: string;
  signatures: string[];
  returns?: string;
}

export default class Method extends React.Component<Props, any> {
  public renderDescription = () => {
    return remark()
      .use(remark2react)
      .processSync(this.props.description).contents;
  };

  public render() {
    return (
      <div>
        <h2 id={this.props.name}>
          <pre>{this.props.name}</pre>
        </h2>
        <div>{this.renderDescription()}</div>
        <div>
          {this.props.signatures &&
            this.props.signatures.map(signature => (
              <div key={signature}>
                <pre>
                  <code>{signature}</code>
                </pre>
              </div>
            ))}
        </div>
        {this.props.fields && (
          <div>
            <h3> Arguments </h3>
            <Struct fields={this.props.fields} />
          </div>
        )}
        {this.props.returns && (
          <div>
            <h3>Returns</h3>
            <p>{this.props.returns}</p>
          </div>
        )}
      </div>
    );
  }
}
