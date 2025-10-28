import type { IDataformExtension } from "df/core";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

class SampleExtension implements IDataformExtension {
    public compile(request: dataform.ICompileExecutionRequest, session: Session): void {
        session.publish('sample-action').query("SELECT 1 as sample_id");
    }
}

export function extension(): IDataformExtension {
    return new SampleExtension();
}
