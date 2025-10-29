import type { IDataformExtension } from "df/core";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

class SampleExtension implements IDataformExtension {
    public compile(request: dataform.ICompileExecutionRequest, session: Session): void {
        if (request.compileConfig?.projectConfigOverride?.vars["throw-error"] === "true") {
            throw new Error("throwing exception as requested!");
        }

        if (request.compileConfig?.projectConfigOverride?.vars["store-compile-error"] === "true") {
            session.compileError(new Error("storing compilation error as requested!"));
        }

        session.publish('sample-action').query("SELECT 1 as sample_id");
    }
}

export function extension(): IDataformExtension {
    return new SampleExtension();
}
