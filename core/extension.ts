import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

/**
 * Extension interface.
 */
export interface IDataformExtension {
    /**
     * Run additional compilation steps.
     * Passed session should be used for both new nodes creation and persisting errors.
     */
    compile(request: dataform.ICompileExecutionRequest, session: Session): void;
}
