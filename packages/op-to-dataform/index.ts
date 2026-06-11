import type { IDataformExtension } from "df/core";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";
import { nativeRequire } from "df/core/utils";
import { load as loadYaml } from "js-yaml";
import { transpilePipeline, IPipeline } from "./transpiler";

class OpToDataformExtension implements IDataformExtension {
    public compile(request: dataform.ICompileExecutionRequest, session: Session): void {
        if (request.compileConfig?.projectConfigOverride?.vars["throw-error"] === "true") {
            throw new Error("throwing exception as requested!");
        }

        if (request.compileConfig?.projectConfigOverride?.vars["store-compile-error"] === "true") {
            session.compileError(new Error("storing compilation error as requested!"));
        }

        const filePaths = request.compileConfig?.filePaths || [];

        const definitionFiles = filePaths.filter(
            file =>
                (file.startsWith("definitions/") || file.startsWith("definitions\\")) &&
                (file.endsWith(".yaml") || file.endsWith(".yml"))
        );

        if (definitionFiles.length > 0) {
            definitionFiles.forEach(file => {
                const parsedYaml = this.parseYaml(file);
                if (parsedYaml) {
                    transpilePipeline(parsedYaml, session, file);
                }
            });
        }
    }

    private parseYaml(file: string): IPipeline | null {
        const globalAny = globalThis as any;
        globalAny.tempDisableActionRegistration = true;
        try {
            nativeRequire(file);
        } catch (e) {
            console.error("Require failed for file:", file, e);
        }
        globalAny.tempDisableActionRegistration = false;

        const normalizedFile = file.replace(/\\/g, "/");
        let content = "";
        if (globalAny.rawFilesCache) {
            for (const key of Object.keys(globalAny.rawFilesCache)) {
                const normalizedKey = key.replace(/\\/g, "/");
                if (normalizedKey.endsWith(normalizedFile)) {
                    content = globalAny.rawFilesCache[key];
                    break;
                }
            }
        }
        return loadYaml(content) as IPipeline;
    }
}

export function extension(): IDataformExtension {
    return new OpToDataformExtension();
}
