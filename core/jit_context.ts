import { IActionContext, ITableContext, JitContext, Resolvable } from "df/core/contextables";
import { ambiguousActionNameMsg, resolvableAsTarget, ResolvableMap, stringifyResolvable, toResolvable } from "df/core/utils";
import { dataform } from "df/protos/ts";

function canonicalTargetValue(target: dataform.ITarget): string {
    return `${target.database}.${target.schema}.${target.name}`;
}

function schemaTargetValue(target: dataform.ITarget): string {
    return `${target.schema}.${target.name}`;
}

/** Generate SQL action JiT context. */
export class SqlActionJitContext implements JitContext<IActionContext> {
    private readonly resolvableMap: ResolvableMap<string>;

    constructor(
        public readonly adapter: dataform.DbAdapter,
        public readonly data: { [k: string]: any } | undefined,
        private readonly target: dataform.ITarget,
        dependencies: dataform.ITarget[],
    ) {
        const resolvables = [target, ...dependencies];
        this.resolvableMap = new ResolvableMap(resolvables.map(dep => ({
            actionTarget: dep,
            value: canonicalTargetValue(dep)
        })));
    }

    public self(): string {
        return this.resolve(this.name());
    }

    public name(): string {
        return this.target.name;
    }

    public ref(ref: Resolvable | string[], ...rest: string[]): string {
        return this.resolve(ref, ...rest);
    }

    public resolve(ref: Resolvable | string[], ...rest: string[]): string {
        ref = toResolvable(ref, rest);
        const refTarget = resolvableAsTarget(ref);
        const candidates = this.resolvableMap.find(refTarget);

        if (candidates.length > 1) {
            throw new Error(ambiguousActionNameMsg(ref, candidates));
        }

        return this.resolveReference(
            stringifyResolvable(ref),
            candidates.length > 0 ? candidates[0] : undefined);
    }

    public schema(): string {
        return this.target.schema;
    }

    public database(): string {
        return this.target.database;
    }


    private resolveReference(name: string, resolvedName?: string): string {
        if (!!resolvedName) {
            return `\`${resolvedName}\``;
        }
        throw new Error(`Undeclared dependency referenced: ${name}.\n` +
            "JiT action must have its dependencies declared explicitly.");
    }
}

/** JiT context for table and view actions. */
export class TableJitContext extends SqlActionJitContext implements JitContext<ITableContext> {
    constructor(
        adapter: dataform.DbAdapter,
        data: { [k: string]: any } | undefined,
        target: dataform.ITarget,
        dependencies: dataform.ITarget[],
    ) {
        super(adapter, data, target, dependencies);
    }

    public when(cond: boolean, trueCase: string, falseCase?: string) {
        return cond ? trueCase : falseCase || "";
    }

    public incremental(): boolean {
        return false;
    }
}

/** JiT context for incremental table actions. */
export class IncrementalTableJitContext extends TableJitContext {
    constructor(adapter: dataform.DbAdapter,
        data: { [k: string]: any } | undefined,
        target: dataform.ITarget,
        dependencies: dataform.ITarget[],
        private readonly isIncrementalContext: boolean,
    ) {
        super(adapter, data, target, dependencies);
    }

    public incremental(): boolean {
        return this.isIncrementalContext;
    }
}
