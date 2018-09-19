import * as $protobuf from "protobufjs";
/** Properties of a ProjectConfig. */
export interface IProjectConfig {

    /** ProjectConfig warehouse */
    warehouse?: (string|null);

    /** ProjectConfig defaultSchema */
    defaultSchema?: (string|null);

    /** ProjectConfig assertionSchema */
    assertionSchema?: (string|null);

    /** ProjectConfig datasetPaths */
    datasetPaths?: (string[]|null);

    /** ProjectConfig includePaths */
    includePaths?: (string[]|null);

    /** ProjectConfig dependencies */
    dependencies?: ({ [k: string]: string }|null);
}

/** Represents a ProjectConfig. */
export class ProjectConfig implements IProjectConfig {

    /**
     * Constructs a new ProjectConfig.
     * @param [properties] Properties to set
     */
    constructor(properties?: IProjectConfig);

    /** ProjectConfig warehouse. */
    public warehouse: string;

    /** ProjectConfig defaultSchema. */
    public defaultSchema: string;

    /** ProjectConfig assertionSchema. */
    public assertionSchema: string;

    /** ProjectConfig datasetPaths. */
    public datasetPaths: string[];

    /** ProjectConfig includePaths. */
    public includePaths: string[];

    /** ProjectConfig dependencies. */
    public dependencies: { [k: string]: string };

    /**
     * Creates a new ProjectConfig instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ProjectConfig instance
     */
    public static create(properties?: IProjectConfig): ProjectConfig;

    /**
     * Encodes the specified ProjectConfig message. Does not implicitly {@link ProjectConfig.verify|verify} messages.
     * @param message ProjectConfig message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IProjectConfig, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ProjectConfig message, length delimited. Does not implicitly {@link ProjectConfig.verify|verify} messages.
     * @param message ProjectConfig message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IProjectConfig, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a ProjectConfig message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ProjectConfig
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ProjectConfig;

    /**
     * Decodes a ProjectConfig message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ProjectConfig
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ProjectConfig;

    /**
     * Verifies a ProjectConfig message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a ProjectConfig message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ProjectConfig
     */
    public static fromObject(object: { [k: string]: any }): ProjectConfig;

    /**
     * Creates a plain object from a ProjectConfig message. Also converts values to other types if specified.
     * @param message ProjectConfig
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: ProjectConfig, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this ProjectConfig to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of a Target. */
export interface ITarget {

    /** Target schema */
    schema?: (string|null);

    /** Target name */
    name?: (string|null);
}

/** Represents a Target. */
export class Target implements ITarget {

    /**
     * Constructs a new Target.
     * @param [properties] Properties to set
     */
    constructor(properties?: ITarget);

    /** Target schema. */
    public schema: string;

    /** Target name. */
    public name: string;

    /**
     * Creates a new Target instance using the specified properties.
     * @param [properties] Properties to set
     * @returns Target instance
     */
    public static create(properties?: ITarget): Target;

    /**
     * Encodes the specified Target message. Does not implicitly {@link Target.verify|verify} messages.
     * @param message Target message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: ITarget, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified Target message, length delimited. Does not implicitly {@link Target.verify|verify} messages.
     * @param message Target message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: ITarget, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a Target message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns Target
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): Target;

    /**
     * Decodes a Target message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns Target
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): Target;

    /**
     * Verifies a Target message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a Target message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns Target
     */
    public static fromObject(object: { [k: string]: any }): Target;

    /**
     * Creates a plain object from a Target message. Also converts values to other types if specified.
     * @param message Target
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: Target, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this Target to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of a Materialization. */
export interface IMaterialization {

    /** Materialization name */
    name?: (string|null);

    /** Materialization dependencies */
    dependencies?: (string[]|null);

    /** Materialization type */
    type?: (string|null);

    /** Materialization target */
    target?: (ITarget|null);

    /** Materialization query */
    query?: (string|null);

    /** Materialization protected */
    "protected"?: (boolean|null);

    /** Materialization partitionBy */
    partitionBy?: (string|null);

    /** Materialization descriptions */
    descriptions?: ({ [k: string]: string }|null);

    /** Materialization where */
    where?: (string|null);

    /** Materialization uniqueKey */
    uniqueKey?: (string|null);

    /** Materialization pres */
    pres?: (string[]|null);

    /** Materialization posts */
    posts?: (string[]|null);

    /** Materialization assertions */
    assertions?: (string[]|null);

    /** Materialization parsedColumns */
    parsedColumns?: (string[]|null);
}

/** Represents a Materialization. */
export class Materialization implements IMaterialization {

    /**
     * Constructs a new Materialization.
     * @param [properties] Properties to set
     */
    constructor(properties?: IMaterialization);

    /** Materialization name. */
    public name: string;

    /** Materialization dependencies. */
    public dependencies: string[];

    /** Materialization type. */
    public type: string;

    /** Materialization target. */
    public target?: (ITarget|null);

    /** Materialization query. */
    public query: string;

    /** Materialization protected. */
    public protected: boolean;

    /** Materialization partitionBy. */
    public partitionBy: string;

    /** Materialization descriptions. */
    public descriptions: { [k: string]: string };

    /** Materialization where. */
    public where: string;

    /** Materialization uniqueKey. */
    public uniqueKey: string;

    /** Materialization pres. */
    public pres: string[];

    /** Materialization posts. */
    public posts: string[];

    /** Materialization assertions. */
    public assertions: string[];

    /** Materialization parsedColumns. */
    public parsedColumns: string[];

    /**
     * Creates a new Materialization instance using the specified properties.
     * @param [properties] Properties to set
     * @returns Materialization instance
     */
    public static create(properties?: IMaterialization): Materialization;

    /**
     * Encodes the specified Materialization message. Does not implicitly {@link Materialization.verify|verify} messages.
     * @param message Materialization message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IMaterialization, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified Materialization message, length delimited. Does not implicitly {@link Materialization.verify|verify} messages.
     * @param message Materialization message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IMaterialization, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a Materialization message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns Materialization
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): Materialization;

    /**
     * Decodes a Materialization message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns Materialization
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): Materialization;

    /**
     * Verifies a Materialization message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a Materialization message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns Materialization
     */
    public static fromObject(object: { [k: string]: any }): Materialization;

    /**
     * Creates a plain object from a Materialization message. Also converts values to other types if specified.
     * @param message Materialization
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: Materialization, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this Materialization to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of an Operation. */
export interface IOperation {

    /** Operation name */
    name?: (string|null);

    /** Operation dependencies */
    dependencies?: (string[]|null);

    /** Operation statements */
    statements?: (string[]|null);
}

/** Represents an Operation. */
export class Operation implements IOperation {

    /**
     * Constructs a new Operation.
     * @param [properties] Properties to set
     */
    constructor(properties?: IOperation);

    /** Operation name. */
    public name: string;

    /** Operation dependencies. */
    public dependencies: string[];

    /** Operation statements. */
    public statements: string[];

    /**
     * Creates a new Operation instance using the specified properties.
     * @param [properties] Properties to set
     * @returns Operation instance
     */
    public static create(properties?: IOperation): Operation;

    /**
     * Encodes the specified Operation message. Does not implicitly {@link Operation.verify|verify} messages.
     * @param message Operation message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IOperation, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified Operation message, length delimited. Does not implicitly {@link Operation.verify|verify} messages.
     * @param message Operation message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IOperation, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an Operation message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns Operation
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): Operation;

    /**
     * Decodes an Operation message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns Operation
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): Operation;

    /**
     * Verifies an Operation message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an Operation message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns Operation
     */
    public static fromObject(object: { [k: string]: any }): Operation;

    /**
     * Creates a plain object from an Operation message. Also converts values to other types if specified.
     * @param message Operation
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: Operation, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this Operation to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of an Assertion. */
export interface IAssertion {

    /** Assertion name */
    name?: (string|null);

    /** Assertion dependencies */
    dependencies?: (string[]|null);

    /** Assertion queries */
    queries?: (string[]|null);
}

/** Represents an Assertion. */
export class Assertion implements IAssertion {

    /**
     * Constructs a new Assertion.
     * @param [properties] Properties to set
     */
    constructor(properties?: IAssertion);

    /** Assertion name. */
    public name: string;

    /** Assertion dependencies. */
    public dependencies: string[];

    /** Assertion queries. */
    public queries: string[];

    /**
     * Creates a new Assertion instance using the specified properties.
     * @param [properties] Properties to set
     * @returns Assertion instance
     */
    public static create(properties?: IAssertion): Assertion;

    /**
     * Encodes the specified Assertion message. Does not implicitly {@link Assertion.verify|verify} messages.
     * @param message Assertion message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IAssertion, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified Assertion message, length delimited. Does not implicitly {@link Assertion.verify|verify} messages.
     * @param message Assertion message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IAssertion, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an Assertion message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns Assertion
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): Assertion;

    /**
     * Decodes an Assertion message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns Assertion
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): Assertion;

    /**
     * Verifies an Assertion message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an Assertion message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns Assertion
     */
    public static fromObject(object: { [k: string]: any }): Assertion;

    /**
     * Creates a plain object from an Assertion message. Also converts values to other types if specified.
     * @param message Assertion
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: Assertion, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this Assertion to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of a CompiledGraph. */
export interface ICompiledGraph {

    /** CompiledGraph projectConfig */
    projectConfig?: (IProjectConfig|null);

    /** CompiledGraph materializations */
    materializations?: (IMaterialization[]|null);

    /** CompiledGraph operations */
    operations?: (IOperation[]|null);

    /** CompiledGraph assertions */
    assertions?: (IAssertion[]|null);
}

/** Represents a CompiledGraph. */
export class CompiledGraph implements ICompiledGraph {

    /**
     * Constructs a new CompiledGraph.
     * @param [properties] Properties to set
     */
    constructor(properties?: ICompiledGraph);

    /** CompiledGraph projectConfig. */
    public projectConfig?: (IProjectConfig|null);

    /** CompiledGraph materializations. */
    public materializations: IMaterialization[];

    /** CompiledGraph operations. */
    public operations: IOperation[];

    /** CompiledGraph assertions. */
    public assertions: IAssertion[];

    /**
     * Creates a new CompiledGraph instance using the specified properties.
     * @param [properties] Properties to set
     * @returns CompiledGraph instance
     */
    public static create(properties?: ICompiledGraph): CompiledGraph;

    /**
     * Encodes the specified CompiledGraph message. Does not implicitly {@link CompiledGraph.verify|verify} messages.
     * @param message CompiledGraph message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: ICompiledGraph, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified CompiledGraph message, length delimited. Does not implicitly {@link CompiledGraph.verify|verify} messages.
     * @param message CompiledGraph message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: ICompiledGraph, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a CompiledGraph message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns CompiledGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): CompiledGraph;

    /**
     * Decodes a CompiledGraph message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns CompiledGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): CompiledGraph;

    /**
     * Verifies a CompiledGraph message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a CompiledGraph message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns CompiledGraph
     */
    public static fromObject(object: { [k: string]: any }): CompiledGraph;

    /**
     * Creates a plain object from a CompiledGraph message. Also converts values to other types if specified.
     * @param message CompiledGraph
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: CompiledGraph, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this CompiledGraph to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of an ExecutionTask. */
export interface IExecutionTask {

    /** ExecutionTask type */
    type?: (string|null);

    /** ExecutionTask statement */
    statement?: (string|null);

    /** ExecutionTask ignoreErrors */
    ignoreErrors?: (boolean|null);
}

/** Represents an ExecutionTask. */
export class ExecutionTask implements IExecutionTask {

    /**
     * Constructs a new ExecutionTask.
     * @param [properties] Properties to set
     */
    constructor(properties?: IExecutionTask);

    /** ExecutionTask type. */
    public type: string;

    /** ExecutionTask statement. */
    public statement: string;

    /** ExecutionTask ignoreErrors. */
    public ignoreErrors: boolean;

    /**
     * Creates a new ExecutionTask instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ExecutionTask instance
     */
    public static create(properties?: IExecutionTask): ExecutionTask;

    /**
     * Encodes the specified ExecutionTask message. Does not implicitly {@link ExecutionTask.verify|verify} messages.
     * @param message ExecutionTask message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IExecutionTask, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ExecutionTask message, length delimited. Does not implicitly {@link ExecutionTask.verify|verify} messages.
     * @param message ExecutionTask message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IExecutionTask, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an ExecutionTask message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ExecutionTask
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ExecutionTask;

    /**
     * Decodes an ExecutionTask message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ExecutionTask
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ExecutionTask;

    /**
     * Verifies an ExecutionTask message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an ExecutionTask message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ExecutionTask
     */
    public static fromObject(object: { [k: string]: any }): ExecutionTask;

    /**
     * Creates a plain object from an ExecutionTask message. Also converts values to other types if specified.
     * @param message ExecutionTask
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: ExecutionTask, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this ExecutionTask to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of an ExecutionNode. */
export interface IExecutionNode {

    /** ExecutionNode name */
    name?: (string|null);

    /** ExecutionNode dependencies */
    dependencies?: (string[]|null);

    /** ExecutionNode tasks */
    tasks?: (IExecutionTask[]|null);
}

/** Represents an ExecutionNode. */
export class ExecutionNode implements IExecutionNode {

    /**
     * Constructs a new ExecutionNode.
     * @param [properties] Properties to set
     */
    constructor(properties?: IExecutionNode);

    /** ExecutionNode name. */
    public name: string;

    /** ExecutionNode dependencies. */
    public dependencies: string[];

    /** ExecutionNode tasks. */
    public tasks: IExecutionTask[];

    /**
     * Creates a new ExecutionNode instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ExecutionNode instance
     */
    public static create(properties?: IExecutionNode): ExecutionNode;

    /**
     * Encodes the specified ExecutionNode message. Does not implicitly {@link ExecutionNode.verify|verify} messages.
     * @param message ExecutionNode message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IExecutionNode, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ExecutionNode message, length delimited. Does not implicitly {@link ExecutionNode.verify|verify} messages.
     * @param message ExecutionNode message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IExecutionNode, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an ExecutionNode message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ExecutionNode
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ExecutionNode;

    /**
     * Decodes an ExecutionNode message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ExecutionNode
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ExecutionNode;

    /**
     * Verifies an ExecutionNode message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an ExecutionNode message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ExecutionNode
     */
    public static fromObject(object: { [k: string]: any }): ExecutionNode;

    /**
     * Creates a plain object from an ExecutionNode message. Also converts values to other types if specified.
     * @param message ExecutionNode
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: ExecutionNode, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this ExecutionNode to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of an ExecutionGraph. */
export interface IExecutionGraph {

    /** ExecutionGraph projectConfig */
    projectConfig?: (IProjectConfig|null);

    /** ExecutionGraph runConfig */
    runConfig?: (IRunConfig|null);

    /** ExecutionGraph nodes */
    nodes?: (IExecutionNode[]|null);
}

/** Represents an ExecutionGraph. */
export class ExecutionGraph implements IExecutionGraph {

    /**
     * Constructs a new ExecutionGraph.
     * @param [properties] Properties to set
     */
    constructor(properties?: IExecutionGraph);

    /** ExecutionGraph projectConfig. */
    public projectConfig?: (IProjectConfig|null);

    /** ExecutionGraph runConfig. */
    public runConfig?: (IRunConfig|null);

    /** ExecutionGraph nodes. */
    public nodes: IExecutionNode[];

    /**
     * Creates a new ExecutionGraph instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ExecutionGraph instance
     */
    public static create(properties?: IExecutionGraph): ExecutionGraph;

    /**
     * Encodes the specified ExecutionGraph message. Does not implicitly {@link ExecutionGraph.verify|verify} messages.
     * @param message ExecutionGraph message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IExecutionGraph, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ExecutionGraph message, length delimited. Does not implicitly {@link ExecutionGraph.verify|verify} messages.
     * @param message ExecutionGraph message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IExecutionGraph, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an ExecutionGraph message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ExecutionGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ExecutionGraph;

    /**
     * Decodes an ExecutionGraph message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ExecutionGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ExecutionGraph;

    /**
     * Verifies an ExecutionGraph message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an ExecutionGraph message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ExecutionGraph
     */
    public static fromObject(object: { [k: string]: any }): ExecutionGraph;

    /**
     * Creates a plain object from an ExecutionGraph message. Also converts values to other types if specified.
     * @param message ExecutionGraph
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: ExecutionGraph, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this ExecutionGraph to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of an ExecutedTask. */
export interface IExecutedTask {

    /** ExecutedTask task */
    task?: (IExecutionTask|null);

    /** ExecutedTask ok */
    ok?: (boolean|null);

    /** ExecutedTask error */
    error?: (string|null);

    /** ExecutedTask testResults */
    testResults?: (string|null);
}

/** Represents an ExecutedTask. */
export class ExecutedTask implements IExecutedTask {

    /**
     * Constructs a new ExecutedTask.
     * @param [properties] Properties to set
     */
    constructor(properties?: IExecutedTask);

    /** ExecutedTask task. */
    public task?: (IExecutionTask|null);

    /** ExecutedTask ok. */
    public ok: boolean;

    /** ExecutedTask error. */
    public error: string;

    /** ExecutedTask testResults. */
    public testResults: string;

    /**
     * Creates a new ExecutedTask instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ExecutedTask instance
     */
    public static create(properties?: IExecutedTask): ExecutedTask;

    /**
     * Encodes the specified ExecutedTask message. Does not implicitly {@link ExecutedTask.verify|verify} messages.
     * @param message ExecutedTask message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IExecutedTask, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ExecutedTask message, length delimited. Does not implicitly {@link ExecutedTask.verify|verify} messages.
     * @param message ExecutedTask message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IExecutedTask, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an ExecutedTask message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ExecutedTask
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ExecutedTask;

    /**
     * Decodes an ExecutedTask message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ExecutedTask
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ExecutedTask;

    /**
     * Verifies an ExecutedTask message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an ExecutedTask message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ExecutedTask
     */
    public static fromObject(object: { [k: string]: any }): ExecutedTask;

    /**
     * Creates a plain object from an ExecutedTask message. Also converts values to other types if specified.
     * @param message ExecutedTask
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: ExecutedTask, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this ExecutedTask to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of an ExecutedNode. */
export interface IExecutedNode {

    /** ExecutedNode name */
    name?: (string|null);

    /** ExecutedNode tasks */
    tasks?: (IExecutedTask[]|null);

    /** ExecutedNode ok */
    ok?: (boolean|null);

    /** ExecutedNode skipped */
    skipped?: (boolean|null);
}

/** Represents an ExecutedNode. */
export class ExecutedNode implements IExecutedNode {

    /**
     * Constructs a new ExecutedNode.
     * @param [properties] Properties to set
     */
    constructor(properties?: IExecutedNode);

    /** ExecutedNode name. */
    public name: string;

    /** ExecutedNode tasks. */
    public tasks: IExecutedTask[];

    /** ExecutedNode ok. */
    public ok: boolean;

    /** ExecutedNode skipped. */
    public skipped: boolean;

    /**
     * Creates a new ExecutedNode instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ExecutedNode instance
     */
    public static create(properties?: IExecutedNode): ExecutedNode;

    /**
     * Encodes the specified ExecutedNode message. Does not implicitly {@link ExecutedNode.verify|verify} messages.
     * @param message ExecutedNode message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IExecutedNode, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ExecutedNode message, length delimited. Does not implicitly {@link ExecutedNode.verify|verify} messages.
     * @param message ExecutedNode message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IExecutedNode, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an ExecutedNode message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ExecutedNode
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ExecutedNode;

    /**
     * Decodes an ExecutedNode message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ExecutedNode
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ExecutedNode;

    /**
     * Verifies an ExecutedNode message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an ExecutedNode message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ExecutedNode
     */
    public static fromObject(object: { [k: string]: any }): ExecutedNode;

    /**
     * Creates a plain object from an ExecutedNode message. Also converts values to other types if specified.
     * @param message ExecutedNode
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: ExecutedNode, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this ExecutedNode to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of an ExecutedGraph. */
export interface IExecutedGraph {

    /** ExecutedGraph projectConfig */
    projectConfig?: (IProjectConfig|null);

    /** ExecutedGraph runConfig */
    runConfig?: (IRunConfig|null);

    /** ExecutedGraph nodes */
    nodes?: (IExecutedNode[]|null);
}

/** Represents an ExecutedGraph. */
export class ExecutedGraph implements IExecutedGraph {

    /**
     * Constructs a new ExecutedGraph.
     * @param [properties] Properties to set
     */
    constructor(properties?: IExecutedGraph);

    /** ExecutedGraph projectConfig. */
    public projectConfig?: (IProjectConfig|null);

    /** ExecutedGraph runConfig. */
    public runConfig?: (IRunConfig|null);

    /** ExecutedGraph nodes. */
    public nodes: IExecutedNode[];

    /**
     * Creates a new ExecutedGraph instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ExecutedGraph instance
     */
    public static create(properties?: IExecutedGraph): ExecutedGraph;

    /**
     * Encodes the specified ExecutedGraph message. Does not implicitly {@link ExecutedGraph.verify|verify} messages.
     * @param message ExecutedGraph message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IExecutedGraph, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ExecutedGraph message, length delimited. Does not implicitly {@link ExecutedGraph.verify|verify} messages.
     * @param message ExecutedGraph message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IExecutedGraph, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an ExecutedGraph message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ExecutedGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): ExecutedGraph;

    /**
     * Decodes an ExecutedGraph message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ExecutedGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): ExecutedGraph;

    /**
     * Verifies an ExecutedGraph message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates an ExecutedGraph message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ExecutedGraph
     */
    public static fromObject(object: { [k: string]: any }): ExecutedGraph;

    /**
     * Creates a plain object from an ExecutedGraph message. Also converts values to other types if specified.
     * @param message ExecutedGraph
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: ExecutedGraph, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this ExecutedGraph to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of a RunConfig. */
export interface IRunConfig {

    /** RunConfig nodes */
    nodes?: (string[]|null);

    /** RunConfig includeDependencies */
    includeDependencies?: (boolean|null);

    /** RunConfig fullRefresh */
    fullRefresh?: (boolean|null);

    /** RunConfig carryOn */
    carryOn?: (boolean|null);
}

/** Represents a RunConfig. */
export class RunConfig implements IRunConfig {

    /**
     * Constructs a new RunConfig.
     * @param [properties] Properties to set
     */
    constructor(properties?: IRunConfig);

    /** RunConfig nodes. */
    public nodes: string[];

    /** RunConfig includeDependencies. */
    public includeDependencies: boolean;

    /** RunConfig fullRefresh. */
    public fullRefresh: boolean;

    /** RunConfig carryOn. */
    public carryOn: boolean;

    /**
     * Creates a new RunConfig instance using the specified properties.
     * @param [properties] Properties to set
     * @returns RunConfig instance
     */
    public static create(properties?: IRunConfig): RunConfig;

    /**
     * Encodes the specified RunConfig message. Does not implicitly {@link RunConfig.verify|verify} messages.
     * @param message RunConfig message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IRunConfig, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified RunConfig message, length delimited. Does not implicitly {@link RunConfig.verify|verify} messages.
     * @param message RunConfig message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IRunConfig, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a RunConfig message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns RunConfig
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): RunConfig;

    /**
     * Decodes a RunConfig message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns RunConfig
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): RunConfig;

    /**
     * Verifies a RunConfig message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a RunConfig message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns RunConfig
     */
    public static fromObject(object: { [k: string]: any }): RunConfig;

    /**
     * Creates a plain object from a RunConfig message. Also converts values to other types if specified.
     * @param message RunConfig
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: RunConfig, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this RunConfig to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of a Profile. */
export interface IProfile {

    /** Profile threads */
    threads?: (number|null);

    /** Profile redshift */
    redshift?: (IJDBC|null);

    /** Profile postgres */
    postgres?: (IJDBC|null);

    /** Profile bigquery */
    bigquery?: (IBigQuery|null);

    /** Profile snowflake */
    snowflake?: (ISnowflake|null);
}

/** Represents a Profile. */
export class Profile implements IProfile {

    /**
     * Constructs a new Profile.
     * @param [properties] Properties to set
     */
    constructor(properties?: IProfile);

    /** Profile threads. */
    public threads: number;

    /** Profile redshift. */
    public redshift?: (IJDBC|null);

    /** Profile postgres. */
    public postgres?: (IJDBC|null);

    /** Profile bigquery. */
    public bigquery?: (IBigQuery|null);

    /** Profile snowflake. */
    public snowflake?: (ISnowflake|null);

    /** Profile connection. */
    public connection?: ("redshift"|"postgres"|"bigquery"|"snowflake");

    /**
     * Creates a new Profile instance using the specified properties.
     * @param [properties] Properties to set
     * @returns Profile instance
     */
    public static create(properties?: IProfile): Profile;

    /**
     * Encodes the specified Profile message. Does not implicitly {@link Profile.verify|verify} messages.
     * @param message Profile message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IProfile, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified Profile message, length delimited. Does not implicitly {@link Profile.verify|verify} messages.
     * @param message Profile message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IProfile, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a Profile message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns Profile
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): Profile;

    /**
     * Decodes a Profile message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns Profile
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): Profile;

    /**
     * Verifies a Profile message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a Profile message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns Profile
     */
    public static fromObject(object: { [k: string]: any }): Profile;

    /**
     * Creates a plain object from a Profile message. Also converts values to other types if specified.
     * @param message Profile
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: Profile, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this Profile to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of a JDBC. */
export interface IJDBC {

    /** JDBC hostName */
    hostName?: (string|null);

    /** JDBC port */
    port?: (number|Long|null);

    /** JDBC userName */
    userName?: (string|null);

    /** JDBC password */
    password?: (string|null);

    /** JDBC databaseName */
    databaseName?: (string|null);
}

/** Represents a JDBC. */
export class JDBC implements IJDBC {

    /**
     * Constructs a new JDBC.
     * @param [properties] Properties to set
     */
    constructor(properties?: IJDBC);

    /** JDBC hostName. */
    public hostName: string;

    /** JDBC port. */
    public port: (number|Long);

    /** JDBC userName. */
    public userName: string;

    /** JDBC password. */
    public password: string;

    /** JDBC databaseName. */
    public databaseName: string;

    /**
     * Creates a new JDBC instance using the specified properties.
     * @param [properties] Properties to set
     * @returns JDBC instance
     */
    public static create(properties?: IJDBC): JDBC;

    /**
     * Encodes the specified JDBC message. Does not implicitly {@link JDBC.verify|verify} messages.
     * @param message JDBC message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IJDBC, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified JDBC message, length delimited. Does not implicitly {@link JDBC.verify|verify} messages.
     * @param message JDBC message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IJDBC, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a JDBC message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns JDBC
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): JDBC;

    /**
     * Decodes a JDBC message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns JDBC
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): JDBC;

    /**
     * Verifies a JDBC message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a JDBC message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns JDBC
     */
    public static fromObject(object: { [k: string]: any }): JDBC;

    /**
     * Creates a plain object from a JDBC message. Also converts values to other types if specified.
     * @param message JDBC
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: JDBC, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this JDBC to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of a Snowflake. */
export interface ISnowflake {

    /** Snowflake accountId */
    accountId?: (string|null);

    /** Snowflake userName */
    userName?: (string|null);

    /** Snowflake password */
    password?: (string|null);

    /** Snowflake role */
    role?: (string|null);

    /** Snowflake databaseName */
    databaseName?: (string|null);

    /** Snowflake warehouse */
    warehouse?: (string|null);
}

/** Represents a Snowflake. */
export class Snowflake implements ISnowflake {

    /**
     * Constructs a new Snowflake.
     * @param [properties] Properties to set
     */
    constructor(properties?: ISnowflake);

    /** Snowflake accountId. */
    public accountId: string;

    /** Snowflake userName. */
    public userName: string;

    /** Snowflake password. */
    public password: string;

    /** Snowflake role. */
    public role: string;

    /** Snowflake databaseName. */
    public databaseName: string;

    /** Snowflake warehouse. */
    public warehouse: string;

    /**
     * Creates a new Snowflake instance using the specified properties.
     * @param [properties] Properties to set
     * @returns Snowflake instance
     */
    public static create(properties?: ISnowflake): Snowflake;

    /**
     * Encodes the specified Snowflake message. Does not implicitly {@link Snowflake.verify|verify} messages.
     * @param message Snowflake message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: ISnowflake, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified Snowflake message, length delimited. Does not implicitly {@link Snowflake.verify|verify} messages.
     * @param message Snowflake message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: ISnowflake, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a Snowflake message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns Snowflake
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): Snowflake;

    /**
     * Decodes a Snowflake message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns Snowflake
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): Snowflake;

    /**
     * Verifies a Snowflake message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a Snowflake message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns Snowflake
     */
    public static fromObject(object: { [k: string]: any }): Snowflake;

    /**
     * Creates a plain object from a Snowflake message. Also converts values to other types if specified.
     * @param message Snowflake
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: Snowflake, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this Snowflake to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

/** Properties of a BigQuery. */
export interface IBigQuery {

    /** BigQuery projectId */
    projectId?: (string|null);

    /** BigQuery credentials */
    credentials?: (BigQuery.ICredentials|null);
}

/** Represents a BigQuery. */
export class BigQuery implements IBigQuery {

    /**
     * Constructs a new BigQuery.
     * @param [properties] Properties to set
     */
    constructor(properties?: IBigQuery);

    /** BigQuery projectId. */
    public projectId: string;

    /** BigQuery credentials. */
    public credentials?: (BigQuery.ICredentials|null);

    /**
     * Creates a new BigQuery instance using the specified properties.
     * @param [properties] Properties to set
     * @returns BigQuery instance
     */
    public static create(properties?: IBigQuery): BigQuery;

    /**
     * Encodes the specified BigQuery message. Does not implicitly {@link BigQuery.verify|verify} messages.
     * @param message BigQuery message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: IBigQuery, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified BigQuery message, length delimited. Does not implicitly {@link BigQuery.verify|verify} messages.
     * @param message BigQuery message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: IBigQuery, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a BigQuery message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns BigQuery
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): BigQuery;

    /**
     * Decodes a BigQuery message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns BigQuery
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): BigQuery;

    /**
     * Verifies a BigQuery message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: { [k: string]: any }): (string|null);

    /**
     * Creates a BigQuery message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns BigQuery
     */
    public static fromObject(object: { [k: string]: any }): BigQuery;

    /**
     * Creates a plain object from a BigQuery message. Also converts values to other types if specified.
     * @param message BigQuery
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: BigQuery, options?: $protobuf.IConversionOptions): { [k: string]: any };

    /**
     * Converts this BigQuery to JSON.
     * @returns JSON object
     */
    public toJSON(): { [k: string]: any };
}

export namespace BigQuery {

    /** Properties of a Credentials. */
    interface ICredentials {

        /** Credentials type */
        type?: (string|null);

        /** Credentials projectId */
        projectId?: (string|null);

        /** Credentials privateKeyId */
        privateKeyId?: (string|null);

        /** Credentials privateKey */
        privateKey?: (string|null);

        /** Credentials clientEmail */
        clientEmail?: (string|null);

        /** Credentials clientId */
        clientId?: (string|null);

        /** Credentials authUri */
        authUri?: (string|null);

        /** Credentials tokenUri */
        tokenUri?: (string|null);

        /** Credentials authProviderX509CertUrl */
        authProviderX509CertUrl?: (string|null);

        /** Credentials clientX509CertUrl */
        clientX509CertUrl?: (string|null);
    }

    /** Represents a Credentials. */
    class Credentials implements ICredentials {

        /**
         * Constructs a new Credentials.
         * @param [properties] Properties to set
         */
        constructor(properties?: BigQuery.ICredentials);

        /** Credentials type. */
        public type: string;

        /** Credentials projectId. */
        public projectId: string;

        /** Credentials privateKeyId. */
        public privateKeyId: string;

        /** Credentials privateKey. */
        public privateKey: string;

        /** Credentials clientEmail. */
        public clientEmail: string;

        /** Credentials clientId. */
        public clientId: string;

        /** Credentials authUri. */
        public authUri: string;

        /** Credentials tokenUri. */
        public tokenUri: string;

        /** Credentials authProviderX509CertUrl. */
        public authProviderX509CertUrl: string;

        /** Credentials clientX509CertUrl. */
        public clientX509CertUrl: string;

        /**
         * Creates a new Credentials instance using the specified properties.
         * @param [properties] Properties to set
         * @returns Credentials instance
         */
        public static create(properties?: BigQuery.ICredentials): BigQuery.Credentials;

        /**
         * Encodes the specified Credentials message. Does not implicitly {@link BigQuery.Credentials.verify|verify} messages.
         * @param message Credentials message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: BigQuery.ICredentials, writer?: $protobuf.Writer): $protobuf.Writer;

        /**
         * Encodes the specified Credentials message, length delimited. Does not implicitly {@link BigQuery.Credentials.verify|verify} messages.
         * @param message Credentials message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: BigQuery.ICredentials, writer?: $protobuf.Writer): $protobuf.Writer;

        /**
         * Decodes a Credentials message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns Credentials
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): BigQuery.Credentials;

        /**
         * Decodes a Credentials message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns Credentials
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): BigQuery.Credentials;

        /**
         * Verifies a Credentials message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a Credentials message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns Credentials
         */
        public static fromObject(object: { [k: string]: any }): BigQuery.Credentials;

        /**
         * Creates a plain object from a Credentials message. Also converts values to other types if specified.
         * @param message Credentials
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: BigQuery.Credentials, options?: $protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this Credentials to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };
    }
}
