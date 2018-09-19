/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars*/
"use strict";

var $protobuf = require("protobufjs/minimal");

// Common aliases
var $Reader = $protobuf.Reader, $Writer = $protobuf.Writer, $util = $protobuf.util;

// Exported root namespace
var $root = $protobuf.roots["default"] || ($protobuf.roots["default"] = {});

$root.ProjectConfig = (function() {

    /**
     * Properties of a ProjectConfig.
     * @exports IProjectConfig
     * @interface IProjectConfig
     * @property {string|null} [warehouse] ProjectConfig warehouse
     * @property {string|null} [defaultSchema] ProjectConfig defaultSchema
     * @property {string|null} [assertionSchema] ProjectConfig assertionSchema
     * @property {Array.<string>|null} [datasetPaths] ProjectConfig datasetPaths
     * @property {Array.<string>|null} [includePaths] ProjectConfig includePaths
     * @property {Object.<string,string>|null} [dependencies] ProjectConfig dependencies
     */

    /**
     * Constructs a new ProjectConfig.
     * @exports ProjectConfig
     * @classdesc Represents a ProjectConfig.
     * @implements IProjectConfig
     * @constructor
     * @param {IProjectConfig=} [properties] Properties to set
     */
    function ProjectConfig(properties) {
        this.datasetPaths = [];
        this.includePaths = [];
        this.dependencies = {};
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * ProjectConfig warehouse.
     * @member {string} warehouse
     * @memberof ProjectConfig
     * @instance
     */
    ProjectConfig.prototype.warehouse = "";

    /**
     * ProjectConfig defaultSchema.
     * @member {string} defaultSchema
     * @memberof ProjectConfig
     * @instance
     */
    ProjectConfig.prototype.defaultSchema = "";

    /**
     * ProjectConfig assertionSchema.
     * @member {string} assertionSchema
     * @memberof ProjectConfig
     * @instance
     */
    ProjectConfig.prototype.assertionSchema = "";

    /**
     * ProjectConfig datasetPaths.
     * @member {Array.<string>} datasetPaths
     * @memberof ProjectConfig
     * @instance
     */
    ProjectConfig.prototype.datasetPaths = $util.emptyArray;

    /**
     * ProjectConfig includePaths.
     * @member {Array.<string>} includePaths
     * @memberof ProjectConfig
     * @instance
     */
    ProjectConfig.prototype.includePaths = $util.emptyArray;

    /**
     * ProjectConfig dependencies.
     * @member {Object.<string,string>} dependencies
     * @memberof ProjectConfig
     * @instance
     */
    ProjectConfig.prototype.dependencies = $util.emptyObject;

    /**
     * Creates a new ProjectConfig instance using the specified properties.
     * @function create
     * @memberof ProjectConfig
     * @static
     * @param {IProjectConfig=} [properties] Properties to set
     * @returns {ProjectConfig} ProjectConfig instance
     */
    ProjectConfig.create = function create(properties) {
        return new ProjectConfig(properties);
    };

    /**
     * Encodes the specified ProjectConfig message. Does not implicitly {@link ProjectConfig.verify|verify} messages.
     * @function encode
     * @memberof ProjectConfig
     * @static
     * @param {IProjectConfig} message ProjectConfig message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ProjectConfig.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.warehouse != null && message.hasOwnProperty("warehouse"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.warehouse);
        if (message.defaultSchema != null && message.hasOwnProperty("defaultSchema"))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.defaultSchema);
        if (message.datasetPaths != null && message.datasetPaths.length)
            for (var i = 0; i < message.datasetPaths.length; ++i)
                writer.uint32(/* id 3, wireType 2 =*/26).string(message.datasetPaths[i]);
        if (message.includePaths != null && message.includePaths.length)
            for (var i = 0; i < message.includePaths.length; ++i)
                writer.uint32(/* id 4, wireType 2 =*/34).string(message.includePaths[i]);
        if (message.assertionSchema != null && message.hasOwnProperty("assertionSchema"))
            writer.uint32(/* id 5, wireType 2 =*/42).string(message.assertionSchema);
        if (message.dependencies != null && message.hasOwnProperty("dependencies"))
            for (var keys = Object.keys(message.dependencies), i = 0; i < keys.length; ++i)
                writer.uint32(/* id 6, wireType 2 =*/50).fork().uint32(/* id 1, wireType 2 =*/10).string(keys[i]).uint32(/* id 2, wireType 2 =*/18).string(message.dependencies[keys[i]]).ldelim();
        return writer;
    };

    /**
     * Encodes the specified ProjectConfig message, length delimited. Does not implicitly {@link ProjectConfig.verify|verify} messages.
     * @function encodeDelimited
     * @memberof ProjectConfig
     * @static
     * @param {IProjectConfig} message ProjectConfig message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ProjectConfig.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a ProjectConfig message from the specified reader or buffer.
     * @function decode
     * @memberof ProjectConfig
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {ProjectConfig} ProjectConfig
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ProjectConfig.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.ProjectConfig(), key;
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.warehouse = reader.string();
                break;
            case 2:
                message.defaultSchema = reader.string();
                break;
            case 5:
                message.assertionSchema = reader.string();
                break;
            case 3:
                if (!(message.datasetPaths && message.datasetPaths.length))
                    message.datasetPaths = [];
                message.datasetPaths.push(reader.string());
                break;
            case 4:
                if (!(message.includePaths && message.includePaths.length))
                    message.includePaths = [];
                message.includePaths.push(reader.string());
                break;
            case 6:
                reader.skip().pos++;
                if (message.dependencies === $util.emptyObject)
                    message.dependencies = {};
                key = reader.string();
                reader.pos++;
                message.dependencies[key] = reader.string();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a ProjectConfig message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof ProjectConfig
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {ProjectConfig} ProjectConfig
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ProjectConfig.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a ProjectConfig message.
     * @function verify
     * @memberof ProjectConfig
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ProjectConfig.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.warehouse != null && message.hasOwnProperty("warehouse"))
            if (!$util.isString(message.warehouse))
                return "warehouse: string expected";
        if (message.defaultSchema != null && message.hasOwnProperty("defaultSchema"))
            if (!$util.isString(message.defaultSchema))
                return "defaultSchema: string expected";
        if (message.assertionSchema != null && message.hasOwnProperty("assertionSchema"))
            if (!$util.isString(message.assertionSchema))
                return "assertionSchema: string expected";
        if (message.datasetPaths != null && message.hasOwnProperty("datasetPaths")) {
            if (!Array.isArray(message.datasetPaths))
                return "datasetPaths: array expected";
            for (var i = 0; i < message.datasetPaths.length; ++i)
                if (!$util.isString(message.datasetPaths[i]))
                    return "datasetPaths: string[] expected";
        }
        if (message.includePaths != null && message.hasOwnProperty("includePaths")) {
            if (!Array.isArray(message.includePaths))
                return "includePaths: array expected";
            for (var i = 0; i < message.includePaths.length; ++i)
                if (!$util.isString(message.includePaths[i]))
                    return "includePaths: string[] expected";
        }
        if (message.dependencies != null && message.hasOwnProperty("dependencies")) {
            if (!$util.isObject(message.dependencies))
                return "dependencies: object expected";
            var key = Object.keys(message.dependencies);
            for (var i = 0; i < key.length; ++i)
                if (!$util.isString(message.dependencies[key[i]]))
                    return "dependencies: string{k:string} expected";
        }
        return null;
    };

    /**
     * Creates a ProjectConfig message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof ProjectConfig
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {ProjectConfig} ProjectConfig
     */
    ProjectConfig.fromObject = function fromObject(object) {
        if (object instanceof $root.ProjectConfig)
            return object;
        var message = new $root.ProjectConfig();
        if (object.warehouse != null)
            message.warehouse = String(object.warehouse);
        if (object.defaultSchema != null)
            message.defaultSchema = String(object.defaultSchema);
        if (object.assertionSchema != null)
            message.assertionSchema = String(object.assertionSchema);
        if (object.datasetPaths) {
            if (!Array.isArray(object.datasetPaths))
                throw TypeError(".ProjectConfig.datasetPaths: array expected");
            message.datasetPaths = [];
            for (var i = 0; i < object.datasetPaths.length; ++i)
                message.datasetPaths[i] = String(object.datasetPaths[i]);
        }
        if (object.includePaths) {
            if (!Array.isArray(object.includePaths))
                throw TypeError(".ProjectConfig.includePaths: array expected");
            message.includePaths = [];
            for (var i = 0; i < object.includePaths.length; ++i)
                message.includePaths[i] = String(object.includePaths[i]);
        }
        if (object.dependencies) {
            if (typeof object.dependencies !== "object")
                throw TypeError(".ProjectConfig.dependencies: object expected");
            message.dependencies = {};
            for (var keys = Object.keys(object.dependencies), i = 0; i < keys.length; ++i)
                message.dependencies[keys[i]] = String(object.dependencies[keys[i]]);
        }
        return message;
    };

    /**
     * Creates a plain object from a ProjectConfig message. Also converts values to other types if specified.
     * @function toObject
     * @memberof ProjectConfig
     * @static
     * @param {ProjectConfig} message ProjectConfig
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ProjectConfig.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults) {
            object.datasetPaths = [];
            object.includePaths = [];
        }
        if (options.objects || options.defaults)
            object.dependencies = {};
        if (options.defaults) {
            object.warehouse = "";
            object.defaultSchema = "";
            object.assertionSchema = "";
        }
        if (message.warehouse != null && message.hasOwnProperty("warehouse"))
            object.warehouse = message.warehouse;
        if (message.defaultSchema != null && message.hasOwnProperty("defaultSchema"))
            object.defaultSchema = message.defaultSchema;
        if (message.datasetPaths && message.datasetPaths.length) {
            object.datasetPaths = [];
            for (var j = 0; j < message.datasetPaths.length; ++j)
                object.datasetPaths[j] = message.datasetPaths[j];
        }
        if (message.includePaths && message.includePaths.length) {
            object.includePaths = [];
            for (var j = 0; j < message.includePaths.length; ++j)
                object.includePaths[j] = message.includePaths[j];
        }
        if (message.assertionSchema != null && message.hasOwnProperty("assertionSchema"))
            object.assertionSchema = message.assertionSchema;
        var keys2;
        if (message.dependencies && (keys2 = Object.keys(message.dependencies)).length) {
            object.dependencies = {};
            for (var j = 0; j < keys2.length; ++j)
                object.dependencies[keys2[j]] = message.dependencies[keys2[j]];
        }
        return object;
    };

    /**
     * Converts this ProjectConfig to JSON.
     * @function toJSON
     * @memberof ProjectConfig
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ProjectConfig.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return ProjectConfig;
})();

$root.Target = (function() {

    /**
     * Properties of a Target.
     * @exports ITarget
     * @interface ITarget
     * @property {string|null} [schema] Target schema
     * @property {string|null} [name] Target name
     */

    /**
     * Constructs a new Target.
     * @exports Target
     * @classdesc Represents a Target.
     * @implements ITarget
     * @constructor
     * @param {ITarget=} [properties] Properties to set
     */
    function Target(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * Target schema.
     * @member {string} schema
     * @memberof Target
     * @instance
     */
    Target.prototype.schema = "";

    /**
     * Target name.
     * @member {string} name
     * @memberof Target
     * @instance
     */
    Target.prototype.name = "";

    /**
     * Creates a new Target instance using the specified properties.
     * @function create
     * @memberof Target
     * @static
     * @param {ITarget=} [properties] Properties to set
     * @returns {Target} Target instance
     */
    Target.create = function create(properties) {
        return new Target(properties);
    };

    /**
     * Encodes the specified Target message. Does not implicitly {@link Target.verify|verify} messages.
     * @function encode
     * @memberof Target
     * @static
     * @param {ITarget} message Target message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Target.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.schema != null && message.hasOwnProperty("schema"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.schema);
        if (message.name != null && message.hasOwnProperty("name"))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.name);
        return writer;
    };

    /**
     * Encodes the specified Target message, length delimited. Does not implicitly {@link Target.verify|verify} messages.
     * @function encodeDelimited
     * @memberof Target
     * @static
     * @param {ITarget} message Target message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Target.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a Target message from the specified reader or buffer.
     * @function decode
     * @memberof Target
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {Target} Target
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Target.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.Target();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.schema = reader.string();
                break;
            case 2:
                message.name = reader.string();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a Target message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof Target
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {Target} Target
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Target.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a Target message.
     * @function verify
     * @memberof Target
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    Target.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.schema != null && message.hasOwnProperty("schema"))
            if (!$util.isString(message.schema))
                return "schema: string expected";
        if (message.name != null && message.hasOwnProperty("name"))
            if (!$util.isString(message.name))
                return "name: string expected";
        return null;
    };

    /**
     * Creates a Target message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof Target
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {Target} Target
     */
    Target.fromObject = function fromObject(object) {
        if (object instanceof $root.Target)
            return object;
        var message = new $root.Target();
        if (object.schema != null)
            message.schema = String(object.schema);
        if (object.name != null)
            message.name = String(object.name);
        return message;
    };

    /**
     * Creates a plain object from a Target message. Also converts values to other types if specified.
     * @function toObject
     * @memberof Target
     * @static
     * @param {Target} message Target
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    Target.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            object.schema = "";
            object.name = "";
        }
        if (message.schema != null && message.hasOwnProperty("schema"))
            object.schema = message.schema;
        if (message.name != null && message.hasOwnProperty("name"))
            object.name = message.name;
        return object;
    };

    /**
     * Converts this Target to JSON.
     * @function toJSON
     * @memberof Target
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    Target.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return Target;
})();

$root.Materialization = (function() {

    /**
     * Properties of a Materialization.
     * @exports IMaterialization
     * @interface IMaterialization
     * @property {string|null} [name] Materialization name
     * @property {Array.<string>|null} [dependencies] Materialization dependencies
     * @property {string|null} [type] Materialization type
     * @property {ITarget|null} [target] Materialization target
     * @property {string|null} [query] Materialization query
     * @property {boolean|null} ["protected"] Materialization protected
     * @property {string|null} [partitionBy] Materialization partitionBy
     * @property {Object.<string,string>|null} [descriptions] Materialization descriptions
     * @property {string|null} [where] Materialization where
     * @property {string|null} [uniqueKey] Materialization uniqueKey
     * @property {Array.<string>|null} [pres] Materialization pres
     * @property {Array.<string>|null} [posts] Materialization posts
     * @property {Array.<string>|null} [assertions] Materialization assertions
     * @property {Array.<string>|null} [parsedColumns] Materialization parsedColumns
     */

    /**
     * Constructs a new Materialization.
     * @exports Materialization
     * @classdesc Represents a Materialization.
     * @implements IMaterialization
     * @constructor
     * @param {IMaterialization=} [properties] Properties to set
     */
    function Materialization(properties) {
        this.dependencies = [];
        this.descriptions = {};
        this.pres = [];
        this.posts = [];
        this.assertions = [];
        this.parsedColumns = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * Materialization name.
     * @member {string} name
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.name = "";

    /**
     * Materialization dependencies.
     * @member {Array.<string>} dependencies
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.dependencies = $util.emptyArray;

    /**
     * Materialization type.
     * @member {string} type
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.type = "";

    /**
     * Materialization target.
     * @member {ITarget|null|undefined} target
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.target = null;

    /**
     * Materialization query.
     * @member {string} query
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.query = "";

    /**
     * Materialization protected.
     * @member {boolean} protected
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype["protected"] = false;

    /**
     * Materialization partitionBy.
     * @member {string} partitionBy
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.partitionBy = "";

    /**
     * Materialization descriptions.
     * @member {Object.<string,string>} descriptions
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.descriptions = $util.emptyObject;

    /**
     * Materialization where.
     * @member {string} where
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.where = "";

    /**
     * Materialization uniqueKey.
     * @member {string} uniqueKey
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.uniqueKey = "";

    /**
     * Materialization pres.
     * @member {Array.<string>} pres
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.pres = $util.emptyArray;

    /**
     * Materialization posts.
     * @member {Array.<string>} posts
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.posts = $util.emptyArray;

    /**
     * Materialization assertions.
     * @member {Array.<string>} assertions
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.assertions = $util.emptyArray;

    /**
     * Materialization parsedColumns.
     * @member {Array.<string>} parsedColumns
     * @memberof Materialization
     * @instance
     */
    Materialization.prototype.parsedColumns = $util.emptyArray;

    /**
     * Creates a new Materialization instance using the specified properties.
     * @function create
     * @memberof Materialization
     * @static
     * @param {IMaterialization=} [properties] Properties to set
     * @returns {Materialization} Materialization instance
     */
    Materialization.create = function create(properties) {
        return new Materialization(properties);
    };

    /**
     * Encodes the specified Materialization message. Does not implicitly {@link Materialization.verify|verify} messages.
     * @function encode
     * @memberof Materialization
     * @static
     * @param {IMaterialization} message Materialization message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Materialization.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.name != null && message.hasOwnProperty("name"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.name);
        if (message.dependencies != null && message.dependencies.length)
            for (var i = 0; i < message.dependencies.length; ++i)
                writer.uint32(/* id 2, wireType 2 =*/18).string(message.dependencies[i]);
        if (message.type != null && message.hasOwnProperty("type"))
            writer.uint32(/* id 3, wireType 2 =*/26).string(message.type);
        if (message.target != null && message.hasOwnProperty("target"))
            $root.Target.encode(message.target, writer.uint32(/* id 4, wireType 2 =*/34).fork()).ldelim();
        if (message.query != null && message.hasOwnProperty("query"))
            writer.uint32(/* id 5, wireType 2 =*/42).string(message.query);
        if (message.where != null && message.hasOwnProperty("where"))
            writer.uint32(/* id 8, wireType 2 =*/66).string(message.where);
        if (message["protected"] != null && message.hasOwnProperty("protected"))
            writer.uint32(/* id 9, wireType 0 =*/72).bool(message["protected"]);
        if (message.partitionBy != null && message.hasOwnProperty("partitionBy"))
            writer.uint32(/* id 10, wireType 2 =*/82).string(message.partitionBy);
        if (message.parsedColumns != null && message.parsedColumns.length)
            for (var i = 0; i < message.parsedColumns.length; ++i)
                writer.uint32(/* id 12, wireType 2 =*/98).string(message.parsedColumns[i]);
        if (message.pres != null && message.pres.length)
            for (var i = 0; i < message.pres.length; ++i)
                writer.uint32(/* id 13, wireType 2 =*/106).string(message.pres[i]);
        if (message.posts != null && message.posts.length)
            for (var i = 0; i < message.posts.length; ++i)
                writer.uint32(/* id 14, wireType 2 =*/114).string(message.posts[i]);
        if (message.uniqueKey != null && message.hasOwnProperty("uniqueKey"))
            writer.uint32(/* id 15, wireType 2 =*/122).string(message.uniqueKey);
        if (message.descriptions != null && message.hasOwnProperty("descriptions"))
            for (var keys = Object.keys(message.descriptions), i = 0; i < keys.length; ++i)
                writer.uint32(/* id 16, wireType 2 =*/130).fork().uint32(/* id 1, wireType 2 =*/10).string(keys[i]).uint32(/* id 2, wireType 2 =*/18).string(message.descriptions[keys[i]]).ldelim();
        if (message.assertions != null && message.assertions.length)
            for (var i = 0; i < message.assertions.length; ++i)
                writer.uint32(/* id 17, wireType 2 =*/138).string(message.assertions[i]);
        return writer;
    };

    /**
     * Encodes the specified Materialization message, length delimited. Does not implicitly {@link Materialization.verify|verify} messages.
     * @function encodeDelimited
     * @memberof Materialization
     * @static
     * @param {IMaterialization} message Materialization message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Materialization.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a Materialization message from the specified reader or buffer.
     * @function decode
     * @memberof Materialization
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {Materialization} Materialization
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Materialization.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.Materialization(), key;
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.name = reader.string();
                break;
            case 2:
                if (!(message.dependencies && message.dependencies.length))
                    message.dependencies = [];
                message.dependencies.push(reader.string());
                break;
            case 3:
                message.type = reader.string();
                break;
            case 4:
                message.target = $root.Target.decode(reader, reader.uint32());
                break;
            case 5:
                message.query = reader.string();
                break;
            case 9:
                message["protected"] = reader.bool();
                break;
            case 10:
                message.partitionBy = reader.string();
                break;
            case 16:
                reader.skip().pos++;
                if (message.descriptions === $util.emptyObject)
                    message.descriptions = {};
                key = reader.string();
                reader.pos++;
                message.descriptions[key] = reader.string();
                break;
            case 8:
                message.where = reader.string();
                break;
            case 15:
                message.uniqueKey = reader.string();
                break;
            case 13:
                if (!(message.pres && message.pres.length))
                    message.pres = [];
                message.pres.push(reader.string());
                break;
            case 14:
                if (!(message.posts && message.posts.length))
                    message.posts = [];
                message.posts.push(reader.string());
                break;
            case 17:
                if (!(message.assertions && message.assertions.length))
                    message.assertions = [];
                message.assertions.push(reader.string());
                break;
            case 12:
                if (!(message.parsedColumns && message.parsedColumns.length))
                    message.parsedColumns = [];
                message.parsedColumns.push(reader.string());
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a Materialization message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof Materialization
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {Materialization} Materialization
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Materialization.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a Materialization message.
     * @function verify
     * @memberof Materialization
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    Materialization.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.name != null && message.hasOwnProperty("name"))
            if (!$util.isString(message.name))
                return "name: string expected";
        if (message.dependencies != null && message.hasOwnProperty("dependencies")) {
            if (!Array.isArray(message.dependencies))
                return "dependencies: array expected";
            for (var i = 0; i < message.dependencies.length; ++i)
                if (!$util.isString(message.dependencies[i]))
                    return "dependencies: string[] expected";
        }
        if (message.type != null && message.hasOwnProperty("type"))
            if (!$util.isString(message.type))
                return "type: string expected";
        if (message.target != null && message.hasOwnProperty("target")) {
            var error = $root.Target.verify(message.target);
            if (error)
                return "target." + error;
        }
        if (message.query != null && message.hasOwnProperty("query"))
            if (!$util.isString(message.query))
                return "query: string expected";
        if (message["protected"] != null && message.hasOwnProperty("protected"))
            if (typeof message["protected"] !== "boolean")
                return "protected: boolean expected";
        if (message.partitionBy != null && message.hasOwnProperty("partitionBy"))
            if (!$util.isString(message.partitionBy))
                return "partitionBy: string expected";
        if (message.descriptions != null && message.hasOwnProperty("descriptions")) {
            if (!$util.isObject(message.descriptions))
                return "descriptions: object expected";
            var key = Object.keys(message.descriptions);
            for (var i = 0; i < key.length; ++i)
                if (!$util.isString(message.descriptions[key[i]]))
                    return "descriptions: string{k:string} expected";
        }
        if (message.where != null && message.hasOwnProperty("where"))
            if (!$util.isString(message.where))
                return "where: string expected";
        if (message.uniqueKey != null && message.hasOwnProperty("uniqueKey"))
            if (!$util.isString(message.uniqueKey))
                return "uniqueKey: string expected";
        if (message.pres != null && message.hasOwnProperty("pres")) {
            if (!Array.isArray(message.pres))
                return "pres: array expected";
            for (var i = 0; i < message.pres.length; ++i)
                if (!$util.isString(message.pres[i]))
                    return "pres: string[] expected";
        }
        if (message.posts != null && message.hasOwnProperty("posts")) {
            if (!Array.isArray(message.posts))
                return "posts: array expected";
            for (var i = 0; i < message.posts.length; ++i)
                if (!$util.isString(message.posts[i]))
                    return "posts: string[] expected";
        }
        if (message.assertions != null && message.hasOwnProperty("assertions")) {
            if (!Array.isArray(message.assertions))
                return "assertions: array expected";
            for (var i = 0; i < message.assertions.length; ++i)
                if (!$util.isString(message.assertions[i]))
                    return "assertions: string[] expected";
        }
        if (message.parsedColumns != null && message.hasOwnProperty("parsedColumns")) {
            if (!Array.isArray(message.parsedColumns))
                return "parsedColumns: array expected";
            for (var i = 0; i < message.parsedColumns.length; ++i)
                if (!$util.isString(message.parsedColumns[i]))
                    return "parsedColumns: string[] expected";
        }
        return null;
    };

    /**
     * Creates a Materialization message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof Materialization
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {Materialization} Materialization
     */
    Materialization.fromObject = function fromObject(object) {
        if (object instanceof $root.Materialization)
            return object;
        var message = new $root.Materialization();
        if (object.name != null)
            message.name = String(object.name);
        if (object.dependencies) {
            if (!Array.isArray(object.dependencies))
                throw TypeError(".Materialization.dependencies: array expected");
            message.dependencies = [];
            for (var i = 0; i < object.dependencies.length; ++i)
                message.dependencies[i] = String(object.dependencies[i]);
        }
        if (object.type != null)
            message.type = String(object.type);
        if (object.target != null) {
            if (typeof object.target !== "object")
                throw TypeError(".Materialization.target: object expected");
            message.target = $root.Target.fromObject(object.target);
        }
        if (object.query != null)
            message.query = String(object.query);
        if (object["protected"] != null)
            message["protected"] = Boolean(object["protected"]);
        if (object.partitionBy != null)
            message.partitionBy = String(object.partitionBy);
        if (object.descriptions) {
            if (typeof object.descriptions !== "object")
                throw TypeError(".Materialization.descriptions: object expected");
            message.descriptions = {};
            for (var keys = Object.keys(object.descriptions), i = 0; i < keys.length; ++i)
                message.descriptions[keys[i]] = String(object.descriptions[keys[i]]);
        }
        if (object.where != null)
            message.where = String(object.where);
        if (object.uniqueKey != null)
            message.uniqueKey = String(object.uniqueKey);
        if (object.pres) {
            if (!Array.isArray(object.pres))
                throw TypeError(".Materialization.pres: array expected");
            message.pres = [];
            for (var i = 0; i < object.pres.length; ++i)
                message.pres[i] = String(object.pres[i]);
        }
        if (object.posts) {
            if (!Array.isArray(object.posts))
                throw TypeError(".Materialization.posts: array expected");
            message.posts = [];
            for (var i = 0; i < object.posts.length; ++i)
                message.posts[i] = String(object.posts[i]);
        }
        if (object.assertions) {
            if (!Array.isArray(object.assertions))
                throw TypeError(".Materialization.assertions: array expected");
            message.assertions = [];
            for (var i = 0; i < object.assertions.length; ++i)
                message.assertions[i] = String(object.assertions[i]);
        }
        if (object.parsedColumns) {
            if (!Array.isArray(object.parsedColumns))
                throw TypeError(".Materialization.parsedColumns: array expected");
            message.parsedColumns = [];
            for (var i = 0; i < object.parsedColumns.length; ++i)
                message.parsedColumns[i] = String(object.parsedColumns[i]);
        }
        return message;
    };

    /**
     * Creates a plain object from a Materialization message. Also converts values to other types if specified.
     * @function toObject
     * @memberof Materialization
     * @static
     * @param {Materialization} message Materialization
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    Materialization.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults) {
            object.dependencies = [];
            object.parsedColumns = [];
            object.pres = [];
            object.posts = [];
            object.assertions = [];
        }
        if (options.objects || options.defaults)
            object.descriptions = {};
        if (options.defaults) {
            object.name = "";
            object.type = "";
            object.target = null;
            object.query = "";
            object.where = "";
            object["protected"] = false;
            object.partitionBy = "";
            object.uniqueKey = "";
        }
        if (message.name != null && message.hasOwnProperty("name"))
            object.name = message.name;
        if (message.dependencies && message.dependencies.length) {
            object.dependencies = [];
            for (var j = 0; j < message.dependencies.length; ++j)
                object.dependencies[j] = message.dependencies[j];
        }
        if (message.type != null && message.hasOwnProperty("type"))
            object.type = message.type;
        if (message.target != null && message.hasOwnProperty("target"))
            object.target = $root.Target.toObject(message.target, options);
        if (message.query != null && message.hasOwnProperty("query"))
            object.query = message.query;
        if (message.where != null && message.hasOwnProperty("where"))
            object.where = message.where;
        if (message["protected"] != null && message.hasOwnProperty("protected"))
            object["protected"] = message["protected"];
        if (message.partitionBy != null && message.hasOwnProperty("partitionBy"))
            object.partitionBy = message.partitionBy;
        if (message.parsedColumns && message.parsedColumns.length) {
            object.parsedColumns = [];
            for (var j = 0; j < message.parsedColumns.length; ++j)
                object.parsedColumns[j] = message.parsedColumns[j];
        }
        if (message.pres && message.pres.length) {
            object.pres = [];
            for (var j = 0; j < message.pres.length; ++j)
                object.pres[j] = message.pres[j];
        }
        if (message.posts && message.posts.length) {
            object.posts = [];
            for (var j = 0; j < message.posts.length; ++j)
                object.posts[j] = message.posts[j];
        }
        if (message.uniqueKey != null && message.hasOwnProperty("uniqueKey"))
            object.uniqueKey = message.uniqueKey;
        var keys2;
        if (message.descriptions && (keys2 = Object.keys(message.descriptions)).length) {
            object.descriptions = {};
            for (var j = 0; j < keys2.length; ++j)
                object.descriptions[keys2[j]] = message.descriptions[keys2[j]];
        }
        if (message.assertions && message.assertions.length) {
            object.assertions = [];
            for (var j = 0; j < message.assertions.length; ++j)
                object.assertions[j] = message.assertions[j];
        }
        return object;
    };

    /**
     * Converts this Materialization to JSON.
     * @function toJSON
     * @memberof Materialization
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    Materialization.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return Materialization;
})();

$root.Operation = (function() {

    /**
     * Properties of an Operation.
     * @exports IOperation
     * @interface IOperation
     * @property {string|null} [name] Operation name
     * @property {Array.<string>|null} [dependencies] Operation dependencies
     * @property {Array.<string>|null} [statements] Operation statements
     */

    /**
     * Constructs a new Operation.
     * @exports Operation
     * @classdesc Represents an Operation.
     * @implements IOperation
     * @constructor
     * @param {IOperation=} [properties] Properties to set
     */
    function Operation(properties) {
        this.dependencies = [];
        this.statements = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * Operation name.
     * @member {string} name
     * @memberof Operation
     * @instance
     */
    Operation.prototype.name = "";

    /**
     * Operation dependencies.
     * @member {Array.<string>} dependencies
     * @memberof Operation
     * @instance
     */
    Operation.prototype.dependencies = $util.emptyArray;

    /**
     * Operation statements.
     * @member {Array.<string>} statements
     * @memberof Operation
     * @instance
     */
    Operation.prototype.statements = $util.emptyArray;

    /**
     * Creates a new Operation instance using the specified properties.
     * @function create
     * @memberof Operation
     * @static
     * @param {IOperation=} [properties] Properties to set
     * @returns {Operation} Operation instance
     */
    Operation.create = function create(properties) {
        return new Operation(properties);
    };

    /**
     * Encodes the specified Operation message. Does not implicitly {@link Operation.verify|verify} messages.
     * @function encode
     * @memberof Operation
     * @static
     * @param {IOperation} message Operation message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Operation.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.name != null && message.hasOwnProperty("name"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.name);
        if (message.dependencies != null && message.dependencies.length)
            for (var i = 0; i < message.dependencies.length; ++i)
                writer.uint32(/* id 2, wireType 2 =*/18).string(message.dependencies[i]);
        if (message.statements != null && message.statements.length)
            for (var i = 0; i < message.statements.length; ++i)
                writer.uint32(/* id 6, wireType 2 =*/50).string(message.statements[i]);
        return writer;
    };

    /**
     * Encodes the specified Operation message, length delimited. Does not implicitly {@link Operation.verify|verify} messages.
     * @function encodeDelimited
     * @memberof Operation
     * @static
     * @param {IOperation} message Operation message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Operation.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an Operation message from the specified reader or buffer.
     * @function decode
     * @memberof Operation
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {Operation} Operation
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Operation.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.Operation();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.name = reader.string();
                break;
            case 2:
                if (!(message.dependencies && message.dependencies.length))
                    message.dependencies = [];
                message.dependencies.push(reader.string());
                break;
            case 6:
                if (!(message.statements && message.statements.length))
                    message.statements = [];
                message.statements.push(reader.string());
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an Operation message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof Operation
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {Operation} Operation
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Operation.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an Operation message.
     * @function verify
     * @memberof Operation
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    Operation.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.name != null && message.hasOwnProperty("name"))
            if (!$util.isString(message.name))
                return "name: string expected";
        if (message.dependencies != null && message.hasOwnProperty("dependencies")) {
            if (!Array.isArray(message.dependencies))
                return "dependencies: array expected";
            for (var i = 0; i < message.dependencies.length; ++i)
                if (!$util.isString(message.dependencies[i]))
                    return "dependencies: string[] expected";
        }
        if (message.statements != null && message.hasOwnProperty("statements")) {
            if (!Array.isArray(message.statements))
                return "statements: array expected";
            for (var i = 0; i < message.statements.length; ++i)
                if (!$util.isString(message.statements[i]))
                    return "statements: string[] expected";
        }
        return null;
    };

    /**
     * Creates an Operation message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof Operation
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {Operation} Operation
     */
    Operation.fromObject = function fromObject(object) {
        if (object instanceof $root.Operation)
            return object;
        var message = new $root.Operation();
        if (object.name != null)
            message.name = String(object.name);
        if (object.dependencies) {
            if (!Array.isArray(object.dependencies))
                throw TypeError(".Operation.dependencies: array expected");
            message.dependencies = [];
            for (var i = 0; i < object.dependencies.length; ++i)
                message.dependencies[i] = String(object.dependencies[i]);
        }
        if (object.statements) {
            if (!Array.isArray(object.statements))
                throw TypeError(".Operation.statements: array expected");
            message.statements = [];
            for (var i = 0; i < object.statements.length; ++i)
                message.statements[i] = String(object.statements[i]);
        }
        return message;
    };

    /**
     * Creates a plain object from an Operation message. Also converts values to other types if specified.
     * @function toObject
     * @memberof Operation
     * @static
     * @param {Operation} message Operation
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    Operation.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults) {
            object.dependencies = [];
            object.statements = [];
        }
        if (options.defaults)
            object.name = "";
        if (message.name != null && message.hasOwnProperty("name"))
            object.name = message.name;
        if (message.dependencies && message.dependencies.length) {
            object.dependencies = [];
            for (var j = 0; j < message.dependencies.length; ++j)
                object.dependencies[j] = message.dependencies[j];
        }
        if (message.statements && message.statements.length) {
            object.statements = [];
            for (var j = 0; j < message.statements.length; ++j)
                object.statements[j] = message.statements[j];
        }
        return object;
    };

    /**
     * Converts this Operation to JSON.
     * @function toJSON
     * @memberof Operation
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    Operation.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return Operation;
})();

$root.Assertion = (function() {

    /**
     * Properties of an Assertion.
     * @exports IAssertion
     * @interface IAssertion
     * @property {string|null} [name] Assertion name
     * @property {Array.<string>|null} [dependencies] Assertion dependencies
     * @property {Array.<string>|null} [queries] Assertion queries
     */

    /**
     * Constructs a new Assertion.
     * @exports Assertion
     * @classdesc Represents an Assertion.
     * @implements IAssertion
     * @constructor
     * @param {IAssertion=} [properties] Properties to set
     */
    function Assertion(properties) {
        this.dependencies = [];
        this.queries = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * Assertion name.
     * @member {string} name
     * @memberof Assertion
     * @instance
     */
    Assertion.prototype.name = "";

    /**
     * Assertion dependencies.
     * @member {Array.<string>} dependencies
     * @memberof Assertion
     * @instance
     */
    Assertion.prototype.dependencies = $util.emptyArray;

    /**
     * Assertion queries.
     * @member {Array.<string>} queries
     * @memberof Assertion
     * @instance
     */
    Assertion.prototype.queries = $util.emptyArray;

    /**
     * Creates a new Assertion instance using the specified properties.
     * @function create
     * @memberof Assertion
     * @static
     * @param {IAssertion=} [properties] Properties to set
     * @returns {Assertion} Assertion instance
     */
    Assertion.create = function create(properties) {
        return new Assertion(properties);
    };

    /**
     * Encodes the specified Assertion message. Does not implicitly {@link Assertion.verify|verify} messages.
     * @function encode
     * @memberof Assertion
     * @static
     * @param {IAssertion} message Assertion message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Assertion.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.name != null && message.hasOwnProperty("name"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.name);
        if (message.dependencies != null && message.dependencies.length)
            for (var i = 0; i < message.dependencies.length; ++i)
                writer.uint32(/* id 2, wireType 2 =*/18).string(message.dependencies[i]);
        if (message.queries != null && message.queries.length)
            for (var i = 0; i < message.queries.length; ++i)
                writer.uint32(/* id 3, wireType 2 =*/26).string(message.queries[i]);
        return writer;
    };

    /**
     * Encodes the specified Assertion message, length delimited. Does not implicitly {@link Assertion.verify|verify} messages.
     * @function encodeDelimited
     * @memberof Assertion
     * @static
     * @param {IAssertion} message Assertion message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Assertion.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an Assertion message from the specified reader or buffer.
     * @function decode
     * @memberof Assertion
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {Assertion} Assertion
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Assertion.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.Assertion();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.name = reader.string();
                break;
            case 2:
                if (!(message.dependencies && message.dependencies.length))
                    message.dependencies = [];
                message.dependencies.push(reader.string());
                break;
            case 3:
                if (!(message.queries && message.queries.length))
                    message.queries = [];
                message.queries.push(reader.string());
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an Assertion message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof Assertion
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {Assertion} Assertion
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Assertion.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an Assertion message.
     * @function verify
     * @memberof Assertion
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    Assertion.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.name != null && message.hasOwnProperty("name"))
            if (!$util.isString(message.name))
                return "name: string expected";
        if (message.dependencies != null && message.hasOwnProperty("dependencies")) {
            if (!Array.isArray(message.dependencies))
                return "dependencies: array expected";
            for (var i = 0; i < message.dependencies.length; ++i)
                if (!$util.isString(message.dependencies[i]))
                    return "dependencies: string[] expected";
        }
        if (message.queries != null && message.hasOwnProperty("queries")) {
            if (!Array.isArray(message.queries))
                return "queries: array expected";
            for (var i = 0; i < message.queries.length; ++i)
                if (!$util.isString(message.queries[i]))
                    return "queries: string[] expected";
        }
        return null;
    };

    /**
     * Creates an Assertion message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof Assertion
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {Assertion} Assertion
     */
    Assertion.fromObject = function fromObject(object) {
        if (object instanceof $root.Assertion)
            return object;
        var message = new $root.Assertion();
        if (object.name != null)
            message.name = String(object.name);
        if (object.dependencies) {
            if (!Array.isArray(object.dependencies))
                throw TypeError(".Assertion.dependencies: array expected");
            message.dependencies = [];
            for (var i = 0; i < object.dependencies.length; ++i)
                message.dependencies[i] = String(object.dependencies[i]);
        }
        if (object.queries) {
            if (!Array.isArray(object.queries))
                throw TypeError(".Assertion.queries: array expected");
            message.queries = [];
            for (var i = 0; i < object.queries.length; ++i)
                message.queries[i] = String(object.queries[i]);
        }
        return message;
    };

    /**
     * Creates a plain object from an Assertion message. Also converts values to other types if specified.
     * @function toObject
     * @memberof Assertion
     * @static
     * @param {Assertion} message Assertion
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    Assertion.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults) {
            object.dependencies = [];
            object.queries = [];
        }
        if (options.defaults)
            object.name = "";
        if (message.name != null && message.hasOwnProperty("name"))
            object.name = message.name;
        if (message.dependencies && message.dependencies.length) {
            object.dependencies = [];
            for (var j = 0; j < message.dependencies.length; ++j)
                object.dependencies[j] = message.dependencies[j];
        }
        if (message.queries && message.queries.length) {
            object.queries = [];
            for (var j = 0; j < message.queries.length; ++j)
                object.queries[j] = message.queries[j];
        }
        return object;
    };

    /**
     * Converts this Assertion to JSON.
     * @function toJSON
     * @memberof Assertion
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    Assertion.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return Assertion;
})();

$root.CompiledGraph = (function() {

    /**
     * Properties of a CompiledGraph.
     * @exports ICompiledGraph
     * @interface ICompiledGraph
     * @property {IProjectConfig|null} [projectConfig] CompiledGraph projectConfig
     * @property {Array.<IMaterialization>|null} [materializations] CompiledGraph materializations
     * @property {Array.<IOperation>|null} [operations] CompiledGraph operations
     * @property {Array.<IAssertion>|null} [assertions] CompiledGraph assertions
     */

    /**
     * Constructs a new CompiledGraph.
     * @exports CompiledGraph
     * @classdesc Represents a CompiledGraph.
     * @implements ICompiledGraph
     * @constructor
     * @param {ICompiledGraph=} [properties] Properties to set
     */
    function CompiledGraph(properties) {
        this.materializations = [];
        this.operations = [];
        this.assertions = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * CompiledGraph projectConfig.
     * @member {IProjectConfig|null|undefined} projectConfig
     * @memberof CompiledGraph
     * @instance
     */
    CompiledGraph.prototype.projectConfig = null;

    /**
     * CompiledGraph materializations.
     * @member {Array.<IMaterialization>} materializations
     * @memberof CompiledGraph
     * @instance
     */
    CompiledGraph.prototype.materializations = $util.emptyArray;

    /**
     * CompiledGraph operations.
     * @member {Array.<IOperation>} operations
     * @memberof CompiledGraph
     * @instance
     */
    CompiledGraph.prototype.operations = $util.emptyArray;

    /**
     * CompiledGraph assertions.
     * @member {Array.<IAssertion>} assertions
     * @memberof CompiledGraph
     * @instance
     */
    CompiledGraph.prototype.assertions = $util.emptyArray;

    /**
     * Creates a new CompiledGraph instance using the specified properties.
     * @function create
     * @memberof CompiledGraph
     * @static
     * @param {ICompiledGraph=} [properties] Properties to set
     * @returns {CompiledGraph} CompiledGraph instance
     */
    CompiledGraph.create = function create(properties) {
        return new CompiledGraph(properties);
    };

    /**
     * Encodes the specified CompiledGraph message. Does not implicitly {@link CompiledGraph.verify|verify} messages.
     * @function encode
     * @memberof CompiledGraph
     * @static
     * @param {ICompiledGraph} message CompiledGraph message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    CompiledGraph.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.materializations != null && message.materializations.length)
            for (var i = 0; i < message.materializations.length; ++i)
                $root.Materialization.encode(message.materializations[i], writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
        if (message.operations != null && message.operations.length)
            for (var i = 0; i < message.operations.length; ++i)
                $root.Operation.encode(message.operations[i], writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
        if (message.assertions != null && message.assertions.length)
            for (var i = 0; i < message.assertions.length; ++i)
                $root.Assertion.encode(message.assertions[i], writer.uint32(/* id 3, wireType 2 =*/26).fork()).ldelim();
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig"))
            $root.ProjectConfig.encode(message.projectConfig, writer.uint32(/* id 4, wireType 2 =*/34).fork()).ldelim();
        return writer;
    };

    /**
     * Encodes the specified CompiledGraph message, length delimited. Does not implicitly {@link CompiledGraph.verify|verify} messages.
     * @function encodeDelimited
     * @memberof CompiledGraph
     * @static
     * @param {ICompiledGraph} message CompiledGraph message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    CompiledGraph.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a CompiledGraph message from the specified reader or buffer.
     * @function decode
     * @memberof CompiledGraph
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {CompiledGraph} CompiledGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    CompiledGraph.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.CompiledGraph();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 4:
                message.projectConfig = $root.ProjectConfig.decode(reader, reader.uint32());
                break;
            case 1:
                if (!(message.materializations && message.materializations.length))
                    message.materializations = [];
                message.materializations.push($root.Materialization.decode(reader, reader.uint32()));
                break;
            case 2:
                if (!(message.operations && message.operations.length))
                    message.operations = [];
                message.operations.push($root.Operation.decode(reader, reader.uint32()));
                break;
            case 3:
                if (!(message.assertions && message.assertions.length))
                    message.assertions = [];
                message.assertions.push($root.Assertion.decode(reader, reader.uint32()));
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a CompiledGraph message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof CompiledGraph
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {CompiledGraph} CompiledGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    CompiledGraph.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a CompiledGraph message.
     * @function verify
     * @memberof CompiledGraph
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    CompiledGraph.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig")) {
            var error = $root.ProjectConfig.verify(message.projectConfig);
            if (error)
                return "projectConfig." + error;
        }
        if (message.materializations != null && message.hasOwnProperty("materializations")) {
            if (!Array.isArray(message.materializations))
                return "materializations: array expected";
            for (var i = 0; i < message.materializations.length; ++i) {
                var error = $root.Materialization.verify(message.materializations[i]);
                if (error)
                    return "materializations." + error;
            }
        }
        if (message.operations != null && message.hasOwnProperty("operations")) {
            if (!Array.isArray(message.operations))
                return "operations: array expected";
            for (var i = 0; i < message.operations.length; ++i) {
                var error = $root.Operation.verify(message.operations[i]);
                if (error)
                    return "operations." + error;
            }
        }
        if (message.assertions != null && message.hasOwnProperty("assertions")) {
            if (!Array.isArray(message.assertions))
                return "assertions: array expected";
            for (var i = 0; i < message.assertions.length; ++i) {
                var error = $root.Assertion.verify(message.assertions[i]);
                if (error)
                    return "assertions." + error;
            }
        }
        return null;
    };

    /**
     * Creates a CompiledGraph message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof CompiledGraph
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {CompiledGraph} CompiledGraph
     */
    CompiledGraph.fromObject = function fromObject(object) {
        if (object instanceof $root.CompiledGraph)
            return object;
        var message = new $root.CompiledGraph();
        if (object.projectConfig != null) {
            if (typeof object.projectConfig !== "object")
                throw TypeError(".CompiledGraph.projectConfig: object expected");
            message.projectConfig = $root.ProjectConfig.fromObject(object.projectConfig);
        }
        if (object.materializations) {
            if (!Array.isArray(object.materializations))
                throw TypeError(".CompiledGraph.materializations: array expected");
            message.materializations = [];
            for (var i = 0; i < object.materializations.length; ++i) {
                if (typeof object.materializations[i] !== "object")
                    throw TypeError(".CompiledGraph.materializations: object expected");
                message.materializations[i] = $root.Materialization.fromObject(object.materializations[i]);
            }
        }
        if (object.operations) {
            if (!Array.isArray(object.operations))
                throw TypeError(".CompiledGraph.operations: array expected");
            message.operations = [];
            for (var i = 0; i < object.operations.length; ++i) {
                if (typeof object.operations[i] !== "object")
                    throw TypeError(".CompiledGraph.operations: object expected");
                message.operations[i] = $root.Operation.fromObject(object.operations[i]);
            }
        }
        if (object.assertions) {
            if (!Array.isArray(object.assertions))
                throw TypeError(".CompiledGraph.assertions: array expected");
            message.assertions = [];
            for (var i = 0; i < object.assertions.length; ++i) {
                if (typeof object.assertions[i] !== "object")
                    throw TypeError(".CompiledGraph.assertions: object expected");
                message.assertions[i] = $root.Assertion.fromObject(object.assertions[i]);
            }
        }
        return message;
    };

    /**
     * Creates a plain object from a CompiledGraph message. Also converts values to other types if specified.
     * @function toObject
     * @memberof CompiledGraph
     * @static
     * @param {CompiledGraph} message CompiledGraph
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    CompiledGraph.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults) {
            object.materializations = [];
            object.operations = [];
            object.assertions = [];
        }
        if (options.defaults)
            object.projectConfig = null;
        if (message.materializations && message.materializations.length) {
            object.materializations = [];
            for (var j = 0; j < message.materializations.length; ++j)
                object.materializations[j] = $root.Materialization.toObject(message.materializations[j], options);
        }
        if (message.operations && message.operations.length) {
            object.operations = [];
            for (var j = 0; j < message.operations.length; ++j)
                object.operations[j] = $root.Operation.toObject(message.operations[j], options);
        }
        if (message.assertions && message.assertions.length) {
            object.assertions = [];
            for (var j = 0; j < message.assertions.length; ++j)
                object.assertions[j] = $root.Assertion.toObject(message.assertions[j], options);
        }
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig"))
            object.projectConfig = $root.ProjectConfig.toObject(message.projectConfig, options);
        return object;
    };

    /**
     * Converts this CompiledGraph to JSON.
     * @function toJSON
     * @memberof CompiledGraph
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    CompiledGraph.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return CompiledGraph;
})();

$root.ExecutionTask = (function() {

    /**
     * Properties of an ExecutionTask.
     * @exports IExecutionTask
     * @interface IExecutionTask
     * @property {string|null} [type] ExecutionTask type
     * @property {string|null} [statement] ExecutionTask statement
     * @property {boolean|null} [ignoreErrors] ExecutionTask ignoreErrors
     */

    /**
     * Constructs a new ExecutionTask.
     * @exports ExecutionTask
     * @classdesc Represents an ExecutionTask.
     * @implements IExecutionTask
     * @constructor
     * @param {IExecutionTask=} [properties] Properties to set
     */
    function ExecutionTask(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * ExecutionTask type.
     * @member {string} type
     * @memberof ExecutionTask
     * @instance
     */
    ExecutionTask.prototype.type = "";

    /**
     * ExecutionTask statement.
     * @member {string} statement
     * @memberof ExecutionTask
     * @instance
     */
    ExecutionTask.prototype.statement = "";

    /**
     * ExecutionTask ignoreErrors.
     * @member {boolean} ignoreErrors
     * @memberof ExecutionTask
     * @instance
     */
    ExecutionTask.prototype.ignoreErrors = false;

    /**
     * Creates a new ExecutionTask instance using the specified properties.
     * @function create
     * @memberof ExecutionTask
     * @static
     * @param {IExecutionTask=} [properties] Properties to set
     * @returns {ExecutionTask} ExecutionTask instance
     */
    ExecutionTask.create = function create(properties) {
        return new ExecutionTask(properties);
    };

    /**
     * Encodes the specified ExecutionTask message. Does not implicitly {@link ExecutionTask.verify|verify} messages.
     * @function encode
     * @memberof ExecutionTask
     * @static
     * @param {IExecutionTask} message ExecutionTask message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutionTask.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.type != null && message.hasOwnProperty("type"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.type);
        if (message.statement != null && message.hasOwnProperty("statement"))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.statement);
        if (message.ignoreErrors != null && message.hasOwnProperty("ignoreErrors"))
            writer.uint32(/* id 3, wireType 0 =*/24).bool(message.ignoreErrors);
        return writer;
    };

    /**
     * Encodes the specified ExecutionTask message, length delimited. Does not implicitly {@link ExecutionTask.verify|verify} messages.
     * @function encodeDelimited
     * @memberof ExecutionTask
     * @static
     * @param {IExecutionTask} message ExecutionTask message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutionTask.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an ExecutionTask message from the specified reader or buffer.
     * @function decode
     * @memberof ExecutionTask
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {ExecutionTask} ExecutionTask
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutionTask.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.ExecutionTask();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.type = reader.string();
                break;
            case 2:
                message.statement = reader.string();
                break;
            case 3:
                message.ignoreErrors = reader.bool();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an ExecutionTask message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof ExecutionTask
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {ExecutionTask} ExecutionTask
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutionTask.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an ExecutionTask message.
     * @function verify
     * @memberof ExecutionTask
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ExecutionTask.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.type != null && message.hasOwnProperty("type"))
            if (!$util.isString(message.type))
                return "type: string expected";
        if (message.statement != null && message.hasOwnProperty("statement"))
            if (!$util.isString(message.statement))
                return "statement: string expected";
        if (message.ignoreErrors != null && message.hasOwnProperty("ignoreErrors"))
            if (typeof message.ignoreErrors !== "boolean")
                return "ignoreErrors: boolean expected";
        return null;
    };

    /**
     * Creates an ExecutionTask message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof ExecutionTask
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {ExecutionTask} ExecutionTask
     */
    ExecutionTask.fromObject = function fromObject(object) {
        if (object instanceof $root.ExecutionTask)
            return object;
        var message = new $root.ExecutionTask();
        if (object.type != null)
            message.type = String(object.type);
        if (object.statement != null)
            message.statement = String(object.statement);
        if (object.ignoreErrors != null)
            message.ignoreErrors = Boolean(object.ignoreErrors);
        return message;
    };

    /**
     * Creates a plain object from an ExecutionTask message. Also converts values to other types if specified.
     * @function toObject
     * @memberof ExecutionTask
     * @static
     * @param {ExecutionTask} message ExecutionTask
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ExecutionTask.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            object.type = "";
            object.statement = "";
            object.ignoreErrors = false;
        }
        if (message.type != null && message.hasOwnProperty("type"))
            object.type = message.type;
        if (message.statement != null && message.hasOwnProperty("statement"))
            object.statement = message.statement;
        if (message.ignoreErrors != null && message.hasOwnProperty("ignoreErrors"))
            object.ignoreErrors = message.ignoreErrors;
        return object;
    };

    /**
     * Converts this ExecutionTask to JSON.
     * @function toJSON
     * @memberof ExecutionTask
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ExecutionTask.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return ExecutionTask;
})();

$root.ExecutionNode = (function() {

    /**
     * Properties of an ExecutionNode.
     * @exports IExecutionNode
     * @interface IExecutionNode
     * @property {string|null} [name] ExecutionNode name
     * @property {Array.<string>|null} [dependencies] ExecutionNode dependencies
     * @property {Array.<IExecutionTask>|null} [tasks] ExecutionNode tasks
     */

    /**
     * Constructs a new ExecutionNode.
     * @exports ExecutionNode
     * @classdesc Represents an ExecutionNode.
     * @implements IExecutionNode
     * @constructor
     * @param {IExecutionNode=} [properties] Properties to set
     */
    function ExecutionNode(properties) {
        this.dependencies = [];
        this.tasks = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * ExecutionNode name.
     * @member {string} name
     * @memberof ExecutionNode
     * @instance
     */
    ExecutionNode.prototype.name = "";

    /**
     * ExecutionNode dependencies.
     * @member {Array.<string>} dependencies
     * @memberof ExecutionNode
     * @instance
     */
    ExecutionNode.prototype.dependencies = $util.emptyArray;

    /**
     * ExecutionNode tasks.
     * @member {Array.<IExecutionTask>} tasks
     * @memberof ExecutionNode
     * @instance
     */
    ExecutionNode.prototype.tasks = $util.emptyArray;

    /**
     * Creates a new ExecutionNode instance using the specified properties.
     * @function create
     * @memberof ExecutionNode
     * @static
     * @param {IExecutionNode=} [properties] Properties to set
     * @returns {ExecutionNode} ExecutionNode instance
     */
    ExecutionNode.create = function create(properties) {
        return new ExecutionNode(properties);
    };

    /**
     * Encodes the specified ExecutionNode message. Does not implicitly {@link ExecutionNode.verify|verify} messages.
     * @function encode
     * @memberof ExecutionNode
     * @static
     * @param {IExecutionNode} message ExecutionNode message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutionNode.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.name != null && message.hasOwnProperty("name"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.name);
        if (message.tasks != null && message.tasks.length)
            for (var i = 0; i < message.tasks.length; ++i)
                $root.ExecutionTask.encode(message.tasks[i], writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
        if (message.dependencies != null && message.dependencies.length)
            for (var i = 0; i < message.dependencies.length; ++i)
                writer.uint32(/* id 3, wireType 2 =*/26).string(message.dependencies[i]);
        return writer;
    };

    /**
     * Encodes the specified ExecutionNode message, length delimited. Does not implicitly {@link ExecutionNode.verify|verify} messages.
     * @function encodeDelimited
     * @memberof ExecutionNode
     * @static
     * @param {IExecutionNode} message ExecutionNode message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutionNode.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an ExecutionNode message from the specified reader or buffer.
     * @function decode
     * @memberof ExecutionNode
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {ExecutionNode} ExecutionNode
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutionNode.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.ExecutionNode();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.name = reader.string();
                break;
            case 3:
                if (!(message.dependencies && message.dependencies.length))
                    message.dependencies = [];
                message.dependencies.push(reader.string());
                break;
            case 2:
                if (!(message.tasks && message.tasks.length))
                    message.tasks = [];
                message.tasks.push($root.ExecutionTask.decode(reader, reader.uint32()));
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an ExecutionNode message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof ExecutionNode
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {ExecutionNode} ExecutionNode
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutionNode.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an ExecutionNode message.
     * @function verify
     * @memberof ExecutionNode
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ExecutionNode.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.name != null && message.hasOwnProperty("name"))
            if (!$util.isString(message.name))
                return "name: string expected";
        if (message.dependencies != null && message.hasOwnProperty("dependencies")) {
            if (!Array.isArray(message.dependencies))
                return "dependencies: array expected";
            for (var i = 0; i < message.dependencies.length; ++i)
                if (!$util.isString(message.dependencies[i]))
                    return "dependencies: string[] expected";
        }
        if (message.tasks != null && message.hasOwnProperty("tasks")) {
            if (!Array.isArray(message.tasks))
                return "tasks: array expected";
            for (var i = 0; i < message.tasks.length; ++i) {
                var error = $root.ExecutionTask.verify(message.tasks[i]);
                if (error)
                    return "tasks." + error;
            }
        }
        return null;
    };

    /**
     * Creates an ExecutionNode message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof ExecutionNode
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {ExecutionNode} ExecutionNode
     */
    ExecutionNode.fromObject = function fromObject(object) {
        if (object instanceof $root.ExecutionNode)
            return object;
        var message = new $root.ExecutionNode();
        if (object.name != null)
            message.name = String(object.name);
        if (object.dependencies) {
            if (!Array.isArray(object.dependencies))
                throw TypeError(".ExecutionNode.dependencies: array expected");
            message.dependencies = [];
            for (var i = 0; i < object.dependencies.length; ++i)
                message.dependencies[i] = String(object.dependencies[i]);
        }
        if (object.tasks) {
            if (!Array.isArray(object.tasks))
                throw TypeError(".ExecutionNode.tasks: array expected");
            message.tasks = [];
            for (var i = 0; i < object.tasks.length; ++i) {
                if (typeof object.tasks[i] !== "object")
                    throw TypeError(".ExecutionNode.tasks: object expected");
                message.tasks[i] = $root.ExecutionTask.fromObject(object.tasks[i]);
            }
        }
        return message;
    };

    /**
     * Creates a plain object from an ExecutionNode message. Also converts values to other types if specified.
     * @function toObject
     * @memberof ExecutionNode
     * @static
     * @param {ExecutionNode} message ExecutionNode
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ExecutionNode.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults) {
            object.tasks = [];
            object.dependencies = [];
        }
        if (options.defaults)
            object.name = "";
        if (message.name != null && message.hasOwnProperty("name"))
            object.name = message.name;
        if (message.tasks && message.tasks.length) {
            object.tasks = [];
            for (var j = 0; j < message.tasks.length; ++j)
                object.tasks[j] = $root.ExecutionTask.toObject(message.tasks[j], options);
        }
        if (message.dependencies && message.dependencies.length) {
            object.dependencies = [];
            for (var j = 0; j < message.dependencies.length; ++j)
                object.dependencies[j] = message.dependencies[j];
        }
        return object;
    };

    /**
     * Converts this ExecutionNode to JSON.
     * @function toJSON
     * @memberof ExecutionNode
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ExecutionNode.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return ExecutionNode;
})();

$root.ExecutionGraph = (function() {

    /**
     * Properties of an ExecutionGraph.
     * @exports IExecutionGraph
     * @interface IExecutionGraph
     * @property {IProjectConfig|null} [projectConfig] ExecutionGraph projectConfig
     * @property {IRunConfig|null} [runConfig] ExecutionGraph runConfig
     * @property {Array.<IExecutionNode>|null} [nodes] ExecutionGraph nodes
     */

    /**
     * Constructs a new ExecutionGraph.
     * @exports ExecutionGraph
     * @classdesc Represents an ExecutionGraph.
     * @implements IExecutionGraph
     * @constructor
     * @param {IExecutionGraph=} [properties] Properties to set
     */
    function ExecutionGraph(properties) {
        this.nodes = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * ExecutionGraph projectConfig.
     * @member {IProjectConfig|null|undefined} projectConfig
     * @memberof ExecutionGraph
     * @instance
     */
    ExecutionGraph.prototype.projectConfig = null;

    /**
     * ExecutionGraph runConfig.
     * @member {IRunConfig|null|undefined} runConfig
     * @memberof ExecutionGraph
     * @instance
     */
    ExecutionGraph.prototype.runConfig = null;

    /**
     * ExecutionGraph nodes.
     * @member {Array.<IExecutionNode>} nodes
     * @memberof ExecutionGraph
     * @instance
     */
    ExecutionGraph.prototype.nodes = $util.emptyArray;

    /**
     * Creates a new ExecutionGraph instance using the specified properties.
     * @function create
     * @memberof ExecutionGraph
     * @static
     * @param {IExecutionGraph=} [properties] Properties to set
     * @returns {ExecutionGraph} ExecutionGraph instance
     */
    ExecutionGraph.create = function create(properties) {
        return new ExecutionGraph(properties);
    };

    /**
     * Encodes the specified ExecutionGraph message. Does not implicitly {@link ExecutionGraph.verify|verify} messages.
     * @function encode
     * @memberof ExecutionGraph
     * @static
     * @param {IExecutionGraph} message ExecutionGraph message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutionGraph.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig"))
            $root.ProjectConfig.encode(message.projectConfig, writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
        if (message.runConfig != null && message.hasOwnProperty("runConfig"))
            $root.RunConfig.encode(message.runConfig, writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
        if (message.nodes != null && message.nodes.length)
            for (var i = 0; i < message.nodes.length; ++i)
                $root.ExecutionNode.encode(message.nodes[i], writer.uint32(/* id 3, wireType 2 =*/26).fork()).ldelim();
        return writer;
    };

    /**
     * Encodes the specified ExecutionGraph message, length delimited. Does not implicitly {@link ExecutionGraph.verify|verify} messages.
     * @function encodeDelimited
     * @memberof ExecutionGraph
     * @static
     * @param {IExecutionGraph} message ExecutionGraph message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutionGraph.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an ExecutionGraph message from the specified reader or buffer.
     * @function decode
     * @memberof ExecutionGraph
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {ExecutionGraph} ExecutionGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutionGraph.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.ExecutionGraph();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.projectConfig = $root.ProjectConfig.decode(reader, reader.uint32());
                break;
            case 2:
                message.runConfig = $root.RunConfig.decode(reader, reader.uint32());
                break;
            case 3:
                if (!(message.nodes && message.nodes.length))
                    message.nodes = [];
                message.nodes.push($root.ExecutionNode.decode(reader, reader.uint32()));
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an ExecutionGraph message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof ExecutionGraph
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {ExecutionGraph} ExecutionGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutionGraph.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an ExecutionGraph message.
     * @function verify
     * @memberof ExecutionGraph
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ExecutionGraph.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig")) {
            var error = $root.ProjectConfig.verify(message.projectConfig);
            if (error)
                return "projectConfig." + error;
        }
        if (message.runConfig != null && message.hasOwnProperty("runConfig")) {
            var error = $root.RunConfig.verify(message.runConfig);
            if (error)
                return "runConfig." + error;
        }
        if (message.nodes != null && message.hasOwnProperty("nodes")) {
            if (!Array.isArray(message.nodes))
                return "nodes: array expected";
            for (var i = 0; i < message.nodes.length; ++i) {
                var error = $root.ExecutionNode.verify(message.nodes[i]);
                if (error)
                    return "nodes." + error;
            }
        }
        return null;
    };

    /**
     * Creates an ExecutionGraph message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof ExecutionGraph
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {ExecutionGraph} ExecutionGraph
     */
    ExecutionGraph.fromObject = function fromObject(object) {
        if (object instanceof $root.ExecutionGraph)
            return object;
        var message = new $root.ExecutionGraph();
        if (object.projectConfig != null) {
            if (typeof object.projectConfig !== "object")
                throw TypeError(".ExecutionGraph.projectConfig: object expected");
            message.projectConfig = $root.ProjectConfig.fromObject(object.projectConfig);
        }
        if (object.runConfig != null) {
            if (typeof object.runConfig !== "object")
                throw TypeError(".ExecutionGraph.runConfig: object expected");
            message.runConfig = $root.RunConfig.fromObject(object.runConfig);
        }
        if (object.nodes) {
            if (!Array.isArray(object.nodes))
                throw TypeError(".ExecutionGraph.nodes: array expected");
            message.nodes = [];
            for (var i = 0; i < object.nodes.length; ++i) {
                if (typeof object.nodes[i] !== "object")
                    throw TypeError(".ExecutionGraph.nodes: object expected");
                message.nodes[i] = $root.ExecutionNode.fromObject(object.nodes[i]);
            }
        }
        return message;
    };

    /**
     * Creates a plain object from an ExecutionGraph message. Also converts values to other types if specified.
     * @function toObject
     * @memberof ExecutionGraph
     * @static
     * @param {ExecutionGraph} message ExecutionGraph
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ExecutionGraph.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults)
            object.nodes = [];
        if (options.defaults) {
            object.projectConfig = null;
            object.runConfig = null;
        }
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig"))
            object.projectConfig = $root.ProjectConfig.toObject(message.projectConfig, options);
        if (message.runConfig != null && message.hasOwnProperty("runConfig"))
            object.runConfig = $root.RunConfig.toObject(message.runConfig, options);
        if (message.nodes && message.nodes.length) {
            object.nodes = [];
            for (var j = 0; j < message.nodes.length; ++j)
                object.nodes[j] = $root.ExecutionNode.toObject(message.nodes[j], options);
        }
        return object;
    };

    /**
     * Converts this ExecutionGraph to JSON.
     * @function toJSON
     * @memberof ExecutionGraph
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ExecutionGraph.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return ExecutionGraph;
})();

$root.ExecutedTask = (function() {

    /**
     * Properties of an ExecutedTask.
     * @exports IExecutedTask
     * @interface IExecutedTask
     * @property {IExecutionTask|null} [task] ExecutedTask task
     * @property {boolean|null} [ok] ExecutedTask ok
     * @property {string|null} [error] ExecutedTask error
     * @property {string|null} [testResults] ExecutedTask testResults
     */

    /**
     * Constructs a new ExecutedTask.
     * @exports ExecutedTask
     * @classdesc Represents an ExecutedTask.
     * @implements IExecutedTask
     * @constructor
     * @param {IExecutedTask=} [properties] Properties to set
     */
    function ExecutedTask(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * ExecutedTask task.
     * @member {IExecutionTask|null|undefined} task
     * @memberof ExecutedTask
     * @instance
     */
    ExecutedTask.prototype.task = null;

    /**
     * ExecutedTask ok.
     * @member {boolean} ok
     * @memberof ExecutedTask
     * @instance
     */
    ExecutedTask.prototype.ok = false;

    /**
     * ExecutedTask error.
     * @member {string} error
     * @memberof ExecutedTask
     * @instance
     */
    ExecutedTask.prototype.error = "";

    /**
     * ExecutedTask testResults.
     * @member {string} testResults
     * @memberof ExecutedTask
     * @instance
     */
    ExecutedTask.prototype.testResults = "";

    /**
     * Creates a new ExecutedTask instance using the specified properties.
     * @function create
     * @memberof ExecutedTask
     * @static
     * @param {IExecutedTask=} [properties] Properties to set
     * @returns {ExecutedTask} ExecutedTask instance
     */
    ExecutedTask.create = function create(properties) {
        return new ExecutedTask(properties);
    };

    /**
     * Encodes the specified ExecutedTask message. Does not implicitly {@link ExecutedTask.verify|verify} messages.
     * @function encode
     * @memberof ExecutedTask
     * @static
     * @param {IExecutedTask} message ExecutedTask message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutedTask.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.task != null && message.hasOwnProperty("task"))
            $root.ExecutionTask.encode(message.task, writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
        if (message.ok != null && message.hasOwnProperty("ok"))
            writer.uint32(/* id 2, wireType 0 =*/16).bool(message.ok);
        if (message.error != null && message.hasOwnProperty("error"))
            writer.uint32(/* id 3, wireType 2 =*/26).string(message.error);
        if (message.testResults != null && message.hasOwnProperty("testResults"))
            writer.uint32(/* id 4, wireType 2 =*/34).string(message.testResults);
        return writer;
    };

    /**
     * Encodes the specified ExecutedTask message, length delimited. Does not implicitly {@link ExecutedTask.verify|verify} messages.
     * @function encodeDelimited
     * @memberof ExecutedTask
     * @static
     * @param {IExecutedTask} message ExecutedTask message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutedTask.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an ExecutedTask message from the specified reader or buffer.
     * @function decode
     * @memberof ExecutedTask
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {ExecutedTask} ExecutedTask
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutedTask.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.ExecutedTask();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.task = $root.ExecutionTask.decode(reader, reader.uint32());
                break;
            case 2:
                message.ok = reader.bool();
                break;
            case 3:
                message.error = reader.string();
                break;
            case 4:
                message.testResults = reader.string();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an ExecutedTask message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof ExecutedTask
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {ExecutedTask} ExecutedTask
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutedTask.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an ExecutedTask message.
     * @function verify
     * @memberof ExecutedTask
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ExecutedTask.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.task != null && message.hasOwnProperty("task")) {
            var error = $root.ExecutionTask.verify(message.task);
            if (error)
                return "task." + error;
        }
        if (message.ok != null && message.hasOwnProperty("ok"))
            if (typeof message.ok !== "boolean")
                return "ok: boolean expected";
        if (message.error != null && message.hasOwnProperty("error"))
            if (!$util.isString(message.error))
                return "error: string expected";
        if (message.testResults != null && message.hasOwnProperty("testResults"))
            if (!$util.isString(message.testResults))
                return "testResults: string expected";
        return null;
    };

    /**
     * Creates an ExecutedTask message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof ExecutedTask
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {ExecutedTask} ExecutedTask
     */
    ExecutedTask.fromObject = function fromObject(object) {
        if (object instanceof $root.ExecutedTask)
            return object;
        var message = new $root.ExecutedTask();
        if (object.task != null) {
            if (typeof object.task !== "object")
                throw TypeError(".ExecutedTask.task: object expected");
            message.task = $root.ExecutionTask.fromObject(object.task);
        }
        if (object.ok != null)
            message.ok = Boolean(object.ok);
        if (object.error != null)
            message.error = String(object.error);
        if (object.testResults != null)
            message.testResults = String(object.testResults);
        return message;
    };

    /**
     * Creates a plain object from an ExecutedTask message. Also converts values to other types if specified.
     * @function toObject
     * @memberof ExecutedTask
     * @static
     * @param {ExecutedTask} message ExecutedTask
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ExecutedTask.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            object.task = null;
            object.ok = false;
            object.error = "";
            object.testResults = "";
        }
        if (message.task != null && message.hasOwnProperty("task"))
            object.task = $root.ExecutionTask.toObject(message.task, options);
        if (message.ok != null && message.hasOwnProperty("ok"))
            object.ok = message.ok;
        if (message.error != null && message.hasOwnProperty("error"))
            object.error = message.error;
        if (message.testResults != null && message.hasOwnProperty("testResults"))
            object.testResults = message.testResults;
        return object;
    };

    /**
     * Converts this ExecutedTask to JSON.
     * @function toJSON
     * @memberof ExecutedTask
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ExecutedTask.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return ExecutedTask;
})();

$root.ExecutedNode = (function() {

    /**
     * Properties of an ExecutedNode.
     * @exports IExecutedNode
     * @interface IExecutedNode
     * @property {string|null} [name] ExecutedNode name
     * @property {Array.<IExecutedTask>|null} [tasks] ExecutedNode tasks
     * @property {boolean|null} [ok] ExecutedNode ok
     * @property {boolean|null} [skipped] ExecutedNode skipped
     */

    /**
     * Constructs a new ExecutedNode.
     * @exports ExecutedNode
     * @classdesc Represents an ExecutedNode.
     * @implements IExecutedNode
     * @constructor
     * @param {IExecutedNode=} [properties] Properties to set
     */
    function ExecutedNode(properties) {
        this.tasks = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * ExecutedNode name.
     * @member {string} name
     * @memberof ExecutedNode
     * @instance
     */
    ExecutedNode.prototype.name = "";

    /**
     * ExecutedNode tasks.
     * @member {Array.<IExecutedTask>} tasks
     * @memberof ExecutedNode
     * @instance
     */
    ExecutedNode.prototype.tasks = $util.emptyArray;

    /**
     * ExecutedNode ok.
     * @member {boolean} ok
     * @memberof ExecutedNode
     * @instance
     */
    ExecutedNode.prototype.ok = false;

    /**
     * ExecutedNode skipped.
     * @member {boolean} skipped
     * @memberof ExecutedNode
     * @instance
     */
    ExecutedNode.prototype.skipped = false;

    /**
     * Creates a new ExecutedNode instance using the specified properties.
     * @function create
     * @memberof ExecutedNode
     * @static
     * @param {IExecutedNode=} [properties] Properties to set
     * @returns {ExecutedNode} ExecutedNode instance
     */
    ExecutedNode.create = function create(properties) {
        return new ExecutedNode(properties);
    };

    /**
     * Encodes the specified ExecutedNode message. Does not implicitly {@link ExecutedNode.verify|verify} messages.
     * @function encode
     * @memberof ExecutedNode
     * @static
     * @param {IExecutedNode} message ExecutedNode message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutedNode.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.name != null && message.hasOwnProperty("name"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.name);
        if (message.tasks != null && message.tasks.length)
            for (var i = 0; i < message.tasks.length; ++i)
                $root.ExecutedTask.encode(message.tasks[i], writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
        if (message.ok != null && message.hasOwnProperty("ok"))
            writer.uint32(/* id 3, wireType 0 =*/24).bool(message.ok);
        if (message.skipped != null && message.hasOwnProperty("skipped"))
            writer.uint32(/* id 4, wireType 0 =*/32).bool(message.skipped);
        return writer;
    };

    /**
     * Encodes the specified ExecutedNode message, length delimited. Does not implicitly {@link ExecutedNode.verify|verify} messages.
     * @function encodeDelimited
     * @memberof ExecutedNode
     * @static
     * @param {IExecutedNode} message ExecutedNode message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutedNode.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an ExecutedNode message from the specified reader or buffer.
     * @function decode
     * @memberof ExecutedNode
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {ExecutedNode} ExecutedNode
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutedNode.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.ExecutedNode();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.name = reader.string();
                break;
            case 2:
                if (!(message.tasks && message.tasks.length))
                    message.tasks = [];
                message.tasks.push($root.ExecutedTask.decode(reader, reader.uint32()));
                break;
            case 3:
                message.ok = reader.bool();
                break;
            case 4:
                message.skipped = reader.bool();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an ExecutedNode message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof ExecutedNode
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {ExecutedNode} ExecutedNode
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutedNode.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an ExecutedNode message.
     * @function verify
     * @memberof ExecutedNode
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ExecutedNode.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.name != null && message.hasOwnProperty("name"))
            if (!$util.isString(message.name))
                return "name: string expected";
        if (message.tasks != null && message.hasOwnProperty("tasks")) {
            if (!Array.isArray(message.tasks))
                return "tasks: array expected";
            for (var i = 0; i < message.tasks.length; ++i) {
                var error = $root.ExecutedTask.verify(message.tasks[i]);
                if (error)
                    return "tasks." + error;
            }
        }
        if (message.ok != null && message.hasOwnProperty("ok"))
            if (typeof message.ok !== "boolean")
                return "ok: boolean expected";
        if (message.skipped != null && message.hasOwnProperty("skipped"))
            if (typeof message.skipped !== "boolean")
                return "skipped: boolean expected";
        return null;
    };

    /**
     * Creates an ExecutedNode message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof ExecutedNode
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {ExecutedNode} ExecutedNode
     */
    ExecutedNode.fromObject = function fromObject(object) {
        if (object instanceof $root.ExecutedNode)
            return object;
        var message = new $root.ExecutedNode();
        if (object.name != null)
            message.name = String(object.name);
        if (object.tasks) {
            if (!Array.isArray(object.tasks))
                throw TypeError(".ExecutedNode.tasks: array expected");
            message.tasks = [];
            for (var i = 0; i < object.tasks.length; ++i) {
                if (typeof object.tasks[i] !== "object")
                    throw TypeError(".ExecutedNode.tasks: object expected");
                message.tasks[i] = $root.ExecutedTask.fromObject(object.tasks[i]);
            }
        }
        if (object.ok != null)
            message.ok = Boolean(object.ok);
        if (object.skipped != null)
            message.skipped = Boolean(object.skipped);
        return message;
    };

    /**
     * Creates a plain object from an ExecutedNode message. Also converts values to other types if specified.
     * @function toObject
     * @memberof ExecutedNode
     * @static
     * @param {ExecutedNode} message ExecutedNode
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ExecutedNode.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults)
            object.tasks = [];
        if (options.defaults) {
            object.name = "";
            object.ok = false;
            object.skipped = false;
        }
        if (message.name != null && message.hasOwnProperty("name"))
            object.name = message.name;
        if (message.tasks && message.tasks.length) {
            object.tasks = [];
            for (var j = 0; j < message.tasks.length; ++j)
                object.tasks[j] = $root.ExecutedTask.toObject(message.tasks[j], options);
        }
        if (message.ok != null && message.hasOwnProperty("ok"))
            object.ok = message.ok;
        if (message.skipped != null && message.hasOwnProperty("skipped"))
            object.skipped = message.skipped;
        return object;
    };

    /**
     * Converts this ExecutedNode to JSON.
     * @function toJSON
     * @memberof ExecutedNode
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ExecutedNode.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return ExecutedNode;
})();

$root.ExecutedGraph = (function() {

    /**
     * Properties of an ExecutedGraph.
     * @exports IExecutedGraph
     * @interface IExecutedGraph
     * @property {IProjectConfig|null} [projectConfig] ExecutedGraph projectConfig
     * @property {IRunConfig|null} [runConfig] ExecutedGraph runConfig
     * @property {Array.<IExecutedNode>|null} [nodes] ExecutedGraph nodes
     */

    /**
     * Constructs a new ExecutedGraph.
     * @exports ExecutedGraph
     * @classdesc Represents an ExecutedGraph.
     * @implements IExecutedGraph
     * @constructor
     * @param {IExecutedGraph=} [properties] Properties to set
     */
    function ExecutedGraph(properties) {
        this.nodes = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * ExecutedGraph projectConfig.
     * @member {IProjectConfig|null|undefined} projectConfig
     * @memberof ExecutedGraph
     * @instance
     */
    ExecutedGraph.prototype.projectConfig = null;

    /**
     * ExecutedGraph runConfig.
     * @member {IRunConfig|null|undefined} runConfig
     * @memberof ExecutedGraph
     * @instance
     */
    ExecutedGraph.prototype.runConfig = null;

    /**
     * ExecutedGraph nodes.
     * @member {Array.<IExecutedNode>} nodes
     * @memberof ExecutedGraph
     * @instance
     */
    ExecutedGraph.prototype.nodes = $util.emptyArray;

    /**
     * Creates a new ExecutedGraph instance using the specified properties.
     * @function create
     * @memberof ExecutedGraph
     * @static
     * @param {IExecutedGraph=} [properties] Properties to set
     * @returns {ExecutedGraph} ExecutedGraph instance
     */
    ExecutedGraph.create = function create(properties) {
        return new ExecutedGraph(properties);
    };

    /**
     * Encodes the specified ExecutedGraph message. Does not implicitly {@link ExecutedGraph.verify|verify} messages.
     * @function encode
     * @memberof ExecutedGraph
     * @static
     * @param {IExecutedGraph} message ExecutedGraph message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutedGraph.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig"))
            $root.ProjectConfig.encode(message.projectConfig, writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
        if (message.runConfig != null && message.hasOwnProperty("runConfig"))
            $root.RunConfig.encode(message.runConfig, writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
        if (message.nodes != null && message.nodes.length)
            for (var i = 0; i < message.nodes.length; ++i)
                $root.ExecutedNode.encode(message.nodes[i], writer.uint32(/* id 3, wireType 2 =*/26).fork()).ldelim();
        return writer;
    };

    /**
     * Encodes the specified ExecutedGraph message, length delimited. Does not implicitly {@link ExecutedGraph.verify|verify} messages.
     * @function encodeDelimited
     * @memberof ExecutedGraph
     * @static
     * @param {IExecutedGraph} message ExecutedGraph message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    ExecutedGraph.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes an ExecutedGraph message from the specified reader or buffer.
     * @function decode
     * @memberof ExecutedGraph
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {ExecutedGraph} ExecutedGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutedGraph.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.ExecutedGraph();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.projectConfig = $root.ProjectConfig.decode(reader, reader.uint32());
                break;
            case 2:
                message.runConfig = $root.RunConfig.decode(reader, reader.uint32());
                break;
            case 3:
                if (!(message.nodes && message.nodes.length))
                    message.nodes = [];
                message.nodes.push($root.ExecutedNode.decode(reader, reader.uint32()));
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes an ExecutedGraph message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof ExecutedGraph
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {ExecutedGraph} ExecutedGraph
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    ExecutedGraph.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies an ExecutedGraph message.
     * @function verify
     * @memberof ExecutedGraph
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    ExecutedGraph.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig")) {
            var error = $root.ProjectConfig.verify(message.projectConfig);
            if (error)
                return "projectConfig." + error;
        }
        if (message.runConfig != null && message.hasOwnProperty("runConfig")) {
            var error = $root.RunConfig.verify(message.runConfig);
            if (error)
                return "runConfig." + error;
        }
        if (message.nodes != null && message.hasOwnProperty("nodes")) {
            if (!Array.isArray(message.nodes))
                return "nodes: array expected";
            for (var i = 0; i < message.nodes.length; ++i) {
                var error = $root.ExecutedNode.verify(message.nodes[i]);
                if (error)
                    return "nodes." + error;
            }
        }
        return null;
    };

    /**
     * Creates an ExecutedGraph message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof ExecutedGraph
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {ExecutedGraph} ExecutedGraph
     */
    ExecutedGraph.fromObject = function fromObject(object) {
        if (object instanceof $root.ExecutedGraph)
            return object;
        var message = new $root.ExecutedGraph();
        if (object.projectConfig != null) {
            if (typeof object.projectConfig !== "object")
                throw TypeError(".ExecutedGraph.projectConfig: object expected");
            message.projectConfig = $root.ProjectConfig.fromObject(object.projectConfig);
        }
        if (object.runConfig != null) {
            if (typeof object.runConfig !== "object")
                throw TypeError(".ExecutedGraph.runConfig: object expected");
            message.runConfig = $root.RunConfig.fromObject(object.runConfig);
        }
        if (object.nodes) {
            if (!Array.isArray(object.nodes))
                throw TypeError(".ExecutedGraph.nodes: array expected");
            message.nodes = [];
            for (var i = 0; i < object.nodes.length; ++i) {
                if (typeof object.nodes[i] !== "object")
                    throw TypeError(".ExecutedGraph.nodes: object expected");
                message.nodes[i] = $root.ExecutedNode.fromObject(object.nodes[i]);
            }
        }
        return message;
    };

    /**
     * Creates a plain object from an ExecutedGraph message. Also converts values to other types if specified.
     * @function toObject
     * @memberof ExecutedGraph
     * @static
     * @param {ExecutedGraph} message ExecutedGraph
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    ExecutedGraph.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults)
            object.nodes = [];
        if (options.defaults) {
            object.projectConfig = null;
            object.runConfig = null;
        }
        if (message.projectConfig != null && message.hasOwnProperty("projectConfig"))
            object.projectConfig = $root.ProjectConfig.toObject(message.projectConfig, options);
        if (message.runConfig != null && message.hasOwnProperty("runConfig"))
            object.runConfig = $root.RunConfig.toObject(message.runConfig, options);
        if (message.nodes && message.nodes.length) {
            object.nodes = [];
            for (var j = 0; j < message.nodes.length; ++j)
                object.nodes[j] = $root.ExecutedNode.toObject(message.nodes[j], options);
        }
        return object;
    };

    /**
     * Converts this ExecutedGraph to JSON.
     * @function toJSON
     * @memberof ExecutedGraph
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    ExecutedGraph.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return ExecutedGraph;
})();

$root.RunConfig = (function() {

    /**
     * Properties of a RunConfig.
     * @exports IRunConfig
     * @interface IRunConfig
     * @property {Array.<string>|null} [nodes] RunConfig nodes
     * @property {boolean|null} [includeDependencies] RunConfig includeDependencies
     * @property {boolean|null} [fullRefresh] RunConfig fullRefresh
     * @property {boolean|null} [carryOn] RunConfig carryOn
     */

    /**
     * Constructs a new RunConfig.
     * @exports RunConfig
     * @classdesc Represents a RunConfig.
     * @implements IRunConfig
     * @constructor
     * @param {IRunConfig=} [properties] Properties to set
     */
    function RunConfig(properties) {
        this.nodes = [];
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * RunConfig nodes.
     * @member {Array.<string>} nodes
     * @memberof RunConfig
     * @instance
     */
    RunConfig.prototype.nodes = $util.emptyArray;

    /**
     * RunConfig includeDependencies.
     * @member {boolean} includeDependencies
     * @memberof RunConfig
     * @instance
     */
    RunConfig.prototype.includeDependencies = false;

    /**
     * RunConfig fullRefresh.
     * @member {boolean} fullRefresh
     * @memberof RunConfig
     * @instance
     */
    RunConfig.prototype.fullRefresh = false;

    /**
     * RunConfig carryOn.
     * @member {boolean} carryOn
     * @memberof RunConfig
     * @instance
     */
    RunConfig.prototype.carryOn = false;

    /**
     * Creates a new RunConfig instance using the specified properties.
     * @function create
     * @memberof RunConfig
     * @static
     * @param {IRunConfig=} [properties] Properties to set
     * @returns {RunConfig} RunConfig instance
     */
    RunConfig.create = function create(properties) {
        return new RunConfig(properties);
    };

    /**
     * Encodes the specified RunConfig message. Does not implicitly {@link RunConfig.verify|verify} messages.
     * @function encode
     * @memberof RunConfig
     * @static
     * @param {IRunConfig} message RunConfig message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    RunConfig.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.nodes != null && message.nodes.length)
            for (var i = 0; i < message.nodes.length; ++i)
                writer.uint32(/* id 1, wireType 2 =*/10).string(message.nodes[i]);
        if (message.fullRefresh != null && message.hasOwnProperty("fullRefresh"))
            writer.uint32(/* id 2, wireType 0 =*/16).bool(message.fullRefresh);
        if (message.includeDependencies != null && message.hasOwnProperty("includeDependencies"))
            writer.uint32(/* id 3, wireType 0 =*/24).bool(message.includeDependencies);
        if (message.carryOn != null && message.hasOwnProperty("carryOn"))
            writer.uint32(/* id 4, wireType 0 =*/32).bool(message.carryOn);
        return writer;
    };

    /**
     * Encodes the specified RunConfig message, length delimited. Does not implicitly {@link RunConfig.verify|verify} messages.
     * @function encodeDelimited
     * @memberof RunConfig
     * @static
     * @param {IRunConfig} message RunConfig message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    RunConfig.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a RunConfig message from the specified reader or buffer.
     * @function decode
     * @memberof RunConfig
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {RunConfig} RunConfig
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    RunConfig.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.RunConfig();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                if (!(message.nodes && message.nodes.length))
                    message.nodes = [];
                message.nodes.push(reader.string());
                break;
            case 3:
                message.includeDependencies = reader.bool();
                break;
            case 2:
                message.fullRefresh = reader.bool();
                break;
            case 4:
                message.carryOn = reader.bool();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a RunConfig message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof RunConfig
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {RunConfig} RunConfig
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    RunConfig.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a RunConfig message.
     * @function verify
     * @memberof RunConfig
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    RunConfig.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.nodes != null && message.hasOwnProperty("nodes")) {
            if (!Array.isArray(message.nodes))
                return "nodes: array expected";
            for (var i = 0; i < message.nodes.length; ++i)
                if (!$util.isString(message.nodes[i]))
                    return "nodes: string[] expected";
        }
        if (message.includeDependencies != null && message.hasOwnProperty("includeDependencies"))
            if (typeof message.includeDependencies !== "boolean")
                return "includeDependencies: boolean expected";
        if (message.fullRefresh != null && message.hasOwnProperty("fullRefresh"))
            if (typeof message.fullRefresh !== "boolean")
                return "fullRefresh: boolean expected";
        if (message.carryOn != null && message.hasOwnProperty("carryOn"))
            if (typeof message.carryOn !== "boolean")
                return "carryOn: boolean expected";
        return null;
    };

    /**
     * Creates a RunConfig message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof RunConfig
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {RunConfig} RunConfig
     */
    RunConfig.fromObject = function fromObject(object) {
        if (object instanceof $root.RunConfig)
            return object;
        var message = new $root.RunConfig();
        if (object.nodes) {
            if (!Array.isArray(object.nodes))
                throw TypeError(".RunConfig.nodes: array expected");
            message.nodes = [];
            for (var i = 0; i < object.nodes.length; ++i)
                message.nodes[i] = String(object.nodes[i]);
        }
        if (object.includeDependencies != null)
            message.includeDependencies = Boolean(object.includeDependencies);
        if (object.fullRefresh != null)
            message.fullRefresh = Boolean(object.fullRefresh);
        if (object.carryOn != null)
            message.carryOn = Boolean(object.carryOn);
        return message;
    };

    /**
     * Creates a plain object from a RunConfig message. Also converts values to other types if specified.
     * @function toObject
     * @memberof RunConfig
     * @static
     * @param {RunConfig} message RunConfig
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    RunConfig.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.arrays || options.defaults)
            object.nodes = [];
        if (options.defaults) {
            object.fullRefresh = false;
            object.includeDependencies = false;
            object.carryOn = false;
        }
        if (message.nodes && message.nodes.length) {
            object.nodes = [];
            for (var j = 0; j < message.nodes.length; ++j)
                object.nodes[j] = message.nodes[j];
        }
        if (message.fullRefresh != null && message.hasOwnProperty("fullRefresh"))
            object.fullRefresh = message.fullRefresh;
        if (message.includeDependencies != null && message.hasOwnProperty("includeDependencies"))
            object.includeDependencies = message.includeDependencies;
        if (message.carryOn != null && message.hasOwnProperty("carryOn"))
            object.carryOn = message.carryOn;
        return object;
    };

    /**
     * Converts this RunConfig to JSON.
     * @function toJSON
     * @memberof RunConfig
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    RunConfig.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return RunConfig;
})();

$root.Profile = (function() {

    /**
     * Properties of a Profile.
     * @exports IProfile
     * @interface IProfile
     * @property {number|null} [threads] Profile threads
     * @property {IJDBC|null} [redshift] Profile redshift
     * @property {IJDBC|null} [postgres] Profile postgres
     * @property {IBigQuery|null} [bigquery] Profile bigquery
     * @property {ISnowflake|null} [snowflake] Profile snowflake
     */

    /**
     * Constructs a new Profile.
     * @exports Profile
     * @classdesc Represents a Profile.
     * @implements IProfile
     * @constructor
     * @param {IProfile=} [properties] Properties to set
     */
    function Profile(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * Profile threads.
     * @member {number} threads
     * @memberof Profile
     * @instance
     */
    Profile.prototype.threads = 0;

    /**
     * Profile redshift.
     * @member {IJDBC|null|undefined} redshift
     * @memberof Profile
     * @instance
     */
    Profile.prototype.redshift = null;

    /**
     * Profile postgres.
     * @member {IJDBC|null|undefined} postgres
     * @memberof Profile
     * @instance
     */
    Profile.prototype.postgres = null;

    /**
     * Profile bigquery.
     * @member {IBigQuery|null|undefined} bigquery
     * @memberof Profile
     * @instance
     */
    Profile.prototype.bigquery = null;

    /**
     * Profile snowflake.
     * @member {ISnowflake|null|undefined} snowflake
     * @memberof Profile
     * @instance
     */
    Profile.prototype.snowflake = null;

    // OneOf field names bound to virtual getters and setters
    var $oneOfFields;

    /**
     * Profile connection.
     * @member {"redshift"|"postgres"|"bigquery"|"snowflake"|undefined} connection
     * @memberof Profile
     * @instance
     */
    Object.defineProperty(Profile.prototype, "connection", {
        get: $util.oneOfGetter($oneOfFields = ["redshift", "postgres", "bigquery", "snowflake"]),
        set: $util.oneOfSetter($oneOfFields)
    });

    /**
     * Creates a new Profile instance using the specified properties.
     * @function create
     * @memberof Profile
     * @static
     * @param {IProfile=} [properties] Properties to set
     * @returns {Profile} Profile instance
     */
    Profile.create = function create(properties) {
        return new Profile(properties);
    };

    /**
     * Encodes the specified Profile message. Does not implicitly {@link Profile.verify|verify} messages.
     * @function encode
     * @memberof Profile
     * @static
     * @param {IProfile} message Profile message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Profile.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.redshift != null && message.hasOwnProperty("redshift"))
            $root.JDBC.encode(message.redshift, writer.uint32(/* id 3, wireType 2 =*/26).fork()).ldelim();
        if (message.bigquery != null && message.hasOwnProperty("bigquery"))
            $root.BigQuery.encode(message.bigquery, writer.uint32(/* id 4, wireType 2 =*/34).fork()).ldelim();
        if (message.snowflake != null && message.hasOwnProperty("snowflake"))
            $root.Snowflake.encode(message.snowflake, writer.uint32(/* id 5, wireType 2 =*/42).fork()).ldelim();
        if (message.threads != null && message.hasOwnProperty("threads"))
            writer.uint32(/* id 7, wireType 0 =*/56).int32(message.threads);
        if (message.postgres != null && message.hasOwnProperty("postgres"))
            $root.JDBC.encode(message.postgres, writer.uint32(/* id 8, wireType 2 =*/66).fork()).ldelim();
        return writer;
    };

    /**
     * Encodes the specified Profile message, length delimited. Does not implicitly {@link Profile.verify|verify} messages.
     * @function encodeDelimited
     * @memberof Profile
     * @static
     * @param {IProfile} message Profile message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Profile.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a Profile message from the specified reader or buffer.
     * @function decode
     * @memberof Profile
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {Profile} Profile
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Profile.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.Profile();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 7:
                message.threads = reader.int32();
                break;
            case 3:
                message.redshift = $root.JDBC.decode(reader, reader.uint32());
                break;
            case 8:
                message.postgres = $root.JDBC.decode(reader, reader.uint32());
                break;
            case 4:
                message.bigquery = $root.BigQuery.decode(reader, reader.uint32());
                break;
            case 5:
                message.snowflake = $root.Snowflake.decode(reader, reader.uint32());
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a Profile message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof Profile
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {Profile} Profile
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Profile.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a Profile message.
     * @function verify
     * @memberof Profile
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    Profile.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        var properties = {};
        if (message.threads != null && message.hasOwnProperty("threads"))
            if (!$util.isInteger(message.threads))
                return "threads: integer expected";
        if (message.redshift != null && message.hasOwnProperty("redshift")) {
            properties.connection = 1;
            {
                var error = $root.JDBC.verify(message.redshift);
                if (error)
                    return "redshift." + error;
            }
        }
        if (message.postgres != null && message.hasOwnProperty("postgres")) {
            if (properties.connection === 1)
                return "connection: multiple values";
            properties.connection = 1;
            {
                var error = $root.JDBC.verify(message.postgres);
                if (error)
                    return "postgres." + error;
            }
        }
        if (message.bigquery != null && message.hasOwnProperty("bigquery")) {
            if (properties.connection === 1)
                return "connection: multiple values";
            properties.connection = 1;
            {
                var error = $root.BigQuery.verify(message.bigquery);
                if (error)
                    return "bigquery." + error;
            }
        }
        if (message.snowflake != null && message.hasOwnProperty("snowflake")) {
            if (properties.connection === 1)
                return "connection: multiple values";
            properties.connection = 1;
            {
                var error = $root.Snowflake.verify(message.snowflake);
                if (error)
                    return "snowflake." + error;
            }
        }
        return null;
    };

    /**
     * Creates a Profile message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof Profile
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {Profile} Profile
     */
    Profile.fromObject = function fromObject(object) {
        if (object instanceof $root.Profile)
            return object;
        var message = new $root.Profile();
        if (object.threads != null)
            message.threads = object.threads | 0;
        if (object.redshift != null) {
            if (typeof object.redshift !== "object")
                throw TypeError(".Profile.redshift: object expected");
            message.redshift = $root.JDBC.fromObject(object.redshift);
        }
        if (object.postgres != null) {
            if (typeof object.postgres !== "object")
                throw TypeError(".Profile.postgres: object expected");
            message.postgres = $root.JDBC.fromObject(object.postgres);
        }
        if (object.bigquery != null) {
            if (typeof object.bigquery !== "object")
                throw TypeError(".Profile.bigquery: object expected");
            message.bigquery = $root.BigQuery.fromObject(object.bigquery);
        }
        if (object.snowflake != null) {
            if (typeof object.snowflake !== "object")
                throw TypeError(".Profile.snowflake: object expected");
            message.snowflake = $root.Snowflake.fromObject(object.snowflake);
        }
        return message;
    };

    /**
     * Creates a plain object from a Profile message. Also converts values to other types if specified.
     * @function toObject
     * @memberof Profile
     * @static
     * @param {Profile} message Profile
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    Profile.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults)
            object.threads = 0;
        if (message.redshift != null && message.hasOwnProperty("redshift")) {
            object.redshift = $root.JDBC.toObject(message.redshift, options);
            if (options.oneofs)
                object.connection = "redshift";
        }
        if (message.bigquery != null && message.hasOwnProperty("bigquery")) {
            object.bigquery = $root.BigQuery.toObject(message.bigquery, options);
            if (options.oneofs)
                object.connection = "bigquery";
        }
        if (message.snowflake != null && message.hasOwnProperty("snowflake")) {
            object.snowflake = $root.Snowflake.toObject(message.snowflake, options);
            if (options.oneofs)
                object.connection = "snowflake";
        }
        if (message.threads != null && message.hasOwnProperty("threads"))
            object.threads = message.threads;
        if (message.postgres != null && message.hasOwnProperty("postgres")) {
            object.postgres = $root.JDBC.toObject(message.postgres, options);
            if (options.oneofs)
                object.connection = "postgres";
        }
        return object;
    };

    /**
     * Converts this Profile to JSON.
     * @function toJSON
     * @memberof Profile
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    Profile.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return Profile;
})();

$root.JDBC = (function() {

    /**
     * Properties of a JDBC.
     * @exports IJDBC
     * @interface IJDBC
     * @property {string|null} [hostName] JDBC hostName
     * @property {number|Long|null} [port] JDBC port
     * @property {string|null} [userName] JDBC userName
     * @property {string|null} [password] JDBC password
     * @property {string|null} [databaseName] JDBC databaseName
     */

    /**
     * Constructs a new JDBC.
     * @exports JDBC
     * @classdesc Represents a JDBC.
     * @implements IJDBC
     * @constructor
     * @param {IJDBC=} [properties] Properties to set
     */
    function JDBC(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * JDBC hostName.
     * @member {string} hostName
     * @memberof JDBC
     * @instance
     */
    JDBC.prototype.hostName = "";

    /**
     * JDBC port.
     * @member {number|Long} port
     * @memberof JDBC
     * @instance
     */
    JDBC.prototype.port = $util.Long ? $util.Long.fromBits(0,0,false) : 0;

    /**
     * JDBC userName.
     * @member {string} userName
     * @memberof JDBC
     * @instance
     */
    JDBC.prototype.userName = "";

    /**
     * JDBC password.
     * @member {string} password
     * @memberof JDBC
     * @instance
     */
    JDBC.prototype.password = "";

    /**
     * JDBC databaseName.
     * @member {string} databaseName
     * @memberof JDBC
     * @instance
     */
    JDBC.prototype.databaseName = "";

    /**
     * Creates a new JDBC instance using the specified properties.
     * @function create
     * @memberof JDBC
     * @static
     * @param {IJDBC=} [properties] Properties to set
     * @returns {JDBC} JDBC instance
     */
    JDBC.create = function create(properties) {
        return new JDBC(properties);
    };

    /**
     * Encodes the specified JDBC message. Does not implicitly {@link JDBC.verify|verify} messages.
     * @function encode
     * @memberof JDBC
     * @static
     * @param {IJDBC} message JDBC message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    JDBC.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.hostName != null && message.hasOwnProperty("hostName"))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.hostName);
        if (message.port != null && message.hasOwnProperty("port"))
            writer.uint32(/* id 3, wireType 0 =*/24).int64(message.port);
        if (message.userName != null && message.hasOwnProperty("userName"))
            writer.uint32(/* id 4, wireType 2 =*/34).string(message.userName);
        if (message.password != null && message.hasOwnProperty("password"))
            writer.uint32(/* id 5, wireType 2 =*/42).string(message.password);
        if (message.databaseName != null && message.hasOwnProperty("databaseName"))
            writer.uint32(/* id 6, wireType 2 =*/50).string(message.databaseName);
        return writer;
    };

    /**
     * Encodes the specified JDBC message, length delimited. Does not implicitly {@link JDBC.verify|verify} messages.
     * @function encodeDelimited
     * @memberof JDBC
     * @static
     * @param {IJDBC} message JDBC message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    JDBC.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a JDBC message from the specified reader or buffer.
     * @function decode
     * @memberof JDBC
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {JDBC} JDBC
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    JDBC.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.JDBC();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 2:
                message.hostName = reader.string();
                break;
            case 3:
                message.port = reader.int64();
                break;
            case 4:
                message.userName = reader.string();
                break;
            case 5:
                message.password = reader.string();
                break;
            case 6:
                message.databaseName = reader.string();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a JDBC message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof JDBC
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {JDBC} JDBC
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    JDBC.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a JDBC message.
     * @function verify
     * @memberof JDBC
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    JDBC.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.hostName != null && message.hasOwnProperty("hostName"))
            if (!$util.isString(message.hostName))
                return "hostName: string expected";
        if (message.port != null && message.hasOwnProperty("port"))
            if (!$util.isInteger(message.port) && !(message.port && $util.isInteger(message.port.low) && $util.isInteger(message.port.high)))
                return "port: integer|Long expected";
        if (message.userName != null && message.hasOwnProperty("userName"))
            if (!$util.isString(message.userName))
                return "userName: string expected";
        if (message.password != null && message.hasOwnProperty("password"))
            if (!$util.isString(message.password))
                return "password: string expected";
        if (message.databaseName != null && message.hasOwnProperty("databaseName"))
            if (!$util.isString(message.databaseName))
                return "databaseName: string expected";
        return null;
    };

    /**
     * Creates a JDBC message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof JDBC
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {JDBC} JDBC
     */
    JDBC.fromObject = function fromObject(object) {
        if (object instanceof $root.JDBC)
            return object;
        var message = new $root.JDBC();
        if (object.hostName != null)
            message.hostName = String(object.hostName);
        if (object.port != null)
            if ($util.Long)
                (message.port = $util.Long.fromValue(object.port)).unsigned = false;
            else if (typeof object.port === "string")
                message.port = parseInt(object.port, 10);
            else if (typeof object.port === "number")
                message.port = object.port;
            else if (typeof object.port === "object")
                message.port = new $util.LongBits(object.port.low >>> 0, object.port.high >>> 0).toNumber();
        if (object.userName != null)
            message.userName = String(object.userName);
        if (object.password != null)
            message.password = String(object.password);
        if (object.databaseName != null)
            message.databaseName = String(object.databaseName);
        return message;
    };

    /**
     * Creates a plain object from a JDBC message. Also converts values to other types if specified.
     * @function toObject
     * @memberof JDBC
     * @static
     * @param {JDBC} message JDBC
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    JDBC.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            object.hostName = "";
            if ($util.Long) {
                var long = new $util.Long(0, 0, false);
                object.port = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
            } else
                object.port = options.longs === String ? "0" : 0;
            object.userName = "";
            object.password = "";
            object.databaseName = "";
        }
        if (message.hostName != null && message.hasOwnProperty("hostName"))
            object.hostName = message.hostName;
        if (message.port != null && message.hasOwnProperty("port"))
            if (typeof message.port === "number")
                object.port = options.longs === String ? String(message.port) : message.port;
            else
                object.port = options.longs === String ? $util.Long.prototype.toString.call(message.port) : options.longs === Number ? new $util.LongBits(message.port.low >>> 0, message.port.high >>> 0).toNumber() : message.port;
        if (message.userName != null && message.hasOwnProperty("userName"))
            object.userName = message.userName;
        if (message.password != null && message.hasOwnProperty("password"))
            object.password = message.password;
        if (message.databaseName != null && message.hasOwnProperty("databaseName"))
            object.databaseName = message.databaseName;
        return object;
    };

    /**
     * Converts this JDBC to JSON.
     * @function toJSON
     * @memberof JDBC
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    JDBC.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return JDBC;
})();

$root.Snowflake = (function() {

    /**
     * Properties of a Snowflake.
     * @exports ISnowflake
     * @interface ISnowflake
     * @property {string|null} [accountId] Snowflake accountId
     * @property {string|null} [userName] Snowflake userName
     * @property {string|null} [password] Snowflake password
     * @property {string|null} [role] Snowflake role
     * @property {string|null} [databaseName] Snowflake databaseName
     * @property {string|null} [warehouse] Snowflake warehouse
     */

    /**
     * Constructs a new Snowflake.
     * @exports Snowflake
     * @classdesc Represents a Snowflake.
     * @implements ISnowflake
     * @constructor
     * @param {ISnowflake=} [properties] Properties to set
     */
    function Snowflake(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * Snowflake accountId.
     * @member {string} accountId
     * @memberof Snowflake
     * @instance
     */
    Snowflake.prototype.accountId = "";

    /**
     * Snowflake userName.
     * @member {string} userName
     * @memberof Snowflake
     * @instance
     */
    Snowflake.prototype.userName = "";

    /**
     * Snowflake password.
     * @member {string} password
     * @memberof Snowflake
     * @instance
     */
    Snowflake.prototype.password = "";

    /**
     * Snowflake role.
     * @member {string} role
     * @memberof Snowflake
     * @instance
     */
    Snowflake.prototype.role = "";

    /**
     * Snowflake databaseName.
     * @member {string} databaseName
     * @memberof Snowflake
     * @instance
     */
    Snowflake.prototype.databaseName = "";

    /**
     * Snowflake warehouse.
     * @member {string} warehouse
     * @memberof Snowflake
     * @instance
     */
    Snowflake.prototype.warehouse = "";

    /**
     * Creates a new Snowflake instance using the specified properties.
     * @function create
     * @memberof Snowflake
     * @static
     * @param {ISnowflake=} [properties] Properties to set
     * @returns {Snowflake} Snowflake instance
     */
    Snowflake.create = function create(properties) {
        return new Snowflake(properties);
    };

    /**
     * Encodes the specified Snowflake message. Does not implicitly {@link Snowflake.verify|verify} messages.
     * @function encode
     * @memberof Snowflake
     * @static
     * @param {ISnowflake} message Snowflake message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Snowflake.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.accountId != null && message.hasOwnProperty("accountId"))
            writer.uint32(/* id 2, wireType 2 =*/18).string(message.accountId);
        if (message.userName != null && message.hasOwnProperty("userName"))
            writer.uint32(/* id 3, wireType 2 =*/26).string(message.userName);
        if (message.password != null && message.hasOwnProperty("password"))
            writer.uint32(/* id 4, wireType 2 =*/34).string(message.password);
        if (message.role != null && message.hasOwnProperty("role"))
            writer.uint32(/* id 5, wireType 2 =*/42).string(message.role);
        if (message.databaseName != null && message.hasOwnProperty("databaseName"))
            writer.uint32(/* id 6, wireType 2 =*/50).string(message.databaseName);
        if (message.warehouse != null && message.hasOwnProperty("warehouse"))
            writer.uint32(/* id 7, wireType 2 =*/58).string(message.warehouse);
        return writer;
    };

    /**
     * Encodes the specified Snowflake message, length delimited. Does not implicitly {@link Snowflake.verify|verify} messages.
     * @function encodeDelimited
     * @memberof Snowflake
     * @static
     * @param {ISnowflake} message Snowflake message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    Snowflake.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a Snowflake message from the specified reader or buffer.
     * @function decode
     * @memberof Snowflake
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {Snowflake} Snowflake
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Snowflake.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.Snowflake();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 2:
                message.accountId = reader.string();
                break;
            case 3:
                message.userName = reader.string();
                break;
            case 4:
                message.password = reader.string();
                break;
            case 5:
                message.role = reader.string();
                break;
            case 6:
                message.databaseName = reader.string();
                break;
            case 7:
                message.warehouse = reader.string();
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a Snowflake message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof Snowflake
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {Snowflake} Snowflake
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    Snowflake.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a Snowflake message.
     * @function verify
     * @memberof Snowflake
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    Snowflake.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.accountId != null && message.hasOwnProperty("accountId"))
            if (!$util.isString(message.accountId))
                return "accountId: string expected";
        if (message.userName != null && message.hasOwnProperty("userName"))
            if (!$util.isString(message.userName))
                return "userName: string expected";
        if (message.password != null && message.hasOwnProperty("password"))
            if (!$util.isString(message.password))
                return "password: string expected";
        if (message.role != null && message.hasOwnProperty("role"))
            if (!$util.isString(message.role))
                return "role: string expected";
        if (message.databaseName != null && message.hasOwnProperty("databaseName"))
            if (!$util.isString(message.databaseName))
                return "databaseName: string expected";
        if (message.warehouse != null && message.hasOwnProperty("warehouse"))
            if (!$util.isString(message.warehouse))
                return "warehouse: string expected";
        return null;
    };

    /**
     * Creates a Snowflake message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof Snowflake
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {Snowflake} Snowflake
     */
    Snowflake.fromObject = function fromObject(object) {
        if (object instanceof $root.Snowflake)
            return object;
        var message = new $root.Snowflake();
        if (object.accountId != null)
            message.accountId = String(object.accountId);
        if (object.userName != null)
            message.userName = String(object.userName);
        if (object.password != null)
            message.password = String(object.password);
        if (object.role != null)
            message.role = String(object.role);
        if (object.databaseName != null)
            message.databaseName = String(object.databaseName);
        if (object.warehouse != null)
            message.warehouse = String(object.warehouse);
        return message;
    };

    /**
     * Creates a plain object from a Snowflake message. Also converts values to other types if specified.
     * @function toObject
     * @memberof Snowflake
     * @static
     * @param {Snowflake} message Snowflake
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    Snowflake.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            object.accountId = "";
            object.userName = "";
            object.password = "";
            object.role = "";
            object.databaseName = "";
            object.warehouse = "";
        }
        if (message.accountId != null && message.hasOwnProperty("accountId"))
            object.accountId = message.accountId;
        if (message.userName != null && message.hasOwnProperty("userName"))
            object.userName = message.userName;
        if (message.password != null && message.hasOwnProperty("password"))
            object.password = message.password;
        if (message.role != null && message.hasOwnProperty("role"))
            object.role = message.role;
        if (message.databaseName != null && message.hasOwnProperty("databaseName"))
            object.databaseName = message.databaseName;
        if (message.warehouse != null && message.hasOwnProperty("warehouse"))
            object.warehouse = message.warehouse;
        return object;
    };

    /**
     * Converts this Snowflake to JSON.
     * @function toJSON
     * @memberof Snowflake
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    Snowflake.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    return Snowflake;
})();

$root.BigQuery = (function() {

    /**
     * Properties of a BigQuery.
     * @exports IBigQuery
     * @interface IBigQuery
     * @property {string|null} [projectId] BigQuery projectId
     * @property {BigQuery.ICredentials|null} [credentials] BigQuery credentials
     */

    /**
     * Constructs a new BigQuery.
     * @exports BigQuery
     * @classdesc Represents a BigQuery.
     * @implements IBigQuery
     * @constructor
     * @param {IBigQuery=} [properties] Properties to set
     */
    function BigQuery(properties) {
        if (properties)
            for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                if (properties[keys[i]] != null)
                    this[keys[i]] = properties[keys[i]];
    }

    /**
     * BigQuery projectId.
     * @member {string} projectId
     * @memberof BigQuery
     * @instance
     */
    BigQuery.prototype.projectId = "";

    /**
     * BigQuery credentials.
     * @member {BigQuery.ICredentials|null|undefined} credentials
     * @memberof BigQuery
     * @instance
     */
    BigQuery.prototype.credentials = null;

    /**
     * Creates a new BigQuery instance using the specified properties.
     * @function create
     * @memberof BigQuery
     * @static
     * @param {IBigQuery=} [properties] Properties to set
     * @returns {BigQuery} BigQuery instance
     */
    BigQuery.create = function create(properties) {
        return new BigQuery(properties);
    };

    /**
     * Encodes the specified BigQuery message. Does not implicitly {@link BigQuery.verify|verify} messages.
     * @function encode
     * @memberof BigQuery
     * @static
     * @param {IBigQuery} message BigQuery message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    BigQuery.encode = function encode(message, writer) {
        if (!writer)
            writer = $Writer.create();
        if (message.projectId != null && message.hasOwnProperty("projectId"))
            writer.uint32(/* id 1, wireType 2 =*/10).string(message.projectId);
        if (message.credentials != null && message.hasOwnProperty("credentials"))
            $root.BigQuery.Credentials.encode(message.credentials, writer.uint32(/* id 3, wireType 2 =*/26).fork()).ldelim();
        return writer;
    };

    /**
     * Encodes the specified BigQuery message, length delimited. Does not implicitly {@link BigQuery.verify|verify} messages.
     * @function encodeDelimited
     * @memberof BigQuery
     * @static
     * @param {IBigQuery} message BigQuery message or plain object to encode
     * @param {$protobuf.Writer} [writer] Writer to encode to
     * @returns {$protobuf.Writer} Writer
     */
    BigQuery.encodeDelimited = function encodeDelimited(message, writer) {
        return this.encode(message, writer).ldelim();
    };

    /**
     * Decodes a BigQuery message from the specified reader or buffer.
     * @function decode
     * @memberof BigQuery
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @param {number} [length] Message length if known beforehand
     * @returns {BigQuery} BigQuery
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    BigQuery.decode = function decode(reader, length) {
        if (!(reader instanceof $Reader))
            reader = $Reader.create(reader);
        var end = length === undefined ? reader.len : reader.pos + length, message = new $root.BigQuery();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
            case 1:
                message.projectId = reader.string();
                break;
            case 3:
                message.credentials = $root.BigQuery.Credentials.decode(reader, reader.uint32());
                break;
            default:
                reader.skipType(tag & 7);
                break;
            }
        }
        return message;
    };

    /**
     * Decodes a BigQuery message from the specified reader or buffer, length delimited.
     * @function decodeDelimited
     * @memberof BigQuery
     * @static
     * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
     * @returns {BigQuery} BigQuery
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    BigQuery.decodeDelimited = function decodeDelimited(reader) {
        if (!(reader instanceof $Reader))
            reader = new $Reader(reader);
        return this.decode(reader, reader.uint32());
    };

    /**
     * Verifies a BigQuery message.
     * @function verify
     * @memberof BigQuery
     * @static
     * @param {Object.<string,*>} message Plain object to verify
     * @returns {string|null} `null` if valid, otherwise the reason why it is not
     */
    BigQuery.verify = function verify(message) {
        if (typeof message !== "object" || message === null)
            return "object expected";
        if (message.projectId != null && message.hasOwnProperty("projectId"))
            if (!$util.isString(message.projectId))
                return "projectId: string expected";
        if (message.credentials != null && message.hasOwnProperty("credentials")) {
            var error = $root.BigQuery.Credentials.verify(message.credentials);
            if (error)
                return "credentials." + error;
        }
        return null;
    };

    /**
     * Creates a BigQuery message from a plain object. Also converts values to their respective internal types.
     * @function fromObject
     * @memberof BigQuery
     * @static
     * @param {Object.<string,*>} object Plain object
     * @returns {BigQuery} BigQuery
     */
    BigQuery.fromObject = function fromObject(object) {
        if (object instanceof $root.BigQuery)
            return object;
        var message = new $root.BigQuery();
        if (object.projectId != null)
            message.projectId = String(object.projectId);
        if (object.credentials != null) {
            if (typeof object.credentials !== "object")
                throw TypeError(".BigQuery.credentials: object expected");
            message.credentials = $root.BigQuery.Credentials.fromObject(object.credentials);
        }
        return message;
    };

    /**
     * Creates a plain object from a BigQuery message. Also converts values to other types if specified.
     * @function toObject
     * @memberof BigQuery
     * @static
     * @param {BigQuery} message BigQuery
     * @param {$protobuf.IConversionOptions} [options] Conversion options
     * @returns {Object.<string,*>} Plain object
     */
    BigQuery.toObject = function toObject(message, options) {
        if (!options)
            options = {};
        var object = {};
        if (options.defaults) {
            object.projectId = "";
            object.credentials = null;
        }
        if (message.projectId != null && message.hasOwnProperty("projectId"))
            object.projectId = message.projectId;
        if (message.credentials != null && message.hasOwnProperty("credentials"))
            object.credentials = $root.BigQuery.Credentials.toObject(message.credentials, options);
        return object;
    };

    /**
     * Converts this BigQuery to JSON.
     * @function toJSON
     * @memberof BigQuery
     * @instance
     * @returns {Object.<string,*>} JSON object
     */
    BigQuery.prototype.toJSON = function toJSON() {
        return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
    };

    BigQuery.Credentials = (function() {

        /**
         * Properties of a Credentials.
         * @memberof BigQuery
         * @interface ICredentials
         * @property {string|null} [type] Credentials type
         * @property {string|null} [projectId] Credentials projectId
         * @property {string|null} [privateKeyId] Credentials privateKeyId
         * @property {string|null} [privateKey] Credentials privateKey
         * @property {string|null} [clientEmail] Credentials clientEmail
         * @property {string|null} [clientId] Credentials clientId
         * @property {string|null} [authUri] Credentials authUri
         * @property {string|null} [tokenUri] Credentials tokenUri
         * @property {string|null} [authProviderX509CertUrl] Credentials authProviderX509CertUrl
         * @property {string|null} [clientX509CertUrl] Credentials clientX509CertUrl
         */

        /**
         * Constructs a new Credentials.
         * @memberof BigQuery
         * @classdesc Represents a Credentials.
         * @implements ICredentials
         * @constructor
         * @param {BigQuery.ICredentials=} [properties] Properties to set
         */
        function Credentials(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null)
                        this[keys[i]] = properties[keys[i]];
        }

        /**
         * Credentials type.
         * @member {string} type
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.type = "";

        /**
         * Credentials projectId.
         * @member {string} projectId
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.projectId = "";

        /**
         * Credentials privateKeyId.
         * @member {string} privateKeyId
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.privateKeyId = "";

        /**
         * Credentials privateKey.
         * @member {string} privateKey
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.privateKey = "";

        /**
         * Credentials clientEmail.
         * @member {string} clientEmail
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.clientEmail = "";

        /**
         * Credentials clientId.
         * @member {string} clientId
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.clientId = "";

        /**
         * Credentials authUri.
         * @member {string} authUri
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.authUri = "";

        /**
         * Credentials tokenUri.
         * @member {string} tokenUri
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.tokenUri = "";

        /**
         * Credentials authProviderX509CertUrl.
         * @member {string} authProviderX509CertUrl
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.authProviderX509CertUrl = "";

        /**
         * Credentials clientX509CertUrl.
         * @member {string} clientX509CertUrl
         * @memberof BigQuery.Credentials
         * @instance
         */
        Credentials.prototype.clientX509CertUrl = "";

        /**
         * Creates a new Credentials instance using the specified properties.
         * @function create
         * @memberof BigQuery.Credentials
         * @static
         * @param {BigQuery.ICredentials=} [properties] Properties to set
         * @returns {BigQuery.Credentials} Credentials instance
         */
        Credentials.create = function create(properties) {
            return new Credentials(properties);
        };

        /**
         * Encodes the specified Credentials message. Does not implicitly {@link BigQuery.Credentials.verify|verify} messages.
         * @function encode
         * @memberof BigQuery.Credentials
         * @static
         * @param {BigQuery.ICredentials} message Credentials message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Credentials.encode = function encode(message, writer) {
            if (!writer)
                writer = $Writer.create();
            if (message.type != null && message.hasOwnProperty("type"))
                writer.uint32(/* id 1, wireType 2 =*/10).string(message.type);
            if (message.projectId != null && message.hasOwnProperty("projectId"))
                writer.uint32(/* id 2, wireType 2 =*/18).string(message.projectId);
            if (message.privateKeyId != null && message.hasOwnProperty("privateKeyId"))
                writer.uint32(/* id 3, wireType 2 =*/26).string(message.privateKeyId);
            if (message.privateKey != null && message.hasOwnProperty("privateKey"))
                writer.uint32(/* id 4, wireType 2 =*/34).string(message.privateKey);
            if (message.clientEmail != null && message.hasOwnProperty("clientEmail"))
                writer.uint32(/* id 5, wireType 2 =*/42).string(message.clientEmail);
            if (message.clientId != null && message.hasOwnProperty("clientId"))
                writer.uint32(/* id 6, wireType 2 =*/50).string(message.clientId);
            if (message.authUri != null && message.hasOwnProperty("authUri"))
                writer.uint32(/* id 7, wireType 2 =*/58).string(message.authUri);
            if (message.tokenUri != null && message.hasOwnProperty("tokenUri"))
                writer.uint32(/* id 8, wireType 2 =*/66).string(message.tokenUri);
            if (message.authProviderX509CertUrl != null && message.hasOwnProperty("authProviderX509CertUrl"))
                writer.uint32(/* id 9, wireType 2 =*/74).string(message.authProviderX509CertUrl);
            if (message.clientX509CertUrl != null && message.hasOwnProperty("clientX509CertUrl"))
                writer.uint32(/* id 10, wireType 2 =*/82).string(message.clientX509CertUrl);
            return writer;
        };

        /**
         * Encodes the specified Credentials message, length delimited. Does not implicitly {@link BigQuery.Credentials.verify|verify} messages.
         * @function encodeDelimited
         * @memberof BigQuery.Credentials
         * @static
         * @param {BigQuery.ICredentials} message Credentials message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        Credentials.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a Credentials message from the specified reader or buffer.
         * @function decode
         * @memberof BigQuery.Credentials
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {BigQuery.Credentials} Credentials
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Credentials.decode = function decode(reader, length) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length, message = new $root.BigQuery.Credentials();
            while (reader.pos < end) {
                var tag = reader.uint32();
                switch (tag >>> 3) {
                case 1:
                    message.type = reader.string();
                    break;
                case 2:
                    message.projectId = reader.string();
                    break;
                case 3:
                    message.privateKeyId = reader.string();
                    break;
                case 4:
                    message.privateKey = reader.string();
                    break;
                case 5:
                    message.clientEmail = reader.string();
                    break;
                case 6:
                    message.clientId = reader.string();
                    break;
                case 7:
                    message.authUri = reader.string();
                    break;
                case 8:
                    message.tokenUri = reader.string();
                    break;
                case 9:
                    message.authProviderX509CertUrl = reader.string();
                    break;
                case 10:
                    message.clientX509CertUrl = reader.string();
                    break;
                default:
                    reader.skipType(tag & 7);
                    break;
                }
            }
            return message;
        };

        /**
         * Decodes a Credentials message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof BigQuery.Credentials
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {BigQuery.Credentials} Credentials
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        Credentials.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a Credentials message.
         * @function verify
         * @memberof BigQuery.Credentials
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        Credentials.verify = function verify(message) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (message.type != null && message.hasOwnProperty("type"))
                if (!$util.isString(message.type))
                    return "type: string expected";
            if (message.projectId != null && message.hasOwnProperty("projectId"))
                if (!$util.isString(message.projectId))
                    return "projectId: string expected";
            if (message.privateKeyId != null && message.hasOwnProperty("privateKeyId"))
                if (!$util.isString(message.privateKeyId))
                    return "privateKeyId: string expected";
            if (message.privateKey != null && message.hasOwnProperty("privateKey"))
                if (!$util.isString(message.privateKey))
                    return "privateKey: string expected";
            if (message.clientEmail != null && message.hasOwnProperty("clientEmail"))
                if (!$util.isString(message.clientEmail))
                    return "clientEmail: string expected";
            if (message.clientId != null && message.hasOwnProperty("clientId"))
                if (!$util.isString(message.clientId))
                    return "clientId: string expected";
            if (message.authUri != null && message.hasOwnProperty("authUri"))
                if (!$util.isString(message.authUri))
                    return "authUri: string expected";
            if (message.tokenUri != null && message.hasOwnProperty("tokenUri"))
                if (!$util.isString(message.tokenUri))
                    return "tokenUri: string expected";
            if (message.authProviderX509CertUrl != null && message.hasOwnProperty("authProviderX509CertUrl"))
                if (!$util.isString(message.authProviderX509CertUrl))
                    return "authProviderX509CertUrl: string expected";
            if (message.clientX509CertUrl != null && message.hasOwnProperty("clientX509CertUrl"))
                if (!$util.isString(message.clientX509CertUrl))
                    return "clientX509CertUrl: string expected";
            return null;
        };

        /**
         * Creates a Credentials message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof BigQuery.Credentials
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {BigQuery.Credentials} Credentials
         */
        Credentials.fromObject = function fromObject(object) {
            if (object instanceof $root.BigQuery.Credentials)
                return object;
            var message = new $root.BigQuery.Credentials();
            if (object.type != null)
                message.type = String(object.type);
            if (object.projectId != null)
                message.projectId = String(object.projectId);
            if (object.privateKeyId != null)
                message.privateKeyId = String(object.privateKeyId);
            if (object.privateKey != null)
                message.privateKey = String(object.privateKey);
            if (object.clientEmail != null)
                message.clientEmail = String(object.clientEmail);
            if (object.clientId != null)
                message.clientId = String(object.clientId);
            if (object.authUri != null)
                message.authUri = String(object.authUri);
            if (object.tokenUri != null)
                message.tokenUri = String(object.tokenUri);
            if (object.authProviderX509CertUrl != null)
                message.authProviderX509CertUrl = String(object.authProviderX509CertUrl);
            if (object.clientX509CertUrl != null)
                message.clientX509CertUrl = String(object.clientX509CertUrl);
            return message;
        };

        /**
         * Creates a plain object from a Credentials message. Also converts values to other types if specified.
         * @function toObject
         * @memberof BigQuery.Credentials
         * @static
         * @param {BigQuery.Credentials} message Credentials
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        Credentials.toObject = function toObject(message, options) {
            if (!options)
                options = {};
            var object = {};
            if (options.defaults) {
                object.type = "";
                object.projectId = "";
                object.privateKeyId = "";
                object.privateKey = "";
                object.clientEmail = "";
                object.clientId = "";
                object.authUri = "";
                object.tokenUri = "";
                object.authProviderX509CertUrl = "";
                object.clientX509CertUrl = "";
            }
            if (message.type != null && message.hasOwnProperty("type"))
                object.type = message.type;
            if (message.projectId != null && message.hasOwnProperty("projectId"))
                object.projectId = message.projectId;
            if (message.privateKeyId != null && message.hasOwnProperty("privateKeyId"))
                object.privateKeyId = message.privateKeyId;
            if (message.privateKey != null && message.hasOwnProperty("privateKey"))
                object.privateKey = message.privateKey;
            if (message.clientEmail != null && message.hasOwnProperty("clientEmail"))
                object.clientEmail = message.clientEmail;
            if (message.clientId != null && message.hasOwnProperty("clientId"))
                object.clientId = message.clientId;
            if (message.authUri != null && message.hasOwnProperty("authUri"))
                object.authUri = message.authUri;
            if (message.tokenUri != null && message.hasOwnProperty("tokenUri"))
                object.tokenUri = message.tokenUri;
            if (message.authProviderX509CertUrl != null && message.hasOwnProperty("authProviderX509CertUrl"))
                object.authProviderX509CertUrl = message.authProviderX509CertUrl;
            if (message.clientX509CertUrl != null && message.hasOwnProperty("clientX509CertUrl"))
                object.clientX509CertUrl = message.clientX509CertUrl;
            return object;
        };

        /**
         * Converts this Credentials to JSON.
         * @function toJSON
         * @memberof BigQuery.Credentials
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        Credentials.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        return Credentials;
    })();

    return BigQuery;
})();

module.exports = $root;
