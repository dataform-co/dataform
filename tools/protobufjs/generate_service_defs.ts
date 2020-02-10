#!/usr/bin/env node
import * as fs from "fs";
import * as protobufjs from "protobufjs";
import * as yargs from "yargs";

function camelToLowerCamel(value: string) {
  return value.substr(0, 1).toLowerCase() + value.substr(1, value.length - 1);
}

const argv = yargs
  .option("protos", { required: true, description: "Source .proto files.", type: "array" })
  .option("service", {
    required: true,
    type: "string",
    description: "The name of the service to process."
  })
  .option("output-path", {
    required: true,
    type: "string",
    description: "Output typescript file path."
  })
  .option("root", {
    type: "string",
    required: true,
    description: "The root path or parent directory of the protos."
  })
  .option("protos-import", {
    type: "string",
    required: true,
    description: "The import to use for the generated protobufs library."
  }).argv;

const oldResolvePath = protobufjs.Root.prototype.resolvePath;
protobufjs.Root.prototype.resolvePath = (unusedPath, filename) => {
  return oldResolvePath(".", filename);
};

protobufjs
  .load((argv.protos as string[]).map(protoPath => String(protoPath)))
  .then(root => {
    const processedRoot = processPackage(argv.root, root);
    writeAllServices([processedRoot.name], processedRoot);
  })
  .catch(e => console.log(e));

// TODO: This produces broken results for nested messages/enums. We need to:
// (a) fix isPackage (probably !isService && !isMessage && !isEnum)
// (b) change the IPackage interface to allow messages/enums to be nested inside other messages
const isPackage = (value: any) =>
  !isService(value) && !isMessage(value) && !isEnum(value) && !isHttpRule(value);
const isService = (value: any) => !!value.methods;
const isMessage = (value: any) => !!value.fields;
const isEnum = (value: any) => !!value.values;
const isHttpRule = (value: any) => value.type === "HttpRule";

interface IPackage {
  name: string;
  subpackages: IPackage[];
  services: IService[];
  messages: IMessage[];
  enums: IEnum[];
}

interface IService {
  name: string;
  methods: Array<{
    name: string;
    requestType: string;
    responseType: string;
    // TODO: Find out what these types actually are and replace "any" with that.
    requestStream: any;
    responseStream: any;
  }>;
}

interface IMessage {
  name: string;
}

interface IEnum {
  name: string;
}

function processPackage(name: string, root: any): IPackage {
  const processedPackage: IPackage = {
    name,
    subpackages: [],
    services: [],
    messages: [],
    enums: []
  };
  for (const key of Object.keys(root.nested)) {
    const value = root.nested[key];
    if (isPackage(value)) {
      processedPackage.subpackages.push(processPackage(key, value));
    } else if (isService(value)) {
      const methods = Object.keys(value.methods).map(methodName => {
        const method = value.methods[methodName];
        return {
          name: methodName,
          requestType: method.requestType,
          responseType: method.responseType,
          requestStream: method.requestStream,
          responseStream: method.responseStream
        };
      });
      processedPackage.services.push({ name: key, methods });
    } else if (isMessage(value)) {
      processedPackage.messages.push({ name: key });
    } else if (isEnum(value)) {
      processedPackage.enums.push({ name: key });
    } else if (isHttpRule(value)) {
      // These are for grpc-gateway and can be ignored.
    } else {
      throw new Error("Unrecognized entry in 'nested' property of protobufjs package:" + value);
    }
  }
  return processedPackage;
}

interface IFoundService {
  packageNameParts: string[];
  processedPackage: IPackage;
  service: IService;
}

function writeAllServices(packageNameParts: string[], processedPackage: IPackage) {
  const foundServices: IFoundService[] = [];
  const findPackages = (packageNameParts: string[], processedPackage: IPackage) => {
    return processedPackage.subpackages.forEach(subpackage => {
      subpackage.services.forEach(service =>
        foundServices.push({
          packageNameParts: [...packageNameParts, subpackage.name],
          service,
          processedPackage
        })
      );
      findPackages([].concat([...packageNameParts, subpackage.name]), subpackage);
    });
  };
  findPackages(packageNameParts, processedPackage);

  const serviceToWrite = foundServices.find(
    service => fullyQualifyService(service) === argv.service
  );

  if (!serviceToWrite) {
    throw new Error(
      `Could not find service named "${argv.service}" in services: [${foundServices
        .map(service => fullyQualifyService(service))
        .join(", ")}]`
    );
  }
  write(
    serviceToWrite.processedPackage,
    serviceToWrite.packageNameParts,
    serviceToWrite.service,
    argv["output-path"]
  );
}

function write(
  currentPackage: IPackage,
  currentNamespaceParts: string[],
  service: IService,
  outputPath: string
) {
  // TODO: We should probably namespace the generated files inside the currentNamespaceParts fields.
  fs.writeFileSync(
    outputPath,
    `// GENERATED CODE.
import * as grpc from "grpc";
import * as protos from "${argv["protos-import"]}";
import { promisify } from "util";

export const NAME = "${service.name}";

export interface Service {
  ${service.methods
    .map(method => {
      return `
  ${camelToLowerCamel(method.name)}(call: grpc.ServerUnaryCall<${fullyQualify(
        currentPackage,
        currentNamespaceParts,
        method.requestType,
        true
      )}>): Promise<${fullyQualify(
        currentPackage,
        currentNamespaceParts,
        method.responseType,
        true
      )}>;`;
    })
    .join("")}
}

export interface IGrpcMethodCallResults {
  timeNanos: number;
  err?: Error;
}

function computeElapsedNanosSince(hrtimeStart: [number, number]) {
  const delta = process.hrtime(hrtimeStart);
  return delta[0] * 1e9 + delta[1];
}

export class ServicePromiseWrapper {
  private impl: Service;
  private afterInterceptor: (method: string, call: grpc.ServerUnaryCall<any>, grpcMethodCallResults: IGrpcMethodCallResults) => void;

  // Note that the 'any' typings here are required because we don't know the request type in advance (it depends on the API method which is called).
  constructor(impl: Service, afterInterceptor?: (method: string, call: grpc.ServerUnaryCall<any>, grpcMethodCallResults: IGrpcMethodCallResults) => void) {
    this.impl = impl;
    this.afterInterceptor = afterInterceptor || (() => undefined);
  }

  ${service.methods
    .map(method => {
      return `
  public ${camelToLowerCamel(method.name)}(call: grpc.ServerUnaryCall<${fullyQualify(
        currentPackage,
        currentNamespaceParts,
        method.requestType,
        true
      )}>, callback: (err: any, response: ${fullyQualify(
        currentPackage,
        currentNamespaceParts,
        method.responseType,
        true
      )}) => void) {
    const asyncInner = async () => {
      const startTime = process.hrtime();
      try {
        const response = await this.impl.${camelToLowerCamel(method.name)}(call);
        this.afterInterceptor("${method.name}", call, {
          timeNanos: computeElapsedNanosSince(startTime)
        });
        callback(null, response);
      } catch (err) {
        this.afterInterceptor("${method.name}", call, {
          timeNanos: computeElapsedNanosSince(startTime),
          err
        });
        callback(err, null);
      }
    };
    asyncInner();
  }
  `;
    })
    .join("")}
}

export class Client {
  private client: any;
  constructor(address: string, options?: object) {
     this.client = new grpc.Client(address, grpc.credentials.createInsecure(), options);
  }

  ${service.methods
    .map(method => {
      return `
  public ${camelToLowerCamel(method.name)}(request: ${fullyQualify(
        currentPackage,
        currentNamespaceParts,
        method.requestType,
        true
      )}, options?: grpc.CallOptions): Promise<${fullyQualify(
        currentPackage,
        currentNamespaceParts,
        method.responseType,
        true
      )}> {
    return promisify(this.client.makeUnaryRequest.bind(this.client))(
      DEFINITION.${camelToLowerCamel(method.name)}.path,
      DEFINITION.${camelToLowerCamel(method.name)}.requestSerialize,
      DEFINITION.${camelToLowerCamel(method.name)}.responseDeserialize,
      request,
      new grpc.Metadata(),
      (options || {}) as any
    );
  }
  `;
    })
    .join("")}
}

export const DEFINITION = {

${service.methods
  .map(method => {
    return `
${camelToLowerCamel(method.name)}: {
  path: '/${currentNamespaceParts
    // We need to skip the first entry (argv.root), because it refers to protobuf type namespaces, not actual package names as specified in .proto files.
    .slice(1)
    .map(part => `${part}.`)
    .join("")}${service.name}/${method.name}',
  requestStream: ${!!method.requestStream},
  responseStream: ${!!method.responseStream},
  requestType: ${fullyQualify(currentPackage, currentNamespaceParts, method.requestType, false)},
  responseType: ${fullyQualify(currentPackage, currentNamespaceParts, method.responseType, false)},
  requestSerialize: (v: ${fullyQualify(
    currentPackage,
    currentNamespaceParts,
    method.requestType,
    true
  )}) => new Buffer(${fullyQualify(
      currentPackage,
      currentNamespaceParts,
      method.requestType,
      false
    )}.encode(v).finish()),
  requestDeserialize: (v: Buffer) => ${fullyQualify(
    currentPackage,
    currentNamespaceParts,
    method.requestType,
    false
  )}.decode(new Uint8Array(v)),
  responseSerialize: (v: ${fullyQualify(
    currentPackage,
    currentNamespaceParts,
    method.responseType,
    true
  )}) => new Buffer(${fullyQualify(
      currentPackage,
      currentNamespaceParts,
      method.responseType,
      false
    )}.encode(v).finish()),
  responseDeserialize: (v: Buffer) => ${fullyQualify(
    currentPackage,
    currentNamespaceParts,
    method.responseType,
    false
  )}.decode(new Uint8Array(v)),
}`;
  })
  .join(",")}
}

// TODO: Delete this, it's only intended temporarily, for rolling out the above (fixed) service definition.
export const UNNAMESPACED_SERVICE_DEFINITION = {

  ${service.methods
    .map(method => {
      return `
  ${camelToLowerCamel(method.name)}: {
    path: '/${service.name}/${method.name}',
    requestStream: ${!!method.requestStream},
    responseStream: ${!!method.responseStream},
    requestType: ${fullyQualify(currentPackage, currentNamespaceParts, method.requestType, false)},
    responseType: ${fullyQualify(
      currentPackage,
      currentNamespaceParts,
      method.responseType,
      false
    )},
    requestSerialize: (v: ${fullyQualify(
      currentPackage,
      currentNamespaceParts,
      method.requestType,
      true
    )}) => new Buffer(${fullyQualify(
        currentPackage,
        currentNamespaceParts,
        method.requestType,
        false
      )}.encode(v).finish()),
    requestDeserialize: (v: Buffer) => ${fullyQualify(
      currentPackage,
      currentNamespaceParts,
      method.requestType,
      false
    )}.decode(new Uint8Array(v)),
    responseSerialize: (v: ${fullyQualify(
      currentPackage,
      currentNamespaceParts,
      method.responseType,
      true
    )}) => new Buffer(${fullyQualify(
        currentPackage,
        currentNamespaceParts,
        method.responseType,
        false
      )}.encode(v).finish()),
    responseDeserialize: (v: Buffer) => ${fullyQualify(
      currentPackage,
      currentNamespaceParts,
      method.responseType,
      false
    )}.decode(new Uint8Array(v)),
  }`;
    })
    .join(",")}
}
`
  );
}

function fullyQualifyService(foundService: IFoundService) {
  return `${foundService.packageNameParts
    .slice(1)
    .map(part => `${part}.`)
    .join("")}${foundService.service.name}`;
}

function fullyQualify(
  currentPackage: IPackage,
  currentNamespaceParts: string[],
  type: string,
  asInterface: boolean
) {
  const fullPackageString = determinePackagePath(currentPackage, currentNamespaceParts, type).join(
    "."
  );
  const typeWithoutPackage = type.split(".").slice(-1)[0];
  const iTypeString = `${asInterface ? "I" : ""}${typeWithoutPackage}`;
  return `${fullPackageString}.${iTypeString}`;
}

function determinePackagePath(
  currentPackage: IPackage,
  currentNamespaceParts: string[],
  type: string
) {
  const typeParts = type.split(".");
  if (typeParts.length > 1) {
    // This type is already package-qualified, thus it must live outside of the current package,
    // and we can just use the value more or less as-is.
    return [].concat([argv.root], typeParts.slice(0, -1));
  }
  // The type is un-namespaced. Thus it must either be in the current package or in no package (i.e. at the root).
  if (currentPackage.messages.find(message => message.name === type)) {
    return currentNamespaceParts;
  }
  // If we can't find it in this package, the type must exist in the root package.
  return [argv.root];
}
