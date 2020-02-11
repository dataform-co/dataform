import { grpc } from "grpc-web-client";
import { ProtobufMessage } from "grpc-web-client/dist/message";
import { UnaryOutput } from "grpc-web-client/dist/unary";
import { Method, RPCImpl, RPCImplCallback } from "protobufjs";

export class ProtobufMessageBytes implements grpc.ProtobufMessage {
  public static deserializeBinary(bytes: Uint8Array): ProtobufMessageBytes {
    return new ProtobufMessageBytes(bytes);
  }
  private bytes: Uint8Array;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  public toObject(): {} {
    return {};
  }

  public serializeBinary(): Uint8Array {
    return this.bytes;
  }
}

export class UnaryMethod
  implements grpc.UnaryMethodDefinition<ProtobufMessageBytes, ProtobufMessageBytes> {
  // TODO: Work out the typing here.
  public requestStream = false as any;
  public responseStream = false as any;
  public requestType = ProtobufMessageBytes as any;
  public responseType = ProtobufMessageBytes as any;

  public methodName: string;
  public service: grpc.ServiceDefinition;

  constructor(methodName: string, service: grpc.ServiceDefinition) {
    this.methodName = methodName;
    this.service = service;
  }
}

export function rpcImpl(
  serviceAddress: string,
  serviceName: string,
  metadataProvider?: () => Promise<object>,
  onEnd?: (output: UnaryOutput<ProtobufMessage>) => void
): RPCImpl {
  const makeGrpcCall = async (
    method: Method,
    requestData: Uint8Array,
    callback: RPCImplCallback
  ) => {
    const metadata = await (metadataProvider ? metadataProvider() : Promise.resolve({}));
    grpc.unary(new UnaryMethod(method.name, { serviceName }), {
      request: new ProtobufMessageBytes(requestData),
      host: serviceAddress,
      metadata: new grpc.Metadata({ ...metadata }),
      onEnd: output => {
        if (output.status === grpc.Code.OK) {
          callback(null, output.message.serializeBinary());
        } else {
          callback(new Error(output.statusMessage));
        }
        if (onEnd) {
          onEnd(output);
        }
      }
    });
  };

  return (method: Method, requestData: Uint8Array, callback: RPCImplCallback) =>
    makeGrpcCall(method, requestData, callback).catch(e => callback(e));
}
