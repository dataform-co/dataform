This is a grpc-web proxy written purely in Node, based on the [grpc-web protocol](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md) and heavily inspired / ported from the [improbable-eng/grpc-web](https://github.com/improbable-eng/grpc-web).

If you write gRPC services in Node and want to expose them to web clients, without having to run additional infrastructure such as Envoy or the above libraries then this is for you.

**This is very alpha, and has had limited testing. I would not recommend using it for production deployment just yet.**

This has only been tested with unary gRPC calls.

## Installation

```bash
npm i @dataform/grpc-web-proxy
```

```bash
yarn add @dataform/grpc-web-proxy
```

## Usage (CLI)

To start the proxy, you must set a backend address (the gRPC service you want to expose) and a port to expose the proxy on.

```bash
npx grpc-web-proxy --backend http://localhost:1234 --port 8000
```

By default this will run an insecure HTTP/1 server.

To run an insecure HTTP2 server:

```bash
npx grpc-web-proxy --backend http://localhost:1234 --port 8000 --mode http2-insecure
```

To provide your own certs for a HTTP/2 server:

```bash
npx grpc-web-proxy --backend http://localhost:1234 --port 8000 --mode http2-secure --ssl-key-path somekey.key --ssl-cert-path somecert.crt
```

## Usage (Code)

If you want to run the proxy inside an existing node server, you can do the following:

```js
import { GrpcWebProxy } from "@dataform/grpc-web-proxy";

// Insecure HTTP/1.
new GrpcWebProxy({
    backend: "http://localhost:1234",
    port: 8000
});

// Fake HTTPS/2.
new GrpcWebProxy({
    backend: "http://localhost:1234",
    port: 8000,
    mode: "http2-fake-https"
});

// Secure HTTPS/2.
new GrpcWebProxy({
    backend: "http://localhost:1234",
    port: 8000,
    mode: "http2-secure",
    key: ...,
    cert: ...
});

// Insecure HTTPS/2.
new GrpcWebProxy({
    backend: "http://localhost:1234",
    port: 8000,
    mode: "http2-insecure"
});
```

## Client libraries

To connect to the new server, you will need to use a client side library such as [grpc/grpc-web](https://github.com/grpc/grpc-web) or [improbable-eng/grpc-web](https://github.com/improbable-eng/grpc-web/tree/master/client/grpc-web) in order to speak to the grpc-web-proxy.
