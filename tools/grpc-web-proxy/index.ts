import * as http2 from "http2";

const GRPC_CONTENT_TYPE = "application/grpc";
const GRPC_WEB_CONTENT_TYPE = "application/grpc-web";
const GRPC_WEB_TEXT_CONTENT_TYPE = "application/grpc-web-text";

export interface IGrpcWebProxyOptions {
  backend: string;
  port: number;
  secure?:
    | "fake-https"
    | "insecure"
    | {
        key: string;
        cert: string;
      };
}

export class GrpcWebProxy {
  private webServer: http2.Http2Server;
  private grpcClient: http2.ClientHttp2Session;

  constructor(options: IGrpcWebProxyOptions) {
    // Set defaults.
    options = { secure: "fake-https", ...options };

    // Create the server.
    this.webServer =
      // As this is http2 server, most browsers require it to be https, so this is the default.
      options.secure === "fake-https"
        ? http2.createSecureServer({ key: FAKE_KEY, cert: FAKE_CERT }).listen(options.port)
        : options.secure === "insecure"
        ? http2.createServer().listen(options.port)
        : http2.createSecureServer({ ...options.secure }).listen(options.port);

    // Handle requests.
    this.webServer.on("stream", (stream, headers) => {
      this.handleGrpcWebRequest(stream, headers);
    });

    // Constantly try to connect to the backend.
    const _ = this.connectAndKeepAlive(options.backend);
  }

  private async connectAndKeepAlive(backend: string) {
    while (true) {
      if (!this.grpcClient || this.grpcClient.destroyed) {
        this.grpcClient = http2.connect(backend);
        this.grpcClient.on("connect", () => {
          console.info(`Successfully connected to backend: ${backend}`);
        });
        this.grpcClient.on("error", error => {
          console.error(`Failed to connect to backend: ${backend}\n${error}`);
        });
      }
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  private handleGrpcWebRequest(
    webStream: http2.ServerHttp2Stream,
    webHeaders: http2.IncomingHttpHeaders
  ) {
    try {
      // CORS requests.
      if (webHeaders[":method"] === "OPTIONS") {
        webStream.respond({ ...corsResponseAllowOrigin(webHeaders), ":status": 200 });
        webStream.end();
        return;
      }

      // gRPC-web requests.
      const grpcRequestHeaders = cleanRequestHeaders(webHeaders);
      const grpcRequest = this.grpcClient.request(grpcRequestHeaders);

      webStream.on("data", chunk => {
        grpcRequest.write(chunk);
      });
      webStream.on("close", () => {
        grpcRequest.end();
      });
      grpcRequest.on("response", headers => {
        webStream.respond(cleanResponseHeaders(headers, webHeaders.origin));
      });
      grpcRequest.on("trailers", headers => {
        webStream.write(trailersToPayload(headers));
      });
      grpcRequest.on("data", chunk => {
        webStream.write(chunk);
      });
      grpcRequest.on("error", e => {
        console.error(e);
        webStream.end();
      });
      grpcRequest.on("end", () => {
        webStream.end();
      });
    } catch (e) {
      webStream.end();
      console.error(e);
    }
  }
}

/**
 * Returns a lenient cors response, allowing any origin and all headers sent.
 */
function corsResponseAllowOrigin(requestHeaders: http2.IncomingHttpHeaders) {
  const allowRequestHeaders = requestHeaders["access-control-request-headers"];
  const allowRequestHeadersList = typeof allowRequestHeaders === "string" ? [allowRequestHeaders] : allowRequestHeaders;
  return {
    "access-control-allow-credentials": "true",
    "access-control-allow-headers": [
      "x-grpc-web",
      "content-type",
      ...Object.keys(requestHeaders),
      ...allowRequestHeadersList
    ].join(", "),
    "access-control-allow-methods": "POST",
    "access-control-allow-origin": requestHeaders.origin,
    "access-control-max-age": 600,
    "content-length": 0,
    vary: "origin, access-control-request-method, access-control-request-headers"
  };
}

/**
 * Clean the request headers from web to grpc service.
 */
function cleanRequestHeaders(webHeaders: http2.IncomingHttpHeaders): http2.OutgoingHttpHeaders {
  const contentType = webHeaders["content-type"] || GRPC_WEB_CONTENT_TYPE;
  const incomingContentType = GRPC_WEB_CONTENT_TYPE;
  const isTextFormat = contentType.startsWith(GRPC_WEB_TEXT_CONTENT_TYPE);
  if (isTextFormat) {
    throw new Error("Text format is unsupported.");
  }
  const grpcRequestHeaders: http2.OutgoingHttpHeaders = { ...webHeaders };
  grpcRequestHeaders["content-type"] = contentType.replace(incomingContentType, GRPC_CONTENT_TYPE);
  delete grpcRequestHeaders["content-length"];
  grpcRequestHeaders.protomajor = 2;
  grpcRequestHeaders.protominor = 0;
  grpcRequestHeaders.te = "trailers";
  return grpcRequestHeaders;
}

/**
 * Clean the response headers from grpc service to web.
 */
function cleanResponseHeaders(
  grpcHeaders: http2.IncomingHttpHeaders,
  origin: string | string[]
): http2.OutgoingHttpHeaders {
  const newHeaders: http2.OutgoingHttpHeaders = { ...grpcHeaders };
  // Not entirely sure why this needs to be removed, but it does.
  delete newHeaders[":status"];
  // Set grpc-status to 0 if it's not present in the server response.
  newHeaders["grpc-status"] = newHeaders["grpc-status"] || 0;
  // The original content type was grpc, change to web.
  newHeaders["content-type"] = GRPC_CONTENT_TYPE;
  newHeaders["access-control-allow-origin"] = origin;
  newHeaders["access-control-expose-headers"] = [...Object.keys(newHeaders)].join(", ");
  return newHeaders;
}

/**
 * Turn trailers into response chunks as per the grpc-web protocol.
 */
function trailersToPayload(trailerHeaders: http2.IncomingHttpHeaders) {
  const headersBuffer = Buffer.from(
    Object.keys(trailerHeaders)
      .filter(key => ["grpc-status"].includes(key))
      .map(key => `${key}: ${trailerHeaders[key]}\r\n`)
      .join("")
  );
  const buffer = new ArrayBuffer(5);
  const uint8View = new Uint8Array(buffer);
  const uint32View = new Uint8Array(buffer);
  uint8View.set([128], 0);
  uint32View.set([headersBuffer.byteLength], 1);
  return Buffer.concat([uint8View, headersBuffer]);
}

const FAKE_KEY = `
-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNIoBojiZnziZc
1Etawvo8ZKPYCDEQ/fY7laO43bDNRapmKfoqXJDQfBuqNwRv8PRM7vBYm7+b0RXE
Kn3RHQD9WKe59+Tnwf3VKGjKIwQ96s9kHh/FEfFXeZBa2b7t8tDxHhttruhK1g3+
nhDivufv/Aarz7RqauG6u3v3pFGRYaRNn9yuZdu8zbyerRLTrhYoslIuoBd0Nl40
SPbTcwREYJRayjQqpGWlzvGJvLhVJENVGAzzuMd5wXBVEGN3Wg5fAcwpZCddCW16
uNZEKRhVFv6Cvce3lLB3MvNrd/MOp/WoXVszYXA+bse7Fh3wnQe4g6JBJfZ2MyMr
iB+eBetrAgMBAAECggEBALIJr8kPFuYhVcpbtssfTm/8KPfmpC9LO9qbFW3tevWt
8SaaaDU8AbPxA1HITmGZj272MkO1aaei4HFRL8G+mo1H1MrjDBjZlaTbXZeSrKvQ
kA8k1g4EdKKnL1KqibubcxzSNJwNoi7ReXPzXFRvvH3dDy0vKDb0bNXUwtM8Gk9C
D5JTFhc2uhI6Mvv6NXTsbOlpy0rJejFCd/4owO8xWwXgOJlP2QSO756uCwHE9EVd
m6rxOlZKzrt/2v8rlzEiW/wQk6MzyUcBkS9CiaODRBOwqcNDCZc+rhODd/2V2qgN
g8sORZZ+aGrgkI016cHCxXPzeYBfDib1uYclULJZ0gECgYEA/mebyNWhOcNBpjoq
8iVbQElppxlHvVh4cknbN77OqDDRxZg2xnOnxxzmOK/oIsqMEhs9mRy6r1S84pqB
rkpEfOrYek2PA9agttYVngy7J3yOBXRX4ZTPI50KpFsN45L3XxdizXfJwj6bpvVq
jgmf5OGcfsfXvVkolWiIaJSWihsCgYEAzmvNBeb3+JBkPeYkJYJ10xwH+yOnZd6w
/WlhlKFXekngL+6uRz2vHcoW+1Fw9yRm7pO8wpYWVuj3fYfU9kn9YqdOC/2k+qVl
pvDheCKh06Zk1a5ZdIcePn567Q1T2As+UzJ9nHlAjQxi4g9z3bZAuOFyx6Rm24sz
dDY420vIOPECgYEAs/Zm5TL50fqlvgj3yENUwa0s59+iN/cVfQNx54U6ew/N1JFQ
biHyVY/D6+XDuJi/bS+H63+/L7gpxcK2aaxvtk4KxLmIqZZyKeRXdm5bFhut+33J
jPHPdcnpdUpUEOAtzT6lzeMm/hl70idZMRDt4uMV6TlFC9S+OLKxjAlQVHECgYBn
Hh0unKI1jtQ0w9o8zr5TdsZZ5AbE8glSnqk7mZncojkXWNHBDwSDCiiO5bFcFNhC
yJTcVCPWyMyR7iAp5O5qsQ9hGBWpjKSKT/0iiZJz68SlplJTzwgByidYcnb3Dy7U
Wv27BCuGCrD/Ix8Lm/cbJKy1JOCpPhG3NTsT+fiM0QKBgQDEtot3hM3DNHiKBbUC
Xbk7lPd6/SyYJ2yjqGvEhHiE047Z+M7hmvYV+gwhxb3i6QMoYR464lgi1JH4iGgG
on8mjhpkUa9jQjObZEau+pjsm8BjhUWTNhLYMa5pi1POywjnEoWiqvPHJUM7YPLC
Ml7ChCE3i/uo1FNPVmZ9vmNo1g==
-----END PRIVATE KEY-----
`;
const FAKE_CERT = `
-----BEGIN CERTIFICATE-----
MIIDazCCAlOgAwIBAgIUMNWIVnJyRDpAQ4aCOK9JLc3/N20wDQYJKoZIhvcNAQEL
BQAwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDAeFw0xOTExMDIxNjE2NDJaFw0xOTEy
MDIxNjE2NDJaMEUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEw
HwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDNIoBojiZnziZc1Etawvo8ZKPYCDEQ/fY7laO43bDN
RapmKfoqXJDQfBuqNwRv8PRM7vBYm7+b0RXEKn3RHQD9WKe59+Tnwf3VKGjKIwQ9
6s9kHh/FEfFXeZBa2b7t8tDxHhttruhK1g3+nhDivufv/Aarz7RqauG6u3v3pFGR
YaRNn9yuZdu8zbyerRLTrhYoslIuoBd0Nl40SPbTcwREYJRayjQqpGWlzvGJvLhV
JENVGAzzuMd5wXBVEGN3Wg5fAcwpZCddCW16uNZEKRhVFv6Cvce3lLB3MvNrd/MO
p/WoXVszYXA+bse7Fh3wnQe4g6JBJfZ2MyMriB+eBetrAgMBAAGjUzBRMB0GA1Ud
DgQWBBQ+8iEq1jJ+0o2emTM1cqUsJnJfGzAfBgNVHSMEGDAWgBQ+8iEq1jJ+0o2e
mTM1cqUsJnJfGzAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQC6
WvwMDoY/IQ2HlPMae/hb5RcBYBanM2iLwqPVOEjYw+fXiej4uNJ3zL++tCX50Wwp
JgX9e7+FiC3KDf4TeGDz+VUA0KcBcft6bQYctDWN2WLavRktY2Bly3f+eQlXRtaJ
ZhxFi2aSxthmlIa3qKHkAkwGfSTObGu62HjJa/xtXSLyKA7wid3Sk/c/Qwu9RAd1
J448MERrg0tcvs2NL7/MmdEwMSsMnHQW+XyePfqDvMCicobu+PY9YNTOuDFY+dZl
OcI9EnRqZrVICRaWK/p9U9UNL/mqoYyFqr1O0t4fSsLijzU4Tfybe3GK36RZFu3k
xH2QtZqGBWSDjPDTUyp2
-----END CERTIFICATE-----
`;
