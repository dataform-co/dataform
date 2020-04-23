import { dataform } from "@dataform/protos";
import * as getPort from "get-port";
import * as net from "net";
import * as ssh2 from "ssh2";

/**
 * To connect to databases via an SSH tunnel we create a local socket on this machine
 * on a random port, and forward traffic through an SSH tunnel to the remote SSH server
 * where it then exits the tunnel and connect to a database on e.g. a private network.
 */
export class SSHTunnelProxy {
  public static async create(
    tunnel: dataform.ISSHTunnel,
    destination: { host: string; port: number }
  ) {
    // Find a free local port for the tunnel proxy.
    const localPort = await getPort();
    const sshClient = new ssh2.Client();

    const proxy = net.createServer(sock => {
      sshClient.forwardOut(
        sock.remoteAddress,
        sock.remotePort,
        destination.host,
        destination.port,
        (err, stream) => {
          if (err) {
            return sock.destroy();
          }
          sock.pipe(stream);
          stream.pipe(sock);
        }
      );
    });

    proxy.listen(localPort, "127.0.0.1");

    sshClient.connect({
      host: tunnel.host,
      port: tunnel.port,
      username: tunnel.username,
      privateKey: tunnel.privateKey
    });

    await new Promise(resolve => sshClient.on("ready", () => resolve()));

    return new SSHTunnelProxy(sshClient, proxy, localPort);
  }

  // We need to keep track of connections so we can destroy them, as the proxy server won't close
  // when there are keep-alive connections going on.
  private connections = new Set<net.Socket>();

  private constructor(
    private sshClient: ssh2.Client,
    private proxy: net.Server,
    public localPort: number
  ) {
    proxy.on("connection", socket => {
      this.connections.add(socket);
      socket.once("close", () => this.connections.delete(socket));
    });
  }

  public async close() {
    this.sshClient.destroy();
    // Explicitly destroy existing connections before closing the proxy.
    this.connections.forEach(socket => socket.destroy());
    await new Promise(resolve => this.proxy.close(() => resolve()));
  }
}
