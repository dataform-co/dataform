import { AuthClient, gaxios } from "google-auth-library";

/** A source AuthClient that records its requests and answers them with an impersonated token. */
export class FakeSourceClient extends AuthClient {
  public readonly requests: gaxios.GaxiosOptions[] = [];

  public async getAccessToken() {
    return { token: "source-token" };
  }

  public async getRequestHeaders() {
    return new Headers({ authorization: "Bearer source-token" });
  }

  public async request<T>(options: gaxios.GaxiosOptions): gaxios.GaxiosPromise<T> {
    this.requests.push(options);
    return {
      data: { accessToken: "impersonated-token", expireTime: "2099-01-01T00:00:00Z" },
    } as gaxios.GaxiosResponse<T>;
  }
}
