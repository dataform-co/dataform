import * as assert from "assert";
import { expect } from "chai";
import * as fs from "fs-extra";
import * as http from "http";
import { AddressInfo } from "net";
import * as path from "path";

import { Hook, suite, test } from "df/testing";
import {
  bridgeCloudSdkConfig,
  ensureAdcAvailable,
  IAdcCheckOptions,
  requireAdc,
  resolveCredentialsPath,
} from "df/testing/credentials";
import { TmpDirFixture } from "df/testing/fixtures";

const ADC_ONLY_CONFIG = { projectId: "test-project", location: "US" };
// Never probe the real metadata server: it is reachable on GCE and Cloudtop machines.
const NO_METADATA_SERVER_ENV: NodeJS.ProcessEnv = { METADATA_SERVER_DETECTION: "none" };

suite("testing/credentials", ({ afterEach }) => {
  const tmpDirFixture = new TmpDirFixture(afterEach);

  function writeCredentialsFile(config: object): string {
    const credentialsPath = path.join(tmpDirFixture.createNewTmpDir(), "bigquery.json");
    fs.writeFileSync(credentialsPath, JSON.stringify(config));
    return credentialsPath;
  }

  function createCloudSdkConfigWithAdc(): string {
    const cloudsdkDir = tmpDirFixture.createNewTmpDir();
    fs.writeFileSync(path.join(cloudsdkDir, "application_default_credentials.json"), "{}");
    return cloudsdkDir;
  }

  // Runs `fn` against a minimal stand-in for the GCE metadata server on a random local port,
  // passing it the server address as "host:port" (as a metadata server emulator would be set).
  async function withMetadataServer(
    tokenStatusCode: number,
    fn: (host: string) => Promise<void>,
  ): Promise<void> {
    const server = http.createServer((req, res) => {
      const isTokenRequest =
        req.url === "/computeMetadata/v1/instance/service-accounts/default/token" &&
        req.headers["metadata-flavor"] === "Google";
      res.writeHead(isTokenRequest ? tokenStatusCode : 400, { "Metadata-Flavor": "Google" });
      res.end("{}");
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    try {
      await fn(`127.0.0.1:${(server.address() as AddressInfo).port}`);
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  }

  suite("resolveCredentialsPath", () => {
    function createWorkspace(...fileNames: string[]): string {
      const baseDir = tmpDirFixture.createNewTmpDir();
      fs.mkdirpSync(path.join(baseDir, "test_credentials"));
      for (const fileName of fileNames) {
        fs.writeFileSync(path.join(baseDir, "test_credentials", fileName), "{}");
      }
      return baseDir;
    }

    test("returns the committed default when there is no local override", () => {
      const baseDir = createWorkspace("bigquery.default.json");
      expect(resolveCredentialsPath(baseDir)).to.equal(
        path.join(baseDir, "test_credentials", "bigquery.default.json"),
      );
    });

    test("prefers the git-ignored local override bigquery.json", () => {
      const baseDir = createWorkspace("bigquery.default.json", "bigquery.json");
      expect(resolveCredentialsPath(baseDir)).to.equal(
        path.join(baseDir, "test_credentials", "bigquery.json"),
      );
    });
  });

  suite("bridgeCloudSdkConfig", () => {
    test("sets GOOGLE_APPLICATION_CREDENTIALS from CLOUDSDK_CONFIG when unset", () => {
      const cloudsdkDir = createCloudSdkConfigWithAdc();
      const env: NodeJS.ProcessEnv = { CLOUDSDK_CONFIG: cloudsdkDir };
      bridgeCloudSdkConfig(env);
      expect(env.GOOGLE_APPLICATION_CREDENTIALS).to.equal(
        path.join(cloudsdkDir, "application_default_credentials.json"),
      );
    });

    test("keeps an existing GOOGLE_APPLICATION_CREDENTIALS", () => {
      const env: NodeJS.ProcessEnv = {
        CLOUDSDK_CONFIG: createCloudSdkConfigWithAdc(),
        GOOGLE_APPLICATION_CREDENTIALS: "/existing/creds.json",
      };
      bridgeCloudSdkConfig(env);
      expect(env.GOOGLE_APPLICATION_CREDENTIALS).to.equal("/existing/creds.json");
    });
  });

  suite("ensureAdcAvailable", () => {
    test("succeeds with an explicit service account key in the credentials file", async () => {
      await ensureAdcAvailable({
        credentialsPath: writeCredentialsFile({
          ...ADC_ONLY_CONFIG,
          credentials: '{"type":"service_account"}',
        }),
        env: NO_METADATA_SERVER_ENV,
      });
    });

    test("succeeds when GOOGLE_APPLICATION_CREDENTIALS points to an existing file", async () => {
      const adcPath = path.join(tmpDirFixture.createNewTmpDir(), "adc.json");
      fs.writeFileSync(adcPath, "{}");
      await ensureAdcAvailable({
        credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG),
        env: { ...NO_METADATA_SERVER_ENV, GOOGLE_APPLICATION_CREDENTIALS: adcPath },
      });
    });

    test("fails fast when GOOGLE_APPLICATION_CREDENTIALS points to a missing file", async () => {
      const missingPath = path.join(tmpDirFixture.createNewTmpDir(), "does_not_exist.json");
      await assert.rejects(
        ensureAdcAvailable({
          credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG),
          env: { ...NO_METADATA_SERVER_ENV, GOOGLE_APPLICATION_CREDENTIALS: missingPath },
        }),
        /GOOGLE_APPLICATION_CREDENTIALS points to a non-existent file/,
      );
    });

    test("succeeds with gcloud ADC in $HOME/.config/gcloud", async () => {
      const home = tmpDirFixture.createNewTmpDir();
      const gcloudDir = path.join(home, ".config", "gcloud");
      fs.mkdirpSync(gcloudDir);
      fs.writeFileSync(path.join(gcloudDir, "application_default_credentials.json"), "{}");
      await ensureAdcAvailable({
        credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG),
        env: { ...NO_METADATA_SERVER_ENV, HOME: home },
        platform: "linux",
      });
    });

    test("succeeds when the metadata server at GCE_METADATA_HOST (host:port) returns a token", async () => {
      await withMetadataServer(200, async (host) => {
        await ensureAdcAvailable({
          credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG),
          env: { HOME: tmpDirFixture.createNewTmpDir(), GCE_METADATA_HOST: host },
          platform: "linux",
        });
      });
    });

    test("fails when the metadata server is reachable but returns no token", async () => {
      await withMetadataServer(404, async (host) => {
        await assert.rejects(
          ensureAdcAvailable({
            credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG),
            env: { HOME: tmpDirFixture.createNewTmpDir(), GCE_METADATA_HOST: host },
            platform: "linux",
          }),
          /^Error: No Application Default Credentials found\./,
        );
      });
    });

    test("accepts an explicit http:// scheme in GCE_METADATA_HOST", async () => {
      await withMetadataServer(200, async (host) => {
        await ensureAdcAvailable({
          credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG),
          env: { HOME: tmpDirFixture.createNewTmpDir(), GCE_METADATA_HOST: `http://${host}` },
          platform: "linux",
        });
      });
    });

    test("does not downgrade an explicit https:// GCE_METADATA_HOST to plain http", async () => {
      // The stand-in server only speaks plain HTTP, so an HTTPS probe must fail.
      await withMetadataServer(200, async (host) => {
        await assert.rejects(
          ensureAdcAvailable({
            credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG),
            env: { HOME: tmpDirFixture.createNewTmpDir(), GCE_METADATA_HOST: `https://${host}` },
            platform: "linux",
          }),
          /^Error: No Application Default Credentials found\./,
        );
      });
    });

    test("fails fast with an actionable message when no ADC is available", async () => {
      await assert.rejects(
        ensureAdcAvailable({
          credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG),
          env: { ...NO_METADATA_SERVER_ENV, HOME: tmpDirFixture.createNewTmpDir() },
          platform: "linux",
        }),
        {
          message:
            "No Application Default Credentials found. Run `gcloud auth application-default login` or set GOOGLE_APPLICATION_CREDENTIALS.",
        },
      );
    });
  });

  suite("requireAdc", () => {
    function registerHook(options: IAdcCheckOptions): Hook {
      const registeredHooks: Hook[] = [];
      requireAdc((...args) => {
        const hook = Hook.create(...args);
        registeredHooks.push(hook);
        return hook;
      }, options);
      expect(registeredHooks.map((hook) => hook.options.name)).to.deep.equal([
        "verify Application Default Credentials",
      ]);
      return registeredHooks[0];
    }

    function runHook(hook: Hook): Promise<void> {
      return hook.run({
        testNameMatcher: /.*/,
        path: [],
        results: [],
        beforeEaches: [],
        afterEaches: [],
      });
    }

    test("registers a set-up hook that checks ADC for the given credentials file", async () => {
      // Passes without any ADC because the credentials file contains an explicit key.
      await runHook(
        registerHook({
          credentialsPath: writeCredentialsFile({
            ...ADC_ONLY_CONFIG,
            credentials: '{"type":"service_account"}',
          }),
          env: { ...NO_METADATA_SERVER_ENV },
        }),
      );
    });

    test("bridges CLOUDSDK_CONFIG into GOOGLE_APPLICATION_CREDENTIALS when the suite starts", async () => {
      const cloudsdkDir = createCloudSdkConfigWithAdc();
      const env: NodeJS.ProcessEnv = { ...NO_METADATA_SERVER_ENV, CLOUDSDK_CONFIG: cloudsdkDir };
      const hook = registerHook({ credentialsPath: writeCredentialsFile(ADC_ONLY_CONFIG), env });

      // Registering the hook doesn't touch the environment; running it does.
      expect(env.GOOGLE_APPLICATION_CREDENTIALS).to.equal(undefined);
      await runHook(hook);
      expect(env.GOOGLE_APPLICATION_CREDENTIALS).to.equal(
        path.join(cloudsdkDir, "application_default_credentials.json"),
      );
    });
  });
});
