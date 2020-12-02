import { promiseAny, runAsyncIgnoringErrors, sleep, sleepUntil } from "df/common/promises";
import { IProtoClass, ProtoStringifier } from "df/common/protos";
import { IStringifier, JSONObjectStringifier } from "df/common/strings/stringifier";
import { createHandyClient, IHandyRedis } from "handy-redis";
import Redis from "redis";

interface ICacheCodec<K, V> {
  keyStringifier: IStringifier<K>;
  valueStringifier: IStringifier<V>;
}

interface ICache<K, V> {
  codec: ICacheCodec<K, V>;
  redisOptions?: { url: string };
  ttlMs?: number;
  waitForCacheMillis?: number;
  cacheAccessId: string;
  onTCPError?: (err: Error) => void;
  onConnectionError?: (err: Error) => void;
  onCacheHit?: ({ cacheName, lookupTimeMs }: { cacheName: string; lookupTimeMs: number }) => void;
  onCacheMiss?: ({ cacheName, lookupTimeMs }: { cacheName: string; lookupTimeMs: number }) => void;
}

// If no other key properties are provided, keys are scoped on just the project ID.
export class Cache<K, V> {
  public readonly redisOptions?: Redis.ClientOpts;
  private readonly client: IHandyRedis;
  private ttlMs?: number;
  private codec: ICacheCodec<K, V>;
  private waitForCacheMillis: number;
  private cacheAccessId: string;
  private onConnectionError: (err: Error) => void;
  private onCacheHit: ({
    cacheName,
    lookupTimeMs
  }: {
    cacheName: string;
    lookupTimeMs: number;
  }) => void;
  private onCacheMiss: ({
    cacheName,
    lookupTimeMs
  }: {
    cacheName: string;
    lookupTimeMs: number;
  }) => void;

  constructor(options: ICache<K, V>) {
    this.ttlMs = options.ttlMs;
    this.codec = options.codec;
    this.waitForCacheMillis = options.waitForCacheMillis;
    this.cacheAccessId = options.cacheAccessId;
    this.onConnectionError = options.onConnectionError;
    this.onCacheHit = options.onCacheHit;
    this.onCacheMiss = options.onCacheMiss;
    if (!!options.redisOptions?.url) {
      this.redisOptions = this.redisOptions;
      this.client = createHandyClient({
        // Don't store cache queries in a buffer, as cached values outdate quickly.
        enable_offline_queue: false,
        retry_strategy: (_: any) => {
          // Limit retrying connecting to the redis instance to every 1 second.
          return 1000;
        },
        ...options.redisOptions
      });

      // Overriding default on error here prevents complete failure if there's a connection error.
      // Without this, the error thrown is a TCP error, which breaks through all try/catches.
      // A regular error isn't thrown here either because this binding can't be run inside
      // a try/catch at call time of a method, for example `get`.
      this.client.redis.on("error", options.onTCPError || (() => null));
    }
  }

  public async get({
    key,
    calculateValue,
    forceRefresh
  }: {
    key: K;
    calculateValue: (key: K) => Promise<V>;
    forceRefresh?: boolean;
  }) {
    const keyString = this.createKey(key);

    if (!!forceRefresh && !!this.client) {
      await this.invalidate(key);
    }

    const value = await promiseAny([
      // Calculate value is placed first in this list, as if both fail then the calculation
      // error will be the one propagated.
      (async () => {
        if (!!this.waitForCacheMillis) {
          await sleep(this.waitForCacheMillis);
        }
        return await calculateValue(key);
      })(),
      (async () => {
        const lookupStartTime = Date.now();
        let cachedValue: string;
        try {
          cachedValue = await this.client.get(keyString);
        } catch (err) {
          this.onConnectionError(err);
          throw err;
        }
        if (cachedValue === null) {
          this.onCacheHit({
            cacheName: this.cacheAccessId,
            lookupTimeMs: Date.now() - lookupStartTime
          });
          // Throwing here stops promiseAny from resolving with a null value. This error
          // will never be returned, as the calculation error will be instead.
          throw new Error("Failed to retrieve value from Redis cache");
        }
        this.onCacheMiss({
          cacheName: this.cacheAccessId,
          lookupTimeMs: Date.now() - lookupStartTime
        });
        return this.codec.valueStringifier.parse(cachedValue);
      })()
    ]);

    runAsyncIgnoringErrors(
      (async () => {
        const encodedValue = this.codec.valueStringifier.stringify(value);
        await (!!this.ttlMs
          ? this.client.set(keyString, encodedValue, ["PX", this.ttlMs])
          : this.client.set(keyString, encodedValue));
      })()
    );

    return value;
  }

  public async invalidate(key: K) {
    await this.client.del(this.createKey(key));
  }

  public async waitForLiveness() {
    await sleepUntil(async () => {
      try {
        await this.client.ping();
        return true;
      } catch (e) {
        return false;
      }
    });
  }

  private createKey(key: K) {
    return this.codec.keyStringifier.stringify(key);
  }
}

export function createProtoCacheCodec<K, V>({
  keyProto,
  valueProto
}: {
  keyProto: IProtoClass<K, K>;
  valueProto: IProtoClass<V, V>;
}): ICacheCodec<K, V> {
  return {
    keyStringifier: ProtoStringifier.create(keyProto),
    valueStringifier: ProtoStringifier.create(valueProto)
  };
}

export function createJSONCacheCodec<K, V>(): ICacheCodec<K, V> {
  return {
    keyStringifier: JSONObjectStringifier.create<K>(),
    valueStringifier: JSONObjectStringifier.create<V>()
  };
}
