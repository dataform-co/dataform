import { expect } from "chai";

import { sleepUntil } from "df/common/promises";
import { ProtoUtils } from "df/common/protos/proto_utils";
import { dataform } from "df/protos/ts";
import { Cache, createProtoCacheCodec } from "df/redis/client";
import { suite, test } from "df/testing";
import { ChildProcessForBazelTestEnvironment } from "df/testing/child_process";
import { createHandyClient, IHandyRedis } from "handy-redis";
import Redis from "redis";

const REDIS_CLIENT_OPTIONS = { url: "redis://127.0.0.1:6379" };

suite(__filename, ({ before, after }) => {
  const calculateValue = async (testItem: dataform.Target) =>
    dataform.TestResult.create({
      name: `${testItem.database}.${testItem.schema}.${testItem.name}`
    });
  const codec = createProtoCacheCodec({
    keyProto: dataform.Target,
    valueProto: dataform.TestResult
  });
  let redisServer: ChildProcessForBazelTestEnvironment;
  let testCache: IHandyRedis;

  before("setup", async () => {
    redisServer = new ChildProcessForBazelTestEnvironment(`redis/redis-server`, []);
    redisServer.spawn();
    const untypedClient = Redis.createClient(REDIS_CLIENT_OPTIONS);
    untypedClient.on("error", (e: Error) => {
      // This untypedClient error catch prevents uncatchable errors on TCP connection fail.
    });
    testCache = createHandyClient(untypedClient);
    // Wait for redis server to become responsive.
    await sleepUntil(async () => {
      try {
        await testCache.ping();
        return true;
      } catch (e) {
        return false;
      }
    });
  });

  after("teardown", async () => {
    redisServer.kill();
  });

  test("get value calculation is done if redis connection fails", async () => {
    // Give an invalid port number.
    const cache = new Cache({
      codec,
      redisOptions: { url: "redis://127.0.0.1:1234" },
      cacheAccessId: "get-value-calculation-test"
    });
    const key = dataform.Target.create({
      database: "database",
      schema: "schema",
      name: "name"
    });
    const calculatedValue = await cache.get({ key, calculateValue });
    expect(calculatedValue).deep.equals(await calculateValue(key));
  });

  test("redis server works", async () => {
    await testCache.set("testKey", "testValue");
    const result = await testCache.get("testKey");
    expect(result).to.equal("testValue");
  });

  test("value setting and invalidation", async () => {
    const cache = new Cache({
      codec,
      redisOptions: REDIS_CLIENT_OPTIONS,
      cacheAccessId: "value-setting-and-invalidation-test"
    });
    await cache.waitForLiveness();
    const key = dataform.Target.create({
      database: "oldDatabase",
      schema: "oldSchema",
      name: "oldName"
    });
    const value = await cache.get({ key, calculateValue });
    expect(value).deep.equals(await calculateValue(key));

    // Directly check the value exists in the cache.
    const encodedKey = ProtoUtils.encode(dataform.Target, key);
    let cachedValue: dataform.TestResult;
    // Cache population may not have completed yet, so wait until it has.
    await sleepUntil(async () => {
      cachedValue = codec.valueStringifier.parse(await testCache.get(encodedKey));
      return !!cachedValue?.name;
    });
    expect(cachedValue).deep.equals(value);

    await cache.invalidate(key);
    const invalidatedValue = await testCache.get(encodedKey);
    expect(invalidatedValue).to.equal(null);
  });
});
