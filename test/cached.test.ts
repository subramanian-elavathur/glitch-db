import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import GlitchDB, { type GlitchPartition } from "../src/index.ts";
import { createTempDir, removeTempDir } from "./helpers.ts";

interface TestData {
  a: string;
  b: number;
}

const KEYS = 1000;

describe("cached", () => {
  let tempDirectory: string;
  let withCache: GlitchPartition<TestData>;
  let withoutCache: GlitchPartition<TestData>;
  let smallCache: GlitchPartition<TestData>;

  before(async () => {
    tempDirectory = await createTempDir("cached");
    withCache = new GlitchDB(tempDirectory).getPartition<TestData>(
      "cached",
      undefined,
      KEYS,
    );
    withoutCache = new GlitchDB(tempDirectory, 0).getPartition<TestData>(
      "no-cache",
    );
    smallCache = new GlitchDB(tempDirectory).getPartition<TestData>(
      "small-cache",
      undefined,
      10,
    );
    for (let i = 0; i < KEYS; i++) {
      await withCache.set(`key-${i}`, { a: `a${i}`, b: i });
      await withoutCache.set(`key-${i}`, { a: `a${i}`, b: i });
      await smallCache.set(`key-${i}`, { a: `a${i}`, b: i });
    }
  });

  after(() => removeTempDir(tempDirectory));

  test("without cache", async () => {
    for (let i = 0; i < KEYS; i++) {
      assert.deepEqual(await withoutCache.get(`key-${i}`), {
        a: `a${i}`,
        b: i,
      });
    }
  });

  test("with cache", async () => {
    for (let i = 0; i < KEYS; i++) {
      assert.deepEqual(await withCache.get(`key-${i}`), { a: `a${i}`, b: i });
    }
  });

  test("with a cache smaller than the data set", async () => {
    for (let i = 0; i < KEYS; i++) {
      assert.deepEqual(await smallCache.get(`key-${i}`), { a: `a${i}`, b: i });
    }
  });

  test("updates are reflected in the cache", async () => {
    for (let i = 0; i < KEYS; i++) {
      await withCache.set(`key-${i}`, { a: `b${i}`, b: i + 10 });
    }
    for (let i = 0; i < KEYS; i++) {
      assert.deepEqual(await withCache.get(`key-${i}`), {
        a: `b${i}`,
        b: i + 10,
      });
    }
  });

  test("deletes are reflected in the cache", async () => {
    assert.equal(await withCache.delete("key-0"), true);
    assert.equal(await withCache.exists("key-0"), false);
    assert.equal(await withCache.get("key-0"), undefined);
  });
});
