import * as fs from "fs/promises";
import { memoryUsage } from "process";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";

import GlitchDB, { GlitchPartition } from ".";
import Context from "good-vibes/typings/Context";

const { before, test, after, sync } = group("Cached");

interface TestData {
  a: string;
  b: number;
}

const CACHE_SIZE = 10;

let glitchDBWithCache: GlitchPartition<TestData>;
let glitchDBNoCache: GlitchPartition<TestData>;

let tempDirectoryWithCache: string;
let tempDirectoryNoCache: string;

const printMemoryStats = (c: Context): void => {
  c.log(`Memory Stats`);
  const memStats = memoryUsage();
  c.log(`Total Memory: ${Math.ceil(memStats.heapTotal / 1048576)} MB`);
  c.log(`Used Memory: ${Math.ceil(memStats.heapUsed / 1048576)} MB`);
};

before(async (context) => {
  tempDirectoryWithCache = path.join(os.tmpdir(), "glitch-cached");
  context.log(`Created temp directory for tests at: ${tempDirectoryWithCache}`);
  glitchDBWithCache = new GlitchDB(
    tempDirectoryWithCache
  ).getPartition<TestData>("cached", null, CACHE_SIZE);

  tempDirectoryNoCache = path.join(os.tmpdir(), "glitch-no-cache");
  context.log(`Created temp directory for tests at: ${tempDirectoryNoCache}`);
  glitchDBNoCache = new GlitchDB(
    tempDirectoryNoCache,
    0
  ).getPartition<TestData>("no-cache");

  for (let i = 0; i < CACHE_SIZE; i++) {
    await glitchDBWithCache.set(`key-${i}`, { a: `a${i}`, b: i });
    await glitchDBNoCache.set(`key-${i}`, { a: `a${i}`, b: i });
  }
  context.log(`Glitch DB setup complete with ${CACHE_SIZE} keys`);
  printMemoryStats(context);
  context.done();
});

sync(); // run tests one after the other

test("without cache", async (c) => {
  for (let i = 0; i < CACHE_SIZE; i++) {
    c.check({ a: `a${i}`, b: i }, await glitchDBNoCache.get(`key-${i}`));
  }
  c.log("Loaded data into cache");
  printMemoryStats(c);
  c.done();
});

test("with cache", async (c) => {
  for (let i = 0; i < CACHE_SIZE; i++) {
    c.check({ a: `a${i}`, b: i }, await glitchDBWithCache.get(`key-${i}`));
  }
  printMemoryStats(c);
  c.done();
});

test("update cache", async (c) => {
  for (let i = 0; i < CACHE_SIZE; i++) {
    await glitchDBWithCache.set(`key-${i}`, { a: `b${i}`, b: i + 10 });
  }
  c.check("passthrough", "passthrough");
  printMemoryStats(c);
  c.done();
});

test("check updated cache", async (c) => {
  for (let i = 0; i < CACHE_SIZE; i++) {
    c.check({ a: `b${i}`, b: i + 10 }, await glitchDBWithCache.get(`key-${i}`));
  }
  printMemoryStats(c);
  c.done();
});

after(async (c) => {
  try {
    await fs.rmdir(tempDirectoryWithCache, { recursive: true });
    await fs.rmdir(tempDirectoryNoCache, { recursive: true });
    c.log("Deleted temp directory after tests");
  } catch (e) {
    c.log(`Could not delete temp directory at: ${tempDirectoryWithCache}`);
    c.log(`Could not delete temp directory at: ${tempDirectoryNoCache}`);
  } finally {
    c.done();
    tempDirectoryWithCache = "";
    tempDirectoryNoCache = "";
  }
});
