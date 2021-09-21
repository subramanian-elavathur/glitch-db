import * as fs from "fs/promises";
import { memoryUsage } from "process";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";

import GlitchDB, { GlitchMultiDB } from ".";
import Context from "good-vibes/typings/Context";

let tempDirectory: string;
const { before, test, after, sync } = group("Cached");

interface TestData {
  a: string;
  b: number;
}

const CACHE_SIZE = 10000;

let glitchDB: GlitchDB<TestData>;

const printMemoryStats = (c: Context): void => {
  c.log(`Memory Stats`);
  const memStats = memoryUsage();
  c.log(`Total Memory: ${Math.ceil(memStats.heapTotal / 1048576)} MB`);
  c.log(`Used Memory: ${Math.ceil(memStats.heapUsed / 1048576)} MB`);
};

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch-cached");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);
  const multiDB = new GlitchMultiDB(tempDirectory);
  glitchDB = multiDB.getDatabase<TestData>("cached", null, CACHE_SIZE);
  for (let i = 0; i < CACHE_SIZE; i++) {
    await glitchDB.set(`key-${i}`, { a: `a${i}`, b: i });
  }
  context.log(`Glitch DB setup complete with ${CACHE_SIZE} keys`);
  printMemoryStats(context);
  context.done();
});

sync(); // run tests one after the other

test("without cache", async (c) => {
  for (let i = 0; i < CACHE_SIZE; i++) {
    c.check({ a: `a${i}`, b: i }, await glitchDB.get(`key-${i}`));
  }
  c.log("Loaded data into cache");
  printMemoryStats(c);
  c.done();
});

test("with cache", async (c) => {
  for (let i = 0; i < CACHE_SIZE; i++) {
    c.check({ a: `a${i}`, b: i }, await glitchDB.get(`key-${i}`));
  }
  printMemoryStats(c);
  c.done();
});

after(async (c) => {
  try {
    await fs.rmdir(tempDirectory, { recursive: true });
    c.log("Deleted temp directory after tests");
  } catch (e) {
    c.log(`Could not delete temp directory at: ${tempDirectory}`);
  } finally {
    c.done();
    tempDirectory = "";
  }
});
