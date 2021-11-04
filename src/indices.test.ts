import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";

import GlitchDB, { GlitchPartition } from ".";

let tempDirectory: string;
const { before, test, after, sync } = group("Index");

let glitchDB: GlitchPartition<Cars>;

interface Cars {
  name: string;
  year: number;
  manufacturer: string;
}

const CACHE_SIZE = 100;

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch-index");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);
  glitchDB = new GlitchDB(tempDirectory, 0).getPartition<Cars>("index", [
    "name",
    "year",
    "unknown",
  ]);
  for (let i = 0; i < CACHE_SIZE; i++) {
    await glitchDB.set(`key-${i}`, {
      name: `model-${i}`,
      year: i,
      manufacturer: "Tesla",
    });
  }
  // reset GlitchDB to load data from index file
  glitchDB = new GlitchDB(tempDirectory, 0).getPartition<Cars>("index", [
    "name",
    "year",
  ]);
  context.log(`Glitch DB setup complete`);
  context.done();
});

sync(); // run tests one after the other

test("get by key and index", async (c) => {
  for (let i = 0; i < CACHE_SIZE; i++) {
    const expected: Cars = {
      name: `model-${i}`,
      year: i,
      manufacturer: "Tesla",
    };
    c.check(expected, await glitchDB.get(`key-${i}`));
    c.check(expected, await glitchDB.get(`model-${i}`));
    c.check(expected, await glitchDB.get(`${i}`));
  }
  c.done();
});

test("delete items", async (c) => {
  for (let i = 0; i < CACHE_SIZE / 2; i++) {
    await glitchDB.delete(`${i}`);
  }
  c.done();
});

test("get by key and index post delete", async (c) => {
  for (let i = CACHE_SIZE / 2 + 1; i < CACHE_SIZE; i++) {
    const expected: Cars = {
      name: `model-${i}`,
      year: i,
      manufacturer: "Tesla",
    };
    c.check(expected, await glitchDB.get(`key-${i}`));
    c.check(expected, await glitchDB.get(`model-${i}`));
    c.check(expected, await glitchDB.get(`${i}`));
  }
  c.done();
});

after(async (c) => {
  try {
    await fs.rmdir(tempDirectory, { recursive: true });
    c.log("Deleted temp directory after tests");
    c.done();
  } catch (e) {
    c.log(`Could not delete temp directory at: ${tempDirectory}`);
    c.done();
  } finally {
    tempDirectory = "";
  }
});
