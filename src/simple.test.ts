import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";

import GlitchDB, { AdditionalKeyGenerator, GlitchMultiDB } from ".";

let tempDirectory: string;
const { before, test, after, sync } = group("Simple");

let glitchDB: GlitchDB<string>;

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);
  const multiDB = new GlitchMultiDB(tempDirectory);
  const additionalKeyGenerator: AdditionalKeyGenerator<string> = (
    key,
    value
  ) => [`${key}~${value}`, value];
  glitchDB = multiDB.getDatabase<string>("master", additionalKeyGenerator);
  await glitchDB.set("key-1", "value-1");
  await glitchDB.set("key-2", "value-2");
  await glitchDB.set("key-3", "value-3");
  context.log(`Glitch DB setup with dummy data complete`);
  context.done();
});

sync(); // run tests one after the other

test("get api", async (c) => {
  c.check("value-1", await glitchDB.get("key-1")).done();
});

test("get api with additional keys", async (c) => {
  c.check("value-2", await glitchDB.get("value-2"))
    .check("value-2", await glitchDB.get("key-2~value-2"))
    .done();
});

test("exists api", async (c) => {
  c.check(true, await glitchDB.exists("key-1")).done();
});

test("reset api", async (c) => {
  await glitchDB.set("key-1", "value-4");
  c.check("value-4", await glitchDB.get("value-4"))
    .check("value-4", await glitchDB.get("key-1~value-4"))
    .check("value-4", await glitchDB.get("key-1"))
    .check(undefined, await glitchDB.get("value-1"))
    .check(undefined, await glitchDB.get("key-1~value-1"))
    .done();
});

test("unset api", async (c) => {
  await glitchDB.unset("key-3");
  c.check(undefined, await glitchDB.get("key-3")).done();
});

test("keys api", async (c) => {
  c.check(["key-1", "key-2"], await glitchDB.keys()).done();
});

test("keys api with additional keys", async (c) => {
  c.check(
    ["key-1", "key-1~value-4", "key-2", "key-2~value-2", "value-2", "value-4"],
    await glitchDB.keys(true)
  ).done();
});

test("data api", async (c) => {
  c.check(
    { "key-1": "value-4", "key-2": "value-2" },
    await glitchDB.data()
  ).done();
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
