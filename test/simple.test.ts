import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";

import GlitchDB, { GlitchPartition } from "../src";

let tempDirectory: string;
const { before, test, after, sync } = group("Simple");

let glitchDB: GlitchPartition<string>;
let multiDB: GlitchDB;

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);
  multiDB = new GlitchDB(tempDirectory, 0);
  glitchDB = multiDB.getPartition<string>("master");
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

test("exists api", async (c) => {
  c.check(true, await glitchDB.exists("key-1")).done();
});

test("reset api", async (c) => {
  await glitchDB.set("key-1", "value-4");
  c.check(undefined, await glitchDB.get("value-1"))
    .check(undefined, await glitchDB.get("key-1~value-1"))
    .done();
});

test("delete api", async (c) => {
  await glitchDB.delete("key-3");
  c.check(undefined, await glitchDB.get("key-3")).done();
});

test("keys api", async (c) => {
  c.check(["key-1", "key-2"], await glitchDB.keys()).done();
});

test("data api", async (c) => {
  c.check(
    { "key-1": "value-4", "key-2": "value-2" },
    await glitchDB.data()
  ).done();
});

after(async (c) => {
  try {
    const backupPath = multiDB.backup("./");
    console.log(`Backed up data to ${backupPath}`);
    await fs.rm(tempDirectory, { recursive: true });
    c.log("Deleted temp directory after tests");
    await fs.rm(backupPath);
    c.log("Deleted backup");
    c.done();
  } catch (e) {
    c.log(`Could not delete temp directory at: ${tempDirectory}`);
    c.done();
  } finally {
    tempDirectory = "";
  }
});
