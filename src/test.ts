import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import run, { group } from "good-vibes";

import GlitchDB, { AdditionalKeyGenerator, GlitchMultiDB } from "./";

let tempDirectory: string;
const { before, test, after } = group("Glitch DB");

let glitchDB: GlitchDB<string>;

before(async (done, log) => {
  tempDirectory = path.join(os.tmpdir(), "glitch");
  log(`Created temp directory for tests at: ${tempDirectory}`);
  const multiDB = new GlitchMultiDB(tempDirectory);
  const additionalKeyGenerator: AdditionalKeyGenerator<string> = (
    key,
    value
  ) => [`${key}~${value}`, value];
  glitchDB = multiDB.getDatabase<string>("master", additionalKeyGenerator);
  await glitchDB.set("key-1", "value-1");
  await glitchDB.set("key-2", "value-2");
  await glitchDB.set("key-3", "value-3");
  log(`Glitch DB setup with dummy data complete`);
  done();
});

test("Test retrieve", async (v) => {
  v.check("value-1", await glitchDB.get("key-1")).done();
});

test("Test retrieve using additional keys", async (v) => {
  v.check("value-2", await glitchDB.get("value-2"))
    .check("value-2", await glitchDB.get("key-2~value-2"))
    .done();
});

test("Test exists", async (v) => {
  v.check(true, await glitchDB.exists("key-1")).done();
});

test("Test unset", async (v) => {
  await glitchDB.unset("key-3");
  v.check(undefined, await glitchDB.get("key-3")).done();
});

after(async (done, log) => {
  try {
    await fs.rmdir(tempDirectory, { recursive: true });
    log("Deleted temp directory after tests");
    done();
  } catch (e) {
    log(`Could not delete temp directory at: ${tempDirectory}`);
    done();
  } finally {
    tempDirectory = "";
  }
});

run();
