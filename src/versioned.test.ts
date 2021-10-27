import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";

import GlitchDB, { GlitchVersionedPartition } from ".";

const { before, test, after, sync } = group("versioned");

interface TestData {
  song: string;
  artist: string;
  year: number;
  genre: string[];
  lengthInSeconds: number;
}

let glitchDB: GlitchVersionedPartition<TestData>;
let tempDirectory: string;

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch-versioned");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);
  glitchDB = new GlitchDB(tempDirectory, 5).getVersionedPartition<TestData>(
    "versioned",
    ["artist"]
  );
  await glitchDB.set("gravity", {
    song: "Gravity",
    artist: "John Mayer",
    year: 2005,
    genre: ["Rock", "Blues"],
    lengthInSeconds: 247,
  });
  context.done();
});

sync(); // run tests one after the other

test("get", async (c) => {
  await c.snapshot("get latest version", await glitchDB.get("gravity"));
  await c.snapshot(
    "get latest version by key",
    await glitchDB.get("John Mayer")
  );
  c.done();
});

test("set", async (c) => {
  await glitchDB.set(
    "gravity",
    {
      song: "Gravity2",
      artist: "John Mayerz",
      year: 2005,
      genre: ["Rock", "Blues"],
      lengthInSeconds: 247,
    },
    { updatedBy: "sub-el" }
  );
  await glitchDB.set(
    "delicate",
    {
      song: "Delicate",
      artist: "Damien Rice",
      year: 2002,
      genre: ["Folk", "Rock"],
      lengthInSeconds: 314,
    },
    { updatedBy: "tadow" }
  );
  c.done();
});

test("get all versions", async (c) => {
  await c.snapshot(
    "all gravity versions",
    (
      await glitchDB.getAllVersions("gravity")
    ).map((each) => ({ ...each, createdAt: undefined }))
  );
  await c.snapshot(
    "all delicate versions by key",
    (
      await glitchDB.getAllVersions("Damien Rice")
    ).map((each) => ({ ...each, createdAt: undefined }))
  );
  await c.snapshot(
    "all delicate versions",
    (
      await glitchDB.getAllVersions("delicate")
    ).map((each) => ({ ...each, createdAt: undefined }))
  );
  c.done();
});

test("get specific version", async (c) => {
  await c.snapshot("get gravity latest version", await glitchDB.get("gravity"));
  await c.snapshot("get gravity version 2", await glitchDB.get("gravity", 2));
  await c.snapshot("get delicate version 1", await glitchDB.get("delicate", 1));
  await c.check(undefined, await glitchDB.get("delicate", 46));
  c.done();
});

test("get version with audit", async (c) => {
  await c.snapshot("get gravity version 1", {
    ...(await glitchDB.getVersionWithAudit("gravity", 1)),
    createdAt: undefined,
  });
  await c.snapshot("get delicate version 1", {
    ...(await glitchDB.getVersionWithAudit("delicate")),
    createdAt: undefined,
  });
  await c.check(undefined, await glitchDB.getVersionWithAudit("gravity", 37));
  c.done();
});

test("keys", async (c) => {
  await c.snapshot("get all keys", await glitchDB.keys());
  c.done();
});

test("data", async (c) => {
  await c.snapshot("get all data", await glitchDB.data());
  c.done();
});

test("delete", async (c) => {
  await glitchDB.delete("gravity");
  c.check([], await glitchDB.getAllVersions("gravity"));
  c.check(undefined, await glitchDB.get("gravity"));
  c.check(undefined, await glitchDB.get("John Mayerz"));
  c.check(undefined, await glitchDB.get("gravity", 1));
  c.check(undefined, await glitchDB.get("gravity", 2));
  await glitchDB.delete("gravity"); // test deleting key that does not exist
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
