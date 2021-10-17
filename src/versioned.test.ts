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

const CACHE_SIZE = 10000;

let glitchDB: GlitchVersionedPartition<TestData>;
let tempDirectory: string;

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch-versioned");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);
  glitchDB = new GlitchDB(tempDirectory).getVersionedPartition<TestData>(
    "versioned",
    (_, value) => [value.artist],
    CACHE_SIZE
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
  c.done();
});

test("get all versions", async (c) => {
  await c.snapshot(
    "all versions",
    (
      await glitchDB.getAllVersions("gravity")
    ).map((each) => ({ ...each, updatedAt: undefined }))
  );
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
