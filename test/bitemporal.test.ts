import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";

import GlitchDB, { GlitchBitemporalPartition } from "../src";

const { before, test, after, sync } = group("bitemporal");

interface TestData {
  song: string;
  artist: string;
  year: number;
  genre: string[];
  lengthInSeconds: number;
}

let glitchDB: GlitchBitemporalPartition<TestData>;
let tempDirectory: string;

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch-bitemporal");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);
  glitchDB = new GlitchDB(tempDirectory, 0).getBitemporalPartition<TestData>(
    "bitemporal",
    ["artist"]
  );
  await glitchDB.set("gravity", {
    song: "Gravity",
    artist: "John Mayer",
    year: 2005,
    genre: ["Rock", "Blues"],
    lengthInSeconds: 247,
  });
  setTimeout(async () => {
    await glitchDB.set("gravity", {
      song: "Gravity2",
      artist: "John Mayer",
      year: 2004,
      genre: ["Rockz", "Bluesz"],
      lengthInSeconds: 247,
    });
    context.done();
  }, 2000);
});

sync(); // run tests one after the other

test("get", async (c) => {
  // await c.snapshot("get latest version", await glitchDB.get("gravity"), true);
  // await c.snapshot(
  //   "get latest version by key",
  //   await glitchDB.get("John Mayer"),
  //   true
  // );
  c.done();
});

after(async (c) => {
  try {
    await fs.rm(tempDirectory, { recursive: true });
    c.log("Deleted temp directory after tests");
  } catch (e) {
    c.log(`Could not delete temp directory at: ${tempDirectory}`);
  } finally {
    c.done();
    tempDirectory = "";
  }
});
