import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";
import GlitchDB, { GlitchBitemporalPartition } from "../src";
import { BitemporallyVersionedData } from "./GlitchBitemporalPartition";
import { INFINITY_TIME } from "../src/constants";

const { before, test, after, sync } = group("bitemporal");

const removeCDTimestampsRaw = (data: BitemporallyVersionedData<unknown>) => ({
  ...data,
  createdAt: undefined,
  deletedAt: data.deletedAt === INFINITY_TIME ? INFINITY_TIME : -1,
});

const removeCDTimestamps = (
  data:
    | BitemporallyVersionedData<unknown>
    | BitemporallyVersionedData<unknown>[]
) => {
  return Array.isArray(data)
    ? data.map(removeCDTimestampsRaw)
    : removeCDTimestampsRaw(data);
};

interface TestData {
  song: string;
  artist: string;
  year: number;
  genre: string[];
  lengthInSeconds: number;
}

let glitchDB: GlitchBitemporalPartition<TestData>;
let tempDirectory: string;
let timeAtFirstInsert: number;
const FIXED_TIMESTAMP = 6893;

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch-bitemporal");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);
  glitchDB = new GlitchDB(tempDirectory, 0).getBitemporalPartition<TestData>(
    "bitemporal",
    ["artist"]
  );
  context.log("Setting first value");
  await glitchDB.set("gravity", {
    song: "Gravity",
    artist: "John Mayer",
    year: 2005,
    genre: ["Rock", "Blues"],
    lengthInSeconds: 247,
  });
  await glitchDB.set(
    "ocean",
    {
      song: "Ocean",
      artist: "John Butler",
      year: 2012,
      genre: ["Classical"],
      lengthInSeconds: 724,
    },
    FIXED_TIMESTAMP
  );
  timeAtFirstInsert = new Date().valueOf();
  setTimeout(async () => {
    context.log("Setting second value");
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

sync();

test("get", async (c) => {
  await c.snapshot("get latest version", await glitchDB.get("gravity"));
  await c.snapshot(
    "get latest version by key",
    await glitchDB.get("John Mayer")
  );
  await c.snapshot(
    "get older version",
    await glitchDB.get("gravity", timeAtFirstInsert)
  );
  const resultBeforeFirstInsertion = await glitchDB.get(
    "gravity",
    timeAtFirstInsert - 1000 // less flaky this way
  );
  c.check(undefined, resultBeforeFirstInsertion);
  c.check(undefined, await glitchDB.get("invalid"));
  c.done();
});

test("getVersion", async (c) => {
  await c.snapshot(
    "get latest version",
    removeCDTimestamps(await glitchDB.getVersion("ocean"))
  );
  await c.snapshot(
    "get latest version by key",
    removeCDTimestamps(await glitchDB.getVersion("John Butler"))
  );
  const resultBeforeFirstInsertion = await glitchDB.get(
    "ocean",
    FIXED_TIMESTAMP - 500
  );
  c.check(undefined, resultBeforeFirstInsertion);
  c.done();
});

test("set", async (c) => {
  await glitchDB.set(
    "ocean",
    {
      song: "Titanic",
      artist: "John Butler",
      year: 2001,
      genre: ["Rock"],
      lengthInSeconds: 500,
    },
    1,
    500
  );
  await c.snapshot("get latest version", await glitchDB.get("ocean"));
  await c.snapshot("get older version", await glitchDB.get("ocean", 250));
  c.check(undefined, await glitchDB.get("ocean", 0));
  c.check(undefined, await glitchDB.get("ocean", 2000));
  try {
    await glitchDB.set(
      "ocean",
      {
        song: "Titanic",
        artist: "John Butler",
        year: 2001,
        genre: ["Rock"],
        lengthInSeconds: 500,
      },
      50,
      25
    );
  } catch (error) {
    c.check(
      "Valid To cannot be less than or equal to Valid From",
      error.message
    );
  }
  c.done();
});

test("getAllVersions", async (c) => {
  await glitchDB.set(
    "ocean",
    {
      song: "Highway to hell",
      artist: "John Butler",
      year: 2001,
      genre: ["Rock"],
      lengthInSeconds: 500,
    },
    500,
    7895
  );
  await glitchDB.set(
    "ocean",
    {
      song: "You shook me all night long",
      artist: "John Butler",
      year: 2001,
      genre: ["Rock"],
      lengthInSeconds: 500,
    },
    7895
  );
  await c.snapshot(
    "get all versions",
    removeCDTimestamps(await glitchDB.getAllVersions("ocean"))
  );
  await c.snapshot("get 7895 version", await glitchDB.get("ocean", 7895));
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
