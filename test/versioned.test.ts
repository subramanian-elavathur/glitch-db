import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import GlitchDB, {
  type GlitchUnitemporalPartition,
  INFINITY_TIME,
  type UnitemporallyVersionedData,
} from "../src/index.ts";
import { createTempDir, removeTempDir } from "./helpers.ts";

interface TestData {
  song: string;
  artist: string;
  year: number;
  genre: string[];
  lengthInSeconds: number;
}

const GRAVITY: TestData = {
  song: "Gravity",
  artist: "John Mayer",
  year: 2005,
  genre: ["Rock", "Blues"],
  lengthInSeconds: 247,
};

const GRAVITY_2: TestData = {
  song: "Gravity2",
  artist: "John Mayerz",
  year: 2005,
  genre: ["Rock", "Blues"],
  lengthInSeconds: 247,
};

const DELICATE: TestData = {
  song: "Delicate",
  artist: "Damien Rice",
  year: 2002,
  genre: ["Folk", "Rock"],
  lengthInSeconds: 314,
};

const withoutTimestamps = <Type>({
  createdAt,
  deletedAt,
  ...rest
}: UnitemporallyVersionedData<Type>) => {
  assert.equal(typeof createdAt, "number");
  assert.equal(typeof deletedAt, "number");
  return rest;
};

describe("versioned", () => {
  let tempDirectory: string;
  let glitchDB: GlitchUnitemporalPartition<TestData>;

  before(async () => {
    tempDirectory = await createTempDir("versioned");
    glitchDB = new GlitchDB(tempDirectory, 0).getVersionedPartition<TestData>(
      "versioned",
      ["artist"],
    );
    await glitchDB.set("gravity", GRAVITY);
  });

  after(() => removeTempDir(tempDirectory));

  test("get", async () => {
    assert.deepEqual(await glitchDB.get("gravity"), GRAVITY);
    assert.deepEqual(await glitchDB.get("John Mayer"), GRAVITY);
  });

  test("set", async () => {
    assert.equal(
      await glitchDB.set("gravity", GRAVITY_2, { updatedBy: "sub-el" }),
      true,
    );
    assert.equal(
      await glitchDB.set("delicate", DELICATE, { updatedBy: "tadow" }),
      true,
    );
    assert.equal(await glitchDB.get("John Mayer"), undefined);
    assert.deepEqual(await glitchDB.get("John Mayerz"), GRAVITY_2);
  });

  test("get all versions", async () => {
    assert.deepEqual(
      (await glitchDB.getAllVersions("gravity"))?.map(withoutTimestamps),
      [
        { data: GRAVITY, version: 1 },
        { data: GRAVITY_2, version: 2, metadata: { updatedBy: "sub-el" } },
      ],
    );
    const delicateVersions = [
      { data: DELICATE, version: 1, metadata: { updatedBy: "tadow" } },
    ];
    assert.deepEqual(
      (await glitchDB.getAllVersions("Damien Rice"))?.map(withoutTimestamps),
      delicateVersions,
    );
    assert.deepEqual(
      (await glitchDB.getAllVersions("delicate"))?.map(withoutTimestamps),
      delicateVersions,
    );
    assert.equal(await glitchDB.getAllVersions("missing"), undefined);
  });

  test("get specific version", async () => {
    assert.deepEqual(await glitchDB.get("gravity"), GRAVITY_2);
    assert.deepEqual(await glitchDB.get("gravity", 1), GRAVITY);
    assert.deepEqual(await glitchDB.get("gravity", 2), GRAVITY_2);
    assert.deepEqual(await glitchDB.get("delicate", 1), DELICATE);
    assert.equal(await glitchDB.get("delicate", 46), undefined);
  });

  test("get version with audit", async () => {
    const gravity1 = await glitchDB.getVersion("gravity", 1);
    assert.ok(gravity1);
    assert.ok(gravity1.createdAt > 1);
    assert.ok(gravity1.createdAt <= gravity1.deletedAt);
    assert.notEqual(gravity1.deletedAt, INFINITY_TIME);
    assert.deepEqual(withoutTimestamps(gravity1), {
      data: GRAVITY,
      version: 1,
    });

    const delicate = await glitchDB.getVersion("delicate");
    assert.ok(delicate);
    assert.equal(delicate.deletedAt, INFINITY_TIME);
    assert.ok(delicate.createdAt > 1);
    assert.deepEqual(withoutTimestamps(delicate), {
      data: DELICATE,
      version: 1,
      metadata: { updatedBy: "tadow" },
    });
    assert.equal(await glitchDB.getVersion("gravity", 37), undefined);
  });

  test("keys", async () => {
    assert.deepEqual(await glitchDB.keys(), ["delicate", "gravity"]);
  });

  test("data", async () => {
    assert.deepEqual(await glitchDB.data(), {
      delicate: DELICATE,
      gravity: GRAVITY_2,
    });
  });

  test("delete", async () => {
    assert.equal(await glitchDB.delete("gravity"), true);
    assert.equal(await glitchDB.getAllVersions("gravity"), undefined);
    assert.equal(await glitchDB.get("gravity"), undefined);
    assert.equal(await glitchDB.get("John Mayerz"), undefined);
    assert.equal(await glitchDB.get("gravity", 1), undefined);
    assert.equal(await glitchDB.get("gravity", 2), undefined);
    assert.equal(await glitchDB.delete("gravity"), false);
  });

  test("with cache", async () => {
    const cached = new GlitchDB(tempDirectory).getVersionedPartition<TestData>(
      "cached",
    );
    await cached.set("song", GRAVITY);
    assert.deepEqual(await cached.get("song"), GRAVITY);
    await cached.set("song", GRAVITY_2);
    assert.deepEqual(await cached.get("song"), GRAVITY_2);
    assert.deepEqual(await cached.get("song", 1), GRAVITY);
    assert.deepEqual(await cached.data(), { song: GRAVITY_2 });
  });
});
