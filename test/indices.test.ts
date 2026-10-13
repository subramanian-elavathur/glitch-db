import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import GlitchDB, { type GlitchPartition } from "../src/index.ts";
import { createTempDir, removeTempDir } from "./helpers.ts";

interface Car {
  name: string;
  year: number;
  manufacturer: string;
  specs?: { engine: string };
  tags?: string[];
}

const RECORDS = 100;

const car = (i: number): Car => ({
  name: `model-${i}`,
  year: i,
  manufacturer: "Tesla",
});

describe("indices", () => {
  let tempDirectory: string;
  let glitchDB: GlitchPartition<Car>;

  before(async () => {
    tempDirectory = await createTempDir("index");
    const writer = new GlitchDB(tempDirectory, 0).getPartition<Car>("index", [
      "name",
      "year",
      "unknown",
    ]);
    for (let i = 0; i < RECORDS; i++) {
      await writer.set(`key-${i}`, car(i));
    }
    // a new instance loads the index from disk
    glitchDB = new GlitchDB(tempDirectory, 0).getPartition<Car>("index", [
      "name",
      "year",
    ]);
  });

  after(() => removeTempDir(tempDirectory));

  test("get by key and index", async () => {
    for (let i = 0; i < RECORDS; i++) {
      assert.deepEqual(await glitchDB.get(`key-${i}`), car(i));
      assert.deepEqual(await glitchDB.get(`model-${i}`), car(i));
      assert.deepEqual(await glitchDB.get(`${i}`), car(i));
    }
  });

  test("delete items by index", async () => {
    for (let i = 0; i < RECORDS / 2; i++) {
      assert.equal(await glitchDB.delete(`${i}`), true);
    }
    for (let i = 0; i < RECORDS / 2; i++) {
      assert.equal(await glitchDB.get(`key-${i}`), undefined);
      assert.equal(await glitchDB.get(`model-${i}`), undefined);
      assert.equal(await glitchDB.get(`${i}`), undefined);
    }
  });

  test("get by key and index post delete", async () => {
    for (let i = RECORDS / 2; i < RECORDS; i++) {
      assert.deepEqual(await glitchDB.get(`key-${i}`), car(i));
      assert.deepEqual(await glitchDB.get(`model-${i}`), car(i));
      assert.deepEqual(await glitchDB.get(`${i}`), car(i));
    }
  });

  test("indices follow updated values", async () => {
    await glitchDB.set("key-99", { ...car(99), name: "renamed" });
    assert.equal(await glitchDB.get("model-99"), undefined);
    assert.deepEqual(await glitchDB.get("renamed"), {
      ...car(99),
      name: "renamed",
    });
  });

  test("indices support nested paths", async () => {
    const partition = new GlitchDB(tempDirectory, 0).getPartition<Car>(
      "nested",
      ["specs.engine", "tags[1]", "specs"],
    );
    const roadster = {
      ...car(1),
      specs: { engine: "electric" },
      tags: ["fast", "red"],
    };
    await partition.set("roadster", roadster);
    assert.deepEqual(await partition.get("electric"), roadster);
    assert.deepEqual(await partition.get("red"), roadster);
    assert.equal(await partition.get("fast"), undefined);
    // objects are not valid index values
    assert.equal(await partition.get("[object Object]"), undefined);
  });

  test("data is not affected by index values matching other keys", async () => {
    const partition = new GlitchDB(tempDirectory, 0).getPartition<Car>(
      "shadowed",
      ["name"],
    );
    await partition.set("first", { ...car(1), name: "second" });
    await partition.set("second", car(2));
    assert.deepEqual(await partition.data(), {
      first: { ...car(1), name: "second" },
      second: car(2),
    });
  });

  test("deleting a key keeps index entries claimed by other keys", async () => {
    const partition = new GlitchDB(tempDirectory, 0).getPartition<Car>(
      "shared",
      ["manufacturer"],
    );
    await partition.set("first", car(1));
    await partition.set("second", car(2));
    assert.deepEqual(await partition.get("Tesla"), car(2));
    await partition.delete("first");
    assert.deepEqual(await partition.get("Tesla"), car(2));
  });
});
