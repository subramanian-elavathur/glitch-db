import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { after, before, describe, test } from "node:test";
import { list as listTarball } from "tar";
import GlitchDB, { type GlitchPartition } from "../src/index.ts";
import { createTempDir, removeTempDir } from "./helpers.ts";

describe("simple partition", () => {
  let tempDirectory: string;
  let multiDB: GlitchDB;
  let glitchDB: GlitchPartition<string>;

  before(async () => {
    tempDirectory = await createTempDir("simple");
    multiDB = new GlitchDB(tempDirectory, 0);
    glitchDB = multiDB.getPartition<string>("master");
    await glitchDB.set("key-1", "value-1");
    await glitchDB.set("key-2", "value-2");
    await glitchDB.set("key-3", "value-3");
  });

  after(() => removeTempDir(tempDirectory));

  test("get", async () => {
    assert.equal(await glitchDB.get("key-1"), "value-1");
    assert.equal(await glitchDB.get("missing"), undefined);
  });

  test("exists", async () => {
    assert.equal(await glitchDB.exists("key-1"), true);
    assert.equal(await glitchDB.exists("missing"), false);
  });

  test("set overwrites existing values", async () => {
    assert.equal(await glitchDB.set("key-1", "value-4"), true);
    assert.equal(await glitchDB.get("key-1"), "value-4");
    assert.equal(await glitchDB.get("value-1"), undefined);
  });

  test("delete", async () => {
    assert.equal(await glitchDB.delete("key-3"), true);
    assert.equal(await glitchDB.get("key-3"), undefined);
    assert.equal(await glitchDB.exists("key-3"), false);
    assert.equal(await glitchDB.delete("key-3"), false);
  });

  test("keys", async () => {
    assert.deepEqual(await glitchDB.keys(), ["key-1", "key-2"]);
  });

  test("data", async () => {
    assert.deepEqual(await glitchDB.data(), {
      "key-1": "value-4",
      "key-2": "value-2",
    });
  });

  test("getPartitionByName returns the registered partition", () => {
    assert.equal(multiDB.getPartitionByName("master"), glitchDB);
    assert.throws(() => multiDB.getPartitionByName("unknown"), {
      message: "Glitch Partition with name unknown not found",
    });
  });

  test("backup", async () => {
    const backupDirectory = await createTempDir("backup");
    try {
      const backupPath = multiDB.backup(backupDirectory);
      assert.equal(path.dirname(backupPath), backupDirectory);
      const entries: string[] = [];
      listTarball({
        file: backupPath,
        sync: true,
        onReadEntry: (entry) => entries.push(entry.path),
      });
      assert.ok(entries.includes("./master/key-1.json"));
      assert.ok(entries.includes("./master/key-2.json"));
    } finally {
      await removeTempDir(backupDirectory);
    }
  });
});

describe("partition edge cases", () => {
  let tempDirectory: string;
  let glitchDB: GlitchDB;

  before(async () => {
    tempDirectory = await createTempDir("edge-cases");
    glitchDB = new GlitchDB(tempDirectory);
  });

  after(() => removeTempDir(tempDirectory));

  test("falsy values can be stored, read and deleted", async () => {
    const partition = glitchDB.getPartition<number | string | boolean | null>(
      "falsy",
    );
    const values = { zero: 0, empty: "", false: false, null: null };
    for (const [key, value] of Object.entries(values)) {
      assert.equal(await partition.set(key, value), true);
      assert.equal(await partition.get(key), value);
    }
    assert.deepEqual(await partition.data(), values);
    for (const key of Object.keys(values)) {
      assert.equal(await partition.delete(key), true, `delete ${key}`);
      assert.equal(await partition.exists(key), false);
    }
  });

  test("keys only lists stored keys, verbatim", async () => {
    const partition = glitchDB.getPartition<string>("keys");
    await partition.set("b.json.backup", "first");
    await partition.set("a", "second");
    const partitionDirectory = path.join(tempDirectory, "keys");
    await fs.writeFile(path.join(partitionDirectory, "notes.txt"), "ignored");
    await fs.mkdir(path.join(partitionDirectory, "nested.json"));
    assert.deepEqual(await partition.keys(), ["a", "b.json.backup"]);
  });

  test("keys cannot escape the partition directory", async () => {
    const partition = glitchDB.getPartition<string>("unsafe", ["name"]);
    for (const key of [
      "../escaped",
      "nested/key",
      "back\\slash",
      "__index__",
    ]) {
      assert.equal(await partition.set(key, "value"), false, `set ${key}`);
      assert.equal(await partition.get(key), undefined);
      assert.equal(await partition.exists(key), false);
    }
    await assert.rejects(fs.stat(path.join(tempDirectory, "escaped.json")));
  });

  test("keys and indices can share names with Object.prototype members", async () => {
    const partition = glitchDB.getPartition<{ name: string }>("prototype", [
      "name",
    ]);
    await partition.set("toString", { name: "constructor" });
    assert.deepEqual(await partition.get("toString"), { name: "constructor" });
    assert.deepEqual(await partition.get("constructor"), {
      name: "constructor",
    });
    assert.equal(await partition.get("hasOwnProperty"), undefined);
  });

  test("cached values are isolated from callers", async () => {
    const partition = glitchDB.getPartition<{ tags: string[] }>("isolated");
    const value = { tags: ["original"] };
    await partition.set("key", value);
    value.tags.push("mutated after set");
    const read = await partition.get("key");
    assert.deepEqual(read, { tags: ["original"] });
    read?.tags.push("mutated after get");
    assert.deepEqual(await partition.get("key"), { tags: ["original"] });
  });

  test("cached and uncached reads return the same data", async () => {
    const cached = glitchDB.getPartition<{ when: unknown }>(
      "serialized",
      [],
      10,
    );
    const uncached = glitchDB.getPartition<{ when: unknown }>(
      "serialized",
      [],
      0,
    );
    await cached.set("epoch", { when: new Date(0) });
    const expected = { when: "1970-01-01T00:00:00.000Z" };
    assert.deepEqual(await cached.get("epoch"), expected);
    assert.deepEqual(await uncached.get("epoch"), expected);
  });

  test("concurrent writes keep every index entry", async () => {
    const writes = 50;
    const partition = glitchDB.getPartition<{ name: string }>("concurrent", [
      "name",
    ]);
    const results = await Promise.all(
      Array.from({ length: writes }, (_, i) =>
        partition.set(`key-${i}`, { name: `name-${i}` }),
      ),
    );
    assert.ok(results.every(Boolean));
    const index = JSON.parse(
      await fs.readFile(
        path.join(tempDirectory, "concurrent", "__index__.json"),
        "utf8",
      ),
    ) as Record<string, string>;
    assert.equal(Object.keys(index).length, writes);
    const reopened = new GlitchDB(tempDirectory).getPartition<{ name: string }>(
      "concurrent",
      ["name"],
    );
    for (let i = 0; i < writes; i++) {
      assert.deepEqual(await reopened.get(`name-${i}`), { name: `name-${i}` });
    }
  });

  test("partition path must be a directory", async () => {
    await fs.writeFile(path.join(tempDirectory, "file"), "not a directory");
    const partition = glitchDB.getPartition<string>("file");
    await assert.rejects(partition.get("key"), {
      message: `Specified path ${path.join(tempDirectory, "file")} exists but is not a directory`,
    });
  });
});
