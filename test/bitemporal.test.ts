import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import GlitchDB, {
  type BitemporallyVersionedData,
  type GlitchBitemporalPartition,
  INFINITY_TIME,
} from "../src/index.ts";
import { createTempDir, removeTempDir } from "./helpers.ts";

interface TestData {
  song: string;
  artist: string;
  year: number;
  genre: string[];
  lengthInSeconds: number;
}

const song = (name: string, artist = "John Butler", year = 2001): TestData => ({
  song: name,
  artist,
  year,
  genre: ["Rock"],
  lengthInSeconds: 500,
});

const OCEAN: TestData = {
  song: "Ocean",
  artist: "John Butler",
  year: 2012,
  genre: ["Classical"],
  lengthInSeconds: 724,
};

const GRAVITY: TestData = {
  song: "Gravity",
  artist: "John Mayer",
  year: 2005,
  genre: ["Rock", "Blues"],
  lengthInSeconds: 247,
};

const GRAVITY_2: TestData = {
  song: "Gravity2",
  artist: "John Mayer",
  year: 2004,
  genre: ["Rockz", "Bluesz"],
  lengthInSeconds: 247,
};

const FIXED_TIMESTAMP = 6893;

// strips the transaction timestamps which depend on when the test runs
const summarize = <Type>(version: BitemporallyVersionedData<Type>) => ({
  data: version.data,
  live: version.deletedAt === INFINITY_TIME,
  validFrom: version.validFrom,
  validTo: version.validTo,
});

const liveVersions = async <Type>(
  partition: GlitchBitemporalPartition<Type>,
  key: string,
) =>
  ((await partition.getAllVersions(key)) ?? [])
    .map(summarize)
    .filter((version) => version.live)
    .sort((a, b) => a.validFrom - b.validFrom)
    .map(({ data, validFrom, validTo }) => ({ data, validFrom, validTo }));

describe("bitemporal", () => {
  let tempDirectory: string;
  let multiDB: GlitchDB;
  let glitchDB: GlitchBitemporalPartition<TestData>;
  let timeAtFirstInsert: number;

  before(async () => {
    tempDirectory = await createTempDir("bitemporal");
    multiDB = new GlitchDB(tempDirectory, 0);
    glitchDB = multiDB.getBitemporalPartition<TestData>("bitemporal", [
      "artist",
    ]);
    await glitchDB.set("gravity", GRAVITY);
    await glitchDB.set("ocean", OCEAN, FIXED_TIMESTAMP);
    timeAtFirstInsert = Date.now();
    await sleep(25); // ensures the second version is valid from a later time
    await glitchDB.set("gravity", GRAVITY_2);
  });

  after(() => removeTempDir(tempDirectory));

  test("get", async () => {
    assert.deepEqual(await glitchDB.get("gravity"), GRAVITY_2);
    assert.deepEqual(await glitchDB.get("John Mayer"), GRAVITY_2);
    assert.deepEqual(await glitchDB.get("gravity", timeAtFirstInsert), GRAVITY);
    assert.equal(
      await glitchDB.get("gravity", timeAtFirstInsert - 1000),
      undefined,
    );
    assert.equal(await glitchDB.get("invalid"), undefined);
  });

  test("getVersion", async () => {
    const expected = {
      data: OCEAN,
      live: true,
      validFrom: FIXED_TIMESTAMP,
      validTo: INFINITY_TIME,
    };
    const ocean = await glitchDB.getVersion("ocean");
    assert.ok(ocean);
    assert.deepEqual(summarize(ocean), expected);
    const byIndex = await glitchDB.getVersion("John Butler");
    assert.ok(byIndex);
    assert.deepEqual(summarize(byIndex), expected);
    assert.equal(await glitchDB.get("ocean", FIXED_TIMESTAMP - 500), undefined);
  });

  test("set", async () => {
    assert.equal(await glitchDB.set("ocean", song("Titanic"), 1, 500), true);
    assert.deepEqual(await glitchDB.get("ocean"), OCEAN);
    assert.deepEqual(await glitchDB.get("ocean", 250), song("Titanic"));
    assert.equal(await glitchDB.get("ocean", 0), undefined);
    assert.equal(await glitchDB.get("ocean", 2000), undefined);
  });

  test("set rejects an empty validity period", async () => {
    const before = await glitchDB.getAllVersions("ocean");
    assert.equal(await glitchDB.set("ocean", song("Titanic"), 50, 25), false);
    assert.equal(await glitchDB.set("ocean", song("Titanic"), 50, 50), false);
    assert.deepEqual(await glitchDB.getAllVersions("ocean"), before);
  });

  test("getAllVersions", async () => {
    await glitchDB.set("ocean", song("Highway to hell"), 500, 7895);
    await glitchDB.set("ocean", song("You shook me all night long"), 7895);
    const versions = await glitchDB.getAllVersions("ocean");
    assert.ok(versions);
    assert.deepEqual(versions.map(summarize), [
      { data: OCEAN, live: false, validFrom: 6893, validTo: INFINITY_TIME },
      { data: song("Titanic"), live: true, validFrom: 1, validTo: 500 },
      { data: OCEAN, live: false, validFrom: 7895, validTo: INFINITY_TIME },
      {
        data: song("Highway to hell"),
        live: true,
        validFrom: 500,
        validTo: 7895,
      },
      {
        data: song("You shook me all night long"),
        live: true,
        validFrom: 7895,
        validTo: INFINITY_TIME,
      },
    ]);
    assert.deepEqual(
      await glitchDB.get("ocean", 7895),
      song("You shook me all night long"),
    );
  });

  test("delete", async () => {
    assert.equal(await glitchDB.delete("ocean"), true);
    assert.equal(await glitchDB.getAllVersions("ocean"), undefined);
    assert.equal(await glitchDB.get("John Butler"), undefined);
    assert.equal(await glitchDB.delete("ocean"), false);
  });

  describe("milestoning", () => {
    const A = song("A");
    const B = song("B");
    let partition: GlitchBitemporalPartition<TestData>;

    before(() => {
      partition = multiDB.getBitemporalPartition<TestData>("milestoning");
    });

    test("a version inside an existing version splits it", async () => {
      await partition.set("inside", A, 100);
      await partition.set("inside", B, 200, 300);
      assert.deepEqual(await liveVersions(partition, "inside"), [
        { data: A, validFrom: 100, validTo: 200 },
        { data: B, validFrom: 200, validTo: 300 },
        { data: A, validFrom: 300, validTo: INFINITY_TIME },
      ]);
      assert.deepEqual(await partition.get("inside"), A);
      assert.deepEqual(await partition.get("inside", 150), A);
      assert.deepEqual(await partition.get("inside", 250), B);
      assert.deepEqual(await partition.get("inside", 350), A);
    });

    test("a version overlapping the start of an existing version", async () => {
      await partition.set("leading", A, 100, 200);
      await partition.set("leading", B, 50, 150);
      assert.deepEqual(await liveVersions(partition, "leading"), [
        { data: B, validFrom: 50, validTo: 150 },
        { data: A, validFrom: 150, validTo: 200 },
      ]);
    });

    test("a version overlapping the end of an existing version", async () => {
      await partition.set("trailing", A, 100, 200);
      await partition.set("trailing", B, 150, 250);
      assert.deepEqual(await liveVersions(partition, "trailing"), [
        { data: A, validFrom: 100, validTo: 150 },
        { data: B, validFrom: 150, validTo: 250 },
      ]);
    });

    test("a version covering an existing version replaces it", async () => {
      await partition.set("covering", A, 100, 200);
      await partition.set("covering", B, 50, 250);
      assert.deepEqual(await liveVersions(partition, "covering"), [
        { data: B, validFrom: 50, validTo: 250 },
      ]);
    });

    test("versions that do not overlap are left untouched", async () => {
      await partition.set("adjacent", A, 100, 200);
      await partition.set("adjacent", B, 200, 300);
      const versions = await partition.getAllVersions("adjacent");
      assert.ok(
        versions?.every((version) => version.deletedAt === INFINITY_TIME),
      );
    });

    test("superseded versions keep their audit trail", async () => {
      await partition.set("audit", A, 100);
      await partition.set("audit", B, 200);
      const [superseded] = (await partition.getAllVersions("audit")) ?? [];
      assert.ok(superseded);
      await sleep(5);
      await partition.set("audit", A, 300);
      const [stillSuperseded] = (await partition.getAllVersions("audit")) ?? [];
      assert.deepEqual(stillSuperseded, superseded);
      assert.deepEqual(await liveVersions(partition, "audit"), [
        { data: A, validFrom: 100, validTo: 200 },
        { data: B, validFrom: 200, validTo: 300 },
        { data: A, validFrom: 300, validTo: INFINITY_TIME },
      ]);
    });
  });

  test("indices point at the latest version", async () => {
    const indexed = multiDB.getBitemporalPartition<TestData>("indexed", [
      "artist",
    ]);
    await indexed.set("track", song("Current", "Current Artist"), 100);
    await indexed.set("track", song("Historical", "Historical Artist"), 10, 50);
    assert.deepEqual(
      await indexed.get("Current Artist"),
      song("Current", "Current Artist"),
    );
    assert.equal(await indexed.get("Historical Artist"), undefined);

    await indexed.set("track", song("Next", "Next Artist"), 200);
    assert.equal(await indexed.get("Current Artist"), undefined);
    assert.deepEqual(
      await indexed.get("Next Artist"),
      song("Next", "Next Artist"),
    );
    assert.deepEqual(
      await indexed.get("Next Artist", 150),
      song("Current", "Current Artist"),
    );
  });
});
