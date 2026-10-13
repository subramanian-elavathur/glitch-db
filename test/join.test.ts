import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import GlitchDB, { type GlitchPartition } from "../src/index.ts";
import { createTempDir, removeTempDir } from "./helpers.ts";

interface LeftOne {
  book: string;
  car: string;
}

interface LeftTwo {
  isbn: string;
  year: number;
}

interface RightOne {
  author: string;
  isbn: string;
}

interface RightTwo {
  manufacturer: string;
  year: number;
}

describe("joins", () => {
  let tempDirectory: string;
  let multiDB: GlitchDB;
  let leftOne: GlitchPartition<LeftOne>;
  let leftTwo: GlitchPartition<LeftTwo>;

  before(async () => {
    tempDirectory = await createTempDir("join");
    multiDB = new GlitchDB(tempDirectory, 0);
    leftOne = multiDB.getPartition<LeftOne>("leftOne");
    leftTwo = multiDB.getPartition<LeftTwo>("leftTwo");
    const rightOne = multiDB.getPartition<RightOne>("rightOne");
    const rightTwo = multiDB.getPartition<RightTwo>("rightTwo");

    await leftOne.set("Mozart", { book: "Man called Ove", car: "Saab 92" });
    await leftTwo.set("Bach", { year: 1992, isbn: "9781476738024" });
    await rightOne.set("Man called Ove", {
      author: "Fredrik Backman",
      isbn: "9781476738024",
    });
    await rightTwo.set("Saab 92", { year: 1992, manufacturer: "Saab" });

    leftOne.createJoin("rightOne", "bookInfo", "book");
    leftOne.createJoin("rightTwo", "carInfo", "car");

    leftTwo.createJoin("rightOne", "book", "isbn", "isbn");
    leftTwo.createJoin("rightTwo", "car", "year", "year");
  });

  after(() => removeTempDir(tempDirectory));

  test("get with related", async () => {
    assert.deepEqual(await leftOne.getWithJoins("Mozart"), {
      bookInfo: { author: "Fredrik Backman", isbn: "9781476738024" },
      carInfo: { year: 1992, manufacturer: "Saab" },
      book: "Man called Ove",
      car: "Saab 92",
    });
  });

  test("get with related using right key", async () => {
    assert.deepEqual(await leftTwo.getWithJoins("Bach"), {
      book: { author: "Fredrik Backman", isbn: "9781476738024" },
      car: { year: 1992, manufacturer: "Saab" },
      year: 1992,
      isbn: "9781476738024",
    });
  });

  test("get with related for a missing key", async () => {
    assert.equal(await leftOne.getWithJoins("Beethoven"), undefined);
  });

  test("get with related requires a join", async () => {
    const unjoined = multiDB.getPartition<LeftOne>("unjoined");
    await assert.rejects(unjoined.getWithJoins("Mozart"), {
      message: "No joins defined. Please create a join using 'createJoin' api.",
    });
  });

  test("createJoin validates its arguments", () => {
    assert.throws(() => leftOne.createJoin("", "join", "book"), {
      message: "'db', 'joinName' and 'leftKey' arguments cannot be falsy",
    });
  });

  test("joins skip null values in the joined partition", async () => {
    const nullable = multiDB.getPartition<RightOne | null>("nullable");
    await nullable.set("empty", null);
    await nullable.set("ove", {
      author: "Fredrik Backman",
      isbn: "9781476738024",
    });
    const left = multiDB.getPartition<LeftTwo>("left");
    await left.set("Bach", { year: 1992, isbn: "9781476738024" });
    left.createJoin("nullable", "book", "isbn", "isbn");
    assert.deepEqual(await left.getWithJoins("Bach"), {
      book: { author: "Fredrik Backman", isbn: "9781476738024" },
      year: 1992,
      isbn: "9781476738024",
    });
  });

  test("joins read the latest version of versioned partitions", async () => {
    const authors = multiDB.getVersionedPartition<{ country: string }>(
      "authors",
    );
    await authors.set("Fredrik Backman", { country: "Sweden" });
    const books = multiDB.getPartition<{ title: string; author: string }>(
      "books",
    );
    await books.set("ove", {
      title: "Man called Ove",
      author: "Fredrik Backman",
    });
    books.createJoin("authors", "authorInfo", "author");
    assert.deepEqual(await books.getWithJoins("ove"), {
      authorInfo: { country: "Sweden" },
      title: "Man called Ove",
      author: "Fredrik Backman",
    });
  });
});
