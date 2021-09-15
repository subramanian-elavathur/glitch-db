import * as fs from "fs/promises";
import * as path from "path";
import * as os from "os";
import { group } from "good-vibes";
import GlitchDB, { GlitchMultiDB } from "./index";

let tempDirectory: string;
const { before, test, after, sync } = group("joins");

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

let leftOne: GlitchDB<LeftOne>;
let leftTwo: GlitchDB<LeftTwo>;
let rightOne: GlitchDB<RightOne>;
let rightTwo: GlitchDB<RightTwo>;

before(async (context) => {
  tempDirectory = path.join(os.tmpdir(), "glitch-join");
  context.log(`Created temp directory for tests at: ${tempDirectory}`);

  const multiDB = new GlitchMultiDB(tempDirectory);
  leftOne = multiDB.getDatabase<LeftOne>("leftOne");
  leftTwo = multiDB.getDatabase<LeftTwo>("leftTwo");
  rightOne = multiDB.getDatabase<RightOne>("rightOne");
  rightTwo = multiDB.getDatabase<RightTwo>("rightTwo");

  await leftOne.set("Mozart", {
    book: "Man called Ove",
    car: "Saab 92",
  });

  await leftTwo.set("Bach", {
    year: 1992,
    isbn: "9781476738024",
  });

  await rightOne.set("Man called Ove", {
    author: "Fredrik Backman",
    isbn: "9781476738024",
  });

  await rightTwo.set("Saab 92", {
    year: 1992,
    manufacturer: "Saab",
  });

  leftOne.createJoin(rightOne, "bookInfo", "book");
  leftOne.createJoin(rightTwo, "carInfo", "car");

  leftTwo.createJoin(rightOne, "book", "isbn", "isbn");
  leftTwo.createJoin(rightTwo, "car", "year", "year");

  context.log(`Glitch DB setup with dummy data complete`);
  context.done();
});

sync(); // run tests one after the other

test("get with related", async (c) => {
  await c.snapshot("Mozart", await leftOne.getRelated("Mozart"));
  c.done();
});

test("get with related using right key", async (c) => {
  await c.snapshot("Bach", await leftTwo.getRelated("Bach"));
  c.done();
});

after(async (c) => {
  try {
    await fs.rmdir(tempDirectory, { recursive: true });
    c.log("Deleted temp directory after tests");
    c.done();
  } catch (e) {
    c.log(`Could not delete temp directory at: ${tempDirectory}`);
    c.done();
  } finally {
    tempDirectory = "";
  }
});
