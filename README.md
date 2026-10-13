# glitch-db

[![CI](https://github.com/subramanian-elavathur/glitch-db/actions/workflows/ci.yml/badge.svg)](https://github.com/subramanian-elavathur/glitch-db/actions/workflows/ci.yml) [![npm-version](https://img.shields.io/npm/v/glitch-db?color=blue)](https://www.npmjs.com/package/glitch-db)

A simple, JSON file backed, key-value database with support for versioning, audit trails (unitemporal and bitemporal), indices, caching and joins. Written in TypeScript, so your data stays typed.

Inspired by the music of [Glitch Mob](https://www.youtube.com/watch?v=H-k_Eg7zXuc)

## Installation

```bash
npm install glitch-db
```

glitch-db requires Node.js 22.13 or later. It is published as an ES module, and can be loaded with either `import` or `require`.

## Getting started

Let's create a database, insert a few values and read them back. Save this as `getting-started.mjs`:

```js
import GlitchDB from "glitch-db";

// glitch-db will use this directory to store all data and metadata
const glitchDB = new GlitchDB("./my-database");

// glitch-db can store one or more types of data, for example information about
// bank accounts, customer details and loans can all live inside the same database.
// We refer to these types as "partitions". Let's create one:
const simplePartition = glitchDB.getPartition("simple-key-value");

// now let's add some data to this partition
await simplePartition.set("key-1", "value-1");
await simplePartition.set("key-2", "value-2");
await simplePartition.set("key-3", "value-3");

// this is how we read data from a partition
console.log(await simplePartition.get("key-1")); // value-1

// to check whether a key exists do this
console.log(await simplePartition.exists("key-1")); // true

// you can update values by setting a new value to the same key
await simplePartition.set("key-1", "value-4");
console.log(await simplePartition.get("key-1")); // value-4

// to remove a value use the delete api
await simplePartition.delete("key-3");
console.log(await simplePartition.exists("key-3")); // false

// you can list all keys inside a partition using the keys api
console.log(await simplePartition.keys()); // [ 'key-1', 'key-2' ]

// finally you can also look at all keys and values using the data api
console.log(await simplePartition.data()); // { 'key-1': 'value-4', 'key-2': 'value-2' }

// and that's all there is to it!
```

Run it to see glitch-db in action:

```bash
node getting-started.mjs
```

From CommonJS, use the named export:

```js
const { GlitchDB } = require("glitch-db");
```

## Typed partitions

Every partition is generic over the type of value it stores:

```ts
import GlitchDB, { type GlitchPartition } from "glitch-db";

interface Song {
  title: string;
  artist: string;
  year: number;
}

const db = new GlitchDB("./my-database");
const songs: GlitchPartition<Song> = db.getPartition<Song>("songs");

await songs.set("gravity", {
  title: "Gravity",
  artist: "John Mayer",
  year: 2005,
});
const gravity = await songs.get("gravity"); // Song | undefined
```

Values are stored as JSON, so they should be JSON serializable. Values read back from the database are always fresh copies: for example a `Date` is read back as an ISO string, whether it came from the cache or from disk.

Keys are used as file names, so they cannot contain `/`, `\` or null characters, and cannot be `__index__`. Setting an invalid key returns `false`.

## Caching

Each partition keeps the most recently used values in an LRU cache of up to 1000 keys. The default can be changed for the whole database, or for a single partition:

```js
const db = new GlitchDB("./my-database", 5000); // cache up to 5000 keys per partition
const uncached = db.getPartition("big-values", undefined, 0); // disable caching for this partition
```

## Indices

Indices let you look up values by one of their fields as well as by their key. Pass a list of property paths when creating a partition; nested properties (`address.city`) and array elements (`tags[0]`) are supported:

```js
const cars = db.getPartition("cars", ["name", "specs.engine"]);

await cars.set("car-1", { name: "Model S", specs: { engine: "electric" } });

await cars.get("car-1"); // { name: 'Model S', ... }
await cars.get("Model S"); // { name: 'Model S', ... }
await cars.get("electric"); // { name: 'Model S', ... }
```

Only string, number, boolean and bigint values are indexed. Each index value points at a single key, so when two values share an index value, the most recently written one wins. Indices are kept up to date as values change and are persisted alongside the data.

## Versioned partitions

A versioned (unitemporal) partition keeps every value ever written to a key, along with when it was created and superseded, and optional metadata for audit trails:

```js
const songs = db.getVersionedPartition("versioned-songs", ["artist"]);

await songs.set("gravity", { title: "Gravity", artist: "John Mayer" });
await songs.set(
  "gravity",
  { title: "Gravity (Live)", artist: "John Mayer" },
  { updatedBy: "sub-el" },
);

await songs.get("gravity"); // latest: { title: 'Gravity (Live)', ... }
await songs.get("gravity", 1); // version 1: { title: 'Gravity', ... }

await songs.getVersion("gravity");
// {
//   data: { title: 'Gravity (Live)', artist: 'John Mayer' },
//   createdAt: 1760000000000,
//   deletedAt: 999999999999999,
//   version: 2,
//   metadata: { updatedBy: 'sub-el' }
// }

await songs.getAllVersions("gravity"); // every version, oldest first
```

`deletedAt` is the time a version was superseded, and is `INFINITY_TIME` (exported by glitch-db) for the latest version.

## Bitemporal partitions

A bitemporal partition tracks two timelines: when a value was _valid_ in the real world (`validFrom` to `validTo`), and when it was _recorded_ (`createdAt` to `deletedAt`). This lets you correct the past without losing track of what you knew at the time:

```js
const prices = db.getBitemporalPartition("prices");

// the price is 100 from time 1000 onwards
await prices.set("widget", { price: 100 }, 1000);

// ...but we later learn that it was actually 90 between times 2000 and 3000
await prices.set("widget", { price: 90 }, 2000, 3000);

await prices.get("widget"); // latest: { price: 100 }
await prices.get("widget", 1500); // { price: 100 }
await prices.get("widget", 2500); // { price: 90 }
await prices.get("widget", 3500); // { price: 100 }
await prices.get("widget", 500); // undefined

await prices.getVersion("widget", 2500); // { data: { price: 90 }, validFrom: 2000, validTo: 3000, ... }
await prices.getAllVersions("widget"); // every version, including superseded ones
```

When omitted, `validFrom` defaults to now and `validTo` to `INFINITY_TIME`. `validTo` must be greater than `validFrom`. Indices on a bitemporal partition point at the latest (open ended) version.

## Joins

Partitions can be joined with other partitions of the same database. A join reads a property of the value (`leftKey`) and looks up the matching value in the other partition, either by key or by a property of its values (`rightKey`):

```js
const books = db.getPartition("books");
const authors = db.getPartition("authors");

await authors.set("Fredrik Backman", {
  country: "Sweden",
  isbn: "9781476738024",
});
await books.set("ove", {
  title: "A Man Called Ove",
  author: "Fredrik Backman",
  isbn: "9781476738024",
});

// join on the key of the authors partition
books.createJoin("authors", "authorInfo", "author");
// or join on a property of the values in the authors partition
books.createJoin("authors", "authorByIsbn", "isbn", "isbn");

await books.getWithJoins("ove");
// {
//   authorInfo: { country: 'Sweden', isbn: '9781476738024' },
//   authorByIsbn: { country: 'Sweden', isbn: '9781476738024' },
//   title: 'A Man Called Ove',
//   author: 'Fredrik Backman',
//   isbn: '9781476738024'
// }
```

In TypeScript, pass the shape of the joined result as a type argument: `books.getWithJoins<BookWithAuthor>("ove")`. Joins are held in memory, so they need to be created again when your application restarts.

## Backups

`backup` writes a gzipped tarball of the whole database to an existing directory and returns its path:

```js
const backupPath = db.backup("./backups"); // backups/backup-1760000000000.tgz
```

## API

| `GlitchDB`                                              | Description                                            |
| ------------------------------------------------------- | ------------------------------------------------------ |
| `new GlitchDB(baseDir, defaultCacheSize?)`              | Creates a database stored in `baseDir`                 |
| `getPartition<T>(name, indices?, cacheSize?)`           | Returns a partition                                    |
| `getVersionedPartition<T>(name, indices?, cacheSize?)`  | Returns a versioned (unitemporal) partition            |
| `getBitemporalPartition<T>(name, indices?, cacheSize?)` | Returns a bitemporal partition                         |
| `getPartitionByName<T>(name)`                           | Returns the partition last created with `name`         |
| `backup(outputDirectory)`                               | Creates a tarball of the database and returns its path |

| Partition                                             | Description                                                |
| ----------------------------------------------------- | ---------------------------------------------------------- |
| `get(key)`                                            | Returns the value for a key or index value, or `undefined` |
| `set(key, value)`                                     | Stores a value, returning `true` on success                |
| `exists(key)`                                         | Returns whether a key or index value exists                |
| `delete(key)`                                         | Deletes a value, returning `true` if it existed            |
| `keys()`                                              | Returns all keys, sorted                                   |
| `data()`                                              | Returns all keys and their (latest) values                 |
| `createJoin(partition, joinName, leftKey, rightKey?)` | Defines a join with another partition                      |
| `getWithJoins(key)`                                   | Returns a value along with all of its joined values        |

Versioned partitions also support `get(key, version?)`, `set(key, value, metadata?)`, `getVersion(key, version?)` and `getAllVersions(key)`. Bitemporal partitions support `get(key, validAsOf?)`, `set(key, value, validFrom?, validTo?, metadata?)`, `getVersion(key, validAsOf?)` and `getAllVersions(key)`.

## Storage format

Each partition is a directory inside the database directory, and each key is a JSON file inside it, so the data is easy to inspect, diff and back up:

```
my-database/
├── simple-key-value/
│   ├── key-1.json
│   └── key-2.json
└── cars/
    ├── __index__.json
    └── car-1.json
```

glitch-db is designed for a single process. Writes from multiple processes, or concurrent writes to the same key, may overwrite each other.

## More examples

Check out [test/](./test) for more examples of every feature.

## Development

Development uses the Node.js version in [`.nvmrc`](./.nvmrc) (Node.js 22.18 or later also works). Tests are written with the built-in [Node.js test runner](https://nodejs.org/api/test.html) and run directly against the TypeScript sources.

```bash
npm install
npm test              # run the tests
npm run test:coverage # run the tests with a coverage report
npm run lint          # lint with ESLint
npm run format        # format with Prettier
npm run typecheck     # type check with TypeScript
npm run build         # compile to dist/
npm run check         # lint, check formatting, type check and test
```

## License

[MIT](./LICENSE)
