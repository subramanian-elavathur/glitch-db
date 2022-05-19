# glitch-db

[![codecov](https://codecov.io/gh/subramanian-elavathur/glitch-db/branch/main/graph/badge.svg?token=6XHJ42X7ZR)](https://codecov.io/gh/subramanian-elavathur/glitch-db) ![npm-version](https://img.shields.io/npm/v/glitch-db?color=blue)

A simple, json file backed, key-value database with support for versioning, audits trails (unitemporal and bitemporal), indices, caching and joins. Supports types via typescript.

Inspired by the music of [Glitch Mob](https://www.youtube.com/watch?v=H-k_Eg7zXuc)

## Installation

```bash
npm install glitch-db
```

## Getting started

Lets create a simple GlitchDB, insert a few values and read it back. Write your code in a file called `code.js`

```typescript
import * as path from "path";
import * as os from "os";
import GlitchDB from "glitch-db";

const gettingStarted = async (): void => {
  // glitch-db will use this directory to store all data and metadata
  const databaseDirectory = "./path/to/db/folder";

  // creates a glitch-db instance with caching enabled by default
  const glitchDB = new GlitchDB(tempDirectory);

  // glitch-db can store one or more types of data
  // for example you could store information about bank accounts,
  // customer details, and loans inside the same glitch-db
  // we refer to these types as "partitions". Lets create one
  const simplePartition = glitchDB.getPartition("simple-key-value");

  // now lets add some data to this partition
  await simplePartition.set("key-1", "value-1");
  await simplePartition.set("key-2", "value-2");
  await simplePartition.set("key-3", "value-3");

  // this is how we access data from a partition
  console.log(await simplePartition.get("key-1")); // value-1

  // to check whether a key exists do this
  console.log(await glitchDB.exists("key-1")); // true

  // you can also update values by setting a new value to the same key, here's how
  await glitchDB.set("key-1", "value-4");
  console.log(await simplePartition.get("key-1")); // value-4

  // to remove a value use the delete api
  await glitchDB.delete("key-3");
  console.log(await glitchDB.exists("key-3")); // undefined

  // you can access all keys inside a parition using the 'keys' api
  console.log(await glitchDB.keys()); // ["key-1", "key-2"]

  // finally you can also look at all data (keys and values) using the 'data' api
  console.log(await glitchDB.data()); // { "key-1": "value-4", "key-2": "value-2" }

  // and that all there is to it!
};

gettingStarted();
```

Run this file as per instructions below to see `glitch-db` in action

```bash
node code.js
```

## More Examples

Check out [test/](./test)
