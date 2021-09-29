import LRUCache = require("lru-cache");
const fs = require("fs/promises");

export interface AdditionalKeyGenerator<Type> {
  (key: string, value: Type): string[];
}

const DEFAULT_CACHE_SIZE = 1000;
export default class GlitchDB {
  #baseDir: string;
  #defaultCacheSize?: number;
  #partitions: {
    [key: string]: {
      name: string;
      cache: number;
    };
  };

  constructor(baseDir: string, defaultCacheSize?: number) {
    this.#baseDir = baseDir;
    this.#defaultCacheSize = defaultCacheSize ?? DEFAULT_CACHE_SIZE;
    this.#partitions = {};
  }

  getPartitionByName(name: string): GlitchPartition<any> {
    if (name in this.#partitions) {
      const partition = this.#partitions[name];
      return this.getPartition<any>(partition.name, null, partition.cache);
    }
    throw new Error(`Glitch Partition with name ${name} not found`);
  }

  getPartition<Type>(
    name: string,
    additionalKeyGenerator?: AdditionalKeyGenerator<Type>,
    cacheSize?: number
  ): GlitchPartition<Type> {
    const cacheSizeWithDefault = cacheSize ?? this.#defaultCacheSize;
    this.#partitions[name] = { name, cache: cacheSizeWithDefault };
    return new GlitchPartition<Type>(
      this,
      `${this.#baseDir}/${name}`,
      additionalKeyGenerator,
      cacheSizeWithDefault
    );
  }
}

interface Joiner {
  db: string;
  leftKey: string;
  rightKey?: string;
  joinName: string;
}
export class GlitchPartition<Type> {
  #localDir: string;
  #initComplete: boolean;
  #additionalKeyGenerator: AdditionalKeyGenerator<Type>;
  #joins: {
    [joinName: string]: Joiner;
  };
  #cache: LRUCache<string, Type>;
  #master: GlitchDB;

  constructor(
    master: GlitchDB,
    localDir: string,
    additionalKeyGenerator?: AdditionalKeyGenerator<Type>,
    cacheSize?: number
  ) {
    this.#master = master;
    this.#localDir = localDir;
    this.#additionalKeyGenerator = additionalKeyGenerator;
    this.#joins = {};
    if (cacheSize > 0) {
      this.#cache = new LRUCache(cacheSize);
    }
  }

  async #init() {
    if (this.#initComplete) {
      return; // no need to re-init
    }
    let stat;
    try {
      stat = await fs.stat(this.#localDir);
    } catch (e) {
      await fs.mkdir(this.#localDir, { recursive: true });
      this.#initComplete = true;
    }
    if (stat) {
      if (!stat.isDirectory()) {
        throw new Error(
          `Specified path ${this.#localDir} exists but is not a directory`
        );
      } else {
        this.#initComplete = true;
      }
    }
  }

  createJoin(
    // todo make persistent
    db: string,
    joinName: string,
    leftKey: string,
    rightKey?: string
  ): void {
    if (!db || !joinName || !leftKey) {
      throw new Error(
        `'db', 'joinName' and 'leftKey' arguments cannot be falsy`
      );
    }
    this.#joins[joinName] = {
      db,
      joinName: joinName,
      leftKey,
      rightKey,
    };
  }

  async getWithJoins(key: string): Promise<any> {
    await this.#init();
    const leftData = await this.get(key);
    if (leftData === undefined) {
      return Promise.resolve(undefined);
    }
    if (!Object.keys(this.#joins)?.length) {
      throw new Error(
        `No joins defined. Please create a join using 'createJoin' api.`
      );
    }
    let joinedData = {};
    for (const rightKey of Object.keys(this.#joins)) {
      const joiner = this.#joins[rightKey];
      const db = this.#master.getPartitionByName(joiner.db);
      let rightData;
      if (joiner.rightKey) {
        rightData = Object.values(await db.data()).find(
          (each) => leftData[joiner.leftKey] === each[joiner.rightKey]
        );
      } else {
        rightData = await db.get(leftData[joiner.leftKey]);
      }
      joinedData[joiner.joinName] = rightData;
    }
    return Promise.resolve({ ...joinedData, ...leftData });
  }

  async exists(key: string): Promise<boolean> {
    await this.#init();
    if (this.#cache?.has(key)) {
      return Promise.resolve(true);
    }
    const keyPath = this.#getKeyPath(key);
    let stat;
    try {
      stat = await fs.stat(keyPath);
    } catch (e) {
      return Promise.resolve(false);
    }
    if (stat && stat.isFile()) {
      return Promise.resolve(true);
    } else {
      return Promise.resolve(false);
    }
  }

  async get(key: string): Promise<Type> {
    await this.#init();
    const cachedData = this.#cache?.get(key);
    if (cachedData) {
      return Promise.resolve(cachedData);
    }
    const exists = await this.exists(key);
    if (exists) {
      const data = await fs.readFile(this.#getKeyPath(key), {
        encoding: "utf8",
      });
      const parsed = JSON.parse(data);
      this.#cache?.set(key, parsed);
      return Promise.resolve(parsed);
    }
    return Promise.resolve(undefined);
  }

  async keys(includeAdditionalKeys?: boolean): Promise<string[]> {
    await this.#init();
    const files = await fs.readdir(this.#localDir);
    const keys = [];
    for (const file of files) {
      if (includeAdditionalKeys) {
        keys.push(this.#getKeyFromFile(file));
      } else {
        const stat = await fs.lstat(`${this.#localDir}/${file}`);
        if (!stat.isSymbolicLink()) {
          keys.push(this.#getKeyFromFile(file));
        }
      }
    }
    return keys;
  }

  async data(): Promise<{ [key: string]: Type }> {
    await this.#init();
    const keys = await this.keys();
    const data = {};
    for (const key of keys) {
      data[key] = await this.get(key);
    }
    return data;
  }

  #getKeyPath(key: string): string {
    return `${this.#localDir}/${key}.json`;
  }

  #getKeyFromFile(fileName: string) {
    return fileName.replace(".json", "");
  }

  async set(key: string, value: Type): Promise<boolean> {
    await this.#init();
    // Removing symlinks before setting them again is important
    // because they may be generated using the value object
    // which may have changed later as part of this update instruction
    await this.unset(key, true);
    const filePath = this.#getKeyPath(key);
    try {
      await fs.writeFile(filePath, JSON.stringify(value));
      if (this.#cache) {
        this.#cache?.set(key, value);
      }
      // add symlinks
      try {
        if (this.#additionalKeyGenerator) {
          const keys = this.#additionalKeyGenerator(key, value);
          if (keys?.length) {
            await Promise.all(
              keys.map((each) => fs.symlink(filePath, this.#getKeyPath(each))) // in windows requires admin rights
            );
          }
        }
      } catch (e) {
        console.log(`Error setting additional keys, received exception ${e}`);
        return Promise.resolve(false);
      }
      return Promise.resolve(true);
    } catch (e) {
      console.log(`Error setting value, received exception ${e}`);
      Promise.resolve(false);
    }
  }

  async unset(key: string, symlinksOnly?: boolean): Promise<boolean> {
    await this.#init();
    if (this.#cache?.has(key)) {
      this.#cache?.del(key);
    }
    const value = await this.get(key);
    if (value) {
      try {
        if (!symlinksOnly) {
          await fs.rm(this.#getKeyPath(key));
        }
        try {
          if (this.#additionalKeyGenerator) {
            const keys = this.#additionalKeyGenerator(key, value);
            if (keys?.length) {
              await Promise.all(
                keys.map((each) => fs.unlink(this.#getKeyPath(each)))
              );
            }
          }
        } catch (e) {
          console.log(
            `Error removing additional keys, received exception ${e}`
          );
          return Promise.resolve(false);
        }
        return Promise.resolve(true);
      } catch (e) {
        return Promise.resolve(false);
      }
    }
    return Promise.resolve(true);
  }
}
