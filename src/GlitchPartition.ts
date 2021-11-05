import LRUCache = require("lru-cache");
import lget = require("lodash.get");
import fs = require("fs/promises");
import { INDEX_FILE } from "./constants";
import GlitchDB from "./GlitchDB";

const getIndexFilePath = (dir: string) => `${dir}/${INDEX_FILE}`;

interface Joiner {
  db: string;
  leftKey: string;
  rightKey?: string;
  joinName: string;
}

export interface GlitchPartition<Type> {
  exists: (key: string, version?: number) => Promise<boolean>;
  get: (key: string) => Promise<Type>;
  keys: () => Promise<string[]>;
  data: () => Promise<{ [key: string]: Type }>;
  set: (key: string, value: Type) => Promise<boolean>;
  delete: (key: string) => Promise<boolean>;
  createJoin: (
    db: string,
    joinName: string,
    leftKey: string,
    rightKey?: string
  ) => void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getWithJoins: (key: string) => Promise<any>;
}

export default class GlitchPartitionImpl<Type>
  implements GlitchPartition<Type>
{
  #localDir: string;
  #initComplete: boolean;
  #indices: string[];
  #indexMap: {
    [key: string]: string;
  };
  #joins: {
    [joinName: string]: Joiner;
  };
  protected cache: LRUCache<string, Type>; // cache always maintains the latest data
  #master: GlitchDB;

  constructor(
    master: GlitchDB,
    localDir: string,
    cacheSize?: number,
    indices?: string[]
  ) {
    this.#master = master;
    this.#localDir = localDir;
    this.#joins = {};
    if (cacheSize > 0) {
      this.cache = new LRUCache(cacheSize);
    }
    this.#indices = indices;
    this.#indexMap = {};
  }

  async #loadIndex(): Promise<boolean> {
    try {
      const stat = await fs.stat(getIndexFilePath(this.#localDir));
      if (stat.isFile()) {
        const fileData = await fs.readFile(getIndexFilePath(this.#localDir), {
          encoding: "utf8",
        });
        this.#indexMap = JSON.parse(fileData);
        return Promise.resolve(true);
      } else {
        throw new Error(
          `Index path is not a file! Something is very wrong here, please inspect this path: ${getIndexFilePath(
            this.#localDir
          )}`
        );
      }
    } catch (e) {
      // console.log(`Failed to load index file with error ${e}`);
      return Promise.resolve(false);
    }
  }

  async #flushIndex(): Promise<boolean> {
    try {
      await fs.writeFile(
        getIndexFilePath(this.#localDir),
        JSON.stringify(this.#indexMap)
      );
      return Promise.resolve(true);
    } catch (e) {
      console.log(`Failed to write index file with error ${e}`);
      return Promise.resolve(false);
    }
  }

  protected async init() {
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
        await this.#loadIndex();
        this.#initComplete = true;
      }
    }
  }

  #getKeyFromFile(fileName: string) {
    return fileName.replace(".json", "");
  }

  async keys(): Promise<string[]> {
    await this.init();
    const files = await fs.readdir(this.#localDir);
    return files
      .filter((each) => each !== INDEX_FILE)
      .map(this.#getKeyFromFile);
  }

  protected getKeyPath(key: string): string {
    return `${this.#localDir}/${key}.json`;
  }

  protected resolveKey(key: string) {
    return this.#indexMap[key] ?? key;
  }

  async exists(key: string): Promise<boolean> {
    await this.init();
    const resolvedKey = this.resolveKey(key);
    if (this.cache?.has(resolvedKey)) {
      return Promise.resolve(true);
    }
    const keyPath = this.getKeyPath(resolvedKey);
    try {
      const stat = await fs.stat(keyPath);
      if (stat && stat.isFile()) {
        return Promise.resolve(true);
      } else {
        return Promise.resolve(false);
      }
    } catch (e) {
      // console.log(
      //   `Could not stat file at ${keyPath} due to error ${e}. Its likely that this key does not exist.`
      // );
      return Promise.resolve(false);
    }
  }

  async get(key: string): Promise<Type> {
    await this.init();
    const resolvedKey = this.resolveKey(key);
    const cachedData = this.cache?.get(resolvedKey);
    if (cachedData) {
      return Promise.resolve(cachedData);
    }
    const keyPath = this.getKeyPath(resolvedKey);
    try {
      const fileData = await fs.readFile(keyPath, {
        encoding: "utf8",
      });
      const parsed = JSON.parse(fileData);
      this.cache?.set(resolvedKey, parsed);
      return Promise.resolve(parsed);
    } catch (e) {
      // console.log(
      //   `Could not read file at ${keyPath} due to error ${e}. Its likely that this key does not exist.`
      // );
      return Promise.resolve(undefined);
    }
  }

  async data(): Promise<{ [key: string]: Type }> {
    await this.init();
    const keys = await this.keys();
    const data = {};
    for (const key of keys) {
      data[key] = await this.get(key);
    }
    return data;
  }

  protected async setIndices(key: string, value: Type): Promise<boolean> {
    if (this.#indices?.length) {
      for (const indexPattern of this.#indices) {
        const index = lget(value, indexPattern);
        if (index !== undefined) {
          this.#indexMap["" + index] = key;
        }
      }
      await this.#flushIndex();
      return Promise.resolve(true);
    }
    return Promise.resolve(false);
  }

  protected async deleteIndices(value: Type) {
    if (this.#indices?.length) {
      for (const indexPattern of this.#indices) {
        const index = lget(value, indexPattern);
        delete this.#indexMap[index];
      }
      await this.#flushIndex();
      return Promise.resolve(true);
    }
    return Promise.resolve(false);
  }

  async set(key: string, value: Type): Promise<boolean> {
    await this.init();
    try {
      if (await this.exists(key)) {
        await this.deleteIndices(await this.get(key));
      }
      await fs.writeFile(this.getKeyPath(key), JSON.stringify(value));
      await this.setIndices(key, value);
      this.cache?.set(key, value);
      return Promise.resolve(true);
    } catch (error) {
      console.log(`Error setting value for key: ${key}, due to error ${error}`);
      return Promise.resolve(false);
    }
  }

  async delete(key: string): Promise<boolean> {
    await this.init();
    const resolvedKey = this.resolveKey(key);
    const value = await this.get(resolvedKey);
    this.cache?.del(resolvedKey);
    if (value) {
      try {
        await fs.rm(this.getKeyPath(resolvedKey));
        await this.deleteIndices(value);
        return Promise.resolve(true);
      } catch (e) {
        console.log(
          `Error deleting key ${resolvedKey}, received exception ${e}`
        );
        return Promise.resolve(false);
      }
    }
    return Promise.resolve(false);
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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async getWithJoins(key: string): Promise<any> {
    await this.init();
    const resolvedKey = this.resolveKey(key);
    if (!Object.keys(this.#joins)?.length) {
      throw new Error(
        `No joins defined. Please create a join using 'createJoin' api.`
      );
    }
    const leftData = await this.get(resolvedKey);
    if (leftData === undefined) {
      return Promise.resolve(undefined);
    }
    const joinedData = {};
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
}
