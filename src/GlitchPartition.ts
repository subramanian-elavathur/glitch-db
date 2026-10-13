import * as fs from "node:fs/promises";
import * as path from "node:path";
import { LRUCache } from "lru-cache";
import { INDEX_FILE } from "./constants.ts";
import type GlitchDB from "./GlitchDB.ts";

const KEY_FILE_EXTENSION = ".json";
const INDEX_KEY = INDEX_FILE.slice(0, -KEY_FILE_EXTENSION.length);
const INVALID_KEY_CHARACTERS = /[/\\\0]/;

/**
 * Reads a value at a dot separated path such as `address.city` or `tags[0]`,
 * returning `undefined` when any part of the path is missing.
 */
const getByPath = (value: unknown, propertyPath: string): unknown =>
  propertyPath
    .replace(/\[(\w+)\]/g, ".$1")
    .split(".")
    .filter((segment) => segment.length > 0)
    .reduce<unknown>(
      (current, segment) =>
        current === null || current === undefined
          ? undefined
          : (current as Record<string, unknown>)[segment],
      value,
    );

/**
 * Converts primitive values to the string keys used by indices and joins.
 * Objects, arrays, `null` and `undefined` cannot be used as keys.
 */
const toKey = (value: unknown): string | undefined =>
  typeof value === "string" ||
  typeof value === "number" ||
  typeof value === "boolean" ||
  typeof value === "bigint"
    ? String(value)
    : undefined;

interface Joiner {
  db: string;
  leftKey: string;
  rightKey?: string;
  joinName: string;
}

export interface GlitchPartition<Type> {
  exists: (key: string) => Promise<boolean>;
  get: (key: string) => Promise<Type | undefined>;
  keys: () => Promise<string[]>;
  data: () => Promise<Record<string, Type>>;
  set: (key: string, value: Type) => Promise<boolean>;
  delete: (key: string) => Promise<boolean>;
  createJoin: (
    db: string,
    joinName: string,
    leftKey: string,
    rightKey?: string,
  ) => void;
  getWithJoins: <Joined = Record<string, unknown>>(
    key: string,
  ) => Promise<Joined | undefined>;
}

export default class GlitchPartitionImpl<
  Type,
> implements GlitchPartition<Type> {
  readonly #master: GlitchDB;
  readonly #localDir: string;
  readonly #indices: string[];
  readonly #joins = new Map<string, Joiner>();
  // the cache always holds the latest value of a key, serialized so that callers never share mutable objects with it
  readonly #cache?: LRUCache<string, string>;
  #indexMap = new Map<string, string>();
  #initialized?: Promise<void>;
  #indexWrites = Promise.resolve();

  constructor(
    master: GlitchDB,
    localDir: string,
    cacheSize?: number,
    indices?: string[],
  ) {
    this.#master = master;
    this.#localDir = localDir;
    this.#indices = indices ?? [];
    if (cacheSize && cacheSize > 0) {
      this.#cache = new LRUCache({ max: cacheSize });
    }
  }

  get #indexFilePath(): string {
    return path.join(this.#localDir, INDEX_FILE);
  }

  async #loadIndex(): Promise<void> {
    try {
      const fileData = await fs.readFile(this.#indexFilePath, "utf8");
      this.#indexMap = new Map(
        Object.entries(JSON.parse(fileData) as Record<string, string>),
      );
    } catch {
      // no index has been written for this partition yet
    }
  }

  async #writeIndex(): Promise<void> {
    try {
      await fs.writeFile(
        this.#indexFilePath,
        JSON.stringify(Object.fromEntries(this.#indexMap)),
      );
    } catch (error) {
      console.error(`Failed to write index file with error ${String(error)}`);
    }
  }

  #flushIndex(): Promise<void> {
    // writes are queued as overlapping writes to the same file can interleave and corrupt it
    this.#indexWrites = this.#indexWrites.then(() => this.#writeIndex());
    return this.#indexWrites;
  }

  async #initialize(): Promise<void> {
    let stat;
    try {
      stat = await fs.stat(this.#localDir);
    } catch {
      await fs.mkdir(this.#localDir, { recursive: true });
      return;
    }
    if (!stat.isDirectory()) {
      throw new Error(
        `Specified path ${this.#localDir} exists but is not a directory`,
      );
    }
    await this.#loadIndex();
  }

  protected init(): Promise<void> {
    // memoized so that concurrent first calls do not load the index more than once
    this.#initialized ??= this.#initialize();
    return this.#initialized;
  }

  protected getKeyPath(key: string): string {
    if (INVALID_KEY_CHARACTERS.test(key) || key === INDEX_KEY) {
      throw new Error(
        `Invalid key "${key}": keys cannot contain '/', '\\' or null characters, or be '${INDEX_KEY}'`,
      );
    }
    return path.join(this.#localDir, `${key}${KEY_FILE_EXTENSION}`);
  }

  protected resolveKey(key: string): string {
    return this.#indexMap.get(key) ?? key;
  }

  protected getCached(key: string): Type | undefined {
    const cached = this.#cache?.get(key);
    return cached === undefined ? undefined : (JSON.parse(cached) as Type);
  }

  protected setCached(key: string, value: Type): void {
    const serialized = JSON.stringify(value) as string | undefined;
    if (serialized === undefined) {
      this.#cache?.delete(key);
    } else {
      this.#cache?.set(key, serialized);
    }
  }

  protected async readJson<Data>(key: string): Promise<Data | undefined> {
    try {
      const fileData = await fs.readFile(this.getKeyPath(key), "utf8");
      return JSON.parse(fileData) as Data;
    } catch {
      // the key does not exist, or is not a valid key
      return undefined;
    }
  }

  protected async writeJson(key: string, data: unknown): Promise<void> {
    await fs.writeFile(this.getKeyPath(key), JSON.stringify(data));
  }

  /**
   * Returns the latest value stored against `key`, without resolving indices.
   * Versioned partitions override this to read the latest version.
   */
  protected async getLatest(key: string): Promise<Type | undefined> {
    const cached = this.getCached(key);
    if (cached !== undefined) {
      return cached;
    }
    const value = await this.readJson<Type>(key);
    if (value !== undefined) {
      this.setCached(key, value);
    }
    return value;
  }

  async keys(): Promise<string[]> {
    await this.init();
    const files = await fs.readdir(this.#localDir, { withFileTypes: true });
    return files
      .filter(
        (file) =>
          file.isFile() &&
          file.name.endsWith(KEY_FILE_EXTENSION) &&
          file.name !== INDEX_FILE,
      )
      .map((file) => file.name.slice(0, -KEY_FILE_EXTENSION.length))
      .sort();
  }

  async #isStored(key: string): Promise<boolean> {
    if (this.#cache?.has(key)) {
      return true;
    }
    try {
      const stat = await fs.stat(this.getKeyPath(key));
      return stat.isFile();
    } catch {
      return false;
    }
  }

  async exists(key: string): Promise<boolean> {
    await this.init();
    return this.#isStored(this.resolveKey(key));
  }

  async get(key: string): Promise<Type | undefined> {
    await this.init();
    return this.getLatest(this.resolveKey(key));
  }

  async data(): Promise<Record<string, Type>> {
    const data: Record<string, Type> = {};
    for (const key of await this.keys()) {
      const value = await this.getLatest(key);
      if (value !== undefined) {
        data[key] = value;
      }
    }
    return data;
  }

  protected async setIndices(key: string, value: Type): Promise<void> {
    if (!this.#indices.length) {
      return;
    }
    for (const indexPattern of this.#indices) {
      const index = toKey(getByPath(value, indexPattern));
      if (index !== undefined) {
        this.#indexMap.set(index, key);
      }
    }
    await this.#flushIndex();
  }

  protected async deleteIndices(key: string, value: Type): Promise<void> {
    if (!this.#indices.length) {
      return;
    }
    for (const indexPattern of this.#indices) {
      const index = toKey(getByPath(value, indexPattern));
      // another key may have claimed this index since, leave it alone if so
      if (index !== undefined && this.#indexMap.get(index) === key) {
        this.#indexMap.delete(index);
      }
    }
    await this.#flushIndex();
  }

  async set(key: string, value: Type): Promise<boolean> {
    await this.init();
    try {
      const serialized = JSON.stringify(value);
      const previous = await this.getLatest(key);
      await fs.writeFile(this.getKeyPath(key), serialized);
      if (previous !== undefined) {
        await this.deleteIndices(key, previous);
      }
      await this.setIndices(key, value);
      this.#cache?.set(key, serialized);
      return true;
    } catch (error) {
      console.error(
        `Error setting value for key: ${key}, due to error ${String(error)}`,
      );
      return false;
    }
  }

  async delete(key: string): Promise<boolean> {
    await this.init();
    const resolvedKey = this.resolveKey(key);
    if (!(await this.#isStored(resolvedKey))) {
      return false;
    }
    try {
      const value = await this.getLatest(resolvedKey);
      this.#cache?.delete(resolvedKey);
      await fs.rm(this.getKeyPath(resolvedKey));
      if (value !== undefined) {
        await this.deleteIndices(resolvedKey, value);
      }
      return true;
    } catch (error) {
      console.error(
        `Error deleting key ${resolvedKey}, received exception ${String(error)}`,
      );
      return false;
    }
  }

  createJoin(
    // todo make persistent
    db: string,
    joinName: string,
    leftKey: string,
    rightKey?: string,
  ): void {
    if (!db || !joinName || !leftKey) {
      throw new Error(
        `'db', 'joinName' and 'leftKey' arguments cannot be falsy`,
      );
    }
    this.#joins.set(joinName, { db, joinName, leftKey, rightKey });
  }

  async getWithJoins<Joined = Record<string, unknown>>(
    key: string,
  ): Promise<Joined | undefined> {
    if (!this.#joins.size) {
      throw new Error(
        `No joins defined. Please create a join using 'createJoin' api.`,
      );
    }
    const leftData = (await this.get(key)) as
      Record<string, unknown> | undefined;
    if (leftData === undefined) {
      return undefined;
    }
    const joinedData: Record<string, unknown> = {};
    for (const { db, joinName, leftKey, rightKey } of this.#joins.values()) {
      const partition =
        this.#master.getPartitionByName<Record<string, unknown>>(db);
      const leftValue = leftData[leftKey];
      if (rightKey) {
        joinedData[joinName] = Object.values(await partition.data()).find(
          (rightData) => leftValue === rightData?.[rightKey],
        );
      } else {
        const rightPartitionKey = toKey(leftValue);
        joinedData[joinName] =
          rightPartitionKey === undefined
            ? undefined
            : await partition.get(rightPartitionKey);
      }
    }
    return { ...joinedData, ...leftData } as Joined;
  }
}
