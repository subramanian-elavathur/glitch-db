import LRUCache = require("lru-cache");
import tar = require("tar");
import lget = require("lodash.get");
const fs = require("fs/promises");

const DEFAULT_CACHE_SIZE = 1000;
const INDEX_FILE = "__index__.json";
const getIndexFilePath = (dir: string) => `${dir}/${INDEX_FILE}`;

export default class GlitchDB {
  #baseDir: string;
  #defaultCacheSize?: number;
  #partitions: {
    [key: string]: {
      name: string;
      cache: number;
      versioned: boolean;
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

  backup(outputDirectory: string): string {
    const fileLocation = `${outputDirectory}/backup-${new Date().getTime()}.tgz`;
    tar.create(
      {
        gzip: true,
        sync: true,
        cwd: this.#baseDir,
        file: fileLocation,
      },
      ["."]
    );
    return fileLocation;
  }

  getPartition<Type>(
    name: string,
    indices?: string[],
    cacheSize?: number
  ): GlitchPartition<Type> {
    const cacheSizeWithDefault = cacheSize ?? this.#defaultCacheSize;
    this.#partitions[name] = {
      name,
      cache: cacheSizeWithDefault,
      versioned: false,
    };
    return new GlitchPartitionImpl<Type>(
      this,
      `${this.#baseDir}/${name}`,
      cacheSizeWithDefault,
      indices
    );
  }

  getVersionedPartition<Type>(
    name: string,
    indices?: string[],
    cacheSize?: number
  ): GlitchVersionedPartition<Type> {
    const cacheSizeWithDefault = cacheSize ?? this.#defaultCacheSize;
    this.#partitions[name] = {
      name,
      cache: cacheSizeWithDefault,
      versioned: true,
    };
    return new GlitchPartitionImpl<Type>(
      this,
      `${this.#baseDir}/${name}`,
      cacheSizeWithDefault,
      indices,
      true
    );
  }
}

interface Joiner {
  db: string;
  leftKey: string;
  rightKey?: string;
  joinName: string;
}

interface Version {
  metadata?: {
    [key: string]: string;
  };
  version: number;
  updatedAt: number;
}

interface VersionedData<Type> extends Version {
  data: Type;
}

export interface GlitchPartition<Type> {
  exists: (key: string, version?: number) => Promise<boolean>;
  get: (key: string, version?: number) => Promise<Type>;
  keys: () => Promise<string[]>;
  data: () => Promise<{ [key: string]: Type }>;
  set: (
    key: string,
    value: Type,
    metadata?: { [key: string]: string }
  ) => Promise<boolean>;
  delete: (key: string) => Promise<boolean>;
  createJoin: (
    db: string,
    joinName: string,
    leftKey: string,
    rightKey?: string
  ) => void;
  getWithJoins: (key: string) => Promise<any>;
}

export interface GlitchVersionedPartition<Type> extends GlitchPartition<Type> {
  getVersionWithAudit: (
    key: string,
    version?: number
  ) => Promise<VersionedData<Type>>;
  getAllVersions: (key: string) => Promise<Version[]>;
}

class GlitchPartitionImpl<Type> implements GlitchVersionedPartition<Type> {
  #localDir: string;
  #initComplete: boolean;
  #indices: string[];
  #indexMap: {
    [key: string]: string;
  };
  #joins: {
    [joinName: string]: Joiner;
  };
  #cache: LRUCache<string, Type>; // cache always maintains the latest data
  #master: GlitchDB;
  #versioned: boolean;

  constructor(
    master: GlitchDB,
    localDir: string,
    cacheSize?: number,
    indices?: string[],
    versioned?: boolean
  ) {
    this.#master = master;
    this.#localDir = localDir;
    this.#joins = {};
    if (cacheSize > 0) {
      this.#cache = new LRUCache(cacheSize);
    }
    this.#indices = indices;
    this.#indexMap = {};
    this.#versioned = versioned;
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
      console.log(`Failed to load index file with error ${e}`);
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
        await this.#loadIndex();
        this.#initComplete = true;
      }
    }
  }

  #getKeyFromFile(fileName: string) {
    return fileName.replace(".json", "");
  }

  async keys(): Promise<string[]> {
    await this.#init();
    const files = await fs.readdir(this.#localDir);
    const keys = [];
    for (const file of files) {
      if (file === INDEX_FILE) {
        continue;
      }
      if (this.#versioned) {
        const stat = await fs.lstat(`${this.#localDir}/${file}`);
        // symlink points to the latest version
        if (stat.isSymbolicLink()) {
          keys.push(this.#getKeyFromFile(file));
        }
      } else {
        keys.push(this.#getKeyFromFile(file));
      }
    }
    return keys;
  }

  #getCacheKey(key: string, version?: number) {
    return version > 0 ? `${key}~${version}` : key;
  }

  #getKeyPath(key: string, version?: number): string {
    return version
      ? `${this.#localDir}/${key}.${version}.json`
      : `${this.#localDir}/${key}.json`;
  }

  #resolveKey(key: string) {
    return this.#indexMap[key] ?? key;
  }

  async exists(key: string, version?: number): Promise<boolean> {
    await this.#init();
    const resolvedKey = this.#resolveKey(key);
    if (this.#cache?.has(this.#getCacheKey(resolvedKey, version))) {
      return Promise.resolve(true);
    }
    const keyPath = this.#getKeyPath(resolvedKey, version);
    try {
      const stat = await fs.stat(keyPath);
      if (stat && (stat.isFile() || stat.isSymbolicLink())) {
        return Promise.resolve(true);
      } else {
        return Promise.resolve(false);
      }
    } catch (e) {
      return Promise.resolve(false);
    }
  }

  // todo find key from index
  async get(key: string, version?: number): Promise<Type> {
    await this.#init();
    const resolvedKey = this.#resolveKey(key);
    const cachedData = this.#cache?.get(
      this.#getCacheKey(resolvedKey, version)
    );
    if (cachedData) {
      return Promise.resolve(cachedData);
    }
    const exists = await this.exists(resolvedKey, version);
    if (exists) {
      const fileData = await fs.readFile(
        this.#getKeyPath(resolvedKey, version),
        {
          encoding: "utf8",
        }
      );
      const parsed = JSON.parse(fileData);
      const data = this.#versioned
        ? (parsed as VersionedData<Type>).data
        : parsed;
      this.#cache?.set(this.#getCacheKey(resolvedKey, version), data);
      return Promise.resolve(data);
    }
    return Promise.resolve(undefined);
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

  async getVersionWithAudit(
    key: string,
    version?: number
  ): Promise<VersionedData<Type>> {
    await this.#init();
    const resolvedKey = this.#resolveKey(key);
    const exists = await this.exists(resolvedKey, version);
    if (exists) {
      const fileData = await fs.readFile(
        this.#getKeyPath(resolvedKey, version),
        {
          encoding: "utf8",
        }
      );
      const parsed = JSON.parse(fileData) as VersionedData<Type>;
      return Promise.resolve(parsed);
    }
    return Promise.resolve(undefined);
  }

  async #getNextVersion(key: string): Promise<number> {
    const resolvedKey = this.#resolveKey(key);
    if (this.#versioned) {
      if (await this.exists(resolvedKey)) {
        const data: VersionedData<Type> = await this.getVersionWithAudit(
          resolvedKey
        );
        return Promise.resolve(data.version + 1);
      } else {
        return Promise.resolve(1); // initial version
      }
    } else {
      return Promise.resolve(undefined);
    }
  }

  #getFilePath(file: string): string {
    return `${this.#localDir}/${file}`;
  }

  async getAllVersions(key: string): Promise<Version[]> {
    await this.#init();
    const resolvedKey = this.#resolveKey(key);
    const files = await fs.readdir(this.#localDir);
    const versionedFilesForKey: string[] = [];
    for (const file of files) {
      if (file === INDEX_FILE) {
        continue;
      }
      if (file.includes(resolvedKey)) {
        const stat = await fs.lstat(this.#getFilePath(file));
        if (!stat.isSymbolicLink()) {
          versionedFilesForKey.push(file);
        }
      }
    }
    const data: Version[] = [];
    for (const version of versionedFilesForKey) {
      const fileData = await fs.readFile(this.#getFilePath(version), {
        encoding: "utf8",
      });
      const parsed = JSON.parse(fileData) as VersionedData<Type>;
      data.push({
        version: parsed.version,
        updatedAt: parsed.updatedAt,
        metadata: parsed.metadata,
      });
    }
    return Promise.resolve(data);
  }

  async #removeSymlink(name: string): Promise<boolean> {
    try {
      await fs.unlink(name);
      return Promise.resolve(true);
    } catch (e) {
      console.log(`Error removing symlink, received exception ${e}`);
      return Promise.resolve(false);
    }
  }

  async set(
    key: string,
    value: Type,
    metadata?: { [key: string]: string }
  ): Promise<boolean> {
    await this.#init();
    const nextVersion = await this.#getNextVersion(key);
    const filePath = this.#getKeyPath(key, nextVersion);
    try {
      if (this.#versioned) {
        const versionedData: VersionedData<Type> = {
          data: value,
          updatedAt: new Date().valueOf(),
          version: nextVersion,
          metadata,
        };
        await fs.writeFile(filePath, JSON.stringify(versionedData));
      } else {
        await fs.writeFile(filePath, JSON.stringify(value));
      }
      this.#cache?.set(this.#getCacheKey(key, nextVersion), value);
      try {
        if (this.#versioned) {
          if (nextVersion !== 1) {
            // do not remove symlink if version is 1
            this.#removeSymlink(this.#getKeyPath(key));
          }
          await fs.symlink(filePath, this.#getKeyPath(key));
        }
      } catch (e) {
        console.log(
          `Error setting alias for key: ${key}, received exception ${e}`
        );
        return Promise.resolve(false);
      }
      // generate indices and set into indexBiMap
      if (this.#indices?.length) {
        for (const indexPattern of this.#indices) {
          const index = "" + lget(value, indexPattern); // stringify
          this.#indexMap[index] = key;
        }
      }
      await this.#flushIndex();
      return Promise.resolve(true);
    } catch (e) {
      console.log(`Error setting value, received exception ${e}`);
      Promise.resolve(false);
    }
  }

  async delete(key: string): Promise<boolean> {
    await this.#init();
    const resolvedKey = this.#resolveKey(key);
    const value = await this.get(resolvedKey);
    // clear out cache
    if (this.#versioned) {
      this.#cache
        .keys()
        .filter((each) => each.includes(resolvedKey))
        .forEach((each) => this.#cache?.del(each));
    } else {
      this.#cache?.del(resolvedKey);
    }
    if (value) {
      try {
        if (this.#versioned) {
          await this.#removeSymlink(this.#getKeyPath(resolvedKey));
          const versions = await this.getAllVersions(resolvedKey);
          for (const version of versions) {
            await fs.rm(this.#getKeyPath(resolvedKey, version.version));
          }
        } else {
          await fs.rm(this.#getKeyPath(resolvedKey));
        }
        // remove indices
        if (this.#indices?.length) {
          for (const indexPattern of this.#indices) {
            const index = lget(value, indexPattern);
            delete this.#indexMap[index];
          }
        }
        await this.#flushIndex();
        return Promise.resolve(true);
      } catch (e) {
        console.log(
          `Error deleting key ${resolvedKey}, received exception ${e}`
        );
        return Promise.resolve(false);
      }
    }
    return Promise.resolve(true);
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
    const resolvedKey = this.#resolveKey(key);
    if (!Object.keys(this.#joins)?.length) {
      throw new Error(
        `No joins defined. Please create a join using 'createJoin' api.`
      );
    }
    const leftData = await this.get(resolvedKey);
    if (leftData === undefined) {
      return Promise.resolve(undefined);
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
}
