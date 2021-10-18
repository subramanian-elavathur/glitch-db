import LRUCache = require("lru-cache");
import tar = require("tar");
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
    additionalKeyGenerator?: AdditionalKeyGenerator<Type>,
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
      additionalKeyGenerator,
      cacheSizeWithDefault
    );
  }

  getVersionedPartition<Type>(
    name: string,
    additionalKeyGenerator?: AdditionalKeyGenerator<Type>,
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
      additionalKeyGenerator,
      cacheSizeWithDefault,
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
  additionalKeys?: string[];
}

export interface GlitchPartition<Type> {
  exists: (key: string, version?: number) => Promise<boolean>;
  get: (key: string, version?: number) => Promise<Type>;
  keys: (includeAdditionalKeys?: boolean) => Promise<string[]>;
  data: () => Promise<{ [key: string]: Type }>;
  set: (
    key: string,
    value: Type,
    metadata?: { [key: string]: string }
  ) => Promise<boolean>;
  unset: (key: string, symlinksOnly?: boolean) => Promise<boolean>;
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

// Versioning
// Fully backwards compatible when versioning is not used
// When versioning is used, the version is auto incremented and updatedAt set to current epoch
// metadata can be provided by the users
// File structuring algorithm
// File naming - key.version.json | version is an auto incrementing number
// symlink of format key.json always points to the latest version id - thats how glitch-db knows which is latest
//   this will impact additionalKeyGenerator keys
// api updates
// [DONE] set
//   [DONE] to handle versioning
//   [DONE] set starts with call to unset
//   [DONE] we should just unset symlinks, we should nto remove all versions as part of this
// [DONE] get - should get by key by default and return the .data property of versioned object | cache should not have old versions
// [DONE] keys - should only return non versioned key - and filter out additionalKeys
// [DONE] exists - to return true is key resolves to file or symlink
// [DONE] data - should work as expected once keys is fixed
// [DONE] unset - remove all versions of data and properly handle symlinks too
// [TODO] all 3 new api's should support query by additional keys also

class GlitchPartitionImpl<Type> implements GlitchVersionedPartition<Type> {
  #localDir: string;
  #initComplete: boolean;
  #additionalKeyGenerator: AdditionalKeyGenerator<Type>;
  #joins: {
    [joinName: string]: Joiner;
  };
  #cache: LRUCache<string, Type>; // cache always maintains the latest data
  #master: GlitchDB;
  #versioned: boolean;

  constructor(
    master: GlitchDB,
    localDir: string,
    additionalKeyGenerator?: AdditionalKeyGenerator<Type>,
    cacheSize?: number,
    versioned?: boolean
  ) {
    this.#master = master;
    this.#localDir = localDir;
    this.#additionalKeyGenerator = additionalKeyGenerator;
    this.#joins = {};
    if (cacheSize > 0) {
      this.#cache = new LRUCache(cacheSize);
    }
    this.#versioned = versioned;
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

  async exists(key: string, version?: number): Promise<boolean> {
    await this.#init();
    if (this.#cache?.has(key)) {
      return Promise.resolve(true);
    }
    const keyPath = this.#getKeyPath(key, version);
    let stat;
    try {
      stat = await fs.stat(keyPath);
    } catch (e) {
      return Promise.resolve(false);
    }
    if (stat && (stat.isFile() || stat.isSymbolicLink())) {
      return Promise.resolve(true);
    } else {
      return Promise.resolve(false);
    }
  }

  async get(key: string, version?: number): Promise<Type> {
    await this.#init();
    if (!version) {
      // do not check cache for specific versions
      const cachedData = this.#cache?.get(key);
      if (cachedData) {
        return Promise.resolve(cachedData);
      }
    }
    const exists = await this.exists(key, version);
    if (exists) {
      const data = await fs.readFile(this.#getKeyPath(key, version), {
        encoding: "utf8",
      });
      if (this.#versioned) {
        const parsed = JSON.parse(data) as VersionedData<Type>;
        if (!version) {
          this.#cache?.set(key, parsed.data); // do not set previous version data into cache
        }
        return Promise.resolve(parsed.data);
      } else {
        const parsed = JSON.parse(data);
        this.#cache?.set(key, parsed);
        return Promise.resolve(parsed);
      }
    }
    return Promise.resolve(undefined);
  }

  async getVersionWithAudit(
    key: string,
    version?: number
  ): Promise<VersionedData<Type>> {
    await this.#init();
    const exists = await this.exists(key, version);
    if (exists) {
      const data = await fs.readFile(this.#getKeyPath(key, version), {
        encoding: "utf8",
      });
      const parsed = JSON.parse(data) as VersionedData<Type>;
      return Promise.resolve(parsed);
    }
    return Promise.resolve(undefined);
  }

  async getAllVersions(key: string): Promise<Version[]> {
    await this.#init();
    const files = await fs.readdir(this.#localDir);
    const versionedFilesForKey: string[] = [];
    for (const file of files) {
      const stat = await fs.lstat(`${this.#localDir}/${file}`);
      if (!stat.isSymbolicLink() && file.includes(key)) {
        versionedFilesForKey.push(file);
      }
    }
    const data: Version[] = [];
    for (const version of versionedFilesForKey) {
      const rawData = await fs.readFile(this.#getFilePath(version), {
        encoding: "utf8",
      });
      const parsed = JSON.parse(rawData) as VersionedData<Type>;
      data.push({
        version: parsed.version,
        updatedAt: parsed.updatedAt,
        metadata: parsed.metadata,
      });
    }
    return Promise.resolve(data);
  }

  async keys(includeAdditionalKeys?: boolean): Promise<string[]> {
    await this.#init();
    const files = await fs.readdir(this.#localDir);
    const keys = [];
    for (const file of files) {
      if (this.#versioned) {
        const stat = await fs.lstat(`${this.#localDir}/${file}`);
        // only symbolic link contains latest data for version
        if (stat.isSymbolicLink()) {
          keys.push(this.#getKeyFromFile(file));
        }
      } else {
        if (includeAdditionalKeys) {
          keys.push(this.#getKeyFromFile(file));
        } else {
          const stat = await fs.lstat(`${this.#localDir}/${file}`);
          if (!stat.isSymbolicLink()) {
            keys.push(this.#getKeyFromFile(file));
          }
        }
      }
    }
    // filter out additionalKeys from keys array for versioned dataset
    if (this.#versioned) {
      const additionalKeys = new Set();
      for (const key of keys) {
        const data: VersionedData<Type> = await this.getVersionWithAudit(key);
        if (data?.additionalKeys?.length) {
          for (const additionalKey of data.additionalKeys) {
            additionalKeys.add(additionalKey);
          }
        }
      }
      return keys.filter((each) => !additionalKeys.has(each));
    } else {
      return keys;
    }
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

  #getKeyPath(key: string, version?: number): string {
    return version
      ? `${this.#localDir}/${key}.${version}.json`
      : `${this.#localDir}/${key}.json`;
  }

  #getFilePath(file: string): string {
    return `${this.#localDir}/${file}`;
  }

  #getKeyFromFile(fileName: string) {
    return fileName.replace(".json", "");
  }

  async #getNextVersion(key: string): Promise<number> {
    if (this.#versioned) {
      if (await this.exists(key)) {
        const data: VersionedData<Type> = await this.getVersionWithAudit(key);
        return Promise.resolve(data.version + 1);
      } else {
        return Promise.resolve(1); // initial version
      }
    } else {
      return Promise.resolve(undefined);
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
    // get additional keys
    const keys = this.#additionalKeyGenerator
      ? this.#additionalKeyGenerator(key, value)
      : undefined;
    // Removing symlinks before setting them again is important
    // because they may be generated using the value object
    // which may have changed later as part of this update instruction
    await this.unset(key, true);

    try {
      if (this.#versioned) {
        const versionedData: VersionedData<Type> = {
          data: value,
          updatedAt: new Date().valueOf(),
          version: nextVersion,
          additionalKeys: keys,
          metadata,
        };
        await fs.writeFile(filePath, JSON.stringify(versionedData));
      } else {
        await fs.writeFile(filePath, JSON.stringify(value));
      }

      if (this.#cache) {
        this.#cache?.set(key, value);
      }
      // add symlinks
      try {
        if (keys?.length) {
          if (this.#versioned) {
            keys.push(key);
          }
          await Promise.all(
            keys.map((each) => fs.symlink(filePath, this.#getKeyPath(each))) // in windows requires admin rights
          );
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
        // clear out symlinks first
        try {
          let keys: string[] = this.#additionalKeyGenerator
            ? this.#additionalKeyGenerator(key, value)
            : [];
          if (this.#versioned && !keys.includes(key)) {
            keys.push(key); // keys for versioned datasets are symlinks
          }
          // during the get api call we add information to the cache using the
          // additional keys as well so its important to clear out the cache too
          if (keys?.length) {
            for (const cachedKey of keys) {
              if (this.#cache?.has(cachedKey)) {
                this.#cache?.del(cachedKey);
              }
            }
            await Promise.all(
              keys.map((each) => fs.unlink(this.#getKeyPath(each)))
            );
          }
        } catch (e) {
          console.log(
            `Error removing additional keys, received exception ${e}`
          );
          return Promise.resolve(false);
        }

        // remove actual files next
        if (!symlinksOnly) {
          // remove all versions for versioned datasets
          if (this.#versioned) {
            const versions = await this.getAllVersions(key);
            for (const version of versions) {
              await fs.rm(this.#getKeyPath(key, version.version));
            }
          } else {
            await fs.rm(this.#getKeyPath(key));
          }
        }
        return Promise.resolve(true);
      } catch (e) {
        return Promise.resolve(false);
      }
    }
    return Promise.resolve(true);
  }
}
