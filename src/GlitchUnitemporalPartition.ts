import * as fs from "fs/promises";
import GlitchDB from ".";
import { INFINITY_TIME } from "./constants";
import GlitchPartitionImpl, { GlitchPartition } from "./GlitchPartition";

//  A case against separate versioned partition
//  fundamentally versioning and unitemporal milestoning give
//  the same desired outcome, one represented by version number
//  the other by two timestamps instead
//  its harder for consumers to know the version number than to
//  query for records validAsOf a certain time
//  therefore TODO: add ability to query by validAsOf to this partition
//  however something to note is that most often users will not query by
//  valid as of as they only care about audit history for retention purposes

export interface UnitemporalVersion {
  metadata?: {
    [key: string]: string;
  };
  version: number;
  createdAt: number;
  deletedAt: number;
}

interface UnitemporallyVersionedData<Type> extends UnitemporalVersion {
  data: Type;
}

interface UnitemporallyVersioned<Type> {
  latestVersion: number;
  data: {
    [key: number]: UnitemporallyVersionedData<Type>;
  };
}

export interface GlitchUnitemporalPartition<Type>
  extends GlitchPartition<Type> {
  get: (key: string, version?: number) => Promise<Type>;
  set: (
    key: string,
    value: Type,
    metadata?: { [key: string]: string }
  ) => Promise<boolean>;
  getVersion: (
    key: string,
    version?: number
  ) => Promise<UnitemporallyVersionedData<Type>>;
  getAllVersions: (key: string) => Promise<UnitemporallyVersionedData<Type>[]>;
}

export default class GlitchUniTemporalPartitionImpl<Type>
  extends GlitchPartitionImpl<Type>
  implements GlitchUnitemporalPartition<Type>
{
  constructor(
    master: GlitchDB,
    localDir: string,
    cacheSize?: number,
    indices?: string[]
  ) {
    super(master, localDir, cacheSize, indices);
  }

  #getVersionFromFile(
    file: UnitemporallyVersioned<Type>,
    version?: number
  ): UnitemporallyVersionedData<Type> {
    return file?.data[version ?? file?.latestVersion];
  }

  async get(key: string, version?: number): Promise<Type> {
    await this.init();
    const resolvedKey = this.resolveKey(key);
    if (!version) {
      const cachedData = this.cache?.get(resolvedKey);
      if (cachedData) {
        return Promise.resolve(cachedData);
      }
    }
    const keyPath = this.getKeyPath(resolvedKey);
    try {
      const fileData = await fs.readFile(keyPath, {
        encoding: "utf8",
      });
      const data = JSON.parse(fileData) as UnitemporallyVersioned<Type>;
      const result = this.#getVersionFromFile(data, version);
      if (!version) {
        this.cache?.set(resolvedKey, result?.data); // do not set old versions to cache
      }
      return Promise.resolve(result?.data);
    } catch (e) {
      // console.log(
      //   `Could not read file at ${keyPath} due to error ${e}. Its likely that this key does not exist.`
      // );
      return Promise.resolve(undefined);
    }
  }

  async #getVersionedData(key: string): Promise<UnitemporallyVersioned<Type>> {
    const resolvedKey = this.resolveKey(key);
    const keyPath = this.getKeyPath(resolvedKey);
    try {
      const fileData = await fs.readFile(keyPath, {
        encoding: "utf8",
      });
      const parsed = JSON.parse(fileData) as UnitemporallyVersioned<Type>;
      return Promise.resolve(parsed);
    } catch (e) {
      // console.log(
      //   `Could not read file at ${keyPath} due to error ${e}. Its likely that this key does not exist.`
      // );
      return Promise.resolve(undefined);
    }
  }

  async getVersion(
    key: string,
    version?: number
  ): Promise<UnitemporallyVersionedData<Type>> {
    await this.init();
    return Promise.resolve(
      this.#getVersionFromFile(await this.#getVersionedData(key), version)
    );
  }

  async getAllVersions(
    key: string
  ): Promise<UnitemporallyVersionedData<Type>[]> {
    await this.init();
    const data = await this.#getVersionedData(key);
    return Promise.resolve(data?.data ? Object.values(data.data) : undefined);
  }

  async set(
    key: string,
    value: Type,
    metadata?: { [key: string]: string }
  ): Promise<boolean> {
    await this.init();
    try {
      let data = await this.#getVersionedData(key);
      if (data) {
        await this.deleteIndices(this.#getVersionFromFile(data)?.data);
        data.latestVersion = data.latestVersion + 1;
      } else {
        data = {
          latestVersion: 1,
          data: {},
        };
      }
      const currentTime = new Date().valueOf();
      if (data.latestVersion !== 1) {
        data.data[data.latestVersion - 1] = {
          ...data.data[data.latestVersion - 1],
          deletedAt: currentTime,
        };
      }
      data.data[data.latestVersion] = {
        data: value,
        createdAt: currentTime,
        deletedAt: INFINITY_TIME,
        version: data.latestVersion,
        metadata,
      };
      await fs.writeFile(this.getKeyPath(key), JSON.stringify(data));
      await this.setIndices(key, value);
      this.cache?.set(key, value);
      return Promise.resolve(true);
    } catch (error) {
      console.log(`Error setting value for key: ${key}, due to error ${error}`);
      return Promise.resolve(false);
    }
  }
}
