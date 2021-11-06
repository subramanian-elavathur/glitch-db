import * as fs from "fs/promises";
import { UnitemporalVersion } from "./GlitchUnitemporalPartition";
import GlitchPartitionImpl, { GlitchPartition } from "./GlitchPartition";
import { INFINITY_TIME } from "./constants";
import GlitchDB from ".";

interface BitemporalVersion extends UnitemporalVersion {
  validFrom: number;
  validTo: number;
}

interface BitemporallyVersionedData<Type> extends BitemporalVersion {
  data: Type;
}

interface BitemporallyVersioned<Type> {
  latestVersion: number;
  rangeMap: {
    [validFrom: number]: number; // validFrom to version number map
  };
  data: {
    [key: number]: BitemporallyVersionedData<Type>;
  };
}

export interface GlitchBitemporalPartition<Type> extends GlitchPartition<Type> {
  get: (key: string, validAsOf?: number, version?: number) => Promise<Type>;
  set: (
    key: string,
    value: Type,
    validFrom?: number,
    validTo?: number,
    metadata?: { [key: string]: string }
  ) => Promise<boolean>;
  getVersion: (
    key: string,
    version?: number
  ) => Promise<BitemporallyVersionedData<Type>>;
  getAllVersions: (key: string) => Promise<BitemporallyVersionedData<Type>[]>;
}

export default class GlitchBiTemporalPartitionImpl<Type>
  extends GlitchPartitionImpl<Type>
  implements GlitchBitemporalPartition<Type>
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
    file: BitemporallyVersioned<Type>,
    version?: number
  ): BitemporallyVersionedData<Type> {
    return file?.data[version ?? file?.latestVersion];
  }

  async get(key: string, validAsOf?: number, version?: number): Promise<Type> {
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
      const data = JSON.parse(fileData) as BitemporallyVersioned<Type>;
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

  async #getVersionedData(key: string): Promise<BitemporallyVersioned<Type>> {
    const resolvedKey = this.resolveKey(key);
    const keyPath = this.getKeyPath(resolvedKey);
    try {
      const fileData = await fs.readFile(keyPath, {
        encoding: "utf8",
      });
      const parsed = JSON.parse(fileData) as BitemporallyVersioned<Type>;
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
  ): Promise<BitemporallyVersionedData<Type>> {
    await this.init();
    return Promise.resolve(
      this.#getVersionFromFile(await this.#getVersionedData(key), version)
    );
  }

  async getAllVersions(
    key: string
  ): Promise<BitemporallyVersionedData<Type>[]> {
    await this.init();
    const data = await this.#getVersionedData(key);
    return Promise.resolve(data?.data ? Object.values(data.data) : undefined);
  }

  async set(
    key: string,
    value: Type,
    validFrom?: number,
    validTo?: number,
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
          rangeMap: {},
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
        validFrom: 0,
        validTo: INFINITY_TIME,
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
