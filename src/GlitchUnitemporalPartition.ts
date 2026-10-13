import { INFINITY_TIME } from "./constants.ts";
import GlitchPartitionImpl, {
  type GlitchPartition,
} from "./GlitchPartition.ts";

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
  metadata?: Record<string, string>;
  version?: number;
  createdAt: number;
  deletedAt: number;
}

export interface UnitemporallyVersionedData<Type> extends UnitemporalVersion {
  data: Type;
}

interface UnitemporallyVersioned<Type> {
  latestVersion: number;
  data: Record<number, UnitemporallyVersionedData<Type>>;
}

export interface GlitchUnitemporalPartition<
  Type,
> extends GlitchPartition<Type> {
  get: (key: string, version?: number) => Promise<Type | undefined>;
  set: (
    key: string,
    value: Type,
    metadata?: Record<string, string>,
  ) => Promise<boolean>;
  getVersion: (
    key: string,
    version?: number,
  ) => Promise<UnitemporallyVersionedData<Type> | undefined>;
  getAllVersions: (
    key: string,
  ) => Promise<UnitemporallyVersionedData<Type>[] | undefined>;
}

const getVersionFromFile = <Type>(
  file: UnitemporallyVersioned<Type> | undefined,
  version?: number,
): UnitemporallyVersionedData<Type> | undefined =>
  file?.data[version ?? file.latestVersion];

export default class GlitchUnitemporalPartitionImpl<Type>
  extends GlitchPartitionImpl<Type>
  implements GlitchUnitemporalPartition<Type>
{
  #getVersionedData(
    key: string,
  ): Promise<UnitemporallyVersioned<Type> | undefined> {
    return this.readJson<UnitemporallyVersioned<Type>>(key);
  }

  protected override async getLatest(key: string): Promise<Type | undefined> {
    const cached = this.getCached(key);
    if (cached !== undefined) {
      return cached;
    }
    const latest = getVersionFromFile(await this.#getVersionedData(key))?.data;
    if (latest !== undefined) {
      this.setCached(key, latest); // only the latest version is ever cached
    }
    return latest;
  }

  override async get(key: string, version?: number): Promise<Type | undefined> {
    if (version === undefined) {
      return super.get(key);
    }
    return (await this.getVersion(key, version))?.data;
  }

  async getVersion(
    key: string,
    version?: number,
  ): Promise<UnitemporallyVersionedData<Type> | undefined> {
    await this.init();
    return getVersionFromFile(
      await this.#getVersionedData(this.resolveKey(key)),
      version,
    );
  }

  async getAllVersions(
    key: string,
  ): Promise<UnitemporallyVersionedData<Type>[] | undefined> {
    await this.init();
    const file = await this.#getVersionedData(this.resolveKey(key));
    return file && Object.values(file.data);
  }

  override async set(
    key: string,
    value: Type,
    metadata?: Record<string, string>,
  ): Promise<boolean> {
    await this.init();
    try {
      const file = (await this.#getVersionedData(key)) ?? {
        latestVersion: 0,
        data: {},
      };
      const previous = getVersionFromFile(file);
      const currentTime = Date.now();
      if (previous) {
        previous.deletedAt = currentTime;
      }
      const version = file.latestVersion + 1;
      file.latestVersion = version;
      file.data[version] = {
        data: value,
        createdAt: currentTime,
        deletedAt: INFINITY_TIME,
        version,
        metadata,
      };
      await this.writeJson(key, file);
      if (previous) {
        await this.deleteIndices(key, previous.data);
      }
      await this.setIndices(key, value);
      this.setCached(key, value);
      return true;
    } catch (error) {
      console.error(
        `Error setting value for key: ${key}, due to error ${String(error)}`,
      );
      return false;
    }
  }
}
