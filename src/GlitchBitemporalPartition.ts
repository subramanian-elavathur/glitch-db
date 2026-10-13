import { INFINITY_TIME } from "./constants.ts";
import GlitchPartitionImpl, {
  type GlitchPartition,
} from "./GlitchPartition.ts";
import type { UnitemporalVersion } from "./GlitchUnitemporalPartition.ts";

export interface BitemporalVersion extends UnitemporalVersion {
  validFrom: number;
  validTo: number;
}

export interface BitemporallyVersionedData<Type> extends BitemporalVersion {
  data: Type;
}

interface BitemporallyVersioned<Type> {
  data: BitemporallyVersionedData<Type>[];
}

// todo caching
export interface GlitchBitemporalPartition<Type> extends GlitchPartition<Type> {
  get: (key: string, validAsOf?: number) => Promise<Type | undefined>;
  set: (
    key: string,
    value: Type,
    validFrom?: number,
    validTo?: number,
    metadata?: Record<string, string>,
  ) => Promise<boolean>;
  getVersion: (
    key: string,
    validAsOf?: number,
  ) => Promise<BitemporallyVersionedData<Type> | undefined>;
  getAllVersions: (
    key: string,
  ) => Promise<BitemporallyVersionedData<Type>[] | undefined>;
}

/**
 * Finds the live version valid at `validAsOf`, or the open ended (latest)
 * version when `validAsOf` is not specified. At most one version can match.
 */
const findVersion = <Type>(
  file: BitemporallyVersioned<Type> | undefined,
  validAsOf?: number,
): BitemporallyVersionedData<Type> | undefined => {
  const asOf = validAsOf ?? INFINITY_TIME;
  return file?.data.find(
    (each) =>
      each.deletedAt === INFINITY_TIME &&
      each.validFrom <= asOf &&
      ((asOf === INFINITY_TIME && each.validTo === INFINITY_TIME) ||
        asOf < each.validTo),
  );
};

export default class GlitchBitemporalPartitionImpl<Type>
  extends GlitchPartitionImpl<Type>
  implements GlitchBitemporalPartition<Type>
{
  #getVersionedData(
    key: string,
  ): Promise<BitemporallyVersioned<Type> | undefined> {
    return this.readJson<BitemporallyVersioned<Type>>(key);
  }

  protected override async getLatest(key: string): Promise<Type | undefined> {
    return findVersion(await this.#getVersionedData(key))?.data;
  }

  override async get(
    key: string,
    validAsOf?: number,
  ): Promise<Type | undefined> {
    return (await this.getVersion(key, validAsOf))?.data;
  }

  async getVersion(
    key: string,
    validAsOf?: number,
  ): Promise<BitemporallyVersionedData<Type> | undefined> {
    await this.init();
    return findVersion(
      await this.#getVersionedData(this.resolveKey(key)),
      validAsOf,
    );
  }

  async getAllVersions(
    key: string,
  ): Promise<BitemporallyVersionedData<Type>[] | undefined> {
    await this.init();
    return (await this.#getVersionedData(this.resolveKey(key)))?.data;
  }

  // todo caching
  override async set(
    key: string,
    value: Type,
    validFrom?: number,
    validTo?: number,
    metadata?: Record<string, string>,
  ): Promise<boolean> {
    await this.init();
    try {
      const currentTime = Date.now();
      const newValidFrom = validFrom ?? currentTime;
      const newValidTo = validTo ?? INFINITY_TIME;

      if (newValidTo !== INFINITY_TIME && newValidTo <= newValidFrom) {
        throw new Error("Valid To cannot be less than or equal to Valid From");
      }

      const file = (await this.#getVersionedData(key)) ?? { data: [] };
      const previousLatest = findVersion(file)?.data;

      // every live version that overlaps the new validity period is superseded, while
      // the parts of it that fall outside the new period are carried over as new versions
      const remainders: BitemporallyVersionedData<Type>[] = [];
      for (const row of file.data) {
        const overlaps =
          row.deletedAt === INFINITY_TIME &&
          row.validFrom < newValidTo &&
          newValidFrom < row.validTo;
        if (!overlaps) {
          continue;
        }
        row.deletedAt = currentTime;
        const carriedOver = {
          ...row,
          createdAt: currentTime,
          deletedAt: INFINITY_TIME,
        };
        if (row.validFrom < newValidFrom) {
          remainders.push({ ...carriedOver, validTo: newValidFrom });
        }
        if (newValidTo < row.validTo) {
          remainders.push({ ...carriedOver, validFrom: newValidTo });
        }
      }

      file.data.push(...remainders, {
        data: value,
        createdAt: currentTime,
        deletedAt: INFINITY_TIME,
        validFrom: newValidFrom,
        validTo: newValidTo,
        metadata,
      });

      await this.writeJson(key, file);
      // indices always point at the latest version, which a historical update leaves untouched
      if (previousLatest !== undefined) {
        await this.deleteIndices(key, previousLatest);
      }
      const latest = findVersion(file)?.data;
      if (latest !== undefined) {
        await this.setIndices(key, latest);
      }
      return true;
    } catch (error) {
      console.error(
        `Error setting value for key: ${key}, due to error ${String(error)}`,
      );
      return false;
    }
  }
}
