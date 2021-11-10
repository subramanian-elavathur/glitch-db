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
  data: BitemporallyVersionedData<Type>[];
}

export interface GlitchBitemporalPartition<Type> extends GlitchPartition<Type> {
  get: (key: string, validAsOf?: number) => Promise<Type>;
  set: (
    key: string,
    value: Type,
    validFrom?: number,
    validTo?: number,
    metadata?: { [key: string]: string }
  ) => Promise<boolean>;
  getVersion: (
    key: string,
    validAsOf?: number
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

  async get(key: string, validAsOf?: number): Promise<Type> {
    await this.init();
    return Promise.resolve(undefined);
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
    validAsOf?: number
  ): Promise<BitemporallyVersionedData<Type>> {
    await this.init();
    return Promise.resolve(undefined);
  }

  async getAllVersions(
    key: string
  ): Promise<BitemporallyVersionedData<Type>[]> {
    await this.init();
    const data = await this.#getVersionedData(key);
    return Promise.resolve(data?.data);
  }

  // todo caching
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
      const currentTime = new Date().valueOf();
      const newValidFrom = validFrom ?? currentTime;
      const newValidTo = validTo ?? INFINITY_TIME;

      if (newValidTo !== INFINITY_TIME && newValidTo <= newValidFrom) {
        throw new Error("Valid To cannot be less than or equal to Valid From");
      }

      if (!data?.data?.length) {
        data = {
          data: [
            {
              data: value,
              createdAt: currentTime,
              deletedAt: INFINITY_TIME,
              validFrom: newValidFrom,
              validTo: newValidTo,
              metadata,
            },
          ],
        };
        await fs.writeFile(this.getKeyPath(key), JSON.stringify(data));
        await this.setIndices(key, value);
      } else {
        let rowValidBeforeCurrentRow: BitemporallyVersionedData<Type>;
        data.data = data.data.map((row) => {
          const updatedRow = { ...row };
          if (row.validFrom <= newValidFrom && row.validTo > newValidFrom) {
            updatedRow.deletedAt = currentTime;
            rowValidBeforeCurrentRow = row;
          } else if (newValidFrom <= row.validFrom) {
            updatedRow.deletedAt = currentTime;
          }
          return updatedRow;
        });

        if (rowValidBeforeCurrentRow) {
          data.data.push({
            ...rowValidBeforeCurrentRow,
            validTo: newValidFrom,
          });
        }

        data.data.push({
          data: value,
          createdAt: currentTime,
          deletedAt: INFINITY_TIME,
          validFrom: newValidFrom,
          validTo: newValidTo,
          metadata,
        });

        await fs.writeFile(this.getKeyPath(key), JSON.stringify(data));
        await this.deleteIndices(await this.get(key));
        await this.setIndices(key, value);
      }
      return Promise.resolve(true);
    } catch (error) {
      console.log(`Error setting value for key: ${key}, due to error ${error}`);
      return Promise.resolve(false);
    }
  }
}
