import tar = require("tar");
import { DEFAULT_CACHE_SIZE } from "./constants";
import GlitchPartitionImpl, { GlitchPartition } from "./GlitchPartition";
import GlitchUniTemporalPartitionImpl, {
  GlitchUnitemporalPartition,
} from "./GlitchUnitemporalPartition";

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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  getPartitionByName(name: string): GlitchPartition<any> {
    if (name in this.#partitions) {
      const partition = this.#partitions[name];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
  ): GlitchUnitemporalPartition<Type> {
    const cacheSizeWithDefault = cacheSize ?? this.#defaultCacheSize;
    this.#partitions[name] = {
      name,
      cache: cacheSizeWithDefault,
      versioned: true,
    };
    return new GlitchUniTemporalPartitionImpl<Type>(
      this,
      `${this.#baseDir}/${name}`,
      cacheSizeWithDefault,
      indices
    );
  }
}
