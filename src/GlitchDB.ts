import * as path from "node:path";
import { create as createTarball } from "tar";
import { DEFAULT_CACHE_SIZE } from "./constants.ts";
import GlitchPartitionImpl, {
  type GlitchPartition,
} from "./GlitchPartition.ts";
import GlitchUnitemporalPartitionImpl, {
  type GlitchUnitemporalPartition,
} from "./GlitchUnitemporalPartition.ts";
import GlitchBitemporalPartitionImpl, {
  type GlitchBitemporalPartition,
} from "./GlitchBitemporalPartition.ts";

export default class GlitchDB {
  readonly #baseDir: string;
  readonly #defaultCacheSize: number;
  readonly #partitions = new Map<string, unknown>();

  /**
   * @param baseDir directory used to store all data and metadata
   * @param defaultCacheSize number of keys cached per partition, set to `0` to disable caching
   */
  constructor(baseDir: string, defaultCacheSize?: number) {
    this.#baseDir = baseDir;
    this.#defaultCacheSize = defaultCacheSize ?? DEFAULT_CACHE_SIZE;
  }

  #register<Partition>(name: string, partition: Partition): Partition {
    this.#partitions.set(name, partition);
    return partition;
  }

  #partitionDir(name: string): string {
    return path.join(this.#baseDir, name);
  }

  /**
   * Returns the partition most recently created with `name` by any of the `get*Partition` methods.
   */
  getPartitionByName<Type = unknown>(name: string): GlitchPartition<Type> {
    const partition = this.#partitions.get(name);
    if (partition === undefined) {
      throw new Error(`Glitch Partition with name ${name} not found`);
    }
    return partition as GlitchPartition<Type>;
  }

  /**
   * Creates a gzipped tarball of the database inside `outputDirectory` and returns its path.
   */
  backup(outputDirectory: string): string {
    const fileLocation = path.join(outputDirectory, `backup-${Date.now()}.tgz`);
    createTarball(
      {
        gzip: true,
        sync: true,
        cwd: this.#baseDir,
        file: fileLocation,
      },
      ["."],
    );
    return fileLocation;
  }

  getPartition<Type>(
    name: string,
    indices?: string[],
    cacheSize?: number,
  ): GlitchPartition<Type> {
    return this.#register(
      name,
      new GlitchPartitionImpl<Type>(
        this,
        this.#partitionDir(name),
        cacheSize ?? this.#defaultCacheSize,
        indices,
      ),
    );
  }

  getVersionedPartition<Type>(
    name: string,
    indices?: string[],
    cacheSize?: number,
  ): GlitchUnitemporalPartition<Type> {
    return this.#register(
      name,
      new GlitchUnitemporalPartitionImpl<Type>(
        this,
        this.#partitionDir(name),
        cacheSize ?? this.#defaultCacheSize,
        indices,
      ),
    );
  }

  getBitemporalPartition<Type>(
    name: string,
    indices?: string[],
    cacheSize?: number,
  ): GlitchBitemporalPartition<Type> {
    return this.#register(
      name,
      new GlitchBitemporalPartitionImpl<Type>(
        this,
        this.#partitionDir(name),
        cacheSize ?? this.#defaultCacheSize,
        indices,
      ),
    );
  }
}
