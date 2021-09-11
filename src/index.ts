const fs = require("fs/promises");

export interface AdditionalKeyGenerator<Type> {
  (key: string, value: Type): string[];
}

export class GlitchMultiDB {
  #baseDir: string;

  constructor(baseDir: string) {
    this.#baseDir = baseDir;
  }

  getDatabase<Type>(
    name: string,
    additionalKeyGenerator?: AdditionalKeyGenerator<Type>
  ): GlitchDB<Type> {
    return new GlitchDB<Type>(
      `${this.#baseDir}/${name}`,
      additionalKeyGenerator
    );
  }
}

export default class GlitchDB<Type> {
  #localDir: string;
  #initComplete: boolean;
  #additionalKeyGenerator: AdditionalKeyGenerator<Type>;

  constructor(
    localDir: string,
    additionalKeyGenerator?: AdditionalKeyGenerator<Type>
  ) {
    this.#localDir = localDir;
    this.#additionalKeyGenerator = additionalKeyGenerator;
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

  async exists(key: string): Promise<boolean> {
    await this.#init();
    const keyPath = this.#getKeyPath(key);
    let stat;
    try {
      stat = await fs.stat(keyPath);
    } catch (e) {
      return Promise.resolve(false);
    }
    if (stat && stat.isFile()) {
      return Promise.resolve(true);
    } else {
      return Promise.resolve(false);
    }
  }

  async get(key: string): Promise<Type> {
    await this.#init();
    const exists = await this.exists(key);
    if (exists) {
      const data = await fs.readFile(this.#getKeyPath(key), {
        encoding: "utf8",
      });
      return Promise.resolve(JSON.parse(data));
    }
    return Promise.resolve(undefined);
  }

  async keys(includeAdditionalKeys?: boolean): Promise<string[]> {
    await this.#init();
    const files = await fs.readdir(this.#localDir);
    const keys = [];
    for (const file of files) {
      if (includeAdditionalKeys) {
        keys.push(this.#getKeyFromFile(file));
      } else {
        const stat = await fs.lstat(`${this.#localDir}/${file}`);
        if (!stat.isSymbolicLink()) {
          keys.push(this.#getKeyFromFile(file));
        }
      }
    }
    return keys;
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

  #getKeyPath(key: string): string {
    return `${this.#localDir}/${key}.json`;
  }

  #getKeyFromFile(fileName: string) {
    return fileName.replace(".json", "");
  }

  async set(key: string, value: Type): Promise<boolean> {
    await this.#init();
    // Removing symlinks before setting them again is important
    // because they may be generated using the value object
    // which may have changed later as part of this update instruction
    await this.unset(key, true);
    const filePath = this.#getKeyPath(key);
    try {
      await fs.writeFile(filePath, JSON.stringify(value));
      // add symlinks
      try {
        if (this.#additionalKeyGenerator) {
          const keys = this.#additionalKeyGenerator(key, value);
          if (keys?.length) {
            await Promise.all(
              keys.map((each) => fs.symlink(filePath, this.#getKeyPath(each))) // in windows requires admin rights
            );
          }
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
    const value = await this.get(key);
    if (value) {
      try {
        if (!symlinksOnly) {
          await fs.rm(this.#getKeyPath(key));
        }
        try {
          if (this.#additionalKeyGenerator) {
            const keys = this.#additionalKeyGenerator(key, value);
            if (keys?.length) {
              await Promise.all(
                keys.map((each) => fs.unlink(this.#getKeyPath(each)))
              );
            }
          }
        } catch (e) {
          console.log(
            `Error removing additional keys, received exception ${e}`
          );
          return Promise.resolve(false);
        }
        return Promise.resolve(true);
      } catch (e) {
        return Promise.resolve(false);
      }
    }
    return Promise.resolve(true);
  }
}
