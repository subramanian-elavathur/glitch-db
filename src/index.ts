const fs = require("fs/promises");

export default class GlitchDB<Type> {
  #localDir: string;
  #data: {
    [key: string]: Type;
  };
  #initComplete: boolean;

  constructor(localDir: string) {
    this.#localDir = localDir;
    this.#data = {};
  }

  async #init() {
    if (this.#initComplete) {
      return; // no need to re-init
    }
    console.log(`Checking if specified path ${this.#localDir} exists`);
    let stat;
    try {
      stat = await fs.stat(this.#localDir);
    } catch (e) {
      console.log(`Directory does not exist - will create`);
      await fs.mkdir(this.#localDir, { recursive: true });
      console.log(`Directory created, lets go!`);
      this.#initComplete = true;
    }
    if (stat) {
      if (!stat.isDirectory()) {
        throw new Error(
          `Specified path ${this.#localDir} exists but is not a directory`
        );
      } else {
        console.log(`Directory already exists, lets go!`);
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
      console.log(
        `Key path ${keyPath} is not a file! You may not be able to set a value for this key.`
      );
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

  #getKeyPath(key: string): string {
    return `${this.#localDir}/${key}.json`;
  }

  async set(key: string, value: Type): Promise<boolean> {
    await this.#init();
    this.#data[key] = value;
    try {
      await fs.writeFile(this.#getKeyPath(key), JSON.stringify(value));
      return Promise.resolve(true);
    } catch (e) {
      console.log(`Error setting value, received exception ${e}`);
      Promise.resolve(false);
    }
  }

  async unset(key: string): Promise<boolean> {
    await this.#init();
    const exists = await this.exists(key);
    if (exists) {
      try {
        await fs.rm(this.#getKeyPath(key));
        return Promise.resolve(true);
      } catch (e) {
        return Promise.resolve(false);
      }
    }
    return Promise.resolve(true);
  }
}
