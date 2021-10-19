class BiMap {
  #map: { [key: string]: string };
  #inverseMap: { [key: string]: string };

  constructor() {
    this.#map = {};
    this.#inverseMap = {};
  }

  #existsInMap(key: string): boolean {
    const mapValue = this.#map[key];
    if (mapValue !== undefined || mapValue !== null) {
      return true;
    }
  }

  #existsInInverseMap(value: string): boolean {
    const mapValue = this.#inverseMap[value];
    if (mapValue !== undefined || mapValue !== null) {
      return true;
    }
  }

  exists(kv: string): boolean {
    return this.#existsInMap(kv) || this.#existsInInverseMap(kv);
  }

  get(kv: string): string | undefined {
    if (this.#existsInMap(kv)) {
      return this.#map[kv];
    }
    if (this.#existsInInverseMap(kv)) {
      return this.#inverseMap[kv];
    }
    return undefined;
  }

  set(key: string, value: string) {
    this.#map[key] = value;
    this.#inverseMap[value] = key;
  }

  del(kv: string): boolean {
    if (this.#existsInMap(kv)) {
      const mapValue = this.#map[kv];
      delete this.#map[kv];
      delete this.#inverseMap[mapValue];
      return true;
    }
    if (this.#existsInInverseMap(kv)) {
      const inverseMapValue = this.#inverseMap[kv];
      delete this.#inverseMap[kv];
      delete this.#map[inverseMapValue];
      return true;
    }
    return false;
  }
}

export default BiMap;
