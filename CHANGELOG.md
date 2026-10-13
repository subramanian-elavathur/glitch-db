# Changelog

## Unreleased

### Breaking changes

- Node.js 22.13 or later is required.
- The package is now an ES module. `import GlitchDB from "glitch-db"` now works in native ES modules, and `require("glitch-db")` keeps working from CommonJS through Node.js's support for `require()` of ES modules. CommonJS code can also use the new named export: `const { GlitchDB } = require("glitch-db")`.
- The compiled output moved from `lib/` and `typings/` to `dist/`. Deep imports were never supported by the `exports` map, so this only matters if you referenced those paths directly.
- Types are now accurate under `strict` mode: `get`, `getVersion` and `getAllVersions` return `... | undefined`, `data()` returns `Record<string, Type>`, and `getWithJoins` accepts a type argument for its result instead of returning `any`. The unused `version` parameter was removed from `exists`.
- Keys containing `/`, `\` or null characters, and the key `__index__`, are rejected (`set` returns `false`) instead of reading or writing files outside the partition directory.
- Only string, number, boolean and bigint values are indexed. Object values were previously all indexed as `"[object Object]"`.
- `data()` omits keys without a current value, for example bitemporal keys that only have bounded versions.
- `keys()` returns keys in sorted order.
- Errors are logged with `console.error` instead of `console.log`.

### Added

- Named exports for `GlitchDB` and `INFINITY_TIME`, and type exports for `UnitemporalVersion`, `UnitemporallyVersionedData`, `BitemporalVersion` and `BitemporallyVersionedData`.
- npm provenance for published packages.

### Fixed

- Bitemporal partitions no longer lose the remainder of an existing version when a new version overlaps only part of it (for example, setting a value for `[2000, 3000)` over a value valid from `1000` onwards now keeps the original value valid from `3000` onwards). Versions that were already superseded are no longer modified.
- Bitemporal indices are kept up to date when the indexed value changes, and are no longer changed by updates to historical versions ([#135](https://github.com/subramanian-elavathur/glitch-db/issues/135)).
- Joins to versioned and bitemporal partitions return the latest value instead of the raw file contents. `getPartitionByName` now returns the registered partition instead of creating a new, unversioned one.
- Falsy values such as `0`, `""` and `false` can be deleted.
- Mutating a value returned by `get`, or a value after passing it to `set`, no longer changes the cached value.
- Keys containing `.json` are listed correctly by `keys()`, which also ignores files and directories that are not keys.
- Keys and index values that match `Object.prototype` members, such as `constructor` or `toString`, can be read.
- Deleting a key no longer removes index entries that point at another key.
- Concurrent writes no longer corrupt the index file, and concurrent first use of a partition no longer loses index entries.
- `data()` is no longer affected by index values that match other keys.
- A join with `rightKey` no longer throws when the joined partition contains `null` values.
