import GlitchDB from "./GlitchDB.ts";

export type { GlitchPartition } from "./GlitchPartition.ts";
export type {
  GlitchUnitemporalPartition,
  UnitemporalVersion,
  UnitemporallyVersionedData,
} from "./GlitchUnitemporalPartition.ts";
export type {
  GlitchBitemporalPartition,
  BitemporalVersion,
  BitemporallyVersionedData,
} from "./GlitchBitemporalPartition.ts";
export { INFINITY_TIME } from "./constants.ts";

export { GlitchDB };
export default GlitchDB;
