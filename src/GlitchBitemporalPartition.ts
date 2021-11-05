import { UnitemporalVersion } from "./GlitchUnitemporalPartition";

interface BitemporalVersion extends UnitemporalVersion {
  validFrom: number;
  validTo: number;
}

interface BitemporallyVersionedData<Type> extends BitemporalVersion {
  data: Type;
}

interface BitemporallyVersioned<Type> {
  rangeMap: {
    [validFrom: number]: number; // validFrom to version number map
  };
  data: {
    [key: number]: BitemporallyVersionedData<Type>;
  };
}
