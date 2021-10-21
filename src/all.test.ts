import "./simple.test";
import "./join.test";
import "./cached.test";
import "./versioned.test";
import "./indices.test";
import run from "good-vibes";

run({
  snapshotsDirectory: "./src/__snapshots__",
});
