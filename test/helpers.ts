import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";

export const createTempDir = (name: string): Promise<string> =>
  fs.mkdtemp(path.join(os.tmpdir(), `glitch-${name}-`));

export const removeTempDir = (directory: string): Promise<void> =>
  fs.rm(directory, { recursive: true, force: true });
