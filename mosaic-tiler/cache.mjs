import fs from "fs";
import uniqueString from "unique-string";
import { dirname } from "path";

const TILES_CACHE_DIR_PATH = process.env.TILES_CACHE_DIR_PATH;
const TMP_DIR_PATH = TILES_CACHE_DIR_PATH + "/tmp";

function cacheInit() {
  if (!fs.existsSync(TMP_DIR_PATH)) {
    fs.mkdirSync(TMP_DIR_PATH, { recursive: true });
  }
}

function cachePurgeMosaic() {
  fs.rmSync(`${TILES_CACHE_DIR_PATH}/__mosaic__`);
}

async function cacheGet(cacheKey) {
  try {
    return await fs.promises.readFile(`${TILES_CACHE_DIR_PATH}/${cacheKey}`);
  } catch (err) {
    if (err.code === "ENOENT") {
      return null;
    }

    throw err;
  }
}

async function cachePut(buffer, key) {
  const path = `${TILES_CACHE_DIR_PATH}/${key}`;
  if (!fs.existsSync(dirname(path))) {
    fs.mkdirSync(dirname(path), {
      recursive: true,
    });
  }

  // create empty file if buffer param is falsy value
  buffer = buffer || Buffer.from("");

  // write into temp file and then rename to actual name to avoid read of inflight tiles from concurrent requests
  const temp = `${TMP_DIR_PATH}/${uniqueString()}`;
  await fs.promises.writeFile(temp, buffer);
  await fs.promises.rename(temp, path);
}

export { cacheInit, cachePurgeMosaic, cacheGet, cachePut };
