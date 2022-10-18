import fs from "fs";
import uniqueString from "unique-string";
import { dirname } from "path";

const TILES_CACHE_DIR_PATH = process.env.TILES_CACHE_DIR_PATH;
const TMP_DIR_PATH = TILES_CACHE_DIR_PATH + "/tmp";

async function cacheInit() {
  if (!fs.existsSync(TMP_DIR_PATH)) {
    await fs.promises.mkdir(TMP_DIR_PATH, { recursive: true });
  }

  if ((await cacheGet("__info__.json")) === null) {
    const now = new Date();
    await cachePut(
      Buffer.from(
        JSON.stringify({
          last_updated: new Date(
            now.getFullYear(),
            now.getMonth(),
            now.getDate() - 31 // TODO: remove -31. required to corrently initialize __info__ on already running servers
          ).toISOString(),
        })
      ),
      "__info__.json"
    );
  }
}

function cachePurgeMosaic() {
  return fs.promises.rmdir(`${TILES_CACHE_DIR_PATH}/__mosaic__`);
}

async function cacheGet(key) {
  try {
    return await fs.promises.readFile(`${TILES_CACHE_DIR_PATH}/${key}`);
  } catch (err) {
    if (err.code === "ENOENT") {
      return null;
    }

    throw err;
  }
}

function cacheDelete(key) {
  const path = `${TILES_CACHE_DIR_PATH}/${key}`;
  if (fs.existsSync(path)) {
    return fs.promises.unlink(path);
  }

  return Promise.resolve();
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

export { cacheInit, cachePurgeMosaic, cacheGet, cachePut, cacheDelete };