import fs from "fs";
import { opendir } from "node:fs/promises";
import uniqueString from "unique-string";
import { dirname } from "path";
import { globbyStream } from "globby";

const TILES_CACHE_DIR_PATH = process.env.TILES_CACHE_DIR_PATH;
const TMP_DIR_PATH = TILES_CACHE_DIR_PATH + "/tmp";

async function cacheInit() {
  if (!fs.existsSync(TMP_DIR_PATH)) {
    await fs.promises.mkdir(TMP_DIR_PATH, { recursive: true });
  }

  for (const dir of ["__mosaic__", "__mosaic256px__", "__metadata__"]) {
    const dirPath = `${TILES_CACHE_DIR_PATH}/${dir}`;

    if (!fs.existsSync(dirPath)) {
      await fs.promises.mkdir(dirPath, { recursive: true });
    }
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

function mosaicTilesIterable() {
  return {
    async *[Symbol.asyncIterator]() {
      for (const mosaicTilesDir of ["__mosaic__", "__mosaic256px__"]) {
        const mosaicTilesPath = `${TILES_CACHE_DIR_PATH}/${mosaicTilesDir}`;
        const dir = await opendir(mosaicTilesPath);
        for await (const direntZoom of dir) {
          const zoom = direntZoom.name;
          const dirZoom = await opendir(`${mosaicTilesPath}/${zoom}`);
          for await (const direntX of dirZoom) {
            const x = direntX.name;
            const dirX = await opendir(`${mosaicTilesPath}/${zoom}/${x}`);
            for await (const direntY of dirX) {
              const y = direntY.name;
              yield `${mosaicTilesDir}/${zoom}/${x}/${y}`;
            }
          }
        }
      }
    },
  };
}

function metadataJsonsIterable() {
  return {
    async *[Symbol.asyncIterator]() {
      for await (const path of globbyStream(`${TILES_CACHE_DIR_PATH}/__metadata__/**/*.json`)) {
        yield path.replace(`${TILES_CACHE_DIR_PATH}/`, "");
      }
    },
  };
}

function singleImageTilesIterable(key) {
  return {
    async *[Symbol.asyncIterator]() {
      for await (const path of globbyStream(`${TILES_CACHE_DIR_PATH}/${key}/**/*.png`)) {
        yield path.replace(`${TILES_CACHE_DIR_PATH}/`, "");
      }
    },
  };
}

/**
 * @param {string} key
 */
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

async function fileExists(path) {
  try {
    await fs.promises.stat(path);
    return true;
  } catch (e) {
    return false;
  }
}

async function cacheDelete(key) {
  const path = `${TILES_CACHE_DIR_PATH}/${key}`;
  if (await fileExists(path)) {
    return fs.promises.unlink(path);
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

export {
  cacheInit,
  cachePurgeMosaic,
  cacheGet,
  cachePut,
  cacheDelete,
  mosaicTilesIterable,
  metadataJsonsIterable,
  singleImageTilesIterable,
};
