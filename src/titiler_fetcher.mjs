import got from "got";
import PQueue from "p-queue";
import os from "node:os";

// Getting the number of cores
const numCPUs = os.cpus().length;

console.log("numCPUs:", numCPUs);

const TITILER_BASE_URL = process.env.TITILER_BASE_URL;

const tileRequestQueue = new PQueue({ concurrency: numCPUs });
const activeTileRequests = new Map();

async function fetchTile(url) {
  try {
    const responsePromise = got(url, {
      throwHttpErrors: true,
    });

    const [response, buffer] = await Promise.all([responsePromise, responsePromise.buffer()]);

    if (response.statusCode === 204) {
      return null;
    }

    return buffer;
  } catch (err) {
    if (err.response && (err.response.statusCode === 404 || err.response.statusCode === 500)) {
      return null;
    } else {
      throw err;
    }
  }
}

const FETCH_QUEUE_TTL = parseInt(process.env.TILE_FETCH_TTL_MS, 10) || 1000 * 60 * 10; // 10 minutes default

async function enqueueTileFetching(tileUrl, z, x, y) {
  const url = tileUrl.replace("{z}", z).replace("{x}", x).replace("{y}", y);
  if (activeTileRequests.get(url)) {
    return activeTileRequests.get(url);
  }

  const request = tileRequestQueue
    .add(() => fetchTile(url), { priority: Math.pow(2, z), timeout: FETCH_QUEUE_TTL })
    .catch((error) => {
      if (error.name === "TimeoutError") {
        console.error(`Tile request timed out after ${FETCH_QUEUE_TTL}ms for URL: ${url}`);
      } else {
        console.error(`Error fetching tile: ${url}`, error);
      }
    })
    .finally(() => {
      activeTileRequests.delete(url);
    });

  activeTileRequests.set(url, request);
  return request;
}

export { enqueueTileFetching, tileRequestQueue };

const activeMetaRequests = new Map();
const metadataRequestQueue = new PQueue({ concurrency: numCPUs });

async function fetchTileMetadata(uuid) {
  try {
    const url = new URL(`${TITILER_BASE_URL}/cog/info`);
    url.searchParams.append("url", uuid);
    const metadata = await got(url.href).json();
    return metadata;
  } catch (err) {
    if (err?.response?.statusCode in [404, 500]) {
      return null;
    } else {
      throw err;
    }
  }
}

// deduplicates and limits number of concurrent calls for fetchTileMetadata function
function enqueueMetadataFetching(uuid) {
  if (activeMetaRequests.get(uuid)) {
    return activeMetaRequests.get(uuid);
  }

  const request = metadataRequestQueue
    .add(() => fetchTileMetadata(uuid))
    .finally(() => {
      activeMetaRequests.delete(uuid);
    });

  activeMetaRequests.set(uuid, request);

  return request;
}

export { enqueueMetadataFetching, metadataRequestQueue };
