import got from "got";
import PQueue from "p-queue";
import { Tile, TileImage } from "./tile.mjs";

const TITILER_BASE_URL = process.env.TITILER_BASE_URL;

const tileRequestQueue = new PQueue({ concurrency: 32 });
const activeTileRequests = new Map();

async function fetchTile(tileUrl, z, x, y) {
  const url = tileUrl.replace("{z}", z).replace("{x}", x).replace("{y}", y);

  try {
    const responsePromise = got(url, {
      throwHttpErrors: true,
    });

    const [response, buffer] = await Promise.all([
      responsePromise,
      responsePromise.buffer(),
    ]);

    if (response.statusCode === 204) {
      return null;
    }

    return new Tile(new TileImage(buffer, "png"), z, x, y);
  } catch (err) {
    if (
      err.response &&
      (err.response.statusCode === 404 || err.response.statusCode === 500)
    ) {
      return Tile.createEmpty(z, x, y);
    } else {
      throw err;
    }
  }
}

async function enqueueTileFetching(tileUrl, z, x, y) {
  const url = tileUrl.replace("{z}", z).replace("{x}", x).replace("{y}", y);
  if (activeTileRequests.get(url)) {
    return activeTileRequests.get(url);
  }

  const request = tileRequestQueue
    .add(() => fetchTile(tileUrl, z, x, y))
    .finally(() => {
      activeTileRequests.delete(url);
    });

  activeTileRequests.set(url, request);
  return request;
}

export { enqueueTileFetching, tileRequestQueue };

const activeMetaRequests = new Map();
const metadataRequestQueue = new PQueue({ concurrency: 32 });

async function fetchTileMetadata(uuid) {
  try {
    const url = new URL(`${TITILER_BASE_URL}/cog/info`);
    url.searchParams.append("url", uuid);
    const metadata = await got(url.href).json();
    return metadata;
  } catch (err) {
    if (
      err.response &&
      (err.response.statusCode === 404 || err.response.statusCode === 500)
    ) {
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
