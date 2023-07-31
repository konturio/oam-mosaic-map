import { cacheGet, cachePut } from "./cache.mjs";
import { enqueueMetadataFetching } from "./titiler_fetcher.mjs";
import { keyFromS3Url } from "./key_from_s3_url.mjs";
import { logger } from "./logging.mjs";

async function cacheGetMetadata(key) {
  logger.debug(`Got metadata key: ${key}`);

  const buffer = await cacheGet(`__metadata__/${key}.json`);
  if (buffer === null) {
    return null;
  }

  logger.debug(`Cached metadata buffer: ${buffer}`);
  logger.debug(`Cached metadata buffer string: ${buffer.toString()}`);

  return JSON.parse(buffer.toString());
}

function cachePutMetadata(metadata, key) {
  const buffer = Buffer.from(JSON.stringify(metadata));
  return cachePut(buffer, `__metadata__/${key}.json`);
}

async function getGeotiffMetadata(uuid) {
  const key = keyFromS3Url(uuid);

  let metadata = await cacheGetMetadata(key);
  if (metadata === null) {
    metadata = await enqueueMetadataFetching(uuid);
  }

  await cachePutMetadata(metadata, key);

  if (!metadata) {
    return null;
  }

  const tileUrl = new URL(
    `${process.env.TITILER_BASE_URL}/cog/tiles/WebMercatorQuad/___z___/___x___/___y___@2x`
  );
  tileUrl.searchParams.append("url", uuid);
  for (let i = 0; i < metadata.band_metadata.length; ++i) {
    if (metadata.colorinterp[i] != "undefined") {
      const [idx] = metadata.band_metadata[i];
      tileUrl.searchParams.append("bidx", idx);
    }
  }
  tileUrl.searchParams.append("nodata", "0");

  return {
    minzoom: metadata.minzoom,
    maxzoom: metadata.maxzoom,
    tileUrl: tileUrl.href
      .replace("___z___", "{z}")
      .replace("___x___", "{x}")
      .replace("___y___", "{y}"),
  };
}

export { getGeotiffMetadata };
