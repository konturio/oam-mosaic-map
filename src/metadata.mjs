import { cacheGet, cachePut } from "./cache.mjs";
import { enqueueMetadataFetching } from "./titiler_fetcher.mjs";
import { keyFromS3Url } from "./key_from_s3_url.mjs";
import { logger } from "./logging.mjs";

async function cacheGetMetadata(key) {
  logger.debug(`Got metadata key: ${key}`);

  const buffer = await cacheGet(`__metadata__/${key}.json`);

  if (buffer === null || buffer.length === 0 || buffer.toString() === "") {
    return null;
  }

  logger.debug(`Cached metadata buffer: ${buffer}`);

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

  // Ensure zooms are numeric; fallback only if missing/NaN
  const parsedMin = Number(metadata.minzoom);
  const parsedMax = Number(metadata.maxzoom);
  const resolvedMinzoom = Number.isFinite(parsedMin) ? parsedMin : 0;
  const resolvedMaxzoom = Number.isFinite(parsedMax) ? parsedMax : 24;

  const tileUrl = new URL(
    `${process.env.TITILER_BASE_URL}/cog/tiles/WebMercatorQuad/___z___/___x___/___y___@2x`
  );
  tileUrl.searchParams.append("url", uuid);
  for (let i = 0; i < metadata.band_metadata.length; ++i) {
    if (metadata.colorinterp[i] != "undefined") {
      const [idx] = metadata.band_metadata[i];
      tileUrl.searchParams.append("bidx", idx.replaceAll("b", ""));
    }
  }
  tileUrl.searchParams.append("nodata", "0");

  logger.debug("Constructed TiTiler tile URL template", {
    url: tileUrl.href,
    uuid,
    minzoom: resolvedMinzoom,
    maxzoom: resolvedMaxzoom,
  });

  return {
    minzoom: resolvedMinzoom,
    maxzoom: resolvedMaxzoom,
    tileUrl: tileUrl.href
      .replace("___z___", "{z}")
      .replace("___x___", "{x}")
      .replace("___y___", "{y}"),
  };
}

export { getGeotiffMetadata };
