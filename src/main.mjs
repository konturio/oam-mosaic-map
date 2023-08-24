import { promisify } from "util";
import dotenv from "dotenv";
import sharp from "sharp";
import swaggerUI from "swagger-ui-express";
import swaggerJsDoc from "swagger-jsdoc";
import * as db from "./db.mjs";
import express from "express";
import morgan from "morgan";
import cors from "cors";
import ejs from "ejs";
import zlib from "zlib";
import pLimit from "p-limit";
import { cacheInit, cachePurgeMosaic } from "./cache.mjs";
import { tileRequestQueue, metadataRequestQueue } from "./titiler_fetcher.mjs";
import { mosaic256px, requestCachedMosaic512px, requestCachedMosaic256px } from "./mosaic.mjs";
import { invalidateMosaicCache } from "./mosaic_cache_invalidation_job.mjs";
import { buildFiltersConfigFromRequest } from "./filters.mjs";
import { logger } from "./logging.mjs";

dotenv.config({ path: ".env" });

const PORT = process.env.PORT;
const BASE_URL = process.env.BASE_URL;
const OAM_LAYER_ID = process.env.OAM_LAYER_ID || "openaerialmap";

const gzip = promisify(zlib.gzip);

const app = express();

app.set("etag", "weak");

app.use(morgan(":method :url :status :res[content-length] - :response-time ms"));
app.use(cors());

process.on("unhandledRejection", (error) => {
  logger.error(">>>unhandledRejection", error);
  process.exit(1);
});

function wrapAsyncCallback(callback) {
  return (req, res, next) => {
    try {
      callback(req, res, next).catch(next);
    } catch (err) {
      logger.error(">err", err);
      next(err);
    }
  };
}

/**
 * @param {number} z
 * @param {number} x
 * @param {number} y
 */
function isInvalidZxy(z, x, y) {
  return z < 0 || z >= 32 || x < 0 || y < 0 || x >= Math.pow(2, z) || y >= Math.pow(2, z);
}

const mosaicTilesRouter = express.Router();

mosaicTilesRouter.get(
  "/tilejson.json",
  wrapAsyncCallback(async (req, res) => {
    res.set("Cache-Control", "public, no-cache");
    res.json({
      tilejson: "2.2.0",
      version: "1.0.0",
      scheme: "xyz",
      tiles: [`${BASE_URL}/tiles/{z}/{x}/{y}.png`],
      minzoom: 0,
      maxzoom: 24,
      center: [27.580661773681644, 53.85617102825757, 13],
    });
  })
);

async function mosaic256pxRoute(req, res) {
  res.set("Cache-Control", "public, max-age=300");

  const z = Number(req.params.z);
  const x = Number(req.params.x);
  const y = Number(req.params.y);
  if (isInvalidZxy(z, x, y)) {
    return res.status(404).send("Out of bounds");
  }

  if (z == 0) {
    return res.status(404).end();
  }

  const filters = buildFiltersConfigFromRequest(req);
  const MIN_FILTERABLE_ZOOM = 9;
  let tile;
  if (Object.keys(filters).length > 0) {
    if (z < MIN_FILTERABLE_ZOOM) {
      tile = await mosaic256px(z, x, y);
    } else {
      tile = await mosaic256px(z, x, y, filters);
    }
  } else {
    tile = await requestCachedMosaic256px(z, x, y);
  }

  if (tile.image.empty()) {
    return res.status(204).send();
  }

  res.type(tile.image.extension);
  res.send(tile.image.buffer);
}

mosaicTilesRouter.get("/:z(\\d+)/:x(\\d+)/:y(\\d+).png", wrapAsyncCallback(mosaic256pxRoute));
mosaicTilesRouter.get("/:z(\\d+)/:x(\\d+)/:y(\\d+)@1x.png", wrapAsyncCallback(mosaic256pxRoute));

mosaicTilesRouter.get(
  "/:z(\\d+)/:x(\\d+)/:y(\\d+)@2x.png",
  wrapAsyncCallback(async (req, res) => {
    res.set("Cache-Control", "public, max-age=300");

    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isInvalidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    const tile = await requestCachedMosaic512px(z, x, y);
    if (tile.image.empty()) {
      return res.status(204).send();
    }

    res.type(tile.image.extension);
    res.send(tile.image.buffer);
  })
);

/**
 * @openapi
 * /tiles/{z}/{x}/{y}.png:
 *   get:
 *     description: Get a mosaic tile image. The filtering feature has the capability to filter images based on one or multiple IDs, which can be obtained from the database. Additionally, it can filter images based on their resolution. Images with a resolution_in_meters < 1 are classified as high resolution. Those with 1 <= resolution_in_meters < 5 are classified as medium resolution. And images with a resolution_in_meters value >= 5 are classified as low resolution. Moreover, the filtering feature can also filter images based on their uploaded date. If uploaded_at >= start parameter and uploaded_at <= end parameter, the image will be added to the tile.
 *     parameters:
 *       - name: z
 *         in: path
 *         description: Zoom
 *         required: true
 *         schema:
 *           type: number
 *       - name: x
 *         in: path
 *         description: X
 *         required: true
 *         schema:
 *           type: number
 *       - name: y
 *         in: path
 *         description: Y
 *         required: true
 *         schema:
 *           type: number
 *       - name: start
 *         in: query
 *         description: Start date of the time spot for filtering by the date of uploading, ISO 8601 YYYY-MM-DDTHH:mm:ss.sssZ
 *         required: false
 *         schema:
 *           type: string
 *           format: date-time
 *       - name: end
 *         in: query
 *         description: End date of the time spot for filtering by the date of uploading, ISO 8601 YYYY-MM-DDTHH:mm:ss.sssZ
 *         required: false
 *         schema:
 *           type: string
 *           format: date-time
 *       - name: resolution
 *         in: query
 *         description: Resolution to filter by it
 *         required: false
 *         schema:
 *           type: string
 *           enum: 
 *             - high
 *             - medium
 *             - low
 *       - name: id
 *         in: query
 *         description: One or several image ids to filter by, in form of MongoDB ObjectId Hex String 24 bytes
 *         required: false
 *         schema:
 *           type: array
 *           items:
 *             type: string
 *         style: form
 *         explode: true
 *     responses:
 *       200:
 *         description: Mosaic tile image
 *         content:
 *           image/png: {}
 */
app.use("/tiles", mosaicTilesRouter);

/**
 * @openapi
 * /oam/mosaic/{z}/{x}/{y}.png:
 *   get:
 *     description: This endpoint is an alias of /tiles endpoint 
 *     parameters:
 *       - name: z
 *         in: path
 *         description: Zoom
 *         required: true
 *         schema:
 *           type: number
 *       - name: x
 *         in: path
 *         description: X
 *         required: true
 *         schema:
 *           type: number
 *       - name: y
 *         in: path
 *         description: Y
 *         required: true
 *         schema:
 *           type: number
 *       - name: start
 *         in: query
 *         description: Start date of the time spot for filtering by the date of uploading, ISO 8601 YYYY-MM-DDTHH:mm:ss.sssZ
 *         required: false
 *         schema:
 *           type: string
 *           format: date-time
 *       - name: end
 *         in: query
 *         description: End date of the time spot for filtering by the date of uploading, ISO 8601 YYYY-MM-DDTHH:mm:ss.sssZ
 *         required: false
 *         schema:
 *           type: string
 *           format: date-time
 *       - name: resolution
 *         in: query
 *         description: Resolution to filter by it
 *         required: false
 *         schema:
 *           type: string
 *           enum: 
 *             - high
 *             - medium
 *             - low
 *       - name: id
 *         in: query
 *         description: One or several image ids to filter by, in form of MongoDB ObjectId Hex String 24 bytes
 *         required: false
 *         schema:
 *           type: array
 *           items:
 *             type: string
 *         style: form
 *         explode: true
 *     responses:
 *       200:
 *         description: Mosaic tile image
 *         content:
 *           image/png: {}
 */
app.use("/oam/mosaic", mosaicTilesRouter);

app.get("/mosaic_viewer", function (req, res, next) {
  ejs.renderFile("./src/mosaic_viewer.ejs", { baseUrl: process.env.BASE_URL }, (err, data) => {
    if (err) {
      next(err);
    }
    res.send(data);
  });
});

// separate connection for mvt outlines debug endpoint to make it respond when
// all other connection in pool are busy
let mvtConnection = null;
async function getMvtConnection() {
  if (!mvtConnection) {
    mvtConnection = await db.getClient();
  }
  return mvtConnection;
}

app.get(
  "/outlines/:z(\\d+)/:x(\\d+)/:y(\\d+).mvt",
  wrapAsyncCallback(async (req, res) => {
    res.set("Cache-Control", "public, max-age=300");

    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isInvalidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    const dbClient = await getMvtConnection();

    const { rows } = await dbClient.query({
      name: "mvt-outlines",
      text: `with oam_meta as (
         select geom from public.layers_features
         where layer_id = (select id from public.layers where public_id = '${OAM_LAYER_ID}')
       ),
       mvtgeom as (
         select
           ST_AsMVTGeom(
             ST_Transform(ST_Boundary(geom), 3857),
             ST_TileEnvelope($1, $2, $3)
           ) geom
         from oam_meta
         where ST_Transform(geom, 3857) && ST_TileEnvelope($4, $5, $6)
      )
      select ST_AsMVT(mvtgeom.*) as mvt
      from mvtgeom`,
      values: [z, x, y, z, x, y],
    });
    if (rows.length === 0) {
      return res.status(204).send();
    }

    res.set("Content-Encoding", "gzip");
    res.send(await gzip(rows[0].mvt));
  })
);

let clustersConnection = null;
async function getClustersConnection() {
  if (!clustersConnection) {
    clustersConnection = await db.getClient();
  }
  return clustersConnection;
}

app.get(
  "/clusters/:z(\\d+)/:x(\\d+)/:y(\\d+).mvt",
  wrapAsyncCallback(async (req, res) => {
    res.set("Cache-Control", "public, max-age=300");

    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isInvalidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    const dbClient = await getClustersConnection();

    const { rows } = await dbClient.query({
      name: "mvt-clusters",
      text: `with invisible as (
          select ST_Transform(geom, 3857) as geom from public.layers_features
          where layer_id = (select id from public.layers where public_id = '${OAM_LAYER_ID}')
          and ST_Area(ST_Transform(geom, 3857)) < pow(10 * 20037508.342789244 / 512 * 2 / pow(2, $1), 2)
          and ST_Transform(geom, 3857) && ST_TileEnvelope($2, $3, $4)
      ), clusters as (
          select ST_ClusterKMeans(geom, 1, pow(0.2 * 20037508.342789244 / 512 * 2 / pow(2, $5), 2)) over () as cid, geom from invisible
      ),
      mvtgeom as (
          select
          ST_AsMVTGeom(
              ST_GeometricMedian(ST_Collect(ST_Centroid(geom))),
              ST_TileEnvelope($6, $7, $8)
          ) geom,
          count(*) count
          from clusters
          group by cid
      )
      select ST_AsMVT(mvtgeom.*) as mvt
      from mvtgeom`,
      values: [z, z, x, y, z, z, x, y],
    });
    if (rows.length === 0) {
      return res.status(204).send();
    }

    res.set("Content-Encoding", "gzip");
    res.send(await gzip(rows[0].mvt));
  })
);

app.get(
  "/health",
  wrapAsyncCallback(async (req, res) => {
    res.status(200).send("Ok");
  })
);

app.post("/purge_mosaic_cache", async (req, res) => {
  await cachePurgeMosaic();
  res.send("Ok");
});

app.use(function (err, req, res, next) {
  logger.error(err.stack);
  res.status(500);
  res.send("Internal server error");
  next;
});

function runQueuesStatusLogger() {
  setInterval(() => {
    logger.debug(">tile request queue size", tileRequestQueue.size);
    logger.debug(">metadata request queue size", metadataRequestQueue.size);
    logger.debug(">image processing", sharp.counters());
    logger.debug(">db pool waiting count", db.getWaitingCount());
  }, 1000);
}

function runMosaicCacheInvalidationJob() {
  const limit = pLimit(1);
  setInterval(() => {
    // every next task invalidaiton job should wait for complition of already running one
    // otherwise there might be several invalidation jobs running concurrently and deleting
    // the same stale tiles from cache.
    limit(() => {
      return invalidateMosaicCache().catch((err) => {
        logger.error(">error in invalidateMosaicCache", err);
      });
    });
  }, 30000);
}

const options = {
  definition: {
    openapi: "3.1.0",
    info: {
      title: "Mosaic-tiler API",
      description: "Here are endpoints that return raster OAM mosaic tiles by z-x-y.\n\nThey support filtering by date, resolution, and set of IDs.\n\nThe filter is applied only from the 10-th zoom.",
      version: "1.0.0",
    },
  },
  apis: ["./src/*.mjs"],
};

const specs = swaggerJsDoc(options);
app.use("/api-docs", swaggerUI.serve, swaggerUI.setup(specs));

async function main() {
  try {
    await cacheInit();

    runQueuesStatusLogger();
    runMosaicCacheInvalidationJob();

    app.listen(PORT, () => {
      logger.info(`mosaic-tiler server is listening on port ${PORT}`);
    });
    
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
}

main();
