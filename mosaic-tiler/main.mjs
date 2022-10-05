import { promisify } from "util";
import dotenv from "dotenv";
import sharp from "sharp";
import * as db from "./db.mjs";
import express from "express";
import morgan from "morgan";
import cors from "cors";
import zlib from "zlib";
import { cacheInit, cachePurgeMosaic } from "./cache.mjs";
import {
  requestMosaic,
  tileRequestQueue,
  metadataRequestQueue,
  invalidateMosaicCache,
} from "./mosaic.mjs";

dotenv.config({ path: ".env" });

const PORT = process.env.PORT;
const BASE_URL = process.env.BASE_URL;

const gzip = promisify(zlib.gzip);

const app = express();

app.use(
  morgan(":method :url :status :res[content-length] - :response-time ms")
);
app.use(cors());

process.on("unhandledRejection", (error) => {
  console.log(">>>unhandledRejection", error);
  process.exit(1);
});

function wrapAsyncCallback(callback) {
  return (req, res, next) => {
    try {
      callback(req, res, next).catch(next);
    } catch (err) {
      console.log(">err", err);
      next(err);
    }
  };
}

function isValidZxy(z, x, y) {
  return z < 0 || x < 0 || y < 0 || x >= Math.pow(2, z) || y >= Math.pow(2, z);
}

app.get(
  "/tiles/tilejson.json",
  wrapAsyncCallback(async (req, res) => {
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

app.get(
  "/tiles/:z(\\d+)/:x(\\d+)/:y(\\d+).png",
  wrapAsyncCallback(async (req, res) => {
    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isValidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    const tile = await requestMosaic(z, x, y);
    res.type(tile.extension);
    res.end(tile.buffer);
  })
);

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
    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isValidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    const dbClient = await getMvtConnection();

    const { rows } = await dbClient.query({
      name: "mvt-outlines",
      text: `with oam_meta as (
         select geom from public.layers_features
         where layer_id = (select id from public.layers where public_id = 'openaerialmap')
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
      return res.status(204).end();
    }

    res.writeHead(200, { "Content-Encoding": "gzip" });
    res.end(await gzip(rows[0].mvt));
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
    const z = Number(req.params.z);
    const x = Number(req.params.x);
    const y = Number(req.params.y);
    if (isValidZxy(z, x, y)) {
      return res.status(404).send("Out of bounds");
    }

    const dbClient = await getClustersConnection();

    const { rows } = await dbClient.query({
      name: "mvt-clusters",
      text: `with invisible as (
          select ST_Transform(geom, 3857) as geom from public.layers_features
          where layer_id = (select id from public.layers where public_id = 'openaerialmap')
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
      return res.status(204).end();
    }

    res.writeHead(200, { "Content-Encoding": "gzip" });
    res.end(await gzip(rows[0].mvt));
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
  res.end("Ok");
});

app.use(function (err, req, res, next) {
  console.error(err.stack);
  res.status(500);
  res.end("Internal server error");
  next;
});

function runQueuesStatusLogger() {
  setInterval(() => {
    console.log(">tile request queue size", tileRequestQueue.size);
    console.log(">metadata request queue size", metadataRequestQueue.size);
    console.log(">image processing", sharp.counters());
    console.log(">db pool waiting count", db.getWaitingCount());
  }, 1000);
}

function runMosaicCacheInvalidationJob() {
  setInterval(() => {
    invalidateMosaicCache();
  }, 30000);
}

async function main() {
  try {
    await cacheInit();

    runQueuesStatusLogger();
    runMosaicCacheInvalidationJob();

    app.listen(PORT, () => {
      console.log(`mosaic-tiler server is listening on port ${PORT}`);
    });
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

main();
