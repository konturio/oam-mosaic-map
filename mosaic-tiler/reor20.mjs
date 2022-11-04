import sharp from "sharp";
import { enqueueTileFetching } from "./titiler_fetcher.mjs";
import { Tile, TileImage } from "./tile.mjs";

const TITILER_BASE_URL = process.env.TITILER_BASE_URL;
const TILE_URL = `${TITILER_BASE_URL}/cog/tiles/WebMercatorQuad/{z}/{x}/{y}@1x?url=https://geocint.kontur.io/tiles/reor20/cogs/{path}&nodata=0`;

async function requestReor20DepthTile(z, x, y) {
  const tiles = await Promise.all([
    enqueueTileFetching(
      TILE_URL.replace("{path}", "houston_max_uint16-Depth.vrt"),
      z,
      x,
      y
    ),
    // enqueueTileFetching(
    //   TILE_URL.replace("{path}", "waverly_max_uint16-Depth.vrt"),
    //   z,
    //   x,
    //   y
    // ),
  ]);

  const tileBuffer = await sharp({
    create: {
      width: 256,
      height: 256,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite(
      tiles
        .filter((tile) => !tile.empty())
        .map((tile) => {
          return { input: tile.image.buffer, top: 0, left: 0 };
        })
    )
    .png()
    .toBuffer();

  return new Tile(new TileImage(tileBuffer, "png"), z, x, y);
}

async function requestReor20DischargeTile(z, x, y) {
  const tiles = await Promise.all([
    enqueueTileFetching(
      TILE_URL.replace("{path}", "houston_max_uint16-Discharge.vrt"),
      z,
      x,
      y
    ),
    // enqueueTileFetching(
    //   TILE_URL.replace("{path}", "waverly_max_uint16-Discharge.vrt"),
    //   z,
    //   x,
    //   y
    // ),
  ]);

  const tileBuffer = await sharp({
    create: {
      width: 256,
      height: 256,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite(
      tiles
        .filter((tile) => !tile.empty())
        .map((tile) => {
          return { input: tile.image.buffer, top: 0, left: 0 };
        })
    )
    .png()
    .toBuffer();

  return new Tile(new TileImage(tileBuffer, "png", 256), z, x, y);
}

export { requestReor20DepthTile, requestReor20DischargeTile };
