import sharp from "sharp";

function areZxyNotValid(z, x, y) {
  return z < 0 || x < 0 || y < 0 || x >= Math.pow(2, z) || y >= Math.pow(2, z);
}

class TileImage {
  constructor(buffer, extension, tileSize = 512) {
    if (!(buffer instanceof Buffer || buffer === null)) {
      throw new Error("buffer should be an instance of Buffer or null");
    }
    this.buffer = buffer;

    if (extension !== "png" && extension !== "jpg") {
      throw new Error(".png and .jpg are the only allowed extensions");
    }
    this.extension = extension;

    if (!(tileSize === 256 || tileSize === 512)) {
      throw new Error("invalid tile size");
    }
    this.tileSize = tileSize;
  }

  async transformInJpegIfFullyOpaque() {
    const stats = await sharp(this.buffer).stats();
    if (stats.isOpaque) {
      this.extension = "jpg";
      this.buffer = await sharp(this.buffer).toFormat("jpeg").toBuffer();
    }
  }

  empty() {
    return this.buffer === null || this.buffer.length === 0;
  }

  async scale(factor) {
    const buffer = await sharp(this.buffer)
      .resize({ width: this.tileSize * factor, height: this.tileSize * factor })
      .toBuffer();

    return new TileImage(buffer, this.extension, this.tileSize * factor);
  }

  async extractChild(x, y) {
    const childTileSize = this.tileSize * 0.5;
    const buffer = await sharp(this.buffer)
      .extract({
        left: x * childTileSize,
        top: y * childTileSize,
        width: childTileSize,
        height: childTileSize,
      })
      .toBuffer();

    return new TileImage(buffer, this.extension, childTileSize);
  }
}

class Tile {
  static createEmpty(z, x, y) {
    return new Tile(new TileImage(null, "png"), z, x, y);
  }

  constructor(image, z, x, y) {
    if (!(image instanceof TileImage)) {
      throw new Error("image should be an instance of TileImage");
    }
    this.image = image;

    if (!Number.isInteger(z) || !Number.isInteger(x) || !Number.isInteger(y)) {
      throw new Error(`z, x, y should b integer. iwnput: z = ${z}, x = ${x}, y = ${y}`);
    }
    if (areZxyNotValid(z, x, y)) {
      throw new Error(`tile coordinates: ${z}/${x}/${y} are not valid`);
    }
    this.z = z;
    this.x = x;
    this.y = y;
  }

  transformInJpegIfFullyOpaque() {
    return this.image.transformInJpegIfFullyOpaque();
  }

  empty() {
    return this.image.empty();
  }

  async scale(factor) {
    return new Tile(await this.image.scale(factor), this.z, this.x, this.y);
  }

  async extractChild(z, x, y) {
    if (!(this.image.tileSize === 512)) {
      throw new Error("256 px tile can only be extracted from 512 px tile");
    }

    if (this.z + 1 !== z) {
      throw new Error("can only get offset for next zoom");
    }

    if (this.image.empty()) {
      return this.image;
    }

    const image = await this.image.extractChild(x % 2, y % 2);

    return new Tile(image, z, x, y);
  }
}

export { Tile, TileImage };

async function constructParentTileFromChildren(tiles, z, x, y) {
  const notEmptyTiles = tiles.filter((tile) => !tile.empty());
  if (!notEmptyTiles.length) {
    return Tile.createEmpty(z, x, y);
  }

  let tileSize = notEmptyTiles[0].image.tileSize;
  for (let i = 1; i < notEmptyTiles.length; ++i) {
    if (notEmptyTiles[i].image.tileSize !== tileSize) {
      throw new Error(
        "constructParentTileFromChildren: size of all input tiles should be the same"
      );
    }
  }

  const [upperLeft, upperRight, lowerLeft, lowerRight] = tiles;

  const composite = [];
  if (!upperLeft.empty()) {
    const downscaled = await upperLeft.image.scale(0.5);
    composite.push({ input: downscaled.buffer, top: 0, left: 0 });
  }
  if (!upperRight.empty()) {
    const downscaled = await upperRight.image.scale(0.5);
    composite.push({
      input: downscaled.buffer,
      top: 0,
      left: tileSize / 2,
    });
  }
  if (!lowerLeft.empty()) {
    const downscaled = await lowerLeft.image.scale(0.5);
    composite.push({
      input: downscaled.buffer,
      top: tileSize / 2,
      left: 0,
    });
  }
  if (!lowerRight.empty()) {
    const downscaled = await lowerRight.image.scale(0.5);
    composite.push({
      input: downscaled.buffer,
      top: tileSize / 2,
      left: tileSize / 2,
    });
  }

  const buffer = await sharp({
    create: {
      width: tileSize,
      height: tileSize,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .composite(composite)
    .png()
    .toBuffer();

  return new Tile(new TileImage(buffer, "png"), z, x, y);
}

export { constructParentTileFromChildren };
