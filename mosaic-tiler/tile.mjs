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
      throw new Error("z, x, y should b integer");
    }
    if (areZxyNotValid(z, x, y)) {
      throw new Error(`tile coordinates: ${z}/${x}/${y} are not valid`);
    }
    this.z = z;
    this.x = x;
    this.y = y;
  }

  empty() {
    return this.image.empty();
  }

  async extractChild(z, x, y) {
    if (!(this.image.tileSize === 512)) {
      throw new Error("256 px tile can only be extracted from 512 px tile");
    }

    if (this.image.empty()) {
      return this.image;
    }

    const offset = this.getChildOffset(z, x, y);
    const image = await this.image.extractChild(offset.x, offset.y);

    return new Tile(image, z, x, y);
  }

  getChildOffset(z, x, y) {
    if (this.z + 1 !== z) {
      throw new Error("can only get offset for next zoom");
    }
    const parent = {
      z: this.z,
      x: this.x,
      y: this.y,
    };

    const children = [
      {
        offset: { x: 0, y: 0 },
        tile: { z: parent.z + 1, x: parent.x * 2, y: parent.y * 2 },
      },
      {
        offset: { x: 1, y: 0 },
        tile: { z: parent.z + 1, x: parent.x * 2 + 1, y: parent.y * 2 },
      },
      {
        offset: { x: 1, y: 1 },
        tile: { z: parent.z + 1, x: parent.x * 2 + 1, y: parent.y * 2 + 1 },
      },
      {
        offset: { x: 0, y: 1 },
        tile: { z: parent.z + 1, x: parent.x * 2, y: parent.y * 2 + 1 },
      },
    ];

    for (const child of children) {
      if (child.tile.z === z && child.tile.x === x && child.tile.y === y) {
        return child.offset;
      }
    }

    throw new Error("getOffsetRelativeToParent: should never get here");
  }
}

export { Tile, TileImage };
