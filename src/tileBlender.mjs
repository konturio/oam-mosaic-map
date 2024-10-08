import sharp from "sharp";

const WHITE_TRESHOLD = process.env.WHITE_TRESHOLD ? parseInt(process.env.WHITE_TRESHOLD) : 200;
const BLACK_TRESHOLD = process.env.BLACK_TRESHOLD ? parseInt(process.env.BLACK_TRESHOLD) : 20;

/**
 * Blends tiles by replacing black or white pixels with the underlying pixel colors,
 * while ensuring transparent or empty areas are handled correctly.
 * @param {Buffer[]} tileBuffers - Array of buffers representing the tiles to be composited.
 * @param {number} width - Width of the resulting composite image.
 * @param {number} height - Height of the resulting composite image.
 * @returns {Buffer} - A buffer of the blended image.
 */
export async function blendTiles(tileBuffers, width = 512, height = 512) {
  // Initialize the base image with full transparency
  let baseImage = await sharp({
    create: {
      width,
      height,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    },
  })
    .raw()
    .toBuffer();

  // Composite each tile on top of the base image
  for (const tileBuffer of tileBuffers) {
    const {
      data: tileData,
      info: { width: tileWidth, height: tileHeight },
    } = await sharp(tileBuffer)
      .ensureAlpha() // Ensure there's an alpha channel
      .resize(width, height) // Resize to match the base dimensions if necessary
      .raw()
      .toBuffer({ resolveWithObject: true });

    // Iterate through pixels and blend non-transparent pixels onto the base image
    for (let i = 0; i < baseImage.length; i += 4) {
      const [r, g, b, a] = tileData.slice(i, i + 4);

      // Check if the current pixel is transparent
      if (a === 0) {
        // Skip fully transparent pixels
        continue;
      }

      // Check if the pixel is black or white
      const isBlack = r < BLACK_TRESHOLD && g < BLACK_TRESHOLD && b < BLACK_TRESHOLD && a > 0;
      const isWhite = r > WHITE_TRESHOLD && g > WHITE_TRESHOLD && b > WHITE_TRESHOLD && a > 0;

      // If the pixel is neither black nor white, or is non-transparent, replace the base pixel
      if (!isBlack && !isWhite) {
        baseImage[i] = r;
        baseImage[i + 1] = g;
        baseImage[i + 2] = b;
        baseImage[i + 3] = a; // Retain alpha channel for correct transparency
      }
    }
  }

  // Convert the blended image back to PNG format
  return await sharp(baseImage, { raw: { width, height, channels: 4 } })
    .png()
    .toBuffer();
}
