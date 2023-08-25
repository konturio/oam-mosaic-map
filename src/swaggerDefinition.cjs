/** @type {import('swagger-jsdoc').SwaggerDefinition} */
module.exports = {
  openapi: "3.0.0",
  info: {
    title: "Mosaic-tiler API",
    description:
      "Here are endpoints that return raster OAM mosaic tiles by z-x-y.\n\nThey support filtering by date, resolution, and set of IDs.\n\nThe filter is applied only from the 10-th zoom.",
    version: "1.0.0",
  },
};
