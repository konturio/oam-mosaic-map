import cover from "@mapbox/tile-cover";

const tileCoverCache = {
  0: new WeakMap(),
  1: new WeakMap(),
  2: new WeakMap(),
  3: new WeakMap(),
  4: new WeakMap(),
  5: new WeakMap(),
  6: new WeakMap(),
  7: new WeakMap(),
  8: new WeakMap(),
  9: new WeakMap(),
  10: new WeakMap(),
  11: new WeakMap(),
  12: new WeakMap(),
  13: new WeakMap(),
  14: new WeakMap(),
  15: new WeakMap(),
  16: new WeakMap(),
  17: new WeakMap(),
  18: new WeakMap(),
  19: new WeakMap(),
  20: new WeakMap(),
  21: new WeakMap(),
  22: new WeakMap(),
  23: new WeakMap(),
  24: new WeakMap(),
  25: new WeakMap(),
};

function getTileCover(geojson, zoom) {
  if (tileCoverCache[zoom].get(geojson)) {
    return tileCoverCache[zoom].get(geojson);
  }

  const tileCover = cover.tiles(geojson, { min_zoom: zoom, max_zoom: zoom });
  tileCoverCache[zoom].set(geojson, tileCover);

  return tileCover;
}

export { getTileCover };
