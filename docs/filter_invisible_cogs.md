# Filtering Invisible COGs in Low Zoom Levels

## Overview

This change introduces a filter in our PostGIS queries to **exclude COGs (Cloud Optimized GeoTIFFs)** that are too small to be visible in the requested tile at lower zoom levels.  
Additionally, the number of results returned per query is limited to **100** to improve performance and avoid overwhelming the client.

## What Was Changed

1. **Maximum Results Limit**

   - Added `LIMIT 100` to all tile queries.
   - Ensures the client never receives more than 100 results for a single tile request.

2. **Visibility Filter for Small COGs**

   - When **zoom < 14**, we exclude any COG whose projected bounding box **inside the tile** is smaller than **2×2 pixels** (4 px²).
   - For **zoom ≥ 14**, we exclude COGs smaller than **1×1 pixel** (1 px²).
   - This is calculated in the query using:
     ```sql
     ST_Area(
       ST_Intersection(ST_Envelope(ST_Transform(geom, 3857)), ST_TileEnvelope(z, x, y))
     ) >= (CASE WHEN z < 14 THEN 4 ELSE 1 END) * POWER(
           156543.03392804097 / POWER(2, z), 2
         )
     ```

3. **Rationale**
   - At lower zoom levels, many COGs are so small that they are visually imperceptible.
   - Removing them:
     - Reduces query execution time.
     - Lowers network payload.
     - Keeps map rendering uncluttered.

## Example Query

Example: tile request for `z=10, x=609, y=509`:

```sql
SELECT uuid, ST_AsGeoJSON(ST_Envelope(geom)) geojson
FROM oam_meta
WHERE ST_Intersects(
        ST_Envelope(ST_Transform(geom, 3857)),
        ST_TileEnvelope(10, 609, 509)
      )
  AND ST_Area(
        ST_Intersection(
          ST_Envelope(ST_Transform(geom, 3857)),
          ST_TileEnvelope(10, 609, 509)
        )
      ) >= (CASE WHEN 10 < 14 THEN 4 ELSE 1 END) * POWER(
            156543.03392804097 / POWER(2, 10), 2
          )
ORDER BY resolution_in_meters DESC NULLS LAST, acquisition_end DESC NULLS LAST, feature_id ASC
LIMIT 100;
```

## Results Comparison

We ran a comparison across all tiles for zoom levels 0–6, summing results per zoom.

| Zoom      | Total w/o Filter | Total w/ Filter | Diff   | % Reduction | Avg/Tile w/o Filter | Avg/Tile w/ Filter |
| --------- | ---------------- | --------------- | ------ | ----------- | ------------------- | ------------------ |
| 0         | 18327            | 34              | 18293  | 99.81%      | 18327.0000          | 34.0000            |
| 1         | 18370            | 56              | 18314  | 99.70%      | 4592.5000           | 14.0000            |
| 2         | 18380            | 81              | 18299  | 99.56%      | 1148.7500           | 5.0625             |
| 3         | 18445            | 318             | 18127  | 98.28%      | 288.2031            | 4.9688             |
| 4         | 18481            | 951             | 17530  | 94.85%      | 72.1914             | 3.7148             |
| 5         | 18683            | 2926            | 15757  | 84.34%      | 18.2451             | 2.8574             |
| 6         | 19370            | 4271            | 15099  | 77.95%      | 4.7290              | 1.0427             |
| **Total** | **130056**       | **8637**        | 121419 | **93.36%**  | 23.8154             | 1.5816             |

## Summary

- The filter drastically reduces the number of results for low zoom levels, with reductions of over **99%** for zooms 0–3.
- Even at zoom 6, the number of results drops by **82%**.
- This improves performance and focuses rendering on visible, relevant imagery.

## The query to calculate statictic above

```sql
-- Comparison of the number of features "with filter" vs "without filter"
-- for all tiles from z=0 to max_zoom in the 'openaerialmap' layer

WITH
-- ====== configuration ======
cfg AS (
  SELECT
    14::int AS threshold_zoom,  -- below this zoom apply the "large" threshold
    4::int  AS px2_smallzoom,   -- area in pixels² for z < threshold_zoom
    1::int  AS px2_bigzoom,     -- area in pixels² for z >= threshold_zoom
    6::int  AS max_zoom         -- maximum zoom to iterate over
),
-- ====== select layer ======
layer_sel AS (
  SELECT id AS layer_id
  FROM public.layers
  WHERE public_id = 'openaerialmap'
),
-- ====== generate tiles ======
zlist AS (
  SELECT generate_series(0, (SELECT max_zoom FROM cfg)) AS z
),
tiles AS (
  SELECT
    z.z,
    x.i AS x,
    y.j AS y,
    ST_TileEnvelope(z.z, x.i, y.j) AS tile3857
  FROM zlist z
  JOIN LATERAL generate_series(0, (1<<z.z)-1) AS x(i) ON TRUE
  JOIN LATERAL generate_series(0, (1<<z.z)-1) AS y(j) ON TRUE
),
-- ====== candidate features ======
per_tile_features AS (
  SELECT
    t.z, t.x, t.y, t.tile3857,
    ST_Envelope(ST_Transform(f.geom, 3857)) AS env_3857
  FROM tiles t
  CROSS JOIN layer_sel ls
  JOIN LATERAL (
    SELECT geom
    FROM public.layers_features f
    WHERE f.layer_id = ls.layer_id
      AND f.geom && ST_Transform(t.tile3857, 4326)
  ) f ON TRUE
),
-- ====== count per tile ======
per_tile_counts AS (
  SELECT
    p.z, p.x, p.y,
    COUNT(*) FILTER (
      WHERE ST_Intersects(p.env_3857, p.tile3857)
    ) AS cnt_no_filter,
    COUNT(*) FILTER (
      WHERE ST_Intersects(p.env_3857, p.tile3857)
        AND ST_Area(
              ST_Intersection(p.env_3857, p.tile3857)
            ) >= (
                  CASE
                    WHEN p.z < (SELECT threshold_zoom FROM cfg)
                      THEN (SELECT px2_smallzoom FROM cfg)
                    ELSE (SELECT px2_bigzoom   FROM cfg)
                  END
                ) * POWER(156543.03392804097 / POWER(2, p.z), 2)
    ) AS cnt_with_filter
  FROM per_tile_features p
  GROUP BY p.z, p.x, p.y
),
-- ====== aggregate per zoom ======
by_zoom AS (
  SELECT
    z,
    SUM(cnt_no_filter)  AS total_no_filter,
    SUM(cnt_with_filter) AS total_with_filter,
    (2^z * 2^z)::bigint AS tile_count -- total tiles at this zoom
  FROM per_tile_counts
  GROUP BY z
),
-- ====== grand total ======
grand_total AS (
  SELECT
    NULL::int AS z,
    SUM(total_no_filter)  AS total_no_filter,
    SUM(total_with_filter) AS total_with_filter,
    SUM(tile_count)        AS tile_count
  FROM by_zoom
)
-- ====== output ======
SELECT
  z,
  total_no_filter,
  total_with_filter,
  (total_no_filter - total_with_filter) AS diff,
  CASE
    WHEN total_no_filter > 0
      THEN ROUND(100.0 * (total_no_filter - total_with_filter) / total_no_filter, 2)
    ELSE 0
  END AS pct_reduction,
  ROUND(total_no_filter::numeric / tile_count, 4) AS avg_per_tile_no_filter,
  ROUND(total_with_filter::numeric / tile_count, 4) AS avg_per_tile_with_filter
FROM by_zoom

UNION ALL

SELECT
  z,
  total_no_filter,
  total_with_filter,
  (total_no_filter - total_with_filter) AS diff,
  CASE
    WHEN total_no_filter > 0
      THEN ROUND(100.0 * (total_no_filter - total_with_filter) / total_no_filter, 2)
    ELSE 0
  END AS pct_reduction,
  ROUND(total_no_filter::numeric / tile_count, 4) AS avg_per_tile_no_filter,
  ROUND(total_with_filter::numeric / tile_count, 4) AS avg_per_tile_with_filter
FROM grand_total
ORDER BY z NULLS LAST;
```
