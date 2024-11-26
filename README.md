# Mosaic Tiler

A server that generates tiles containing all images present in OpenAerialMap.

![Map](map.png)

## About

Mosaic tiler is a supplementary application that operates as an additional layer on top of a dynamic raster tiler. Its function is to combine and cache tiles from multiple images.

## Dependencies:

TiTIler
PostgreSQL with installed PostGIS

## Installation:

Please note that you are expected to have preinstalled TiTiler and PostgreSQL with ingested OAM images index.

```bash
git clone https://github.com/konturio/oam-mosaic-map
cd oam-mosaic-map
npm i
npm start
```

It is necessary to have the following environment variables properly set:

- `BASE_URL` is the root URL of the server.
- `TITILER_BASE_URL` is the URL of your Titiler installation.
- `TILES_CACHE_DIR_PATH` is the path for the tiles cache.
- `PGDATABASE`, `PORT`, `PGHOST`, `PGUSER` and `PGPASSWORD` are PostgreSQL-related variables.

API docs and Swagger UI url: BASE_URL/api-docs/

## k8s

Ready Helm configuration for mosaic tiler:
https://github.com/konturio/disaster-ninja-cd/tree/main/helm/raster-tiler

## How does a mosaic tiler work:

When the Mosaic Tiler receives a request for a tile, it handles it in the following steps:

1. Query the list of geotiffs contained within the requested tile from the database. Using PostgreSQL with PostGIS allows delegating the handling of spatial queries from the tiler to PostGIS.
2. Process each of the contained images, either by requesting TiTiler to generate a tile for a geotiff or by retrieving a tile from the cache. It is important to note that the Mosaic Tiler does not handle geotiffs directly and instead delegates this responsibility entirely to TiTiler, focusing only on working with PNG and JPEG tile images.

3. Create the resulting mosaic tile by stacking the tiles of all contained images on top of each other in a specific order (currently, the images are sorted based on their resolution, with the highest quality images appearing at the top).

## Logging

To setup logging level use LOG_LEVEL env variable:

```sh
LOG_LEVEL=error node src/main.mjs
```

Available log levels:

```js
const levels = {
  error: 0,
  warn: 1,
  info: 2,
  http: 3,
  verbose: 4,
  debug: 5,
  silly: 6,
};
```

## How to run mosaic map locally

1. Make sure pgadmin is installed and running on your machine.
2. Install kubectl and/or Lens on your machine.
3. Set up active port forwarding to the production layers-db.
4. Open pgadmin and create a new server. Fill the credentials as follows:

   - Name: mosaic-map-prod
   - Hostname: localhost
   - Port: 'the port you are forwarding to'
   - Maintenance database: layers-db
   - Username: 'username to connect to the layers-db'
   - Password: 'password to connect to the layers-db'
   - Click save and then connect to the server.

5. Run the project locally using docker compose:

   ```bash
   docker-compose up --build
   ```

6. Connect pgadmin to your local database:

   - Right-click on Servers -> Create -> Server
   - Fill the credentials as follows:
     - Name: mosaic-map-local
     - Hostname: localhost
     - Port: 5432
     - Maintenance database: postgres
     - Username: postgres
     - Password: postgres
       Click save and then connect to the server.

7. Export the data from the production database to your local database as csv files:

   - Right-click on the `layers_features` table -> PSQL Tool
   - Run the one more query:

     ```sql
      \COPY (SELECT feature_id, 1 AS layer_id, properties, geom, last_updated, zoom FROM public.layers_features WHERE layer_id = (SELECT id FROM public.layers WHERE public_id = 'openaerialmap') ORDER BY last_updated DESC LIMIT 100) TO '/path_on_your_pc/exported_file.csv'WITH CSV HEADER DELIMITER ',' QUOTE '`';
     ```

8. Import the csv files to your local database:

   - Right-click on the local database `layers_features` table -> Import/Export Data
   - Select the exported csv file and click on the Import button.
   - Don't forget to set quote character to '`' and delimiter to ','.
   - Click on the Import button.

9. Open the browser and navigate to http://localhost:8001/mosaic-viewer to view the mosaic map.

## Environment variables

| Name                    | Description                             | Default Value   |
| ----------------------- | --------------------------------------- | --------------- |
| `PGHOST`                | PostgreSQL host                         |                 |
| `PGUSER`                | PostgreSQL user                         |                 |
| `PGPASSWORD`            | PostgreSQL password                     |                 |
| `PGDATABASE`            | PostgreSQL database name                | `postgres`      |
| `OAM_LAYER_ID`          | OpenAerialMap layer ID                  | `openaerialmap` |
| `PORT`                  | Server port                             |                 |
| `BASE_URL`              | Root URL of the server                  |                 |
| `TITILER_BASE_URL`      | URL of your Titiler installation        |                 |
| `TILES_CACHE_DIR_PATH`  | Path for the tiles cache                | `/tiles`        |
| `LOG_LEVEL`             | Logging level                           |                 |
| `DB_POOL_SIZE`          | Size of the PostgreSQL connection pool  | `16`            |
| `DB_DISABLE_SSL`        | Disable SSL for PostgreSQL connection   | `false`         |
| `TILE_FETCH_TIMEOUT_MS` | Tile fetch timeout in milliseconds      | `60000`         |
| `FETCH_QUEUE_TTL_MS`    | Fetch promise queue TTL in milliseconds | `600000`        |

## Tests

To run tests:

```bash
npm test
```

To update snapshots:

```bash
UPDATE_SNAPSHOTS=1 npm test
```
