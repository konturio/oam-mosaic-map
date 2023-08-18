const mongoObjectIdHexStringRx = /^[a-f\d]{24}$/i;
/**
 * @param {string} str
 */
export function isMongoObjectIdHexString(str) {
  return mongoObjectIdHexStringRx.test(str);
}

/**
 * check if string is ISO 8601 date YYYY-MM-DDTHH:mm:ss.sssZ or ±YYYYYY-MM-DDTHH:mm:ss.sssZ
 * @param {string} val
 */
export function isISODateString(val) {
  const d = new Date(val);
  return !Number.isNaN(d.valueOf()) && d.toISOString() === val;
}

/** @typedef {{ startDatetime?: string, endDatetime?: string, ids?: string[], resolution?: "high"|"medium"|"low" }} MosaicFiltersConfig */

/**
 *
 * @param {import('express').Request} req
 * @returns {MosaicFiltersConfig}
 */
export function buildFiltersConfigFromRequest(req) {
  const filters = {};

  // Dates
  if (req.query.start && isISODateString(req.query.start)) {
    filters.startDatetime = req.query.start;
  }
  if (req.query.end && isISODateString(req.query.end)) {
    filters.endDatetime = req.query.end;
  }

  // IDs
  if (req.query.id) {
    const ids = Array.isArray(req.query.id) ? req.query.id : [req.query.id];
    if (ids.every(isMongoObjectIdHexString)) {
      filters.ids = ids;
    }
  }

  // Resolution
  // (0-1) [1-5) [5-)
  if (req.query.resolution && ["high", "medium", "low"].includes(req.query.resolution)) {
    filters.resolution = req.query.resolution;
  }

  return filters;
}

/**
 * @param {string} OAM_LAYER_ID
 * @param {number} z
 * @param {number} x
 * @param {number} y
 * @param {MosaicFiltersConfig} filters
 */
export function buildParametrizedFiltersQuery(OAM_LAYER_ID, z, x, y, filters = {}) {
  const tags = [];
  /** @type {Array<unknown>} */
  let sqlQueryParams = [z, x, y];
  let sqlWhereClause = "ST_TileEnvelope($1, $2, $3) && ST_Transform(geom, 3857)";
  let nextParamIndex = 4;

  // filter by date
  if (filters.startDatetime) {
    sqlWhereClause += ` and (uploaded_at >= $${nextParamIndex++}::timestamptz)`;
    sqlQueryParams.push(filters.startDatetime);
    tags.push("start");
  }
  if (filters.endDatetime) {
    sqlWhereClause += ` and (uploaded_at <= $${nextParamIndex++}::timestamptz)`;
    sqlQueryParams.push(filters.endDatetime);
    tags.push("end");
  }

  // filter by ids - expects ids=String[]
  if (filters.ids) {
    sqlWhereClause += ` and (feature_id = ANY($${nextParamIndex++}))`;
    sqlQueryParams.push(filters.ids);
    tags.push("ids");
  }

  // filter by resolution
  if (filters.resolution) {
    tags.push(filters.resolution);
    switch (filters.resolution) {
      case "high":
        sqlWhereClause += ` and (resolution_in_meters < 1)`;
        break;
      case "medium":
        sqlWhereClause += ` and (resolution_in_meters >= 1 and resolution_in_meters < 5)`;
        break;
      case "low":
        sqlWhereClause += ` and (resolution_in_meters >= 5)`;
        break;
      default:
        break;
    }
  }

  const sqlQuery = `with oam_meta as (
    select
      feature_id,
      (properties->>'gsd')::real as resolution_in_meters, 
      (properties->>'uploaded_at')::timestamptz as uploaded_at,
      properties->>'uuid' as uuid, 
      geom
    from public.layers_features
    where layer_id = (select id from public.layers where public_id = '${OAM_LAYER_ID}')
  )
  select uuid, ST_AsGeoJSON(ST_Envelope(geom)) geojson
  from oam_meta
  where ${sqlWhereClause}
  order by resolution_in_meters desc nulls last, uploaded_at desc nulls last, feature_id asc`;

  return { sqlQuery, sqlQueryParams, queryTag: tags.join("_") };
}
