import { expect, test } from "@jest/globals";
import {
  isISODateString,
  isMongoObjectIdHexString,
  buildFiltersConfigFromRequest,
  buildParametrizedFiltersQuery,
} from "../src/filters.mjs";

test("isISODateString", () => {
  expect(isISODateString("2023-08-04T14:16:36.414Z")).toBeTruthy();
  expect(isISODateString("2023-08-00T14:16:36.414Z")).toBeFalsy();
  expect(isISODateString("2007-04-05")).toBeFalsy();
  expect(isISODateString("2007-04-05T14:30Z")).toBeFalsy();
  expect(isISODateString("")).toBeFalsy();
});

test("isMongoObjectIdHexString", () => {
  expect(isMongoObjectIdHexString("64cd084423e51c0001889af8")).toBeTruthy();
  expect(isMongoObjectIdHexString("__64cd084423e51c00889af8")).toBeFalsy();
  expect(isMongoObjectIdHexString("64cd084423e51c00889af8  ")).toBeFalsy();
  expect(isMongoObjectIdHexString("")).toBeFalsy();
  expect(isMongoObjectIdHexString("64cd084423e51c00889af8")).toBeFalsy();
});

test("MosaicFiltersConfig", () => {
  const req = {
    query: {
      start: "2023-08-01T14:16:36.414Z",
      end: "2023-08-02T14:16:36.414Z",
      id: "64cd084423e51c0001889af8",
      resolution: "high",
    },
  };
  const filters = buildFiltersConfigFromRequest(req);
  expect(Array.isArray(filters.ids)).toBe(true);
});

test("ParametrizedFiltersQuery", () => {
  const q = buildParametrizedFiltersQuery('1', 1, 2, 3, {
    startDatetime: "2023-08-01T14:16:36.414Z",
    endDatetime: "2023-08-04T14:16:36.414Z",
    ids: ["64cd084423e51c0001889af8", "64cd084423e51c0001889af9"],
    resolution: "low",
  });
  const { sqlQuery, sqlQueryParams } = q;
  expect(q).toBeTruthy();
});
