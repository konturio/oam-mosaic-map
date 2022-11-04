import { jest } from "@jest/globals";
import fs from "fs";

jest.setTimeout(30000);

process.env.TITILER_BASE_URL = "https://test-apps02.konturlabs.com/titiler/";

const { requestReor20DepthTile } = await import("../reor20.mjs");

test("requestReor20DepthTile(15, 7684, 13540)", async () => {
  const tile = await requestReor20DepthTile(15, 7684, 13540);

  expect(
    Buffer.compare(fs.readFileSync("./__tests__/reor.png"), tile.image.buffer)
  ).toBe(0);
});
