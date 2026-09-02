import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";

import {
  campaignCloudDocumentNamespace,
  cleanCampaignCloudName,
  normalizeCampaignCloudName,
  validateCampaignCloudProfileShape
} from "../campaign/campaign-cloud-save.js";

test("campaign cloud save names are stable, trimmed, and case-insensitive", () => {
  assert.equal(cleanCampaignCloudName("  Varek   Campaign  "), "Varek Campaign");
  assert.equal(normalizeCampaignCloudName("  Varek   Campaign  "), "varek campaign");
  assert.equal(
    campaignCloudDocumentNamespace("VAREK campaign"),
    campaignCloudDocumentNamespace("  varek   CAMPAIGN ")
  );
});

test("campaign cloud save validation rejects incomplete payloads", () => {
  assert.equal(validateCampaignCloudProfileShape(null), false);
  assert.equal(validateCampaignCloudProfileShape({ runId: "campaign_1", deck: [] }), false);
  assert.equal(validateCampaignCloudProfileShape({
    runId: "campaign_1",
    deck: ["starter_coin"],
    region: 2,
    level: 4
  }), true);
});

test("campaign state bootstraps the browser cloud-save module", async () => {
  const source = await fs.readFile(new URL("../campaign/campaign-state.js", import.meta.url), "utf8");
  assert.match(source, /campaign-cloud-save\.js\?v=1/);
  assert.match(source, /installCampaignCloudSave/);
  assert.match(source, /loadCampaignProfile/);
  assert.match(source, /saveCampaignProfile/);
});
