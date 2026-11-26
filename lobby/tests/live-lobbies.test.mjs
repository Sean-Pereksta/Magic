import assert from "node:assert/strict";
import {test} from "node:test";
import directory from "../../shared/managed-servers.js";

const origin = "https://lobby-one.trycloudflare.com";

test("each Live Lobbies join keeps the selected host and its native handoff", () => {
  assert.equal(directory.projectId, "magic-servers-1ca0c");
  for (const game of ["acornrally", "bannerlands", "trashtactics", "lastsignal", "cat"]) {
    const acorn = game === "acornrally";
    const url = origin + (acorn ? "/?room=join" : "/__catnmice/join");
    const room = {game, url, origin, visibility: "public"};
    const href = new URL(directory.joinUrl(room));
    assert.equal(href.origin, origin);
    assert.equal(href.pathname, acorn ? "/" : "/__catnmice/join");
    assert.equal(href.search, acorn ? "?room=join" : "");
    assert.equal(href.hash, acorn ? "#join=1" : "");
  }
});

test("an expired server cannot enter another match", async () => {
  assert.equal(await directory.probe({expires: Date.now() - 1}), null);
});
