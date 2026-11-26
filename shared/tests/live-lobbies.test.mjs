import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import {test} from "node:test";
import {runInNewContext} from "node:vm";
import directory from "../managed-servers.js";

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

test("flat server rows preserve distinct games using the same join code", async () => {
  function node(properties = {}) {
    const events = new Map();
    return {
      dataset: {},
      children: [],
      addEventListener: (type, fn) => events.set(type, fn),
      fire: type => events.get(type)?.(),
      setAttribute(key, value) { this[key] = value; },
      append(...children) { this.children.push(...children); },
      replaceChildren(...children) { this.children = children; },
      ...properties
    };
  }

  const rooms = node();
  const search = node({value: ""});
  const all = node({dataset: {liveFilter: "all"}});
  const cat = node({dataset: {liveFilter: "catandmouse"}});
  const controls = new Map([
    ["[data-live-rooms]", rooms], ["[data-live-status]", node()], ["[data-live-feedback]", node()],
    ["[data-live-search]", search], ["[data-live-refresh]", node()]
  ]);
  const section = {
    closest: () => ({classList: {contains: () => true}}),
    querySelector: selector => controls.get(selector),
    querySelectorAll: () => [all, cat]
  };
  const listing = {code: "1234", origin, visibility: "public", phase: "waiting", players: 1,
    capacity: 8, expires: Date.now() + 30_000, joinable: true};
  const acorn = {...listing, game: "acornrally", name: "Forest", url: origin + "/?room=join"};
  const catOrigin = "https://lobby-two.trycloudflare.com";
  const mouse = {...listing, game: "cat", name: "Cheese", origin: catOrigin, url: catOrigin + "/__catnmice/join"};
  let assigned, checked;
  const reader = {
    ...directory,
    publicServers: async game => [acorn, mouse].filter(room => room.game === game),
    probe: async room => (checked = room)
  };
  const script = readFileSync(new URL("../../lobby/live-lobbies.mjs", import.meta.url), "utf8")
    .replace(/^import [^\n]*\n/, "");
  runInNewContext(script, {
    document: {getElementById: () => section, querySelector() {}, createElement: () => node(), addEventListener() {}},
    window: {CatnmiceServers: reader, addEventListener() {}},
    location: {hash: "", assign: url => { assigned = url; }},
    MutationObserver: class { observe() {} },
    setInterval() {}
  });
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(rooms.children.map(row => row.dataset.roomId), ["acornrally:1234", "cat:1234"]);
  search.value = "acorn rally";
  search.fire("input");
  assert.deepEqual(rooms.children.map(row => row.dataset.roomId), ["acornrally:1234"]);
  search.value = "";
  cat.fire("click");
  assert.deepEqual(rooms.children.map(row => row.dataset.roomId), ["cat:1234"]);
  await rooms.children[0].children.at(-1).fire("click");
  assert.equal(checked, mouse);
  assert.equal(assigned, catOrigin + "/__catnmice/join");
});
