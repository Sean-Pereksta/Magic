import assert from "node:assert/strict";
import {test} from "node:test";
import {catRooms, joinHref} from "../live-lobbies.mjs";

const now = Date.parse("2026-11-17T12:00:00Z");
const room = {
  game: "catandmouse", id: "open-room", roomName: "Flour Mill", hostName: "Host",
  map: "flour-mill", difficulty: "Standard", capacity: 4, openSeats: 2, version: "1",
  serverUrl: "https://flour-mill.trycloudflare.com", expiresAt: new Date(now + 30_000).toISOString()
};

test("only fresh public rooms with the right version and space enable joining", () => {
  const result = catRooms([
    room,
    {...room, id: "full", openSeats: 0},
    {...room, id: "requires-update", version: "older"},
    {...room, id: "expired", expiresAt: new Date(now).toISOString()},
    {...room, id: "private", public: false},
    {...room, id: "nonpublic-host", serverUrl: "http://127.0.0.1:8787/"},
    {...room, id: "untrusted-path", serverUrl: room.serverUrl + "/elsewhere"},
    {...room, id: "untrusted-id#join"}
  ], {version: "1", now});
  assert.deepEqual(result.map(x => [x.id, x.joinable]), [
    ["open-room", true], ["full", false], ["requires-update", false]
  ]);
});

test("both guest links carry the selected room and an Arcade return address", () => {
  const arcade = "https://www.catnmice.com/lobby/lobby.html?username=host#ignored";
  const [catRoom] = catRooms([room], {version: "1", now});
  for (const game of ["catandmouse", "bannerlands"]) {
    const destination = new URL(joinHref(game, catRoom, arcade));
    const request = new URLSearchParams(destination.hash.slice(1));
    assert.equal(request.get("room"), "open-room");
    assert.equal(request.get("returnUrl"), "https://www.catnmice.com/lobby/lobby.html#live-lobbies");
    if (game === "catandmouse") {
      assert.equal(destination.origin, room.serverUrl);
      assert.equal(destination.pathname, "/");
    } else {
      assert.equal(destination.origin, "https://sean-pereksta.github.io");
      assert.equal(destination.pathname, "/Truth/games/bannerlands/");
      assert.equal(request.get("host"), room.serverUrl);
    }
  }
});
