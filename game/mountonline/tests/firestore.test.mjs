import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { pack, unpack } from "../codec.mjs";
import { Realm } from "../realm.mjs";
import { newProfile, emptyArmy } from "../data.mjs";

test("the complete supplied rules change only Mountain Blade permissions", async () => {
  const additions = await readFile(
    new URL(
      "../../../docs/mountonline/firestore-additions.rules",
      import.meta.url,
    ),
    "utf8",
  );
  const complete = await readFile(
    new URL(
      "../../../docs/mountonline/firestore-complete.rules",
      import.meta.url,
    ),
    "utf8",
  );
  const repository = await readFile(
    new URL("../../../firestore.rules", import.meta.url),
    "utf8",
  );
  assert.ok(repository.includes(additions));
  let original = complete.replace(additions + "\n\n", "");
  original = original.replace(
    `    match /games_rogueduel/{gameId} {
      allow read: if isAuthed();
      allow write: if isAuthed() && gameId != 'mountandblade_online';

      match /{document=**} {
        allow read: if isAuthed();
        allow write: if isAuthed() && gameId != 'mountandblade_online';
      }
    }`,
    `    match /games_rogueduel/{gameId} {
      allow read, write: if isAuthed();

      match /{document=**} {
        allow read, write: if isAuthed();
      }
    }`,
  );
  assert.equal(
    createHash("sha256").update(original).digest("hex"),
    "79f8f464fc7bd51a11664c6f5e887bd49ad9e9190e008439ae8eed43ff78e533",
  );
});

test("native browser compression round-trips maximum admitted simultaneous troops below the Firestore limit", async () => {
  const realm = new Realm();
  for (let i = 0; i < 16; i++) {
    const p = newProfile("Commander " + i);
    p.army = { ...emptyArmy(), recruits: 600 };
    p.level = 100;
    realm.enter("p" + i, p);
  }
  for (let i = 0; i < 8; i += 2)
    realm.startBattle("p" + i, "pvp", "p" + (i + 1));
  const state = {
    world: realm.checkpoint(),
    players: [...realm.players],
    battles: [...realm.battles],
    events: realm.events,
  };
  const encoded = await pack(state);
  assert.ok(
    encoded.length < 850000,
    `${encoded.length} bytes exceeds the checkpoint budget`,
  );
  assert.deepEqual(await unpack(encoded), state);
  const view = realm.snapshot("p0"),
    packedView = await pack(view);
  assert.ok(packedView.length < 850000);
  assert.deepEqual(await unpack(packedView), view);
  assert.equal(
    [...realm.battles.values()].reduce((n, b) => n + b.units.length, 0),
    4800,
  );
});
