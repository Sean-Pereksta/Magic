const assert = require("node:assert/strict");
const admin = require("firebase-admin");
async function main() {
  if (!process.env.FIRESTORE_EMULATOR_HOST)
    throw new Error("Run through the demo Firestore emulator.");
  const projectId = "demo-mountonline";
  await fetch(
    `http://${process.env.FIRESTORE_EMULATOR_HOST}/emulator/v1/projects/${projectId}/databases/(default)/documents`,
    { method: "DELETE" },
  );
  const app = admin.initializeApp({ projectId }, "mount-service-tests"),
    db = app.firestore(),
    { MountService } = await import("../../../server/mountonline/service.mjs"),
    { emptyArmy } = await import("../data.mjs");
  let now = 1000000;
  const auth = {
      verifyIdToken: async (token) => {
        if (!/^test-[abc]$/.test(token))
          throw Object.assign(new Error("bad token"), {
            code: "auth/invalid-id-token",
          });
        return { uid: "uid-" + token.at(-1) };
      },
    },
    make = (id) => new MountService({ db, auth, now: () => now, serverId: id });
  let service = make("host-one");
  try {
    await db
      .doc("factionForgeAccounts/uid-a")
      .set({ verified: true, username: "CaptainA" });
    await db.doc("factionForgeNames/CaptainA").set({ uid: "uid-a" });
    await db
      .doc("mountOnlineAccounts/uid-b")
      .set({ verified: true, username: "CaptainB" });
    await db.doc("games_rogueduel/mountandblade_online/players/CaptainA").set({
      user: "CaptainA",
      gold: 400,
      level: 2,
      xp: 30,
      x: 20,
      y: 60,
      army: { ...emptyArmy(), recruits: 10, footmen: 5, archers: 5 },
      inventory: { banners: ["old-banner"], equipped: [] },
    });
    await db
      .doc("games_rogueduel/mountandblade_online/castles/old-keep")
      .set({ ownerName: "CaptainA", x: 30, y: 60, garrison: 35 });
    await service.acquire();
    const a = await service.identity("test-a"),
      b = await service.identity("test-b"),
      c = await service.identity("test-c");
    assert.equal(a.username, "CaptainA");
    assert.equal(c.username, null);
    await assert.rejects(() => service.identity("wrong"), /bad token/);
    const sa = await service.join(a),
      sb = await service.join(b);
    assert.equal(sa.profile.gold, 400);
    assert.equal(sa.profile.army.recruits, 10);
    assert.equal(sa.profile.legacyInventory.banners[0], "old-banner");
    assert.ok(
      sa.settlements.some(
        (s) => s.id === "legacy-old-keep" && s.owner === a.uid,
      ),
    );
    assert.equal(sb.profile.username, "CaptainB");
    assert.ok(!sa.others.some((o) => "gold" in o || "inventory" in o));
    await assert.rejects(() => make("other-host").acquire(), /Another server/);
    await service.join(c);
    const guest = service.realm.player(c.uid);
    guest.gold = 1700;
    guest.level = 5;
    guest.supplies = 70;
    guest.skills.leadership = 2;
    guest.stats.wins = 2;
    guest.inventory = { banners: ["wayfarer"], equipped: ["wayfarer"] };
    await service.persist();
    await db
      .doc("mountOnlineAccounts/uid-c")
      .set({ verified: true, username: "CaptainC" });
    await db.doc("games_rogueduel/mountandblade_online/players/CaptainC").set({
      user: "CaptainC",
      gold: 100,
      level: 1,
      army: { ...emptyArmy(), recruits: 4 },
      inventory: { banners: ["original-c"], equipped: [] },
    });
    await db
      .doc("games_rogueduel/mountandblade_online/castles/old-c-keep")
      .set({ owner: "CaptainC", x: 50, y: 60, garrison: 22 });
    const linkedC = await service.identity("test-c"),
      merged = (await service.join(linkedC)).profile;
    assert.equal(merged.username, "CaptainC");
    assert.equal(merged.level, 5);
    assert.equal(merged.gold, 1700);
    assert.equal(merged.skills.leadership, 2);
    assert.equal(merged.stats.wins, 2);
    assert.equal(merged.supplies, 70);
    assert.deepEqual(merged.inventory.equipped, ["wayfarer"]);
    assert.ok(
      service.realm.world.settlements.some(
        (s) => s.id === "legacy-old-c-keep" && s.owner === c.uid,
      ),
    );

    const request = {
      type: "recruit",
      settlement: "hearthford",
      count: 5,
      requestId: "same-recruit-0001",
      uid: b.uid,
    };
    await Promise.all([
      service.request(a, request),
      service.request(a, request),
    ]);
    assert.equal(service.realm.player(a.uid).gold, 340);
    assert.equal(service.realm.player(a.uid).army.recruits, 15);
    assert.equal(service.realm.player(b.uid).army.recruits, 12);
    await assert.rejects(
      () => service.request(a, { type: "move", point: { x: Infinity, y: 1 } }),
      /destination/,
    );
    await assert.rejects(() =>
      service.request(a, {
        type: "train",
        unit: "dragonKnights",
        requestId: "invalid-upgrade-1",
      }),
    );
    assert.equal(service.realm.player(a.uid).gold, 340);
    const started = await service.request(a, {
        type: "engage",
        kind: "pvp",
        target: b.uid,
        requestId: "battle-start-0001",
      }),
      battleId = started.snapshot.battle.id,
      live = service.realm.battles.get(battleId);
    assert.equal(service.realm.player(b.uid).activeBattle, battleId);
    assert.equal(service.snapshot(b.uid).team, 1);
    const own = live.units.find((u) => u.team === 0),
      enemy = live.units.find((u) => u.team === 1);
    await service.request(a, {
      type: "command",
      order: "hold",
      ids: [own.id],
      point: { x: 12, y: 20 },
      formation: "square",
    });
    await assert.rejects(
      () =>
        service.request(a, {
          type: "command",
          order: "retreat",
          ids: [enemy.id],
          point: { x: 60, y: 20 },
        }),
      /own active/,
    );
    assert.equal(enemy.order.kind, "auto");
    for (let i = 0; i < 20; i++) {
      now += 50;
      await service.advance(0.05);
    }
    await service.persist();
    const checkpointTime = live.time;
    await service.close();
    service = make("host-two");
    await service.acquire();
    assert.ok(service.realm.battles.has(battleId));
    assert.equal(service.realm.battles.get(battleId).time, checkpointTime);
    assert.equal(service.realm.player(b.uid).activeBattle, battleId);
    await service.join(await service.identity("test-c"));
    service.ready = false;
    await service.acquire();
    assert.equal(service.realm.player("uid-c").username, "CaptainC");
    assert.equal(service.realm.player("uid-c").gold, 1700);
    const restored = service.realm.battles.get(battleId);
    for (const u of restored.units) if (u.team === 0) u.hp = 0;
    now += 50;
    await service.advance(0.05);
    assert.equal(restored.settled, true);
    assert.equal(service.realm.player(b.uid).stats.wins, 1);
    const goldAfter = service.realm.player(b.uid).gold;
    now += 50;
    await service.advance(0.05);
    assert.equal(service.realm.player(b.uid).gold, goldAfter);
    assert.equal(
      (await db.doc(`mountOnlineBattles/${battleId}`).get()).data().settled,
      true,
    );
    await service.request(a, request);
    assert.equal(service.realm.player(a.uid).gold, 340);
    await assert.rejects(
      () =>
        service.request(b, {
          type: "result",
          winner: 1,
          gold: 1000000,
          requestId: "forged-result-001",
        }),
      /Unknown action/,
    );
    assert.equal(service.realm.player(b.uid).gold, goldAfter);
    await service.request(b, {
      type: "allianceCreate",
      name: "The Roadwardens",
      requestId: "found-alliance-01",
    });
    const alliance = service.realm.alliances[0];
    await service.request(a, {
      type: "allianceRequest",
      alliance: alliance.id,
      requestId: "ask-alliance-0001",
    });
    await service.request(b, {
      type: "allianceAccept",
      alliance: alliance.id,
      member: a.uid,
      requestId: "accept-alliance-1",
    });
    assert.equal(service.realm.player(a.uid).allianceId, alliance.id);
    assert.equal(service.realm.player(b.uid).allianceId, alliance.id);
    await assert.rejects(
      () =>
        service.request(a, {
          type: "engage",
          kind: "pvp",
          target: b.uid,
          requestId: "friendly-fire-01",
        }),
      /Allied armies/,
    );
    const keep = service.realm.world.settlements.find(
      (c) => c.id === "legacy-old-keep",
    );
    service.realm.player(b.uid).x = keep.x - 1;
    service.realm.player(b.uid).y = keep.y;
    await assert.rejects(
      () =>
        service.request(b, {
          type: "engage",
          kind: "siege",
          target: keep.id,
          requestId: "allied-castle-01",
        }),
      /alliance protects/,
    );
    await service.request(b, {
      type: "allianceLeave",
      alliance: alliance.id,
      requestId: "leave-alliance-01",
    });
    assert.equal(service.realm.player(b.uid).allianceId, null);
    assert.equal(
      (await db.doc("mountOnlineRealm/state").get()).data().alliances[0].owner,
      a.uid,
    );
    const before = service.realm.player(b.uid).gold;
    await db
      .doc("mountOnlineRealm/authority")
      .set({ owner: "new-authority", epoch: 99, until: now + 30000 });
    await assert.rejects(
      () =>
        service.request(b, {
          type: "contract",
          id: "first-blood",
          requestId: "lease-lost-00001",
        }),
      /no longer owns/,
    );
    assert.equal(service.ready, false);
    assert.equal(service.realm.player(b.uid).gold, before);
    console.log(
      JSON.stringify({
        suite: "Mountain Blade authoritative service",
        migration: true,
        lateBindingPreservesProgress: true,
        connectedTravelerRecovery: true,
        receiptReplay: true,
        concurrentPurchases: true,
        ownership: true,
        pvp: true,
        battleRecovery: true,
        singleSettlement: true,
        alliances: true,
        alliedCastleProtection: true,
        leaseLoss: true,
        passed: true,
      }),
    );
  } finally {
    await service.close();
    await app.delete();
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
