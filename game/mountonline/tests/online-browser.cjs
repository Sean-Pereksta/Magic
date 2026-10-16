// Two real browser clients use the real modular Firestore SDK and security rules.
// The static file server is test tooling only; there is no game/API server.
const assert = require("node:assert/strict"),
  fs = require("node:fs"),
  path = require("node:path");
const { initializeTestEnvironment } = require("@firebase/rules-unit-testing");
const sdk = require("firebase/firestore");
const { start } = require("./serve.cjs"),
  { launch } = require("./chromium.cjs");

async function main() {
  if (!process.env.FIRESTORE_EMULATOR_HOST)
    throw new Error("Use the demo Firestore emulator.");
  const [host, port] = process.env.FIRESTORE_EMULATOR_HOST.split(":");
  const env = await initializeTestEnvironment({
    projectId: "demo-mountonline",
    firestore: {
      host,
      port: Number(port),
      rules: fs.readFileSync(
        path.resolve(__dirname, "../../../firestore.rules"),
        "utf8",
      ),
    },
  });
  const { server, origin } = await start();
  let browser;
  const errors = [],
    apiRequests = [];
  try {
    await env.clearFirestore();
    const { newProfile, emptyArmy } = await import("../data.mjs");
    await env.withSecurityRulesDisabled(async (context) => {
      const db = context.firestore();
      for (const letter of ["a", "b"]) {
        const uid = "uid-" + letter,
          username = "Captain" + letter.toUpperCase();
        await sdk.setDoc(sdk.doc(db, "mountOnlineProfiles/" + uid), {
          ...newProfile(username),
          uid,
          gold: 1000,
          migrated: true,
          x: letter === "a" ? 15.5 : 18.5,
          y: 44.5,
        });
        await sdk.setDoc(sdk.doc(db, "mountOnlineAccounts/" + uid), {
          username,
          verified: true,
        });
        await sdk.setDoc(sdk.doc(db, "mountOnlineNames/" + username), { uid });
      }
      await sdk.setDoc(
        sdk.doc(db, "games_rogueduel/mountandblade_online/players/OldCaptain"),
        {
          gold: 1900,
          level: 4,
          xp: 75,
          army: { ...emptyArmy(), recruits: 130, footmen: 10 },
          inventory: {
            banners: ["wayfarer"],
            equipped: ["wayfarer"],
            archived: "heirloom",
          },
        },
      );
      await sdk.setDoc(
        sdk.doc(db, "games_rogueduel/mountandblade_online/castles/ancestral"),
        {
          owner: "OldCaptain",
          ownerName: "OldCaptain",
          x: 35,
          y: 55,
          garrison: 88,
        },
      );
    });
    browser = await launch();
    async function contextFor(letter, touch = false) {
      const context = await browser.newContext({
        viewport: touch
          ? { width: 390, height: 844 }
          : { width: 1280, height: 800 },
        hasTouch: touch,
        isMobile: touch,
      });
      // Browser Firebase distribution is already self-contained except app.js.
      await context.route("https://www.gstatic.com/firebasejs/**", (route) => {
        const name = new URL(route.request().url()).pathname.split("/").at(-1);
        if (!["firebase-app.js", "firebase-firestore.js"].includes(name))
          return route.abort();
        return route.fulfill({
          contentType: "text/javascript",
          body: fs.readFileSync(
            path.resolve(__dirname, "../../../node_modules/firebase", name),
          ),
        });
      });
      await context.route("**/game/mountonline/firebase.mjs", (route) =>
        route.fulfill({
          contentType: "text/javascript",
          body: `
import { initializeApp } from 'https://www.gstatic.com/firebasejs/12.0.0/firebase-app.js';
import * as sdk from 'https://www.gstatic.com/firebasejs/12.0.0/firebase-firestore.js';
export async function connectFirebase() {
  const app = initializeApp({ projectId:'demo-mountonline', apiKey:'test-only', appId:'test-only' });
  const db = sdk.getFirestore(app);
  sdk.connectFirestoreEmulator(db, ${JSON.stringify(host)}, ${Number(port)}, { mockUserToken: { sub:'uid-${letter}', user_id:'uid-${letter}' } });
  sdk.setLogLevel('silent');
  return { db, sdk, user:{ uid:'uid-${letter}' } };
}`,
        }),
      );
      context.on("page", (page) => {
        page.on("pageerror", (error) => errors.push(error.message));
        page.on("request", (req) => {
          if (/\/api\/mountonline|\/socket\.io/.test(req.url()))
            apiRequests.push(req.url());
        });
      });
      return context;
    }
    async function join(context, name = "SpoofedURLName") {
      const page = await context.newPage();
      await page.goto(
        origin + "/game/mountonline.html?username=" + encodeURIComponent(name),
      );
      await page.waitForFunction(
        () => !document.querySelector("#startOnline").disabled,
      );
      await page.click("#startOnline");
      await page.waitForFunction(
        () =>
          __MBO_TEST__.mode === "online" &&
          !document.getElementById("app").hidden &&
          __MBO_TEST__.network?.connected,
        null,
        { timeout: 35000 },
      );
      return page;
    }
    const ac = await contextFor("a"),
      bc = await contextFor("b", true);
    let a = await join(ac),
      b = await join(bc);
    await a.waitForFunction(() => __MBO_TEST__.network.authority?.ready);
    await a.waitForFunction(() =>
      __MBO_TEST__.state.others.some((p) => p.username === "CaptainB"),
    );
    assert.equal(await a.locator("#commanderName").textContent(), "CaptainA");
    assert.equal(await b.locator("#commanderName").textContent(), "CaptainB");
    const startX = await a.evaluate(() => __MBO_TEST__.state.profile.x);
    await a.evaluate(async () => {
      const p = __MBO_TEST__.state.profile;
      await __MBO_TEST__.network.action({
        type: "move",
        point: { x: p.x + 1, y: p.y },
      });
    });
    await a.waitForFunction(
      (x) => __MBO_TEST__.state.profile.x > x + 0.4,
      startX,
    );
    await a.evaluate(() => __MBO_TEST__.network.action({ type: "halt" }));
    const purchased = await a.evaluate(async () => {
      const n = __MBO_TEST__.network,
        sdk = n.sdk;
      const input = {
        type: "train",
        unit: "footmen",
        count: 1,
        requestId: "same-purchase-001",
      };
      const before = (
        await sdk.getDoc(n.ref("mountOnlineProfiles/" + n.uid))
      ).data();
      await Promise.all([n.action(input), n.action(input)]);
      await n.action(input);
      const after = (
        await sdk.getDoc(n.ref("mountOnlineProfiles/" + n.uid))
      ).data();
      return {
        before: before.gold,
        after: after.gold,
        count: after.army.footmen - before.army.footmen,
      };
    });
    console.log("Firestore purchase replay checked");
    assert.equal(purchased.count, 1);
    assert.ok(purchased.after < purchased.before);
    const denials = await b.evaluate(async () => {
      const n = __MBO_TEST__.network;
      async function denied(fn) {
        try {
          await fn();
          return false;
        } catch (e) {
          return e.code === "permission-denied";
        }
      }
      return {
        otherSave: await denied(() =>
          n.sdk.updateDoc(n.ref("mountOnlineProfiles/uid-a"), { gold: 999999 }),
        ),
        ownInflation: await denied(() =>
          n.sdk.updateDoc(n.ref("mountOnlineProfiles/uid-b"), { gold: 999999 }),
        ),
        stealLease: await denied(() =>
          n.sdk.setDoc(n.ref("mountOnlineRealms/living-realm"), {
            schemaVersion: 1,
            ownerUid: n.uid,
            ownerSession: n.sessionId,
            epoch: 2,
            released: false,
            renewedAt: n.sdk.serverTimestamp(),
          }),
        ),
      };
    });
    assert.deepEqual(denials, {
      otherSave: true,
      ownInflation: true,
      stealLease: true,
    });
    const position = await a.evaluate(() => {
      const o = __MBO_TEST__.state.others.find((p) => p.id === "uid-b");
      return __MBO_TEST__.renderer.toScreen(o.x, o.y);
    });
    await a.mouse.click(position.x, position.y);
    await a.waitForSelector('[data-context="engage"]');
    await a.click('[data-context="engage"]');
    await Promise.all([
      a.waitForFunction(() => !!__MBO_TEST__.battle),
      b.waitForFunction(() => !!__MBO_TEST__.battle),
    ]);
    console.log("Shared Firestore battle started");
    const battleId = await a.evaluate(() => __MBO_TEST__.battle.id);
    assert.equal(await b.evaluate(() => __MBO_TEST__.battle.id), battleId);
    assert.equal(await b.evaluate(() => __MBO_TEST__.team), 1);
    await a.click('[data-order="hold"]');
    await b.click('[data-order="hold"]');
    await a.waitForFunction(() =>
      __MBO_TEST__.battle.units
        .filter((u) => u.team === 0)
        .every((u) => u.order.kind === "hold"),
    );
    const enemyDenied = await a.evaluate(async () => {
      const enemy = __MBO_TEST__.battle.units.find((u) => u.team === 1);
      try {
        await __MBO_TEST__.network.action({
          type: "command",
          order: "retreat",
          ids: [enemy.id],
          point: { x: 1, y: 1 },
        });
        return false;
      } catch (error) {
        return /own|army|command|select/i.test(error.message);
      }
    });
    assert.equal(enemyDenied, true);
    await b.close();
    b = await join(bc);
    await b.waitForFunction(() => !!__MBO_TEST__.battle);
    assert.equal(await b.evaluate(() => __MBO_TEST__.battle.id), battleId);
    assert.equal(await b.evaluate(() => __MBO_TEST__.team), 1);
    // Kill the elected host's tab without running stop(): the survivor must
    // acquire the expired lease and restore the same unfinished battle.
    await a.close();
    await b.waitForFunction(() => __MBO_TEST__.network.authority?.ready, null, {
      timeout: 35000,
    });
    assert.equal(await b.evaluate(() => __MBO_TEST__.battle.id), battleId);
    console.log("Abrupt host handover restored the battle");
    a = await join(ac);
    await a.waitForFunction(() => !!__MBO_TEST__.battle);
    assert.equal(await a.evaluate(() => __MBO_TEST__.battle.id), battleId);
    await a.click('[data-order="charge"]');
    await b.click('[data-order="charge"]');
    await Promise.all([
      a.waitForFunction(() => document.getElementById("result").open, null, {
        timeout: 65000,
      }),
      b.waitForFunction(() => document.getElementById("result").open, null, {
        timeout: 65000,
      }),
    ]);
    let profiles;
    await env.withSecurityRulesDisabled(async (context) => {
      const db = context.firestore();
      profiles = await Promise.all(
        ["uid-a", "uid-b"].map(async (uid) =>
          (await sdk.getDoc(sdk.doc(db, "mountOnlineProfiles/" + uid))).data(),
        ),
      );
    });
    assert.equal(profiles[0].stats.battles, 1);
    assert.equal(profiles[1].stats.battles, 1);
    assert.equal(profiles[0].stats.wins + profiles[1].stats.wins, 1);
    await a.click("#returnRealm");
    await b.click("#returnRealm");
    await a.evaluate(() =>
      __MBO_TEST__.network.action({
        type: "allianceCreate",
        name: "Oak Banner",
      }),
    );
    await a.waitForFunction(() => !!__MBO_TEST__.state.profile.allianceId);
    const alliance = await a.evaluate(
      () => __MBO_TEST__.state.profile.allianceId,
    );
    await b.evaluate(
      (alliance) =>
        __MBO_TEST__.network.action({ type: "allianceRequest", alliance }),
      alliance,
    );
    await a.evaluate(
      (alliance) =>
        __MBO_TEST__.network.action({
          type: "allianceAccept",
          alliance,
          member: "uid-b",
        }),
      alliance,
    );
    await b.waitForFunction(
      (id) => __MBO_TEST__.state.profile.allianceId === id,
      alliance,
    );
    assert.equal(
      await a.evaluate(async () => {
        try {
          await __MBO_TEST__.network.action({
            type: "engage",
            kind: "pvp",
            target: "uid-b",
          });
          return false;
        } catch (error) {
          return /allied/i.test(error.message);
        }
      }),
      true,
    );
    await b.evaluate(
      (alliance) =>
        __MBO_TEST__.network.action({ type: "allianceLeave", alliance }),
      alliance,
    );
    await b.waitForFunction(() => !__MBO_TEST__.state.profile.allianceId);
    // Direct saves remain UID-bound; a legacy display name in a URL imports nothing.
    const cc = await contextFor("c");
    let c = await join(cc, "OldCaptain");
    assert.equal(await c.evaluate(() => __MBO_TEST__.state.profile.gold), 260);
    assert.equal(
      await c.evaluate(() => __MBO_TEST__.state.profile.linked),
      false,
    );
    await env.withSecurityRulesDisabled(async (context) => {
      const db = context.firestore();
      await sdk.setDoc(sdk.doc(db, "mountOnlineAccounts/uid-c"), {
        username: "OldCaptain",
        verified: true,
      });
      await sdk.setDoc(sdk.doc(db, "mountOnlineNames/OldCaptain"), {
        uid: "uid-c",
      });
    });
    await c.evaluate(async () => {
      await __MBO_TEST__.network.stop();
    });
    await c.close();
    c = await join(cc, "OldCaptain");
    await c.waitForFunction(() => __MBO_TEST__.state.profile.migrated === true);
    const migrated = await c.evaluate(() => {
      const p = __MBO_TEST__.state.profile;
      return {
        gold: p.gold,
        total: p.army.recruits + p.reserve.recruits,
        heirloom: p.legacyInventory.archived,
        keep: __MBO_TEST__.state.settlements.find(
          (s) => s.id === "legacy-ancestral",
        )?.garrison,
      };
    });
    assert.deepEqual(migrated, {
      gold: 1900,
      total: 130,
      heirloom: "heirloom",
      keep: 88,
    });
    await c.evaluate(async () => {
      await __MBO_TEST__.network.stop();
    });
    await c.close();
    c = await join(cc, "OldCaptain");
    assert.equal(
      await c.evaluate(
        () =>
          __MBO_TEST__.state.profile.army.recruits +
          __MBO_TEST__.state.profile.reserve.recruits,
      ),
      130,
    );
    const newerTab = await join(cc, "OldCaptain");
    await c.evaluate(async () => {
      const n = __MBO_TEST__.network;
      try {
        await n.heartbeat();
      } catch (error) {
        n.connectionError(error);
      }
    });
    assert.equal(await c.evaluate(() => __MBO_TEST__.network.stopped), true);
    assert.equal(
      await newerTab.evaluate(() => __MBO_TEST__.network.connected),
      true,
    );
    await newerTab.evaluate(async () => {
      await __MBO_TEST__.network.stop();
    });
    await a.evaluate(() => __MBO_TEST__.returnMenu());
    await b.evaluate(() => __MBO_TEST__.returnMenu());
    await c.evaluate(async () => {
      await __MBO_TEST__.network.stop();
    });
    assert.deepEqual(errors, []);
    assert.deepEqual(apiRequests, []);
    console.log(
      JSON.stringify({
        suite: "Firestore-only browser multiplayer",
        desktopAndTouch: true,
        realSecurityRules: true,
        idempotentPurchase: true,
        unauthorizedWritesDenied: true,
        sharedBattle: true,
        movementAndAlliances: true,
        duplicateTabOwnership: true,
        enemyOrdersRejected: true,
        reconnectedSeat: true,
        abruptHostHandover: true,
        oneBattleResult: true,
        verifiedLegacyMigration: true,
        noNodeOrSocketIORequests: true,
        passed: true,
      }),
    );
    await ac.close();
    await bc.close();
    await cc.close();
  } catch (error) {
    console.error(
      "command diagnostic",
      await env
        .withSecurityRulesDisabled(async (context) =>
          (
            await sdk.getDocs(
              sdk.collection(
                context.firestore(),
                "mountOnlineRealms/living-realm/commands",
              ),
            )
          ).docs.map((d) => ({
            id: d.id,
            type: d.data().type,
            state: d.data().state,
            result: d.data().result,
          })),
        )
        .catch(() => []),
    );
    if (browser)
      for (const context of browser.contexts())
        for (const page of context.pages())
          console.error(
            "browser diagnostic",
            await page
              .evaluate(() => {
                const n = globalThis.__MBO_TEST__?.network;
                return {
                  uid: n?.uid,
                  connected: n?.connected,
                  lease: n?.lease,
                  host: !!n?.authority,
                  ready: n?.authority?.ready,
                  hostError: n?.lastHostError && {
                    message: n.lastHostError.message,
                    code: n.lastHostError.code,
                  },
                  menu: document.getElementById("menuMessage")?.textContent,
                };
              })
              .catch(() => ({})),
          );
    throw error;
  } finally {
    await browser?.close();
    await new Promise((resolve) => server.close(resolve));
    await env.cleanup();
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
