// Real Firestore emulator + actual HTTP/Socket.IO/game UI. Only Firebase's token
// verifier and browser Auth SDK are fixtures; no production auth bypass exists.
const assert = require("node:assert/strict"),
  http = require("node:http"),
  express = require("express"),
  admin = require("firebase-admin"),
  { Server } = require("socket.io"),
  { serve } = require("./serve.cjs"),
  { launch } = require("./chromium.cjs"),
  { mountMountOnline } = require("../../../server/mountonline/mount.cjs");
async function main() {
  if (!process.env.FIRESTORE_EMULATOR_HOST)
    throw new Error("Run through the demo Firestore emulator.");
  const projectId = "demo-mountonline";
  await fetch(
    `http://${process.env.FIRESTORE_EMULATOR_HOST}/emulator/v1/projects/${projectId}/databases/(default)/documents`,
    { method: "DELETE" },
  );
  const adminApp = admin.initializeApp({ projectId }, "mount-browser-tests"),
    db = adminApp.firestore(),
    app = express(),
    server = http.createServer(app),
    io = new Server(server),
    { newProfile } = await import("../data.mjs");
  let host, browser;
  app.get("/existing-game-health", (_req, res) => res.json({ ok: true }));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = "http://127.0.0.1:" + server.address().port;
  const auth = {
    verifyIdToken: async (token) => {
      assert.match(token || "", /^fixture-[abc]$/);
      return { uid: "uid-" + token.at(-1) };
    },
  };
  try {
    for (const name of ["a", "b"]) {
      const p = newProfile(name === "a" ? "CaptainA" : "CaptainB");
      p.uid = "uid-" + name;
      p.migrated = true;
      p.gold = 1000;
      p.x = name === "a" ? 15.5 : 18.5;
      p.y = 44.5;
      await db.doc("mountOnlineProfiles/" + p.uid).set(p);
      await db
        .doc("mountOnlineAccounts/" + p.uid)
        .set({ verified: true, username: p.username });
    }
    host = await mountMountOnline(app, {
      db,
      auth,
      io,
      projectId: "bible-game-246c0",
      allowedOrigins: [origin],
      logger: console,
    });
    assert.ok(host);
    app.use(serve);
    browser = await launch();
    const errors = [];
    async function contextFor(letter, touch = false) {
      const c = await browser.newContext({
        viewport: touch
          ? { width: 390, height: 844 }
          : { width: 1280, height: 800 },
        hasTouch: touch,
        isMobile: touch,
      });
      await c.route(
        "https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js",
        (r) =>
          r.fulfill({
            contentType: "text/javascript",
            body: "export const getApps=()=>[];export const initializeApp=()=>({});",
          }),
      );
      await c.route(
        "https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js",
        (r) =>
          r.fulfill({
            contentType: "text/javascript",
            body: `const auth={currentUser:{uid:'uid-${letter}',getIdToken:async()=>'fixture-${letter}'}};export const getAuth=()=>auth;export const signInAnonymously=async()=>({user:auth.currentUser});export const onAuthStateChanged=(_auth,cb)=>{queueMicrotask(()=>cb(auth.currentUser));return ()=>{};};`,
          }),
      );
      c.on("page", (p) => p.on("pageerror", (e) => errors.push(e.message)));
      return c;
    }
    async function join(c) {
      const p = await c.newPage();
      await p.goto(origin + "/game/mountonline.html?username=SpoofedURLName");
      await p.waitForFunction(
        () => !document.querySelector("#startOnline").disabled,
      );
      await p.click("#startOnline");
      await p.waitForFunction(
        () =>
          __MBO_TEST__.mode === "online" &&
          !document.getElementById("app").hidden &&
          __MBO_TEST__.network.socket?.connected,
        null,
        { timeout: 20000 },
      );
      return p;
    }
    const ac = await contextFor("a"),
      bc = await contextFor("b", true);
    let a = await join(ac),
      b = await join(bc);
    assert.equal(await a.locator("#commanderName").textContent(), "CaptainA");
    assert.equal(await b.locator("#commanderName").textContent(), "CaptainB");
    await a.waitForFunction(() =>
      __MBO_TEST__.state.others.some((o) => o.username === "CaptainB"),
    );
    const target = await a.evaluate(() => {
      const o = __MBO_TEST__.state.others.find(
        (o) => o.username === "CaptainB",
      );
      return __MBO_TEST__.renderer.toScreen(o.x, o.y);
    });
    await a.mouse.click(target.x, target.y);
    await a.waitForSelector('[data-context="engage"]');
    await a.click('[data-context="engage"]');
    await Promise.all([
      a.waitForFunction(() => !!__MBO_TEST__.battle),
      b.waitForFunction(() => !!__MBO_TEST__.battle),
    ]);
    const battleId = await a.evaluate(() => __MBO_TEST__.battle.id);
    assert.equal(await b.evaluate(() => __MBO_TEST__.battle.id), battleId);
    assert.equal(await b.evaluate(() => __MBO_TEST__.team), 1);
    assert.equal(
      await b.evaluate(() =>
        __MBO_TEST__.battle.units
          .filter((u) => __MBO_TEST__.renderer.selected.has(u.id))
          .every((u) => u.team === 1),
      ),
      true,
    );
    await a.click('[data-order="hold"]');
    await b.click('[data-order="hold"]');
    await a.waitForFunction(() =>
      __MBO_TEST__.battle.units
        .filter((u) => u.team === 0)
        .every((u) => u.order.kind === "hold"),
    );
    const bad = await a.evaluate(async () => {
      const id = __MBO_TEST__.battle.units.find((u) => u.team === 1).id;
      const r = await fetch("/api/mountonline/action", {
        method: "POST",
        headers: {
          Authorization: "Bearer fixture-a",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          type: "command",
          order: "retreat",
          ids: [id],
          point: { x: 1, y: 1 },
          uid: "uid-b",
        }),
      });
      return r.status;
    });
    assert.equal(bad, 400);
    assert.ok(
      host.service.realm.battles
        .get(battleId)
        .units.filter((u) => u.team === 1)
        .every((u) => u.order.kind === "hold"),
    );
    await b.close();
    b = await join(bc);
    await b.waitForFunction(
      () => __MBO_TEST__.battle?.id === __MBO_TEST__.state.profile.activeBattle,
    );
    assert.equal(await b.evaluate(() => __MBO_TEST__.battle.id), battleId);
    assert.equal(await b.evaluate(() => __MBO_TEST__.team), 1);
    await a.click('[data-order="charge"]');
    await Promise.all([
      a.waitForFunction(() => document.getElementById("result").open, null, {
        timeout: 55000,
      }),
      b.waitForFunction(() => document.getElementById("result").open, null, {
        timeout: 55000,
      }),
    ]);
    const pa = (await db.doc("mountOnlineProfiles/uid-a").get()).data(),
      pb = (await db.doc("mountOnlineProfiles/uid-b").get()).data();
    assert.equal(pa.stats.battles, 1);
    assert.equal(pb.stats.battles, 1);
    assert.equal(pa.stats.wins + pb.stats.wins, 1);
    assert.equal(pa.activeBattle, null);
    assert.equal(pb.activeBattle, null);
    const result = (
      await db.doc("mountOnlineBattles/" + battleId).get()
    ).data();
    assert.equal(result.settled, true);
    const denied = await fetch(origin + "/api/mountonline/join", {
      method: "POST",
      headers: {
        Origin: "https://unconfigured.invalid",
        Authorization: "Bearer fixture-a",
        "Content-Type": "application/json",
      },
      body: "{}",
    });
    assert.equal(denied.status, 403);
    assert.equal(
      (await (await fetch(origin + "/existing-game-health")).json()).ok,
      true,
    );
    const cc = await contextFor("c");
    await cc.route("**/socket.io/socket.io.esm.min.js", (r) => r.abort());
    const c = await cc.newPage();
    await c.goto(origin + "/game/mountonline.html");
    await c.waitForFunction(
      () => !document.querySelector("#startOnline").disabled,
    );
    await c.click("#startOnline");
    await c.waitForFunction(
      () =>
        __MBO_TEST__.network?.poll &&
        __MBO_TEST__.state?.profile.uid === "uid-c",
    );
    assert.match(await c.locator("#commanderName").textContent(), /^Wanderer/);
    await c.click("#haltMarch");
    assert.equal(
      await c.evaluate(() => __MBO_TEST__.state.profile.army.recruits),
      12,
    );
    assert.deepEqual(errors, []);
    console.log(
      JSON.stringify({
        suite: "Mountain Blade two-browser multiplayer",
        desktopAndTouch: true,
        sameBattle: true,
        verifiedIdentity: true,
        enemyOrdersRejected: true,
        reconnectedSeat: true,
        oneRewardPerPlayer: true,
        httpFallback: true,
        cors: true,
        otherGameUnaffected: true,
        errors: errors.length,
        passed: true,
      }),
    );
    await ac.close();
    await bc.close();
    await cc.close();
  } finally {
    await browser?.close();
    await host?.close();
    await new Promise((resolve) => io.close(resolve));
    if (server.listening) await new Promise((resolve) => server.close(resolve));
    await adminApp.delete();
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
