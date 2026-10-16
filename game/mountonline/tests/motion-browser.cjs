// Static-browser fixture feeds the same callback used by real Firestore views.
// Irregular packets are deliberate; no production hooks or network bypass exist.
const assert = require("node:assert/strict"),
  { start } = require("./serve.cjs"),
  { launch } = require("./chromium.cjs");
async function main() {
  const { server, origin } = await start();
  let browser;
  const report = [];
  try {
    browser = await launch();
    for (const device of [
      { name: "desktop", width: 1440, height: 900, touch: false },
      { name: "phone", width: 390, height: 844, touch: true },
      { name: "landscape", width: 844, height: 390, touch: true },
    ]) {
      const context = await browser.newContext({
        viewport: { width: device.width, height: device.height },
        hasTouch: device.touch,
        isMobile: device.touch,
      });
      const page = await context.newPage(),
        errors = [];
      page.on("pageerror", (e) => errors.push(e.message));
      await page.route("https://www.gstatic.com/**", (route) => route.abort());
      await page.goto(origin + "/game/mountonline.html?username=MotionCaptain");
      await page.waitForFunction(
        () =>
          !!globalThis.__MBO_TEST__ &&
          !document.querySelector("#startLocal").disabled,
      );
      await page.evaluate(async () => {
        const { Realm } = await import("/game/mountonline/realm.mjs"),
          { newProfile, emptyArmy } = await import(
            "/game/mountonline/data.mjs"
          );
        const { createBattle, battleView } = await import(
          "/game/mountonline/battle.mjs"
        );
        const realm = new Realm();
        realm.enter("captain", newProfile("MotionCaptain"));
        const base = realm.snapshot("captain");
        base.syncInterval = 1200;
        base.profile.x = 20.5;
        base.profile.y = 44.5;
        base.profile.route = [];
        base.npcs = [
          {
            id: "patrol",
            kind: "patrol",
            x: 24.5,
            y: 46.5,
            army: { ...emptyArmy(), recruits: 10 },
          },
        ];
        base.others = [
          { id: "ally", username: "Ally", x: 26.5, y: 48.5, armySize: 12 },
        ];
        window.motionBase = base;
        window.motionSamples = [];
        window.motionFetches = 0;
        window.motionWalks = [];
        const renderer = __MBO_TEST__.renderer,
          draw = renderer.draw.bind(renderer),
          sprite = renderer.sprite.bind(renderer),
          fetch = window.fetch;
        renderer.draw = (s, map, b, team, alpha, now) => {
          window.motionSamples.push({
            now,
            x: s.profile.x,
            npc: s.npcs[0]?.x,
            other: s.others[0]?.x,
            unit: b?.units[0]?.x,
            raw: __MBO_TEST__.state.profile.x,
          });
          draw(s, map, b, team, alpha, now);
        };
        renderer.sprite = (key, state, x, y, time, facing, size) => {
          if (size === 1.05) window.motionWalks.push(state);
          sprite(key, state, x, y, time, facing, size);
        };
        window.fetch = (...args) => {
          window.motionFetches++;
          return fetch(...args);
        };
        window.feedWorld = (delta) => {
          const s = structuredClone(motionBase);
          s.profile.x += delta;
          s.npcs[0].x += delta;
          s.others[0].x += delta;
          __MBO_TEST__.feedRemote(s);
        };
        const b = createBattle({
          id: "motion-field",
          armyA: { ...emptyArmy(), recruits: 2 },
          armyB: { ...emptyArmy(), footmen: 1 },
        });
        b.units[0].x = 10.5;
        b.units[0].y = 19.5;
        b.units[0].state = "move";
        b.units[1].x = 12.5;
        b.units[1].y = 26.5;
        b.units[1].state = "idle";
        b.units[2].x = 53.5;
        b.units[2].y = 20.5;
        window.feedBattle = (delta) => {
          const s = structuredClone(motionBase),
            next = structuredClone(b);
          next.units[0].x += delta;
          next.units[0].px = next.units[0].x;
          next.units[0].py = next.units[0].y;
          s.battle = battleView(next);
          s.profile.activeBattle = next.id;
          s.syncInterval = 500;
          __MBO_TEST__.feedRemote(s);
        };
        feedWorld(0);
      });
      await page.waitForTimeout(100);
      await page.evaluate(() => {
        motionSamples.length = 0;
        motionWalks.length = 0;
        feedWorld(3);
      });
      await page.waitForTimeout(300);
      const burst = await page.evaluate(() => {
        const before = __MBO_TEST__.motion.state.profile.x;
        feedWorld(6);
        return {
          before,
          after: __MBO_TEST__.motion.state.profile.x,
          raw: __MBO_TEST__.state.profile.x,
        };
      });
      assert.ok(burst.before < 23.5);
      assert.ok(Math.abs(burst.after - burst.before) < 0.25);
      assert.equal(burst.raw, 26.5);
      await page.waitForTimeout(175);
      await page.evaluate(() => feedWorld(7));
      await page.waitForTimeout(1550);
      const world = await page.evaluate(() => ({
        samples: motionSamples,
        walks: motionWalks,
        profile: __MBO_TEST__.motion.state.profile.x,
        raw: __MBO_TEST__.state.profile.x,
        network: motionFetches,
      }));
      assert.equal(world.raw, 27.5);
      assert.ok(Math.abs(world.profile - 27.5) < 0.02);
      assert.ok(world.walks.includes("move"));
      assert.equal(world.walks.at(-1), "idle");
      assert.equal(world.network, 0);
      const worldSteps = world.samples
        .slice(1)
        .map((p, i) => Math.abs(p.x - world.samples[i].x));
      assert.ok(
        Math.max(...worldSteps) < 0.55,
        `${device.name}: world jump ${Math.max(...worldSteps)}`,
      );
      assert.ok(
        world.samples.filter(
          (p, i) => i && Math.abs(p.x - world.samples[i - 1].x) > 0.001,
        ).length > 45,
      );
      assert.ok(
        world.samples.every(
          (p, i) => !i || p.npc >= world.samples[i - 1].npc - 0.001,
        ),
      );
      assert.ok(
        world.samples.every(
          (p, i) => !i || p.other >= world.samples[i - 1].other - 0.001,
        ),
      );
      // Stop coordinates equal the rendered destination, so clicking the company
      // uses its visible position rather than the last Firestore grid step.
      await page.evaluate(() => feedBattle(0));
      await page.waitForTimeout(50);
      await page.evaluate(() => {
        motionSamples.length = 0;
        feedBattle(6);
        __MBO_TEST__.renderer.selected.clear();
      });
      await page.waitForTimeout(100);
      const troop = await page.evaluate(() => {
        const u = __MBO_TEST__.motion.battle.units[0];
        return {
          id: u.id,
          raw: __MBO_TEST__.battle.units[0].x,
          shown: u.x,
          point: __MBO_TEST__.renderer.toScreen(u.x, u.y),
        };
      });
      assert.ok(troop.raw - troop.shown > 2);
      if (device.touch)
        await page.touchscreen.tap(troop.point.x, troop.point.y);
      else await page.mouse.click(troop.point.x, troop.point.y);
      assert.deepEqual(
        await page.evaluate(() => [...__MBO_TEST__.renderer.selected]),
        [troop.id],
        `${device.name}: selection missed ${JSON.stringify(troop)}`,
      );
      const correction = await page.evaluate(() => {
        const before = __MBO_TEST__.motion.battle.units[0].x;
        feedBattle(7);
        return Math.abs(__MBO_TEST__.motion.battle.units[0].x - before);
      });
      assert.ok(correction < 0.5);
      await page.waitForTimeout(850);
      const combat = await page.evaluate(() => ({
        x: __MBO_TEST__.motion.battle.units[0].x,
        raw: __MBO_TEST__.battle.units[0].x,
        samples: motionSamples,
        network: motionFetches,
        overflow: document.documentElement.scrollWidth > innerWidth,
      }));
      assert.ok(Math.abs(combat.x - combat.raw) < 0.02);
      assert.ok(
        combat.samples.filter(
          (p, i) => i && Math.abs(p.unit - combat.samples[i - 1].unit) > 0.001,
        ).length > 20,
      );
      assert.equal(combat.network, 0);
      assert.equal(combat.overflow, false);
      assert.deepEqual(errors, []);
      report.push({
        device: device.name,
        continuousWorld: true,
        burstUpdates: true,
        continuousBattle: true,
        selectVisibleMovingTroops: true,
        networkRequests: world.network,
        errors: errors.length,
      });
      await context.close();
    }
    console.log(
      JSON.stringify(
        { suite: "Firestore movement presentation", report, passed: true },
        null,
        2,
      ),
    );
  } finally {
    await browser?.close();
    await new Promise((resolve) => server.close(resolve));
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
