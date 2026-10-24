const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { start } = require("./serve.cjs");
const { launch } = require("./chromium.cjs");

async function main() {
  const { server, origin } = await start();
  const output = process.env.MOUNTONLINE_QA_DIR || path.join(require("node:os").tmpdir(), "mountonline-living-qa");
  fs.mkdirSync(output, { recursive: true });
  const report = [];
  let browser;
  try {
    browser = await launch();
    for (const device of [{ name: "desktop", width: 1440, height: 900, touch: false }, { name: "phone", width: 390, height: 844, touch: true }]) {
      const context = await browser.newContext({ viewport: { width: device.width, height: device.height }, hasTouch: device.touch, isMobile: device.touch });
      const page = await context.newPage(), errors = [], missing = [];
      const activate = (selector) => device.touch ? page.locator(selector).tap() : page.locator(selector).click();
      const checkOverflow = async (location) => assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${device.name}: no overflow in ${location}`);
      const selectedOrders = () => page.evaluate(() => {
        const t = __MBO_TEST__;
        return t.battle.units.filter((u) => u.team === t.team && u.hp > 0 && !u.routed && t.renderer.selected.has(u.id)).map((u) => ({ id: u.id, key: u.key, order: u.order.kind, target: u.order.target }));
      });
      const openCard = async (faction) => {
        if (!await page.locator(`[data-faction-card="${faction}"]`).evaluate((e) => e.open)) await activate(`[data-faction-card="${faction}"] > summary`);
      };
      const mapPoint = () => page.evaluate(() => {
        const map = document.querySelector("#map"), dock = document.querySelector("#battleDock").getBoundingClientRect();
        for (let y = 190; y < Math.min(dock.top - 20, innerHeight * .62); y += 25) for (const x of [innerWidth * .45, innerWidth * .3, innerWidth * .6]) {
          if (document.elementFromPoint(x, y) !== map) continue;
          const world = __MBO_TEST__.renderer.toWorld(x, y);
          if (world.x > 1 && world.x < 63 && world.y > 1 && world.y < 39) return { x, y };
        }
        throw new Error("No unobstructed battlefield surface");
      });
      const tapMap = async () => {
        const p = await mapPoint();
        if (device.touch) await page.touchscreen.tap(p.x, p.y); else await page.mouse.click(p.x, p.y);
      };
      page.on("pageerror", (e) => errors.push(e.message));
      page.on("response", (r) => { if (r.status() >= 400 && !r.url().includes("favicon")) missing.push(r.url()); });
      await page.route("https://www.gstatic.com/**", (route) => route.abort());
      await page.goto(`${origin}/game/mountonline.html?username=LivingBrowser-${device.name}`);
      await page.waitForFunction(() => window.__MBO_TEST__ && !document.querySelector("#startLocal").disabled);
      await checkOverflow("menu");
      await activate("#startLocal");
      await page.waitForFunction(() => __MBO_TEST__.mode === "local");
      assert.deepEqual(await page.evaluate(() => ({ width: __MBO_TEST__.realm.world.width, height: __MBO_TEST__.realm.world.height, settlements: __MBO_TEST__.realm.world.settlements.length })), { width: 320, height: 224, settlements: 80 });
      for (const overlay of ["territories", "wars", "relations", "trade", "ownership", "armies", "threats", "none"]) {
        await page.selectOption("#mapOverlay", overlay);
        assert.equal(await page.evaluate(() => __MBO_TEST__.renderer.overlay), overlay);
      }
      await checkOverflow("world");
      await activate('.hud-nav [data-panel="diplomacy"]');
      assert.equal(await page.locator("[data-faction-card]").count(), 10);
      await openCard("crown");
      const beforeGift = await page.evaluate(() => ({ gold: __MBO_TEST__.state.profile.gold, relation: __MBO_TEST__.state.profile.relations.crown }));
      await page.selectOption('[data-negotiation="crown"] [name="proposal"]', "gift");
      await page.fill('[data-negotiation="crown"] [name="gold"]', "30");
      await activate('[data-negotiation="crown"] button[type="submit"]');
      await page.waitForFunction((gold) => __MBO_TEST__.state.profile.gold === gold - 30, beforeGift.gold);
      assert.equal(await page.evaluate(() => __MBO_TEST__.state.profile.relations.crown), beforeGift.relation + 10);
      await openCard("crown");
      await page.selectOption('[data-negotiation="crown"] [name="proposal"]', "defensiveAlliance");
      await page.fill('[data-negotiation="crown"] [name="gold"]', "0");
      await activate('[data-negotiation="crown"] button[type="submit"]');
      await page.waitForFunction(() => __MBO_TEST__.state.profile.diplomacy.lastDecision.status === "countered");
      const offer = await page.evaluate(() => {
        const p = __MBO_TEST__.state.profile;
        return { ...p.diplomacy.offers.find((o) => o.faction === "crown" && o.status === "pending"), balance: p.gold };
      });
      assert.ok(offer.gold > 0 && offer.gold <= offer.balance);
      await openCard("crown");
      await activate(`[data-diplomatic-accept="${offer.id}"]`);
      await page.waitForFunction(() => __MBO_TEST__.state.profile.diplomacy.treaties.some((t) => t.faction === "crown" && t.type === "defensiveAlliance"));
      assert.equal(await page.evaluate(() => __MBO_TEST__.state.profile.gold), offer.balance - offer.gold);
      assert.equal(await page.evaluate((id) => __MBO_TEST__.state.profile.diplomacy.offers.find((o) => o.id === id).status, offer.id), "accepted");
      await checkOverflow("negotiation");
      await page.screenshot({ path: path.join(output, `${device.name}-council.png`) });
      await activate("#closePanel");
      await activate('.hud-nav [data-panel="army"]');
      assert.equal(await page.locator(".unit-card").count(), 34);
      const portraits = await page.evaluate(async () => {
        const { UNITS, FACTIONS } = await import("/game/mountonline/data.mjs");
        const manifest = await (await fetch("/graphics/mountonline/manifest.json")).json();
        const atlas = manifest.atlases[3], image = new Image();
        await new Promise((resolve, reject) => { image.onload = resolve; image.onerror = reject; image.src = `/graphics/mountonline/${atlas.file}`; });
        let matching = 0;
        const hashes = [];
        for (const key of FACTIONS.flatMap((f) => f.signatureUnits)) {
          const unit = UNITS[key], canvas = document.querySelector(`[data-portrait="${key}"]`), expected = document.createElement("canvas");
          expected.width = expected.height = 128;
          expected.getContext("2d").drawImage(image, 2, unit.row * atlas.frameHeight + 2, atlas.frameWidth - 4, atlas.frameHeight - 4, 0, 0, 128, 128);
          const actual = canvas.getContext("2d").getImageData(0, 0, 128, 128).data, reference = expected.getContext("2d").getImageData(0, 0, 128, 128).data;
          let equal = true, opaque = 0, hash = 2166136261;
          for (let i = 0; i < actual.length; i++) { if (actual[i] !== reference[i]) equal = false; hash = Math.imul(hash ^ actual[i], 16777619); if (i % 4 === 3 && actual[i]) opaque++; }
          if (equal && opaque > 50) matching++;
          hashes.push(hash);
        }
        return { matching, unique: new Set(hashes).size };
      });
      assert.deepEqual(portraits, { matching: 20, unique: 20 });
      await checkOverflow("army roster");
      await activate("#closePanel");
      // Seed a controlled encounter through the real Realm API, then issue orders
      // solely through desktop and mobile controls.
      await page.evaluate(async () => {
        const t = __MBO_TEST__, { emptyArmy } = await import("/game/mountonline/data.mjs");
        const p = t.realm.player("local");
        p.army = { ...emptyArmy(), recruits: 6, footmen: 6, archers: 4, falconHorseArchers: 6 };
        const npc = t.realm.npcs.find((n) => n.kind === "bandit");
        npc.x = p.x + 1; npc.y = p.y; npc.army = { ...emptyArmy(), footmen: 20 };
        await t.perform({ type: "engage", kind: "npc", target: npc.id });
      });
      await page.waitForFunction(() => !!__MBO_TEST__.battle);
      await activate('[data-order="hold"]');
      assert.ok((await selectedOrders()).every((u) => u.order === "hold"));
      if (device.touch) await activate('[data-order="attackNearest"]');
      else { await page.locator("#map").focus(); await page.keyboard.press("a"); }
      await page.waitForFunction(() => __MBO_TEST__.pending === null && __MBO_TEST__.battle.units.filter((u) => u.team === 0 && __MBO_TEST__.renderer.selected.has(u.id)).every((u) => u.order.kind === "attackNearest"));
      const attackOrders = await selectedOrders();
      assert.ok(attackOrders.length > 1 && attackOrders.every((u) => Number.isInteger(u.target)), "immediate attack acquires targets without another click");
      assert.ok(new Set(attackOrders.map((u) => u.target)).size > 1, "a selected group distributes its attack targets");
      const single = await page.evaluate(() => {
        const t = __MBO_TEST__, map = document.querySelector("#map");
        return t.battle.units.filter((u) => u.team === t.team && u.hp > 0 && !u.routed)
          .map((u) => t.renderer.toScreen(u.x, u.y)).find((p) => p.x > 15 && p.x < innerWidth - 15 && p.y > 170 && document.elementFromPoint(p.x, p.y) === map);
      });
      assert.ok(single, "a friendly soldier is visible for individual selection");
      if (device.touch) await page.touchscreen.tap(single.x, single.y); else await page.mouse.click(single.x, single.y);
      assert.equal((await selectedOrders()).length, 1);
      if (device.touch) await activate('[data-order="attackNearest"]');
      else { await page.locator("#map").focus(); await page.keyboard.press("a"); }
      assert.ok((await selectedOrders()).every((u) => u.order === "attackNearest" && Number.isInteger(u.target)));
      assert.equal(await page.evaluate(() => __MBO_TEST__.pending), null);
      await activate('[data-group="all"]');
      await page.keyboard.press("Shift+A");
      assert.equal(await page.evaluate(() => __MBO_TEST__.pending), "attack");
      assert.ok((await selectedOrders()).every((u) => u.order === "attackNearest"), "targeted attack waits for a destination");
      await tapMap();
      await page.waitForFunction(() => __MBO_TEST__.pending === null);
      assert.ok((await selectedOrders()).every((u) => u.order === "attack"));
      await activate('[data-group="cavalry"]');
      assert.equal((await selectedOrders()).length, 6);
      await activate(".advanced-orders > summary");
      await activate('[data-order="skirmish"]');
      assert.equal(await page.evaluate(() => __MBO_TEST__.pending), "skirmish");
      await tapMap();
      assert.ok((await selectedOrders()).every((u) => u.key === "falconHorseArchers" && u.order === "skirmish"));
      await activate('[data-group="infantry"]');
      await activate('[data-order="brace"]');
      assert.equal(await page.evaluate(() => __MBO_TEST__.pending), null);
      assert.ok((await selectedOrders()).every((u) => u.order === "brace"));
      await activate(".advanced-orders > summary");
      let pinch = null;
      if (device.touch) {
        const touch = await context.newCDPSession(page);
        const surface = await page.evaluate(() => {
          for (let y = 190; y < 450; y += 25) {
            const points = [145, 245].map((x) => document.elementFromPoint(x, y));
            if (points.every((p) => p?.id === "map")) return { x: 195, y };
          }
          throw new Error("No unobstructed pinch surface");
        });
        const before = await page.evaluate(() => __MBO_TEST__.renderer.cam.zoom);
        const points = (distance) => [{ x: surface.x - distance / 2, y: surface.y, id: 1 }, { x: surface.x + distance / 2, y: surface.y, id: 2 }];
        await touch.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: points(60) });
        await touch.send("Input.dispatchTouchEvent", { type: "touchMove", touchPoints: points(100) });
        await page.waitForFunction((z) => __MBO_TEST__.renderer.cam.zoom > z, before, { timeout: 5000 });
        pinch = { before, after: await page.evaluate(() => __MBO_TEST__.renderer.cam.zoom), surface };
        await touch.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
        await touch.detach();
      }
      await checkOverflow("battle");
      await page.screenshot({ path: path.join(output, `${device.name}-living-battle.png`) });
      if (!device.touch) {
        for (const id of ['shield','flank','horse','splash','commander','defensive','ambush','caravan','raid','rearguard','reinforcement']) {
          await page.evaluate(() => __MBO_TEST__.returnMenu());
          await page.click('#training');
          await page.click(`[data-scenario="${id}"]`);
          await page.waitForFunction((scenario) => __MBO_TEST__.battle?.id === 'training-'+scenario, id);
          assert.ok(await page.evaluate(() => __MBO_TEST__.battle.units.length > 0));
          await page.waitForTimeout(35);
        }
      }
      assert.deepEqual(errors, [], `${device.name}: browser errors`);
      assert.deepEqual(missing, [], `${device.name}: missing assets`);
      report.push({ device: device.name, dimensions: [device.width, device.height], portraits, counterofferGold: offer.gold, immediateAttackTroops: attackOrders.length, pinch, errors });
      await context.close();
    }
  } finally {
    await browser?.close();
    await new Promise((resolve) => server.close(resolve));
  }
  fs.writeFileSync(path.join(output, "living-browser-report.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ output, report }, null, 2));
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
