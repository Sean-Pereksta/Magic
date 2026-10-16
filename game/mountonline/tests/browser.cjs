const assert = require("node:assert/strict"),
  fs = require("node:fs"),
  path = require("node:path"),
  { start } = require("./serve.cjs"),
  { launch } = require("./chromium.cjs");
async function main() {
  const { server, origin } = await start(),
    report = [],
    out =
      process.env.MOUNTONLINE_QA_DIR ||
      path.join(require("node:os").tmpdir(), "mountonline-qa");
  let browser;
  fs.mkdirSync(out, { recursive: true });
  try {
    browser = await launch();
    for (const device of [
      { name: "desktop", width: 1440, height: 900, touch: false },
      { name: "phone", width: 390, height: 844, touch: true },
      { name: "landscape", width: 844, height: 390, touch: true },
    ].filter(
      (d) =>
        !process.env.MOUNTONLINE_TEST_DEVICES ||
        process.env.MOUNTONLINE_TEST_DEVICES.split(",").includes(d.name),
    )) {
      const context = await browser.newContext({
          viewport: { width: device.width, height: device.height },
          hasTouch: device.touch,
          isMobile: device.touch,
        }),
        page = await context.newPage(),
        errors = [],
        failed = [];
      page.on("pageerror", (e) => errors.push(e.message));
      page.on("response", (r) => {
        if (r.status() >= 400 && !r.url().includes("favicon"))
          failed.push(r.url());
      });
      await page.route("https://www.gstatic.com/**", (route) => route.abort());
      await page.goto(origin + "/game/mountonline.html?username=TestCaptain");
      await page.waitForFunction(
        () =>
          !!window.__MBO_TEST__ &&
          !document.querySelector("#startLocal").disabled,
      );
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        true,
      );
      await page.screenshot({
        path: path.join(out, device.name + "-menu.png"),
      });
      await page.click("#help");
      assert.equal(await page.locator("#panel").evaluate((e) => e.open), true);
      await page.click("#closePanel");
      await page.click("#startLocal");
      await page.waitForFunction(
        () =>
          __MBO_TEST__.mode === "local" &&
          !document.querySelector("#app").hidden,
      );
      await page.waitForTimeout(300);
      const size = await page.evaluate(() =>
        Object.values(__MBO_TEST__.state.profile.army).reduce(
          (a, b) => a + b,
          0,
        ),
      );
      assert.equal(size, 26);
      const hearth = await page.evaluate(() =>
        __MBO_TEST__.renderer.toScreen(13.5, 44.5),
      );
      if (device.touch) await page.touchscreen.tap(hearth.x, hearth.y);
      else await page.mouse.click(hearth.x, hearth.y);
      await page.waitForSelector('[data-context="recruit"]');
      await page.click('[data-context="recruit"]');
      await page.waitForFunction(
        () => __MBO_TEST__.state.profile.army.recruits === 17,
      );
      assert.equal(
        await page.evaluate(() => __MBO_TEST__.state.profile.gold),
        200,
      );
      await page.click("#closeContext");
      await page.locator('.hud-nav [data-panel="army"]').click();
      assert.equal(await page.locator(".unit-card").count(), 14);
      await page.click('[data-action="train"][data-unit="footmen"]');
      await page.waitForFunction(
        () => __MBO_TEST__.state.profile.army.footmen === 7,
      );
      await page.click(
        '[data-action="reserve"][data-unit="footmen"]:not([data-withdraw])',
      );
      await page.click(
        '[data-action="reserve"][data-unit="footmen"][data-withdraw]',
      );
      assert.equal(
        await page.evaluate(() => __MBO_TEST__.state.profile.army.footmen),
        7,
      );
      await page.click("#closePanel");
      const point = await page.evaluate(() =>
        __MBO_TEST__.renderer.toScreen(20.5, 47.5),
      );
      if (device.touch) await page.touchscreen.tap(point.x, point.y);
      else await page.mouse.click(point.x, point.y);
      await page.waitForFunction(
        () => __MBO_TEST__.state.profile.route.length > 0,
      );
      const before = await page.evaluate(() => __MBO_TEST__.state.profile.x);
      await page.waitForTimeout(650);
      assert.ok(
        await page.evaluate((x) => __MBO_TEST__.state.profile.x > x, before),
      );
      await page.click("#haltMarch");
      await page.click("#saveGame");
      const gold = await page.evaluate(() => __MBO_TEST__.state.profile.gold);
      await page.screenshot({
        path: path.join(out, device.name + "-world.png"),
      });
      await page.reload();
      await page.waitForFunction(
        () => !document.querySelector("#startLocal").disabled,
      );
      assert.match(await page.locator("#startLocal").textContent(), /Continue/);
      await page.click("#startLocal");
      assert.equal(
        await page.evaluate(() => __MBO_TEST__.state.profile.gold),
        gold,
      );
      await page.locator('.hud-nav [data-panel="settings"]').click();
      await page.selectOption("#quality", "low");
      await page.check("#fps");
      assert.equal(await page.evaluate(() => __MBO_TEST__.renderer.dpr), 1);
      await page.click("#closePanel");
      await page.click("#menuButton");
      await page.click("#practice");
      await page.waitForFunction(() => !!__MBO_TEST__.battle);
      assert.ok(
        await page.evaluate(() => __MBO_TEST__.renderer.selected.size > 60),
      );
      await page.click('[data-order="hold"]');
      assert.equal(
        await page.evaluate(() =>
          __MBO_TEST__.battle.units
            .filter((u) => u.team === 0)
            .every((u) => u.order.kind === "hold"),
        ),
        true,
      );
      const pick = await page.evaluate(() =>
        __MBO_TEST__.battle.units
          .filter((u) => u.team === 0)
          .map((u) => __MBO_TEST__.renderer.toScreen(u.x, u.y))
          .filter(
            (p) =>
              p.y > 160 &&
              p.y <
                document.getElementById("battleDock").getBoundingClientRect()
                  .top -
                  20 &&
              document.elementFromPoint(p.x, p.y) ===
                document.getElementById("map"),
          )
          .slice(0, 2),
      );
      if (device.touch) await page.touchscreen.tap(pick[0].x, pick[0].y);
      else await page.mouse.click(pick[0].x, pick[0].y);
      assert.equal(
        await page.evaluate(() => __MBO_TEST__.renderer.selected.size),
        1,
      );
      await page.click("#multiSelect");
      if (device.touch) await page.touchscreen.tap(pick[1].x, pick[1].y);
      else await page.mouse.click(pick[1].x, pick[1].y);
      assert.equal(
        await page.evaluate(() => __MBO_TEST__.renderer.selected.size),
        2,
        device.name + " additive selection",
      );
      await page.click("#multiSelect");
      const box = await page.evaluate(() => {
        const points = __MBO_TEST__.battle.units
          .filter((u) => u.team === 0)
          .map((u) => __MBO_TEST__.renderer.toScreen(u.x, u.y))
          .filter(
            (p) =>
              p.x > 20 &&
              p.x < innerWidth - 20 &&
              p.y > 160 &&
              p.y <
                document.getElementById("battleDock").getBoundingClientRect()
                  .top -
                  20 &&
              document.elementFromPoint(p.x, p.y) ===
                document.getElementById("map"),
          )
          .slice(0, 8);
        return {
          x1: Math.min(...points.map((p) => p.x)) - 8,
          y1: Math.min(...points.map((p) => p.y)) - 8,
          x2: Math.max(...points.map((p) => p.x)) + 8,
          y2: Math.max(...points.map((p) => p.y)) + 8,
        };
      });
      if (device.touch) {
        const cdp = await context.newCDPSession(page);
        await cdp.send("Input.dispatchTouchEvent", {
          type: "touchStart",
          touchPoints: [{ x: box.x1, y: box.y1, id: 1 }],
        });
        await page.waitForTimeout(360);
        await cdp.send("Input.dispatchTouchEvent", {
          type: "touchMove",
          touchPoints: [{ x: box.x2, y: box.y2, id: 1 }],
        });
        await cdp.send("Input.dispatchTouchEvent", {
          type: "touchEnd",
          touchPoints: [],
        });
        const before = await page.evaluate(
          () => __MBO_TEST__.renderer.cam.zoom,
        );
        for (const [from, to] of [
          [60, 100],
          [100, 60],
        ]) {
          await cdp.send("Input.dispatchTouchEvent", {
            type: "touchStart",
            touchPoints: [
              { x: 200 - from / 2, y: 250, id: 1 },
              { x: 200 + from / 2, y: 250, id: 2 },
            ],
          });
          await cdp.send("Input.dispatchTouchEvent", {
            type: "touchMove",
            touchPoints: [
              { x: 200 - to / 2, y: 250, id: 1 },
              { x: 200 + to / 2, y: 250, id: 2 },
            ],
          });
          if (to === 100)
            assert.ok(
              await page.evaluate(
                (z) => __MBO_TEST__.renderer.cam.zoom > z,
                before,
              ),
            );
          await cdp.send("Input.dispatchTouchEvent", {
            type: "touchEnd",
            touchPoints: [],
          });
        }
        await cdp.detach();
      } else {
        await page.mouse.move(box.x1, box.y1);
        await page.mouse.down();
        await page.mouse.move(box.x2, box.y2, { steps: 10 });
        await page.mouse.up();
      }
      assert.ok(
        await page.evaluate(() => __MBO_TEST__.renderer.selected.size >= 5),
      );
      if (!device.touch) {
        const selected = await page.evaluate(
          () => __MBO_TEST__.renderer.selected.size,
        );
        await page.keyboard.press("Control+1");
        await page.click('[data-group="ranged"]');
        await page.keyboard.press("1");
        assert.equal(
          await page.evaluate(() => __MBO_TEST__.renderer.selected.size),
          selected,
        );
      }
      await page.click('[data-group="all"]');
      await page.selectOption("#formation", "shieldwall");
      await page.click('[data-order="hold"]');
      assert.equal(
        await page.evaluate(() => __MBO_TEST__.battle.formation[0]),
        "shieldwall",
      );
      await page.click('[data-group="infantry"]');
      await page.click('[data-order="attack"]');
      const target = await page.evaluate(() =>
        __MBO_TEST__.renderer.toScreen(
          Math.min(60, __MBO_TEST__.renderer.cam.x + 5),
          20,
        ),
      );
      if (device.touch) await page.touchscreen.tap(target.x, target.y);
      else {
        await page.mouse.move(target.x, target.y);
        await page.waitForFunction(() => !!__MBO_TEST__.renderer.preview);
        await page.mouse.click(target.x, target.y);
      }
      assert.equal(
        await page.evaluate(() =>
          __MBO_TEST__.battle.units
            .filter(
              (u) => u.team === 0 && __MBO_TEST__.renderer.selected.has(u.id),
            )
            .every((u) => u.order.kind === "attack"),
        ),
        true,
      );
      await page.click('[data-group="all"]');
      await page.click('[data-order="charge"]');
      await page.click('[data-ability="rally"]');
      const ally = await page.evaluate(() =>
        __MBO_TEST__.renderer.toScreen(15, 20),
      );
      if (device.touch) await page.touchscreen.tap(ally.x, ally.y);
      else await page.mouse.click(ally.x, ally.y);
      assert.ok(
        await page.evaluate(() => __MBO_TEST__.battle.abilities[0].rally > 0),
      );
      await page.screenshot({
        path: path.join(out, device.name + "-battle.png"),
      });
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        true,
      );
      assert.equal(await page.locator("#battleDock").isVisible(), true);
      const samples = await page.evaluate(async () => {
        const values = [];
        let previous = performance.now();
        for (let i = 0; i < 75; i++) {
          await new Promise(requestAnimationFrame);
          const now = performance.now();
          values.push(now - previous);
          previous = now;
        }
        values.sort((a, b) => a - b);
        return {
          p50: values[37],
          p95: values[71],
          drawMs: __MBO_TEST__.renderer.frameMs,
        };
      });
      if (device.name === "desktop") {
        await page.waitForFunction(() => __MBO_TEST__.battle.finished, null, {
          timeout: 45000,
        });
        await page.waitForFunction(
          () => document.getElementById("result").open,
        );
        await page.click("#returnRealm");
        await page.click("#practiceSiege");
        await page.waitForFunction(() => __MBO_TEST__.battle?.siege);
        await page.waitForFunction(
          () => __MBO_TEST__.battle.gate.hp === 0,
          null,
          { timeout: 45000 },
        );
        await page.waitForFunction(() => __MBO_TEST__.battle.finished, null, {
          timeout: 45000,
        });
        await page.waitForFunction(
          () => document.getElementById("result").open,
        );
        await page.click("#returnRealm");
      }
      assert.deepEqual(errors, [], device.name + " JS errors");
      assert.deepEqual(failed, [], device.name + " failed local assets");
      report.push({
        device: device.name,
        viewport: [device.width, device.height],
        frames: samples,
        errors: errors.length,
      });
      await context.close();
    }
  } finally {
    await browser?.close();
    await new Promise((resolve) => server.close(resolve));
  }
  fs.writeFileSync(
    path.join(out, "browser-report.json"),
    JSON.stringify(report, null, 2),
  );
  console.log(JSON.stringify({ screenshots: out, report }, null, 2));
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
