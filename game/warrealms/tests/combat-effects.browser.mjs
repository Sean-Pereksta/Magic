// Browser regression harness. Run this file, then open the printed local URL.
// Uses the real WarRealms shell and modules; all Firebase scripts are removed.
// No test server dependency, account, or live game state is needed.
import http from "node:http";
import { readFile } from "node:fs/promises";
const root = new URL("../../", import.meta.url);
const source = await readFile(new URL("warrealms.html", root), "utf8");
const shell = source.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, "");
const run = async function () {
  const results = [];
  const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
  const check = (condition, label) => { if (!condition) throw new Error(label); results.push(label); };
  let fx;
  try {
    document.querySelectorAll(".overlay,.view").forEach(n => n.classList.add("hidden"));
    document.documentElement.classList.add("battleMode"); document.body.classList.add("battleMode");
    document.getElementById("battleView").classList.remove("hidden");
    document.getElementById("enemySnapshotList").innerHTML = '<article class="enemySnapshot" data-target-id="enemy"><b>Enemy</b><span>♥ 40</span></article>';
    const markup = '<article class="gameCard baseCard blue" data-instance-id="fort" style="position:fixed;left:100px;top:210px;width:100px;height:145px;z-index:2"><div class="battleCardVisual"><div style="height:100%;background:linear-gradient(130deg,#57749f,#223449)">Fortress</div></div><div class="cardStatusTray"><span class="cardStatusChip disabled">⌁</span></div><button id="inspectFort">Info</button></article>';
    document.getElementById("attackTargetsBoard").innerHTML = markup;
    document.getElementById("commanderAuthorityValue").textContent = "30";
    const api = await import("/warrealms/ui/combat-effects.js");
    api.setCombatEffectsMode("full");
    const legacyEvents = [];
    fx = api.createCombatEffects({ legacy: async e => { legacyEvents.push(e.type); } });
    fx.capture([], "me"); fx.flush();
    await wait(120);
    fx.capture([
      { id: "hit", type: "base-damage", instanceId: "fort", targetId: "enemy", actorId: "me", amount: 8 },
      { id: "raze", type: "card-destroy", instanceId: "fort", targetId: "enemy", actorId: "me", method: "combat" }
    ], "me");
    document.querySelector('[data-instance-id="fort"]').remove(); fx.flush();
    check(document.querySelectorAll(".wrRazeGhost").length === 1, "art survives authoritative removal");
    check(!document.querySelector(".wrRazeGhost button,.wrRazeGhost [id],.wrRazeGhost [data-instance-id]"), "ghost has no controls or live IDs");
    check(getComputedStyle(document.getElementById("wrCombatLayer")).pointerEvents === "none", "effects are inert");
    await wait(750);
    check(!!document.querySelector(".wrRazeGhost.burning"), "impact precedes burning");
    check(getComputedStyle(document.querySelector(".wrRazeGhost.burning")).animationName === "wrBaseBurn", "full burn keyframes run");
    await wait(1400);
    check(!document.querySelector(".wrRazeGhost"), "destroyed art is cleaned up");
    fx.capture([{ id: "blocked", type: "health-loss", targetId: "me", absorbed: 5, amount: 0, shieldBroken: true }], "me"); fx.flush();
    await wait(230);
    check(!document.querySelector(".wrCombatNumber.damage"), "shield-only hit does not show health damage");
    check(document.getElementById("wrCombatLayer").textContent.includes("SHIELD BREAK"), "shield break is labeled");
    const boxes = [...document.querySelectorAll(".wrCombatNumber")].map(n => n.getBoundingClientRect());
    check(!boxes.some((a, i) => boxes.slice(i + 1).some(b => a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top)), "values occupy separate lanes");
    await wait(1100);
    api.setCombatEffectsMode("reduced");
    fx.capture([{ id: "reduced-hit", type: "health-loss", targetId: "me", amount: 18 }], "me"); fx.flush();
    await wait(120);
    check(!document.querySelector(".wrCombatParticle,.wrCombatProjectile"), "reduced mode removes particles and projectiles");
    check(document.querySelector(".wrCombatNumber.damage")?.textContent === "−18", "reduced mode retains damage numbers");
    check(getComputedStyle(document.querySelector(".wrCombatNumber.damage")).animationName === "wrStaticFeedback", "reduced numbers remain readable without travel");
    await wait(1200);
    document.getElementById("attackTargetsBoard").innerHTML = markup;
    const card = document.querySelector('[data-instance-id="fort"]');
    card.classList.add("disabledBase");
    let inspected = false;
    document.getElementById("inspectFort").onclick = () => { inspected = true; };
    fx.capture([{ id: "disabled", type: "card-disable", targetId: "enemy", instanceId: "fort" }], "me"); fx.flush();
    await wait(100);
    check(getComputedStyle(card.querySelector(".battleCardVisual")).filter.includes("saturate(0.3)"), "disabled artwork desaturates");
    document.getElementById("inspectFort").click();
    check(inspected, "disabled cards remain inspectable");
    card.classList.remove("disabledBase");
    fx.capture([{ id: "enabled", type: "card-enable", targetId: "enemy", instanceId: "fort" }], "me"); fx.flush();
    await wait(450);
    check(document.getElementById("wrCombatLayer").textContent.includes("REACTIVATED"), "restoration animates");
    await wait(1000);
    // A high-volume synchronized snapshot aggregates pending hits, retaining totals.
    const events = Array.from({ length: 80 }, (_, i) => ({ id: `stress_${i}`, type: "health-loss", targetId: "me", amount: 3 }));
    fx.capture(events, "me"); fx.flush();
    const before = fx.debug().queued;
    fx.capture(JSON.parse(JSON.stringify(events)), "me"); fx.flush();
    check(fx.debug().queued === before, "replayed snapshots are deduplicated");
    check(before <= 64, "queue is bounded");
    let peak = 0;
    for (let i = 0; i < 18; i++) {
      await wait(250);
      const d = fx.debug(); peak = Math.max(peak, d.nodes);
      check(d.tracked <= (innerWidth <= 700 ? 80 : 160) && d.seen <= 256, "memory budget holds");
    }
    const final = fx.debug();
    check(!final.queued && !final.nodes && !final.running, "effect nodes and frame loop reach zero after stress");
    fx.dispose();
    check(!document.getElementById("wrCombatLayer"), "disposal removes layer");
    parent.postMessage({ width: innerWidth, passed: true, checks: results.length, peak, results }, location.origin);
  } catch (error) {
    fx?.dispose();
    parent.postMessage({ width: innerWidth, passed: false, error: error.stack, results }, location.origin);
  }
};
const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, "http://localhost");
  try {
    if (url.pathname === "/") {
      response.setHeader("Content-Type", "text/html");
      response.end(`<title>WarRealms combat regression results</title><pre id="results">Running desktop checks…</pre><iframe id="test" style="border:0;width:1280px;height:900px" src="/fixture"></iframe><script>const results=[];addEventListener('message',e=>{if(e.origin!==location.origin)return;results.push(e.data);document.getElementById('results').textContent=JSON.stringify(results,null,2);if(results.length===1){const frame=document.getElementById('test');frame.style.width='390px';frame.style.height='844px';frame.src='/fixture?mobile=1';}else{document.getElementById('test').remove();document.title=results.every(r=>r.passed)?'PASS — WarRealms combat effects':'FAIL — WarRealms combat effects';}});</script>`);
    } else if (url.pathname === "/fixture") {
      response.setHeader("Content-Type", "text/html"); response.end(shell + `<script type="module">(${run.toString()})()</script>`);
    } else if (/^\/warrealms\/(ui|engine)\/[a-z-]+\.(js|css)$/.test(url.pathname)) {
      response.setHeader("Content-Type", url.pathname.endsWith(".css") ? "text/css" : "text/javascript");
      response.end(await readFile(new URL(`.${url.pathname}`, root)));
    } else { response.writeHead(404); response.end(); }
  } catch (error) { response.writeHead(500); response.end(error.message); }
});
server.listen(8765, "0.0.0.0", () => console.log("Browser regression harness: http://127.0.0.1:8765"));
