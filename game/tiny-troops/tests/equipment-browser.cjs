/* Real catalog, preparation transactions, loot/save recovery, and mobile UI. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..'), errors = [];
const server = http.createServer((req, res) => {
  const filename = path.resolve(root, '.' + new URL(req.url, 'http://local').pathname);
  if (!filename.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(filename, (error, body) => { if (error) return res.writeHead(404).end(); res.setHeader('Content-Type', filename.endsWith('.js') ? 'text/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html'); res.end(body); });
});
let browser;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } }); page.setDefaultTimeout(8000);
  page.on('pageerror', error => errors.push(error.stack));
  await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`);
  await page.waitForFunction(() => window.TinyTroopsEquipment?.ready);
  assert.deepEqual(errors, [], 'Armory must boot without exceptions');
  await page.evaluate(() => {
    window.qaFreeze = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
    window.qaPrep = (names = ['Archer', 'Mortar Tortoise', 'Medic'], wave = 2) => {
      reset(); qaFreeze(); close(); document.getElementById('mainMenu').classList.remove('open'); S.phase = 'recruit'; S.round = wave; S.tt.mode = 'PLAYING'; S.tt.event = S.tt.wavePlan = null; S.tt.draftedRound = wave; S.choices = [];
      S.placing = S.placingEffect = S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null; S.placingUpgrade = S.placingSpecialization = false; S.pendingStarChoices = [];
      S.squad = Array(16).fill(null); names.forEach((name, i) => S.squad[i * 4 + 3] = clone(heroes.find(hero => hero.n === name))); S.bench = [null, null, null]; close(); render(); TinyTroopsEquipment.ensure();
    };
    qaPrep();
    TinyTroopsEquipment.rules.ITEMS.forEach(item => TinyTroopsEquipment.rules.addItem(S, item.id, 'QA'));
    TinyTroopsEquipment.ensure(); TinyTroopsEquipment.openInventory(S.squad[3], 0);
  });
  assert.ok(await page.locator('[data-tt-eq-preview]').count() > 0);
  const copies = await page.evaluate(() => Object.fromEntries(S.tt.equipment.items.map(item => [item.item, item.id])));
  await page.click(`[data-tt-eq-preview="${copies['sharpened-bolts']}"]`);
  assert.match(await page.locator('#ttItemPreview').innerText(), /8% attack damage/);
  const beforeCancel = await page.evaluate(() => JSON.stringify(S.tt.equipment));
  await page.click('#close');
  assert.equal(await page.evaluate(() => JSON.stringify(S.tt.equipment)), beforeCancel);
  await page.evaluate(() => TinyTroopsEquipment.openInventory(S.squad[3], 0));
  await page.click(`[data-tt-eq-preview="${copies['sharpened-bolts']}"]`);
  await page.click(`[data-tt-eq-equip="${copies['sharpened-bolts']}"]`);
  assert.equal(await page.evaluate(() => TinyTroopsEquipment.rules.equipped(S, S.squad[3])[0].item), 'sharpened-bolts');
  assert.equal(await page.evaluate(() => bonus(S.squad[3]).atk >= S.squad[3].baseAtk * .08), true);
  await page.click(`[data-tt-eq-preview="${copies['swift-trigger']}"]`); await page.click(`[data-tt-eq-equip="${copies['swift-trigger']}"]`);
  assert.equal(await page.evaluate(id => S.tt.equipment.items.find(item => item.id === id).towerId, copies['sharpened-bolts']), null);
  await page.evaluate(() => { window.qaSnapshot = stableSnapshot(); restoreSnapshot(qaSnapshot); qaFreeze(); close(); });
  assert.equal(await page.evaluate(() => TinyTroopsEquipment.rules.equipped(S, S.squad[3])[0].item), 'swift-trigger');
  await page.evaluate(() => TinyTroopsEquipment.openUnit(S.squad[3]));
  await page.click('[data-tt-eq-remove="0"]');
  assert.equal(await page.evaluate(() => TinyTroopsEquipment.rules.equipped(S, S.squad[3]).length), 0);
  console.log('PASS visual grid, exact preview, X cancel, equip/replace/unequip, real bonuses and save/restore');

  const lifecycle = await page.evaluate(() => {
    qaPrep(['Archer']); const E = TinyTroopsEquipment.rules, unit = S.squad[3], item = E.addItem(S, 'stone-plating'); TinyTroopsEquipment.equip(unit.ttId, 0, item.id);
    const savedCount = S.tt.equipment.items.length; sellUnit(3); const afterSale = { recovered: !item.towerId, count: S.tt.equipment.items.length };
    qaPrep(['Archer']); const equipped = E.addItem(S, 'sharpened-bolts'); TinyTroopsEquipment.equip(S.squad[3].ttId, 0, equipped.id); fight(); qaFreeze();
    const denied = TinyTroopsEquipment.unequip(S.squad[3].ttId, 0).ok === false;
    const beforeInvalid = JSON.stringify(S.tt.equipment); end('victory'); end(1); const invalidIgnored = JSON.stringify(S.tt.equipment) === beforeInvalid && S.phase === 'battle';
    const checkpoint = stableSnapshot(); const actions = S.tt.equipment.items.length;
    end(true); qaFreeze(); const count = S.tt.equipment.items.length; end(true); qaFreeze(); const duplicates = S.tt.equipment.items.length === count;
    const reward = S.tt.lastEquipmentReward, saved = stableSnapshot(); restoreSnapshot(saved); qaFreeze(); close();
    const reloadCount = S.tt.equipment.items.length;
    restoreSnapshot(checkpoint); qaFreeze(); close(); fight(); qaFreeze(); end(true); qaFreeze();
    return { afterSale, savedCount, denied, invalidIgnored, duplicates, count, actions, reward: reward?.item, reloadCount, replayCount: S.tt.equipment.items.length };
  });
  assert.equal(lifecycle.afterSale.recovered, true); assert.equal(lifecycle.afterSale.count, lifecycle.savedCount);
  assert.equal(lifecycle.denied, true); assert.equal(lifecycle.invalidIgnored, true); assert.equal(lifecycle.duplicates, true); assert.equal(lifecycle.count, lifecycle.actions + 1);
  assert.equal(lifecycle.reward, 'sharpened-bolts'); assert.equal(lifecycle.reloadCount, lifecycle.count); assert.equal(lifecycle.replayCount, lifecycle.count);
  console.log('PASS sale recovery, combat action validation, idempotent victory loot, reload and checkpoint replay');

  const catalog = await page.evaluate(() => {
    const E = TinyTroopsEquipment.rules, failures = [], result = [];
    for (const item of E.ITEMS) {
      // Find a native defender for every item, rather than an invented tower type.
      const definition = heroes.find(hero => E.compatible(item, { ...hero, team: 'ally' }));
      if (!definition) { failures.push(item.id + ': no native compatible defender'); continue; }
      qaPrep([definition.n, 'Squire'], 2); const copy = E.addItem(S, item.id), unit = S.squad[3];
      const equipped = TinyTroopsEquipment.equip(unit.ttId, 0, copy.id); if (!equipped.ok) { failures.push(item.id + ': equip failed'); continue; }
      fight(); qaFreeze(); const validBattle = S.phase === 'battle';
      if (!validBattle) { failures.push(item.id + ': no battle'); continue; }
      S.enemies.forEach(enemy => { enemy.maxHp = enemy.hp = 100000; enemy.shield = 0; enemy.armor = 8; enemy.burn = 2; });
      S.tt.rng = 1;
      for (let attack = 0; attack < 10; attack++) { allyAtk(unit); qaFreeze(); }
      if (![unit.hp, unit.atk, unit.spd, ...S.enemies.map(enemy => enemy.hp)].every(Number.isFinite)) failures.push(item.id + ': invalid combat stats');
      result.push({ id: item.id, attacks: S.battle?.equipmentActs?.[unit.ttId] || 0 });
    }
    return { failures, result };
  });
  assert.deepEqual(catalog.failures, []); assert.equal(catalog.result.length, 18); assert.ok(catalog.result.every(row => row.attacks > 0));
  console.log('PASS all 18 equipment items attached to compatible native defenders and exercised in live combat');

  const procs = await page.evaluate(() => {
    const E = TinyTroopsEquipment.rules, R = TinyTroopsRules, originalRandom = R.nextRandom, observed = {};
    R.nextRandom = () => 0; // Exercise chance effects at their successful boundary.
    try {
      for (const item of E.ITEMS.filter(item => ['burn', 'poison', 'slow', 'guardian', 'blizzard', 'storm', 'echoChance', 'chainChance', 'reach', 'splash'].some(key => item.mods[key]))) {
        const name = heroes.find(hero => E.compatible(item, { ...hero, team: 'ally' })).n;
        qaPrep([name, 'Squire'], 2); const unit = S.squad[3], ally = S.squad[7], copy = E.addItem(S, item.id); TinyTroopsEquipment.equip(unit.ttId, 0, copy.id); fight(); qaFreeze();
        const enemy = S.enemies[0]; S.enemies = Array.from({ length: 5 }, (_, index) => ({ ...enemy, n: 'Equipment Dummy ' + index, hp: 100000, maxHp: 100000, dead: false, shield: 0, armor: 0, burn: 0, poison: 0, slow: 0, focus: 'near' }));
        const shield = ally.shield || 0;
        for (let action = 0; action < 5; action++) allyAtk(unit);
        observed[item.id] = { burn: S.enemies.some(enemy => enemy.burn > 0), poison: S.enemies.some(enemy => enemy.equipmentPoison > 0), slow: S.enemies.some(enemy => enemy.slow > 0), allSlowed: S.enemies.every(enemy => enemy.slow > 0), shield: ally.shield > shield, otherHit: S.enemies.slice(1).some(enemy => enemy.hp < 100000), allHit: S.enemies.every(enemy => enemy.hp < 100000), totalDamage: S.enemies.reduce((sum, enemy) => sum + 100000 - enemy.hp, 0) };
      }
    } finally { R.nextRandom = originalRandom; }
    return observed;
  });
  assert.ok(procs['flame-oil'].burn); assert.ok(procs['venom-chamber'].poison); assert.ok(procs['frost-crystal'].slow);
  assert.ok(procs['guardian-charm'].shield); assert.ok(procs['blizzard-core'].allSlowed); assert.ok(procs['stormheart-engine'].allHit);
  for (const item of ['watchers-lens', 'longshot-scope', 'chain-lightning-coil', 'explosive-payload', 'ancient-siege-relic']) assert.ok(procs[item].otherHit, item + ' must damage extra targets');
  assert.ok(procs['echo-mechanism'].totalDamage > 0);
  console.log('PASS measured burn, poison, slow, periodic shields, blizzard, storms and extra-target damage');

  await page.setViewportSize({ width: 390, height: 844 });
  await page.evaluate(() => { qaPrep(['Frost Archer']); TinyTroopsEquipment.rules.ITEMS.forEach(item => TinyTroopsEquipment.rules.addItem(S, item.id)); TinyTroopsEquipment.openInventory(S.squad[3]); });
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'Mobile equipment must not overflow the viewport');
  await page.selectOption('[data-tt-eq-filter="rarity"]', 'epic');
  assert.ok(await page.locator('[data-tt-eq-preview]').count() > 0);
  await page.locator('[data-tt-eq-preview]').first().click(); assert.ok(await page.locator('#ttItemPreview').innerText());
  await page.keyboard.press('Escape'); assert.equal(await page.locator('#drawer').evaluate(drawer => drawer.classList.contains('open')), false);
  assert.deepEqual(errors, [], 'Equipment interactions must not throw');
  console.log('PASS 390px mobile grid, filters, preview, and Escape dismissal');
})().then(() => { server.close(); return browser?.close(); }).catch(async error => { console.error(error); server.close(); await browser?.close(); process.exitCode = 1; });
