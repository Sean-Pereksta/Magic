/* Native boss attacks and combat recovery, using the shipped game in Chromium. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..');
const errors = [];
const server = http.createServer((req, res) => {
  const filename = path.resolve(root, '.' + decodeURIComponent(new URL(req.url, 'http://local').pathname));
  if (!filename.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(filename, (error, content) => {
    if (error) return res.writeHead(404).end();
    res.setHeader('Content-Type', filename.endsWith('.js') ? 'text/javascript' : filename.endsWith('.css') ? 'text/css' : 'text/html'); res.end(content);
  });
});
let browser;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } }); page.setDefaultTimeout(8000);
  page.on('pageerror', error => errors.push(error.stack));
  const combatErrors = [];
  page.on('console', message => { if (message.type() === 'error' && message.text().startsWith('Tiny Troops combat error')) combatErrors.push(message.text()); });
  await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`);
  await page.waitForFunction(() => window.TinyTroopsPolish?.ready);
  await page.fill('#menuUser', 'RecoveryQA'); await page.fill('#menuPass', 'qa-code'); await page.click('#menuNew');
  await page.evaluate(() => {
    const nativeRegion = region;
    window.qaBoss = null;
    // Let each native boss definition fight for long enough to use its skills,
    // including definitions outside the first region's normal spawn pool.
    region = function () { const r = nativeRegion(); return qaBoss ? { ...r, boss: [...r.boss, qaBoss] } : r; };
    window.qaBattle = function (wave = 5, bossName, armyNames = ['Squire', 'Archer', 'Medic', 'Wizard']) {
      qaBoss = bossName || null;
      reset(); ttClearTimers(); TinyTroopsPolish.timer = null; close(); document.getElementById('mainMenu').classList.remove('open');
      S.phase = 'recruit'; S.round = wave; S.tt.wavePlan = null; S.tt.draft = null; S.tt.event = null; S.tt.draftedRound = wave; S.choices = [];
      S.placing = S.placingEffect = S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = null; S.placingUpgrade = S.placingSpecialization = false; S.pendingStarChoices = [];
      S.squad = Array(wave >= 250 ? 20 : 16).fill(null);
      armyNames.forEach((name, i) => {
        const u = S.squad[i * 4 + 3] = clone(heroes.find(h => h.n === name)); u.baseMaxHp = u.maxHp = u.hp = 1e8; u.baseAtk = u.atk = 1;
      });
      if (bossName) {
        const e = mkEnemy(enemyDefs.find(e => e.n === bossName && e.boss), 1, true);
        S.tt.wavePlan = { round: wave, region: region().n, archetype: 'balanced', threat: 1, enemies: [e] };
      }
      fight(); ttClearTimers(); TinyTroopsPolish.timer = null;
      S.enemies.forEach(e => { e.hp = e.maxHp = 1e8; e.atk = 1; });
    };
  });
  const bosses = await page.evaluate(() => {
    const failures = [], names = [...new Set(enemyDefs.filter(e => e.boss).map(e => e.n))], originalRandom = Math.random;
    Math.random = () => .01;
    try {
      for (const name of names) {
        qaBattle(5, name);
        try {
          if (S.phase !== 'battle' || S.tt.mode !== 'PLAYING') throw new Error('Battle never started: ' + S.phase + ' / ' + TinyTroopsPolish.lastError);
          for (let i = 0; i < 160; i++) {
            S.squad.filter(Boolean).forEach(u => { u.hp = u.maxHp; });
            S.enemies.forEach(e => { if (!e.dead) e.hp = e.maxHp; });
            TinyTroopsPolish.step();
            if (S.phase !== 'battle') throw new Error('Fixture ended before sustained boss attacks: ' + S.phase);
          }
          if (S.battle.tick !== 160) throw new Error('Simulation stopped at tick ' + S.battle.tick);
        }
        catch (error) { failures.push({ name, tick: S.battle?.tick, message: error.message, stack: error.stack }); }
      }
    } finally { Math.random = originalRandom; }
    return { count: names.length, failures };
  });
  console.log('Native boss coverage:', JSON.stringify(bosses));
  assert.deepEqual(bosses.failures, [], 'Every boss must survive repeated native attacks');
  assert.deepEqual(combatErrors, [], 'Native boss actions must not need recovery');
  console.log(`PASS ${bosses.count} native bosses, 160 steps each, including five-second spell pulses`);
  const roster = await page.evaluate(() => {
    const names=[...new Set(heroes.filter(validHero).map(h=>h.n))], failures=[], originalRandom=Math.random;
    Math.random=()=>.01;
    try {
      for (const name of names) {
        qaBattle(5,'Ogre Boss',[name,'Squire','Archer','Medic']);
        try {
          if (S.tt.mode !== 'PLAYING') throw new Error(TinyTroopsPolish.lastError || 'Initialization failed');
          for (let i=0;i<40;i++) {
            S.squad.filter(Boolean).forEach(u=>{u.hp=u.maxHp;}); S.enemies.forEach(e=>{if(!e.dead)e.hp=e.maxHp;}); TinyTroopsPolish.step();
          }
          if(S.battle?.tick!==40)throw new Error('Simulation stopped before periodic abilities');
        } catch(error) {failures.push({name,error:error.message});}
      }
    } finally {Math.random=originalRandom;}
    return {count:names.length,failures};
  });
  assert.deepEqual(roster.failures, []); assert.deepEqual(combatErrors, []);
  console.log(`PASS ${roster.count} recruits through initialization and repeated periodic combat abilities`);

  await page.evaluate(() => {
    window.qaAbilities = tickBattleAbilities;
    window.qaStartBuffs = startBuffs;
    window.qaSummary = () => {
      const s = stableSnapshot();
      return { wave: s.round, coins: s.coins, kills: s.kills, points: s.points, drafted: s.tt.draftedRound, plan: s.tt.wavePlan, army: s.squad.map(u => u && [u.n,u.star,u.baseMaxHp,u.baseAtk,u.baseSpd,u.ttPath,u.fx]), rng: s.tt.rng };
    };
    window.qaInterrupt = () => {
      qaBattle(5, 'Ogre Boss'); const checkpoint = qaSummary();
      tickBattleAbilities = function () {
        S.coins += 999; S.kills += 20; S.points += 400; S.squad[3].dead = true; S.squad[3].hp = 0;
        ttTimeout(() => { S.coins += 100000; }, 60);
        throw new Error('QA broken spell <unsafe>');
      };
      loop(S.battle.id); return checkpoint;
    };
    window.qaStop = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
  });
  const expected = await page.evaluate(() => qaInterrupt());
  await page.locator('#ttRecovery:not([hidden])').waitFor({ state: 'visible' });
  assert.equal(await page.evaluate(() => S.tt.mode), 'PAUSED');
  assert.equal(await page.evaluate(() => ttRunTimers.size), 0);
  assert.ok(await page.locator('#ttRecovery pre').textContent().then(s => s.includes('QA broken spell <unsafe>')));
  assert.equal(await page.locator('#ttRecovery unsafe').count(), 0, 'Diagnostics must be escaped');
  await page.waitForTimeout(100);
  await page.evaluate(() => saveProfile('interrupted combat'));
  const saved = await page.evaluate(() => TinyTroopsSaves.createStore(localStorage).read('recoveryqa').data.state);
  assert.equal(saved.coins, expected.coins); assert.equal(saved.kills, expected.kills); assert.equal(saved.round, expected.wave);
  await page.click('[data-tt-speed="3"]'); await page.click('#ttPause'); await page.evaluate(() => startNextFight());
  assert.equal(await page.evaluate(() => ttRunTimers.size), 0, 'Resume and speed cannot restart a broken battle');
  await page.click('#ttRecovery [data-tt-action="new"]'); await page.click('#ttResume');
  assert.equal(await page.evaluate(() => S.tt.mode), 'PAUSED');
  await page.click('#ttRecovery [data-tt-action="prepare"]');
  assert.equal(await page.evaluate(() => S.phase), 'recruit');
  assert.deepEqual(await page.evaluate(() => qaSummary()), expected, 'Recovery must rewind rewards, army, enemy plan, draft, and RNG');
  assert.equal(await page.locator('#drawer.open').count(), 0, 'Restoring preparation must not offer another card');
  await page.evaluate(() => { tickBattleAbilities = qaAbilities; });
  await page.click('#playNext'); await page.waitForFunction(() => S.battle?.tick > 0);
  assert.deepEqual(await page.evaluate(() => qaSummary()), expected);
  console.log('PASS interrupted-save safety, canceled callbacks, menu resume, rollback, and replay without another recruit');

  const repeated = await page.evaluate(() => qaInterrupt());
  const errorCount = combatErrors.length;
  await page.click('#ttRecovery [data-tt-action="retry"]');
  await page.waitForFunction(() => !!TinyTroopsPolish.recovery);
  assert.equal(combatErrors.length, errorCount + 1, 'A persistent failure must return to recovery');
  await page.click('#ttRecovery [data-tt-action="basic"]');
  await page.waitForFunction(() => S.battle?.basic && S.battle.tick > 2);
  assert.equal(await page.evaluate(() => !!TinyTroopsPolish.recovery), false);
  assert.equal(await page.evaluate(() => S.coins), repeated.coins);
  const basicId = await page.evaluate(() => S.battle.id);
  await page.click('#ttPause'); const tick = await page.evaluate(() => S.battle.tick);
  for (const speed of [1,2,3]) await page.click(`[data-tt-speed="${speed}"]`);
  await page.waitForTimeout(150); assert.equal(await page.evaluate(() => S.battle.tick), tick);
  assert.equal(await page.evaluate(() => S.battle.id), basicId);
  const basicSave = await page.evaluate(() => stableSnapshot());
  assert.equal(basicSave.tt.basicCombatRound, 5);
  await page.evaluate(snapshot => { restoreSnapshot(snapshot); qaStop(); fight(); qaStop(); }, basicSave);
  assert.equal(await page.evaluate(() => S.battle.basic), true, 'Reloading a recovery battle must keep its fallback');
  await page.evaluate(() => { S.enemies.forEach(e => { e.hp = 1; e.shield = 0; }); for (let i=0; i<100 && S.phase==='battle'; i++) TinyTroopsPolish.step(); qaStop(); openRelic(); });
  assert.equal(await page.evaluate(() => S.phase), 'relic'); assert.equal(await page.evaluate(() => S.tt.basicCombatRound), undefined);
  const reward = await page.evaluate(() => ({ coins:S.coins,kills:S.kills,points:S.points }));
  assert.deepEqual(reward, { coins:repeated.coins+12,kills:repeated.kills+1,points:repeated.points+113 });
  await page.evaluate(() => { TinyTroopsPolish.step(); end(true); });
  assert.deepEqual(await page.evaluate(() => ({ coins:S.coins,kills:S.kills,points:S.points })), reward, 'Rewards settle once');
  await page.evaluate(() => {
    pickRelic(0); doneShop(); qaStop(); close();
    S.tt.draftedRound = S.round; S.placing = S.placingEffect = null; S.placingUpgrade = S.placingSpecialization = false;
    tickBattleAbilities = qaAbilities; fight(); qaStop(); TinyTroopsPolish.step();
  });
  assert.equal(await page.evaluate(() => S.round), 6); assert.equal(await page.evaluate(() => !!S.battle?.basic), false);
  assert.equal(await page.evaluate(() => !!TinyTroopsPolish.recovery), false);
  console.log('PASS persistent-error fallback, pause/speeds, fallback reload, real victory, single rewards, and return to normal combat');

  await page.evaluate(() => {
    qaBattle(5, 'Ogre Boss'); const snapshot=stableSnapshot(); restoreSnapshot(snapshot); qaStop();
    startBuffs = () => { throw new Error('QA broken battle initializer'); }; fight();
  });
  assert.equal(await page.evaluate(() => TinyTroopsPolish.recovery?.stage), 'battle setup');
  await page.click('#ttRecovery [data-tt-action="basic"]');
  await page.waitForFunction(() => S.battle?.basic && S.battle.tick > 0);
  await page.evaluate(() => { startBuffs=qaStartBuffs; tickBattleAbilities=qaAbilities; qaStop(); ttTimeout(() => { throw new Error('QA asynchronous combat fault'); }, 5); });
  await page.waitForFunction(() => TinyTroopsPolish.recovery?.stage === 'combat callback');
  assert.equal(await page.evaluate(() => ttRunTimers.size), 0);
  console.log('PASS strict battle setup and asynchronous combat callback recovery');

  for (const viewport of [{ width:1280,height:900 },{ width:390,height:844 },{ width:360,height:640 }]) {
    await page.setViewportSize(viewport);
    const bounds=await page.evaluate(() => ({ width:document.documentElement.scrollWidth,viewport:innerWidth }));
    assert.ok(bounds.width<=bounds.viewport, JSON.stringify(bounds));
    await page.locator('#ttRecovery [data-tt-action="basic"]').scrollIntoViewIfNeeded();
    assert.equal(await page.locator('#ttRecovery [data-tt-action="basic"]').isEnabled(), true);
  }
  await page.click('#ttRecovery [data-tt-action="prepare"]');
  assert.equal(await page.evaluate(() => S.tt.mode), 'PLAYING');
  assert.equal(await page.locator('#ttRecovery:not([hidden])').count(), 0);
  console.log('PASS desktop/mobile recovery controls and repeated recovery');
  const beforeTransition = await page.evaluate(() => {
    qaBattle(5,'Ogre Boss'); const checkpoint=qaSummary(), nativeEnd=end;
    end=function(){ S.coins+=200; S.points+=300; S.round++; S.phase='dead'; S.tt.finished=true; S.tt.checkpoint=null; S.battle=null; throw new Error('QA interrupted victory transition'); };
    S.enemies.forEach(e=>{e.hp=0;e.dead=true;}); loop(S.battle.id); end=nativeEnd;
    saveProfile('interrupted transition'); return checkpoint;
  });
  assert.equal(await page.evaluate(() => TinyTroopsPolish.recovery.wave), 5);
  assert.equal(await page.evaluate(() => TinyTroopsSaves.createStore(localStorage).read('recoveryqa').data.state.coins), beforeTransition.coins);
  await page.click('#ttRecovery [data-tt-action="new"]');
  assert.equal(await page.locator('#ttResume').isVisible(), true);
  await page.click('#ttResume'); await page.click('#ttRecovery [data-tt-action="prepare"]');
  assert.deepEqual(await page.evaluate(() => qaSummary()), beforeTransition);
  console.log('PASS retained checkpoint after failed victory transition, partial reward rollback, and recoverable menu state');
  assert.deepEqual(errors, []);
  console.log('All boss and combat recovery checks passed.');
})().catch(error => { console.error(error); if (errors.length) console.error(errors); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
