/* Real-page regressions for draft loss, deferred decisions, stale actions and recovery. */
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..'), errors = [];
const server = http.createServer((req, res) => {
  const file = path.resolve(root, '.' + new URL(req.url, 'http://local').pathname);
  if (!file.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(file, (error, body) => { if (error) return res.writeHead(404).end(); res.setHeader('Content-Type', file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html'); res.end(body); });
});
let browser;
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu'] });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } }); page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.stack)); await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`); await page.waitForFunction(() => TinyTroopsPolish?.ready);
  await page.fill('#menuUser', 'LifecycleQA'); await page.fill('#menuPass', 'code'); await page.click('#menuNew');
  await page.evaluate(() => {
    window.qaFreeze = () => { ttClearTimers(); TinyTroopsPolish.timer = null; };
    window.qaPrep = (wave = 180) => {
      reset(); qaFreeze(); document.getElementById('mainMenu').classList.remove('open');
      S.phase = 'recruit'; S.round = wave; S.tt.mode = 'PLAYING'; S.tt.event = S.tt.wavePlan = S.tt.draft = null;
      S.tt.deferredDecisions = []; S.tt.pendingPlacement = null; S.tt.draftedRound = wave; S.pendingStarChoices = [];
      S.evoTarget = S.ascTarget = S.starChoiceTarget = S.ultimate10Target = S.placing = S.placingEffect = null;
      S.placingUpgrade = S.placingSpecialization = false; S.squad = Array(16).fill(null); S.bench = Array(3).fill(null);
      S.squad[3] = clone(heroes.find(h => h.n === 'Archer')); close(); render();
    };
  });
  await page.evaluate(() => {
    qaPrep(1); S.tt.draftedRound = null; openRecruit(); const names = S.tt.draft.map(c => c.n); choose(0);
    if (!S.placing) throw new Error('Draft fixture did not select a troop');
    const saved = stableSnapshot(); if (!saved.placing || !saved.tt.pendingPlacement) throw new Error('Selected recruit was lost before save');
    restoreSnapshot(saved); qaFreeze();
    if (!S.placing || S.placing.n !== saved.placing.n) throw new Error('Selected recruit was lost during restore');
    if (!TinyTroopsPolish.cancelPlacement() || S.squad.some(Boolean) && S.squad.filter(Boolean).length !== 1) throw new Error('Cancel corrupted the formation');
    if (S.placing || S.tt.pendingPlacement || JSON.stringify(S.tt.draft.map(c => c.n)) !== JSON.stringify(names)) throw new Error('Cancel consumed or rerolled the draft');
  });
  console.log('PASS selected recruits survive save/reload, cancel preserves formation and exact draft');
  await page.evaluate(() => {
    qaPrep(); const u = S.squad[3]; u.star = 4; u.starProg = starNeed(u) - 1; S.tt.draftedRound = null;
    S.tt.draft = S.choices = [{ type: 'upgrade', n: 'Star Training', e: '⭐' }]; choose(0); tapCell(3); qaFreeze();
    if (u.star !== 5 || S.tt.pendingPlacement || S.tt.draftedRound !== S.round) throw new Error('Applied training was not consumed before evolution');
    TinyTroopsPolish.deferChoice(); const state = stableSnapshot(); restoreSnapshot(state); qaFreeze();
    if (S.placingUpgrade || S.tt.pendingPlacement || S.squad[3].star !== 5) throw new Error('Reload regranted a training card that was already used');
  });
  console.log('PASS training that unlocks evolution is consumed before Later/save/reload');
  await page.evaluate(() => {
    qaPrep(); S.tt.draftedRound = null; S.tt.draft = S.choices = [{ type: 'tt-path', n: 'Specialize a troop', e: '🔀' }];
    choose(0); tapCell(3); qaFreeze(); if (S.tt.pendingPlacement || S.tt.draftedRound !== S.round) throw new Error('Selected specialization card was not consumed');
    close(); benchUnit(3); restoreSnapshot(stableSnapshot()); qaFreeze();
    if (S.placingSpecialization || S.tt.pendingPlacement) throw new Error('Reload regranted an applied specialization card');
    TinyTroopsPolish.openDeferredChoice(); document.querySelector('[data-tt-path]').click(); qaFreeze();
    if (!S.bench[0].ttPath || TinyTroopsPolish.pendingDecisionCount()) throw new Error('A deferred path could not apply after its troop moved to the bench');
  });
  console.log('PASS applied specialization stays consumed and its deferred choice follows a troop to the bench');
  await page.evaluate(() => {
    qaPrep(); const u = S.squad[3]; const before = JSON.stringify({ u: u, coins: S.coins, boost: S.boost });
    const result = TinyTroopsPolish.applyUpgrade(u, () => { u.baseAtk += 500; S.coins -= 15; S.boost.atk += 20; throw new Error('Injected failed upgrade'); });
    if (result || JSON.stringify({ u: u, coins: S.coins, boost: S.boost }) !== before) throw new Error('Failed upgrade changed progression or resources');
    const detached = clone(heroes[0]); if (TinyTroopsPolish.applyUpgrade(detached, () => detached.star++)) throw new Error('Detached troop could be upgraded');
  });
  console.log('PASS failed upgrades restore troop stats and resources; removed troop references are rejected');
  await page.evaluate(() => {
    qaPrep(1); fight(); qaFreeze(); const enemy = S.enemies[0], coins = S.coins, kills = S.kills;
    S.enemies = S.enemies.filter(e => e !== enemy); const hp = enemy.hp; dmg(S.squad[3], enemy, 1e9); kill(S.squad[3], enemy); heal(S.squad[3], enemy, 1e9);
    if (enemy.hp !== hp || S.coins !== coins || S.kills !== kills) throw new Error('Detached target changed battle rewards');
    const dead = S.enemies[0]; if (dead) { dead.hp = 0; dead.dead = true; dmg(S.squad[3], dead, 1e9); kill(S.squad[3], dead); if (S.coins !== coins) throw new Error('Dead target paid a reward twice'); }
    const startPoints = S.points; end(true); qaFreeze(); const settled = { round: S.round, points: S.points, coins: S.coins }; end(true); end(false); qaFreeze();
    if (S.points <= startPoints || JSON.stringify(settled) !== JSON.stringify({ round: S.round, points: S.points, coins: S.coins })) throw new Error('Repeated wave settlement changed rewards');
  });
  console.log('PASS removed/dead targets and duplicate battle endings cannot grant duplicate rewards');
  await page.evaluate(() => {
    qaPrep(); const u = S.squad[3]; u.star = 5; openEvolution(u); const ids = S.evoChoices.map(c => c.id); TinyTroopsPolish.deferChoice();
    if (S.phase !== 'recruit' || TinyTroopsPolish.pendingDecisionCount() !== 1) throw new Error('Evolution still locks preparation');
    fight(); qaFreeze(); if (S.phase !== 'battle') throw new Error('Deferred evolution still blocks battle');
    const state = stableSnapshot(); restoreSnapshot(state); qaFreeze(); TinyTroopsPolish.openDeferredChoice();
    if (JSON.stringify(S.evoChoices.map(c => c.id)) !== JSON.stringify(ids)) throw new Error('Deferred evolution offers changed after reload');
    TinyTroopsPolish.deferChoice(); if (TinyTroopsPolish.pendingDecisionCount() !== 1) throw new Error('Reopening duplicated the decision');
  });
  console.log('PASS evolution can be postponed through a battle and reload without lost or duplicate choices');
  await page.evaluate(() => {
    qaPrep(); const u = S.squad[3]; u.star = 8; u.evo = u.ascension = 'qa'; u.id ||= u.ttId;
    S.pendingStarChoices = [{ unitId: u.id, unit: u, star: 8 }]; processNextStarChoice();
    const ids = S.starChoiceChoices.map(c => c.id); TinyTroopsPolish.deferChoice(); fight(); qaFreeze();
    if (S.phase !== 'battle') throw new Error('Deferred high-star choice still blocks battle');
    restoreSnapshot(stableSnapshot()); qaFreeze(); TinyTroopsPolish.openDeferredChoice();
    if (JSON.stringify(S.starChoiceChoices.map(c => c.id)) !== JSON.stringify(ids)) throw new Error('High-star offers rerolled after reload');
  });
  console.log('PASS high-star choice postponement preserves randomized offers through reload');
  await page.evaluate(() => {
    qaPrep(); const u = S.squad[3]; u.star = 8; u.evo = u.ascension = 'qa'; u.id ||= u.ttId;
    S.pendingStarChoices = [{ unitId: u.id, unit: u, star: 8 }]; fight(); qaFreeze();
    if (S.phase !== 'lateStarChoice' || S.starChoiceStar !== 8 || S.evoTarget) throw new Error('Queued late-star choice was misrouted to an unavailable base evolution');
    close(); fight(); qaFreeze(); if (S.phase !== 'battle') throw new Error('Closing the queued late-star choice blocked the wave');
  });
  console.log('PASS queued high-star choices never route into an unavailable base evolution');
  await page.evaluate(() => {
    qaPrep(5); S.phase = 'relic'; S.shopOpen = true; openRelic(); const ids = S.tt.relicDraft.slice(); close();
    if (S.phase !== 'shop' || S.tt.relicDraft || TinyTroopsPolish.pendingDecisionCount() !== 1) throw new Error('Relic still locks the store');
    doneShop(); qaFreeze(); TinyTroopsPolish.openDeferredChoice(); if (JSON.stringify(S.tt.relicDraft) !== JSON.stringify(ids)) throw new Error('Deferred relic offers changed');
    pickRelic(0); qaFreeze(); if (TinyTroopsPolish.pendingDecisionCount() || S.relics.length !== 1 || S.phase !== 'recruit') throw new Error('Deferred relic was not settled exactly once');
  });
  console.log('PASS boss relics can wait until after the store and settle exactly once');
  await page.evaluate(() => { qaPrep(1); fight(); qaFreeze(); document.getElementById('ttPause').click(); const tick = S.battle.tick; openMainMenu(); document.getElementById('ttResume').click(); if (S.tt.mode !== 'PAUSED' || S.battle.tick !== tick) throw new Error('Menu resume changed a manually paused battle'); });
  await page.evaluate(() => {
    document.getElementById('ttPause').click(); qaFreeze(); S.battle.id = NaN; loop(NaN);
  });
  // A valid loop token enters the step validator; use a bad tick to exercise recovery.
  await page.evaluate(() => { S.battle.id = S._battleSeq; S.battle.tick = -1; loop(S.battle.id); if (TinyTroopsPolish.battleState !== 'RECOVERY' || !TinyTroopsPolish.recovery) throw new Error('Invalid state did not enter recovery'); });
  await page.click('[data-tt-action="prepare"]');
  assert.equal(await page.evaluate(() => TinyTroopsPolish.battleState), 'PREPARATION');
  console.log('PASS paused menu state is preserved and invalid battle tokens recover to preparation');
  assert.deepEqual(errors, []); console.log('All Tiny Troops lifecycle regressions passed.');
})().catch(error => { console.error(error); if (errors.length) console.error(errors); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });

