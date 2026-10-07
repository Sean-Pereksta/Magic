const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..'), errors = [];
const server = http.createServer((req, res) => {
  const file = path.resolve(root, '.' + new URL(req.url, 'http://local').pathname);
  if (!file.startsWith(root + path.sep)) return res.writeHead(403).end();
  fs.readFile(file, (error, body) => { if (error) return res.writeHead(404).end(); res.setHeader('Content-Type', file.endsWith('.js') ? 'text/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html'); res.end(body); });
});
let browser;
async function configure(page) {
  page.on('pageerror', error => errors.push(error.stack));
  await page.route('https://www.gstatic.com/**', route => route.abort());
  await page.goto(`http://127.0.0.1:${server.address().port}/game/roguecard.html`);
  await page.waitForFunction(() => window.TinyTroopsSaveFiles && window.TinyTroopsPolish?.ready);
}
async function start(page, name, code = 'qa-code') {
  await page.fill('#menuUser', name); await page.fill('#menuPass', code); await page.click('#menuNew');
  await page.waitForFunction(() => S.tt.started && !document.getElementById('mainMenu').classList.contains('open'));
}
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  browser = await chromium.launch({ headless: true, ...(process.env.BROWSER_EXECUTABLE ? { executablePath: process.env.BROWSER_EXECUTABLE } : {}), args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] });
  const page = await browser.newPage({ viewport: { width: 390, height: 844 } }); await configure(page);
  assert.match(await page.locator('#ttSaveList').innerText(), /No saved runs yet/);
  await start(page, 'Save Army'); await page.click('#body .choice:first-child'); await page.click('.cell[data-i="3"]');
  await page.waitForFunction(() => S.phase === 'battle'); await page.evaluate(() => openMainMenu());
  assert.ok(await page.locator('[data-tt-load-save="save_army"]').isVisible());
  assert.equal(await page.evaluate(() => JSON.parse(localStorage.getItem('tinyTroops.save.v2.save_army')).state.squad.filter(Boolean).length), 1);
  await page.click('[data-tt-load-save="save_army"]'); assert.equal(await page.evaluate(() => S.squad.filter(Boolean).length), 1);
  assert.equal(await page.evaluate(() => S.tt.draftedRound), 1);
  await page.evaluate(() => openMainMenu()); await page.click('#menuNew');
  assert.equal(await page.inputValue('#menuUser'), 'Save Army 2');
  assert.deepEqual(await page.evaluate(() => JSON.parse(localStorage.getItem('tinyTroops.meta.v3.save_army_2')).units), await page.evaluate(() => JSON.parse(localStorage.getItem('tinyTroops.meta.v3.save_army')).units));
  assert.equal(await page.evaluate(() => JSON.parse(localStorage.getItem('tinyTroops.save.v2.save_army')).state.squad.filter(Boolean).length), 1);
  await page.evaluate(async () => { S.coins = 77; S.round = 27; await saveProfile('save browser QA'); openMainMenu(); });
  assert.match(await page.locator('#ttSaveList').innerText(), /Wave 27/);
  await page.reload(); await page.waitForFunction(() => window.TinyTroopsSaveFiles);
  assert.equal(await page.locator('[data-tt-load-save]').count(), 2);
  await page.click('[data-tt-load-save="save_army_2"]'); assert.equal(await page.evaluate(() => S.round), 27); assert.equal(await page.evaluate(() => S.coins), 77);
  console.log('PASS visible save files, automatic battle checkpoint, one-click load, reload, and separate named runs');

  await page.evaluate(() => { openMainMenu(); localStorage.setItem('tinyTroops.save.v2.save_army_2', '{broken'); }); await page.click('#ttRefreshSaves');
  assert.equal(await page.locator('[data-tt-load-save="save_army_2"]').innerText(), 'Recover');
  await page.click('[data-tt-load-save="save_army_2"]'); assert.equal(await page.evaluate(() => S.round), 27);
  const failed = await page.evaluate(async () => {
    const key = 'tinyTroops.save.v2.save_army_2', before = localStorage.getItem(key), original = Storage.prototype.setItem;
    Storage.prototype.setItem = function (key, value) { if (key.startsWith('tinyTroops.save.v2.')) throw new DOMException('Storage is full', 'QuotaExceededError'); return original.call(this, key, value); };
    S.coins += 10; const result = await saveProfile('quota QA');
    const unchanged = localStorage.getItem(key) === before, message = document.getElementById('ttSaveState').textContent; Storage.prototype.setItem = original;
    return { result, unchanged, message };
  });
  assert.equal(failed.result.local, false); assert.equal(failed.unchanged, true); assert.match(failed.message, /Save failed/);
  await page.evaluate(() => { S.coins = 99; window.dispatchEvent(new Event('pagehide')); });
  assert.equal(await page.evaluate(() => JSON.parse(localStorage.getItem('tinyTroops.save.v2.save_army_2')).state.coins), 99);
  console.log('PASS recovery backup, honest storage failure, retained old checkpoint, and synchronous page-exit save');

  const cloud = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  await cloud.addInitScript(() => {
    window.qaCloud = {}; window.qaFailRead = false; window.qaFailWrite = false;
    const clone = data => data ? JSON.parse(JSON.stringify(data)) : data;
    const reference = name => ({ path: name, get: async () => { if (window.qaFailRead) throw new Error('offline cloud'); const data = clone(window.qaCloud[name]); return { exists: !!data, data: () => data }; }, collection: collectionName => collection(name + '/' + collectionName) });
    const collection = name => ({ doc: id => reference(name + '/' + id) });
    const database = { collection, runTransaction: async callback => { const writes = []; const tx = { get: ref => ref.get(), set: (ref, data) => writes.push([ref.path, clone(data)]) }; await callback(tx); if (window.qaFailWrite) throw new Error('cloud write failed'); for (const [name, data] of writes) window.qaCloud[name] = data; } };
    const firestore = () => database; firestore.FieldValue = { serverTimestamp: () => 'server timestamp' };
    window.firebase = { apps: [{}], firestore, auth: () => ({ signInAnonymously: async () => ({ user: { uid: 'qa-user' } }) }) };
  });
  await configure(cloud); await start(cloud, 'Cloud Army'); await cloud.waitForFunction(() => window.qaCloud['tiny_troops_profiles/cloud_army']);
  assert.match(await cloud.locator('#ttSaveState').innerText(), /device \+ cloud/);
  const cachedLoad = await cloud.evaluate(async () => {
    S.round = 12; S.coins = 51; await saveProfile('cloud QA'); window.qaFailRead = true; S.coins = 0;
    const loaded = await loadProfile('Cloud Army', 'qa-code'); window.qaFailRead = false;
    return { loaded, wave: S.round, coins: S.coins, message: document.getElementById('menuStatus').textContent };
  });
  assert.equal(cachedLoad.loaded, true); assert.equal(cachedLoad.wave, 12); assert.equal(cachedLoad.coins, 51); assert.match(cachedLoad.message, /cloud unavailable/);
  const imported = await cloud.evaluate(async () => {
    const source = JSON.parse(JSON.stringify(window.qaCloud['tiny_troops_profiles/cloud_army'])); source.username = 'Other Device'; source.cleanUsername = 'other_device'; source.state.round = 42; source.updatedAtMs += 100;
    window.qaCloud['tiny_troops_profiles/other_device'] = source;
    await loadProfile('Other Device', 'qa-code'); openMainMenu();
    return { wave: S.round, saved: TinyTroopsSaveFiles.list().some(s => s.clean === 'other_device') };
  });
  assert.equal(imported.wave, 42); assert.equal(imported.saved, true);
  const rejected = await cloud.evaluate(async () => { const id = S.tt.id, result = await loadProfile('Other Device', 'wrong-code'); return { result, unchanged: S.tt.id === id }; });
  assert.equal(rejected.result, false); assert.equal(rejected.unchanged, true);
  await start(cloud, 'Occupied Cloud', 'different-code');
  await cloud.evaluate(async () => {
    const current = JSON.parse(localStorage.getItem('tinyTroops.save.v2.occupied_cloud'));
    window.qaCloud['tiny_troops_profiles/occupied_cloud'] = { ...current, password: 'original-code' };
    await saveProfile('wrong cloud code QA');
  });
  assert.equal(await cloud.evaluate(() => window.qaCloud['tiny_troops_profiles/occupied_cloud'].password), 'original-code');
  await cloud.click('#ttResolveSave'); assert.match(await cloud.locator('#ttSaveConflict').innerText(), /different code/);
  assert.equal(await cloud.locator('[data-tt-save-resolve="load"]').isEnabled(), true);
  await cloud.click('[data-tt-save-resolve="continue"]'); assert.equal(await cloud.evaluate(() => S.tt.mode), 'PLAYING');
  console.log('PASS cloud adapter transactions, offline local fallback, cloud-only discovery/cache, and save-code protection');

  // A new device may discover an older account only after it has already played.
  async function conflictingAccount(name, wave = 77) {
    await cloud.evaluate(() => { window.qaFailRead = true; openMainMenu(); }); await start(cloud, name);
    return cloud.evaluate(async ({ name, wave }) => {
      window.qaFailRead = false; S.coins = 55; S.round = 12; await saveProfile('device progress');
      const clean = cleanUsername(name), current = JSON.parse(localStorage.getItem(TinyTroopsSaves.PREFIX + clean)), older = JSON.parse(JSON.stringify(current));
      older.state.tt.id = current.state.tt.id + '-cloud'; older.state.round = wave; older.state.coins = 900; older.updatedAtMs -= 10000;
      window.qaCloud['tiny_troops_profiles/' + clean] = older;
      await saveProfile('cloud collision QA'); return { clean, deviceId: current.state.tt.id, cloudId: older.state.tt.id };
    }, { name, wave });
  }
  const copy = await conflictingAccount('Account Copy');
  // The recovery button must work even when a recruit dialog is open.
  await cloud.click('#ttResolveSave'); assert.equal(await cloud.locator('[data-tt-save-resolve="copy"]').isEnabled(), true);
  await cloud.click('[data-tt-save-resolve="copy"]'); await cloud.waitForFunction(() => !document.getElementById('mainMenu').classList.contains('open'));
  assert.equal(await cloud.inputValue('#menuUser'), 'Account Copy 2');
  assert.equal(await cloud.evaluate(({ clean }) => window.qaCloud['tiny_troops_profiles/' + clean].state.round, copy), 77);
  assert.equal(await cloud.evaluate(() => window.qaCloud['tiny_troops_profiles/account_copy_2'].state.coins), 55);
  await cloud.click('#body .choice:first-child'); assert.ok(await cloud.evaluate(() => S.placing));
  console.log('PASS same-name conflict controls stay clickable, preserve the old cloud account, save separately, and recruiting still works');

  const loadedAccount = await conflictingAccount('Account Load', 88);
  await cloud.click('#ttResolveSave'); await cloud.click('[data-tt-save-resolve="load"]');
  await cloud.waitForFunction(() => S.round === 88);
  assert.equal(await cloud.evaluate(() => S.coins), 900); assert.equal(await cloud.evaluate(() => S.tt.id), loadedAccount.cloudId);
  const backup = await cloud.evaluate(() => TinyTroopsSaveFiles.list().find(s => s.clean.startsWith('account_load_device')));
  assert.equal(backup.wave, 12); assert.equal(backup.coins, 55);
  await cloud.evaluate(async () => { S.coins = 901; await saveProfile('loaded existing account'); });
  assert.equal(await cloud.evaluate(() => window.qaCloud['tiny_troops_profiles/account_load'].state.coins), 901);
  assert.equal(await cloud.evaluate(() => TinyTroopsSaveFiles.conflict()), null);
  console.log('PASS loading the older existing account keeps newer device progress in a separate file and resumes cloud saving');

  const auto = await conflictingAccount('Account Auto', 66);
  const autoLoaded = await cloud.evaluate(async () => loadProfile('Account Auto', 'qa-code'));
  assert.equal(autoLoaded, true); assert.equal(await cloud.evaluate(() => S.tt.id), auto.cloudId);
  assert.equal(await cloud.evaluate(() => TinyTroopsSaveFiles.list().find(s => s.clean.startsWith('account_auto_device')).wave), 12);
  console.log('PASS ordinary Load recognizes the existing account despite a newer conflicting device file');

  await conflictingAccount('Account Offline'); await cloud.click('#ttResolveSave');
  await cloud.evaluate(() => { window.qaFailRead = true; }); await cloud.click('[data-tt-save-resolve="load"]');
  await cloud.waitForFunction(() => document.getElementById('menuStatus').textContent.includes('Cloud could not be reached'));
  assert.equal(await cloud.locator('[data-tt-save-resolve="load"]').isEnabled(), true); assert.equal(await cloud.locator('#menuLoad').isEnabled(), true);
  assert.equal(await cloud.evaluate(() => S.round), 12); await cloud.click('[data-tt-save-resolve="continue"]');
  await cloud.evaluate(() => { window.qaFailRead = false; });
  console.log('PASS failed cloud recovery unlocks every option and leaves the current device run playable');

  // Starting a new run checks cloud-only and legacy account names too.
  await cloud.evaluate(() => openMainMenu()); await start(cloud, 'Account Load');
  assert.equal(await cloud.inputValue('#menuUser'), 'Account Load 2');
  assert.equal(await cloud.evaluate(() => window.qaCloud['tiny_troops_profiles/account_load'].state.coins), 901);
  console.log('PASS new run preflight preserves cloud account names and automatically chooses a free save name');

  await conflictingAccount('Account Offline Reload', 54);
  const offlineReload = await cloud.evaluate(async () => {
    window.qaFailRead = true; const loaded = await loadProfile('Account Offline Reload', 'qa-code'); window.qaFailRead = false;
    await saveProfile('offline fallback ownership QA');
    return { loaded, wave: S.round, cloudWave: window.qaCloud['tiny_troops_profiles/account_offline_reload'].state.round, conflict: TinyTroopsSaveFiles.conflict()?.type };
  });
  assert.deepEqual(offlineReload, { loaded: true, wave: 12, cloudWave: 54, conflict: 'save/name-conflict' });
  console.log('PASS offline device loading retains the cloud-name guard when connectivity returns');

  await cloud.evaluate(() => {
    const legacy = JSON.parse(JSON.stringify(window.qaCloud['tiny_troops_profiles/account_load']));
    legacy.username = 'Legacy Account'; legacy.cleanUsername = 'legacy_account'; delete legacy.state.tt; legacy.state.round = 43;
    window.qaCloud['users/qa-user/tiny_troops_profiles/legacy_account'] = legacy; openMainMenu();
  });
  await start(cloud, 'Legacy Account'); assert.equal(await cloud.inputValue('#menuUser'), 'Legacy Account 2');
  assert.equal(await cloud.evaluate(() => window.qaCloud['users/qa-user/tiny_troops_profiles/legacy_account'].state.round), 43);
  assert.equal(await cloud.evaluate(async () => loadProfile('Legacy Account', 'qa-code')), true);
  assert.equal(await cloud.evaluate(() => S.round), 43);
  assert.equal(await cloud.evaluate(() => TinyTroopsSaveFiles.conflict()), null);
  console.log('PASS preflight and account loading recognize older saves without a modern run ID');

  // A denied/full device store still offers Continue; no action stays locked.
  await conflictingAccount('Account Quota', 58); await cloud.click('#ttResolveSave');
  await cloud.evaluate(() => { window.qaStorageWrite = Storage.prototype.setItem; Storage.prototype.setItem = function(key, value) { if (key.startsWith(TinyTroopsSaves.PREFIX)) throw new Error('Storage full'); return qaStorageWrite.call(this, key, value); }; });
  await cloud.click('[data-tt-save-resolve="load"]');
  await cloud.waitForFunction(() => document.getElementById('menuStatus').textContent.includes('Could not preserve'));
  assert.equal(await cloud.locator('[data-tt-save-resolve="continue"]').isEnabled(), true); assert.equal(await cloud.evaluate(() => S.round), 12);
  await cloud.click('[data-tt-save-resolve="copy"]'); await cloud.waitForFunction(() => !document.getElementById('mainMenu').classList.contains('open'));
  assert.equal(await cloud.inputValue('#menuUser'), 'Account Quota 2');
  assert.equal(await cloud.evaluate(() => window.qaCloud['tiny_troops_profiles/account_quota_2'].state.round), 12);
  assert.equal(await cloud.evaluate(() => window.qaCloud['tiny_troops_profiles/account_quota'].state.round), 58);
  assert.match(await cloud.locator('#ttSaveState').innerText(), /Saved to cloud.*browser storage unavailable/);
  await cloud.evaluate(() => { Storage.prototype.setItem = qaStorageWrite; });
  console.log('PASS a failed backup unlocks recovery and separate cloud saving works even with full device storage');

  await page.evaluate(() => openMainMenu());
  for (const size of [{ width: 1280, height: 900 }, { width: 390, height: 844 }, { width: 360, height: 640 }]) {
    await page.setViewportSize(size);
    assert.ok(await page.locator('[data-tt-load-save="save_army_2"]').isVisible());
    const bounds = await page.evaluate(() => { const r = document.querySelector('.menuCard').getBoundingClientRect(); return { scroll: document.documentElement.scrollWidth, width: innerWidth, top: r.top, bottom: r.bottom, height: innerHeight }; });
    assert.ok(bounds.scroll <= bounds.width && bounds.top >= 0 && bounds.bottom <= bounds.height + 1, JSON.stringify(bounds));
  }
  assert.deepEqual(errors, []); console.log('All Tiny Troops save browser checks passed. Cloud adapter cases use a deterministic Firebase mock.');
})().catch(error => { console.error(error); if (errors.length) console.error(errors); process.exitCode = 1; }).finally(async () => { await browser?.close(); await new Promise(resolve => server.close(resolve)); });
