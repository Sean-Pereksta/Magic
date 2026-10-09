/* Real offline decoding and command playback through keyboard and touch. */
'use strict';
const assert = require('node:assert/strict'), path = require('node:path');
const { pathToFileURL } = require('node:url'), { chromium } = require('playwright');
const url = pathToFileURL(path.resolve(__dirname, '../../apes-together-strong.html')).href;

async function probe(page) {
  await page.waitForFunction(() => ATS.audio.commandBuffers.length === 9);
  return page.evaluate(() => {
    const a = ATS.audio, ctx = a.ctx, create = ctx.createBufferSource.bind(ctx);
    window.sampleStarts = [];
    ctx.createBufferSource = () => {
      const source = create(), start = source.start.bind(source);
      source.start = (...args) => {
        const clip = a.commandBuffers.find(c => c.buffer === source.buffer);
        if (clip) sampleStarts.push({ id: clip.id, time: args[0], duration: clip.buffer.duration, loop: source.loop });
        return start(...args);
      };
      return source;
    };
    const play = a._playCommandSounds;
    a._playCommandSounds = function (...args) {
      const original = Math.random; let first = true;
      Math.random = () => { if (first) { first = false; return .9; } return original(); };
      try { return play.apply(this, args); } finally { Math.random = original; }
    };
    const g = ATS.game;
    g.king.hp = g.king.maxHp = 1e6;
    for (let i = 0; i < 5; i++) g.makeApe(g.king.x + 30 + i * 12, g.king.y, 'follow');
    g.syncIndexes(); g.apeGrid.rebuild([g.king, ...g.apes]);
    return a.commandBuffers.map(c => ({ id: c.id, duration: c.buffer.duration, channels: c.buffer.numberOfChannels }));
  });
}
async function reset(page) {
  await page.evaluate(() => ATS.audio._stopCommandSounds());
  await page.waitForFunction(() => ATS.audio.commandSources.size === 0);
  await page.evaluate(() => { sampleStarts.length = 0; ATS.audio.last = Object.create(null); ATS.game.commandCD = 0; });
}
async function chorus(page) {
  const calls = await page.evaluate(() => sampleStarts);
  assert.equal(calls.length, 3);
  assert.equal(new Set(calls.map(c => c.id)).size, 3);
  assert.equal(new Set(calls.map(c => c.time)).size, 1);
  assert.ok(calls.every(c => !c.loop && c.duration > 0));
  assert.ok(await page.evaluate(() => ATS.audio.commandGain.gain.value * ATS.audio.commandSources.size <= .180001));
}

(async () => {
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}) });
  const errors = [], requests = [];
  const watch = page => { page.on('pageerror', e => errors.push(e.message)); page.on('request', r => { if (/^https?:/.test(r.url())) requests.push(r.url()); }); };
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } }); watch(page);
    await page.goto(url); assert.equal(await page.evaluate(() => ATS.audio.ctx), null);
    await page.locator('#newRun').click(); const decoded = await probe(page);
    assert.ok(decoded.every(c => c.duration > 0 && c.channels >= 1));
    for (const key of ['q', 'r', 'f', 'x', 'e']) { await reset(page); await page.keyboard.press(key); await chorus(page); }
    await reset(page); await page.evaluate(() => { const g = ATS.game, a = g.makeApe(g.king.x + 35, g.king.y, 'follow'); g.siege.select(a.species); });
    await page.keyboard.press('f'); await chorus(page);
    await page.keyboard.press('Escape'); await page.waitForFunction(() => ATS.audio.commandSources.size === 0); assert.equal(await page.evaluate(() => ATS.audio.paused), true);
    await page.locator('#resumeRun').click(); await reset(page);
    await page.evaluate(() => ATS.audio.setSettings({ enabled: false })); await page.keyboard.press('q'); assert.equal(await page.evaluate(() => sampleStarts.length), 0);
    await page.evaluate(() => ATS.audio.setSettings({ enabled: true, volume: 0 })); await reset(page); await page.keyboard.press('r'); assert.equal(await page.evaluate(() => sampleStarts.length), 0);
    await page.evaluate(() => ATS.audio.setSettings({ volume: .45 })); await reset(page); await page.keyboard.press('q'); await chorus(page);
    await page.evaluate(() => { Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); document.dispatchEvent(new Event('visibilitychange')); });
    await page.waitForFunction(() => ATS.audio.commandSources.size === 0); assert.equal(await page.evaluate(() => ATS.audio.paused), true);
    await page.evaluate(() => { delete document.hidden; document.dispatchEvent(new Event('visibilitychange')); });

    const mobile = await browser.newPage({ viewport: { width: 800, height: 850 }, hasTouch: true, isMobile: true }); watch(mobile);
    await mobile.goto(url); await mobile.locator('#newRun').tap(); await probe(mobile);
    await mobile.evaluate(() => ATS.mobileCommands.toggleArmy(true));
    for (const command of ['call', 'recall']) { await reset(mobile); await mobile.locator('[data-army-command="' + command + '"]').tap(); await chorus(mobile); }
    await mobile.evaluate(() => ATS.audio.dispose()); assert.equal(await mobile.evaluate(() => ATS.audio.commandSources.size), 0);
    assert.deepEqual(errors, []); assert.deepEqual(requests, [], 'downloaded HTML plays without network access');
    console.log(JSON.stringify({ result: 'PASS: nine original files decode offline; keyboard, selected-unit and touch commands start distinct simultaneous trios; soft mix, mute, zero volume, pause, hidden tab and disposal verified', decoded }, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
