const assert = require('node:assert/strict');
const { test } = require('node:test');
const { chromium } = require('playwright');

test('the hosted game drafts teammates, starts combat and supports a live join', { timeout: 45_000 }, async t => {
  const { startMobaHost } = await import('../game/moba-mania/server/host.mjs');
  const host = await startMobaHost({ name:'Browser test lobby' });
  t.after(() => host.close());
  const browser = await chromium.launch({ executablePath:process.env.CHROMIUM_PATH || undefined });
  t.after(() => browser.close());
  const errors = [];
  const joinPage = async (name, viewport) => {
    const page = await browser.newPage({ viewport });
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(host.origin + host.joinPath());
    await page.locator('#online-name').fill(name);
    return page;
  };

  const hostPage = await joinPage('Rift Host');
  await hostPage.locator('#online-hero').selectOption('bulwark');
  await hostPage.getByRole('button', { name:'Join Azure seat 1' }).click();
  await hostPage.locator('#online-start').waitFor({ state:'visible' });

  const guest = await joinPage('Rift Guest', { width:390, height:844 });
  await guest.locator('#online-hero').selectOption('mirelle');
  await guest.getByRole('button', { name:'Join Rose seat 2' }).click();
  await guest.getByRole('button', { name:'Switch Azure seat 2' }).click();
  await hostPage.locator('.online-team[data-team="0"] .online-seat', { hasText:'Rift Guest' }).getByText('Mirelle', { exact:false }).waitFor();
  assert.equal(await hostPage.locator('.online-seat strong', { hasText:'Bot on start' }).count(), 4);
  assert.equal(await guest.locator('#online-start').isVisible(), false);

  await hostPage.locator('#online-start').click();
  await hostPage.locator('#hud').waitFor({ state:'visible' });
  await guest.locator('#hud').waitFor({ state:'visible' });
  await guest.waitForFunction(() => document.getElementById('hero-name').textContent.includes('Mirelle'));
  await hostPage.waitForFunction(() => document.getElementById('match-clock').textContent !== '0:00');

  const late = await joinPage('Live Joiner');
  await late.getByRole('button', { name:'Take over Rose seat 1' }).click();
  await late.locator('#hud').waitFor({ state:'visible' });
  assert.equal(await late.locator('#online-dialog').isVisible(), false);
  await late.locator('#pause-button').click();
  assert.equal(await late.locator('#save-match').isVisible(), false);
  await late.locator('#quit-match').click();
  await late.locator('#menu-screen').waitFor({ state:'visible' });
  assert.deepEqual(errors, []);
});
