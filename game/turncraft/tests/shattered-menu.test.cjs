const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '../../..');
const regions = [
  'amber_wastes',
  'ironwood_basin',
  'sundered_coast',
  'ashglass_expanse',
  'frozen_rift',
  'heart_of_fracture'
].map((id, i) => ({
  id,
  name: [
    'Amber Wastes',
    'Ironwood Basin',
    'Sundered Coast',
    'Ashglass Expanse',
    'Frozen Rift',
    'Heart of the Fracture'
  ][i],
  description: 'Distinct defensive routes and contested resource sites.'
}));
const catalog = {
  regions,
  missions: regions.flatMap((r, i) =>
    Array.from({ length: 4 }, (_, j) => ({
      id: `m${i * 4 + j + 1}`,
      index: i * 4 + j,
      regionId: r.id,
      title: `Operation ${i * 4 + j + 1}`,
      waves: 5,
      briefing: {
        speaker: 'elara_venn',
        text: 'Hold this position, Commander.'
      },
      objectives: [{ label: 'Protect the command base.' }]
    }))
  ),
  arenas: regions.map((r) => ({
    id: r.id,
    regionId: r.id,
    name: r.name + ' Outpost'
  })),
  characters: [{ id: 'elara_venn', name: 'Elara Venn' }]
};
const fakeFactions = ['frontier', 'ascendant', 'brood'].map((id, i) => ({
  id,
  name: ['Frontier Directorate', 'Ascendant Conclave', 'Brood Dominion'][i],
  short: id,
  icon: '◆',
  color: '#e5b678',
  summary: 'Command your army.',
  strengths: 'Defense · Mobility · Firepower',
  weaknesses: 'Protect your economy',
  playstyle: 'Combined arms'
}));
const fixture = `<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/game/turncraft/shattered-front/menu.css"><div id="overlay"><div id="startCard"></div></div><script>window.launches=[];window.TurncraftExpansion={catalog:${JSON.stringify(catalog)},getFactions:()=>${JSON.stringify(fakeFactions)},getUnits:()=>({rifleman:{f:'frontier',name:'Rifleman',desc:'Ranged infantry'},zealot:{f:'ascendant',name:'Zealot'},zergling:{f:'brood',name:'Zergling'}}),selectFaction:id=>window.faction=id,configureSolo:options=>window.solo=options,showSkirmish:()=>window.skirmish=true,startCampaign:(id,options)=>launches.push({id,...options}),startSurvival:(id,options)=>launches.push({id,...options}),setProfile:()=>{},resume:()=>{}};</script><script type="module" src="/game/turncraft/shattered-front/menu.mjs"></script>`;
async function setup(width = 1440, height = 1000) {
  const browser = await chromium.launch({
    headless: true,
    executablePath: process.env.CHROMIUM_EXECUTABLE_PATH || undefined
  });
  const page = await browser.newPage({ viewport: { width, height } }),
    errors = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await page.route('**/*', (route) => {
    const url = new URL(route.request().url());
    if (
      process.env.TURNCRAFT_EXPANSION_ART_DIR &&
      url.pathname.startsWith('/shattered-front/')
    ) {
      const artRoot = path.resolve(process.env.TURNCRAFT_EXPANSION_ART_DIR),
        art = path.resolve(
          artRoot,
          url.pathname.slice('/shattered-front/'.length)
        );
      if (art.startsWith(artRoot + path.sep) && fs.existsSync(art))
        return route.fulfill({
          contentType: art.endsWith('.png') ? 'image/png' : 'image/webp',
          body: fs.readFileSync(art)
        });
    }
    if (url.pathname === '/fixture')
      return route.fulfill({ contentType: 'text/html', body: fixture });
    if (url.pathname.endsWith('/profile.mjs'))
      return route.fulfill({
        contentType: 'text/javascript',
        body: `export function watchProfile(cb){cb({identity:{uid:'test',displayName:'Test Commander'},profile:{}});return ()=>{}} export function rankFor(){return {name:'Unranked'}}`
      });
    const file = path.resolve(root, '.' + url.pathname);
    if (
      url.host === 'turncraft.test' &&
      file.startsWith(root + path.sep) &&
      fs.existsSync(file) &&
      fs.statSync(file).isFile()
    )
      return route.fulfill({
        contentType: file.endsWith('.css')
          ? 'text/css'
          : file.endsWith('.json')
            ? 'application/json'
            : file.endsWith('.webp')
              ? 'image/webp'
              : 'text/javascript',
        body: fs.readFileSync(file)
      });
    return route.fulfill({ status: 404, body: '' });
  });
  await page.goto('http://turncraft.test/fixture');
  await page.getByRole('heading', { name: 'TURNCRAFT', exact: true }).waitFor();
  return { browser, page, errors };
}
test('campaign gates advance on victory, preserves stars, and deploys the confirmed faction', async () => {
  const { browser, page, errors } = await setup();
  try {
    await page.getByRole('button', { name: /^01 Campaign/ }).click();
    assert.equal(await page.locator('.sf-mission:disabled').count(), 3);
    await page.getByRole('button', { name: /Operation 1  Protect/ }).count();
    await page.locator('.sf-mission').first().click();
    await page.getByRole('button', { name: 'Choose your faction →' }).click();
    for (const name of [
      'Ascendant Conclave',
      'Brood Dominion',
      'Frontier Directorate'
    ])
      await page.locator('.sf-faction-tab').filter({ hasText: name }).click();
    await page
      .locator('.sf-faction-tab')
      .filter({ hasText: 'Brood Dominion' })
      .click();
    await page.getByRole('button', { name: 'Confirm & deploy →' }).click();
    await page.locator('#sfMenu').waitFor({ state: 'hidden' });
    assert.equal(await page.locator('#sfMenu').isVisible(), false);
    assert.deepEqual(await page.evaluate(() => launches), [
      { id: 'm1', factionId: 'brood', difficulty: 'medium' }
    ]);
    await page.evaluate(() =>
      window.dispatchEvent(
        new CustomEvent('turncraft:expansion-result', {
          detail: {
            mode: 'campaign',
            missionId: 'm1',
            won: true,
            stars: 2,
            wave: 5,
            optionalObjectives: []
          }
        })
      )
    );
    await page
      .getByRole('button', { name: 'Continue to next mission →' })
      .click();
    assert.equal(
      await page
        .getByRole('heading', { name: 'Operation 2', exact: true })
        .count(),
      1
    );
    await page
      .getByRole('button', { name: '← Command center', exact: true })
      .click();
    await page.getByRole('button', { name: /^01 Campaign/ }).click();
    assert.equal(await page.locator('.sf-mission:disabled').count(), 2);
    assert.match(
      await page.locator('.sf-mission').first().textContent(),
      /★★☆/
    );
    assert.deepEqual(errors, []);
  } finally {
    await browser.close();
  }
});
test('desktop, phone, and landscape navigation stays inside viewport and remains touch accessible', async () => {
  for (const [w, h] of [
    [1440, 1000],
    [390, 844],
    [844, 390]
  ]) {
    const { browser, page, errors } = await setup(w, h);
    try {
      const geometry = await page.evaluate(() => ({
        width: document.querySelector('#sfMenu').scrollWidth,
        buttons: [...document.querySelectorAll('.sf-nav-item')].map(
          (n) => n.getBoundingClientRect().height
        )
      }));
      assert.ok(geometry.width <= w + 1);
      assert.ok(geometry.buttons.every((h) => h >= 44));
      if (process.env.TURNCRAFT_SCREENSHOT_DIR) {
        fs.mkdirSync(process.env.TURNCRAFT_SCREENSHOT_DIR, { recursive: true });
        await page.screenshot({
          path: path.join(
            process.env.TURNCRAFT_SCREENSHOT_DIR,
            `shattered-menu-${w}x${h}.png`
          ),
          fullPage: true
        });
      }
      await page.getByRole('button', { name: /^04 Endless Survival/ }).click();
      assert.equal(await page.locator('.sf-arena button:disabled').count(), 5);
      await page.locator('.sf-arena button').first().click();
      await page.getByRole('button', { name: 'Confirm & deploy →' }).click();
      await page.locator('#sfMenu').waitFor({ state: 'hidden' });
      assert.equal(await page.evaluate(() => launches[0].id), 'amber_wastes');
      assert.deepEqual(errors, []);
    } finally {
      await browser.close();
    }
  }
});
test('custom opponent factions are passed to the preserved skirmish setup', async () => {
  const { browser, page, errors } = await setup();
  try {
    await page.getByRole('button', { name: /^03 Single Player/ }).click();
    await page.getByLabel('Your faction').selectOption('ascendant');
    await page.getByLabel('Opponent 1').selectOption('brood');
    await page.getByLabel('Opponent 2').selectOption('');
    await page
      .getByRole('button', { name: 'Battlefield & match settings →' })
      .click();
    assert.deepEqual(await page.evaluate(() => solo), {
      factionId: 'ascendant',
      opponentFactions: ['brood']
    });
    assert.equal(await page.evaluate(() => skirmish), true);
    assert.deepEqual(errors, []);
  } finally {
    await browser.close();
  }
});
