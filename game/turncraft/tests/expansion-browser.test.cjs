const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const {assetPath, enablePixelInspection} = require('./helpers/cloudflare-assets.cjs');

const repo = path.resolve(__dirname, '../../..');
const units = [
  ['frontier', 'suppressorExosuit', 'suppressionSweep'],
  ['frontier', 'pulseOperative', 'disruptionPulse'],
  ['frontier', 'forwardFabricator', 'fieldMuster'],
  ['ascendant', 'latticeSkiff', 'warpConduit'],
  ['ascendant', 'chronoweaver', 'temporalDistortion'],
  ['ascendant', 'voidLancer', 'voidSpear'],
  ['brood', 'tunnelMaw', 'tunnelBreach'],
  ['brood', 'sporeback', 'sporeBloom'],
  ['brood', 'broodMatriarch', 'bloodClutch']
];
const expose = `window.expansionClock=100000;nowMs=()=>window.expansionClock;
window.tcExpansion={
  run:source=>eval(source),
  prepare(faction,type){resize();terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));entities=[];resourceNodes=[];players=[makePlayer(0,'Test',faction,false),makePlayer(1,'Opponent','frontier',false)];players.forEach(p=>{p.minerals=5000;p.gas=5000});gameStarted=true;gameOver=false;paused=false;modeState=null;lobbyId='';PLAYER=0;nextEntityId=1;areaEffects=[];pendingImpacts=[];temporaryObstacles=[];tileVisibleToFaction=()=>true;overlay.style.display='none';const e=addEntity('unit',type,0,8,8);selectEntity(e);focusSelected();draw();return e.id},
  select:selectEntity,
  load(file){return loadTurncraftImage(file)},
  assets(){return [...ASSET_IMAGES].map(([file,img])=>({file,width:img.naturalWidth||img.width,height:img.naturalHeight||img.height,atlas:img.turncraftAtlas}))}
};`;
const source = fs.readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8')
  .replace(/^import .*;$/gm, '')
  .replace('\nboot();', '\n' + expose)
  .replace('<script type="module">', '<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');

async function setup(width = 1280, height = 900) {
  const browser = await chromium.launch({headless: true, executablePath: process.env.CHROMIUM_EXECUTABLE_PATH || undefined, args: ['--no-sandbox']});
  const page = await browser.newPage({viewport: {width, height}}), errors = [];
  await page.addInitScript(enablePixelInspection);
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => {
    const artwork = assetPath(route.request().url());
    if (artwork) return route.fulfill({path: artwork, contentType: 'image/webp', headers: {'Access-Control-Allow-Origin': '*'}});
    const pathname = decodeURIComponent(new URL(route.request().url()).pathname);
    if (pathname === '/game/turncraft.html') return route.fulfill({contentType: 'text/html', body: source});
    const file = path.resolve(repo, '.' + pathname);
    if (file.startsWith(repo + path.sep) && fs.existsSync(file) && fs.statSync(file).isFile()) return route.fulfill({path: file});
    return route.fulfill({status: 404, body: ''});
  });
  await page.goto('http://turncraft.test/game/turncraft.html');
  assert.deepEqual(errors, []);
  return {browser, page, errors};
}

test('nine units show their own selectable ability, visible cooldown and commands on desktop and mobile', async () => {
  for (const [width, height] of [[1280, 900], [390, 844], [844, 390]]) {
    const {browser, page, errors} = await setup(width, height);
    try {
      for (const [faction, type, ability] of units) {
        await page.evaluate(([f, t]) => tcExpansion.prepare(f, t), [faction, type]);
        const button = page.locator(`#abilityButtons [data-ability="${ability}"]`);
        assert.equal(await button.count(), 1, type);
        assert.equal(await button.isVisible(), true, type);
        assert.equal(await button.getAttribute('aria-keyshortcuts'), 'Q', type);
        const state = await page.evaluate(() => {
          const panel = document.getElementById('commandPanel').getBoundingClientRect();
          const ability = document.querySelector('#abilityButtons [data-ability]').getBoundingClientRect();
          return {name: document.getElementById('selectionName').textContent, text: document.querySelector('#abilityButtons').textContent, panel: {x: panel.x, y: panel.y, right: panel.right, bottom: panel.bottom}, ability: {width: ability.width, height: ability.height}};
        });
        assert.ok(state.name.trim().length > 4, type);
        assert.match(state.text, /READY/);
        assert.ok(state.panel.x >= -1 && state.panel.y >= -1 && state.panel.right <= width + 1 && state.panel.bottom <= height + 1, `${type}: ${JSON.stringify(state.panel)}`);
        assert.ok(state.ability.width >= 44 && state.ability.height >= 44, `${type}: ${JSON.stringify(state.ability)}`);
        await button.click();
        const order = await page.evaluate(() => tcExpansion.run('({mode:interactionMode,payload:interactionPayload,ready:selected().abilityReadyAt,clock:nowMs()})'));
        if (['fieldMuster', 'bloodClutch'].includes(ability)) assert.ok(order.ready > order.clock, type);
        else {assert.equal(order.mode, 'ability', type); assert.equal(order.payload, ability, type)}
        await page.keyboard.press('Escape');
      }
      assert.deepEqual(errors, []);
    } finally {await browser.close()}
  }
});

test('original expansion PNGs and alternate deployment states load through the real asset system', async () => {
  const {browser, page, errors} = await setup();
  try {
    const files = await page.evaluate(types => types.map(type => tcExpansion.run(`assetKeyForType(${JSON.stringify(type)})`)), units.map(row => row[1]));
    files.push('forward fabricator deployed.png', 'tunnel maw open.png', 'broodling.png');
    const loaded = await page.evaluate(async files => Promise.all(files.map(async file => ({file, loaded: await tcExpansion.load(file)}))), files);
    assert.ok(loaded.every(asset => asset.loaded), JSON.stringify(loaded));
    const assets = await page.evaluate(() => tcExpansion.assets());
    assert.ok(assets.every(asset => asset.width >= 128 && asset.height >= 128), JSON.stringify(assets));
    assert.ok(assets.every(asset => asset.atlas?.sheet?.startsWith('https://') && asset.atlas.frame.w > 0 && asset.atlas.frame.h > 0), 'migrated static artwork must crop a real production atlas frame');
    const rendered = await page.evaluate(types => {
      const keys = [];
      for (const [faction, type] of types) {
        tcExpansion.prepare(faction, type);
        keys.push(tcExpansion.run('entityAssetKey(selected())'));
      }
      tcExpansion.prepare('frontier', 'forwardFabricator');
      keys.push(tcExpansion.run("selected().fabricatorDeployed=true;draw();entityAssetKey(selected())"));
      tcExpansion.prepare('brood', 'tunnelMaw');
      keys.push(tcExpansion.run("selected().tunnelDigging={};draw();entityAssetKey(selected())"));
      return keys;
    }, units);
    assert.ok(rendered.every(file => files.includes(file)), JSON.stringify(rendered));
    assert.ok(rendered.includes('forward fabricator deployed.png'));
    assert.ok(rendered.includes('tunnel maw open.png'));
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});

test('deployed Fabricator exposes paid infantry training, queue cancellation, rally and pack-up controls', async () => {
  const {browser, page, errors} = await setup();
  try {
    await page.evaluate(() => {tcExpansion.prepare('frontier', 'forwardFabricator');tcExpansion.run("addEntity('structure','academy',0,3,3)")});
    await page.locator('#abilityButtons [data-ability="fieldMuster"]').click();
    await page.evaluate(() => {window.expansionClock += 3001;tcExpansion.run('updateExpansionLogistics();refreshUI(true)')});
    assert.match(await page.locator('#abilityButtons').textContent(), /Pack Up/);
    assert.equal(await page.locator('#actionList .actionCard').count(), 2);
    assert.match(await page.locator('#actionList .actionCard').first().textContent(), /90/);
    await page.locator('#actionList .actionCard').first().click();
    assert.equal(await page.evaluate(() => tcExpansion.run('selected().queue.length')), 1);
    await page.locator('#commandButtons button').filter({hasText: /^Rally/}).click();
    assert.equal(await page.evaluate(() => tcExpansion.run('interactionMode')), 'rally');
    await page.keyboard.press('Escape');
    await page.locator('#commandButtons button').filter({hasText: /^Cancel Last/}).click();
    assert.equal(await page.evaluate(() => tcExpansion.run('selected().queue.length')), 0);
    await page.locator('#abilityButtons [data-ability="fieldMuster"]').click();
    assert.equal(await page.evaluate(() => tcExpansion.run('selected().fabricatorDeployed')), false);
    assert.equal(await page.locator('#actionList .actionCard').count(), 0);
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});

test('eligible troops receive an Enter Tunnel control and temporary summons do not', async () => {
  const {browser, page, errors} = await setup();
  try {
    await page.evaluate(() => {tcExpansion.prepare('brood', 'tunnelMaw');tcExpansion.run("const maw=selected();addEntity('structure','hatchery',0,8,7);useAbility(maw,'tunnelBreach',{x:14,y:8});window.expansionClock+=3001;updateExpansionLogistics();selectEntity(addEntity('unit','roach',0,9,8));")});
    await page.locator('#commandButtons button').filter({hasText: /^Enter Tunnel/}).click();
    assert.ok(await page.evaluate(() => tcExpansion.run('selected().x>=12')));
    await page.evaluate(() => tcExpansion.run("const summon=addEntity('unit','broodling',0,9,8);summon.temporarySummon=true;selectEntity(summon)"));
    assert.equal(await page.locator('#commandButtons button').filter({hasText: /^Enter Tunnel/}).count(), 0);
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});

test('active deployment, channels, fields and temporary swarms render at ordinary battlefield zoom', async () => {
  const {browser, page, errors} = await setup();
  try {
    for (const [faction, type, ability] of units) {
      await page.evaluate(([faction, type, ability]) => {
        tcExpansion.prepare(faction, type);
        const accepted = tcExpansion.run(`(()=>{const caster=selected();
          if(caster.type==='latticeSkiff'){addEntity('structure','pylon',0,14,8);addEntity('unit','zealot',0,13,8)}
          if(caster.type==='voidLancer')addEntity('structure','barracks',1,13,8);
          if(caster.type==='tunnelMaw')addEntity('structure','hatchery',0,8,7);
          const target=caster.type==='latticeSkiff'||caster.type==='tunnelMaw'?{x:14,y:8}:caster.type==='voidLancer'?{x:13,y:8}:{x:12,y:8};
          const accepted=useAbility(caster,${JSON.stringify(ability)},target);cam.zoom=1;draw();
          if(caster.fabricatorDeployAt){window.expansionClock+=3001;updateExpansionLogistics();draw()}
          if(caster.tunnelDigging){window.expansionClock+=3001;updateExpansionLogistics();draw()}
          refreshUI(true);return accepted})()`);
        if (!accepted) throw new Error(type + ' could not activate its visual state');
      }, [faction, type, ability]);
    }
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});
