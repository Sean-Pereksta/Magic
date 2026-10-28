const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const {assetPath} = require('./helpers/cloudflare-assets.cjs');
const repo = path.resolve(__dirname, '../../..');

async function setup(width, height, touch) {
  const browser = await chromium.launch({headless: true, executablePath: process.env.CHROMIUM_EXECUTABLE_PATH || undefined, args: ['--no-sandbox']});
  const page = await browser.newPage({viewport: {width, height}, hasTouch: touch}), errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const source = fs.readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8')
    .replace(/^import .*;$/gm, '')
    .replace('\nboot();', `\nwindow.TurncraftStartup?.complete();
      window.groupClock=100000;nowMs=()=>window.groupClock;window.groupTest=source=>eval(source);
      resize();entities=[];resourceNodes=[];nextEntityId=1;
      players=[makePlayer(0,'Player','ascendant',false),makePlayer(1,'Enemy','brood',false)];
      terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
      gameStarted=true;gameOver=false;paused=false;PLAYER=0;modeState=null;lobbyId='';isController=true;
      pendingImpacts=[];areaEffects=[];temporaryObstacles=[];scanEffects=[];tileVisibleToFaction=()=>true;
      overlay.style.display='none';initialCameraSet=true;spriteRuntime?.reset();
      window.testCommands=[];const actualSend=sendCommand;sendCommand=c=>{testCommands.push(c);return actualSend(c)};
    `)
    .replace('<script type="module">', '<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
  await page.route('**/*', route => {
    const image = assetPath(route.request().url());
    if (image) return route.fulfill({path: image, contentType: 'image/webp', headers: {'Access-Control-Allow-Origin': '*'}});
    const pathname = decodeURIComponent(new URL(route.request().url()).pathname);
    if (pathname === '/game/turncraft.html') return route.fulfill({body: source, contentType: 'text/html'});
    const file = path.resolve(repo, '.' + pathname);
    if (file.startsWith(repo + path.sep) && fs.existsSync(file) && fs.statSync(file).isFile()) return route.fulfill({path: file});
    return route.fulfill({status: 404, body: ''});
  });
  await page.goto('http://turncraft.test/game/turncraft.html');
  assert.deepEqual(errors, []);
  return {browser, page, errors};
}

for (const [width, height, touch] of [[1280, 900, false], [390, 844, true], [844, 390, true]]) {
  test(`${width}x${height}: one Blink command repositions ten Dragoons and targeting cancels safely`, async () => {
    const {browser, page, errors} = await setup(width, height, touch);
    try {
      await page.evaluate(() => groupTest(`globalThis.blinkArmy=Array.from({length:10},(_,i)=>addEntity('unit','dragoon',0,10+i%5,10+Math.floor(i/5)));selectEntities(blinkArmy);refreshUI(true);cam.zoom=.5;focusSelected();draw()`));
      const button = page.locator('[data-ability="phaseStep"]');
      assert.equal(await button.count(), 1);
      assert.match(await button.locator('small').first().textContent(), /10\/10/);
      assert.equal(await button.getAttribute('data-ready'), 'true');
      assert.equal(await button.isEnabled(), true);
      if (!touch) {
        await page.evaluate(() => {const item=document.createElement('button');item.id='reservedItem';item.dataset.hotkey='Q';document.body.appendChild(item);groupTest('refreshUI(false)')});
        const shortcuts=await page.locator('[data-ability]').evaluateAll(buttons=>buttons.map(b=>({key:b.dataset.hotkey,aria:b.getAttribute('aria-keyshortcuts'),label:b.querySelector('kbd')?.textContent})));
        assert.ok(shortcuts.every(b=>b.key!=='Q'&&b.key===b.aria&&b.key===b.label));
        assert.equal(new Set(shortcuts.map(b=>b.key)).size,shortcuts.length);
        await page.keyboard.press('q');
        assert.equal(await page.evaluate(()=>testCommands.length),0);
        await page.evaluate(()=>{document.getElementById('reservedItem').remove();groupTest('refreshUI(false)')});
      }
      await button.click();
      assert.equal(await page.evaluate(() => groupTest('interactionMode')), 'ability');
      if (touch) {
        await page.locator('.abilityCancel').click();
        assert.equal(await page.evaluate(()=>groupTest('interactionMode')),null);
        assert.equal(await page.evaluate(()=>testCommands.length),0);
        await button.click();
      }
      await page.keyboard.press('Escape');
      assert.equal(await page.evaluate(() => groupTest('interactionMode')), null);
      assert.equal(await page.evaluate(() => testCommands.length), 0);
      await button.click();
      const destination = await page.evaluate(() => groupTest(`hoverTile={x:7,y:10};drawTargetPreview();worldToScreen(7.5,10.5)`));
      if (touch) await page.touchscreen.tap(destination.x, destination.y);
      else {await page.mouse.move(destination.x, destination.y);await page.mouse.click(destination.x, destination.y)}
      const result = await page.evaluate(() => groupTest(`updateSpritePresentation();({commands:testCommands,mode:interactionMode,walking:blinkArmy.some(u=>spriteRuntime?.states.get(u.id)?.moving),units:blinkArmy.map((u,i)=>({x:u.x,y:u.y,dx:u.x-(10+i%5),dy:u.y-(10+Math.floor(i/5)),cooldown:u.abilityReadyAt-nowMs(),order:u.moveDestination}))})`));
      assert.equal(result.commands.length, 1);
      assert.equal(result.commands[0].type, 'groupAbility');
      assert.equal(result.commands[0].abilityId, 'phaseStep');
      assert.equal(new Set(result.units.map(u => `${u.x},${u.y}`)).size, 10);
      assert.ok(result.units.every(u => u.dx < 0 && Math.hypot(u.dx, u.dy) <= 2.3 && u.cooldown > 0 && !u.order), JSON.stringify(result));
      assert.equal(result.mode, null);
      assert.equal(result.walking, false);
      if (process.env.TURNCRAFT_SCREENSHOT_DIR) {
        fs.mkdirSync(process.env.TURNCRAFT_SCREENSHOT_DIR, {recursive: true});
        await page.evaluate(()=>groupTest('window.groupClock+=120;updateSpritePresentation();draw()'));
        await page.screenshot({path: path.join(process.env.TURNCRAFT_SCREENSHOT_DIR, `group-blink-${width}.png`)});
      }
      assert.deepEqual(errors, []);
    } finally {await browser.close()}
  });
}
