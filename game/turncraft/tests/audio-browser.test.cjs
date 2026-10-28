const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const {assetPath} = require('./helpers/cloudflare-assets.cjs');
const repo = path.resolve(__dirname, '../../..');

async function setup(width = 1280, height = 900, touch = false) {
  const browser = await chromium.launch({headless: true, executablePath: process.env.CHROMIUM_EXECUTABLE_PATH || undefined, args: ['--no-sandbox']});
  const page = await browser.newPage({viewport: {width, height}, hasTouch: touch}), errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const source = fs.readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8')
    .replace(/^import .*;$/gm, '')
    .replace('\nboot();', `\nwindow.TurncraftStartup?.complete();
      window.audioClock=100000;nowMs=()=>window.audioClock;window.audioTest=source=>eval(source);
      resize();entities=[];resourceNodes=[];nextEntityId=1;
      players=[makePlayer(0,'Player','frontier',false),makePlayer(1,'Enemy','brood',false)];
      terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
      gameStarted=true;gameOver=false;paused=false;PLAYER=0;modeState=null;lobbyId='';isController=true;
      pendingImpacts=[];areaEffects=[];temporaryObstacles=[];scanEffects=[];tileVisibleToFaction=()=>true;
      overlay.style.display='none';initialCameraSet=true;spriteRuntime?.reset();
    `)
    .replace('<script type="module">', '<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
  await page.route('**/*', route => {
    const image = assetPath(route.request().url());
    if (image) return route.fulfill({path: image, contentType: 'image/webp', headers: {'Access-Control-Allow-Origin': '*'}});
    const pathname = decodeURIComponent(new URL(route.request().url()).pathname);
    if (pathname === '/game/turncraft.html') return route.fulfill({body: source, contentType: 'text/html'});
    const file = path.resolve(repo, '.' + pathname);
    if (file.startsWith(repo + path.sep) && fs.existsSync(file) && fs.statSync(file).isFile()) return route.fulfill({path: file, contentType: file.endsWith('.mp3') ? 'audio/mpeg' : undefined});
    return route.fulfill({status: 404, body: ''});
  });
  await page.goto('http://turncraft.test/game/turncraft.html');
  return {browser, page, errors};
}

test('real supplied MP3 decodes, loops, uses reduced volume, and saved controls survive reload', async () => {
  const {browser, page, errors} = await setup();
  try {
    assert.equal(await page.evaluate(() => TurncraftAudio.debug().unlocked), false);
    await page.locator('#tcAudio summary').click();
    await page.waitForFunction(() => document.getElementById('tcAudio').parentNode === document.body);
    const controls = await page.locator('.tcAudioControls').boundingBox();
    assert.ok(controls.y >= 0 && controls.y + controls.height <= 900, 'audio settings remain fully visible outside the clipped command dock');
    await page.waitForFunction(() => {const m = TurncraftAudio.debug().music; return m && m.duration > 1 && !m.paused && m.currentTime > 0;});
    const track = await page.evaluate(() => {const m = TurncraftAudio.debug().music; return {loop: m.loop, volume: m.volume, duration: m.duration, error: m.error?.message};});
    assert.equal(track.loop, true); assert.equal(track.volume, .35); assert.ok(track.duration > 30); assert.equal(track.error, undefined);
    await page.evaluate(() => {const m = TurncraftAudio.debug().music; m.currentTime = m.duration - .15;});
    await page.waitForFunction(() => TurncraftAudio.debug().music.currentTime < 3);
    await page.locator('[data-audio="music"]').fill('22');
    await page.locator('[data-audio="effects"]').fill('48');
    assert.deepEqual(await page.evaluate(() => TurncraftAudio.settings), {music: .22, effects: .48, muted: false});
    await page.locator('[data-audio="muted"]').check();
    assert.equal(await page.evaluate(() => TurncraftAudio.debug().music.paused), true);
    await page.reload();
    assert.deepEqual(await page.evaluate(() => TurncraftAudio.settings), {music: .22, effects: .48, muted: true});
    assert.deepEqual(errors, []);
  } finally {await browser.close();}
});

for (const [width, height, touch] of [[1280, 900, false], [390, 844, true], [844, 390, true]]) test(`${width}: action and visible combat cues use real audio; duplicate/hidden/offscreen events stay silent`, async () => {
  const {browser, page, errors} = await setup(width, height, touch);
  try {
    await page.locator('#tcAudio summary').click();
    const popup = await page.locator('.tcAudioControls').boundingBox();
    assert.ok(popup.y >= 0 && popup.y + popup.height <= height, 'audio settings fit the viewport');
    if (width <= 920) {
      const [button, minimap] = await Promise.all([page.locator('#tcAudio').boundingBox(), page.locator('#minimapDock').boundingBox()]);
      const overlaps = button.x < minimap.x + minimap.width && button.x + button.width > minimap.x && button.y < minimap.y + minimap.height && button.y + button.height > minimap.y;
      assert.equal(overlaps, false, 'sound control stays clear of the minimap in either orientation');
    }
    await page.evaluate(() => {window.audioEvents=[];const play=TurncraftAudio.play;TurncraftAudio.play=function(kind, options) {audioEvents.push({kind, options});return play(kind, options);};});
    await page.evaluate(() => audioTest(`globalThis.soundUnit=addEntity('unit','rifleman',0,10,10);selectEntities([soundUnit]);cam.zoom=.6;focusSelected();draw();sendCommand({type:'hold',entityId:soundUnit.id});`));
    assert.ok((await page.evaluate(() => audioEvents.map(e=>e.kind))).includes('select'));
    assert.ok((await page.evaluate(() => audioEvents.map(e=>e.kind))).includes('order'));
    await page.evaluate(() => audioTest(`spriteRuntime.combat.emit('sound-test','release',10,10,nowMs(),{family:'ballistic',visible:true});spriteRuntime.combat.emit('sound-test','release',10,10,nowMs(),{family:'ballistic',visible:true});spriteRuntime.combat.emit('hidden-test','release',10,10,nowMs(),{visible:false});spriteRuntime.combat.emit('offscreen-test','release',1000,1000,nowMs(),{});`));
    assert.equal(await page.evaluate(() => audioEvents.filter(e=>e.kind==='release').length), 1);
    assert.equal(await page.evaluate(() => TurncraftAudio.debug().context.state), 'running');
    await page.evaluate(() => {const scene=document.createElement('section');scene.id='sfMenu';scene.hidden=false;document.getElementById('sfMenu')?.remove();document.body.append(scene);});
    await page.waitForFunction(() => document.getElementById('tcAudio').parentNode === document.body);
    const rect=await page.locator('#tcAudio').boundingBox(); assert.ok(rect.x>=0&&rect.x+rect.width<=width);
    await page.evaluate(() => {document.getElementById('sfMenu').hidden=true;});
    await page.waitForFunction(() => document.getElementById('tcAudio').parentNode === document.body);
    assert.deepEqual(errors, []);
  } finally {await browser.close();}
});
