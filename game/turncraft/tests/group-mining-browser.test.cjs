const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');

const repo = path.resolve(__dirname, '../../..');
const expose = `window.miningClock=100000;nowMs=()=>window.miningClock;
window.tcMining={run:source=>eval(source),prepare(faction){
  resize();terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
  entities=[];resourceNodes=[];nextEntityId=1;nextNodeId=1;
  players=[makePlayer(0,'Test',faction,false),makePlayer(1,'Opponent','frontier',false)];players.forEach(p=>{p.minerals=5000;p.gas=5000});
  gameStarted=true;gameOver=false;paused=false;modeState=null;lobbyId='';PLAYER=0;
  areaEffects=[];pendingImpacts=[];temporaryObstacles=[];tileVisibleToFaction=()=>true;overlay.style.display='none';
  const fac=FACTIONS[faction];addEntity('structure',fac.base,0,10,10);
  window.miningWorkers=[[5,9],[5,10],[5,11]].map(([x,y])=>addEntity('unit',fac.worker,0,x,y));
  window.miningNodes=[[13,10],[13,11],[13,9]].map(([x,y])=>{addNode('mineral',x,y,1800);return resourceNodes.at(-1)});
  addNode('gas',12,13,1800);window.miningGas=resourceNodes.at(-1);addEntity('structure',fac.gas,0,12,13);
  window.miningCommands=[];sendCommand=c=>{window.miningCommands.push(c);applyCommand({...c,slot:PLAYER})};
  selectEntities(window.miningWorkers);cam.zoom=.8;centerOn(12,11);draw();refreshUI(true);
}};`;
const source = fs.readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8')
  .replace(/^import .*;$/gm, '')
  .replace('\nboot();', '\n' + expose + '\nwindow.TurncraftStartup?.complete();')
  .replace('<script type="module">', '<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
// Artwork content does not affect input routing; this fixture keeps the test offline.
const placeholder = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/6kQAAAAASUVORK5CYII=', 'base64');

async function setup(viewport, hasTouch) {
  const browser = await chromium.launch({headless: true, executablePath: process.env.CHROMIUM_EXECUTABLE_PATH || undefined, args: ['--no-sandbox']});
  const page = await browser.newPage({viewport, hasTouch}), errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => {
    const request = route.request(), url = new URL(request.url());
    if (request.resourceType() === 'image') return route.fulfill({contentType: 'image/png', body: placeholder});
    if (url.pathname === '/game/turncraft.html') return route.fulfill({contentType: 'text/html', body: source});
    const file = path.resolve(repo, '.' + decodeURIComponent(url.pathname));
    if (file.startsWith(repo + path.sep) && fs.existsSync(file) && fs.statSync(file).isFile()) return route.fulfill({path: file});
    return route.fulfill({status: 404, body: ''});
  });
  await page.goto('http://turncraft.test/game/turncraft.html');
  return {browser, page, errors};
}

async function tapTile(page, x, y, hasTouch) {
  const point = await page.evaluate(([x, y]) => tcMining.run(`worldToScreen(${x}+.5,${y}+.5)`), [x, y]);
  if (hasTouch) await page.touchscreen.tap(point.x, point.y);
  else await page.mouse.click(point.x, point.y);
}

test('desktop and touch group resource taps and the Mine button issue one distributed order without losing selection', async () => {
  for (const [viewport, hasTouch] of [[{width: 1280, height: 900}, false], [{width: 390, height: 844}, true]]) {
    const {browser, page, errors} = await setup(viewport, hasTouch);
    try {
      for (const faction of ['frontier', 'ascendant', 'brood']) {
        await page.evaluate(faction => tcMining.prepare(faction), faction);
        const mine = page.locator('#commandButtons button').filter({hasText: /^Mine$/});
        assert.equal(await mine.count(), 1, `${faction}: group Mine control`);
        await tapTile(page, 13, 10, hasTouch);
        let result = await page.evaluate(() => ({commands: miningCommands, selected: tcMining.run('selectedIds'), nodes: miningWorkers.map(w => w.economyNodeId), ids: miningWorkers.map(w => w.id)}));
        assert.equal(result.commands.length, 1, `${faction}: mineral tap`);
        assert.equal(result.commands[0].type, 'groupMine');
        assert.equal(new Set(result.nodes).size, 3);
        assert.deepEqual(result.selected, result.ids);

        await tapTile(page, 12, 13, hasTouch);
        result = await page.evaluate(() => ({commands: miningCommands, selected: tcMining.run('selectedIds'), nodes: miningWorkers.map(w => w.economyNodeId), ids: miningWorkers.map(w => w.id), gas: miningGas.id}));
        assert.equal(result.commands.length, 2, `${faction}: friendly extractor tap`);
        assert.equal(result.commands[1].type, 'groupMine');
        assert.equal(result.commands[1].nodeId, result.gas);
        assert.deepEqual(result.selected, result.ids);
        assert.ok(result.nodes.every(id => id === result.gas));

        await mine.click();
        assert.equal(await page.evaluate(() => tcMining.run('interactionMode')), 'mine');
        await tapTile(page, 13, 11, hasTouch);
        result = await page.evaluate(() => ({commands: miningCommands, mode: tcMining.run('interactionMode'), nodes: miningWorkers.map(w => w.economyNodeId)}));
        assert.equal(result.commands.length, 3, `${faction}: armed Mine`);
        assert.equal(result.commands[2].type, 'groupMine');
        assert.equal(result.mode, null);
        assert.equal(new Set(result.nodes).size, 3);
      }
      assert.deepEqual(errors, []);
    } finally {
      await browser.close();
    }
  }
});
