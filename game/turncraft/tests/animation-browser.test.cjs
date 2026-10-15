const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const {assetPath, enablePixelInspection} = require('./helpers/cloudflare-assets.cjs');
const {baseUrl: cloudflareBase, images: visualImages} = require('../assets/visual-asset-index.json');
const expansionManifest = require('../shattered-front-assets.json');
const animationManifest = require('../assets/animation-manifest.json');
const localArtwork = new Set(Object.values(visualImages).filter(record => record.localFallback).map(record => new URL(record.localFallback, 'http://turncraft.test/game/turncraft.html').href));
const requestedArtwork = (requests, file) => requests.has(new URL(file, cloudflareBase).href) || visualImages[file].localFallback && requests.has(new URL(visualImages[file].localFallback, 'http://turncraft.test/game/turncraft.html').href);
const {COMMAND_SCENE_FILES} = require('../preload.js');
const extraArtwork = new Set([
  ...Object.values(expansionManifest.assets).map(asset => new URL(asset.path, expansionManifest.defaultBaseURL).href),
  ...COMMAND_SCENE_FILES.map(file => new URL(file, cloudflareBase).href)
]);
const fixturePng = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aD1sAAAAASUVORK5CYII=', 'base64');

const repo = path.resolve(__dirname, '../../..');
const expose = `window.animationClock=100000;nowMs=()=>window.animationClock;
window.tcAnimation={
  run:source=>eval(source),
  prepare(){resize();entities=[];resourceNodes=[];nextEntityId=1;
    players=[makePlayer(0,'Player','frontier',false),makePlayer(1,'Opponent','brood',false)];
    players.forEach(p=>{p.minerals=5000;p.gas=5000});
    terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
    chosenMapId='classic';gameStarted=true;gameOver=false;paused=false;PLAYER=0;
    selectedId=null;selectedIds=[];modeState=null;lobbyId='';isController=true;
    pendingImpacts=[];areaEffects=[];temporaryObstacles=[];scanEffects=[];
    initialCameraSet=true;tileVisibleToFaction=()=>true;overlay.style.display='none';
    spriteRuntime?.reset();cam.x=0;cam.y=0;cam.zoom=1;return !!spriteRuntime;
  },
  advance(ms){window.animationClock+=ms;updateSpritePresentation();},
  get runtime(){return spriteRuntime},
  draw
};`;

async function setup(width = 1280, height = 900, {boot = false} = {}) {
  const browser = await chromium.launch({headless: true, executablePath: process.env.CHROMIUM_EXECUTABLE_PATH || undefined, args: ['--no-sandbox']});
  const page = await browser.newPage({viewport: {width, height}}), errors = [], artworkRequests = new Set();
  page.on('request', request => {if (request.url().startsWith(cloudflareBase) || localArtwork.has(request.url())) artworkRequests.add(request.url())});
  await page.addInitScript(enablePixelInspection);
  page.on('pageerror', error => errors.push(error.message));
  const source = fs.readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8')
    .replace(/^import .*;$/gm, '')
    .replace('\nboot();', '\n' + expose + (boot ? '\nboot();' : '\nwindow.TurncraftStartup?.complete();'))
    .replace('<script type="module">', '<script>const initializeApp=()=>({}),getAuth=()=>({currentUser:{uid:"local-test"}}),getFirestore=()=>({}),onAuthStateChanged=(auth,callback)=>{setTimeout(()=>callback(auth.currentUser),0);return()=>{}},signInAnonymously=()=>Promise.resolve();');
  await page.route('**/*', route => {
    const artwork = assetPath(route.request().url());
    if (artwork) return route.fulfill({path: artwork, contentType: 'image/webp', headers: {'Access-Control-Allow-Origin': '*'}});
    if (extraArtwork.has(route.request().url())) return route.fulfill({body: fixturePng, contentType: 'image/png', headers: {'Access-Control-Allow-Origin': '*'}});
    const pathname = decodeURIComponent(new URL(route.request().url()).pathname);
    if (pathname === '/game/turncraft.html') return route.fulfill({contentType: 'text/html', body: source});
    const file = path.resolve(repo, '.' + pathname);
    if (file.startsWith(repo + path.sep) && fs.existsSync(file) && fs.statSync(file).isFile()) return route.fulfill({path: file});
    return route.fulfill({status: 404, body: ''});
  });
  await page.goto('http://turncraft.test/game/turncraft.html');
  assert.deepEqual(errors, [], 'real browser initialization');
  assert.equal(await page.evaluate(boot => boot ? tcAnimation.run('!!spriteRuntime') : tcAnimation.prepare(),boot), true, 'animation script must initialize in the actual game');
  return {browser, page, errors, artworkRequests};
}

test('actual startup preloads every graphic while the deployed battlefield fills its bounded texture cache lazily', async () => {
  const {browser,page,errors,artworkRequests} = await setup(1280,900,{boot:true});
  try {
    await page.waitForFunction(() => window.TurncraftStartup.isReady,{},{timeout:30000});
    const initial = await page.evaluate(() => tcAnimation.run(`({assets:Object.keys(spriteRuntime.manifest.assets).length,staticFiles:[...ASSET_IMAGES.keys()],textures:spriteRuntime.cache.stats()})`));
    assert.ok(initial.assets>=76,'startup must use the comprehensive production manifest');
    assert.ok(initial.staticFiles.length<=6,'startup must not decode the whole legacy PNG roster');
    for (const file of Object.keys(visualImages)) assert.ok(requestedArtwork(artworkRequests,file), `prelaunch artwork includes ${file}`);
    for (const url of extraArtwork) assert.ok(artworkRequests.has(url), `prelaunch artwork includes ${url}`);
    assert.equal(initial.textures.ready, 0, 'preloading releases full images before the renderer requests visible textures');
    assert.ok(initial.textures.bytes<=initial.textures.limit);
    if (await page.locator('#sfMenu').isVisible()) {
      await page.getByRole('button', {name: /Single Player/}).click();
      await page.getByRole('button', {name: 'Battlefield & match settings →'}).click();
    }
    await page.locator('#startBtn').click();
    await page.waitForFunction(() => tcAnimation.runtime.cache.stats().ready>0 && tcAnimation.runtime.cache.stats().pending===0,{},{timeout:20000});
    const deployed = await page.evaluate(() => tcAnimation.run(`({started:gameStarted,entities:entities.filter(e=>e.alive!==false).length,textures:spriteRuntime.cache.stats(),mapped:entities.filter(e=>spriteRuntime.manifest.assets[e.type]).length})`));
    assert.equal(deployed.started,true);
    assert.ok(deployed.entities>0);
    assert.ok(deployed.mapped>0);
    assert.ok(deployed.textures.ready>0,'the running game must decode visible animation atlases');
    assert.ok(deployed.textures.bytes<=deployed.textures.limit);
    // A starting camera can show troops before their base. Bring a deployed
    // structure into view before requiring a lazy structure-atlas request.
    await page.evaluate(() => tcAnimation.run("(()=>{const base=entities.find(e=>e.alive!==false&&e.kind==='structure');if(base){centerOn(base.x,base.y);draw()}})()"));
    await page.waitForFunction(() => tcAnimation.runtime.cache.stats().pending===0);
    await page.waitForFunction(() => tcAnimation.run("terrainRenderer.stats.loaded.includes('effects') && terrainRenderer.stats.loaded.includes('classic:props')"));
    assert.ok(Object.keys(visualImages).every(file => requestedArtwork(artworkRequests,file)));
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('the real browser decodes every roster atlas and resolves every required directional animation', async () => {
  const {browser, page, errors} = await setup();
  try {
    const result = await page.evaluate(async () => {
      const runtime = tcAnimation.runtime;
      const loaded = await runtime.loadManifest('/game/turncraft/assets/animation-manifest.json');
      if (!loaded) return {loaded};
      const assets = runtime.manifest.assets, roster = tcAnimation.run('Object.keys(UNITS)'),
        structures = tcAnimation.run('Object.keys(STRUCTURES)'), selectable = ['larva','cocoon'], sheets = new Map(), failures = [];
      for (const type of [...roster, ...structures, ...selectable]) {
        const asset = assets[type];
        if (!asset) {failures.push(type + ': missing asset');continue}
        const required = roster.includes(type) ? ['idle','move','attack','hit','death'] : type==='larva' ? ['idle','death'] :
          type==='cocoon' ? ['idle','morph','death'] : ['idle','construction','production','death'];
        for (const state of required) {
          if (!asset.states[state]) failures.push(type + ': missing ' + state);
        }
        for (const [state, definition] of Object.entries(asset.states)) {
          if (!definition.sheet) {failures.push(type + '/' + state + ': missing sheet');continue}
          if (!sheets.has(definition.sheet)) {
            const image = new Image();
            const ok = await new Promise(resolve => {image.onload = () => resolve(true);image.onerror = () => resolve(false);
              image.src = new URL(definition.sheet, location.href).href});
            sheets.set(definition.sheet, ok ? {width:image.naturalWidth,height:image.naturalHeight} : null);
          }
          const bounds = sheets.get(definition.sheet);
          if (!bounds) {failures.push(definition.sheet + ': decode failed');continue}
          if (bounds.width > 2048 || bounds.height > 2048) failures.push(definition.sheet + ': exceeds mobile texture limit');
          for (const direction of definition.directions || ['S']) {
            for (let frame = 0; frame < definition.frames; frame++) {
              const rect = TurncraftAnimation.frameForState(definition,direction,0,{progress:(frame + .01)/definition.frames});
              if (rect.x < 0 || rect.y < 0 || rect.x + rect.w > bounds.width || rect.y + rect.h > bounds.height)
                failures.push(type + '/' + state + '/' + direction + '/' + frame + ': out of atlas bounds');
            }
          }
          if (roster.includes(type) && required.includes(state) && definition.directions?.length !== 8)
            failures.push(type + '/' + state + ': needs eight directions');
        }
      }
      return {loaded,roster:roster.length,structures:structures.length,sheets:sheets.size,failures};
    });
    assert.equal(result.loaded, true);
    assert.equal(result.roster, 40);
    assert.equal(result.structures, 34);
    assert.equal(result.sheets, new Set(Object.values(animationManifest.assets).flatMap(asset => Object.values(asset.states).map(state => state.sheet))).size);
    assert.deepEqual(result.failures, []);
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});

test('tile movement renders continuously through the original cooldown and never changes simulation speed', async () => {
  const {browser, page, errors} = await setup();
  try {
    const state = await page.evaluate(() => tcAnimation.run(`(()=>{
      const unit=addEntity('unit','rifleman',0,8,8);updateSpritePresentation();
      const cooldown=effectiveMoveCd(unit)*1000,at=nowMs();
      const moved=moveEntity(unit,9,8);updateSpritePresentation();
      const start={x:unit.x,px:unit.px,ready:unit.moveReadyAt,state:spriteRuntime.states.get(unit.id).state};
      window.animationClock=at+cooldown/2;updateSpritePresentation();
      const middle=unit.px;
      cam.x+=91;cam.zoom=1.8;draw();const cameraPosition=unit.px;
      const early=moveEntity(unit,10,8);
      applyCommand({type:'stop',entityId:unit.id,slot:0});updateSpritePresentation();const stopped=unit.px;
      window.animationClock=at+cooldown;updateSpritePresentation();
      const end={x:unit.x,px:unit.px,state:spriteRuntime.states.get(unit.id).state,ready:unit.moveReadyAt};
      const second=moveEntity(unit,10,8);updateSpritePresentation();const boundary=unit.px;
      window.animationClock+=cooldown/4;updateSpritePresentation();
      return {moved,cooldown,at,start,middle,cameraPosition,early,stopped,end,second,boundary,next:unit.px,saved:serializeEntity(unit)};
    })()`));
    assert.equal(state.moved, true);
    assert.equal(state.start.x, 9);
    assert.equal(state.start.px, 8);
    assert.equal(state.start.ready, state.at + state.cooldown);
    assert.equal(state.start.state, 'move');
    assert.ok(Math.abs(state.middle - 8.5) < .01, JSON.stringify(state));
    assert.equal(state.cameraPosition, state.middle, 'camera changes must not advance movement');
    assert.equal(state.early, false, 'animation cannot bypass movement cooldown');
    assert.equal(state.stopped, state.middle, 'interrupted orders cannot jump backward');
    assert.equal(state.end.px, 9);
    assert.equal(state.end.x, 9);
    assert.equal(state.end.state, 'idle', 'stationary units cannot retain walking animation');
    assert.equal(state.second, true);
    assert.equal(state.boundary, 9);
    assert.ok(state.next > 9 && state.next < 10);
    assert.equal(Object.hasOwn(state.saved, 'px'), false);
    assert.equal(Object.hasOwn(state.saved, 'py'), false);
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});

test('remote snapshots retain render continuity, reject stale state and safely correct ability teleports', async () => {
  const {browser, page, errors} = await setup();
  try {
    const state = await page.evaluate(() => tcAnimation.run(`(()=>{
      const unit=addEntity('unit','rifleman',0,8,8);updateSpritePresentation();
      const old=serializeState();old.simulationTick=1;
      const next=JSON.parse(JSON.stringify(old));next.simulationTick=2;
      next.entities[0].x=9;next.entities[0].moveReadyAt=nowMs()+effectiveMoveCd(unit)*1000;
      applyRemoteState(next);updateSpritePresentation(nowMs(),true);const initial=entities[0].px;
      window.animationClock+=250;updateSpritePresentation(nowMs(),true);const during=entities[0].px;
      applyRemoteState(old);updateSpritePresentation(nowMs(),true);const stale={x:entities[0].x,px:entities[0].px};
      const jump=JSON.parse(JSON.stringify(next));jump.simulationTick=3;jump.entities[0].x=22;jump.entities[0].y=20;
      jump.entities[0].moveReadyAt=0;applyRemoteState(jump);updateSpritePresentation(nowMs(),true);
      const teleport={x:entities[0].x,y:entities[0].y,px:entities[0].px,py:entities[0].py};
      const saved=serializeState();return {initial,during,stale,teleport,keys:Object.keys(saved.entities[0])};
    })()`));
    assert.equal(state.initial, 8);
    assert.ok(state.during > 8 && state.during < 9);
    assert.equal(state.stale.x, 9);
    assert.equal(state.stale.px, state.during, 'out-of-order updates cannot pull sprites backward');
    assert.deepEqual(state.teleport, {x:22, y:20, px:22, py:20});
    assert.ok(!state.keys.some(key => /sprite|render|segment|direction|^p[xy]$/i.test(key)), state.keys.join(', '));
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});

test('restoring an authoritative alive unit removes its earlier cosmetic death without duplicating its body', async () => {
  const {browser,page,errors} = await setup();
  try {
    const result = await page.evaluate(() => tcAnimation.run(`(()=>{
      const unit=addEntity('unit','rifleman',0,8,8);updateSpritePresentation();
      const saved=JSON.parse(JSON.stringify(serializeState()));killEntity(unit);updateSpritePresentation();
      const before=spriteRuntime.ghosts.length;saved.simulationTick=simulationTick+1;applyRemoteState(saved);updateSpritePresentation();
      const restored=entityById(unit.id);selectEntity(restored);const screen=worldToScreen(restored.px+.5,restored.py+.5);
      return {before,ghosts:spriteRuntime.ghosts.length,bodies:entities.filter(e=>e.id===unit.id&&e.alive!==false).length,
        state:spriteRuntime.states.get(unit.id).state,selected:selectedId===unit.id,hit:selectableEntityAtScreen(screen.x,screen.y)?.id,id:unit.id};
    })()`));
    assert.equal(result.before,1);
    assert.equal(result.ghosts,0);
    assert.equal(result.bodies,1);
    assert.equal(result.state,'idle');
    assert.equal(result.selected,true);
    assert.equal(result.hit,result.id);
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('cosmetic movement follows existing obstacle routes and keeps turning continuous at corners', async () => {
  const {browser,page,errors} = await setup();
  try {
    const result = await page.evaluate(() => tcAnimation.run(`(()=>{
      terrain[8*BOARD_W+9]={x:9,y:8,type:'rock',blocked:true};
      const unit=addEntity('unit','rifleman',0,8,8);updateSpritePresentation();
      const at=nowMs(),cooldown=effectiveMoveCd(unit)*1000,accepted=moveEntity(unit,9,9);
      updateSpritePresentation();
      window.animationClock=at+cooldown*.25;updateSpritePresentation();const first={px:unit.px,py:unit.py,direction:spriteRuntime.states.get(unit.id).direction};
      window.animationClock=at+cooldown*.75;updateSpritePresentation();const second={px:unit.px,py:unit.py,direction:spriteRuntime.states.get(unit.id).direction};
      window.animationClock=at+cooldown;updateSpritePresentation();return {accepted,at,cooldown,first,second,end:{px:unit.px,py:unit.py,x:unit.x,y:unit.y,ready:unit.moveReadyAt}};
    })()`));
    assert.equal(result.accepted,true);
    assert.ok(Math.abs(result.first.px-8)<.01 && result.first.py>8 && result.first.py<9,'rendered path must go around the rock');
    assert.equal(result.first.direction,'S');
    assert.ok(result.second.px>8 && result.second.px<9 && Math.abs(result.second.py-9)<.01);
    assert.equal(result.second.direction,'E');
    assert.deepEqual(result.end,{px:9,py:9,x:9,y:9,ready:result.at+result.cooldown});
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('touch taps and stationary holds retain moving troop selection without issuing movement', async () => {
  const {browser,page,errors} = await setup(390,844);
  try {
    const points = await page.evaluate(() => tcAnimation.run(`(()=>{
      const troop=addEntity('unit','rifleman',0,8,8),neighbor=addEntity('unit','rifleman',0,9,9);
      updateSpritePresentation();moveEntity(troop,10,8);window.animationClock+=effectiveMoveCd(troop)*250;updateSpritePresentation();
      const point=e=>worldToScreen((e.px??e.x)+.5,(e.py??e.y)+.5);
      centerOn(9,8);lastCanvasTap={at:0,entityId:null};
      return {troop:{id:troop.id,...point(troop)},neighbor:{id:neighbor.id,...point(neighbor)}};
    })()`));
    const touch = async (point,id,type) => page.evaluate(({point,id,type}) => {
      const canvas=document.querySelector('#game');canvas.setPointerCapture=()=>{};
      canvas.dispatchEvent(new PointerEvent(type,{pointerId:id,pointerType:'touch',button:0,clientX:point.x,clientY:point.y,bubbles:true}));
    },{point,id,type});
    await touch(points.troop,1,'pointerdown');await touch(points.troop,1,'pointerup');
    assert.deepEqual(await page.evaluate(() => tcAnimation.run('selectedIds.slice()')),[points.troop.id]);
    await touch(points.neighbor,2,'pointerdown');await touch(points.neighbor,2,'pointerup');
    assert.deepEqual(await page.evaluate(() => tcAnimation.run('selectedIds.slice()')),[points.neighbor.id]);
    await touch(points.troop,3,'pointerdown');
    await page.evaluate(id => tcAnimation.run(`window.animationClock+=effectiveMoveCd(entityById(${id}))*500;updateSpritePresentation()`),points.troop.id);
    await page.waitForTimeout(410);
    await touch(points.troop,3,'pointerup');
    const held = await page.evaluate(() => tcAnimation.run('({ids:selectedIds.slice(),orders:entities.map(e=>e.moveDestination)})'));
    assert.deepEqual(held.ids.sort((a,b)=>a-b),[points.troop.id,points.neighbor.id].sort((a,b)=>a-b));
    assert.ok(held.orders.every(order=>order===null),'a held visual target cannot issue a movement order');
    const enemy = await page.evaluate(troopId => tcAnimation.run(`(()=>{
      selectEntity(entityById(${troopId}));const foe=addEntity('unit','zergling',1,15,8);
      updateSpritePresentation();moveEntity(foe,13,8);window.animationClock+=effectiveMoveCd(foe)*250;updateSpritePresentation();
      centerOn(12,8);lastCanvasTap={at:0,entityId:null};const point=worldToScreen(foe.px+.5,foe.py+.5);return {id:foe.id,...point};
    })()`),points.troop.id);
    await touch(enemy,4,'pointerdown');await touch(enemy,4,'pointerup');
    assert.equal(await page.evaluate(id => tcAnimation.run(`entityById(${id}).targetId`),points.troop.id),enemy.id);
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('combat and death animations follow actual attacks while dead and hidden units remain unselectable', async () => {
  const {browser, page, errors} = await setup();
  try {
    const state = await page.evaluate(() => tcAnimation.run(`(()=>{
      const rifle=addEntity('unit','rifleman',0,8,8),enemy=addEntity('unit','zergling',1,10,8);
      updateSpritePresentation();const hp=enemy.hp,cooldown=effectiveAttackCd(rifle)*1000,at=nowMs();
      fireAt(rifle,enemy);updateSpritePresentation();
      const shot={state:spriteRuntime.states.get(rifle.id).state,direction:spriteRuntime.states.get(rifle.id).direction,ready:rifle.attackReadyAt,hp:enemy.hp,pending:pendingImpacts.length};
      window.animationClock+=50;applyDamage(enemy,1,rifle);updateSpritePresentation();
      const hit=spriteRuntime.states.get(enemy.id).state;
      selectEntity(enemy);killEntity(enemy,rifle);updateSpritePresentation();
      const death={alive:enemy.alive,selected:selectedIds.includes(enemy.id),occupied:!!entityAt(10,8),ghosts:spriteRuntime.ghosts.length};
      const hidden=addEntity('unit','zergling',1,25,25);tileVisibleToFaction=(f,x,y)=>x<20;updateSpritePresentation();
      killEntity(hidden,rifle);updateSpritePresentation();const hiddenGhosts=spriteRuntime.ghosts.length;
      window.animationClock+=2500;updateSpritePresentation();return {hp,cooldown,at,shot,hit,death,hiddenGhosts,expired:spriteRuntime.ghosts.length};
    })()`));
    assert.equal(state.shot.state, 'attack');
    assert.equal(state.shot.direction, 'E');
    assert.equal(state.shot.ready, state.at + state.cooldown);
    assert.equal(state.shot.hp, state.hp, 'ranged damage must retain authoritative impact timing');
    assert.ok(state.shot.pending > 0);
    assert.equal(state.hit, 'hit');
    assert.deepEqual(state.death, {alive:false, selected:false, occupied:false, ghosts:1});
    assert.equal(state.hiddenGhosts, 1, 'hidden deaths must not produce cosmetic objects');
    assert.equal(state.expired, 0);
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});

test('projectiles track moving rendered targets while preserving their original impact time and snapshot data', async () => {
  const {browser,page,errors} = await setup();
  try {
    const result = await page.evaluate(() => tcAnimation.run(`(()=>{
      const source=addEntity('unit','rifleman',0,8,8),target=addEntity('unit','zergling',1,11,8);
      updateSpritePresentation();moveEntity(target,11,10);window.animationClock+=effectiveMoveCd(target)*250;updateSpritePresentation();
      const hp=target.hp;fireAt(source,target);updateSpritePresentation();
      const authoritative=JSON.stringify(source.projectileAnim),impact=JSON.stringify(pendingImpacts);
      const initial=spriteProjectileOrigin(source,source.projectileAnim),direction=spriteRuntime.states.get(source.id).direction;
      moveEntity(source,9,8);window.animationClock+=100;updateSpritePresentation();
      const endpoint=spriteProjectileOrigin(source,source.projectileAnim);
      return {initial,direction,endpoint,body:{x:target.px+.5,y:target.py+.5},source:source.px,
        sourceOriginal:{x:JSON.parse(authoritative).x,y:JSON.parse(authoritative).y},
        same:authoritative===JSON.stringify(source.projectileAnim),sameImpact:impact===JSON.stringify(pendingImpacts),sameHP:hp===target.hp};
    })()`));
    assert.equal(result.direction,'E','firing orientation must face the currently displayed target body');
    assert.ok(Math.abs(result.endpoint.tx-result.body.x)<.001 && Math.abs(result.endpoint.ty-result.body.y)<.001);
    assert.ok(result.source>8,'the source must actually move after the shot');
    assert.equal(result.endpoint.x,result.sourceOriginal.x,'shots cannot follow the firing unit after launch');
    assert.equal(result.endpoint.y,result.sourceOriginal.y);
    assert.equal(result.same,true,'visual target tracking cannot mutate synchronized projectile coordinates');
    assert.equal(result.sameImpact,true,'visual target tracking cannot move damage timing');
    assert.equal(result.sameHP,true,'the tracking animation must not apply damage early');
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('working, construction, production and special animations follow authoritative jobs and timers', async () => {
  const {browser,page,errors} = await setup();
  try {
    const result = await page.evaluate(() => tcAnimation.run(`(()=>{
      const base=addEntity('structure','commandHub',0,7,7),worker=addEntity('unit','engineer',0,8,8);
      addNode('mineral',8,8,500);updateMining();updateSpritePresentation();
      const work=spriteRuntime.states.get(worker.id).state;
      applyCommand({type:'stop',entityId:worker.id,slot:0});updateSpritePresentation();
      const stopped=spriteRuntime.states.get(worker.id).state;
      const site=addEntity('structure','barracks',0,9,8,{complete:false,buildStart:nowMs()-500,buildEnd:nowMs()+500});
      applyCommand({type:'assistBuild',entityId:worker.id,targetId:site.id,slot:0});updateSpritePresentation();
      const construction={worker:spriteRuntime.states.get(worker.id).state,site:spriteRuntime.states.get(site.id).state,
        progress:constructionProgress(site),start:site.buildStart,end:site.buildEnd};
      completeConstruction(site);queueUnit(site,'rifleman');updateSpritePresentation();
      const production={state:spriteRuntime.states.get(site.id).state,queue:site.queue.length,start:site.buildStart,end:site.buildEnd};
      const marauder=addEntity('unit','marauder',0,13,8);updateSpritePresentation();
      const ability=useAbility(marauder,'fortify');updateSpritePresentation();
      const special={accepted:ability,state:spriteRuntime.states.get(marauder.id).state,ready:marauder.abilityReadyAt};
      const tank=addEntity('unit','siegeTank',0,15,8);updateSpritePresentation();
      const deployed=useAbility(tank,'siegeMode');updateSpritePresentation();
      return {work,stopped,construction,production,special,deployed,deployState:spriteRuntime.states.get(tank.id).state,time:nowMs()};
    })()`));
    assert.equal(result.work,'work');
    assert.equal(result.stopped,'idle');
    assert.equal(result.construction.worker,'work');
    assert.equal(result.construction.site,'construction');
    assert.equal(result.construction.progress,.5);
    assert.equal(result.production.state,'production');
    assert.equal(result.production.queue,1);
    assert.equal(result.production.start,result.construction.start);
    assert.equal(result.production.end,result.construction.end,'presentation cannot change construction completion time');
    assert.equal(result.special.accepted,true);
    assert.equal(result.special.state,'ability');
    assert.ok(result.special.ready>result.time);
    assert.equal(result.deployed,true);
    assert.equal(result.deployState,'deploy');
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('selectable larvae become visual cocoons and hatch on the unchanged authoritative morph timer', async () => {
  const {browser,page,errors} = await setup();
  try {
    const result = await page.evaluate(() => tcAnimation.run(`(()=>{
      players[0]=makePlayer(0,'Brood','brood',false);players[0].minerals=5000;players[0].gas=5000;
      const hatchery=addEntity('structure','hatchery',0,7,7);addEntity('structure','spawningPool',0,8,7);
      const larva=addLarva(0,8,8,hatchery.id);selectEntity(larva);updateSpritePresentation();
      const before={id:larva.id,asset:spriteRuntime.states.get(larva.id).assetType,kind:larva.kind};
      const time=nowMs(),duration=scaledTime(UNITS.zergling.time)*1000,accepted=morphLarva(larva,'zergling');updateSpritePresentation();
      const cocoon={type:larva.type,kind:larva.kind,asset:spriteRuntime.states.get(larva.id).assetType,
        state:spriteRuntime.states.get(larva.id).state,end:larva.morphEnd,saved:serializeEntity(larva)};
      window.animationClock=time+duration/2;updateSpritePresentation();const midpoint=spriteMorphProgress(larva);
      updateProduction(.016);const early=larva.kind;
      window.animationClock=time+duration;updateProduction(.016);updateSpritePresentation();
      return {before,time,duration,accepted,cocoon,midpoint,early,after:{id:larva.id,type:larva.type,kind:larva.kind,
        asset:spriteRuntime.states.get(larva.id).assetType,selected:selectedId===larva.id}};
    })()`));
    assert.equal(result.before.asset,'larva');
    assert.equal(result.before.kind,'larva');
    assert.equal(result.accepted,true);
    assert.equal(result.cocoon.type,'larva','the asset key cannot overwrite saved gameplay type');
    assert.equal(result.cocoon.kind,'cocoon');
    assert.equal(result.cocoon.asset,'cocoon');
    assert.equal(result.cocoon.state,'morph');
    assert.equal(result.cocoon.end,result.time+result.duration);
    assert.ok(Math.abs(result.midpoint-.5)<1e-9);
    assert.equal(result.early,'cocoon','a presentation frame cannot finish morphing early');
    assert.equal(Object.hasOwn(result.cocoon.saved,'assetType'),false);
    assert.deepEqual(result.after,{id:result.before.id,type:'zergling',kind:'unit',asset:'zergling',selected:true});
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('setup and in-battle graphics selectors agree and preserve Low, Medium and High across reloads', async () => {
  const {browser,page,errors} = await setup();
  try {
    for (const quality of ['low','medium','high']) {
      await page.evaluate(() => tcAnimation.run("gameStarted=false;overlay.style.display='flex';renderStartMenu()"));
      await page.locator('#graphicsQuality').selectOption(quality);
      const selected = await page.evaluate(() => ({quality:tcAnimation.runtime.quality,saved:localStorage.getItem('csf.graphicsQuality'),
        setup:document.querySelector('#graphicsQuality').value,battle:document.querySelector('#battleGraphicsQuality').value}));
      assert.deepEqual(selected,{quality,saved:quality,setup:quality,battle:quality});
      await page.reload();
      assert.equal(await page.evaluate(() => tcAnimation.runtime.quality),quality);
      assert.equal(await page.locator('#battleGraphicsQuality').inputValue(),quality);
    }
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('graphics quality resamples real atlases once and keeps every source rectangle inside the decoded texture', async () => {
  const {browser,page,errors} = await setup();
  try {
    assert.equal(await page.evaluate(() => tcAnimation.runtime.loadManifest('/game/turncraft/assets/animation-manifest.json')),true);
    const decoded=[];
    for (const [quality,scale] of [['low',.5],['medium',.75],['high',1]]) {
      await page.evaluate(quality => tcAnimation.run(`setGraphicsQuality(${JSON.stringify(quality)});spriteRuntime.cache.get(spriteRuntime.manifest.assets.rifleman.states.move.sheet)`),quality);
      await page.waitForFunction(() => tcAnimation.runtime.cache.stats().pending===0,{},{timeout:10000});
      const result = await page.evaluate(() => tcAnimation.run(`(()=>{
        const meta=spriteRuntime.manifest.assets.rifleman.states.move,file=meta.sheet,entry=spriteRuntime.cache.entries.get(file);
        const image=entry.image,scale=spriteRuntime.cache.sourceScale(file),width=image.naturalWidth||image.width,height=image.naturalHeight||image.height;
        let rectangle=null;const original=ctx.drawImage;
        ctx.drawImage=function(image,...args){if(args.length===8)rectangle=args.slice(0,4);return original.call(this,image,...args)};
        let drew;try{drew=spriteRuntime.drawState(ctx,'rifleman','move',{direction:'NW',progress:1,size:48})}finally{ctx.drawImage=original}
        const changed=window.previousQualityImage!==image;window.previousQualityImage=image;
        return {drew,scale,width,height,rectangle,changed,bytes:entry.bytes,expansion:{x:spriteRuntime.cache.frameExpansion(file),y:spriteRuntime.cache.frameExpansionY(file)},raw:TurncraftAnimation.frameForState(meta,'NW',0,{progress:1})};
      })()`));
      assert.equal(result.drew,true);
      assert.equal(result.scale,scale);
      assert.equal(result.changed,true,'changing quality must rebuild the decoded image rather than repeatedly decoding at draw time');
      assert.deepEqual(result.rectangle,[result.raw.x*result.expansion.x*scale,result.raw.y*result.expansion.y*scale,result.raw.w*result.expansion.x*scale,result.raw.h*result.expansion.y*scale]);
      assert.ok(result.rectangle[0]+result.rectangle[2]<=result.width && result.rectangle[1]+result.rectangle[3]<=result.height);
      assert.equal(result.bytes,result.width*result.height*4);
      decoded.push(result.bytes);
    }
    assert.ok(decoded[0]<decoded[1] && decoded[1]<decoded[2],'lower quality must reduce actual decoded texture memory');
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('static atlas previews use the repaired vertical and horizontal frame geometry at low quality',async()=>{
  const {browser,page,errors}=await setup();
  try{
    assert.equal(await page.evaluate(()=>tcAnimation.runtime.loadManifest('/game/turncraft/assets/animation-manifest.json')),true);
    await page.evaluate(()=>tcAnimation.run("setGraphicsQuality('low');spriteRuntime.cache.get(spriteRuntime.manifest.assets.engineer.states.idle.sheet)"));
    await page.waitForFunction(()=>tcAnimation.runtime.cache.stats().pending===0);
    const result=await page.evaluate(()=>tcAnimation.run(`(async()=>{
      const definition=spriteRuntime.manifest.assets.engineer.states.idle,file=definition.sheet,image=spriteRuntime.cache.get(file),rectangles=[];
      const original=CanvasRenderingContext2D.prototype.drawImage;
      CanvasRenderingContext2D.prototype.drawImage=function(source,...args){if(source===image&&this.canvas.width===256&&this.canvas.height===256)rectangles.push(args.slice(0,4));return original.call(this,source,...args)};
      let loaded;try{loaded=await loadStaticAtlasFrame('engineer.png')}finally{CanvasRenderingContext2D.prototype.drawImage=original}
      const preview=ASSET_IMAGES.get('engineer.png'),pixels=preview?.getContext('2d').getImageData(0,0,256,256).data;
      return {loaded,rectangles,nonempty:pixels&&pixels.some((value,index)=>index%4===3&&value>16),
        raw:TurncraftAnimation.frameForState(definition,'S',0),scale:spriteRuntime.cache.sourceScale(file),
        expansion:{x:spriteRuntime.cache.frameExpansion(file),y:spriteRuntime.cache.frameExpansionY(file)},type:preview?.turncraftAtlas?.type};
    })()`));
    assert.equal(result.loaded,true);assert.equal(result.type,'engineer');assert.equal(result.nonempty,true);
    assert.ok(result.expansion.y>1,'this fixture must exercise a vertically padded troop atlas');
    assert.deepEqual(result.rectangles,[[result.raw.x*result.expansion.x*result.scale,result.raw.y*result.expansion.y*result.scale,
      result.raw.w*result.expansion.x*result.scale,result.raw.h*result.expansion.y*result.scale]]);
    assert.deepEqual(errors,[]);
  }finally{await browser.close()}
});

test('explicit symmetric Brood direction corrections preserve anchors and scaled texture bounds', async () => {
  const {browser,page,errors} = await setup();
  try {
    assert.equal(await page.evaluate(() => tcAnimation.runtime.loadManifest('/game/turncraft/assets/animation-manifest.json')),true);
    const profiles=[['drone','E'],['lurker','E'],['tunnelMaw','W']];
    for (const [quality,scale] of [['low',.5],['high',1]]) {
      await page.evaluate(({quality,profiles}) => {
        tcAnimation.run(`setGraphicsQuality(${JSON.stringify(quality)})`);
        for(const [type] of profiles)tcAnimation.runtime.cache.get(tcAnimation.runtime.manifest.assets[type].states.idle.sheet);
      },{quality,profiles});
      await page.waitForFunction(() => tcAnimation.runtime.cache.stats().pending===0,{},{timeout:10000});
      const result = await page.evaluate(profiles => {
        const runtime=tcAnimation.runtime,results=[];
        const bounds=canvas=>{const pixels=canvas.getContext('2d').getImageData(0,0,256,256).data;let minX=256,maxX=-1,minY=256,maxY=-1,count=0;
          for(let y=0;y<256;y++)for(let x=0;x<256;x++)if(pixels[(y*256+x)*4+3]>16){minX=Math.min(minX,x);maxX=Math.max(maxX,x);minY=Math.min(minY,y);maxY=Math.max(maxY,y);count++}
          return {minX,maxX,minY,maxY,count}};
        for(const [type,direction] of profiles){
          const definition=runtime.manifest.assets[type].states.idle,flags=definition.directionFlipX;
          const normal=document.createElement('canvas'),flipped=document.createElement('canvas');normal.width=flipped.width=256;normal.height=flipped.height=256;
          const context=flipped.getContext('2d');let flips=0,rectangle=null;const originalScale=context.scale,originalDraw=context.drawImage;
          context.scale=function(x,y){if(x===-1&&y===1)flips++;return originalScale.call(this,x,y)};
          context.drawImage=function(image,...args){rectangle=args.slice(0,4);return originalDraw.call(this,image,...args)};
          const corrected=flags?.[direction]===true;flags[direction]=false;
          const drewNormal=runtime.drawState(normal.getContext('2d'),type,'idle',{direction,x:128,y:180,size:80});
          flags[direction]=true;const drewFlipped=runtime.drawState(context,type,'idle',{direction,x:128,y:180,size:80});
          results.push({type,corrected,drewNormal,drewFlipped,flips,normal:bounds(normal),flipped:bounds(flipped),
            rectangle,scale:runtime.cache.sourceScale(definition.sheet),imageWidth:runtime.cache.entries.get(definition.sheet).image.width,
            imageHeight:runtime.cache.entries.get(definition.sheet).image.height});
        }
        const asymmetric=Object.entries(runtime.manifest.assets).filter(([type,asset])=>asset.faction!=='brood')
          .flatMap(([type,asset])=>Object.values(asset.states).flatMap(state=>Object.entries(state.directionFlipX||{}).filter(([direction,value])=>value===true).map(([direction])=>type+'/'+direction)));
        return {results,asymmetric};
      },profiles);
      assert.deepEqual(result.asymmetric,[],'Frontier and Ascendant equipment must never be mirrored');
      for(const profile of result.results){
        assert.equal(profile.corrected,true,profile.type);
        assert.equal(profile.drewNormal,true);assert.equal(profile.drewFlipped,true);
        assert.equal(profile.flips,1);
        assert.equal(profile.scale,scale);
        assert.ok(profile.normal.count>100 && profile.flipped.count>100);
        assert.ok(Math.abs(profile.flipped.minX-(255-profile.normal.maxX))<=1 && Math.abs(profile.flipped.maxX-(255-profile.normal.minX))<=1,'mirroring must preserve the centered anchor');
        assert.equal(profile.normal.minY,profile.flipped.minY);assert.equal(profile.normal.maxY,profile.flipped.maxY);
        assert.ok(profile.rectangle[0]+profile.rectangle[2]<=profile.imageWidth && profile.rectangle[1]+profile.rectangle[3]<=profile.imageHeight);
      }
    }
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('missing sprite textures fall back to existing artwork without blocking selection or commands', async () => {
  const {browser,page,errors} = await setup();
  try {
    assert.equal(await page.evaluate(() => tcAnimation.run("loadTurncraftImage('rifleman.png')")),true);
    await page.evaluate(() => tcAnimation.run(`
      spriteRuntime.manifest={assets:{rifleman:{anchor:[.5,.5],states:{idle:{sheet:'../graphics/turncraft/missing-atlas.webp',
        frameWidth:128,frameHeight:128,frames:4,fps:6,directions:['S'],rows:{S:0},loop:true}}}}};
      selectEntity(addEntity('unit','rifleman',0,8,8));updateSpritePresentation();draw();
    `));
    await page.waitForFunction(() => tcAnimation.runtime.cache.stats().pending===0,{},{timeout:5000});
    const result = await page.evaluate(() => tcAnimation.run(`(()=>{
      let originalDraws=0;const original=ctx.drawImage;
      ctx.drawImage=function(image,...args){if(image===getAsset('rifleman.png'))originalDraws++;return original.call(this,image,...args)};
      try {drawEntitySprite(selected());} finally {ctx.drawImage=original;}
      const unit=selected();const moved=issueMoveOrder(unit,9,8);
      return {originalDraws,moved,selected:selectedId===unit.id,position:unit.x,failed:spriteRuntime.cache.entries.get('../graphics/turncraft/missing-atlas.webp')?.status};
    })()`));
    assert.ok(result.originalDraws>0,'the original painted image must actually draw after an atlas failure');
    assert.equal(result.moved,true);
    assert.equal(result.selected,true);
    assert.equal(result.position,9);
    assert.equal(result.failed,'failed');
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('impact sprites only draw real recent damage and stay within the per-frame graphics budget', async () => {
  const {browser,page,errors} = await setup();
  try {
    await page.evaluate(() => tcAnimation.run("terrainRenderer.load('classic')"));
    const result = await page.evaluate(() => tcAnimation.run(`(()=>{
      for(let i=0;i<500;i++)addEntity('unit','rifleman',0,i%BOARD_W,Math.floor(i/BOARD_W));
      updateSpritePresentation();
      let before=terrainRenderer.stats.effectDraws;drawEffects();const untouched=terrainRenderer.stats.effectDraws-before;
      for(const unit of entities)applyDamage(unit,1);
      const draws={};for(const quality of ['low','medium','high']){
        setGraphicsQuality(quality);before=terrainRenderer.stats.effectDraws;drawEffects();draws[quality]=terrainRenderer.stats.effectDraws-before;
      }
      window.animationClock+=300;before=terrainRenderer.stats.effectDraws;drawEffects();const expired=terrainRenderer.stats.effectDraws-before;
      tcAnimation.prepare();const source=addEntity('unit','rifleman',0,8,8),target=addEntity('unit','zergling',1,10,8);
      updateSpritePresentation();fireAt(source,target);const shots={};
      for(const [name,hidden] of [['target',10],['source',8],['visible',-1]]){
        tileVisibleToFaction=(f,x,y)=>x!==hidden;before=terrainRenderer.stats.effectDraws;drawShots();shots[name]=terrainRenderer.stats.effectDraws-before;
      }
      return {untouched,draws,expired,shots};
    })()`));
    assert.equal(result.untouched,0,'units without a damage event must not invoke impact atlas draws');
    for (const [quality,cap] of [['low',8],['medium',32],['high',64]]) {
      assert.ok(result.draws[quality]>0 && result.draws[quality]<=cap,quality + ': ' + result.draws[quality]);
    }
    assert.equal(result.expired,0,'completed impact events must stop drawing');
    assert.equal(result.shots.target,0,'projectile sprites must not reveal an obscured target');
    assert.equal(result.shots.source,0,'projectile sprites must not reveal an obscured source');
    assert.ok(result.shots.visible>0,'visible projectile events must draw the production effect atlas');
    assert.deepEqual(errors,[]);
  } finally {await browser.close()}
});

test('all six terrain atlases render with reusable deterministic caches without changing map state', async () => {
  const {browser, page, errors} = await setup();
  try {
    for (const map of ['classic','ember-rift','frostline-basin','verdant-divide','shattered-dunes','orbital-foundry']) {
      const state = await page.evaluate(map => tcAnimation.run(`(async()=>{
        chosenMapId=${JSON.stringify(map)};createGame([{name:'One',factionId:'frontier',ai:false},{name:'Two',factionId:'brood',ai:false}]);
        initialCameraSet=true;tileVisibleToFaction=()=>true;
        const loaded=await terrainRenderer.load(chosenMapId);
        const state=()=>JSON.stringify({entities:entities.map(serializeEntity),players,resourceNodes,
          areaEffects,pendingImpacts,temporaryObstacles,modeState,terrainCode:encodeTerrain()});
        const before=state();draw();const first=terrainRenderer.stats.builds;
        draw();draw();return {loaded,same:before===state(),first,stats:terrainRenderer.stats};
      })()`), map);
      assert.equal(state.loaded, true, map + ': actual terrain atlas must decode');
      assert.equal(state.same, true, map + ': decoration must preserve complete authoritative state');
      assert.ok(state.stats, map + ': terrain renderer must initialize');
      assert.ok(state.stats.loaded.includes(map));
      assert.ok(state.stats.loaded.includes(map + ':props'));
      assert.ok(state.stats.loaded.includes('effects'));
      assert.equal(state.stats.builds, state.first, map + ': stable terrain chunks must be reused');
      assert.ok(state.stats.hits > 0);
    }
    assert.deepEqual(errors, []);
  } finally {await browser.close()}
});

test('desktop and phone large battles respect texture, particle and death-effect budgets at every quality', async () => {
  for (const [width,height] of [[1280,900],[390,844]]) {
    const {browser,page,errors} = await setup(width,height);
    try {
      assert.equal(await page.evaluate(() => tcAnimation.runtime.loadManifest('/game/turncraft/assets/animation-manifest.json')), true);
      for (const quality of ['low','medium','high']) {
        await page.evaluate(quality => tcAnimation.run(`(()=>{
          tcAnimation.prepare();spriteRuntime.setQuality(${JSON.stringify(quality)});cam.zoom=.65;
          players=[makePlayer(0,'Frontier','frontier',false),makePlayer(1,'Ascendant','ascendant',false),makePlayer(2,'Brood','brood',false)];
          for(let i=0;i<600;i++){const faction=i%2===0?0:i%4===1?1:2;
            addEntity('unit',['rifleman','zealot','zergling'][faction],faction,i%BOARD_W,Math.floor(i/BOARD_W));}
          updateSpritePresentation();
          for(let i=0;i<120;i++)killEntity(entities[i*2+1]);
          selectEntities(entities.filter(e=>e.alive!==false&&e.faction===PLAYER));
          for(let i=240;i<560;i+=4){const target=entities[i+1+(i%8?2:0)];issueAttack(entities[i],target);fireAt(entities[i],target);}
          for(let i=0;i<40;i++)spawnFx(i%20,Math.floor(i/20),'#ffd180',24);
          for(const type of ['rifleman','zealot','zergling'])for(const state of Object.values(spriteRuntime.manifest.assets[type]?.states||{}))spriteRuntime.cache.get(state.sheet);
          draw();
        })()`),quality);
        await page.waitForFunction(() => tcAnimation.runtime.cache.stats().pending === 0,{},{timeout:20000});
        const result = await page.evaluate(() => tcAnimation.run(`(()=>{
          const logical=()=>JSON.stringify(entities.map(e=>[e.id,e.x,e.y,e.hp,e.alive,e.moveReadyAt,e.attackReadyAt]));
          const before=logical(),samples=[];let rendererSelectionCalls=0;const originalSelection=selectedEntities;
          selectedEntities=(...args)=>{rendererSelectionCalls++;return originalSelection(...args)};
          try{drawEntities()}finally{selectedEntities=originalSelection}
          for(let i=0;i<45;i++){window.animationClock+=16;const start=performance.now();draw();samples.push(performance.now()-start)}
          samples.sort((a,b)=>a-b);return {same:before===logical(),p95:samples[Math.floor(samples.length*.95)],
            mean:samples.reduce((a,b)=>a+b,0)/samples.length,particles:particles.length,
            selected:selectedIds.length,factions:new Set(entities.filter(e=>e.alive!==false).map(e=>e.faction)).size,
            rendererSelectionCalls,budget:spriteRuntime.budget,stats:spriteRuntime.stats()};
        })()`));
        assert.equal(result.same,true,'render stress must preserve every logical unit and timer');
        assert.equal(result.selected,300,'stress must cover a large selected army');
        assert.equal(result.factions,3,'stress must cover visible friendly and opposing factions');
        assert.equal(result.rendererSelectionCalls,0,'sprite drawing must reuse indexed selection context');
        assert.ok(result.stats.textures.ready>0,'stress must exercise actual decoded sprite textures');
        assert.ok(result.stats.textures.bytes<=result.budget.textureBytes,'decoded atlases must stay under the configured budget');
        assert.ok(result.stats.ghosts<=result.budget.ghosts,'local death animations must stay bounded');
        assert.ok(result.particles<=result.budget.particles,'particles must follow quality settings');
        assert.ok(result.p95<200,`${width}px/${quality}: render stalled for ${result.p95.toFixed(1)}ms`);
        console.log(`TurnCraft render ${width}px ${quality}: ${result.mean.toFixed(2)}ms mean, ${result.p95.toFixed(2)}ms p95; ${result.stats.textures.ready} cached atlases`);
      }
      assert.deepEqual(errors,[]);
    } finally {await browser.close()}
  }
});

test('every repaired sheet has isolated nonempty frames and transparent gutters at every quality',async()=>{
  const {browser,page,errors}=await setup();
  try{
    const result=await page.evaluate(async()=>{
      const runtime=tcAnimation.runtime;await runtime.loadManifest('/game/turncraft/assets/animation-manifest.json');
      const failures=[],sheets=Object.entries(runtime.manifest.cropRepairs),expectedFrames=sheets.reduce((total,[,repair])=>total+repair.crops.reduce((sum,row)=>sum+row.length,0),0)*3;let checked=0;
      for(const quality of ['low','medium','high']){
        runtime.setQuality(quality);
        for(const [url,repair]of sheets){
          let image=runtime.cache.get(url);for(let retry=0;!image&&retry<200;retry++){await new Promise(resolve=>setTimeout(resolve,10));image=runtime.cache.get(url)}
          if(!image){failures.push(quality+': failed '+url);continue}
          const scale=runtime.cache.sourceScale(url),pitch=repair.frameWidth*scale,height=repair.frameHeight*scale;
          const canvas=document.createElement('canvas');canvas.width=pitch;canvas.height=height;const ctx=canvas.getContext('2d');
          for(let row=0;row<repair.crops.length;row++)for(let col=0;col<repair.crops[row].length;col++){
            ctx.clearRect(0,0,pitch,height);ctx.drawImage(image,col*pitch,row*height,pitch,height,0,0,pitch,height);
            const pixels=ctx.getImageData(0,0,pitch,height).data;let visible=0,edge=0;
            for(let y=0;y<height;y++)for(let x=0;x<pitch;x++){const alpha=pixels[(y*pitch+x)*4+3];if(alpha>16)visible++;if((x<2||x>=pitch-2||(repair.destinationOffsets&&(y<2||y>=height-2)))&&alpha>0)edge++}
            if(!visible||edge)failures.push(`${quality}:${url}:${row}:${col} empty=${!visible} gutterPixels=${edge}`);
            checked++;
          }
          if(Math.max(image.width,image.height)>2048)failures.push('mobile texture limit '+url);
          if(runtime.cache.bytes>runtime.cache.maxBytes)failures.push('texture budget '+quality);
        }
      }
      return {sheets:sheets.length,checked,expectedFrames,failures};
    });
    assert.ok(result.sheets>0);assert.equal(result.checked,result.expectedFrames);assert.deepEqual(result.failures,[]);assert.deepEqual(errors,[]);
  }finally{await browser.close()}
});
