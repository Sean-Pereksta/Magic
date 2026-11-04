import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { createGame } from './fixtures/legacy-game.mjs';
import { declareWar, orderArmy, resolveMovement, parseSave } from '../core.mjs';
import { emptyUnits } from '../economy.mjs';
import { configureOfflineBattles, beginEncounter, saveEncounterCheckpoint } from '../offline-encounters.mjs';
import { beginBattle, createBattle, issueOrder, rangedAccuracy, stepBattle, terrainHeight } from '../battle-sim.mjs';
import { ramApproach, ramHandle } from '../battle-siege.mjs';

const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright');
const root=fileURLToPath(new URL('../../../',import.meta.url));
const server=http.createServer(async(req,res)=>{try{let name=decodeURIComponent(new URL(req.url,'http://localhost').pathname);if(name.endsWith('/'))name+='index.html';const file=path.resolve(root,`.${name}`);if(!file.startsWith(root))throw Error();const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.json':'application/json','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const base=`http://127.0.0.1:${server.address().port}`,key='catnmice.iron-throne.v1';
const commandableTroops = b => b.units.filter(u => u.owner === b.playerOwner && u.id !== b.commanderId);
async function resumeWithPausedClock(page){
  const resume=page.locator('#resume');
  for(let i=0;i<300&&(!await resume.isVisible()||!await resume.isEnabled());i++)await page.clock.runFor(50);
  await resume.click({force:true});
  for(let i=0;i<300&&!await page.locator('[data-battle-action=enter]').isVisible();i++)await page.clock.runFor(50);
}
function fixture(stressCap,defending=false){
  const s=parseSave(JSON.stringify(createGame(321))),a=s.armies[0],d=s.armies[1];
  declareWar(s,a.owner,d.owner);
  for(let q=7;q<15;q++)for(let r=7;r<14;r++)Object.assign(s.tiles[`${q},${r}`],{terrain:'plains',owner:null,building:null,levels:{},walls:0,fortIntegrity:0,road:false,river:false});
  Object.assign(a,{name:'Ember Vanguard',tile:'10,10',units:{...emptyUnits(),menAtArms:14,spearman:8,archer:6,knight:3,ram:1},path:[],target:null,order:'hold',morale:1});
  Object.assign(d,{name:'Wintermere Guard',tile:'11,10',units:{...emptyUnits(),levy:18,crossbow:4},path:[],target:null,order:'hold',morale:1});
  const attacker=defending?d:a,defender=defending?a:d;
  if(defending)a.units.crossbow=3;
  Object.assign(s.tiles[defender.tile],{owner:defender.owner,name:'Stonewatch',building:'fort',levels:{fort:1,wall:1},walls:42,fortIntegrity:55});
  s.armies.push({...structuredClone(a),id:`army-${s.nextId++}`,name:'Northern Spears',tile:'10,11',units:{...emptyUnits(),spearman:6,lightCavalry:2}});
  if(stressCap){a.units.menAtArms=Math.max(110,stressCap/2+10);d.units.levy=Math.max(95,stressCap/2-5);d.units.crossbow=18;}
  configureOfflineBattles(s,stressCap?{battleSize:stressCap}:{});orderArmy(s,attacker.owner,attacker.id,defender.tile,'attack');resolveMovement(s,attacker.owner);
  assert.ok(s.offlineBattle);
  if(stressCap){
    beginEncounter(s);
    const b=createBattle(s.offlineBattle,{maxActive:stressCap});beginBattle(b);
    assert.equal(b.units.filter(u=>u.status==='active').length,stressCap);
    issueOrder(b,'all','hold',{atCurrent:true});
    const foot=b.units.filter(u=>u.type==='menAtArms'&&!u.commander&&u.status==='active');
    const ram=b.units.find(u=>u.type==='ram'),east=b.layout.walls.find(w=>w.id==='wall-east-front'),west=b.layout.ladders[0];
    const place=(u,p)=>Object.assign(u,{x:p.x,z:p.z,y:terrainHeight(b,p.x,p.z)});
    const head=ramApproach(east);place(ram,head);ram.facing=head.facing;
    const carriers=foot.slice(0,6);
    assert.ok(issueOrder(b,carriers.map(u=>u.id),'crew',{ramId:ram.id,wallId:east.id}).accepted);
    for(const u of carriers)place(u,ramHandle(ram,u.order.handle));
    const climbers=foot.slice(6,22);
    climbers.forEach((u,i)=>place(u,{x:west.foot.x,z:west.foot.z-i*1.4}));
    issueOrder(b,climbers.map(u=>u.id),'ladder',{ladderId:west.id});
    for(const u of b.units.filter(u=>u.side===0&&u.type==='archer'&&u.status==='active')){
      place(u,{x:-30+(u.x%5),z:-8+(u.z%4)});issueOrder(b,[u.id],'hold',{atCurrent:true});
    }
    // Resume just before first impact; nearby climbers and wall marksmen remain engaged.
    for(let n=0;n<160;n++){
      stepBattle(b,.05);
      if(ram.action?.kind==='ramStrike'&&!ram.action.impacted&&ram.action.elapsed>=ram.action.windup-.15&&b.units.some(u=>u.action?.kind==='climb')&&b.nextProjectile>0)break;
    }
    assert.ok(ram.action?.kind==='ramStrike'&&b.nextProjectile&&b.units.some(u=>u.action?.kind==='climb'),'a ram strike, archery and ladder assault are underway');
    saveEncounterCheckpoint(s,b);
  }
  return s;
}
async function checkBattlefieldHotkeys(page,custom){
  const saved=()=>page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
  if(page.viewportSize().width>700){
    await page.locator('#battle-canvas').focus();
    const facing=b=>b.units.find(u=>u.id===b.commanderId).facing;
    const waitForPhysics=()=>page.evaluate(()=>new Promise(resolve=>{
      const start=performance.now();let frames=0;
      function frame(now){if(++frames>=3&&now-start>=110)resolve();else requestAnimationFrame(frame);}
      requestAnimationFrame(frame);
    }));
    const {width,height}=page.viewportSize(),x=Math.floor(width/2),y=Math.floor(height/2);
    for(const sensitivity of [1,3]){
      await page.keyboard.press('Escape');
      await page.locator('#battle-pause-sensitivity').selectOption(String(sensitivity));
      await page.locator('[data-battle-action=resume]').click();
      await page.mouse.move(x,y);await waitForPhysics();await page.keyboard.press('4');
      const before=facing(await saved());
      await page.mouse.move(x+10,y);await waitForPhysics();await page.keyboard.press('4');
      const after=facing(await saved());
      assert.ok(Math.abs((after-before)-.06*sensitivity)<.001,`10 mouse pixels at ${sensitivity}× rotated from ${before} to ${after}`);
    }
    await page.keyboard.press('Escape');
    await page.locator('#battle-pause-sensitivity').selectOption('1');
    await page.locator('[data-battle-action=resume]').click();
  }
  const archerUnits=b=>commandableTroops(b).filter(u=>['archer','veteranArcher'].includes(u.type)&&['active','reserve'].includes(u.status));
  const cavalryOrders=b=>commandableTroops(b).filter(u=>u.family==='mounted').map(u=>({id:u.id,order:u.order,formation:u.formation,holdFire:u.holdFire}));
  const original=await saved();assert.equal(await page.locator('#battle-command-panel').isVisible(),false);
  const commandKeys=await page.locator('[data-battle-key] kbd').allTextContents();
  for(const key of ['W','A','S','D','Shift','Space','E'])assert.equal(commandKeys.includes(key),false,`${key} is reserved for personal control`);
  await page.locator('#battle-canvas').focus();await page.keyboard.press('4');await page.keyboard.press('F3');
  const charged=await saved();assert.ok(archerUnits(charged).every(u=>u.order.kind==='charge'));
  assert.deepEqual(cavalryOrders(charged),cavalryOrders(original),'keyboard archer charge leaves cavalry orders untouched');
  assert.equal(charged.phase,'combat');assert.equal(await page.locator('#battle-command-panel').isVisible(),false);
  assert.equal(await page.locator('#battle-mode').textContent(),'LEAD THE BATTLE','direct orders retain the third-person camera');
  assert.match(await page.locator('#battle-keyboard-feedback').textContent(),/Archers Charge/);
  assert.match(await page.locator('#battle-live-selection').textContent(),/Archers/);
  await page.evaluate(()=>new Promise(resolve=>{
    const start=performance.now();let frames=0;
    function next(now){if(++frames>=3&&now-start>=150)resolve();else requestAnimationFrame(next);}
    requestAnimationFrame(next);
  }));
  await page.keyboard.press('F1');
  const held=await saved();assert.ok(held.time>charged.time,'combat time continues while direct orders are issued');
  assert.ok(archerUnits(held).every(u=>u.order.kind==='hold'&&Math.abs(u.order.x-u.x)<1e-9&&Math.abs(u.order.z-u.z)<1e-9),'Hold here anchors every selected troop at its current location');
  assert.deepEqual(cavalryOrders(held),cavalryOrders(original));
  await page.keyboard.press('F5');assert.ok(archerUnits(await saved()).every(u=>!u.holdFire),'F5 resumes fire for a holding group');
  await page.keyboard.press('F5');assert.ok(archerUnits(await saved()).every(u=>u.holdFire),'F5 toggles back to hold fire');
  await page.keyboard.down('F3');const once=await saved();await page.keyboard.down('F3');await page.keyboard.up('F3');
  assert.equal((await saved()).events.filter(e=>e.type==='order').length,once.events.filter(e=>e.type==='order').length,'key repeats do not issue duplicate commands');
  await page.keyboard.press('F1');
  const modified=await page.locator('#battle-canvas').evaluate(canvas=>{const event=new KeyboardEvent('keydown',{key:'F3',code:'F3',ctrlKey:true,bubbles:true,cancelable:true});canvas.dispatchEvent(event);return event.defaultPrevented;});
  assert.equal(modified,false,'browser modifier shortcuts are not intercepted');assert.ok(archerUnits(await saved()).every(u=>u.order.kind==='hold'));
  await page.keyboard.press('Escape');const paused=await saved();await page.keyboard.press('6');await page.keyboard.press('F3');
  assert.deepEqual(await saved(),paused,'paused battles ignore troop and order hotkeys');
  await page.locator('[data-battle-action=resume]').click();await page.keyboard.press('q');
  assert.equal(await page.locator('#battle-mode').textContent(),'LEAD THE BATTLE','Q retains commander view');
  await page.locator('.battle-saved-groups summary').click();await page.locator('#battle-group-name').fill('Typing');
  const editing=await saved();await page.keyboard.press('6');assert.equal(await page.locator('#battle-group-name').inputValue(),'Typing6');
  assert.deepEqual((await saved()).commandSelection,editing.commandSelection,'typing group names does not select cavalry');
  await page.locator('#battle-formation').focus();await page.keyboard.press('4');
  assert.deepEqual((await saved()).commandSelection,editing.commandSelection,'focused selects retain native keyboard input');
  await page.locator('.battle-saved-groups summary').click();await page.locator('#battle-canvas').focus();
  await page.keyboard.press('Tab');
  await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent.includes('REAL TIME'));
  const officerBefore=(await saved()).units.find(u=>u.commander);
  await page.keyboard.down('a');await page.waitForTimeout(120);await page.keyboard.up('a');
  assert.ok(archerUnits(await saved()).every(u=>u.order.kind==='hold'),'A pans left in tactical view instead of issuing an attack order');
  await page.keyboard.press('g');assert.ok(archerUnits(await saved()).every(u=>u.order.kind==='move'),'G sends the selected troops to aimed ground');
  const officerAfter=(await saved()).units.find(u=>u.commander);
  assert.deepEqual([officerAfter.x,officerAfter.z,officerAfter.order],[officerBefore.x,officerBefore.z,officerBefore.order],'tactical camera movement and orders leave the commander in place');
  await page.keyboard.press('8');assert.equal(await page.locator('#battle-saved-group').inputValue(),custom.id,'8 reselects the first saved command group');
  await page.keyboard.press('Tab');await page.keyboard.press('q');assert.equal(await page.locator('#battle-command-panel').isVisible(),false);
}
let browser;
async function checkCommanderMovement(browser){
  for(const commanderType of ['knight','spearman']){
    const initial=fixture();beginEncounter(initial);
    const b=createBattle(initial.offlineBattle,{commanderType});beginBattle(b);
    const officer=b.units.find(u=>u.commander);
    assert.equal(officer.type,commanderType);
    assert.ok(b.units.some(u=>u.armyId===officer.armyId&&u.type==='menAtArms'),'the commander belongs to a mixed campaign army');
    Object.assign(officer,{x:0,z:-37,y:terrainHeight(b,0,-37),facing:0,speed:0});
    issueOrder(b,'all','hold',{atCurrent:true});saveEncounterCheckpoint(initial,b);
    const context=await browser.newContext({viewport:{width:1200,height:800}});
    try{
      await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial,key});
      const page=await context.newPage(),start=new Date('2026-11-04T12:00:00Z');
      await page.clock.install({time:start});await page.clock.pauseAt(new Date(+start+1000));
      await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
      await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
      await page.goto(`${base}/game/iron-throne/index.html`);await resumeWithPausedClock(page);
      await page.locator('[data-battle-action=enter]').click({force:true});
      for(let n=0;n<300&&await page.locator('#battle-mode').textContent()!=='LEAD THE BATTLE';n++)await page.clock.runFor(50);
      assert.equal(await page.locator('#battle-mode').textContent(),'LEAD THE BATTLE');
      const sample=async()=>{
        await page.keyboard.press('4');
        return page.evaluate(key=>{
          const b=JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,u=b.units.find(u=>u.commander);
          return {time:b.time,type:u.type,weapon:u.weapon,mounted:u.mounted,heading:u.facing,x:u.x,z:u.z,speed:u.speed,travel:u.rideDistance||0};
        },key);
      };
      // Position the cursor while paused, so the measured mouse move begins from a known point.
      const x=600,y=400,lookPixels=140,look=lookPixels*.006;
      await page.keyboard.press('Escape');await page.mouse.move(x,y);await page.keyboard.press('Escape');
      await page.clock.runFor(100);
      const rest=await sample();
      await page.mouse.move(x+lookPixels,y);await page.clock.runFor(55);
      const looking=await sample();
      if(commanderType==='spearman'){
        const dt=looking.time-rest.time,turn=looking.heading-rest.heading;
        assert.equal(looking.weapon,'spear');assert.ok(!looking.mounted);
        assert.ok(dt>0&&turn>0&&turn<=2.1*dt+.001&&turn<look,'spear facing advances at its turn rate instead of snapping to the reticle');
        await page.clock.runFor(600);
        assert.ok(Math.abs((await sample()).heading-rest.heading-look)<.004,'the spearman eventually faces the mouse aim');
        continue;
      }
      assert.ok(looking.mounted);assert.equal(looking.heading,rest.heading,'mouse look leaves the resting horse facing forward');
      assert.equal(looking.speed,0);assert.deepEqual([looking.x,looking.z],[rest.x,rest.z]);
      await page.keyboard.down('w');await page.clock.runFor(65);
      const first=await sample(),dt=first.time-looking.time;
      assert.ok(first.speed>0&&first.speed<=2.8*dt+.001&&first.travel>0,'W accelerates the horse from rest');
      assert.ok(first.heading>rest.heading&&first.heading-rest.heading<=1.85*dt+.001&&first.heading-rest.heading<look,'W steers gradually toward the newly turned camera');
      await page.mouse.move(x,y);await page.clock.runFor(1400);
      const straight=await sample();assert.ok(straight.speed>3&&straight.speed>first.speed&&straight.travel>first.travel);
      await page.keyboard.up('w');await page.keyboard.down('d');await page.clock.runFor(250);
      const bent=await sample(),arc=bent.heading-straight.heading;
      const dx=bent.x-straight.x,dz=bent.z-straight.z;
      const forward=dx*Math.sin(straight.heading)+dz*Math.cos(straight.heading);
      const side=dx*Math.cos(straight.heading)-dz*Math.sin(straight.heading);
      assert.ok(arc>0&&arc<=1.85*(bent.time-straight.time)+.001&&forward>.3&&side>0&&side<forward*.5,'D bends the existing forward path into a right-hand arc');
      await page.keyboard.up('d');await page.clock.runFor(200);const coast=await sample();
      assert.ok(coast.speed>0&&coast.speed<bent.speed&&coast.travel>bent.travel,'releasing D slows the horse while it still covers ground');
      await page.clock.runFor(1500);const stopped=await sample();assert.equal(stopped.speed,0);
      await page.clock.runFor(250);assert.equal((await sample()).travel,stopped.travel,'a fully stopped mount stays in place');
      await page.keyboard.down('a');await page.clock.runFor(300);const left=await sample();
      assert.ok(left.heading<stopped.heading&&left.speed>0,'A steers the mount left');
      await page.keyboard.up('a');await page.keyboard.down('s');await page.clock.runFor(200);const back=await sample();
      const towardBack=h=>Math.abs(Math.atan2(Math.sin(Math.PI-h),Math.cos(Math.PI-h)));
      assert.ok(towardBack(back.heading)<towardBack(left.heading)&&towardBack(back.heading)>1&&back.speed<=left.speed,'S brakes into a turnaround without snapping the horse backwards');
      await page.keyboard.up('s');
    }finally{await context.close();}
  }
  console.log('Mounted commander accelerates, steers toward independent mouse look, turns on D and coasts. A foot spearman tracks the same mouse input at a bounded rate.');
}
async function checkPlayerStairs(browser){
  for(const mobile of [false,true]){
    const initial=fixture();beginEncounter(initial);
    const b=createBattle(initial.offlineBattle);beginBattle(b);
    const player=b.units.find(u=>u.commander),enemy=b.units.find(u=>u.side!==player.side),stair=b.layout.stairs[0];
    for(const u of b.units)if(u!==player&&u!==enemy)u.status='escaped';
    Object.assign(player,{x:stair.x,z:stair.z,y:terrainHeight(b,stair.x,stair.z),facing:0});
    Object.assign(enemy,{x:0,z:50,wallId:null,wallPosition:null,explicitOrder:true,holdFire:true,order:{kind:'hold',x:0,z:50}});
    saveEncounterCheckpoint(initial,b);
    const context=await browser.newContext({viewport:mobile?{width:390,height:844}:{width:1280,height:860},hasTouch:mobile});
    try{
      await context.addInitScript(({initial,key})=>localStorage.setItem(key,JSON.stringify(initial)),{initial,key});
      const page=await context.newPage(),start=new Date('2026-11-04T12:00:00Z');
      await page.clock.install({time:start});await page.clock.pauseAt(new Date(+start+1000));
      await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
      await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
      await page.goto(`${base}/game/iron-throne/index.html`);await resumeWithPausedClock(page);
      await page.locator('[data-battle-action=enter]').click({force:true});
      for(let i=0;i<300&&await page.locator('#battle-mode').textContent()!=='LEAD THE BATTLE';i++)await page.clock.runFor(50);
      const button=page.locator('#battle-player-climb');
      const savedPlayer=async()=>{await page.keyboard.press('1');return page.evaluate(key=>{
        const b=JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint;return b.units.find(u=>u.commander);
      },key);};
      assert.match(await button.textContent(),/Ascend Stairs/);
      assert.equal(await button.getAttribute('data-stair'),stair.id);
      await page.keyboard.down('d');await page.clock.runFor(150);await page.keyboard.up('d');
      assert.equal((await savedPlayer()).action,null,'walking beside stairs leaves personal control active');
      if(mobile)await button.tap({force:true});else await page.keyboard.press('e');
      await page.clock.runFor(100);
      assert.equal((await savedPlayer()).action?.kind,'stairs');
      await page.clock.runFor(4300);
      assert.equal((await savedPlayer()).wallId,stair.wallId);
      assert.match(await button.textContent(),/Descend Stairs/);
      await page.clock.runFor(300);
      assert.equal((await savedPlayer()).action,null,'arrival does not immediately reverse the stair traversal');
      if(mobile)await button.tap({force:true});else await page.keyboard.press('e');
      await page.clock.runFor(4300);
      assert.equal((await savedPlayer()).wallId,null);
      console.log(`PASS ${mobile?'phone':'desktop'}: personal stairs require E or the contextual button in both directions`);
    }finally{await context.close();}
  }
}
async function checkPlayerLadder(browser){
  for(const scenario of ['desktop','phone-occupied','phone-destroyed']){
    const mobile=scenario!=='desktop',initial=fixture();beginEncounter(initial);
    const b=createBattle(initial.offlineBattle);beginBattle(b);
    const player=b.units.find(u=>u.commander),other=b.units.find(u=>u.side===player.side&&u!==player&&u.family==='infantry'),enemy=b.units.find(u=>u.side!==player.side);
    const ladder=b.layout.ladders[mobile?1:0],wall=b.layout.walls.find(w=>w.id===ladder.wallId);
    for(const u of b.units)if(u!==player&&u!==other&&u!==enemy)u.status='escaped';
    Object.assign(player,{x:ladder.foot.x+1,z:ladder.foot.z,y:terrainHeight(b,ladder.foot.x+1,ladder.foot.z),facing:0});
    Object.assign(enemy,{x:0,z:50,wallId:null,wallPosition:null,explicitOrder:true,holdFire:true,order:{kind:'hold',x:0,z:50}});
    if(scenario==='phone-occupied'){
      Object.assign(other,ladder.foot);issueOrder(b,[other.id],'ladder',{ladderId:ladder.id});
      stepBattle(b,.05);assert.equal(other.action?.ladderId,ladder.id);
    }else other.status='escaped';
    if(scenario==='phone-destroyed'){wall.hp=0;wall.state='breached';}
    saveEncounterCheckpoint(initial,b);
    const context=await browser.newContext({viewport:mobile?{width:390,height:844}:{width:1280,height:860},hasTouch:mobile});
    try{
      await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial,key});
      const page=await context.newPage();page.setDefaultTimeout(30000);
      const start=new Date('2026-11-04T12:00:00Z');
      await page.clock.install({time:start});await page.clock.pauseAt(new Date(+start+1000));
      await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
      await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
      await page.goto(`${base}/game/iron-throne/index.html`);await resumeWithPausedClock(page);
      await page.locator('[data-battle-action=enter]').click({force:true});
      for(let i=0;i<300&&await page.locator('#battle-mode').textContent()!=='LEAD THE BATTLE';i++)await page.clock.runFor(50);
      assert.equal(await page.locator('#battle-mode').textContent(),'LEAD THE BATTLE');
      const button=page.locator('#battle-player-climb');
      const savedPlayer=async()=>{await page.keyboard.press('4');return page.evaluate(key=>{
        const b=JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint;return b.units.find(u=>u.commander);
      },key);};
      if(scenario==='phone-destroyed'){
        assert.equal(await button.isVisible(),false);await page.keyboard.press('e');await page.clock.runFor(100);
        assert.equal((await savedPlayer()).action,null);continue;
      }
      assert.equal(await button.isVisible(),true);
      assert.ok((await button.textContent()).includes('Climb Ladder'));assert.ok((await button.textContent()).includes(ladder.name));
      assert.equal(await button.getAttribute('data-ladder'),ladder.id);
      await page.keyboard.press('Tab');assert.equal(await button.isVisible(),false,'personal climbing is unavailable from the tactical camera');
      await page.keyboard.press('Tab');
      const exposed=el=>{const r=el.getBoundingClientRect();return el.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));};
      if(mobile){
        assert.equal(await button.isDisabled(),true);assert.match(await button.textContent(),/Foot occupied/);
        await page.keyboard.press('h');assert.equal(await button.isVisible(),true);
        await page.clock.runFor(1600);assert.equal(await button.isEnabled(),true);
        assert.ok(await button.evaluate(exposed)&&await page.locator('#battle-touch-block').evaluate(exposed)&&await page.locator('#battle-stick').evaluate(exposed));
      }else assert.ok(await button.evaluate(exposed));
      if(process.env.IRON_BATTLE_SCREENSHOTS){
        await mkdir(process.env.IRON_BATTLE_SCREENSHOTS,{recursive:true});
        await page.screenshot({path:path.join(process.env.IRON_BATTLE_SCREENSHOTS,`iron-thrones-player-climb-${mobile?'phone':'desktop'}.png`)});
      }
      if(mobile)await button.tap({force:true});else await button.click({force:true});
      await page.clock.runFor(220);
      const climbing=await savedPlayer();assert.equal(climbing.action?.ladderId,ladder.id);assert.equal(climbing.action.personal,true);assert.ok(climbing.y>0);
      assert.equal(await button.isVisible(),false);
      await page.clock.runFor(6100);
      const landed=await savedPlayer();assert.equal(landed.wallId,wall.id);assert.ok(Math.abs(landed.x-ladder.landing.x)>1);
    }finally{await context.close();}
  }
  console.log('Commander Climb Ladder: desktop click and minimized-phone tap landed on their named parapets; occupied and destroyed ladder entries were respected.');
}
async function checkMobileGuard(browser){
  const initial=fixture();beginEncounter(initial);
  const b=createBattle(initial.offlineBattle);beginBattle(b);
  const guard=b.units.find(u=>u.commander),enemy=b.units.find(u=>u.side!==b.playerSide&&u.family==='infantry');
  // The closing duel isolates the player's timing from other arrows and cavalry.
  for(const u of b.units)if(u!==guard&&u!==enemy){u.status='escaped';u.action=null;}
  Object.assign(guard,{status:'active',hp:guard.maxHp,routing:false,x:0,z:-18,y:terrainHeight(b,0,-18),facing:Math.PI});
  Object.assign(enemy,{x:0,z:-19.8,y:terrainHeight(b,0,-19.8),facing:0,wallId:null,wallPosition:null,fortPost:null,
    explicitOrder:true,order:{kind:'hold',x:0,z:-19.8},
    action:{kind:'attack',elapsed:0,windup:1.4,duration:1.85,recovery:.45,targetId:guard.id,targetX:guard.x,targetZ:guard.z,
      targetY:guard.y,facing:0,ranged:false,impacted:false}});
  saveEncounterCheckpoint(initial,b);
  const context=await browser.newContext({viewport:{width:390,height:844},hasTouch:true});
  try{
    await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial,key});
    const page=await context.newPage();page.setDefaultTimeout(30000);
    const clockStart=new Date('2026-11-04T12:00:00Z');
    await page.clock.install({time:clockStart});await page.clock.pauseAt(new Date(+clockStart+1000));
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);await resumeWithPausedClock(page);
    await page.locator('[data-battle-action=enter]').click({force:true});
    for(let i=0;i<300&&await page.locator('#battle-mode').textContent()!=='LEAD THE BATTLE';i++)await page.clock.runFor(50);
    assert.equal(await page.locator('#battle-mode').textContent(),'LEAD THE BATTLE');
    const block=page.locator('#battle-touch-block'),attack=page.locator('[data-battle-touch=attack]'),stick=page.locator('#battle-stick');
    async function controls(){
      const r=await block.boundingBox(),a=await attack.boundingBox(),s=await stick.boundingBox();
      assert.ok(r.width>=44&&r.height>=44&&r.x>195&&r.x>a.x&&r.x>s.x+s.width,'right-side shield is touch-sized beside Attack and clear of movement');
      const exposed=el=>{const r=el.getBoundingClientRect();return el.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));};
      assert.ok(await block.evaluate(exposed)&&await attack.evaluate(exposed)&&await stick.evaluate(exposed));
      return [{id:1,x:r.x+r.width/2,y:r.y+r.height/2},{id:2,x:s.x+s.width/2,y:s.y+s.height/2}];
    }
    await controls();await page.keyboard.press('h');const [shield,thumb]=await controls();
    const cdp=await context.newCDPSession(page);
    const saved=async()=>{
      await page.keyboard.press('4');
      return page.evaluate(({key,id})=>{const b=JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint;
        const guard=b.units.find(u=>u.id===b.commanderId),enemy=b.units.find(u=>u.id===id);
        return {time:b.time,remaining:enemy.action?enemy.action.windup-enemy.action.elapsed:null,x:guard.x,z:guard.z,blocking:guard.blocking,parry:guard.lastParryAt,status:guard.status,guardHeld:guard.guardHeld};
      },{key,id:enemy.id});
    };
    let state=await saved();
    for(let i=0;i<40&&state.remaining>.22;i++){await page.clock.runFor(50);state=await saved();}
    assert.ok(state.remaining>0&&state.remaining<=.22,'the real touch begins just before melee impact');
    await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[shield]});
    await page.clock.runFor(65);
    const raised=await saved();assert.ok(raised.blocking,`shield must be raised before impact: ${JSON.stringify(raised)}`);
    await page.clock.runFor(350);
    assert.match(await page.locator('#battle-keyboard-feedback').textContent(),/attacker stunned/);
    assert.equal(await block.getAttribute('aria-pressed'),'true');
    const held=await saved();assert.equal(held.blocking,true);assert.ok(held.parry>0);
    await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[shield,thumb]});
    await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[shield,{...thumb,x:thumb.x+28}]});
    await page.clock.runFor(450);
    const moved=await saved();assert.equal(moved.blocking,true);assert.ok(Math.hypot(moved.x-held.x,moved.z-held.z)>.02,'the commander moves while maintaining guard');
    await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await cdp.detach();
    assert.equal(await block.getAttribute('aria-pressed'),'false');await attack.tap();
    await page.clock.runFor(150);assert.equal((await saved()).blocking,false);
    console.log('390×844 phone: a real shield press parried an incoming swing; simultaneous stick movement preserved guard in the minimized HUD.');
  }finally{await context.close();}
}
async function checkBattleHUD(browser){
  for(const viewport of [{width:920,height:830},{width:390,height:844}]){
    const mobile=viewport.width<700, context=await browser.newContext({viewport,hasTouch:mobile});
    try{
      const page=await context.newPage(),errors=[];
      page.setDefaultTimeout(30000);
      page.on('pageerror',e=>errors.push(e.message));
      await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial:fixture(),key});
      await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
      await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
      const toggle=page.locator('#battle-hud-toggle'),panel=page.locator('#battle-command-panel'),strip=page.locator('.battle-command-strip');
      const saved=()=>page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
      const commander=s=>s.units.find(u=>u.id===s.commanderId);
      const assertMinimized=async()=>{
        assert.match(await toggle.textContent(),/Show UI/i);
        for(const selector of ['.battle-hud','#battle-command-panel','.battle-command-strip','.battle-player','#battle-live-selection']){
          assert.equal(await page.locator(selector).isVisible(),false,`${selector} leaves the field clear`);
        }
      };
      await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});
      await page.locator('[data-battle-action=enter]').click();await toggle.waitFor();
      await page.waitForFunction(key=>JSON.parse(localStorage.getItem(key)).offlineBattle?.checkpoint?.phase==='deployment',key);
      if(!await panel.isVisible())await page.keyboard.press('q');
      await panel.waitFor();
      await (mobile?toggle.tap():toggle.click());await assertMinimized();
      await page.keyboard.press('h');assert.match(await toggle.textContent(),/Hide UI/i);
      assert.equal(await panel.isVisible(),true,'H remembers that the full orders list was open');
      assert.equal(await strip.isVisible(),false,'the expanded list has no duplicate edge strip');
      await page.keyboard.press('h');await assertMinimized();
      await page.locator('[data-battle-action=begin]').click();
      await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent==='LEAD THE BATTLE');
      await assertMinimized();await page.keyboard.press('4');await page.keyboard.press('F2');
      const recalled=await saved(),archers=commandableTroops(recalled).filter(u=>u.type==='archer');
      assert.ok(archers.length&&archers.every(u=>u.order.kind==='follow'),'hidden HUD still accepts troop and order hotkeys');
      if(mobile){
        const stick=page.locator('#battle-stick'),attack=page.locator('[data-battle-touch=attack]');
        const unobstructed=el=>{const r=el.getBoundingClientRect();return el.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));};
        assert.ok(await stick.evaluate(unobstructed)&&await attack.evaluate(unobstructed),'both touch controls are exposed');
        const r=await stick.boundingBox(),x=r.x+r.width/2,y=r.y+r.height/2,cdp=await context.newCDPSession(page);
        await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x,y}]});
        await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x,y:y-30}]});
        await page.waitForTimeout(450);
        await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await cdp.detach();
        await page.keyboard.press('F1');const moved=commander(await saved()),was=commander(recalled);
        assert.ok(Math.hypot(moved.x-was.x,moved.z-was.z)>.02,'touch stick moves commander with HUD hidden');
        await attack.tap();
      }
      await page.keyboard.press('q');await panel.waitFor();
      assert.equal(await strip.isVisible(),false);
      assert.equal(await page.locator('#battle-mode').textContent(),'LEAD THE BATTLE','Q opens orders in the current camera');
      await page.keyboard.press('q');await panel.waitFor({state:'hidden'});
      if(/Hide UI/i.test(await toggle.textContent()))await toggle.click();
      await page.keyboard.press('Tab');
      await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent.includes('TACTICAL'));
      await assertMinimized();
      await page.keyboard.press('Escape');await page.locator('#battle-pause').waitFor();
      await page.locator('[data-battle-action=exit]').click();await page.locator('#battle-encounter').waitFor();
      await page.reload();await page.locator('#custom-battle').click();await page.getByRole('button',{name:'Fight Now'}).click();
      await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent==='LEAD THE BATTLE');
      await assertMinimized();await page.keyboard.press('h');
      assert.match(await toggle.textContent(),/Hide UI/i);
      await page.keyboard.press('Escape');await page.locator('[data-battle-action=exit]').click();
      await page.locator('#custom-setup').waitFor();

      const recovery=fixture();beginEncounter(recovery);const b=createBattle(recovery.offlineBattle);beginBattle(b);
      Object.assign(commander(b),{status:'dead',hp:0,action:null});saveEncounterCheckpoint(recovery,b);
      await page.evaluate(({key,recovery})=>localStorage.setItem(key,JSON.stringify(recovery)),{key,recovery});
      await page.reload();await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});await page.locator('[data-battle-action=enter]').click();
      await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent.includes('TACTICAL'));
      assert.match(await toggle.textContent(),/Hide UI/i);
      assert.equal(await panel.isVisible(),false,'incapacitation changes camera without forcing the full panel open');
      assert.deepEqual(errors,[]);
      console.log(`Battle HUD visibility, hotkeys, preferences and commander recovery passed at ${viewport.width}×${viewport.height}`);
    }finally{await context.close();}
  }
}
async function checkSiegePicking(browser){
  const width=1280,height=800;
  for(const defending of [false,true]){
    const initial=fixture(undefined,defending);beginEncounter(initial);
    const b=createBattle(initial.offlineBattle);
    if(defending){
      const [captain,guard]=b.units.filter(u=>u.type==='archer'&&u.owner===b.playerOwner);
      b.units.find(u=>u.commander).commander=false;captain.commander=true;b.commanderId=captain.id;
      issueOrder(b,[guard.id],'walls',{wallId:'wall-east-front'});
    }
    beginBattle(b);
    assert.equal(b.playerSide,Number(defending));
    const commander=b.units.find(u=>u.commander),z=defending?35:-8,yaw=defending?Math.PI:0;
    Object.assign(commander,{x:0,y:0,z,facing:yaw,wallId:null,wallPosition:null});
    const ram=b.units.find(u=>u.type==='ram');
    if(!defending)Object.assign(ram,{x:12,y:terrainHeight(b,12,-13),z:-13});
    saveEncounterCheckpoint(initial,b);
    const context=await browser.newContext({viewport:{width,height},hasTouch:true});
    try{
      await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial,key});
      const page=await context.newPage();
      page.on('pageerror',error=>console.error(error.message));
      await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
      await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
      await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});
      await page.locator('[data-battle-action=enter]').click();
      await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent==='LEAD THE BATTLE');
      if(defending)assert.ok((await page.locator('#battle-player-status').textContent()).includes(`Accuracy ${rangedAccuracy(b,commander)}/100`),'an archer commander sees his own current rating');
      await page.locator('#battle-canvas').focus();await page.keyboard.press('Tab');
      await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent.includes('TACTICAL'));
      const saved=()=>page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
      // Project the physical objects into the ordinary tactical camera. The UI alone performs all picks.
      function screenPoint({x,y,z:worldZ}){
        const forward=x*Math.sin(yaw)+(worldZ-z)*Math.cos(yaw),side=-x*Math.cos(yaw)+(worldZ-z)*Math.sin(yaw);
        const focal=height/(2*Math.tan(Math.PI/6)),scale=focal/(90+forward*Math.cos(.8)-y*Math.sin(.8));
        return [width/2+side*scale,height/2-(forward*Math.sin(.8)+y*Math.cos(.8))*scale];
      }
      const wall=b.layout.walls.find(w=>w.id==='wall-east-front');
      await page.mouse.move(width/2,height/2);
      if(defending){
        await page.keyboard.press('5');
        assert.match(await page.locator('#battle-live-selection').textContent(),/Crossbows .*Accuracy \d+(?:–\d+)?\/100/);
        await page.keyboard.press('4');
        const grades=commandableTroops(b).filter(u=>u.type==='archer').map(u=>rangedAccuracy(b,u));
        assert.ok((await page.locator('#battle-live-selection').textContent()).includes(`Accuracy ${Math.min(...grades)}–${Math.max(...grades)}/100 · +10 on parapets`),'selected archer ratings include their occupied wall posts');
        await page.keyboard.press('v');
        await page.mouse.click(...screenPoint({x:wall.x,y:wall.height,z:wall.z}));
        const guards=commandableTroops(await saved()).filter(u=>u.type==='archer'&&u.status==='active');
        assert.ok(guards.length&&guards.every(u=>u.order.kind==='walls'&&u.order.wallId===wall.id),'V assigns the selected archers to the clicked parapet');
      }else{
        const ladder=b.layout.ladders[0];
        await page.keyboard.press('3');await page.keyboard.press('l');
        await page.touchscreen.tap(...screenPoint({x:ladder.x,y:ladder.height/2,z:(ladder.foot.z+ladder.landing.z)/2}));
        const spears=commandableTroops(await saved()).filter(u=>u.type==='spearman'&&u.status==='active');
        assert.ok(spears.length&&spears.every(u=>u.order.kind==='ladder'&&u.order.ladderId===ladder.id),'a battlefield tap sends the company to the west ladder');
        await page.mouse.move(width/2,height/2);await page.keyboard.press('2');await page.keyboard.press('b');
        await page.mouse.click(...screenPoint({x:ram.x,y:ram.y+.8,z:ram.z}));
        assert.match(await page.locator('#battle-order-status').textContent(),/gate or exterior wall/);
        await page.mouse.click(...screenPoint({x:wall.x,y:wall.height/2,z:wall.z}));
        const snapshot=await saved(),beam=snapshot.units.find(u=>u.id===ram.id);
        assert.equal(beam.ramTargetId,wall.id);
        assert.ok(snapshot.units.filter(u=>u.order.kind==='crew'&&u.order.ramId===beam.id).length>=6,'B assigns selected infantry to the clicked timber and east wall');
        await page.waitForFunction(()=>document.querySelector('#battle-siege-status')?.textContent.includes('/6 pushers · 4 needed to roll'));
      }
      assert.ok(!await page.locator('#battlefield').evaluate(f=>f.classList.contains('is-targeting')));
    }finally{await context.close();}
  }
  console.log('Live siege battlefield clicks and taps selected the west ladder, friendly ram, east breach and defensive parapet.');
}
async function stressCommands(browser){
  for(const cap of [150,200,300,400]){
    const context=await browser.newContext({viewport:{width:950,height:700},hasTouch:true});
    await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial:fixture(cap),key});
    const page=await context.newPage(),errors=[];
    page.on('pageerror',error=>errors.push(error.message));
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});
    await page.locator('[data-battle-action=enter]').click();
    await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent==='LEAD THE BATTLE');
    const saved=()=>page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    await page.keyboard.down('Shift');await page.keyboard.press('4');await page.keyboard.press('F2');await page.keyboard.up('Shift');
    let snapshot=await saved();
    const selected=b=>commandableTroops(b).filter(u=>u.type==='archer'&&!['dead','escaped'].includes(u.status));
    assert.ok(selected(snapshot).length&&selected(snapshot).every(u=>u.order.kind==='follow'),'Shift+F2 recalls the selected archers');
    await page.locator('[data-battle-key=hold]').click();
    assert.ok(selected(await saved()).every(u=>u.order.kind==='hold'),'mouse button holds their ground');
    assert.equal(await page.locator('#battle-canvas').evaluate(c=>document.activeElement===c),true);
    await page.locator('[data-battle-key=advance]').tap();
    assert.ok(selected(await saved()).every(u=>u.order.kind==='advance'),'touch button advances their company');
    const stick=await page.locator('#battle-stick').boundingBox(), strip=await page.locator('.battle-command-strip').boundingBox();
    assert.ok(strip.y+strip.height<stick.y,'command buttons remain clear of the stick');
    await page.keyboard.press('l');assert.ok(await page.locator('#battlefield').evaluate(f=>f.classList.contains('is-targeting')));
    await page.keyboard.press('Escape');assert.equal(await page.locator('#battle-pause').isVisible(),false);
    const t=snapshot.time;
    await page.waitForTimeout(600);await page.keyboard.press('F1');
    snapshot=await saved();
    assert.ok(snapshot.time>t&&snapshot.events.some(e=>e.type==='ramImpact')&&snapshot.nextProjectile>0);
    assert.deepEqual(errors,[]);
    console.log(`Siege keyboard, mouse and touch passed at ${cap} active troops`);
    await context.close();
  }
}
try{
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  if(process.argv.includes('--stress-only'))await stressCommands(browser);
  else if(process.argv.includes('--targets-only'))await checkSiegePicking(browser);
  else if(process.argv.includes('--hud-only'))await checkBattleHUD(browser);
  else if(process.argv.includes('--block-only'))await checkMobileGuard(browser);
  else if(process.argv.includes('--player-stairs-only'))await checkPlayerStairs(browser);
  else if(process.argv.includes('--player-climb-only')){await checkPlayerLadder(browser);await checkPlayerStairs(browser);}
  else if(process.argv.includes('--mounted-controls-only'))await checkCommanderMovement(browser);
  else {
  for(const viewport of [{width:1365,height:900},{width:390,height:844}]){
    const context=await browser.newContext({viewport,hasTouch:viewport.width<700});
    const initial=fixture();await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial,key});
    const page=await context.newPage(),errors=[];page.setDefaultTimeout(30000);page.setDefaultNavigationTimeout(60000);
    page.on('pageerror',error=>errors.push(error.message));
    if(process.env.IRON_BATTLE_DEBUG){page.on('response',r=>{if(r.status()>=400&&r.url().startsWith(base))console.error(r.status(),r.url());});page.on('pageerror',error=>console.error(error.message));}
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});
    await page.locator('#battle-encounter').waitFor({state:'visible'});
    assert.match(await page.locator('#battle-title').textContent(),/Stonewatch/);
    assert.equal(await page.locator('#battle-encounter-size').inputValue(),'300','new campaigns default to larger armies');
    await page.locator('#battle-encounter-size').selectOption('200');
    assert.equal(await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattles.battleSize,key),200);
    await page.reload();await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});
    assert.equal(await page.locator('#battle-encounter-size').inputValue(),'200','battle size preference survives reloading before deployment');
    await page.locator('[data-battle-action=close]').first().click();
    assert.equal(await page.locator('#battle-pending').isVisible(),true);
    assert.equal(await page.locator('#end-turn').isDisabled(),true);
    const frozen=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key);
    assert.deepEqual(frozen.armies,initial.armies,'closing an encounter does not change troop counts');
    await page.locator('#battle-pending').click();
    await page.locator('[data-battle-action=enter]').click();
    await page.locator('#battle-canvas').waitFor({state:'visible'});
    await page.locator('#battle-deployment').waitFor({state:'visible'});
    await page.waitForFunction(()=>JSON.parse(localStorage.getItem('catnmice.iron-throne.v1')).offlineBattle?.checkpoint?.phase==='deployment');
    assert.equal(await page.evaluate(()=>document.querySelector('#battle-canvas').width>0),true);
    const beforeOrders=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    assert.equal(beforeOrders.maxActive,200,'new battles use the selected field limit');
    await page.locator('#battle-army').selectOption(initial.armies[0].id);
    await page.locator('[data-battle-group=archers]').click();
    assert.equal(await page.locator('[data-battle-group=archers]').getAttribute('aria-pressed'),'true');
    const armyArchers = b => commandableTroops(b).filter(u=>u.armyId===initial.armies[0].id&&u.type==='archer');
    assert.equal(await page.locator('[data-battle-group-count=archers]').textContent(),String(armyArchers(beforeOrders).length));
    const card=await page.locator('[data-battle-group=archers]').boundingBox();
    const touch=await page.evaluate(()=>matchMedia('(pointer: coarse)').matches);
    assert.ok(touch?Math.min(card.width,card.height)>=44:card.height>=34,'troop cards fit the primary input device');
    await page.locator('#battle-formation').selectOption('skirmish');
    assert.match(await page.locator('#battle-order-status').textContent(),/Skirmish applied/);
    let commands=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    const mounted=b=>commandableTroops(b).filter(u=>u.armyId===initial.armies[0].id&&u.family==='mounted');
    assert.deepEqual(mounted(commands),mounted(beforeOrders),'reforming archers leaves cavalry orders and positions untouched');
    assert.ok(armyArchers(commands).every(u=>u.formation==='skirmish'),'formation selection takes effect without another order');
    assert.deepEqual(commands.units.find(u=>u.commander),beforeOrders.units.find(u=>u.commander),'troop formation changes leave the commander untouched');
    await page.locator('.battle-saved-groups summary').click();
    await page.locator('#battle-group-name').fill('Left flank archers');
    await page.locator('[data-battle-action=save-group]').click();
    commands=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    const custom=commands.commandGroups[0];assert.equal(custom.name,'Left flank archers');assert.equal(custom.unitIds.length,armyArchers(commands).length);
    assert.equal(custom.unitIds.includes(commands.commanderId),false,'saved command groups exclude the commander');
    assert.ok(custom.unitIds.every(id=>commands.units.some(u=>u.id===id&&u.armyId===initial.armies[0].id&&u.type==='archer')),'saved groups preserve exact source army and troop membership');
    await page.locator('[data-battle-group=cavalry]').click();
    await page.locator('#battle-formation').selectOption('wedge');
    commands=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    assert.ok(mounted(commands).every(u=>u.formation==='wedge'));
    await page.locator('#battle-saved-group').selectOption(custom.id);
    assert.match(await page.locator('#battle-selection-status').textContent(),/Left flank archers/);
    await page.locator('[data-battle-order=holdFire]').click();
    const grouped=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    assert.ok(grouped.units.filter(u=>custom.unitIds.includes(u.id)).every(u=>u.holdFire));
    assert.deepEqual(mounted(grouped),mounted(commands),'a saved archer group cannot change cavalry orders');
    await page.locator('.battle-saved-groups summary').click();
    if(process.env.IRON_BATTLE_SCREENSHOTS){await mkdir(process.env.IRON_BATTLE_SCREENSHOTS,{recursive:true});await page.screenshot({path:path.join(process.env.IRON_BATTLE_SCREENSHOTS,`iron-thrones-deployment-${viewport.width}.png`)});}
    if(viewport.width<700)await page.locator('[data-battle-action=commands]').first().click();
    await page.locator('[data-battle-action=begin]').click();
    await page.waitForTimeout(500);
    await checkBattlefieldHotkeys(page,custom);
    await page.locator('[data-battle-action=pause]').click();
    await page.locator('[data-battle-action=exit]').click();
    await page.locator('#battle-encounter').waitFor({state:'visible'});
    const checkpoint=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    assert.equal(checkpoint.phase,'combat');assert.ok(checkpoint.time>0);
    assert.equal(await page.locator('#battle-encounter-size').isDisabled(),true);
    assert.equal(await page.locator('#battle-encounter-size').inputValue(),'200');
    assert.match(await page.locator('#battle-size-note').textContent(),/saved battle keeps its 200-troop limit/);
    await page.locator('[data-battle-action=close]').first().click();
    await page.locator('#menu-button').click();
    await page.locator('#offline-battle-size').selectOption('100');
    await page.locator('[data-close=menu]').click();
    const lowerPreference=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key);
    assert.equal(lowerPreference.offlineBattles.battleSize,100);
    assert.deepEqual(lowerPreference.offlineBattle.checkpoint,checkpoint,'changing the next-battle size does not resize or restart a saved battle');
    await page.reload();await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});
    assert.match(await page.locator('[data-battle-action=enter]').textContent(),/Resume/);
    assert.equal(await page.locator('#battle-encounter-size').inputValue(),'200','resuming retains the checkpoint size despite a lower preference');
    assert.equal(await page.locator('#battle-encounter-size').isDisabled(),true);
    await page.locator('[data-battle-action=enter]').click();
    await page.locator('#battle-canvas').waitFor({state:'visible'});
    await page.waitForTimeout(250);
    assert.equal(await page.locator('#battle-saved-group').inputValue(),custom.id,'custom group selection restores after reloading');
    assert.match(await page.locator('#battle-selection-status').textContent(),/Left flank archers/);
    await page.locator('[data-battle-action=pause]').click();
    const resumed=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    assert.ok(resumed.time>=checkpoint.time);
    assert.equal(resumed.maxActive,200);
    assert.deepEqual(resumed.commandGroups,checkpoint.commandGroups,'save recovery preserves custom group roster membership');
    await page.locator('[data-battle-action=finish]').click({timeout:60000});
    await page.locator('#battle-result-banner').waitFor({state:'visible',timeout:120000});
    const applied=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key);
    assert.equal(applied.offlineBattle,null);
    parseSave(JSON.stringify(applied));
    const report=applied.lastOfflineBattleReport;
    for(const p of report.participants){const row=report.result.armies.find(a=>a.armyId===p.armyId);for(const [type,n]of Object.entries(p.units))assert.equal(n,(row.units[type]||0)+(row.losses[type]||0));}
    await page.reload();await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});
    const again=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key);assert.deepEqual(again.armies,applied.armies,'reloading the report does not reapply casualties');
    await page.locator('[data-battle-action=report]').click();
    await page.getByRole('button',{name:'Close report'}).click();
    assert.equal(await page.locator('#battle-pending').isVisible(),false);assert.equal(await page.locator('#end-turn').isDisabled(),false);
    assert.deepEqual(errors,[]);console.log(`Offline battlefield UI passed ${viewport.width}x${viewport.height}`);await context.close();
  }
  const context=await browser.newContext({viewport:{width:1280,height:800}}),initial=fixture();
  await context.addInitScript(({initial,key})=>localStorage.setItem(key,JSON.stringify(initial)),{initial,key});
  const page=await context.newPage();page.setDefaultTimeout(30000);
  await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
  await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
  await page.route('**/assets/battle/v1/pack.json',route=>route.fulfill({status:503,body:''}));
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();await page.locator('#welcome').waitFor({state:'hidden'});
  await page.evaluate(()=>{window.originalStorageSet=Storage.prototype.setItem;Storage.prototype.setItem=function(){throw new DOMException('Quota exceeded','QuotaExceededError');};});
  await page.locator('[data-battle-action=enter]').click();
  await page.waitForFunction(()=>document.querySelector('#battle-notice').textContent.includes('could not be saved'));
  assert.equal(await page.locator('#battlefield').isVisible(),false);
  await page.evaluate(()=>{Storage.prototype.setItem=window.originalStorageSet;});
  await page.locator('[data-battle-action=enter]').click();
  await page.waitForFunction(()=>document.querySelector('#battle-notice').textContent.includes('could not open'));
  const failed=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key);
  assert.equal(failed.offlineBattle.status,'active');assert.equal(failed.offlineBattle.checkpoint,null);
  assert.deepEqual(failed.armies.map(a=>a.units),initial.armies.map(a=>a.units),'missing models do not damage armies');
  await page.locator('[data-battle-action=auto]').click();
  await page.locator('#battle-result-banner').waitFor({state:'visible'});
  console.log('Offline battlefield storage/model failure recovery passed');await context.close();
  await checkSiegePicking(browser);
  await checkBattleHUD(browser);
  await checkMobileGuard(browser);
  await checkPlayerLadder(browser);
  await checkPlayerStairs(browser);
  await checkCommanderMovement(browser);
  }
}finally{await browser?.close();await new Promise(r=>server.close(r));}
