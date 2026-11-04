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
import { beginBattle, createBattle, issueOrder, stepBattle, terrainHeight } from '../battle-sim.mjs';
import { ramApproach, ramHandle } from '../battle-siege.mjs';

const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright');
const root=fileURLToPath(new URL('../../../',import.meta.url));
const server=http.createServer(async(req,res)=>{try{let name=decodeURIComponent(new URL(req.url,'http://localhost').pathname);if(name.endsWith('/'))name+='index.html';const file=path.resolve(root,`.${name}`);if(!file.startsWith(root))throw Error();const body=await readFile(file);res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.json':'application/json','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const base=`http://127.0.0.1:${server.address().port}`,key='catnmice.iron-throne.v1';
function fixture(stressCap,defending=false){
  const s=parseSave(JSON.stringify(createGame(321))),a=s.armies[0],d=s.armies[1];
  declareWar(s,a.owner,d.owner);
  for(let q=7;q<15;q++)for(let r=7;r<14;r++)Object.assign(s.tiles[`${q},${r}`],{terrain:'plains',owner:null,building:null,levels:{},walls:0,fortIntegrity:0,road:false,river:false});
  Object.assign(a,{name:'Ember Vanguard',tile:'10,10',units:{...emptyUnits(),menAtArms:14,spearman:8,archer:6,knight:3,ram:1},path:[],target:null,order:'hold',morale:1});
  Object.assign(d,{name:'Wintermere Guard',tile:'11,10',units:{...emptyUnits(),levy:18,crossbow:4},path:[],target:null,order:'hold',morale:1});
  const attacker=defending?d:a,defender=defending?a:d;
  Object.assign(s.tiles[defender.tile],{owner:defender.owner,name:'Stonewatch',building:'fort',levels:{fort:1,wall:1},walls:42,fortIntegrity:55});
  s.armies.push({...structuredClone(a),id:`army-${s.nextId++}`,name:'Northern Spears',tile:'10,11',units:{...emptyUnits(),spearman:6,lightCavalry:2}});
  if(stressCap){a.units.menAtArms=110;d.units.levy=95;d.units.crossbow=18;}
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
  const archerUnits=b=>b.units.filter(u=>u.owner===b.playerOwner&&['archer','veteranArcher'].includes(u.type)&&['active','reserve'].includes(u.status));
  const cavalryOrders=b=>b.units.filter(u=>u.owner===b.playerOwner&&u.family==='mounted').map(u=>({id:u.id,order:u.order,formation:u.formation,holdFire:u.holdFire}));
  const original=await saved();assert.equal(await page.locator('#battle-command-panel').isVisible(),false);
  await page.locator('#battle-canvas').focus();await page.keyboard.press('4');await page.keyboard.press('F3');
  const charged=await saved();assert.ok(archerUnits(charged).every(u=>u.order.kind==='charge'));
  assert.deepEqual(cavalryOrders(charged),cavalryOrders(original),'keyboard archer charge leaves cavalry orders untouched');
  assert.equal(charged.phase,'combat');assert.equal(await page.locator('#battle-command-panel').isVisible(),false);
  assert.equal(await page.locator('#battle-mode').textContent(),'LEAD THE BATTLE','direct orders retain the third-person camera');
  assert.match(await page.locator('#battle-keyboard-feedback').textContent(),/Archers Charge/);
  assert.match(await page.locator('#battle-live-selection').textContent(),/Archers/);
  await page.waitForTimeout(150);await page.keyboard.press('F1');
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
  await page.keyboard.down('a');await page.waitForTimeout(120);await page.keyboard.up('a');
  assert.ok(archerUnits(await saved()).every(u=>u.order.kind==='hold'),'A pans left in tactical view instead of issuing an attack order');
  await page.keyboard.press('g');assert.ok(archerUnits(await saved()).every(u=>u.order.kind==='move'),'G sends the selected troops to aimed ground');
  await page.keyboard.press('8');assert.equal(await page.locator('#battle-saved-group').inputValue(),custom.id,'8 reselects the first saved command group');
  await page.keyboard.press('Tab');await page.keyboard.press('q');assert.equal(await page.locator('#battle-command-panel').isVisible(),false);
}
let browser;
async function checkSiegePicking(browser){
  const width=1280,height=800;
  for(const defending of [false,true]){
    const initial=fixture(undefined,defending);beginEncounter(initial);
    const b=createBattle(initial.offlineBattle);beginBattle(b);
    assert.equal(b.playerSide,Number(defending));
    const commander=b.units.find(u=>u.commander),z=defending?35:-8,yaw=defending?Math.PI:0;
    Object.assign(commander,{x:0,y:0,z,facing:yaw,wallId:null,wallPosition:null});
    issueOrder(b,[commander.id],'hold',{atCurrent:true});
    const ram=b.units.find(u=>u.type==='ram');
    if(!defending)Object.assign(ram,{x:12,y:terrainHeight(b,12,-13),z:-13});
    saveEncounterCheckpoint(initial,b);
    const context=await browser.newContext({viewport:{width,height},hasTouch:true});
    try{
      await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial,key});
      const page=await context.newPage();
      await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
      await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
      await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
      await page.locator('[data-battle-action=enter]').click();
      await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent==='LEAD THE BATTLE');
      await page.keyboard.press('Tab');await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent.includes('TACTICAL'));
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
        await page.keyboard.press('4');await page.keyboard.press('v');
        await page.mouse.click(...screenPoint({x:wall.x,y:wall.height,z:wall.z}));
        const guards=(await saved()).units.filter(u=>u.owner===b.playerOwner&&u.type==='archer'&&u.status==='active');
        assert.ok(guards.length&&guards.every(u=>u.order.kind==='walls'&&u.order.wallId===wall.id),'V assigns the selected archers to the clicked parapet');
      }else{
        const ladder=b.layout.ladders[0];
        await page.keyboard.press('3');await page.keyboard.press('l');
        await page.touchscreen.tap(...screenPoint({x:ladder.x,y:ladder.height/2,z:(ladder.foot.z+ladder.landing.z)/2}));
        const spears=(await saved()).units.filter(u=>u.owner===b.playerOwner&&u.type==='spearman'&&u.status==='active');
        assert.ok(spears.length&&spears.every(u=>u.order.kind==='ladder'&&u.order.ladderId===ladder.id),'a battlefield tap sends the company to the west ladder');
        await page.mouse.move(width/2,height/2);await page.keyboard.press('2');await page.keyboard.press('b');
        await page.mouse.click(...screenPoint({x:ram.x,y:ram.y+.8,z:ram.z}));
        assert.match(await page.locator('#battle-order-status').textContent(),/gate or exterior wall/);
        await page.mouse.click(...screenPoint({x:wall.x,y:wall.height/2,z:wall.z}));
        const snapshot=await saved(),beam=snapshot.units.find(u=>u.id===ram.id);
        assert.equal(beam.ramTargetId,wall.id);
        assert.ok(snapshot.units.filter(u=>u.order.kind==='crew'&&u.order.ramId===beam.id).length>=6,'B assigns selected infantry to the clicked timber and east wall');
        await page.waitForFunction(()=>document.querySelector('#battle-siege-status')?.textContent.includes('/6 carriers · 4 needed to lift'));
      }
      assert.ok(!await page.locator('#battlefield').evaluate(f=>f.classList.contains('is-targeting')));
    }finally{await context.close();}
  }
  console.log('Live siege battlefield clicks and taps selected the west ladder, friendly ram, east breach and defensive parapet.');
}
async function stressCommands(browser){
  for(const cap of [150,200]){
    const context=await browser.newContext({viewport:{width:950,height:700},hasTouch:true});
    await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial:fixture(cap),key});
    const page=await context.newPage(),errors=[];
    page.on('pageerror',error=>errors.push(error.message));
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
    await page.locator('[data-battle-action=enter]').click();
    await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent==='LEAD THE BATTLE');
    const saved=()=>page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    await page.keyboard.down('Shift');await page.keyboard.press('4');await page.keyboard.press('F2');await page.keyboard.up('Shift');
    let snapshot=await saved();
    const selected=b=>b.units.filter(u=>u.owner===b.playerOwner&&u.type==='archer'&&!['dead','escaped'].includes(u.status));
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
  else {
  for(const viewport of [{width:1365,height:900},{width:390,height:844}]){
    const context=await browser.newContext({viewport,hasTouch:viewport.width<700});
    const initial=fixture();await context.addInitScript(({initial,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(initial));},{initial,key});
    const page=await context.newPage(),errors=[];page.setDefaultTimeout(30000);page.setDefaultNavigationTimeout(60000);
    page.on('pageerror',error=>errors.push(error.message));
    if(process.env.IRON_BATTLE_DEBUG){page.on('response',r=>{if(r.status()>=400&&r.url().startsWith(base))console.error(r.status(),r.url());});page.on('pageerror',error=>console.error(error.message));}
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
    await page.locator('#battle-encounter').waitFor({state:'visible'});
    assert.match(await page.locator('#battle-title').textContent(),/Stonewatch/);
    assert.equal(await page.locator('#battle-encounter-size').inputValue(),'150','new battles default to the balanced size');
    await page.locator('#battle-encounter-size').selectOption('200');
    assert.equal(await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattles.battleSize,key),200);
    await page.reload();await page.locator('#resume').click();
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
    assert.equal(await page.locator('[data-battle-group-count=archers]').textContent(),'6');
    assert.ok((await page.locator('[data-battle-group=archers]').boundingBox()).height>=44,'troop cards remain touch-sized');
    await page.locator('#battle-formation').selectOption('skirmish');
    assert.match(await page.locator('#battle-order-status').textContent(),/Skirmish applied/);
    let commands=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    const mounted=b=>b.units.filter(u=>u.armyId===initial.armies[0].id&&u.family==='mounted');
    assert.deepEqual(mounted(commands),mounted(beforeOrders),'reforming archers leaves cavalry orders and positions untouched');
    assert.ok(commands.units.filter(u=>u.armyId===initial.armies[0].id&&u.type==='archer').every(u=>u.formation==='skirmish'),'formation selection takes effect without another order');
    await page.locator('.battle-saved-groups summary').click();
    await page.locator('#battle-group-name').fill('Left flank archers');
    await page.locator('[data-battle-action=save-group]').click();
    commands=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)).offlineBattle.checkpoint,key);
    const custom=commands.commandGroups[0];assert.equal(custom.name,'Left flank archers');assert.equal(custom.unitIds.length,6);
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
    await page.reload();await page.locator('#resume').click();
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
    await page.reload();await page.locator('#resume').click();
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
  await page.goto(`${base}/game/iron-throne/index.html`);await page.locator('#resume').click();
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
  }
}finally{await browser?.close();await new Promise(r=>server.close(r));}
