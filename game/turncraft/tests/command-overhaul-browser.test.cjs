const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');

const repo=path.resolve(__dirname,'../../..');
const expose=`window.squareClock=100000;nowMs=()=>window.squareClock;
window.tcSquare={run:source=>eval(source),prepare(faction='frontier'){
 resize();terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
 entities=[];resourceNodes=[];nextEntityId=1;nextNodeId=1;players=[makePlayer(0,'Test',faction,false),makePlayer(1,'Opponent','frontier',false)];
 gameStarted=true;gameOver=false;paused=false;modeState=null;lobbyId='';PLAYER=0;areaEffects=[];pendingImpacts=[];temporaryObstacles=[];tileVisibleToFaction=()=>true;overlay.style.display='none';
 window.squareTarget=addEntity('unit',FACTIONS[faction].worker,0,8,8);window.squareGroup=[addEntity('unit','rifleman',0,3,3),addEntity('unit','rifleman',0,4,3)];
 window.squareCommands=[];sendCommand=c=>{window.squareCommands.push(c);applyCommand({...c,slot:PLAYER})};
 selectEntities(squareGroup);lastCanvasTap={at:0,entityId:null};lastStackTap={key:'',idx:0,at:0};cam.zoom=.65;centerOn(10,9);draw();refreshUI(true);
},reselect(){selectEntities(squareGroup);lastCanvasTap={at:0,entityId:null};lastStackTap={key:'',idx:0,at:0};window.squareClock+=1000;}};`;
const source=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8')
 .replace(/^import .*;$/gm,'')
 .replace('\nboot();','\n'+expose+'\nwindow.TurncraftStartup?.complete();')
 .replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
// Selection uses battlefield coordinates, so image pixels are irrelevant to this offline fixture.
const placeholder=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/6kQAAAAASUVORK5CYII=','base64');

async function setup(viewport,hasTouch){
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
 const page=await browser.newPage({viewport,hasTouch}),errors=[];page.on('pageerror',error=>errors.push(error.message));
 // Hold thresholds must measure simulated input time, not browser protocol latency under load.
 await page.clock.install({time:new Date('2026-01-01T00:00:00Z')});
 await page.route('**/*',route=>{const request=route.request(),url=new URL(request.url());
  if(request.resourceType()==='image')return route.fulfill({contentType:'image/png',body:placeholder});
  if(url.pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});
  const file=path.resolve(repo,'.'+decodeURIComponent(url.pathname));
  if(file.startsWith(repo+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file});
  return route.fulfill({status:404,body:''});
 });
 await page.goto('http://turncraft.test/game/turncraft.html');
 await page.clock.pauseAt(new Date('2026-01-01T01:00:00Z'));
 return {browser,page,errors};
}
async function point(page,x,y){return page.evaluate(([x,y])=>tcSquare.run('worldToScreen('+x+','+y+')'),[x,y])}
async function tap(page,p,touch){if(touch)await page.touchscreen.tap(p.x,p.y);else await page.mouse.click(p.x,p.y)}
async function state(page){return page.evaluate(()=>({selected:tcSquare.run('selectedIds'),target:squareTarget.id,group:squareGroup.map(e=>e.id),commands:squareCommands,destinations:squareGroup.map(e=>e.moveDestination)}))}

test('selected Dragoon commands retain DOM identity, focus and tooltips through three minutes of resource and timer changes',async()=>{
 const {browser,page,errors}=await setup({width:1440,height:900},false);
 try{
  await page.evaluate(()=>{tcSquare.prepare('ascendant');tcSquare.run(`window.dragoon=addEntity('unit','dragoon',0,9,8);selectEntity(dragoon);window.savedCommand=document.querySelector('#commandButtons button');window.savedAbility=document.querySelector('#abilityButtons [data-ability]');window.savedCooldown=document.querySelector('#cooldownGrid b');savedCommand.focus();`)});
  const result=await page.evaluate(()=>tcSquare.run(`(()=>{const signature=computeUiSignature(),title=savedAbility.title;for(let i=0;i<1200;i++){squareClock+=150;players[0].minerals+=3;players[0].gas+=1;dragoon.hp=Math.max(1,dragoon.maxHp-(i%10));dragoon.attackReadyAt=squareClock+800;dragoon.abilityReadyAt=i%12?squareClock+2000:0;dragoon.targetId=i%2?999:null;dragoon.moveReadyAt=squareClock+300;refreshUI(true)}return {sameCommand:savedCommand===document.querySelector('#commandButtons button'),sameAbility:savedAbility===document.querySelector('#abilityButtons [data-ability]'),sameCooldown:savedCooldown===document.querySelector('#cooldownGrid b'),focus:document.activeElement===savedCommand,signature:signature===computeUiSignature(),title:savedAbility.title===title,portrait:!!document.querySelector('#selectionPortrait canvas'),groups:document.querySelectorAll('#controlGroups button').length,width:savedCommand.getBoundingClientRect().width}})()`));
  assert.deepEqual(result,{sameCommand:true,sameAbility:true,sameCooldown:true,focus:true,signature:true,title:true,portrait:true,groups:9,width:44});assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('production cards and queue progress stay mounted while costs, power and completion change',async()=>{
 const {browser,page,errors}=await setup({width:1440,height:900},false);
 try{
  const result=await page.evaluate(()=>{tcSquare.prepare();return tcSquare.run(`(()=>{window.building=addEntity('structure','barracks',0,9,8);selectEntity(building);const card=actionList.querySelector('[data-action-type="rifleman"]');card.focus();players[0].minerals=0;refreshUI();const disabled=card.disabled;players[0].minerals=5000;refreshUI();const enabled=!card.disabled;building.queue=[{kind:'train',type:'rifleman',startAt:squareClock,finishAt:squareClock+15000}];refreshUI();const queue=productionQueue.querySelector('.queueCircle');for(let i=0;i<50;i++){squareClock+=200;refreshUI(true)}return {disabled,enabled,same:card===actionList.querySelector('[data-action-type="rifleman"]'),focus:card===document.activeElement,queue:queue===productionQueue.querySelector('.queueCircle'),updated:queue.querySelector('b').textContent!=='15.0s'}})()`)});
  assert.deepEqual(result,{disabled:true,enabled:true,same:true,focus:true,queue:true,updated:true});assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('technology unlocks and research levels update existing cards, while deployed fabrication changes the available layout',async()=>{
 const {browser,page,errors}=await setup({width:1440,height:900},false);
 try{
  const result=await page.evaluate(()=>{tcSquare.prepare();return tcSquare.run(`(()=>{
   players[0].minerals=5000;players[0].gas=5000;const worker=addEntity('unit','engineer',0,5,5);selectEntity(worker);const factory=actionList.querySelector('[data-action-type="factory"]'),before=factory.disabled;addEntity('structure','barracks',0,7,5);refreshUI();const unlocked=!factory.disabled&&factory===actionList.querySelector('[data-action-type="factory"]');
   const id=Object.keys(RESEARCH_DEFS).find(id=>(RESEARCH_DEFS[id].shared||RESEARCH_DEFS[id].faction==='frontier')&&RESEARCH_DEFS[id].maxLevel>1),type=RESEARCH_DEFS[id].structures[0],lab=addEntity('structure',type,0,10,10);selectEntity(lab);const research=actionList.querySelector('[data-research="'+id+'"]'),oldCost=research.querySelector('.actionCost').textContent;players[0].research[id]=1;refreshUI();const researchUpdated=research===actionList.querySelector('[data-research="'+id+'"]')&&research.querySelector('.actionCost').textContent!==oldCost&&research.querySelector('.researchBadge').textContent.startsWith('Level 1/');
   const fabricator=addEntity('unit','forwardFabricator',0,9,7);selectEntity(fabricator);const packed=actionList.querySelectorAll('[data-action-mode="fabricate"]').length;fabricator.fabricatorDeployed=true;refreshUI();const deployed=actionList.querySelectorAll('[data-action-mode="fabricate"]').length,card=actionList.querySelector('[data-action-mode="fabricate"]');players[0].minerals=0;refreshUI();const guarded=card.disabled;fabricator.fabricatorDeployed=false;refreshUI();return {before,unlocked,researchUpdated,packed,deployed,guarded,repacked:actionList.querySelectorAll('[data-action-mode="fabricate"]').length};
  })()`)});
  assert.deepEqual(result,{before:true,unlocked:true,researchUpdated:true,packed:0,deployed:2,guarded:true,repacked:0});assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('number keys 1–9 assign only on hold, tap without panning, double-tap centers, and blur cancels',async()=>{
 const {browser,page,errors}=await setup({width:1440,height:900},false);
 try{
  await page.evaluate(()=>{tcSquare.prepare();tcSquare.run(`window.groupCenters=[];centerOn=(x,y)=>groupCenters.push({x,y});`)});
  for(let i=1;i<=9;i++){
   await page.keyboard.down(String(i));await page.clock.runFor(499);assert.equal(await page.evaluate(i=>tcSquare.run(`controlGroupSlots[${i-1}].length`),i),0);await page.clock.runFor(1);
   await page.keyboard.down(String(i));await page.keyboard.up(String(i));assert.equal(await page.evaluate(i=>tcSquare.run(`controlGroupSlots[${i-1}].length`),i),2);
  }
  await page.evaluate(()=>tcSquare.run(`selectEntity(squareTarget);squareClock+=1000`));await page.keyboard.press('9');assert.deepEqual((await state(page)).selected,(await state(page)).group);assert.equal(await page.evaluate(()=>tcSquare.run('groupCenters.length')),0);
  await page.evaluate(()=>tcSquare.run('squareClock+=100'));await page.keyboard.press('9');assert.equal(await page.evaluate(()=>tcSquare.run('groupCenters.length')),1);
  await page.evaluate(()=>tcSquare.run(`selectEntity(squareTarget)`));await page.keyboard.down('4');await page.evaluate(()=>window.dispatchEvent(new Event('blur')));await page.clock.runFor(600);await page.keyboard.up('4');assert.equal(await page.evaluate(()=>tcSquare.run('controlGroupSlots[3].length')),2);
  await page.evaluate(()=>tcSquare.run(`controlGroupSlots[3]=[];selectEntity(squareTarget)`));await page.keyboard.down('4');await page.clock.runFor(250);await page.locator('#pauseBtn').click();await page.clock.runFor(600);await page.keyboard.up('4');assert.equal(await page.evaluate(()=>tcSquare.run('controlGroupSlots[3].length')),0);
  assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('ten selected troops switch to every friendly structure footprint square without sending orders',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  const result=await page.evaluate(()=>{tcSquare.prepare();return tcSquare.run(`(()=>{const army=Array.from({length:10},(_,i)=>addEntity('unit','rifleman',0,2+i,3)),building=addEntity('structure','commandHub',0,10,9);building.footprintWidth=2;building.footprintHeight=2;const selected=[];for(const [x,y] of [[10.01,9.01],[11.99,9.99],[10.01,10.99],[11.99,10.99]]){selectEntities(army);lastCanvasTap={at:0,entityId:null};squareClock+=1000;const p=worldToScreen(x,y);handlePolishTap(p.x,p.y);selected.push(selectedId)}return {selected,building:building.id,commands:squareCommands.length,moved:army.some(e=>e.moveDestination)}})()`)});
  assert.deepEqual(result.selected,Array(4).fill(result.building));assert.equal(result.commands,0);assert.equal(result.moved,false);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('visible enemy square corners attack, Shift-click adds troops, and empty selection never moves',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  const result=await page.evaluate(()=>{tcSquare.prepare();return tcSquare.run(`(()=>{const enemy=addEntity('unit','zealot',1,12,9);const p=worldToScreen(12.99,9.99);handlePolishTap(p.x,p.y);const command=squareCommands.at(-1);selectEntities([]);const ground=worldToScreen(15.5,15.5);handlePolishTap(ground.x,ground.y);return {type:command.type,target:command.targetId,expected:enemy.id,count:squareCommands.length}})()`)});
  assert.equal(result.type,'groupAttack');assert.equal(result.target,result.expected);assert.equal(result.count,1);
  await page.evaluate(()=>tcSquare.reselect());const p=await point(page,8.99,8.99);await page.keyboard.down('Shift');await page.mouse.click(p.x,p.y);await page.keyboard.up('Shift');const selected=await state(page);assert.deepEqual(selected.selected,[...selected.group,selected.target]);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('music-independent control group state restores locally without entering shared snapshots',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  const result=await page.evaluate(()=>{tcSquare.prepare();return tcSquare.run(`(()=>{matchStartedAt=125000;restoredControlGroupKey='';restoreLocalControlGroups();assignControlGroup(8);const expected=controlGroupSlots[8].slice(),snapshot=serializeState();controlGroupSlots[8]=[];restoredControlGroupKey='';restoreLocalControlGroups();const restored=controlGroupSlots[8].slice();entities.find(e=>e.id===expected[0]).alive=false;restoredControlGroupKey='';restoreLocalControlGroups();return {expected,restored,valid:controlGroupSlots[8],snapshotKeys:Object.keys(snapshot).filter(key=>/group/i.test(key)&&!/ability/i.test(key))}})()`)});
  assert.deepEqual(result.restored,result.expected);assert.deepEqual(result.valid,[result.expected[1]]);assert.deepEqual(result.snapshotKeys,[]);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('campaign control groups survive an actual timer-rebased checkpoint restore under their stable run ID',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  const result=await page.evaluate(async()=>{const {rebaseTimers}=await import('/game/turncraft/shattered-front/engine.mjs');tcSquare.prepare();window.rebaseGroupsCheckpoint=rebaseTimers;return tcSquare.run(`(()=>{window.TurncraftExpansion={getState:()=>({resultId:'campaign-groups-checkpoint',mode:'campaign'})};matchStartedAt=1700000000000;restoredControlGroupKey='';restoreLocalControlGroups();assignControlGroup(8);const expected=controlGroupSlots[8].slice(),key=controlGroupStorageKey(),snapshot=serializeState();controlGroupSlots[8]=[];restoredControlGroupKey='';applyRemoteState(rebaseGroupsCheckpoint(JSON.parse(JSON.stringify(snapshot)),60000));return {expected,restored:controlGroupSlots[8],sameKey:key===controlGroupStorageKey(),rebased:matchStartedAt===1700000060000}})()`)});
  assert.deepEqual(result.restored,result.expected);assert.equal(result.sameKey,true);assert.equal(result.rebased,true);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('a live unpaused multiplayer menu blocks gameplay shortcuts, cancels quick menu holds and permits form typing',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  const abilityKey=await page.evaluate(()=>{tcSquare.prepare('ascendant');return tcSquare.run(`(()=>{lobbyId='live-input-guard';const dragoon=addEntity('unit','dragoon',0,9,8);selectEntity(squareTarget);assignControlGroup(3);selectEntity(dragoon);window.menuSelection=selectedIds.slice();window.menuAssignedGroup=controlGroupSlots[3].slice();window.gameplayMenu=document.getElementById('sfMenu');if(!gameplayMenu){gameplayMenu=document.createElement('section');gameplayMenu.id='sfMenu';document.body.appendChild(gameplayMenu)}gameplayMenu.hidden=true;gameplayMenu.innerHTML='<input id="liveMenuInput" aria-label="Menu notes">';return document.querySelector('[data-ability="phaseStep"]').dataset.hotkey})()`)});
  await page.keyboard.down('4');await page.clock.runFor(200);
  // The complete open/close occurs before another HUD refresh or hold timeout.
  await page.evaluate(()=>{gameplayMenu.hidden=false;window.dispatchEvent(new CustomEvent('sf:menu'));gameplayMenu.hidden=true});
  await page.clock.runFor(600);await page.keyboard.up('4');
  let result=await page.evaluate(()=>tcSquare.run(`({selected:selectedIds,expected:menuSelection,assigned:controlGroupSlots[3],group:menuAssignedGroup,pending:controlGroupPresses.size})`));
  assert.deepEqual(result.selected,result.expected);assert.deepEqual(result.assigned,result.group);assert.equal(result.pending,0);
  // A camera key held before opening cannot keep panning behind the menu.
  await page.keyboard.down('w');await page.evaluate(()=>{gameplayMenu.hidden=false;window.dispatchEvent(new CustomEvent('sf:menu'));window.menuKeyPrevention=[];window.addEventListener('keydown',event=>menuKeyPrevention.push(event.defaultPrevented))});
  for(const key of ['4','m','a','x','h','Shift+s','f','w','ArrowRight','Space',abilityKey])await page.keyboard.press(key);
  await page.keyboard.up('w');await page.clock.runFor(600);
  result=await page.evaluate(()=>tcSquare.run(`({selected:selectedIds,expected:menuSelection,assigned:controlGroupSlots[3],group:menuAssignedGroup,commands:squareCommands.length,mode:interactionMode,paused,keys:cameraKeys.size,pending:controlGroupPresses.size,prevented:menuKeyPrevention.some(Boolean)})`));
  assert.deepEqual(result.selected,result.expected);assert.deepEqual(result.assigned,result.group);assert.equal(result.commands,0);assert.equal(result.mode,null);assert.equal(result.paused,false);assert.equal(result.keys,0);assert.equal(result.pending,0);assert.equal(result.prevented,false);
  await page.locator('#liveMenuInput').focus();await page.keyboard.type('army 149 qwe');assert.equal(await page.locator('#liveMenuInput').inputValue(),'army 149 qwe');assert.equal(await page.evaluate(()=>squareCommands.length),0);
  await page.evaluate(()=>{gameplayMenu.hidden=true;document.activeElement.blur()});await page.keyboard.press(abilityKey);assert.equal(await page.evaluate(()=>tcSquare.run('interactionMode')),'ability');assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('command-base cooldowns and bunker or dropship occupancy update the same visible controls',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  for(const [faction,research,key] of [['frontier','orbitalRecon','scan'],['ascendant','recallMatrix','recall']]){
   const result=await page.evaluate(([faction,research,key])=>{tcSquare.prepare(faction);return tcSquare.run(`(()=>{players[0].research[${JSON.stringify(research)}]=1;const base=addEntity('structure',FACTIONS[players[0].factionId].base,0,10,10);base.commandCooldowns={${key}:squareClock+2000};selectEntity(base);const button=abilityButtons.querySelector('[data-command-ability="${key}"]'),initial=button.disabled;squareClock+=2200;refreshUI(true);return {initial,ready:!button.disabled,same:button===abilityButtons.querySelector('[data-command-ability="${key}"]')}})()`)},[faction,research,key]);
   assert.deepEqual(result,{initial:true,ready:true,same:true});
  }
  const result=await page.evaluate(()=>{tcSquare.prepare();return tcSquare.run(`(()=>{
   const bunker=addEntity('structure','bunker',0,10,10),unit=addEntity('unit','rifleman',0,10,11);selectEntity(bunker);const button=commandButtons.querySelector('[data-command-label="Unload All"]'),initial=button.disabled;garrisonUnit(unit,bunker);refreshUI(true);const loaded=!button.disabled&&button.querySelector('small').textContent.startsWith('1/');button.click();const unloaded=button.disabled&&button.querySelector('small').textContent.startsWith('0/'),same=button===commandButtons.querySelector('[data-command-label="Unload All"]');
   const ship=addEntity('unit','dropship',0,12,10),passenger=addEntity('unit','rifleman',0,12,11);selectEntity(ship);const unload=commandButtons.querySelector('[data-command-label="Unload Passengers"]'),empty=unload.disabled;ship.passengers=[passenger.id];passenger.transportedIn=ship.id;passenger.garrisonedIn=ship.id;refreshUI();const aboard=!unload.disabled&&unload.querySelector('small').textContent.startsWith('1/');unload.click();return {initial,loaded,unloaded,same,empty,aboard,landed:unload.disabled&&unload.querySelector('small').textContent.startsWith('0/'),shipSame:unload===commandButtons.querySelector('[data-command-label="Unload Passengers"]')};
  })()`)});
  assert.deepEqual(result,{initial:true,loaded:true,unloaded:true,same:true,empty:true,aboard:true,landed:true,shipSame:true});assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('Shift-click and Shift-box add friendly troops without replacing, removing, duplicating or ordering them',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  const points=await page.evaluate(()=>{tcSquare.prepare();return tcSquare.run(`(()=>{window.additiveArmy=[...squareGroup,addEntity('unit','rifleman',0,10,8),addEntity('unit','rifleman',0,11,9)];window.additiveBuilding=addEntity('structure','barracks',0,12,9);selectEntities(additiveArmy.slice(0,2));assignControlGroup(3);return {start:worldToScreen(9.9,7.9),end:worldToScreen(11.9,9.9),unit:worldToScreen(10.5,8.5),building:worldToScreen(12.99,9.99)}})()`)});
  async function box(shift,releaseEarly=false){await page.mouse.move(points.start.x,points.start.y);if(shift)await page.keyboard.down('Shift');await page.mouse.down();if(releaseEarly)await page.keyboard.up('Shift');if(!shift)await page.clock.runFor(380);await page.mouse.move(points.end.x,points.end.y,{steps:4});await page.mouse.up();if(shift&&!releaseEarly)await page.keyboard.up('Shift')}
  await box(true,true);let result=await page.evaluate(()=>tcSquare.run(`({selected:selectedIds,expected:additiveArmy.map(e=>e.id),commands:squareCommands.length,numbered:controlGroupSlots[3]})`));
  assert.deepEqual(result.selected,result.expected);assert.equal(result.commands,0);assert.deepEqual(result.numbered,result.expected.slice(0,2),'selection changes do not reassign a numbered group');
  await box(true);assert.deepEqual(await page.evaluate(()=>tcSquare.run('selectedIds')),result.expected,'boxing already selected troops keeps each exactly once');
  await page.keyboard.down('Shift');await page.mouse.click(points.unit.x,points.unit.y);await page.keyboard.up('Shift');assert.deepEqual(await page.evaluate(()=>tcSquare.run('selectedIds')),result.expected,'Shift-click never removes a selected troop');
  await box(false);assert.deepEqual(await page.evaluate(()=>tcSquare.run('selectedIds')),result.expected.slice(2),'ordinary held box still replaces selection');
  await page.mouse.click(points.building.x,points.building.y);assert.deepEqual(await page.evaluate(()=>tcSquare.run('selectedIds')),[await page.evaluate(()=>additiveBuilding.id)],'friendly structure squares still select the structure');assert.equal(await page.evaluate(()=>squareCommands.length),0);
  const moving=await page.evaluate(()=>tcSquare.run(`selectEntities(additiveArmy.slice(0,2));worldToScreen(squareTarget.x+.99,squareTarget.y+.99)`));await page.keyboard.down('Shift');await page.mouse.move(moving.x,moving.y);await page.mouse.down();await page.evaluate(()=>{squareTarget.x=squareTarget.px=16;squareTarget.y=squareTarget.py=12});await page.keyboard.up('Shift');await page.mouse.up();assert.deepEqual(await page.evaluate(()=>tcSquare.run('selectedIds')),[...result.expected.slice(0,2),await page.evaluate(()=>squareTarget.id)],'a moving troop pressed with Shift is added even if Shift is released before the troop moves away');assert.equal(await page.evaluate(()=>squareCommands.length),0);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});
