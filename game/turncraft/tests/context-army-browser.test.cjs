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

test('every faction builds and trains with visible unmodified keys that share the button action',async()=>{
 const {browser,page,errors}=await setup({width:1440,height:900},false);
 try{
  for(const faction of ['frontier','ascendant','brood']){
   const construction=await page.evaluate(f=>{tcSquare.prepare(f);return tcSquare.run(`(()=>{players[0].minerals=5000;players[0].gas=5000;for(const [i,type]of BUILD_ORDER[players[0].factionId].entries())addEntity('structure',type,0,20+i%8,20+Math.floor(i/8));selectEntity(squareTarget);return [...actionList.querySelectorAll('button')].map(b=>({type:b.dataset.actionType,key:b.dataset.hotkey,badge:b.querySelector('kbd').textContent,aria:b.getAttribute('aria-keyshortcuts')}))})()`)},faction);
   assert.ok(construction.length>=9,faction);assert.equal(new Set(construction.map(b=>b.key)).size,construction.length);for(const b of construction){assert.equal(b.key.length,1);assert.equal(b.badge,b.key);assert.equal(b.aria,b.key);assert.ok(!/[wasdaxbmghf0-9\[]/i.test(b.key),b.key)}
   let collision=await page.evaluate(()=>[...document.querySelectorAll('#commandButtons [data-hotkey],#actionList [data-hotkey],#abilityButtons [data-hotkey]')].map(b=>b.dataset.hotkey.toLowerCase()));assert.equal(new Set(collision).size,collision.length,`${faction}: no command/action/ability collision`);
   const building=construction.find(b=>b.type===({frontier:'supplyDepot',ascendant:'pylon',brood:'broodColony'}[faction]));await page.keyboard.press(building.key.toLowerCase());let result=await page.evaluate(()=>tcSquare.run('({mode:interactionMode,type:interactionPayload,commands:squareCommands.length})'));assert.deepEqual(result,{mode:'build',type:building.type,commands:0});await page.keyboard.press('Escape');await page.locator(`#actionList [data-action-type="${building.type}"]`).click();assert.deepEqual(await page.evaluate(()=>tcSquare.run('({mode:interactionMode,type:interactionPayload,commands:squareCommands.length})')),result);await page.keyboard.press('Escape');
   const training=await page.evaluate(f=>tcSquare.run(`(()=>{const producer=${JSON.stringify(f)}==='brood'?addLarva(0,8,10,entities.find(e=>isBase(e)&&ownEntity(e)).id):entities.find(e=>e.type===(${JSON.stringify(f)}==='frontier'?'barracks':'gateway'));selectEntity(producer);return [...actionList.querySelectorAll('button')].map(b=>({type:b.dataset.actionType,mode:b.dataset.actionMode,key:b.dataset.hotkey,badge:b.querySelector('kbd').textContent,aria:b.getAttribute('aria-keyshortcuts')}))})()`),faction);
   assert.ok(training.length>=2);assert.equal(new Set(training.map(b=>b.key)).size,training.length);for(const b of training){assert.equal(b.key.length,1);assert.equal(b.badge,b.key);assert.equal(b.aria,b.key)}
   collision=await page.evaluate(()=>[...document.querySelectorAll('#commandButtons [data-hotkey],#actionList [data-hotkey],#abilityButtons [data-hotkey]')].map(b=>b.dataset.hotkey.toLowerCase()));assert.equal(new Set(collision).size,collision.length);
   const troop=training.find(b=>b.type===({frontier:'rifleman',ascendant:'zealot',brood:'zergling'}[faction]));await page.keyboard.press(troop.key.toLowerCase());const command=await page.evaluate(()=>squareCommands.at(-1));assert.equal(command.type,faction==='brood'?'morph':'train');assert.equal(command.unitType,troop.type);
   if(faction==='brood')await page.evaluate(()=>tcSquare.run("selectEntity(addLarva(0,8,10,entities.find(e=>isBase(e)&&ownEntity(e)).id))"));await page.locator(`#actionList [data-action-type="${troop.type}"]`).click();const clicked=await page.evaluate(()=>squareCommands.at(-1));assert.equal(clicked.type,command.type);assert.equal(clicked.unitType,command.unitType);
  }
  assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('0 and its chip select the entire own army including offscreen troops and passengers without issuing orders',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  await page.evaluate(()=>{tcSquare.prepare();tcSquare.run(`const far=addEntity('unit','rifleman',0,BOARD_W-1,BOARD_H-1),air=addEntity('unit','phoenix',0,12,10),support=addEntity('unit','medic',0,11,10),ship=addEntity('unit','dropship',0,10,10),garrison=addEntity('unit','rifleman',0,7,7),passenger=addEntity('unit','medic',0,10,10);garrison.garrisonedIn=999;passenger.garrisonedIn=ship.id;passenger.transportedIn=ship.id;ship.passengers=[passenger.id];const dead=addEntity('unit','rifleman',0,6,6);dead.alive=false;addEntity('unit','rifleman',1,8,6);addEntity('structure','barracks',0,6,8);addLarva(0,8,9,null);const cocoon=addEntity('unit','zergling',0,9,9);cocoon.kind='cocoon';entities=[garrison,passenger,...entities.filter(e=>e!==garrison&&e!==passenger)];window.allExpected=entities.filter(e=>e.alive!==false&&e.kind==='unit'&&ownEntity(e)&&!isWorker(e)).map(e=>e.id);window.deployedExpected=allExpected.filter(id=>{const e=entityById(id);return !e.garrisonedIn&&!e.transportedIn});window.passengerIds=[garrison.id,passenger.id];selectEntity(squareTarget);assignControlGroup(0);`)});
  await page.keyboard.press('0');let result=await page.evaluate(()=>tcSquare.run(`({selected:selectedIds,expected:allExpected,leadGarrisoned:!!(selected().garrisonedIn||selected().transportedIn),count:document.querySelector('#allArmyControl .groupCount').textContent,commands:squareCommands.length,groups:controlGroupSlots.length,one:controlGroupSlots[0]})`));assert.deepEqual(new Set(result.selected),new Set(result.expected));assert.equal(result.leadGarrisoned,false);assert.equal(Number(result.count),result.expected.length);assert.equal(result.commands,0);assert.equal(result.groups,9);assert.deepEqual(result.one,[await page.evaluate(()=>squareTarget.id)]);
  await page.evaluate(()=>tcSquare.run('selectEntity(squareTarget)'));await page.locator('#allArmyControl').click();assert.deepEqual(new Set(await page.evaluate(()=>tcSquare.run('selectedIds'))),new Set(result.expected));assert.equal(await page.evaluate(()=>squareCommands.length),0);
  await page.keyboard.press('m');const destination=await page.evaluate(()=>tcSquare.run('worldToScreen(16.5,15.5)'));await page.mouse.click(destination.x,destination.y);result=await page.evaluate(()=>tcSquare.run(`({command:squareCommands.at(-1),deployed:deployedExpected,passengers:passengerIds.map(id=>({id,garrisoned:!!entityById(id).garrisonedIn,transported:!!entityById(id).transportedIn})),casters:abilityCasters('burstFire').map(e=>e.id)})`));assert.equal(result.command.type,'groupMove');assert.deepEqual(new Set(result.command.entityIds),new Set(result.deployed));assert.ok(result.passengers.every(e=>e.garrisoned));assert.ok(result.passengers.every(e=>!result.casters.includes(e.id)));
  await page.evaluate(()=>tcSquare.run(`entities.forEach(e=>{if(e.kind==='unit'&&!e.garrisonedIn&&!e.transportedIn&&!isWorker(e))e.alive=false});selectEntity(squareTarget)`));await page.keyboard.press('0');result=await page.evaluate(()=>tcSquare.run(`({selected:selectedIds,expected:passengerIds,move:commandButtons.querySelector('[data-command-label="Move Group"]').disabled,attack:commandButtons.querySelector('[data-command-label="Attack Group"]').disabled,commands:squareCommands.length})`));assert.deepEqual(new Set(result.selected),new Set(result.expected));assert.equal(result.move,true);assert.equal(result.attack,true);assert.equal(result.commands,1);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('0 respects editing, pause, menus, composition, repeats and modifiers and never assigns a group',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  await page.evaluate(()=>{tcSquare.prepare();tcSquare.run(`selectEntity(squareTarget);window.zeroCalls=0;const selectArmy=selectAllArmy;selectAllArmy=()=>{zeroCalls++;return selectArmy()};window.zeroSelection=selectedIds.slice();window.zeroMenu=document.getElementById('sfMenu');if(!zeroMenu){zeroMenu=document.createElement('section');zeroMenu.id='sfMenu';document.body.appendChild(zeroMenu)}zeroMenu.hidden=true;`)});
  await page.evaluate(()=>tcSquare.run('paused=true'));await page.keyboard.press('0');await page.evaluate(()=>tcSquare.run('paused=false;zeroMenu.hidden=false'));await page.keyboard.press('0');await page.evaluate(()=>tcSquare.run(`zeroMenu.hidden=true;const input=document.createElement('input');input.id='zeroInput';document.body.appendChild(input);input.focus()`));await page.keyboard.type('0');assert.equal(await page.locator('#zeroInput').inputValue(),'0');await page.locator('#zeroInput').evaluate(input=>input.blur());
  await page.evaluate(()=>{for(const options of [{repeat:true},{isComposing:true},{ctrlKey:true},{metaKey:true},{altKey:true},{shiftKey:true}])window.dispatchEvent(new KeyboardEvent('keydown',{key:'0',bubbles:true,...options}))});let result=await page.evaluate(()=>tcSquare.run('({calls:zeroCalls,selected:selectedIds,expected:zeroSelection,commands:squareCommands.length,groups:controlGroupSlots.length})'));assert.equal(result.calls,0);assert.deepEqual(result.selected,result.expected);assert.equal(result.commands,0);assert.equal(result.groups,9);await page.keyboard.press('0');assert.equal(await page.evaluate(()=>zeroCalls),1);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('stable Stop and Hold buttons resolve newly unloaded or remotely boarded troops at click time',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  const result=await page.evaluate(()=>{tcSquare.prepare();return tcSquare.run(`(()=>{
   const ship=addEntity('unit','dropship',0,10,10),passenger=addEntity('unit','rifleman',0,10,11);loadPassenger(ship,passenger);selectAllArmy();const stop=commandButtons.querySelector('[data-command-label="Stop All"]'),hold=commandButtons.querySelector('[data-command-label="Hold All"]'),signature=computeUiSignature(),ids=selectedIds.slice();stop.click();const initiallyExcluded=!squareCommands.at(-1).entityIds.includes(passenger.id);
   unloadDropship(ship);refreshUI(true);hold.click();const unloadedIncluded=squareCommands.at(-1).entityIds.includes(passenger.id);stop.click();const stoppedAfterUnload=squareCommands.at(-1).entityIds.includes(passenger.id),sameAfterUnload=stop===commandButtons.querySelector('[data-command-label="Stop All"]')&&hold===commandButtons.querySelector('[data-command-label="Hold All"]')&&signature===computeUiSignature();
   const snapshot=serializeState(),remotePassenger=snapshot.entities.find(e=>e.id===passenger.id),remoteShip=snapshot.entities.find(e=>e.id===ship.id);remotePassenger.garrisonedIn=ship.id;remotePassenger.transportedIn=ship.id;remotePassenger.order='Transported';remoteShip.passengers=[passenger.id];applyRemoteState(snapshot);stop.click();const excludedFromStop=!squareCommands.at(-1).entityIds.includes(passenger.id);hold.click();const excludedFromHold=!squareCommands.at(-1).entityIds.includes(passenger.id),sameAfterRemote=stop===commandButtons.querySelector('[data-command-label="Stop All"]')&&hold===commandButtons.querySelector('[data-command-label="Hold All"]')&&signature===computeUiSignature(),stillSelected=JSON.stringify(ids)===JSON.stringify(selectedIds),passengerOrder=entityById(passenger.id).order;
   for(const e of entities)if(e.kind==='unit'&&ownEntity(e)&&e.id!==passenger.id)e.alive=false;selectAllArmy();const singleStop=commandButtons.querySelector('[data-command-label="Stop"]'),singleHold=commandButtons.querySelector('[data-command-label="Hold / Auto"]'),count=squareCommands.length;singleStop.onclick();singleHold.onclick();const singleDisabled=singleStop.disabled&&singleHold.disabled&&squareCommands.length===count;const current=entityById(passenger.id);current.garrisonedIn=null;current.transportedIn=null;refreshUI(true);const reenabled=!singleStop.disabled&&!singleHold.disabled&&singleStop===commandButtons.querySelector('[data-command-label="Stop"]');singleStop.click();const singleDeployed=squareCommands.at(-1).type==='stop'&&squareCommands.at(-1).entityId===passenger.id;
   return {initiallyExcluded,unloadedIncluded,stoppedAfterUnload,sameAfterUnload,excludedFromStop,excludedFromHold,sameAfterRemote,stillSelected,passengerOrder,singleDisabled,reenabled,singleDeployed};
  })()`)});
  assert.deepEqual(result,{initiallyExcluded:true,unloadedIncluded:true,stoppedAfterUnload:true,sameAfterUnload:true,excludedFromStop:true,excludedFromHold:true,sameAfterRemote:true,stillSelected:true,passengerOrder:'Transported',singleDisabled:true,reenabled:true,singleDeployed:true});assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});
