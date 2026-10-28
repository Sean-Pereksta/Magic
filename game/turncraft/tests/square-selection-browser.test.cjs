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

for(const [label,viewport,hasTouch] of [['desktop',{width:1280,height:900},false],['touch',{width:390,height:844},true]]){
 test(`${label} occupied-square corners select all worker factions without moving the prior group`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   for(const faction of ['frontier','ascendant','brood']){
    await page.evaluate(f=>tcSquare.prepare(f),faction);
    for(const [x,y] of [[8.02,8.02],[8.98,8.02],[8.02,8.98],[8.98,8.98]]){
     await page.evaluate(()=>tcSquare.reselect());await tap(page,await point(page,x,y),hasTouch);const r=await state(page);
     assert.deepEqual(r.selected,[r.target],`${faction} corner ${x},${y}`);assert.deepEqual(r.commands,[]);assert.deepEqual(r.destinations,[null,null]);
    }
   }
   assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });
 test(`${label} moving troops select at their logical and painted squares and corner double-taps select their type`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   await page.evaluate(()=>{tcSquare.prepare();tcSquare.run('squareTarget.x=12;squareTarget.px=8.2;squareTarget.py=8.2')});
   for(const [x,y] of [[12.02,8.02],[12.98,8.98],[8.22,8.22],[9.18,9.18]]){
    await page.evaluate(()=>tcSquare.reselect());await tap(page,await point(page,x,y),hasTouch);const r=await state(page);
    assert.deepEqual(r.selected,[r.target],`moving square ${x},${y}`);assert.deepEqual(r.commands,[]);
   }
   await page.evaluate(()=>{tcSquare.prepare();window.squareSame=tcSquare.run("addEntity('unit','engineer',0,9,9)")});
   const corner=await point(page,8.98,8.98);await tap(page,corner,hasTouch);await page.evaluate(()=>{squareClock+=100});await tap(page,corner,hasTouch);
   const r=await state(page),same=await page.evaluate(()=>squareSame.id);assert.deepEqual(r.selected,[r.target,same]);assert.deepEqual(r.commands,[]);assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });
 test(`${label} short taps retain the troop pressed even if it moves or disappears before release`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   const cdp=hasTouch?await page.context().newCDPSession(page):null;
   for(const outcome of ['moved','dead','transported','hidden']){
    await page.evaluate(()=>tcSquare.prepare());const p=await point(page,8.98,8.98);
    if(hasTouch)await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:p.x,y:p.y}]});else{await page.mouse.move(p.x,p.y);await page.mouse.down()}
    await page.evaluate(outcome=>{
     if(outcome==='moved')squareTarget.x=squareTarget.px=squareTarget.y=squareTarget.py=12;
     else if(outcome==='dead')squareTarget.alive=false;
     else if(outcome==='transported')squareTarget.garrisonedIn=123;
     else tcSquare.run('tileVisibleToFaction=(f,x,y)=>x!==8||y!==8');
    },outcome);
    await page.clock.runFor(100);
    if(hasTouch)await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});else await page.mouse.up();
    const r=await state(page);assert.deepEqual(r.selected,outcome==='moved'?[r.target]:r.group,outcome);assert.deepEqual(r.commands,[],outcome);assert.deepEqual(r.destinations,[null,null],outcome);
   }
   assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });
 test(`${label} deliberate repeated clicks still cycle co-located troops after preserving pressed targets`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   await page.evaluate(()=>{tcSquare.prepare();window.squareStacked=tcSquare.run("addEntity('unit','rifleman',0,8,8)")});const p=await point(page,8.5,8.5);
   await tap(page,p,hasTouch);let r=await state(page);assert.deepEqual(r.selected,[r.target]);
   await page.evaluate(()=>{squareClock+=400});await tap(page,p,hasTouch);r=await state(page);const stacked=await page.evaluate(()=>squareStacked.id);
   assert.deepEqual(r.selected,[stacked]);assert.deepEqual(r.commands,[]);assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });
}

test('desktop Shift-click adds a troop from its square corner without ordering the selected group',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  await page.evaluate(()=>tcSquare.prepare());const p=await point(page,8.98,8.98);await page.keyboard.down('Shift');await page.mouse.click(p.x,p.y);await page.keyboard.up('Shift');
  const r=await state(page);assert.deepEqual(r.selected,[...r.group,r.target]);assert.deepEqual(r.commands,[]);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('desktop stationary long presses select a troop square corner and consume a vanished troop without orders',async()=>{
 const {browser,page,errors}=await setup({width:1280,height:900},false);
 try{
  for(const vanished of [false,true]){
   await page.evaluate(()=>tcSquare.prepare());const p=await point(page,8.98,8.98);await page.mouse.move(p.x,p.y);await page.mouse.down();
   if(vanished)await page.evaluate(()=>{squareTarget.alive=false});
   await page.clock.runFor(450);await page.mouse.up();const r=await state(page);
   assert.deepEqual(r.selected,vanished?r.group:[r.target],vanished?'vanished troop':'stationary troop');assert.deepEqual(r.commands,[]);assert.deepEqual(r.destinations,[null,null]);
  }
  assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('touch hold at a troop square corner adds and removes that troop without issuing orders',async()=>{
 const {browser,page,errors}=await setup({width:390,height:844},true);
 try{
  await page.evaluate(()=>tcSquare.prepare());const p=await point(page,8.98,8.98),cdp=await page.context().newCDPSession(page);
  const hold=async()=>{await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:p.x,y:p.y}]});await page.clock.runFor(430);await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]})};
  await hold();let r=await state(page);assert.deepEqual(r.selected,[...r.group,r.target]);assert.deepEqual(r.commands,[]);
  await hold();r=await state(page);assert.deepEqual(r.selected,r.group);assert.deepEqual(r.commands,[]);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});
