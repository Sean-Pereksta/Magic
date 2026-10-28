const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');

const repo=path.resolve(__dirname,'../../..');
const expose=`window.buildClock=100000;nowMs=()=>window.buildClock;
window.tcBuild={run:source=>eval(source),prepare(faction='frontier'){
 resize();terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
 entities=[];resourceNodes=[];nextEntityId=1;nextNodeId=1;players=[makePlayer(0,'Test',faction,false),makePlayer(1,'Opponent','frontier',false)];players.forEach(p=>{p.minerals=300;p.gas=0});
 gameStarted=true;gameOver=false;paused=false;modeState=null;lobbyId='';PLAYER=0;areaEffects=[];pendingImpacts=[];temporaryObstacles=[];tileVisibleToFaction=()=>true;overlay.style.display='none';
 window.buildWorker=addEntity('unit',FACTIONS[faction].worker,0,3,8);window.buildCommands=[];sendCommand=c=>{window.buildCommands.push(c);applyCommand({...c,slot:PLAYER})};
 lastAutoExpandedSelectionId=null;selectEntities([buildWorker]);lastCanvasTap={at:0,entityId:null};cam.zoom=innerWidth<500?.5:.7;centerOn(13,8);draw();refreshUI(true);
},advance(){for(let i=0;i<120;i++){window.buildClock+=500;updatePersistentOrders();updateProduction(.5)}refreshUI(true)}};`;
const source=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8')
 .replace(/^import .*;$/gm,'')
 .replace('\nboot();','\n'+expose+'\nwindow.TurncraftStartup?.complete();')
 .replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
// Construction input and state do not depend on artwork pixels; keep these checks offline.
const placeholder=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/6kQAAAAASUVORK5CYII=','base64');

async function setup(viewport,hasTouch){
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
 const page=await browser.newPage({viewport,hasTouch}),errors=[];page.on('pageerror',error=>errors.push(error.message));
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
async function tapTile(page,x,y,hasTouch){
 const point=await page.evaluate(([x,y])=>tcBuild.run('worldToScreen('+x+'+.5,'+y+'+.5)'),[x,y]);
 if(hasTouch)await page.touchscreen.tap(point.x,point.y);else await page.mouse.click(point.x,point.y);
}

for(const [label,viewport,hasTouch] of [['desktop',{width:1280,height:900},false],['touch',{width:390,height:844},true]]){
 test(`${label}: every faction can choose a 300-mineral base and construct ten squares from its worker`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   for(const [faction,name,type] of [['frontier','Command Hub','commandHub'],['ascendant','Nexus','nexus'],['brood','Hatchery','hatchery']]){
    await page.evaluate(f=>tcBuild.prepare(f),faction);
    assert.equal(await page.locator('#commandButtons [data-command-label="Build"] kbd').innerText(),'B');
    await page.locator('#commandButtons [data-command-label="Build"]').click();
    const card=page.locator('#actionList .actionCard').filter({hasText:name});
    assert.equal(await card.isEnabled(),true,`${name} affordable at exactly 300`);
    assert.equal((await card.locator('.actionCost').innerText()).trim(),'◆ 300');
    await card.click();
    const highlighted=await page.evaluate(()=>tcBuild.run('legalTiles.some(p=>p.x===13&&p.y===8)'));
    assert.equal(highlighted,true,`${name} highlights a distant site without nearby deposits`);
    await page.evaluate(()=>tcBuild.run('centerOn(13,8);draw()'));
    await tapTile(page,13,8,hasTouch);
    let result=await page.evaluate(()=>({commands:buildCommands,pending:buildWorker.pendingBuild,alive:buildWorker.alive!==false,minerals:tcBuild.run('players[0].minerals'),mode:tcBuild.run('interactionMode')}));
    assert.equal(result.commands.length,1);assert.equal(result.commands[0].type,'build');assert.equal(result.commands[0].buildType,type);assert.equal(result.commands[0].x,13);assert.equal(result.commands[0].y,8);assert.ok(result.pending);assert.equal(result.alive,true);assert.equal(result.minerals,300);assert.equal(result.mode,null);
    await page.evaluate(()=>tcBuild.advance());
    result=await page.evaluate(type=>tcBuild.run(`({built:entities.filter(e=>e.type==='${type}'&&e.x===13&&e.y===8).length,complete:entities.find(e=>e.type==='${type}'&&e.x===13&&e.y===8)?.complete,minerals:players[0].minerals,pending:!!buildWorker.pendingBuild,workerAlive:buildWorker.alive!==false})`),type);
    assert.equal(result.built,1);assert.equal(result.complete,true);assert.equal(result.minerals,0);assert.equal(result.pending,false);assert.equal(result.workerAlive,faction!=='brood');
   }
   assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });

 test(`${label}: normal distant structures highlight only visible open sites and a tap queues the worker`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   await page.evaluate(()=>{tcBuild.prepare();tcBuild.run("tileVisibleToFaction=(f,x,y)=>x!==14;addEntity('unit','probe',1,15,8);tileAt(16,8).blocked=true;addNode('mineral',17,8,1000)")});
   assert.equal(await page.locator('#commandButtons [data-command-label="Build"] kbd').innerText(),'B');
    await page.locator('#commandButtons [data-command-label="Build"]').click();
   await page.locator('#actionList .actionCard').filter({hasText:'Supply Depot'}).click();
   assert.deepEqual(await page.evaluate(()=>tcBuild.run('[13,14,15,16,17].map(x=>legalTiles.some(p=>p.x===x&&p.y===8))')),[true,false,false,false,false]);
   await page.evaluate(()=>tcBuild.run('centerOn(13,8);draw()'));
   await tapTile(page,13,8,hasTouch);
   assert.equal(await page.evaluate(()=>buildWorker.pendingBuild?.type),'supplyDepot');
   assert.equal(await page.evaluate(()=>tcBuild.run('players[0].minerals')),300);
   await page.evaluate(()=>tcBuild.advance());
   assert.equal(await page.evaluate(()=>tcBuild.run("entities.find(e=>e.type==='supplyDepot'&&e.x===13&&e.y===8)?.complete")),true);
   assert.equal(await page.evaluate(()=>tcBuild.run('players[0].minerals')),200);
   assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });
}
