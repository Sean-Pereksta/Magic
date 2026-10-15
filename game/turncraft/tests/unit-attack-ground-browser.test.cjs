const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');

const repo=path.resolve(__dirname,'../../..');
const expose=`window.groundAttackClock=100000;nowMs=()=>window.groundAttackClock;
window.tcGroundAttack={run:source=>eval(source),prepare({group=true,targets=true}={}){
 resize();terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
 entities=[];resourceNodes=[];nextEntityId=1;nextNodeId=1;players=[makePlayer(0,'Test','frontier',false),makePlayer(1,'Opponent','frontier',false),makePlayer(2,'Ally','frontier',false)];
 players[0].teamId=players[2].teamId=1;players[1].teamId=2;
 gameStarted=true;gameOver=false;paused=false;modeState=null;lobbyId='';PLAYER=0;areaEffects=[];pendingImpacts=[];temporaryObstacles=[];tileVisibleToFaction=()=>true;overlay.style.display='none';endOverlay.style.display='none';
 window.groundAttackTroops=[addEntity('unit','rifleman',0,8,10)];if(group)groundAttackTroops.push(addEntity('unit','rifleman',0,22,10));
 window.groundAttackEnemies=targets?[addEntity('unit','rifleman',1,8,16),addEntity('unit','rifleman',1,22,16)]:[];
 window.groundAttackCommands=[];sendCommand=c=>{groundAttackCommands.push(c);applyCommand({...c,slot:PLAYER})};
 interactionMode=null;interactionPayload=null;lastAutoExpandedSelectionId=null;selectEntities(groundAttackTroops);lastCanvasTap={at:0,entityId:null};lastStackTap={key:'',idx:0,at:0};cam.zoom=innerWidth<500?.5:.7;centerOn(16,8);draw();refreshUI(true);
},advance(steps=1){for(let i=0;i<steps;i++){groundAttackClock+=100;updatePersistentOrders();updateCombat(.1);resolveProjectileImpacts();updateProduction(.1);updateRegeneration(.1);updateAreaEffects()}},state(){return {selected:[...selectedIds],troops:groundAttackTroops.map(e=>({id:e.id,x:e.x,y:e.y,targetId:e.targetId,directTargetId:e.directTargetId,moveDestination:e.moveDestination,attackMoveDestination:e.attackMoveDestination,order:e.order})),enemies:groundAttackEnemies.map(e=>({id:e.id,x:e.x,y:e.y})),commands:groundAttackCommands,mode:interactionMode}}};`;
const source=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8')
 .replace(/^import .*;$/gm,'')
 .replace('\nboot();','\n'+expose+'\nwindow.TurncraftStartup?.complete();')
 .replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
// These checks cover real keyboard/pointer routing and authoritative orders, independent of art pixels.
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
 await page.goto('http://turncraft.test/game/turncraft.html');await page.clock.pauseAt(new Date('2026-01-01T01:00:00Z'));
 return {browser,page,errors};
}
async function tapTile(page,x,y,hasTouch){
 const point=await page.evaluate(([x,y])=>tcGroundAttack.run('worldToScreen('+x+'+.5,'+y+'+.5)'),[x,y]);
 if(hasTouch)await page.touchscreen.tap(point.x,point.y);else await page.mouse.click(point.x,point.y);
}
async function armAttack(page,hasTouch,group=true){
 if(hasTouch)await page.locator('#commandButtons button').filter({has:page.locator('span').filter({hasText:group?/^Attack Group$/:/^Attack$/})}).tap();
 else await page.keyboard.press('a');
 assert.equal(await page.evaluate(()=>tcGroundAttack.run('interactionMode')),'attack');
}
async function state(page){return page.evaluate(()=>tcGroundAttack.state())}

for(const [label,viewport,hasTouch] of [['desktop A',{width:1280,height:900},false],['touch Attack',{width:390,height:844},true]]){
 test(`${label}: a ground order picks the nearest enemy separately for each troop rather than around the click`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   await page.evaluate(()=>{tcGroundAttack.prepare();tcGroundAttack.run("groundAttackEnemies.push(addEntity('unit','rifleman',1,16,5))")});
   await armAttack(page,hasTouch);await tapTile(page,16,4,hasTouch);
   const result=await state(page);
   assert.deepEqual(result.selected,result.troops.map(e=>e.id));assert.equal(result.mode,null);
   assert.equal(result.commands.length,1);assert.equal(result.commands[0].type,'groupAttackGround');assert.deepEqual(result.commands[0].entityIds,result.selected);
   assert.equal(result.commands[0].targetId,undefined);assert.equal(result.commands[0].x,16);assert.equal(result.commands[0].y,4);
   assert.deepEqual(result.troops.map(e=>e.targetId),result.enemies.slice(0,2).map(e=>e.id));
   assert.notEqual(result.troops[0].targetId,result.troops[1].targetId);
   assert.deepEqual(result.troops.map(e=>[e.x,e.y]),[[8,10],[22,10]],'targets are acquired before taking a step toward the click');
   assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });

 test(`${label}: an exact enemy click retains a shared direct target while ground acquisition ignores teammates and hidden enemies`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   await page.evaluate(()=>{tcGroundAttack.prepare();tcGroundAttack.run('centerOn(8,16);draw()')});
   await armAttack(page,hasTouch);await tapTile(page,8,16,hasTouch);
   let result=await state(page);assert.equal(result.commands[0].type,'groupAttack');assert.equal(result.commands[0].targetId,result.enemies[0].id);
   assert.deepEqual(result.troops.map(e=>e.directTargetId),result.troops.map(()=>result.enemies[0].id));assert.deepEqual(result.selected,result.troops.map(e=>e.id));
   await page.evaluate(()=>{tcGroundAttack.prepare();tcGroundAttack.run("addEntity('unit','rifleman',2,8,11);addEntity('unit','rifleman',2,22,11);addEntity('unit','rifleman',1,8,12);addEntity('unit','rifleman',1,22,12);tileVisibleToFaction=(f,x,y)=>y!==12;centerOn(16,4);draw()")});
   await armAttack(page,hasTouch);await tapTile(page,16,4,hasTouch);
   result=await state(page);assert.deepEqual(result.troops.map(e=>e.targetId),result.enemies.map(e=>e.id));assert.deepEqual(result.selected,result.troops.map(e=>e.id));assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });

 test(`${label}: with no initial enemy, a ground order advances until an enemy reaches the troop's normal aggro distance`,async()=>{
  const {browser,page,errors}=await setup(viewport,hasTouch);
  try{
   await page.evaluate(()=>{tcGroundAttack.prepare({group:false,targets:false});tcGroundAttack.run('centerOn(24,10);draw()')});
   await armAttack(page,hasTouch,false);await tapTile(page,24,10,hasTouch);
   let result=await state(page);assert.equal(result.commands[0].type,'attackGround');assert.equal(result.commands[0].entityId,result.troops[0].id);assert.equal(result.troops[0].targetId,null);
   const first=result.troops[0];assert.ok(first.x>8||first.moveDestination,'the unit moves toward the clicked ground');
   await page.evaluate(()=>tcGroundAttack.run("window.enteringEnemy=addEntity('unit','rifleman',1,groundAttackTroops[0].x+8,groundAttackTroops[0].y);groundAttackEnemies.push(enteringEnemy)"));
   await page.evaluate(()=>tcGroundAttack.advance());result=await state(page);assert.equal(result.troops[0].targetId,null,'fallback movement does not reuse the initial doubled search radius');
   await page.evaluate(()=>tcGroundAttack.run('enteringEnemy.x=groundAttackTroops[0].x+4;enteringEnemy.px=enteringEnemy.x;enteringEnemy.y=enteringEnemy.py=groundAttackTroops[0].y'));
   await page.evaluate(()=>tcGroundAttack.advance());result=await state(page);
   assert.equal(result.troops[0].targetId,result.enemies[0].id);assert.deepEqual(result.selected,[result.troops[0].id]);assert.equal(result.commands.length,1);assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });
}
