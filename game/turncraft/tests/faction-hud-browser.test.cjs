const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {assetPath}=require('./helpers/cloudflare-assets.cjs');

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

async function setup(viewport,hasTouch,{missingFrames=false}={}){
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
 const page=await browser.newPage({viewport,hasTouch}),errors=[];page.on('pageerror',error=>errors.push(error.message));
 // Hold thresholds must measure simulated input time, not browser protocol latency under load.
 await page.clock.install({time:new Date('2026-01-01T00:00:00Z')});
 await page.route('**/*',route=>{const request=route.request(),url=new URL(request.url());
  if(url.pathname.includes('/ui/frames/')){const frame=path.resolve(repo,'.'+decodeURIComponent(url.pathname));return missingFrames||!fs.existsSync(frame)?route.fulfill({status:404,body:''}):route.fulfill({path:frame,contentType:'image/webp'})}const artwork=assetPath(request.url());if(artwork)return route.fulfill({path:artwork,contentType:'image/webp',headers:{'Access-Control-Allow-Origin':'*'}});if(request.resourceType()==='image')return route.fulfill({contentType:'image/png',body:placeholder});
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

for(const [label,viewport,touch] of [['desktop',{width:1440,height:900},false],['phone',{width:390,height:844},true]]){
 test(`${label}: all three faction consoles use distinct materials and passive frames while keeping commands stable`,async()=>{
  const {browser,page,errors}=await setup(viewport,touch);
  try{
   const materials=[];
   for(const faction of ['frontier','ascendant','brood']){
    await page.evaluate(f=>{tcSquare.prepare(f);tcSquare.run(`selectEntity(squareTarget);window.hudSavedButton=document.querySelector('#commandButtons button');window.hudSavedCrest=document.getElementById('tcFactionCrest');hudSavedButton.focus();`);},faction);
    const result=await page.evaluate(()=>tcSquare.run(`(()=>{const button=hudSavedButton,rect=button.getBoundingClientRect(),panel=document.getElementById('commandPanel'),style=getComputedStyle(panel),frame=getComputedStyle(panel,'::after');for(let i=0;i<50;i++){squareClock+=150;players[PLAYER].minerals+=1;refreshUI(true)}return {faction:document.body.dataset.hudFaction,surface:style.backgroundImage,frame:frame.borderImageSource,slice:frame.borderImageSlice,passive:frame.pointerEvents,hit:document.elementFromPoint(rect.x+rect.width/2,rect.y+rect.height/2)?.closest('button')===button,same:button===document.querySelector('#commandButtons button'),focus:document.activeElement===button,crest:document.getElementById('tcFactionCrest')===hudSavedCrest,width:rect.width,height:rect.height,name:selectionName.textContent}})()`));
    assert.equal(result.faction,faction);assert.match(result.frame,new RegExp('/'+faction+'\\.webp'));assert.equal(result.slice,'160');assert.equal(result.passive,'none');assert.equal(result.hit,true);assert.equal(result.same,true);assert.equal(result.focus,true);assert.equal(result.crest,true);assert.ok(result.width>=44&&result.height>=44);assert.ok(result.name.length>0);materials.push(result.surface);
    // These are the same underlying commands used before the decorative overlay.
    await page.locator('#commandButtons [data-command-label="Move"]').click();assert.equal(await page.evaluate(()=>tcSquare.run('interactionMode')),'move');await page.keyboard.press('Escape');assert.equal(await page.evaluate(()=>tcSquare.run('interactionMode')),null);
    if(process.env.TURNCRAFT_HUD_SCREENSHOT_DIR){
     await page.evaluate(async()=>{await tcSquare.run("spriteRuntime.loadManifest(new URL('turncraft/assets/animation-manifest.json',location.href).href)");tcSquare.run(`const faction=players[PLAYER].factionId,type={frontier:'rifleman',ascendant:'dragoon',brood:'hydralisk'}[faction];window.hudArmy=Array.from({length:8},(_,i)=>addEntity('unit',type,0,7+i%4,8+Math.floor(i/4)));addEntity('structure',FACTIONS[faction].base,0,6,11);players[0].minerals=540;players[0].gas=160;selectEntities(hudArmy);assignControlGroup(3);selectEntity(hudArmy[0]);cam.zoom=.7;focusSelected();draw();refreshUI(true)`)});
     await page.waitForTimeout(500);await page.evaluate(()=>tcSquare.run('draw();refreshUI()'));
     fs.mkdirSync(process.env.TURNCRAFT_HUD_SCREENSHOT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.TURNCRAFT_HUD_SCREENSHOT_DIR,`hud-${faction}-${label}.png`)});
    }
   }
   assert.equal(new Set(materials).size,3);assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 });
}

test('faction chrome follows the actual local seat through enemy inspection, snapshots, new battles and setup previews',async()=>{
 const {browser,page,errors}=await setup({width:1440,height:900},false);
 try{
  const result=await page.evaluate(()=>{tcSquare.prepare('ascendant');return tcSquare.run(`(()=>{const enemy=addEntity('unit','rifleman',1,11,8);selectEntity(enemy);const inspected=document.body.dataset.hudFaction;PLAYER=1;refreshUI(true);const seat=document.body.dataset.hudFaction,snapshot=serializeState();snapshot.players[1].factionId='brood';applyRemoteState(snapshot);const restored=document.body.dataset.hudFaction;PLAYER=0;createGame([{name:'Commander',factionId:'ascendant',ai:false},{name:'Opponent',factionId:'brood',ai:true}]);const created=document.body.dataset.hudFaction;gameStarted=false;chosenFaction='frontier';renderStartMenu();const preview=document.body.dataset.hudFaction;return {inspected,seat,restored,created,preview}})()`)});
  assert.deepEqual(result,{inspected:'ascendant',seat:'frontier',restored:'brood',created:'ascendant',preview:'frontier'});assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('missing frame artwork leaves the faction palette, text and command hit targets usable',async()=>{
 const {browser,page,errors}=await setup({width:390,height:844},true,{missingFrames:true});
 try{
  for(const faction of ['frontier','ascendant','brood']){
   await page.evaluate(f=>{tcSquare.prepare(f);tcSquare.run('selectEntity(squareTarget);refreshUI(true)')},faction);
   const result=await page.evaluate(()=>({name:document.getElementById('selectionName').textContent,palette:getComputedStyle(document.getElementById('commandPanel')).backgroundImage,border:getComputedStyle(document.getElementById('commandPanel')).borderColor,overflow:document.documentElement.scrollWidth>innerWidth}));
   assert.ok(result.name.length>0);assert.notEqual(result.palette,'none');assert.notEqual(result.border,'rgba(0, 0, 0, 0)');assert.equal(result.overflow,false);
   await page.locator('#commandButtons [data-command-label="Move"]').click();assert.equal(await page.evaluate(()=>tcSquare.run('interactionMode')),'move');await page.keyboard.press('Escape');
  }
  assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});
