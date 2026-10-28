const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {assetPath}=require('./helpers/cloudflare-assets.cjs');
const repo=path.resolve(__dirname,'../../..');
const expose=`window.compactClock=100000;nowMs=()=>window.compactClock;
window.compactTest=source=>eval(source);
window.prepareCompact=(faction='frontier',selection='worker')=>{
 resize();terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
 entities=[];resourceNodes=[];nextEntityId=1;nextNodeId=1;players=[makePlayer(0,'Test',faction,false),makePlayer(1,'Opponent','frontier',false)];
 players[0].minerals=5000;players[0].gas=5000;
 gameStarted=true;gameOver=false;paused=false;modeState=null;lobbyId='';PLAYER=0;areaEffects=[];pendingImpacts=[];temporaryObstacles=[];tileVisibleToFaction=()=>true;overlay.style.display='none';
 const worker=addEntity('unit',FACTIONS[faction].worker,0,8,8),troop=addEntity('unit',{frontier:'rifleman',ascendant:'dragoon',brood:'hydralisk'}[faction],0,9,8),base=addEntity('structure',FACTIONS[faction].base,0,6,11);
 window.compactSelection=selection==='worker'?worker:selection==='base'?base:troop;
 selectEntity(compactSelection);commandPanel.classList.remove('mobileCollapsed','mobileExpanded');cam.zoom=.7;focusSelected();draw();refreshUI(true);
};window.TurncraftStartup?.complete();`;
const source=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8').replace(/^import .*;$/gm,'').replace('\nboot();','\n'+expose).replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
const placeholder=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/6kQAAAAASUVORK5CYII=','base64');
async function setup(viewport,touch,{before=false}={}){
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']}),page=await browser.newPage({viewport,hasTouch:touch}),errors=[];
 page.on('pageerror',error=>errors.push(error.message));await page.clock.install({time:new Date('2026-01-01T00:00:00Z')});
 const beforeCss=before?fs.readFileSync(path.join(repo,'game/turncraft/compact-command.css'),'utf8').split('/* Compact battle workspace.')[0]:null;
 await page.route('**/*',route=>{const request=route.request(),url=new URL(request.url());
  if(url.pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});
  if(beforeCss&&url.pathname==='/game/turncraft/compact-command.css')return route.fulfill({contentType:'text/css',body:beforeCss});
  const art=assetPath(request.url());if(art)return route.fulfill({path:art,contentType:'image/webp',headers:{'Access-Control-Allow-Origin':'*'}});
  const file=path.resolve(repo,'.'+decodeURIComponent(url.pathname));if(file.startsWith(repo+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file});
  if(request.resourceType()==='image')return route.fulfill({contentType:'image/png',body:placeholder});return route.fulfill({status:404,body:''});
 });
 await page.goto('http://turncraft.test/game/turncraft.html');await page.clock.pauseAt(new Date('2026-01-01T01:00:00Z'));
 return {browser,page,errors};
}
function intersects(a,b){return a.x<b.x+b.width&&a.x+a.width>b.x&&a.y<b.y+b.height&&a.y+a.height>b.y;}
const views=[['desktop',{width:1440,height:900},false,160],['phone',{width:390,height:844},true,230],['landscape',{width:844,height:390},true,210]];
for(const[label,viewport,touch,heightLimit]of views)test(`${label}: compact workspace preserves scrollable actions, top groups, readable selection and transient feedback`,async()=>{
 const {browser,page,errors}=await setup(viewport,touch);
 try{
  await page.evaluate(()=>{prepareCompact('frontier','worker');if(!document.querySelector('.sfMenuUtility')){const menu=document.createElement('button');menu.className='hudBtn sfMenuUtility';menu.textContent='Menu';document.getElementById('utilityButtons').append(menu);}});
  const geometry=await page.evaluate(()=>{const bounds=id=>{const r=document.getElementById(id).getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height};};const movement=document.getElementById('commandButtons'),actions=document.getElementById('actionList');return {dock:bounds('commandPanel'),groups:bounds('controlGroups'),resources:bounds('resourceHud'),minimap:bounds('minimapDock'),sound:bounds('tcAudio'),name:bounds('selectionName'),move:bounds('commandButtons'),actions:bounds('actionList'),movement:{client:movement.clientHeight,scroll:movement.scrollHeight,overflow:getComputedStyle(movement).overflowY},actionScroll:{client:actions.clientHeight,scroll:actions.scrollHeight,overflow:getComputedStyle(actions).overflowY},hidden:['selectionMeta','selectionOrder','contextSummary','cooldownGrid','hintLine','statusText'].every(id=>getComputedStyle(document.getElementById(id)).display==='none'),buttons:[...document.querySelectorAll('#commandButtons button,#actionList button,#utilityButtons button,#controlGroups button')].filter(e=>getComputedStyle(e).display!=='none').map(e=>({width:e.getBoundingClientRect().width,height:e.getBoundingClientRect().height,context:!!e.closest('#utilityButtons')})),groupCount:document.querySelectorAll('#controlGroups .controlGroupChip').length,overflow:document.documentElement.scrollWidth>innerWidth};});
  assert.ok(geometry.dock.height<=heightLimit,JSON.stringify(geometry));assert.ok(geometry.dock.y>=0&&geometry.dock.y+geometry.dock.height<=viewport.height,JSON.stringify(geometry.dock));
  assert.ok(geometry.groups.y<=6);assert.equal(geometry.groupCount,10);assert.equal(intersects(geometry.groups,geometry.resources),false);assert.equal(intersects(geometry.groups,geometry.sound),false);assert.equal(intersects(geometry.resources,geometry.minimap),false);assert.equal(intersects(geometry.minimap,geometry.sound),false);
  assert.ok(geometry.name.width>80&&geometry.name.height>=16);assert.equal(geometry.hidden,true);assert.equal(geometry.overflow,false);
  assert.ok(geometry.move.width<=110&&geometry.move.height<=100,JSON.stringify(geometry.move));assert.equal(geometry.movement.overflow,'auto');assert.ok(geometry.movement.scroll>geometry.movement.client);
  assert.equal(geometry.actionScroll.overflow,'auto');assert.ok(geometry.actionScroll.scroll>=geometry.actionScroll.client,'building options stay in a bounded grid');if(touch)assert.ok(geometry.actionScroll.scroll>geometry.actionScroll.client,'smaller viewports scroll through all building options');
  assert.ok(geometry.buttons.every(b=>b.width>=44&&b.height>=(touch||!b.context?44:26)),JSON.stringify(geometry.buttons));
  const accessible=await page.evaluate(()=>{const box=document.getElementById('commandButtons'),last=box.querySelector('button:last-child');last.scrollIntoView({block:'nearest'});const r=last.getBoundingClientRect(),b=box.getBoundingClientRect();return {inBox:r.top>=b.top&&r.bottom<=b.bottom,hit:document.elementFromPoint(r.x+r.width/2,r.y+r.height/2)?.closest('button')===last};});assert.deepEqual(accessible,{inBox:true,hit:true});
  const menuReachable=await page.evaluate(()=>{const menu=document.querySelector('.sfMenuUtility'),box=document.getElementById('utilityButtons');menu.scrollIntoView({block:'nearest',inline:'nearest'});const r=menu.getBoundingClientRect(),b=box.getBoundingClientRect();return {width:r.width,height:r.height,inBox:r.left>=b.left&&r.right<=b.right&&r.top>=b.top&&r.bottom<=b.bottom,hit:document.elementFromPoint(r.x+r.width/2,r.y+r.height/2)?.closest('button')===menu};});assert.ok(menuReachable.width>=44&&menuReachable.height>=(touch?44:26),JSON.stringify(menuReachable));assert.equal(menuReachable.inBox,true);assert.equal(menuReachable.hit,true);
  await page.evaluate(()=>compactTest(`window.compactButton=commandButtons.querySelector('button');window.compactDockHeight=commandPanel.getBoundingClientRect().height;toast('Not enough minerals',1200);`));
  assert.equal(await page.locator('#toast').getAttribute('aria-live'),'polite');assert.equal(await page.locator('#toast').isVisible(),true);const feedback=await page.locator('#toast').boundingBox();assert.ok(feedback.x>=0&&feedback.x+feedback.width<=viewport.width&&feedback.y>=0&&feedback.y+feedback.height<=viewport.height);if(label!=='landscape')assert.ok(feedback.y+feedback.height<geometry.dock.y);
  await page.clock.runFor(1201);assert.equal(await page.locator('#toast').isVisible(),false);assert.equal(await page.evaluate(()=>compactTest('compactButton===commandButtons.querySelector("button")&&compactDockHeight===commandPanel.getBoundingClientRect().height')),true);
  if(touch){const collapsed=await page.evaluate(()=>{const panel=document.getElementById('commandPanel');panel.classList.add('mobileCollapsed');const r=panel.getBoundingClientRect(),name=document.getElementById('selectionName').getBoundingClientRect();return {height:r.height,nameInDock:name.top>=r.top&&name.bottom<=r.bottom,hidden:['selectionMeta','selectionOrder','cooldownGrid'].every(id=>getComputedStyle(document.getElementById(id)).display==='none')};});assert.ok(collapsed.height<=56);assert.equal(collapsed.nameInDock,true);assert.equal(collapsed.hidden,true);await page.evaluate(()=>document.getElementById('commandPanel').classList.remove('mobileCollapsed'));}
  const abilities=await page.evaluate(()=>{prepareCompact('ascendant','troop');compactTest('compactSelection.abilityReadyAt=compactClock+1000;refreshUI()');const buttons=[...document.querySelectorAll('#abilityButtons [data-ability]')];return {count:buttons.length,descriptionsHidden:buttons.every(b=>[...b.querySelectorAll('small,.abilityExplain')].every(n=>getComputedStyle(n).display==='none')),tooltips:buttons.every(b=>b.title.length>20),progress:buttons.some(b=>parseFloat(b.style.getPropertyValue('--cooldown'))>0)};});assert.ok(abilities.count>0);assert.equal(abilities.descriptionsHidden,true);assert.equal(abilities.tooltips,true);assert.equal(abilities.progress,true);
  if(process.env.TURNCRAFT_COMPACT_SCREENSHOT_DIR){await page.evaluate(async()=>{await compactTest("spriteRuntime.loadManifest(new URL('turncraft/assets/animation-manifest.json',location.href).href)");prepareCompact('ascendant','worker');});await page.waitForTimeout(600);await page.evaluate(()=>compactTest('draw();refreshUI()'));await page.waitForTimeout(600);await page.evaluate(()=>compactTest('draw();refreshUI()'));fs.mkdirSync(process.env.TURNCRAFT_COMPACT_SCREENSHOT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.TURNCRAFT_COMPACT_SCREENSHOT_DIR,`after-${label}.png`)});}
  assert.deepEqual(errors,[]);
 }finally{await browser.close()}
 if(process.env.TURNCRAFT_COMPACT_SCREENSHOT_DIR){const before=await setup(viewport,touch,{before:true});try{await before.page.evaluate(()=>prepareCompact('ascendant','worker'));await before.page.screenshot({path:path.join(process.env.TURNCRAFT_COMPACT_SCREENSHOT_DIR,`before-${label}.png`)});}finally{await before.browser.close();}}
});
