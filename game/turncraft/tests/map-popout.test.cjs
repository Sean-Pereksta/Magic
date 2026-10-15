const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {chromium}=require('playwright');
const game=path.resolve(__dirname,'../..');
const html=fs.readFileSync(process.env.TURNCRAFT_SOURCE||path.join(game,'turncraft.html'),'utf8');
const exposure=`window.tcPopout={
  get map(){return chosenMapId},get locked(){return mapVoteLocked()},get state(){return serializeState()},
  render:renderStartMenu,select:selectBattlefield,vote:resolveMapVote,ready:publishReady,
  lobby(record){username='Commander';lobbyId='test-room';lobbyRef={id:'test-room'};humanNames=['Commander','Other'];lobbyData={chessStarfrontReady:record||{}};renderStartMenu()},
  start(){resize();createGame(buildSoloSlots())},remote(data){applyRemoteState(data)}
};resize();renderStartMenu();overlay.style.display='flex';`;
const mocks=`const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({}),serverTimestamp=()=>1;
window.tcWrites=[];window.tcFailWrite=false;window.tcHoldWrite=null;
async function setDoc(ref,data,options){window.tcWrites.push({ref,data,options});if(window.tcHoldWrite)await window.tcHoldWrite;if(window.tcFailWrite)throw new Error('test write failure')}`;
const source=html.replace(/^import .*;$/gm,'').replace('\nboot();','\n'+exposure+'\nwindow.TurncraftStartup?.complete();').replace('<script type="module">','<script>'+mocks);
async function setup(width=1280,height=900){
  const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
  const page=await browser.newPage({viewport:{width,height}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.route('**/*',route=>{const url=new URL(route.request().url());if(url.pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});const file=path.join(game,url.pathname.replace('/game/',''));if(file.startsWith(game+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file});return route.fulfill({status:404,body:''})});
  await page.goto('http://turncraft.test/game/turncraft.html');assert.deepEqual(errors,[]);return{browser,page,errors};
}
const ids=['classic','ember-rift','frostline-basin','verdant-divide','shattered-dunes','orbital-foundry'];
async function open(page){await page.locator('#mapSelectBtn').click();await page.locator('#mapPopout.open').waitFor()}
async function closed(page){await page.locator('#mapPopout').waitFor({state:'hidden'})}

test('majority and first-ready tie resolution remain unchanged',()=>{
  require('../maps.js');const maps=globalThis.TurncraftMaps.definitions;
  // Evaluate the production resolver; UI redesign must preserve its exact voting semantics.
  const resolver=html.match(/function resolveMapVote\(records[^)]*\)\{[^\n]+/)[0];
  const context={MAP_DEFINITIONS:maps};vm.createContext(context);vm.runInContext(resolver,context);
  assert.equal(context.resolveMapVote([{mapVote:'ember-rift',ready:true,readyAtMs:3},{mapVote:'classic',ready:true,readyAtMs:1},{mapVote:'ember-rift',ready:true,readyAtMs:2}]),'ember-rift');
  assert.equal(context.resolveMapVote([{mapVote:'frostline-basin',ready:true,readyAtMs:4},{mapVote:'orbital-foundry',ready:true,readyAtMs:2}]),'orbital-foundry');
  assert.equal(context.resolveMapVote([{mapVote:'missing',ready:true},{mapVote:'ember-rift',ready:false}]),'classic');
});

// Fast state tests use the production menu functions with a small native-dialog model.
function menuModel(){
  require('../maps.js');const maps=globalThis.TurncraftMaps.definitions,ids=['mapChoices','mapSelectBtn','selectedMapPreview','selectedMapName','selectedMapTheme','mapSummary','mapSelectAction','mapPopoutNote','mapPopout','mapPopoutClose','startBtn','startStatus'];
  let nextTimer=0;const timers=new Map(),previews=[],stored=new Map();
  const node=()=>({children:[],replaceChildren(){this.children=[]},querySelectorAll(){return this.children.filter(child=>child.dataset.map)},dataset:{},disabled:false,open:false,inert:false,style:{setProperty(){}},attributes:{},listeners:{},classes:new Set(),setAttribute(key,value){this.attributes[key]=value},appendChild(child){this.children.push(child)},querySelector(selector){if(selector==='canvas')return this.preview||(this.preview=node());const match=selector.match(/data-map="([^"]+)"/);return match&&elements.mapChoices.children.find(child=>child.dataset.map===match[1])},addEventListener(key,fn){this.listeners[key]=fn},focus(){elements.active=this},scrollIntoView(){},showModal(){this.open=true},close(){this.open=false},getBoundingClientRect(){return{left:10,top:10,right:90,bottom:90}},classList:{}});
  const attach=n=>{n.classList={add:c=>n.classes.add(c),remove:c=>n.classes.delete(c),toggle(c,value){if(value)n.classes.add(c);else n.classes.delete(c)}};return n};
  const elements=Object.fromEntries(ids.map(id=>[id,attach(node())]));
  const document={getElementById:id=>elements[id],createElement:()=>attach(node())};
  const context={document,MAP_DEFINITIONS:maps,BASE_MAP_IDS:new Set(maps.map(map=>map.id)),ensureCompatibleMap(){},desiredParticipantCount:()=>3,mapCapacity:id=>maps.find(map=>map.id===(id||context.chosenMapId)).capacity,lobbyId:'',lobbyData:null,username:'Commander',lobbyRef:{},chosenMapId:'classic',chosenFaction:'frontier',chosenGameMode:'annihilation',gameStarted:false,mapReadyPending:false,readyMapVote:null,mapPopoutCloseTimer:null,startBtn:elements.startBtn,startStatus:elements.startStatus,
    localStorage:{setItem:(key,value)=>stored.set(key,value)},mapDefinition:id=>maps.find(map=>map.id===(id||context.chosenMapId)),safeId:id=>id,drawMapPreview:(canvas,id)=>previews.push(id),loadMapAtlas(){},renderStartStatus(){},toast(){},nowMs:()=>100,serverTimestamp:()=>1,setDoc:async()=>{},window:{matchMedia:()=>({matches:false})},requestAnimationFrame:fn=>fn(),setTimeout:fn=>{timers.set(++nextTimer,fn);return nextTimer},clearTimeout:id=>timers.delete(id)};
  vm.createContext(context);const fragment=html.slice(html.indexOf('function mapVoteLocked()'),html.indexOf('function renderStartMenu()'));vm.runInContext(fragment,context);
  vm.runInContext(html.slice(html.indexOf('async function publishReady(){'),html.indexOf('function renderStartStatus(){')),context);
  return{context,elements,previews,stored,timers,finishClose(){for(const fn of [...timers.values()])fn();timers.clear()}};
}

test('menu state updates previews and persistence and closes once despite repeated renders',()=>{
  const m=menuModel(),c=m.context;c.renderMapChoices();assert.equal(m.elements.mapChoices.children.filter(child=>child.dataset.map).length,14);assert.equal(m.elements.mapPopout.open,false);
  c.openMapPopout();assert.equal(m.elements.mapPopout.open,true);assert.equal(m.elements.active.dataset.map,'classic');
  assert.equal(c.selectBattlefield('ember-rift'),true);assert.equal(c.chosenMapId,'ember-rift');assert.equal(m.stored.get('csf.map'),'ember-rift');assert.equal(m.elements.selectedMapPreview.dataset.renderedMap,'ember-rift');assert.equal(m.elements.mapSelectBtn.attributes['aria-expanded'],'false');assert.equal(m.timers.size,1);
  c.closeMapPopout();c.closeMapPopout();assert.equal(m.timers.size,1);m.finishClose();assert.equal(m.elements.mapPopout.open,false);assert.equal(m.elements.active,m.elements.mapSelectBtn);
  assert.equal(c.selectBattlefield('invalid'),false);assert.equal(c.chosenMapId,'ember-rift');c.openMapPopout();c.gameStarted=true;c.closeMapPopout(false,true);assert.equal(m.elements.mapPopout.open,false);assert.equal(m.timers.size,0);
});

test('ready state keeps the voted map immutable in flight, on failure and on reconnect',async()=>{
  const m=menuModel(),c=m.context;c.lobbyId='test';c.selectBattlefield('verdant-divide');c.openMapPopout();
  let release;let write;c.setDoc=async(ref,data)=>{write=data;await new Promise(resolve=>release=resolve)};
  const ready=c.publishReady();assert.equal(c.mapVoteLocked(),true);assert.equal(m.elements.mapSelectBtn.disabled,true);assert.equal(c.selectBattlefield('classic'),false);assert.equal(c.chosenMapId,'verdant-divide');
  for(let i=0;i<5;i++)c.renderMapChoices();assert.equal(m.timers.size,1);m.finishClose();assert.equal(m.elements.mapPopout.open,false);release();await ready;
  assert.equal(write.chessStarfrontReady.Commander.mapVote,'verdant-divide');assert.equal(c.mapVoteLocked(),true);assert.equal(c.mapReadyPending,false);assert.equal(m.elements.mapSelectAction.textContent,'Vote Locked');
  const n=menuModel();n.context.lobbyId='test';n.context.setDoc=async()=>{throw Error('offline')};await n.context.publishReady();assert.equal(n.context.mapVoteLocked(),false);assert.equal(n.elements.mapSelectBtn.disabled,false);assert.equal(n.elements.startBtn.disabled,false);assert.equal(n.context.selectBattlefield('orbital-foundry'),true);
  n.context.lobbyData={chessStarfrontReady:{Commander:{ready:true,mapVote:'frostline-basin'}}};n.context.renderMapChoices();assert.equal(n.context.chosenMapId,'frostline-basin');assert.equal(n.context.mapVoteLocked(),true);assert.equal(n.elements.mapSelectBtn.disabled,true);assert.equal(n.context.selectBattlefield('classic'),false);
});

test('compact battlefield card opens categorized strategic previews and selection updates, saves and closes',async()=>{
  const {browser,page,errors}=await setup();try{
    assert.equal(await page.locator('#mapPopout').isVisible(),false);assert.equal(await page.locator('#startCard [data-map]').count(),0);
    assert.equal(await page.locator('#selectedMapName').textContent(),'Classic Frontier');
    for(const id of ids){
      await open(page);assert.equal(await page.locator('#mapChoices [data-map]').count(),14);assert.equal(await page.locator('#mapSelectBtn').getAttribute('aria-expanded'),'true');
      assert.equal(await page.locator(`[data-map="${await page.evaluate(()=>tcPopout.map)}"]`).evaluate(el=>el===document.activeElement),true);
      await page.locator(`[data-map="${id}"]`).click();await closed(page);
      const result=await page.evaluate(()=>({map:tcPopout.map,saved:localStorage.getItem('csf.map'),preview:document.getElementById('selectedMapPreview').dataset.renderedMap,focused:document.activeElement.id}));
      assert.equal(result.map,id);assert.equal(result.saved,id);assert.equal(result.preview,id);assert.equal(result.focused,'mapSelectBtn');
      assert.equal(await page.locator('#mapSelectBtn').getAttribute('aria-expanded'),'false');
    }
    await page.reload();assert.equal(await page.evaluate(()=>tcPopout.map),'orbital-foundry');assert.deepEqual(errors,[]);
  }finally{await browser.close()}
});

test('keyboard focus stays inside dialog; Escape, outside click and close button restore focus',async()=>{
  const {browser,page,errors}=await setup();try{
    await open(page);await page.locator('[data-map="orbital-foundry"]').focus();await page.keyboard.press('Tab');
    assert.equal(await page.evaluate(()=>document.getElementById('mapPopout').contains(document.activeElement)),true);
    await page.keyboard.press('Escape');await closed(page);assert.equal(await page.evaluate(()=>document.activeElement.id),'mapSelectBtn');
    await open(page);await page.mouse.click(3,3);await closed(page);assert.equal(await page.evaluate(()=>document.activeElement.id),'mapSelectBtn');
    await open(page);await page.locator('#mapPopoutClose').click();await closed(page);assert.deepEqual(errors,[]);
  }finally{await browser.close()}
});

test('battlefield voting locks during ready write and after success; failed writes allow choosing again',async()=>{
  const {browser,page,errors}=await setup();try{
    await page.evaluate(()=>{tcPopout.lobby();tcPopout.select('shattered-dunes');window.tcHoldWrite=new Promise(resolve=>window.tcReleaseWrite=resolve);window.tcReadyPromise=tcPopout.ready()});
    assert.equal(await page.evaluate(()=>tcPopout.locked),true);assert.equal(await page.locator('#mapSelectBtn').isDisabled(),true);
    assert.equal(await page.evaluate(()=>tcPopout.select('classic')),false);assert.equal(await page.evaluate(()=>tcPopout.map),'shattered-dunes');
    await page.evaluate(async()=>{window.tcReleaseWrite();await window.tcReadyPromise});
    assert.equal(await page.evaluate(()=>tcWrites[0].data.chessStarfrontReady.Commander.mapVote),'shattered-dunes');
    assert.equal(await page.evaluate(()=>tcPopout.locked),true);assert.equal(await page.locator('#mapSelectAction').textContent(),'Vote Locked');
    await page.reload();await page.evaluate(()=>tcPopout.lobby({Commander:{ready:true,mapVote:'frostline-basin'},Other:{ready:true,mapVote:'classic'}}));
    assert.equal(await page.evaluate(()=>tcPopout.map),'frostline-basin');assert.equal(await page.locator('#mapSelectBtn').isDisabled(),true);assert.match(await page.locator('#startStatus').textContent(),/Frostline Basin 1/);
    await page.reload();await page.evaluate(async()=>{tcPopout.lobby();window.tcFailWrite=true;await tcPopout.ready()});
    assert.equal(await page.evaluate(()=>tcPopout.locked),false);assert.equal(await page.locator('#mapSelectBtn').isDisabled(),false);await open(page);await page.locator('[data-map="ember-rift"]').click();await closed(page);assert.deepEqual(errors,[]);
  }finally{await browser.close()}
});

test('phone and landscape map popouts scroll without overflow and selected map starts and synchronizes',async()=>{
  for(const [width,height] of [[390,844],[844,390],[1280,900]]){
    const {browser,page,errors}=await setup(width,height);try{
      await open(page);
      const layout=await page.locator('#mapPopout').evaluate(el=>{const r=el.getBoundingClientRect(),grid=document.getElementById('mapChoices');return{x:r.left,y:r.top,right:r.right,bottom:r.bottom,overflow:el.scrollWidth-el.clientWidth,scrollable:grid.scrollHeight>=grid.clientHeight}});
      assert.ok(layout.x>=0&&layout.y>=0&&layout.right<=width+1&&layout.bottom<=height+1,JSON.stringify(layout));assert.ok(layout.overflow<=2);assert.equal(layout.scrollable,true);
      const choice=page.locator('[data-map="orbital-foundry"]');await choice.scrollIntoViewIfNeeded();const size=await choice.boundingBox();assert.ok(size.width>=44&&size.height>=44);
      await choice.click();await closed(page);const snapshot=await page.evaluate(()=>{tcPopout.start();return tcPopout.state});assert.equal(snapshot.chosenMapId,'orbital-foundry');
      await page.reload();await open(page);await page.evaluate(data=>tcPopout.remote(data),snapshot);assert.equal(await page.locator('#mapPopout').isVisible(),false);assert.equal(await page.evaluate(()=>tcPopout.map),'orbital-foundry');assert.deepEqual(errors,[]);
    }finally{await browser.close()}
  }
});
