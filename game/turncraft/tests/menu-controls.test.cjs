const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const game=path.resolve(__dirname,'../..');
const exposure=`window.tcMenu={
  menu(){resize();renderStartMenu();overlay.style.display='flex'},
  render:renderStartMenu,
  start(f='frontier'){resize();createGame([{name:'Test',factionId:f,ai:false},{name:'Opponent',factionId:'brood',ai:false}])},
  get config(){return{faction:chosenFaction,difficulty:chosenDifficulty,count:chosenAiCount,slots:buildSoloSlots(),tuning:DIFFICULTY[chosenDifficulty]}},
  get mode(){return interactionMode},get payload(){return interactionPayload},get units(){return entities},
  get selected(){return selectedEntities()},get now(){return nowMs()},
  select(types){const list=types.map((type,i)=>addEntity('unit',type,PLAYER,7+i,8));selectEntities(list);return list.map(e=>e.id)},
  selectBase(research){players[PLAYER].research[research]=1;selectEntity(entities.find(e=>e.faction===PLAYER&&isBase(e)))},
  prepareGas(){const w=entities.find(e=>e.faction===PLAYER&&isWorker(e));w.x=3;w.y=3;w.px=3;w.py=3;const n=resourceNodes.find(n=>n.type==='gas');n.x=10;n.y=5;terrain.forEach(t=>t.blocked=false);addEntity('structure',FACTIONS[players[PLAYER].factionId].base,PLAYER,11,6);visionCacheAt=0;selectEntity(w);centerOn(n.x,n.y);refreshUI(true);const p=worldToScreen(n.x+.5,n.y+.5);return{x:p.x,y:p.y,minerals:players[PLAYER].minerals,workerId:w.id,gasType:FACTIONS[players[PLAYER].factionId].gas}},
  get gasOrder(){const w=selected();return{job:w.pendingBuild,move:w.moveDestination,minerals:players[PLAYER].minerals}},
  refresh(){refreshUI(true)},pause(){paused=true}
};window.tcMenu.menu();`;
const source=fs.readFileSync(path.join(game,'turncraft.html'),'utf8').replace(/^import .*;$/gm,'').replace('\nboot();','\n'+exposure+'\nwindow.TurncraftStartup?.complete();').replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
async function setup(width=1280,height=900){
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
 const page=await browser.newPage({viewport:{width,height}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.route('**/*',route=>{const url=new URL(route.request().url());if(url.pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});const file=path.join(game,url.pathname.replace('/game/',''));if(file.startsWith(game+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file});return route.fulfill({status:404,body:''})});
 await page.goto('http://turncraft.test/game/turncraft.html');assert.deepEqual(errors,[]);return{browser,page,errors};
}

test('illustrated faction selection and real difficulty choices persist without replacing focus',async()=>{
 const {browser,page,errors}=await setup();try{
  for(const faction of ['frontier','ascendant','brood']){const choice=page.locator(`.factionCard[data-faction="${faction}"]`);await choice.click();assert.equal(await choice.getAttribute('aria-pressed'),'true');assert.equal(await page.evaluate(()=>tcMenu.config.faction),faction)}
  assert.equal(await page.locator('.factionHero img').evaluateAll(images=>images.filter(img=>img.complete&&img.naturalWidth===1536).length),3);
  const intervals=[];for(const id of ['easy','medium','hard','pro']){await page.locator(`[data-difficulty="${id}"]`).click();const result=await page.evaluate(()=>({config:tcMenu.config,saved:localStorage.getItem('csf.difficulty'),native:document.getElementById('difficultySelect').value}));assert.equal(result.config.difficulty,id);assert.equal(result.saved,id);assert.equal(result.native,id);intervals.push(result.config.tuning.thinkInterval)}
  assert.ok(intervals.every((value,i)=>i===0||value<intervals[i-1]));
  await page.locator('[data-difficulty="hard"]').focus();await page.evaluate(()=>{tcMenu.render();tcMenu.render()});assert.equal(await page.locator('[data-difficulty="hard"]').evaluate(el=>el===document.activeElement),true);
  await page.locator('#aiCountSelect').selectOption('1');assert.equal(await page.evaluate(()=>tcMenu.config.slots.length),2);await page.locator('#aiCountSelect').selectOption('2');assert.equal(await page.evaluate(()=>tcMenu.config.slots.length),3);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('ability hotkeys activate ready casters, select targeting, and respect cooldowns and input focus',async()=>{
 const {browser,page,errors}=await setup();try{
  for(const [faction,type] of [['frontier','engineer'],['ascendant','probe'],['brood','drone']]){
   await page.evaluate(([f,t])=>{tcMenu.start(f);tcMenu.select([t])},[faction,type]);const key=page.locator('#abilityButtons [data-hotkey="Q"]');assert.equal(await key.getAttribute('aria-keyshortcuts'),'Q');assert.equal(await key.locator('kbd').isVisible(),true);
   await page.keyboard.press('q');const at=await page.evaluate(()=>tcMenu.selected[0].abilityReadyAt);assert.ok(at>await page.evaluate(()=>tcMenu.now));await page.keyboard.press('q');assert.equal(await page.evaluate(()=>tcMenu.selected[0].abilityReadyAt),at);
  }
  await page.evaluate(()=>{tcMenu.start('ascendant');tcMenu.select(['templar','templar'])});await page.keyboard.press('q');assert.equal(await page.evaluate(()=>tcMenu.mode),'ability');assert.equal(await page.evaluate(()=>tcMenu.payload),'psionicStorm');await page.keyboard.press('Escape');assert.equal(await page.evaluate(()=>tcMenu.mode),null);
  await page.evaluate(()=>tcMenu.select(['sentry']));await page.keyboard.press('e');assert.equal(await page.evaluate(()=>tcMenu.payload),'forcefield');await page.keyboard.press('Escape');
  await page.keyboard.press('Control+q');assert.equal(await page.evaluate(()=>tcMenu.mode),null);await page.keyboard.press('Shift+q');assert.equal(await page.evaluate(()=>tcMenu.mode),null);
  await page.evaluate(()=>{const input=document.createElement('textarea');input.id='testTyping';document.body.append(input);input.focus()});await page.keyboard.press('q');assert.equal(await page.evaluate(()=>tcMenu.mode),null);await page.locator('#testTyping').evaluate(el=>el.remove());
  await page.evaluate(()=>tcMenu.pause());await page.keyboard.press('q');assert.equal(await page.evaluate(()=>tcMenu.mode),null);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('mouse and touch gas placement clicks queue distant construction for every faction',async()=>{
 const {browser,page,errors}=await setup();try{
  for(const faction of ['frontier','ascendant','brood'])for(const pointerType of ['mouse','touch']){
   const target=await page.evaluate(f=>{tcMenu.start(f);return tcMenu.prepareGas()},faction);
   await page.locator('#actionList .actionCard').filter({hasText:new RegExp(target.gasType,'i')}).click();assert.equal(await page.evaluate(()=>tcMenu.mode),'build');
   if(pointerType==='mouse')await page.mouse.click(target.x,target.y);else await page.evaluate(({x,y})=>{const canvas=document.getElementById('game');canvas.setPointerCapture=()=>{};for(const type of ['pointerdown','pointerup'])canvas.dispatchEvent(new PointerEvent(type,{pointerId:55,pointerType:'touch',button:0,clientX:x,clientY:y,bubbles:true}))},target);
   const order=await page.evaluate(()=>tcMenu.gasOrder);assert.equal(order.job?.type,target.gasType,`${faction} ${pointerType}`);assert.ok(order.move);assert.equal(order.minerals,target.minerals);assert.equal(await page.evaluate(()=>tcMenu.mode),null);
  }
  assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});

test('setup remains scrollable and deploy stays accessible on phone, landscape, and desktop',async()=>{
 for(const [width,height] of [[390,844],[844,390],[1280,800]]){
  const {browser,page,errors}=await setup(width,height);try{
   const layout=await page.evaluate(()=>{const card=document.getElementById('startCard'),rect=card.getBoundingClientRect();return{x:rect.x,y:rect.y,right:rect.right,bottom:rect.bottom,overflow:getComputedStyle(card).overflowY,buttons:[...document.querySelectorAll('.factionCard,.difficultyChoice')].map(b=>({width:b.clientWidth,height:b.clientHeight}))}});
   assert.ok(layout.x>=0&&layout.y>=0&&layout.right<=width+1&&layout.bottom<=height+1,JSON.stringify(layout));assert.equal(layout.overflow,'auto');assert.ok(layout.buttons.every(b=>b.width>=44&&b.height>=44));
   await page.locator('#startBtn').scrollIntoViewIfNeeded();assert.equal(await page.locator('#startBtn').isVisible(),true);
   if(process.env.TURNCRAFT_SCREENSHOT_DIR){fs.mkdirSync(process.env.TURNCRAFT_SCREENSHOT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.TURNCRAFT_SCREENSHOT_DIR,`menu-${width}x${height}.png`)});if(width===1280){await page.evaluate(()=>document.getElementById('startCard').scrollTop=0);await page.screenshot({path:path.join(process.env.TURNCRAFT_SCREENSHOT_DIR,'menu-desktop-top.png')})}}
   await page.locator('#startCard').evaluate(el=>el.scrollTop=el.scrollHeight);assert.equal(await page.locator('#objectiveSelect').isVisible(),true);
   const objective=await page.locator('#objectiveSelect').boundingBox();const deploy=await page.locator('#startBtn').boundingBox();assert.ok(objective.y+objective.height<=deploy.y,JSON.stringify({objective,deploy}));
   if(process.env.TURNCRAFT_SCREENSHOT_DIR)await page.screenshot({path:path.join(process.env.TURNCRAFT_SCREENSHOT_DIR,`menu-bottom-${width}x${height}.png`)});
   await page.locator('#startBtn').click();assert.equal(await page.locator('#overlay').isVisible(),false);assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 }
});
