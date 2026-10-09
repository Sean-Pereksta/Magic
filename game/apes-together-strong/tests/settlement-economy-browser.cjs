/* Actual mouse/touch orders, independent expedition settings and save restoration. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{for(const mobile of [false,true]){
  const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  const press=async selector=>{const el=page.locator(selector);if(mobile)await el.tap();else await el.click()};
  await page.goto(pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href);
  await page.waitForFunction(()=>ATSVisualAssets.status==='ready');await page.locator('#seedInput').fill('ECONOMY-COUNCIL');await press('#newRun');await page.waitForFunction(()=>ATS.screen==='play');
  await page.evaluate(()=>{
   const g=ATS.game;g.update=()=>{};g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.settlements=[];g.effects=[];
   g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;
   g.world.settlementPlot=()=>({valid:false,pending:true,trees:[],rocks:[],blocked:[]});
   g.king.x=g.king.y=0;g.king.hp=g.king.maxHp;g.time=100;g.progression.tier=2;g.progression.acknowledged=2;
   const s={id:'economy-gallery',name:'Farwood',x:0,y:0,population:1,level:1,radius:100,food:100000,wood:100000,livingFounding:true};g.settlements.push(s);
   g.makeApe(0,10,'settled',s.id);g.refreshSettlements();g.colonies.init(s);s.attack=true;window.economyHome=s;
  });
  if(mobile)await press('#mobileSettlementButton');else await page.keyboard.press('b');
  await page.waitForFunction(()=>ATS.screen==='build');assert.equal(await page.locator('#resourceExpeditions').isVisible(),true);
  await page.locator('#expeditionFoodRange').selectOption('extended');await page.locator('#expeditionWoodRange').selectOption('frontier');await page.locator('#expeditionPriority').selectOption('wood');
  assert.deepEqual(await page.evaluate(()=>JSON.parse(JSON.stringify(economyHome.expeditionSettings))),{foodRange:'extended',woodRange:'frontier',priority:'wood'});
  assert.match(await page.locator('#expeditionSummary').innerText(),/parties.*available adults/);
  const options=await page.evaluate(()=>ATS.game.colonies.catalog(economyHome).filter(e=>e.available&&!e.blueprint).map(e=>({kind:e.kind,cost:e.cost})));
  let spentWood=0,spentFood=0,count=0;
  for(const option of options){
   await press('[data-structure="'+option.kind+'"]');spentWood+=option.cost.wood;spentFood+=option.cost.food||0;count++;
   const order=await page.evaluate(()=>({count:economyHome.projects.length,wood:economyHome.wood,food:economyHome.food}));
   assert.deepEqual(order,{count,wood:100000-spentWood,food:100000-spentFood});
   assert.match(await page.locator('#buildFeedback').innerText(),/commissioned/);
  }
  for(let i=0;i<105;i++){await press('[data-structure=hut]');spentWood+=8;count++}
  assert.equal(await page.evaluate(()=>economyHome.projects.length),count);
  assert.match(await page.locator('#buildQueue').innerText(),/Finding the next open plot/);
  assert.equal(await page.locator('[data-structure=royalExpansion]').isEnabled(),false);
  assert.equal(await page.locator('[data-structure=warlordExpansion]').isEnabled(),false);
  const restored=await page.evaluate(()=>{
   const loaded=ATSGame.fromJSON(JSON.parse(JSON.stringify(ATS.game.serialize()))),s=loaded.settlement('economy-gallery');
   return{settings:s.expeditionSettings,projects:s.projects.length,wood:s.wood,food:s.food};
  });
  assert.deepEqual(restored,{settings:{foodRange:'extended',woodRange:'frontier',priority:'wood'},projects:count,wood:100000-spentWood,food:100000-spentFood});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await page.locator('#expeditionFoodRange').scrollIntoViewIfNeeded();
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'economy-council-touch.png':'economy-council-desktop.png')})}
  assert.deepEqual(errors,[]);await page.close();
 }
 console.log('PASS: every affordable catalog button, 100+ delayed orders, exact payment, unique upgrades, independent food/lumber controls, save restoration and desktop/touch layout.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
