const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})});
 const reports=[];
 try{for(const scenario of ['walking','battle']){
  const context=await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true});
  await context.route('**/*',async route=>{const u=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(u.pathname));if(u.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');await page.locator('#practice-button').waitFor();
  const fixture=await page.evaluate(async scenario=>{
   const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs');
   const v=new Vault(()=>{}),s=await v.open('Trail Practice','practice-only-not-ranked',{create:true,species:'capuchin',mode:'practice'});
   for(let i=1;i<4;i++){const p=structuredClone(C.newSave('Fixture','macaque','practice',i).run.monkey);p.id=`fixture-${i}`;s.collection[p.id]=p;}
   C.setParty(s,[s.run.activeId,'fixture-1','fixture-2']);s.run.fruit=100;
   if(scenario==='battle'){C.advance(s,C.ENCOUNTER_STEP);C.readyBattle(s);s.run.battle.enemy.hp=1000;s.run.battle.enemy.energy=0;s.run.battle.enemyAction={type:'move',id:'breathe'};s.run.battle.intent='breathe';s.run.items.berry=3;s.run.items.revive=1;s.collection['fixture-1'].hp=0;}
   s.run.monkey.hp=C.playerStats(s).hp/2;
   const max=C.playerStats(s).hp,start=s.run.monkey.hp,id=s.run.activeId;await v.checkpoint(s,true);return{max,start,id};
  },scenario);
  await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
  if(scenario==='walking'){
   await page.locator('.practice-tools summary').click();for(let i=0;i<5;i++)await page.locator('[data-action="practice-step"][data-id="N"]').click();
   await page.locator('[data-action="tab"][data-id="team"]').click();
   assert.match(await page.locator('.training-row').innerText(),/25 XP available/);
   assert.match(await page.locator('.training-row').innerText(),/3 fruit \+ 22 XP/);
   const hp=Number(await page.locator('.companion-profile [aria-label="Health"]').getAttribute('aria-valuenow'));
   assert.equal(hp,Math.round(fixture.start+fixture.max*.125));
   await page.locator('[data-action="train"]').click();assert.match(await page.locator('.training-row').innerText(),/3 XP available/);
   await page.reload();await page.locator('#practice-button').click();await page.locator('[data-action="tab"][data-id="team"]').click();
   assert.match(await page.locator('.profile-art').innerText(),/Lv 2/);assert.match(await page.locator('.training-row').innerText(),/3 XP available/);
  }else{
   await page.locator('[data-action="ready"]').click();
   await page.locator('[data-action="battle-heal"]').click();await page.locator('[data-action="battle-item"][data-id="berry"]').click();
   assert.match(await page.locator('.battle-title .tag').innerText(),/ROUND 1/);
   assert.equal(await page.locator('[data-action="move"][data-id="strike"]').isEnabled(),true);
   assert.equal(await page.locator('[data-action="battle-heal"]').isDisabled(),true);
   await page.locator('[data-action="move"][data-id="strike"]').click();await page.locator('[data-action="battle-heal"]').waitFor();
   assert.match(await page.locator('.battle-title .tag').innerText(),/ROUND 2/);
   await page.waitForTimeout(700);
   await page.locator('[data-action="battle-heal"]').click();await page.locator('#battle-supply-target').selectOption('fixture-1');
   await page.locator('[data-action="battle-item"][data-id="revive"]').click();
   assert.match(await page.locator('.battle-title .tag').innerText(),/ROUND 2/);
   assert.equal(await page.locator('[data-action="move"][data-id="strike"]').isEnabled(),true);
   assert.ok(!/KO|Knocked out/.test(await page.locator('.team-slot[data-id="fixture-1"]').innerText()));
   await page.reload();await page.locator('#practice-button').click();await page.locator('[data-action="ready"]').click();
   assert.equal(await page.locator('[data-action="battle-heal"]').isDisabled(),true,'Reload must retain this round’s supply allowance');
   await page.locator('[data-action="move"][data-id="strike"]').click();assert.match(await page.locator('.battle-title .tag').innerText(),/ROUND 3/);
  }
  const size=await page.evaluate(()=>({w:innerWidth,s:document.documentElement.scrollWidth}));assert.ok(size.s<=size.w,'Mobile layout overflow');assert.deepEqual(errors,[]);reports.push({scenario,errors:0,overflow:0});await context.close();
 }}finally{await browser.close();}
 console.log(JSON.stringify(reports,null,2));
})().catch(e=>{console.error(e);process.exitCode=1;});
