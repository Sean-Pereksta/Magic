// Exercises the real static app, including saved fixture reloads and manual claims.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),origin='http://127.0.0.1:7777',out=process.env.MONKEY_TRAILS_OUTPUT;
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
async function client(browser,scenario){
 const context=await browser.newContext({viewport:{width:320,height:900},isMobile:true,hasTouch:true});
 await context.route('**/*',async route=>{
  const u=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(u.pathname));
  if(u.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();
  let body=fs.readFileSync(file);
  if(u.pathname==='/game/monkey-trails/app.mjs')body=body.toString()+'\nglobalThis.__balanceDebug={get state(){return state;},render,renderGPS,checkpoint};\n';
  await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body});
 });
 const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
 await page.goto(origin+'/game/monkey-trails.html');await page.locator('#practice-button').waitFor();
 const fixture=await page.evaluate(async scenario=>{
  const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs');
  const v=new Vault(()=>{}),s=await v.open('Trail Practice','practice-only-not-ranked',{create:true,species:'capuchin',mode:'practice'});
  let info={};
  if(scenario==='strong'||scenario==='resisted'){
   C.advance(s,C.ENCOUNTER_STEP);C.beginPendingEncounter(s);C.readyBattle(s);const p=s.run.monkey,b=s.run.battle,e=b.enemy;
   p.level=8;p.stage=1;p.types=['fire'];p.learned.push('cinder');p.loadout=['strike','cinder','brace','breathe'];p.hp=C.stats(p).hp;p.energy=C.stats(p).maxEnergy;
   e.species='mossmop';e.types=[scenario==='strong'?'leaf':'fire'];e.level=1;e.stage=0;e.hp=1000;e.energy=12;e.loadout=['strike','brace','breathe'];b.enemyAction={type:'move',id:'strike'};b.intent='strike';s.rng=48217;
  }else if(scenario==='evolution'){
   const p=s.run.monkey;p.level=3;p.hp=C.stats(p).hp;s.run.essence.leaf=3;info={before:C.stats(p)};
  }else{
   const Q=await import('/game/monkey-trails/challenges.mjs');
   Q.normalizeChallenges(s);const challenge=Q.regionChallenges(s,'emerald-jungle').find(q=>q.kind==='walk'||q.metric==='distance'||q.type==='distance'||q.kind==='distance');
   if(!challenge)throw Error('The initial region has no walking challenge.');
   const progress=challenge.goal-15;s.world.totalDistance=progress;s.world.regions['emerald-jungle'].distance=progress;
   Q.trackChallenge(s,null);info={id:challenge.id,goal:challenge.goal,reward:challenge.reward};
  }
  await v.checkpoint(s,true);return info;
 },scenario);
 await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
 return{page,context,errors,fixture};
}
async function clean(c){const box=await c.page.evaluate(()=>({w:innerWidth,s:document.documentElement.scrollWidth}));assert.ok(box.s<=box.w,'No horizontal overflow at320px');assert.deepEqual(c.errors,[]);}
async function screenshot(page,name){if(out){fs.mkdirSync(out,{recursive:true});await page.screenshot({path:path.join(out,name+'.png'),fullPage:true});}}
async function openCompanionDetails(page){if(!await page.locator('.companion-detail-view').count()){await page.locator('#view .team-slot[data-action="monkey-actions"]').first().click();await page.locator('#dialog-content [data-action="companion-details"]').click();}}
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
 try{
  for(const scenario of ['strong','resisted']){
   const c=await client(browser,scenario),p=c.page;
   await p.locator('[data-action="ready"]').click();const badge=p.locator('[data-action="move"][data-id="cinder"] .effectiveness-badge');
   assert.equal(await badge.getAttribute('data-effectiveness'),scenario);assert.match(await badge.innerText(),scenario==='strong'?/Very effective/i:/Not effective/i);
   assert.equal(await p.locator('[data-action="move"][data-id="brace"] .effectiveness-badge').count(),0,'Support has no damage matchup badge');
   const before=await p.evaluate(()=>__balanceDebug.state.run.battle.enemy.hp);
   await p.locator('[data-action="move"][data-id="cinder"]').click();await p.waitForTimeout(700);
   const result=p.locator('.battle-feedback [data-hit-side="player"]');await result.waitFor();
   assert.equal(await result.getAttribute('data-effectiveness'),scenario);assert.match(await result.innerText(),scenario==='strong'?/Very effective/i:/Not effective/i);
   const hit=await p.evaluate(()=>({hp:__balanceDebug.state.run.battle.enemy.hp,hits:__balanceDebug.state.run.battle.lastHits,last:__balanceDebug.state.run.battle.lastHit}));
   assert.ok(before>hit.hp,'The typed attack actually damages the opponent');
   assert.equal(await p.locator('.battle-feedback [data-hit-side="enemy"]').count(),1,'Player feedback survives the responding enemy turn');
   await screenshot(p,'monkey-balance-'+scenario);await clean(c);report.push({scenario,errors:0,overflow:0});await c.context.close();
  }
  {
   const c=await client(browser,'evolution'),p=c.page;
   await p.locator('[data-action="tab"][data-id="team"]').click();await openCompanionDetails(p);await p.locator('[data-action="companion-section"][data-id="evolve"]').click();const offer=p.locator('[data-action="evolution-preview"][data-id="leaf"]'),goal=offer.locator('.essence-goal[data-essence-type="leaf"]');
   assert.equal(await goal.locator('[data-essence-current]').innerText(),'3');assert.equal(await goal.locator('[data-essence-required]').innerText(),'6');assert.equal(await goal.locator('[role="progressbar"]').getAttribute('aria-valuenow'),'3');
   await offer.click();const commit=p.locator('[data-action="evolve"][data-id="leaf"]');assert.equal(await commit.isDisabled(),true);
   for(const stat of ['hp','atk','def','speed','maxEnergy'])assert.ok(await p.locator('.evolution-stat-gains [data-stat="'+stat+'"]').count(),'Preview includes '+stat);
   await screenshot(p,'monkey-balance-evolution');
   await p.locator('.dialog-close').click();await p.evaluate(()=>{__balanceDebug.state.run.essence.leaf=6;__balanceDebug.render();});
   await p.locator('[data-action="evolution-preview"][data-id="leaf"]').click();assert.equal(await p.locator('[data-action="evolve"][data-id="leaf"]').isEnabled(),true);await p.locator('[data-action="evolve"][data-id="leaf"]').click();
   const evolved=await p.evaluate(async()=>{const C=await import('/game/monkey-trails/core.mjs'),s=__balanceDebug.state;await __balanceDebug.checkpoint(true);return{stage:s.run.monkey.stage,essence:s.run.essence.leaf,stats:C.playerStats(s)};});
   assert.equal(evolved.stage,1);assert.equal(evolved.essence,0);for(const stat of ['hp','atk','def','speed','maxEnergy'])assert.ok(evolved.stats[stat]>c.fixture.before[stat],stat+' improves on evolution');
   await p.reload();await p.locator('#practice-button').click();await p.locator('#game').waitFor({state:'visible'});assert.equal(await p.evaluate(()=>__balanceDebug.state.run.monkey.stage),1);await clean(c);report.push({scenario:'evolution',errors:0,overflow:0});await c.context.close();
  }
  {
   const c=await client(browser,'challenges'),p=c.page,id=c.fixture.id;
   await p.locator('[data-action="lands"]').first().click();await p.locator('.land-atlas [data-id="emerald-jungle"]').click();
   const row=p.locator('.region-challenge[data-challenge-id="'+id+'"]');await row.waitFor();const claim=row.locator('[data-action="challenge-claim"]');assert.equal(await claim.isDisabled(),true);
   await row.locator('[data-action="challenge-track"]').click();await p.locator('.dialog-close').click();await p.locator('.tracked-region-challenge[data-challenge-id="'+id+'"]').waitFor();
   const before=await p.evaluate(()=>({fruit:__balanceDebug.state.run.fruit,items:structuredClone(__balanceDebug.state.run.items)}));
   await p.locator('[data-action="map-step"][data-id="N"]').click();
   const tracked=p.locator('.tracked-region-challenge[data-challenge-id="'+id+'"]');assert.equal(await tracked.locator('[role="progressbar"]').getAttribute('aria-valuenow'),String(c.fixture.goal));
   assert.equal(await tracked.locator('.challenge-meter i').evaluate(el=>el.style.width),'100%');
   const unclaimed=await p.evaluate(()=>({fruit:__balanceDebug.state.run.fruit,items:structuredClone(__balanceDebug.state.run.items)}));assert.deepEqual(unclaimed,before,'Completing a challenge does not grant its reward before Claim');
   await p.locator('[data-action="challenge-open"]').first().click();const completed=p.locator('.region-challenge[data-challenge-id="'+id+'"]');assert.equal(await completed.locator('[data-action="challenge-claim"]').isEnabled(),true);
   await screenshot(p,'monkey-balance-challenge');await completed.locator('[data-action="challenge-claim"]').click();
   const after=await p.evaluate(()=>({fruit:__balanceDebug.state.run.fruit,items:structuredClone(__balanceDebug.state.run.items)}));assert.equal(after.fruit-before.fruit,c.fixture.reward.fruit||0);for(const [item,n]of Object.entries(c.fixture.reward.items||{}))assert.equal(after.items[item]-before.items[item],n);
   await p.waitForFunction(id=>__balanceDebug.state.meta.regionChallenges.claimed.includes(id)&&[...document.querySelectorAll('.region-challenge')].some(el=>el.dataset.challengeId===id&&el.classList.contains('is-claimed')),id);
   const again=p.locator('.region-challenge[data-challenge-id="'+id+'"] [data-action="challenge-claim"]');assert.equal(await again.isDisabled(),true,'Claimed reward cannot be claimed twice');
   const duplicate=await p.evaluate(async id=>{const {claimChallenge}=await import('/game/monkey-trails/challenges.mjs'),s=__balanceDebug.state,before=JSON.stringify({fruit:s.run.fruit,items:s.run.items});let rejected=false;try{claimChallenge(s,id);}catch(error){rejected=/already claimed/.test(error.message);}return{rejected,unchanged:before===JSON.stringify({fruit:s.run.fruit,items:s.run.items})};},id);
   assert.deepEqual(duplicate,{rejected:true,unchanged:true},'Duplicate reward execution is rejected without changing inventory');
   await p.locator('.dialog-close').click();await p.evaluate(()=>__balanceDebug.checkpoint(true));await p.reload();await p.locator('#practice-button').click();await p.locator('#game').waitFor({state:'visible'});
   const persisted=await p.evaluate(()=>({fruit:__balanceDebug.state.run.fruit,items:structuredClone(__balanceDebug.state.run.items)}));assert.deepEqual(persisted,after,'Claimed inventory survives reload');await clean(c);report.push({scenario:'regional challenge',errors:0,overflow:0});await c.context.close();
  }
 }finally{await browser.close();}
 console.log(JSON.stringify(report,null,2));
})().catch(e=>{console.error(e);process.exitCode=1;});
