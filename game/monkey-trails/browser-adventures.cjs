// Local routed integration checks. This script does not start a server or make
// production Firebase writes. Run with the same Playwright setup as browser-smoke.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
async function readState(page){return page.evaluate(()=>window.readAdventureState());}
async function saved(page,condition,arg){await page.waitForFunction(async({source,arg})=>{const s=await window.readAdventureState();return new Function('s','arg',`return (${source})(s,arg)`)(s,arg);},{source:condition.toString(),arg},{timeout:15000});return readState(page);}
async function openCompanionDetails(page){if(!await page.locator('.companion-detail-view').count()){await page.locator('#view .team-slot[data-action="monkey-actions"]').first().click();await page.locator('#dialog-content [data-action="companion-details"]').click();}}
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),reports=[];
 try{for(const scenario of ['welcome','pending','recap','evolution','lineup','discovery','lead','companion-actions']){
  const context=await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true});
  await context.route('**/*',async route=>{const u=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(u.pathname));if(u.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();return route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
  const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');await page.locator('#practice-button').waitFor();
  if(scenario==='welcome'){
   assert.equal(await page.locator('#welcome-setup').isVisible(),false);await page.locator('[data-action="welcome-continue"]').click();assert.equal(await page.locator('#welcome-setup').isVisible(),true);assert.equal(await page.locator('#starter-setup').isVisible(),false);await page.locator('#creature-name').fill('Kept Name');await page.locator('[data-action="welcome-new"]').click();assert.equal(await page.locator('#starter-setup').isVisible(),true);assert.equal(await page.locator('#creature-name').inputValue(),'Kept Name');assert.match(await page.locator('#save-form').innerText(),/There is no password reset/);
  }else{
   const fixture=await page.evaluate(async scenario=>{
    const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),v=new Vault(()=>{});let s=await v.open('Trail Practice','practice-only-not-ranked',{create:true,species:'capuchin',mode:'practice'});
    if(scenario==='discovery'){
     for(let seed=1;seed<1000;seed++){const candidate=C.newSave('Trail Practice','capuchin','practice',seed);C.advance(candidate,C.RESOURCE_STEP,0,C.RESOURCE_STEP);if(candidate.run.lastDiscovery?.category==='essence'){s=C.newSave('Trail Practice','capuchin','practice',seed);break;}}
    }
    if(scenario==='companion-actions'){
     for(let i=0;i<3;i++){const p=structuredClone(C.newSave('Bench',i===1?'langur':'macaque','practice',i+5).run.monkey);p.id=`action-${i}`;p.nickname=`Friend ${i}`;s.collection[p.id]=p;}
     C.setParty(s,[s.run.activeId,'action-0','action-1']);s.run.monkey.level=3;s.run.monkey.training=C.trainingCost(s.run.monkey).xp;s.run.fruit=100;s.run.essence.leaf=6;s.collection['action-0'].training=22;s.collection['action-2'].hp=9;
    }
    if(scenario==='lead'){
     for(let i=0;i<2;i++){const p=structuredClone(C.newSave('Bench',i?'macaque':'langur','practice',i+4).run.monkey);p.id=`lead-${i}`;s.collection[p.id]=p;}
     C.setParty(s,[s.run.activeId,'lead-0','lead-1']);s.collection['lead-0'].hp=17;s.collection['lead-1'].hp=0;
    }
    if(scenario==='pending'||scenario==='lead')C.advance(s,C.ENCOUNTER_STEP,0,C.ENCOUNTER_STEP);
    if(scenario==='lead'){s.collection['lead-0'].hp=17;for(const p of s.run.pendingEncounter.enemies){p.hp=999;p.energy=0;p.loadout=['breathe'];}}
    if(scenario==='evolution'){s.run.monkey.level=3;s.run.essence.leaf=99;s.run.monkey.hp=C.playerStats(s).hp;}
    await v.checkpoint(s,true);window.readAdventureState=async()=>{const reader=new Vault(()=>{});return reader.open('Trail Practice','practice-only-not-ranked',{mode:'practice'});};
    return {runId:s.run.id,pending:structuredClone(s.run.pendingEncounter),id:s.run.activeId,essence:s.run.essence.leaf};
   },scenario);
   await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
if(scenario==='companion-actions'){
    await page.locator('[data-action="tab"][data-id="team"]').click();assert.equal(await page.locator('.companion-controls').count(),0);
    assert.ok(await page.locator(`.team-slot[data-id="${fixture.id}"]`).evaluate(el=>el.classList.contains('is-evolution-ready')));assert.ok(await page.locator('.team-slot[data-id="action-0"]').evaluate(el=>el.classList.contains('is-training-ready')));
    await page.locator('.team-slot[data-id="action-0"]').click();assert.equal(await page.locator('.companion-quick-actions>button').count(),2);await page.locator('[data-action="companion-swap"]').click();await page.locator('[data-action="lineup-replace"][data-id="action-2"]').click();let s=await saved(page,s=>s.run.party.includes('action-2'));assert.equal(s.collection['action-2'].hp,9);assert.equal(s.run.party[1],'action-2');
    await page.locator('[data-action="tab"][data-id="sanctuary"]').click();assert.equal(await page.locator('.collection-card').count(),4);await page.locator('.collection-card[data-id="action-0"]').click();await page.locator('[data-action="companion-add"]').click();await page.locator('[data-action="lineup-replace"][data-outgoing="action-1"]').click();s=await saved(page,s=>s.run.party[2]==='action-0');assert.equal(s.collection['action-2'].hp,9);
    await page.locator('.collection-card[data-id="action-0"]').click();await page.locator('[data-action="companion-details"]').click();await page.locator('[data-action="train"]').click();s=await saved(page,s=>s.collection['action-0'].level===2);assert.equal(s.run.party.length,3);await page.locator('[data-action="companion-details-back"]').click();assert.equal(await page.locator('.collection-grid').count(),1);
   }else if(scenario==='lead'){
    await page.locator('[data-action="pending-begin"]').click();await page.locator('.battle-lead-picker').waitFor();assert.equal(await page.locator('.battle-lead-choices button').count(),3);assert.equal(await page.locator('[data-action="battle-lead"][data-id="lead-1"]').isDisabled(),true);
    await page.locator('[data-action="battle-lead"][data-id="lead-0"]').click();let s=await saved(page,s=>s.run.activeId==='lead-0');assert.equal(s.collection['lead-0'].hp,17);assert.equal(s.run.battle.round,1);assert.equal(s.run.battle.ready,false);
    await page.locator('[data-action="ready"]').click();s=await saved(page,s=>s.run.battle.openingLocked===true);assert.equal(s.run.battle.round,1);assert.equal(await page.locator('.battle-lead-picker').count(),0);
    await page.reload();await page.evaluate(async()=>{const{Vault}=await import('/game/monkey-trails/storage.mjs');window.readAdventureState=()=>new Vault(()=>{}).open('Trail Practice','practice-only-not-ranked',{mode:'practice'});});await page.locator('#practice-button').click();await page.locator('[data-action="ready"]').waitFor();assert.equal(await page.locator('.battle-lead-picker').count(),0);await page.locator('[data-action="ready"]').click();
    await page.locator('[data-action="switch-menu"]').click();await page.locator(`[data-action="switch-preview"][data-id="${fixture.id}"]`).click();await page.locator('[data-action="switch-confirm"]').click();s=await saved(page,s=>s.run.battle.round===2);assert.equal(s.run.activeId,fixture.id);
   }else if(scenario==='pending'){
    await page.locator('.pending-encounter').waitFor();await page.locator('.practice-tools summary').click();await page.locator('[data-action="practice-step"][data-id="N"]').click();let s=await saved(page,s=>s.run.distance>610);assert.equal(s.run.pendingEncounter.id,fixture.pending.id);assert.deepEqual(s.run.pendingEncounter.enemies,fixture.pending.enemies);assert.equal(s.run.phase,'trail');
    await page.reload();await page.evaluate(async()=>{const{Vault}=await import('/game/monkey-trails/storage.mjs');window.readAdventureState=()=>new Vault(()=>{}).open('Trail Practice','practice-only-not-ranked',{mode:'practice'});});await page.locator('#practice-button').click();await page.locator('.pending-encounter').waitFor();
    const before=await readState(page);await page.locator('[data-action="pending-pass"]').click();s=await saved(page,s=>!s.run.pendingEncounter);assert.equal(s.run.wins,before.run.wins);assert.equal(s.run.fruit,before.run.fruit);assert.deepEqual(s.run.essence,before.run.essence);assert.equal(Object.keys(s.collection).length,Object.keys(before.collection).length);assert.ok(Math.abs(s.run.nextEncounter-s.run.distance-609.6)<1e-6);
   }else if(scenario==='recap'){
    await page.locator('.practice-tools summary').click();await page.locator('[data-action="practice-step"][data-id="N"]').click();const before=await saved(page,s=>s.run.distance>0);await page.locator('[data-action="finish-walk"]').click();await page.locator('.recap-stats').waitFor();assert.match(await page.locator('#dialog-content').innerText(),/accepted feet/);const after=await readState(page);assert.equal(after.run.id,fixture.runId);assert.equal(after.run.distance,before.run.distance);assert.equal(after.run.phase,'trail');assert.deepEqual(after.run.party,before.run.party);assert.equal(after.meta.walkSessions.active,null);
   }else if(scenario==='evolution'){
    await page.locator('[data-action="tab"][data-id="team"]').click();await openCompanionDetails(page);await page.locator('.companion-sections [data-action="companion-section"][data-id="evolve"]').click();await page.locator('[data-action="evolution-preview"][data-id="leaf"]').click();await page.locator('[data-action="evolve"]').click();await page.locator('[data-reveal="evolution"]').waitFor();let s=await saved(page,(s,id)=>s.collection[id].stage===1,fixture.id);const amount=s.run.essence.leaf;assert.ok(amount<fixture.essence);assert.equal(await page.locator('.reveal-stat-grid>div').count(),5);await page.locator('[data-action="reveal-done"]').click();await saved(page,s=>s.meta.presentation.reveals.every(row=>row.seen));
    await page.reload();await page.evaluate(async()=>{const{Vault}=await import('/game/monkey-trails/storage.mjs');window.readAdventureState=()=>new Vault(()=>{}).open('Trail Practice','practice-only-not-ranked',{mode:'practice'});});await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});assert.equal(await page.locator('[data-reveal="evolution"]').count(),0);s=await readState(page);assert.equal(s.collection[fixture.id].stage,1);assert.equal(s.run.essence.leaf,amount);await page.locator('[data-action="settings"]').click();await page.locator('[data-action="activity-open"]').click();await page.locator('[data-action="reveal-replay"]').click();await page.locator('[data-reveal="evolution"]').waitFor();s=await readState(page);assert.equal(s.run.essence.leaf,amount);
   }else if(scenario==='lineup'){
    await page.locator('[data-action="tab"][data-id="team"]').click();await openCompanionDetails(page);await page.locator('.companion-lineups summary').click();await page.locator('#lineup-name').fill('Balanced Crew');await page.locator('[data-action="lineup-save"]').click();let s=await saved(page,s=>s.meta.companions?.lineups.length===1);const before=s.collection[fixture.id].hp;await page.locator('.companion-lineups summary').click();await page.locator('[data-action="lineup-apply"]').click();s=await readState(page);assert.deepEqual(s.run.party,[fixture.id]);assert.equal(s.collection[fixture.id].hp,before);
   }else if(scenario==='discovery'){
    await page.locator('.practice-tools summary').click();await page.locator('[data-action="practice-step"][data-id="N"]').click();await page.locator('#discovery-popup').waitFor({state:'visible'});assert.match(await page.locator('#discovery-popup').innerText(),/essence/);assert.equal(errors.length,0);await page.locator('[data-action="activity-open"]').click();assert.match(await page.locator('.trail-activity').innerText(),/essence/);
   }
  }
  const size=await page.evaluate(()=>({w:innerWidth,s:document.documentElement.scrollWidth}));assert.ok(size.s<=size.w,`${scenario}: phone layout overflow`);assert.deepEqual(errors,[],`${scenario}: browser errors`);reports.push({scenario,errors:0,overflow:0});await context.close();
 }}finally{await browser.close();}
 console.log(JSON.stringify(reports,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
