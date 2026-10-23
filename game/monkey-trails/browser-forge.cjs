// Reproducible browser checks against the real app and encrypted device vault.
// Run: node game/monkey-trails/browser-forge.cjs
// Optional: MONKEY_TRAILS_PLAYWRIGHT=/path/to/playwright
//           MONKEY_TRAILS_BROWSER_EXECUTABLE=/path/to/chromium
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),url='http://127.0.0.1:7777/game/monkey-trails.html';
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const fixtureName='Trail Practice',fixturePassword='practice-only-not-ranked';
const craftMix={fruit:8,'essence:fire':4,'item:revive':1};
const percent=value=>`${Number((value*100).toFixed(4))}%`;

async function readSaved(page){
  return page.evaluate(async({name,password})=>{
    const {identity,derive,unseal}=await import('/game/monkey-trails/storage.mjs');
    const id=await identity(name,'practice'),envelope=JSON.parse(localStorage.getItem('monkey-trails.v1.'+id));
    if(!envelope)throw Error('The encrypted fixture checkpoint is missing.');
    return unseal(envelope,await derive(password,envelope.salt),id);
  },{name:fixtureName,password:fixturePassword});
}
async function waitForSaved(page,predicate,label){
  let state;const deadline=Date.now()+12000;
  while(Date.now()<deadline){state=await readSaved(page);if(predicate(state))return state;await page.waitForTimeout(50);}
  throw Error(`Encrypted checkpoint did not reach ${label}; forge receipt: ${JSON.stringify(state?.meta?.forge||null)}`);
}
async function openPractice(page){
  await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
}
async function openPack(page){
  await page.locator('[data-action="tab"][data-id="pack"]').click();await page.locator('[data-forge-panel]').waitFor();
  await waitForSaved(page,state=>state.meta.interface?.tab==='pack','the Pack tab checkpoint');
}
async function addIngredient(page,id,count){
  await page.locator('#forge-resource').selectOption(id);await page.locator('#forge-amount').fill(String(count));
  await page.locator('[data-action="forge-add"]').click();
  assert.equal(await page.locator(`[data-forge-quantity="${id}"]`).inputValue(),String(count));
}
async function addCraftMix(page){for(const[id,count]of Object.entries(craftMix))await addIngredient(page,id,count);}
async function doubleClickRoll(page){
  // Two clicks in one event loop also cover the period while encryption is pending.
  await page.locator('[data-action="forge-roll"]').evaluate(button=>{button.click();button.click();});
}
async function waitForResult(page,pending=false){
  await page.waitForFunction(pending=>{
    const eyebrow=document.querySelector('[data-forge-result] .forge-result .eyebrow');
    return eyebrow?.textContent===(pending?'SAVE PENDING':'LAST CRAFT · SAVED');
  },pending);
}
async function preview(page,mix){
  const state=await readSaved(page);
  return page.evaluate(async({state,mix})=>(await import('/game/monkey-trails/forge.mjs')).previewForge(state,mix),{state,mix});
}
async function assertPreview(page,mix){
  const expected=await preview(page,mix);
  assert.deepEqual(await page.locator('[data-forge-preview] .forge-odds-row > strong').allTextContents(),expected.rarityOdds.map(row=>percent(row.chance)));
  const elements=await page.locator('[data-forge-preview] .forge-elements > span').allTextContents();
  for(const[index,row]of expected.elementOdds.entries())assert.ok(elements[index].endsWith(' '+percent(row.chance)),`${row.name} odds match the actual ticket allocation`);
  const displayedTotal=(await page.locator('[data-forge-preview] .forge-odds-row > strong').allTextContents()).reduce((sum,text)=>sum+Number(text.replace('%','')),0);
  assert.ok(Math.abs(displayedTotal-100)<1e-8,'Displayed rarity odds total exactly 100%');
  const spend=await page.locator('[data-forge-preview] .forge-spend').innerText();
  for(const row of expected.costSummary)assert.ok(spend.includes(`${row.count} ${row.name}`),'The preview states the actual charged ingredients');
  return expected;
}
async function predictCraft(page){
  const state=await readSaved(page);
  return page.evaluate(async({state,mix})=>{
    const F=await import('/game/monkey-trails/forge.mjs'),before=structuredClone(state);const receipt=F.craftForge(state,mix,F.nextForgeOperationId(state));return{before,state,receipt};
  },{state,mix:craftMix});
}
function resourceAmount(state,id){
  if(id==='fruit')return state.run.fruit;
  const split=id.indexOf(':'),kind=id.slice(0,split),key=id.slice(split+1);
  return(kind==='essence'?state.run.essence:kind==='item'?state.run.items:kind==='fragment'?state.meta.fragments:state.meta.bondMaterials)[key]||0;
}
function assertPaidCosts(before,after,receipt){
  for(const row of receipt.costSummary){
    assert.equal(resourceAmount(before,row.id)-resourceAmount(after,row.id),row.count,`${row.name}: spend the receipt's charged quantity once`);
    assert.equal(receipt.costInputs[row.id],row.count);
    assert.equal(row.count,receipt.campBonus?Math.ceil(receipt.inputs[row.id]*4/5):receipt.inputs[row.id]);
  }
}
function assertCraftState(actual,expected){
  assert.deepEqual(actual.meta.forge,expected.meta.forge,'The persisted receipt is the one originally rolled');
  assert.deepEqual(actual.meta.gear,expected.meta.gear,'One duplicate adds exactly one equipment rank');
  assert.deepEqual(actual.meta.equipmentCopies,expected.meta.equipmentCopies,'Only the eligible individual duplicate is upgraded');
  assert.equal(actual.rng,expected.rng,'The craft consumes exactly one RNG transition');
  assert.equal(actual.run.fruit,expected.run.fruit);assert.deepEqual(actual.run.essence,expected.run.essence);
  assert.deepEqual(actual.run.items,expected.run.items);assert.deepEqual(actual.meta.fragments,expected.meta.fragments);
}
async function seedFixture(page,scenario){
  return page.evaluate(async({scenario,name,password})=>{
    const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs');
    const vault=new Vault(()=>{}),state=await vault.open(name,password,{create:true,species:'capuchin',mode:'practice'});
    state.rng=765;state.run.fruit=100;for(const element of Object.keys(C.ELEMENTS))state.run.essence[element]=25;
    state.run.items.revive=3;state.meta.fragments.worldroot=5;
    const reserve=structuredClone(C.newSave('Reserve','macaque','practice',11).run.monkey);reserve.id='forge-reserve';reserve.nickname='Reserve';state.collection[reserve.id]=reserve;
    C.setParty(state,[state.run.activeId,reserve.id]);
    // Any roll is a known duplicate, making the exact tier increase deterministic.
    for(const id of Object.keys(C.GEAR))state.meta.gear[id]=1;
    C.equip(state,'heart',state.run.activeId);state.run.monkey.hp=10;reserve.hp=12;
    if(scenario!=='odds-layout'){state.run.phase='camp';state.run.camp={heals:2,seedBoost:false};}
    await vault.checkpoint(state,true);if(!vault.localOK)throw Error('Could not save the browser fixture.');
    return{lead:state.run.activeId,reserve:reserve.id,leadHp:10,reserveHp:12};
  },{scenario,name:fixtureName,password:fixturePassword});
}

async function checkOddsAndLayout(page){
  const layout=await page.evaluate(()=>{
    const main=document.querySelector('.walk-main'),scene=document.querySelector('.land-scene');
    return{trackingFirst:main?.firstElementChild?.classList.contains('tracking-controls'),trackingBeforeScene:!!(document.querySelector('.tracking-controls')?.compareDocumentPosition(scene)&Node.DOCUMENT_POSITION_FOLLOWING),discoveryAfterScene:scene?.nextElementSibling?.classList.contains('discovery'),legends:document.querySelectorAll('.upgrade-legend,.readiness-legend').length,readyBadges:document.querySelectorAll('.upgrade-ready-badge').length};
  });
  assert.deepEqual(layout,{trackingFirst:true,trackingBeforeScene:true,discoveryAfterScene:true,legends:0,readyBadges:0});
  for(const tab of ['team','sanctuary']){
    await page.locator(`[data-action="tab"][data-id="${tab}"]`).click();
    assert.equal(await page.locator('.upgrade-legend,.readiness-legend').count(),0,`${tab}: no unconditional readiness legend`);
    assert.equal(await page.locator('.upgrade-ready-badge').count(),0,`${tab}: an unavailable upgrade has no ready badge`);
  }
  await openPack(page);assert.equal(await page.locator('[data-action="forge-roll"]').isDisabled(),true);
  await addIngredient(page,'fruit',8);const plain=await assertPreview(page,{fruit:8});
  assert.equal(await page.locator('[data-action="forge-roll"]').isEnabled(),true,'A valid mix can be crafted directly on the trail');
  assert.match(await page.locator('[data-forge-reason]').innerText(),/Ready: one guaranteed equipment roll/);
  assert.equal(await page.locator('.forge-camp-bonus').count(),0);
  await addIngredient(page,'item:revive',1);const rare=await assertPreview(page,{fruit:8,'item:revive':1});
  assert.ok(rare.rarityOdds.find(row=>row.id==='legendary').chance>plain.rarityOdds.find(row=>row.id==='legendary').chance,'Rarer resources improve the displayed legendary odds');
  await addIngredient(page,'essence:fire',4);const elemental=await assertPreview(page,craftMix);
  assert.ok(elemental.elementOdds.find(row=>row.id==='fire').chance>rare.elementOdds.find(row=>row.id==='fire').chance,'Adding fire essence raises fire-item odds');
  assert.ok(elemental.elementOdds.find(row=>row.id==='fire').chance>elemental.elementOdds.find(row=>row.id==='water').chance);
  await page.locator('[data-action="forge-odds"]').click();
  assert.match(await page.locator('.forge-item-odds > .muted').innerText(),new RegExp(`${elemental.outcomes.length} eligible items share 100%`));
  assert.equal(elemental.rarityOdds.length,6,'All six rarity probabilities are displayed');
  assert.equal(await page.locator('.forge-outcome').count(),12,'The full catalogue uses compact paginated tiles');
  const names='.forge-outcome button > span:not(.forge-gear-icon)',firstNames=await page.locator(names).allTextContents();await page.locator('[data-action="forge-odds-page"][data-id="1"]').click();
  assert.notDeepEqual(await page.locator(names).allTextContents(),firstNames);
  await page.locator('[data-odds-filter="rarity"]').selectOption('rare');await page.locator('[data-odds-filter="element"]').selectOption('fire');
  const expected=elemental.outcomes.filter(row=>row.rarity==='rare'&&row.element==='fire').sort((a,b)=>b.chance-a.chance||a.name.localeCompare(b.name));
  assert.deepEqual(await page.locator(names).allTextContents(),expected.map(row=>row.name));
  assert.deepEqual(await page.locator('.forge-outcome button > strong').allTextContents(),expected.map(row=>percent(row.chance)));
  for(let index=0;index<expected.length;index++){
    await page.locator('.forge-outcome [data-action="inventory-item"]').nth(index).click();assert.equal(await page.locator('.equipment-detail-heading h2').innerText(),expected[index].name);assert.ok((await page.locator('.equipment-description').innerText()).length>10,'Possible items open their full real effects');await page.locator('[data-action="inventory-back-odds"]').click();
  }
  await page.locator('#dialog-content [data-action="close-dialog"]').click();
  const predicted=await predictCraft(page);assert.equal(predicted.receipt.campBonus,false);
  await doubleClickRoll(page);await waitForResult(page);
  const saved=await waitForSaved(page,state=>state.meta.forge?.serial===1,'the trail craft');
  assertCraftState(saved,predicted.state);assertPaidCosts(predicted.before,saved,predicted.receipt);
}

async function checkCraftAndTransfer(page,fixture){
  await openPack(page);await addCraftMix(page);const camp=await assertPreview(page,craftMix);
  assert.match(await page.locator('.forge-camp-bonus').innerText(),/20% ingredient discount/);
  const trail=await page.evaluate(async({state,mix})=>{state.run.phase='trail';return(await import('/game/monkey-trails/forge.mjs')).previewForge(state,mix);},{state:await readSaved(page),mix:craftMix});
  for(const rarity of ['rare','legendary'])assert.ok(camp.rarityOdds.find(row=>row.id===rarity).chance>trail.rarityOdds.find(row=>row.id===rarity).chance,'Camp improves the same mix’s rare and legendary odds');
  const predicted=await predictCraft(page);assert.equal(predicted.receipt.campBonus,true);
  await doubleClickRoll(page);await waitForResult(page);
  let saved=await waitForSaved(page,state=>state.meta.forge?.serial===1,'the first craft');assertCraftState(saved,predicted.state);assertPaidCosts(predicted.before,saved,predicted.receipt);
  assert.equal(saved.meta.forge.last.kind,'upgraded');assert.equal(saved.meta.forge.last.tier,2);
  assert.equal(await page.locator('[data-forge-quantity]').count(),0);assert.equal(await page.locator('[data-action="forge-roll"]').isDisabled(),true);
  await page.reload();await openPractice(page);await openPack(page);await waitForResult(page);
  saved=await readSaved(page);assertCraftState(saved,predicted.state);
  assert.equal(await page.locator('.forge-result h3').innerText(),predicted.receipt.name);
  assert.match(await page.locator('.forge-result .equipment-slot-heading small').innerText(),/Rank 2/);
  await page.locator('.pack-equipment-current > summary').click();await page.locator('#gear-target').selectOption(fixture.reserve);await page.locator('.pack-equipment-current > summary').click();
  await page.locator('#view [data-action="gear-picker"][data-id="charm"]').click();
  await page.locator('[data-gear-filter="element"]').selectOption('leaf');await page.locator('[data-gear-filter="rarity"]').selectOption('common');
  await page.locator('#dialog-content [data-action="inventory-item"][data-id="heart"]').click();const move=page.locator('#dialog-content [data-action="inventory-equip"]');assert.match(await move.innerText(),/Move from/);await move.click();
  saved=await waitForSaved(page,state=>state.collection[fixture.reserve].equipment.charm==='heart','the new equipment owner');
  assert.equal(saved.collection[fixture.lead].equipment.charm,null);assert.equal(saved.meta.equipped.charm,null);
  assert.equal(saved.collection[fixture.lead].hp,fixture.leadHp);assert.equal(saved.collection[fixture.reserve].hp,fixture.reserveHp);
  assert.equal(Object.values(saved.collection).filter(p=>Object.values(p.equipment).includes('heart')).length,1);
  assert.deepEqual(saved.meta.forge,predicted.state.meta.forge,'Moving gear does not create another craft');
  await page.locator('#dialog-content [data-action="close-dialog"]').click();
  await page.reload();await openPractice(page);saved=await readSaved(page);
  assert.equal(saved.collection[fixture.reserve].equipment.charm,'heart');assert.equal(saved.collection[fixture.reserve].hp,fixture.reserveHp);
}

async function checkFailedSaveRetry(page){
  await openPack(page);await addCraftMix(page);const before=await readSaved(page),predicted=await predictCraft(page);
  await page.evaluate(async()=>{
    const {Vault}=await import('/game/monkey-trails/storage.mjs'),original=Vault.prototype.put;
    window.__forgeFailDeviceSave=true;window.__forgeFailedWrites=0;
    Vault.prototype.put=function(...args){
      if(this.mode==='practice'&&window.__forgeFailDeviceSave){this.localOK=false;window.__forgeFailedWrites++;this.status('Device save failed: injected browser test failure.','error');return;}
      return original.apply(this,args);
    };
  });
  await doubleClickRoll(page);await waitForResult(page,true);
  assert.ok(await page.evaluate(()=>window.__forgeFailedWrites)>0,'The real checkpoint attempted the injected device write');
  assert.match(await page.locator('[data-action="forge-roll"]').innerText(),/Retry saving crafted item/);
  assert.equal(await page.locator('[data-action="forge-add"]').isDisabled(),true);
  assert.equal(await page.locator('.forge-result [data-equip-start="true"]').isDisabled(),true);
  assert.equal(await page.locator('.forge-result h3').innerText(),predicted.receipt.name);
  const unsaved=await readSaved(page);assert.deepEqual(unsaved.meta.forge,before.meta.forge);assert.deepEqual(unsaved.meta.gear,before.meta.gear);
  assert.equal(unsaved.run.fruit,before.run.fruit);assert.deepEqual(unsaved.run.essence,before.run.essence);assert.deepEqual(unsaved.run.items,before.run.items);
  await page.evaluate(()=>{window.__forgeFailDeviceSave=false;});await doubleClickRoll(page);await waitForResult(page);
  const saved=await waitForSaved(page,state=>state.meta.forge?.serial===1,'the retried craft');assertCraftState(saved,predicted.state);assertPaidCosts(predicted.before,saved,predicted.receipt);
  assert.equal(saved.meta.forge.last.operationId,'forge-1');assert.equal(saved.meta.forge.last.tier,2);
  await page.reload();await openPractice(page);await openPack(page);await waitForResult(page);
  assertCraftState(await readSaved(page),predicted.state);assert.equal(await page.locator('[data-action="forge-roll"]').isDisabled(),true);
}

(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),reports=[];
  try{for(const scenario of ['odds-layout','craft-equipment','failed-save-retry']){
    const context=await browser.newContext({viewport:{width:scenario==='odds-layout'?320:390,height:844},isMobile:true,hasTouch:true});
    try{
      await context.route('**/*',async route=>{
        const request=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(request.pathname));
        if(request.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();
        await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});
      });
      const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
      await page.goto(url);await page.locator('#practice-button').waitFor();const fixture=await seedFixture(page,scenario);await openPractice(page);
      if(scenario==='odds-layout')await checkOddsAndLayout(page);
      else if(scenario==='craft-equipment')await checkCraftAndTransfer(page,fixture);
      else await checkFailedSaveRetry(page);
      const size=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));
      assert.ok(size.scroll<=size.width,`${scenario}: mobile layout overflow`);assert.deepEqual(errors,[],`${scenario}: browser runtime errors`);
      reports.push({scenario,errors:0,overflow:0});
    }finally{await context.close();}
  }}finally{await browser.close();}
  console.log(JSON.stringify(reports,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
