// Real app coverage for the walking companion, habitat map and crafting values.
// Local fixtures use encrypted practice saves; every external request is blocked.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),url='http://127.0.0.1:7777/game/monkey-trails.html';
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const fixtureName='Trail Practice',fixturePassword='practice-only-not-ranked';
const output=process.env.MONKEY_TRAILS_OUTPUT;if(output)fs.mkdirSync(output,{recursive:true});
const tab=(page,id)=>page.locator(`[data-action="tab"][data-id="${id}"]`);
const teamMonkey=(page,id)=>page.locator(`.team-strip [data-action="monkey-actions"][data-id="${id}"]`).first();
const activeChoice=(page,id)=>page.locator(`#dialog-content [data-action="active-monkey"][data-id="${id}"]`);
const percent=value=>String(Number((value*100).toFixed(2)))+'%';
const contrastColors=(foreground,background)=>{const luminance=color=>color.match(/[\d.]+/g).slice(0,3).map(Number).map(n=>n/255).map(n=>n<=.04045?n/12.92:((n+.055)/1.055)**2.4).reduce((total,n,i)=>total+n*[.2126,.7152,.0722][i],0),a=luminance(foreground),b=luminance(background);return(Math.max(a,b)+.05)/(Math.min(a,b)+.05);};

async function readSaved(page){return page.evaluate(async({name,password})=>{
  const {identity,derive,unseal}=await import('/game/monkey-trails/storage.mjs'),id=await identity(name,'practice'),envelope=JSON.parse(localStorage.getItem('monkey-trails.v1.'+id));
  if(!envelope)throw Error('Encrypted practice fixture missing.');
  return unseal(envelope,await derive(password,envelope.salt),id);
},{name:fixtureName,password:fixturePassword});}
async function waitSaved(page,predicate,label){const deadline=Date.now()+12000;let saved;while(Date.now()<deadline){saved=await readSaved(page);if(predicate(saved))return saved;await page.waitForTimeout(40);}throw Error('Save did not reach '+label+'; '+await page.locator('#save-status').innerText());}
async function openPractice(page){await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});}
async function closeDialog(page){await page.locator('#inspect-dialog .dialog-close').click();}
async function assertNoOverflow(page,label){const overflow=await page.evaluate(()=>document.documentElement.scrollWidth-innerWidth);assert.ok(overflow<=1,label+': '+overflow+'px horizontal overflow');}
async function capture(page,label,selector){if(output&&page.viewportSize().width===390){await page.locator('#toast').waitFor({state:'hidden',timeout:6000});await (selector?page.locator(selector):page).screenshot({path:path.join(output,`exploration-${label}-390.png`),...(selector?{}:{fullPage:false})});}}
async function checkMapReadability(page){
  const geometry=await page.evaluate(()=>{
    const summary=document.querySelector('.active-monkey-summary'),button=summary.querySelector('button'),color=getComputedStyle(button).color,background=getComputedStyle(summary).backgroundColor;
    const box=selector=>{const r=document.querySelector(selector).getBoundingClientRect();return{x:r.x,y:r.y,right:r.right,bottom:r.bottom};};
    return{color,background,coordinates:box('.map-location'),legend:box('.habitat-map-legend>rect')};
  });
  const contrast=contrastColors(geometry.color,geometry.background);
  assert.ok(contrast>=4.5,'Active monkey name and bonus have readable contrast: '+contrast.toFixed(2));
  const c=geometry.coordinates,l=geometry.legend;assert.ok(c.right<=l.x||c.x>=l.right||c.bottom<=l.y||c.y>=l.bottom,'Virtual coordinates do not cover habitat legend: '+JSON.stringify({c,l}));
  return Number(contrast.toFixed(2));
}

async function seedFixture(page){return page.evaluate(async({name,password})=>{
  const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),vault=new Vault(()=>{}),s=await vault.open(name,password,{create:true,species:'capuchin',mode:'practice'}),lead=s.run.monkey;
  const create=(id,species,nickname)=>{const p=structuredClone(lead);p.id=id;p.species=species;p.nickname=nickname;p.types=[...C.SPECIES[species].types];p.level=20;p.stage=0;p.training=100000;p.highestForm={stage:0,types:[...p.types]};p.learned=[];p.loadout=['strike','brace','breathe'];C.syncProgression(p);p.hp=C.stats(p).hp;p.energy=C.stats(p).maxEnergy;s.collection[id]=p;return p;};
  const rare=create('exploration-rare','rootstone','Grove Scout'),ultraId=Object.keys(C.SPECIES).find(id=>C.SPECIES[id].rarity==='ultra'),ultra=create('exploration-ultra',ultraId,'Crown Scout');
  const outside=create('exploration-sanctuary','echoquartz','Sanctuary Scout');
  C.setParty(s,[lead.id,rare.id,ultra.id]);s.run.explorerId=lead.id;s.run.fruit=10000;
  for(const id of Object.keys(C.ITEMS))s.run.items[id]=8;
  s.run.items.starOre=0;s.run.items.crownCore=0;s.run.items.ingot=4;s.run.items.charmMaterial=8;
  for(const element of Object.keys(C.ELEMENTS))s.run.essence[element]=8;
  await vault.checkpoint(s,true);if(!vault.localOK)throw Error('Encrypted fixture save failed.');
  return{leadId:lead.id,leadSpecies:lead.species,rareId:rare.id,rareSpecies:rare.species,ultraId:ultra.id,ultraSpecies:ultra.species,outsideId:outside.id,rareProfile:C.explorationProfile(rare),rareCost:C.trainingCost(rare),ultraCost:C.trainingCost(ultra)};
},{name:fixtureName,password:fixturePassword});}

async function checkActiveCompanion(page,fixture){
  const before=await readSaved(page);
  await tab(page,'team').click();assert.ok(await teamMonkey(page,fixture.rareId).locator('[data-exploration-badge]').count(),'Team shows a distinct exploration ability symbol');await teamMonkey(page,fixture.rareId).click();
  const choices=page.locator('.companion-quick-actions > button');assert.equal(await choices.count(),3,'Lineup monkey offers Swap, Details and Active monkey');
  assert.match(await activeChoice(page,fixture.rareId).innerText(),/active monkey/i);
  await activeChoice(page,fixture.rareId).click();
  let saved=await waitSaved(page,s=>s.run.explorerId===fixture.rareId,'rare walking companion');
  assert.equal(saved.run.activeId,before.run.activeId,'Walking choice preserves the battle starter');
  assert.deepEqual(saved.run.party,before.run.party,'Walking choice preserves lineup order');
  for(const id of before.run.party){assert.equal(saved.collection[id].hp,before.collection[id].hp,'Choice preserves HP');assert.equal(saved.collection[id].energy,before.collection[id].energy,'Choice preserves energy');}
  const selected=teamMonkey(page,fixture.rareId);assert.ok(await selected.evaluate(el=>el.classList.contains('exploration-active')),'Selected monkey has a distinct active roster border');await selected.hover();
  const teamColors=await selected.evaluate(el=>({color:getComputedStyle(el.querySelector(':scope>strong')).color,background:getComputedStyle(el).backgroundColor})),teamNameContrast=contrastColors(teamColors.color,teamColors.background);assert.ok(teamNameContrast>=4.5,'Active roster name stays readable when hovered: '+teamNameContrast.toFixed(2));
  await assertNoOverflow(page,'Team exploration badges');await capture(page,'team-badges','#view .team-strip');
  await tab(page,'walk').click();assert.equal(await page.locator('.land-traveler').getAttribute('data-monkey-id'),fixture.rareId);assert.equal(await page.locator('.land-traveler .monkey-art').getAttribute('data-species'),fixture.rareSpecies,'Map displays the selected walking monkey');
  assert.match(await page.locator('.active-monkey-summary').innerText(),/Grove Scout/,'Walk identifies the selected companion and its bonus');
  await page.locator('[data-action="map-step"][data-id="N"]').click();
  assert.equal(await page.locator('.land-traveler .monkey-art').getAttribute('data-species'),fixture.rareSpecies,'Walking animation keeps the selected portrait');
  await waitSaved(page,s=>s.run.distance>0&&s.run.explorerId===fixture.rareId,'accepted movement with walking choice');
  await page.reload();await openPractice(page);await tab(page,'walk').click();
  saved=await readSaved(page);assert.equal(saved.run.explorerId,fixture.rareId,'Walking companion persists in encrypted save');
  assert.equal(await page.locator('.land-traveler .monkey-art').getAttribute('data-species'),fixture.rareSpecies,'Reload restores map portrait');
  const activeSummaryContrast=await checkMapReadability(page);
  await capture(page,'active-map','.landscape');
  await tab(page,'sanctuary').click();const card=page.locator(`.collection-card[data-id="${fixture.rareId}"]`);assert.ok(await card.locator('[data-exploration-badge]').count(),'Roster shows exploration ability symbol');await card.click();
  assert.equal(await activeChoice(page,fixture.rareId).count(),1,'Roster action sheet also offers Active monkey');await closeDialog(page);
  await page.locator(`.collection-card[data-id="${fixture.outsideId}"]`).click();
  const outside=activeChoice(page,fixture.outsideId);assert.equal(await outside.count(),1,'Sanctuary monkey explains the walking requirement');assert.equal(await outside.isDisabled(),true,'Only active lineup companions can be selected');await closeDialog(page);
  await tab(page,'team').click();await teamMonkey(page,fixture.ultraId).click();await activeChoice(page,fixture.ultraId).click();
  await waitSaved(page,s=>s.run.explorerId===fixture.ultraId,'Ultra walking companion');await tab(page,'walk').click();
  assert.equal(await page.locator('.land-traveler .monkey-art').getAttribute('data-species'),fixture.ultraSpecies,'Ultra selection updates map immediately');
  return{independentBattleLead:true,encryptedReopen:true,teamAndRosterActions:true,activeSummaryContrast,teamNameContrast:Number(teamNameContrast.toFixed(2))};
}

async function checkTraitsAndTraining(page,fixture){
  await tab(page,'team').click();await teamMonkey(page,fixture.rareId).click();await page.locator(`[data-action="companion-details"][data-id="${fixture.rareId}"]`).click();
  const details=page.locator(`#view [data-exploration-profile][data-monkey-id="${fixture.rareId}"]`);await details.waitFor();
  const text=await details.innerText();assert.ok(text.includes(percent(fixture.rareProfile.forageChance)),'Details show the precise additional pickup chance');assert.ok(text.includes(percent(fixture.rareProfile.rareEncounterBonus)),'Details show the precise rare enemy weight bonus');assert.match(text,/ingot/i,'Earth monkey details explain its favored ingots');
  await capture(page,'traits',`#view [data-exploration-profile][data-monkey-id="${fixture.rareId}"]`);
  await page.locator('[data-action="companion-section"][data-id="train"]').click();
  const rareMaterials=page.locator(`[data-training-monkey="${fixture.rareId}"] [data-training-material]`);assert.ok(await rareMaterials.count(),'Late rare training displays special material requirements');assert.match(await rareMaterials.allTextContents().then(rows=>rows.join(' ')),/star ore/i);assert.equal(await page.locator(`[data-action="train"][data-id="${fixture.rareId}"]`).isDisabled(),true,'Missing Star Ore prevents late rare training even with enough fruit and XP');
  await teamMonkey(page,fixture.ultraId).click();await page.locator(`[data-action="companion-details"][data-id="${fixture.ultraId}"]`).click();await page.locator('[data-action="companion-section"][data-id="train"]').click();
  const ultraMaterials=await page.locator(`[data-training-monkey="${fixture.ultraId}"] [data-training-material]`).allTextContents();assert.match(ultraMaterials.join(' '),/crown core/i,'Late Ultra training names its exclusive Ultra victory material');assert.equal(await page.locator(`[data-action="train"][data-id="${fixture.ultraId}"]`).isDisabled(),true,'Missing Crown Core prevents late Ultra training');
  await assertNoOverflow(page,'Team details');return{preciseTraits:true,lateTrainingMaterials:true};
}

async function checkHabitats(page){
  await tab(page,'walk').click();const legend=page.locator('.habitat-map-legend');assert.equal(await legend.getAttribute('data-current-habitat'),'grove','Starting neighborhood is a broad Grove');const grovePatch=await legend.getAttribute('data-current-patch');
  assert.match(await legend.textContent(),/480 m habitat/,'Map states the larger habitat scale');assert.match(await legend.textContent(),/fruit.*Leaf essence/i,'Grove names its favored resources');
  assert.ok(await page.locator('.terrain-habitats .terrain-habitat[data-habitat-size="480"]').count(),'Map visibly draws broad persistent habitat areas');
  const weights=async()=>page.evaluate(async state=>{const H=await import('/game/monkey-trails/habitats.mjs'),E=await import('/game/monkey-trails/exploration.mjs');return{habitat:H.habitatAt(state),weights:E.trailResourceWeights(state,H.habitatAt(state))};},await readSaved(page));
  const grove=await weights();
  for(let i=0;i<32;i++)await page.locator('[data-action="map-step"][data-id="E"]').click();
  await waitSaved(page,s=>s.world.regions['emerald-jungle'].position.east===480,'walking into the cave neighborhood');
  assert.equal(await legend.getAttribute('data-current-habitat'),'cave','Walking across the neighborhood enters a cave formation');assert.match(await legend.textContent(),/ingots.*Earth essence/i,'Cave names its favored resources');
  const cave=await weights();assert.equal(cave.habitat.id,'cave','Resource lookup uses the same cave shown by the map');assert.ok(cave.weights['item:ingot']>grove.weights['item:ingot'],'Cave walking improves actual ingot odds');assert.ok(cave.weights.fruit<grove.weights.fruit,'Cave and Grove have different fruit odds');
  await checkMapReadability(page);await capture(page,'cave-map','.land-scene');
  const cavePatch=await legend.getAttribute('data-current-patch');await page.reload();await openPractice(page);await tab(page,'walk').click();assert.equal(await page.locator('.habitat-map-legend').getAttribute('data-current-patch'),cavePatch,'Habitat survives encrypted reopening');
  for(let i=0;i<32;i++)await page.locator('[data-action="map-step"][data-id="W"]').click();assert.equal(await page.locator('.habitat-map-legend').getAttribute('data-current-patch'),grovePatch,'Returning to the same neighborhood restores the original Grove');await assertNoOverflow(page,'Habitat map');
  return{habitatMeters:480,groveToCave:true,geographyPersists:true};
}

async function checkForgeValues(page){
  await tab(page,'pack').click();await page.locator('[data-action="pack-section"][data-id="forge"]').click();await page.locator('[data-forge-panel]').waitFor();
  const expected=await page.evaluate(async state=>(await import('/game/monkey-trails/forge.mjs')).forgeInventory(state).filter(row=>row.owned>0),await readSaved(page));
  assert.ok(expected.length>10,'Fixture exposes a variety of ingredient squares');
  for(const row of expected){const tile=page.locator(`[data-forge-item="${row.id}"]`),value=tile.locator('.forge-tile-value');assert.equal(await value.count(),1,row.name+' has its value on the square');assert.equal(await value.innerText(),`${row.value} power each`,row.name+' shows its actual crafting value');}
  await page.locator('[data-forge-search]').fill('Ingot');const ingot=page.locator('[data-forge-item="item:ingot"]');await ingot.scrollIntoViewIfNeeded();assert.equal(await ingot.locator('.forge-tile-value').innerText(),'5 power each');
  const geometry=await ingot.evaluate(el=>{const bounds=selector=>{const range=document.createRange();range.selectNodeContents(el.querySelector(selector));const r=range.getBoundingClientRect();return{x:r.x,y:r.y,right:r.right,bottom:r.bottom};},r=el.getBoundingClientRect();return{tile:{x:r.x,y:r.y,right:r.right,bottom:r.bottom},value:bounds('.forge-tile-value'),quantity:bounds('.forge-tile-quantity')};});
  assert.ok(geometry.value.x>=geometry.tile.x-1&&geometry.value.right<=geometry.tile.right+1&&geometry.value.bottom<=geometry.tile.bottom+1,'Ingot value remains inside its little square');
  const a=geometry.value,b=geometry.quantity;assert.ok(a.right<=b.x+1||a.x>=b.right-1||a.bottom<=b.y+1||a.y>=b.bottom-1,'Crafting value and quantity do not overlap: '+JSON.stringify(geometry));
  await assertNoOverflow(page,'Forge');
  await capture(page,'forge');
  return{ingredientValues:expected.length,ingotCraftingValue:5};
}

(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),reports=[];
  try{for(const width of [320,390,1280]){const context=await browser.newContext({viewport:{width,height:844},isMobile:width<700,hasTouch:width<700,reducedMotion:'reduce'});try{
    await context.route('**/*',async route=>{const request=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(request.pathname));if(request.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();return route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
    const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));await page.goto(url);await page.locator('#practice-button').waitFor();const fixture=await seedFixture(page);await openPractice(page);
    const active=await checkActiveCompanion(page,fixture),traits=await checkTraitsAndTraining(page,fixture),habitats=width===390?await checkHabitats(page):{},forge=await checkForgeValues(page);assert.deepEqual(errors,[],width+'px has no browser runtime errors');reports.push({width,...active,...traits,...habitats,...forge,pageErrors:errors.length});
  }finally{await context.close();}}}finally{await browser.close();}
  console.log(JSON.stringify(reports,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
