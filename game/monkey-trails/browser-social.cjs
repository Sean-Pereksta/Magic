// Two independent browser clients share a test-only transactional document service.
// Production app modules are routed from disk; only the connection injection is patched.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),origin='http://127.0.0.1:7777';
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const clone=x=>x==null?x:structuredClone(x),docs=new Map();let revision=0;
function matches(d,filters){return filters.every(([key,op,value])=>op==='array-contains'?Array.isArray(d[key])&&d[key].includes(value):op==='=='?d[key]===value:op==='>'?d[key]>value:op==='<'?d[key]<value:false);}
function rows(prefix,filters=[],limit=100,order=[]){let result=[...docs].filter(([p,d])=>p.startsWith(prefix+'/')&&!p.slice(prefix.length+1).includes('/')&&matches(d,filters));const field=(d,key)=>key.split('.').reduce((v,k)=>v?.[k],d);if(order.length)result.sort(([ap,a],[bp,b])=>{for(const [key,direction]of order){const x=field(a,key),y=field(b,key),delta=x<y?-1:x>y?1:0;if(delta)return direction==='desc'?-delta:delta;}return ap.localeCompare(bp)*(order.at(-1)[1]==='desc'?-1:1);});return result.slice(0,limit==null?undefined:limit).map(([p,d])=>({id:p.split('/').at(-1),...clone(d)}));}
function rpc({method,path,filters,limit,order,base,writes}){
 if(method==='get')return clone(docs.get(path)||null);
 if(method==='list')return rows(path,filters,limit,order);
 if(method==='snapshot')return {revision,docs:Object.fromEntries([...docs].map(([p,d])=>[p,clone(d)]))};
 if(method==='commit'){
  if(base!==revision)return false;
  for(const [type,p,d]of writes){if(type==='delete')docs.delete(p);else docs.set(p,clone(d));}
  if(writes.length)revision++;return true;
 }
 throw Error('Unknown test RPC');
}
async function openClient(browser,{uid,name,width=390,seed=7121,existing=false}){
 const context=await browser.newContext({viewport:{width,height:844},isMobile:true,hasTouch:true});
 await context.exposeFunction('__socialRPC',rpc);
 await context.addInitScript(({uid})=>{
  const clone=x=>x==null?x:structuredClone(x);
  const adapter={
   get:path=>window.__socialRPC({method:'get',path}),
   list:(path,filters=[],limit=100,order=[])=>window.__socialRPC({method:'list',path,filters,limit,order}),
   async transaction(callback){for(let tries=0;tries<30;tries++){
    const snapshot=await window.__socialRPC({method:'snapshot'}),values=new Map(Object.entries(snapshot.docs)),writes=[];
    const result=await callback({get:async p=>clone(values.get(p)||null),set(p,d){values.set(p,clone(d));writes.push(['set',p,clone(d)]);},delete(p){values.delete(p);writes.push(['delete',p]);}});
    if(await window.__socialRPC({method:'commit',base:snapshot.revision,writes}))return clone(result);
   }throw Error('Test transaction contention');},
   watchDoc(path,fn,error){return watch(()=>adapter.get(path),fn,error);},
   watchQuery(path,filters,fn,error){return watch(()=>adapter.list(path,filters),fn,error);}
  };
  function watch(read,fn,error){let active=true,running=false,last;
   const run=async()=>{if(!active||running)return;running=true;try{const value=await read(),raw=JSON.stringify(value);if(active&&raw!==last){last=raw;fn(value);}}catch(e){if(active)error?.(e);}finally{running=false;}};
   const timer=setInterval(run,100);void run();return()=>{active=false;clearInterval(timer);};
  }
  window.__socialTestConnection=async()=>({uid,adapter});
  // A deterministic phone fix is only returned after the opt-in checkbox is used.
  Object.defineProperty(navigator,'geolocation',{configurable:true,value:{getCurrentPosition(success){success({coords:{latitude:40,longitude:-74,accuracy:8},timestamp:Date.now()});},watchPosition(){return 1;},clearWatch(){}}});
 },{uid});
 await context.route('**/*',async route=>{
  const u=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(u.pathname));
  if(u.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();
  let body=fs.readFileSync(file);
  if(u.pathname==='/game/monkey-trails/app.mjs'){
   let source=body.toString();assert.ok(source.includes('new SocialController({'),'App social controller integration is missing');
   source=source.replace('new SocialController({','new SocialController({connect:()=>globalThis.__socialTestConnection(),');
   source+='\nglobalThis.__socialDebug={social,render:()=>render(),get state(){return state;},get vault(){return vault;}};\n';body=source;
  }
  await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body});
 });
 const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));
 await page.goto(origin+'/game/monkey-trails.html');await page.locator('#practice-button').waitFor();
 if(!existing)await page.evaluate(async({name,seed})=>{
  const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs');
  const fakeCloud={read:async()=>null,commit:async(_id,base)=>base+1};
  const v=new Vault(()=>{},localStorage,async()=>fakeCloud),s=await v.open(name,'social-test-password',{create:true,species:'capuchin',mode:'gps'});
  s.rng=seed;for(const p of C.team(s)){p.level=6;p.hp=11.25;p.energy=3;p.status={poison:2};p.statusPower={poison:.25};}
  for(const [i,species]of ['macaque','langur'].entries()){const p=structuredClone(C.newSave('Fixture',species,'gps',seed+i+10).run.monkey);p.id=`reserve-${i}`;p.level=6;p.hp=22+i;p.energy=4+i;s.collection[p.id]=p;}
  C.setParty(s,[s.run.activeId,'reserve-0','reserve-1']);s.run.fruit=100;s.run.items.berry=8;await v.checkpoint(s,true);await v.sync();
 },{name,seed});
 await page.locator('[data-action="welcome-continue"]').click();await page.locator('#creature-name').fill(name);await page.locator('#creature-password').fill('social-test-password');await page.locator('#save-form button[value="open"]').click();
 await page.locator('#game').waitFor({state:'visible'});await page.waitForFunction(()=>window.__socialDebug.social.model.status==='ready');
 await page.locator('[data-action="tab"][data-id="social"]').click();
 return{page,context,errors,uid,name};
}
async function form(page,kind,values){
 await page.waitForFunction(()=>!window.__socialDebug.social.actionTask);
 const locator=page.locator(`[data-social-form="${kind}"]`);await locator.waitFor({state:'attached'});
 // Fill the real form in one browser task so concurrent snapshot paints cannot
 // detach a select between individual Playwright actions.
 await locator.evaluate((f,values)=>{
  let parent=f.parentElement;while(parent){if(parent.tagName==='DETAILS')parent.open=true;parent=parent.parentElement;}
  for(const [key,value]of Object.entries(values)){const field=f.elements.namedItem(key);if(!field)throw Error('Missing social form field '+key);field.value=String(value);field.dispatchEvent(new Event('input',{bubbles:true}));field.dispatchEvent(new Event('change',{bubbles:true}));}
  if(!f.checkValidity())throw Error('Invalid social form: '+[...f.elements].filter(el=>el.validity&&!el.validity.valid).map(el=>`${el.name}: ${el.validationMessage}`).join('; '));
  f.requestSubmit();
 },values);
 await page.waitForFunction(()=>!window.__socialDebug.social.actionTask);
 assert.equal(await page.locator('.social-error:visible').count(),0,await page.locator('.social-error').allTextContents());
}
async function notices(page){
 for(let i=0;i<12;i++){
  const notice=page.locator('[data-social-action="dismiss-reward"]');if(!await notice.count())break;
  await notice.first().click();await page.waitForFunction(()=>!window.__socialDebug.social.actionTask);await page.waitForTimeout(120);
 }
}
async function clean(client){
 const size=await client.page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));assert.ok(size.scroll<=size.width,`${client.name} mobile overflow ${size.scroll-size.width}px`);assert.deepEqual(client.errors,[]);
}
const expedition=page=>page.evaluate(()=>{const s=__socialDebug.state;return {runId:s.run.id,phase:s.run.phase,distance:s.run.distance,items:s.run.items,monkeys:s.run.party.map(id=>{const p=s.collection[id];return{id,hp:p.hp,energy:p.energy,status:p.status,statusPower:p.statusPower,stage:p.stage};})};});
async function crewCard(page,uid,scope='tribe'){
 const card=page.locator(`[data-crew-id="${uid}"][data-crew-scope="${scope}"]`);await card.waitFor();
 return card.evaluate(async el=>{
  const {teamPower,monkeyPower}=await import('/game/monkey-trails/power.mjs'),{SPECIES}=await import('/game/monkey-trails/core.mjs');
  const model=__socialDebug.social.model,uid=el.dataset.crewId,p=(el.dataset.crewScope==='nearby'?model.nearby:model.members).find(p=>p.uid===uid||p.id===uid);
  return {power:Number(el.dataset.crewPower),expectedPower:teamPower(p.team),rank:Number(el.dataset.crewRank),rankLabel:el.querySelector('.social-crew-rank')?.textContent,
   slots:[...el.querySelectorAll('.social-crew-slot')].map((slot,index)=>({index:Number(slot.dataset.slot),id:slot.dataset.monkeyId||null,power:Number(slot.dataset.monkeyPower||0),portrait:!!slot.querySelector('.monkey-art'),text:slot.textContent})),
   expectedSlots:p.team.map(p=>({id:p.id,power:monkeyPower(p),name:p.nickname||SPECIES[p.species]?.name||'Companion'})),text:el.textContent};
 });
}
async function assertCrew(page,uid,scope='tribe'){
 const card=await crewCard(page,uid,scope);assert.equal(card.power,card.expectedPower,'Crew power is the sum of its starting three');assert.equal(card.slots.length,3,'Crew cards show all three starting slots');
 assert.deepEqual(card.slots.map(p=>p.index),[0,1,2],'Crew order stays in starting-slot order');
 for(const [i,p]of card.expectedSlots.entries()){
  assert.equal(card.slots[i].id,p.id);assert.equal(card.slots[i].power,p.power);assert.ok(card.slots[i].portrait,`Starting slot ${i+1} has a monkey portrait`);assert.ok(card.slots[i].text.includes(p.name),`Starting slot ${i+1} shows its monkey name`);
 }
 return card;
}
async function hostedCrewLayout(page){
 // Exercise the production SVG-to-image upgrade with committed assets as the
 // hosted source. Native images retain their intrinsic 240px dimensions, so
 // these bounds catch selectors that accidentally style only the initial SVG.
 await page.evaluate(async()=>{
  const {hydrateHostedPortraits}=await import('/game/monkey-trails/art.mjs');
  globalThis.MONKEY_TRAILS_ASSET_BASE='/game/monkey-trails/assets/';hydrateHostedPortraits(document.querySelector('#view'));
 });
 await page.waitForFunction(()=>document.querySelectorAll('.social-crew-slot img.monkey-art').length===6&&!!document.querySelector('.social-boss-portrait img.monkey-art'));
 const sizes=await page.evaluate(()=>[...document.querySelectorAll('.social-crew-slot img.monkey-art,.social-boss-portrait img.monkey-art')].map(img=>{
  const r=img.getBoundingClientRect(),slot=img.parentElement.getBoundingClientRect();
  return {kind:img.parentElement.className,loaded:img.complete&&img.naturalWidth>0,width:r.width,height:r.height,parentWidth:slot.width,parentHeight:slot.height,left:r.left,right:r.right,parentLeft:slot.left,parentRight:slot.right};
 }));
 assert.equal(sizes.length,7);
 for(const size of sizes){assert.ok(size.loaded);assert.ok(size.width>0&&size.height>0);assert.ok(size.width<=size.parentWidth+1&&size.height<=size.parentHeight+1,`Hosted portrait stays inside its card: ${JSON.stringify(size)}`);assert.ok(size.left>=size.parentLeft-1&&size.right<=size.parentRight+1,`Hosted portrait stays within horizontal card bounds: ${JSON.stringify(size)}`);}
 await page.evaluate(()=>delete globalThis.MONKEY_TRAILS_ASSET_BASE);
}

(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})});
 const clients=[],report=[];
 try{
  let code;
  if(!process.env.MONKEY_TRAILS_DIRECTORY_ONLY){
  const alice=await openClient(browser,{uid:'alice-online',name:'Alice Trail',width:390}),bob=await openClient(browser,{uid:'bob-online',name:'Bob Trail',width:320});clients.push(alice,bob);
  const a=alice.page,b=bob.page;
  await form(a,'create-tribe',{name:'Moonleaf Tribe',shape:'shield',symbol:'moon',color:'#78629a'});
  await a.locator('.tribe-code strong').waitFor();code=await a.locator('.tribe-code strong').innerText();
  await form(b,'join-tribe',{code});await a.waitForFunction(()=>__socialDebug.social.model.members.length===2);await b.waitForFunction(()=>__socialDebug.social.model.members.length===2);
  assert.match(await a.locator('.tribe-heading').innerText(),/2 \/ 10/);assert.equal(docs.get(`tribes/${code}`).emblem.symbol,'moon');
  await form(a,'update-emblem',{shape:'diamond',symbol:'crown',color:'#b08c36'});assert.equal(docs.get(`tribes/${code}`).emblem.symbol,'crown');report.push('create, join and update tribe emblem');console.log('Passed tribe setup');

  // Identical starting crews share first place; rankings compare these tribe members.
  const tiedAlice=await assertCrew(a,alice.uid),tiedBob=await assertCrew(a,bob.uid);
  assert.equal(tiedAlice.power,tiedBob.power);assert.equal(tiedAlice.rank,1);assert.equal(tiedBob.rank,1);
  assert.match(tiedAlice.rankLabel,/Tribe #1/);assert.match(tiedBob.rankLabel,/Tribe #1/);
  const eventPreview=await a.locator('.social-boss-event').evaluate(async el=>{
   const {bossReward}=await import('/game/monkey-trails/social-combat.mjs'),rewards=el.querySelector('.social-event-rewards');
   return {art:!!el.querySelector('.social-boss-portrait svg'),level:Number(rewards.dataset.bossLevel),tokens:Object.fromEntries([...rewards.querySelectorAll('[data-reward]')].map(n=>[n.dataset.reward,Number(n.dataset.amount)])),expected:bossReward(Number(rewards.dataset.bossLevel))};
  });
  assert.ok(eventPreview.art,'Tribe event shows the actual boss portrait');assert.equal(eventPreview.level,1);
  assert.deepEqual(eventPreview.tokens,{fruit:eventPreview.expected.fruit,xp:eventPreview.expected.xp,...eventPreview.expected.items});
  assert.equal(await a.locator('#view [data-social-action="start-raid"]').isEnabled(),true);
  await b.screenshot({path:'/tmp/monkey-crew-ui-tribe-mobile.png',fullPage:true});
  await a.setViewportSize({width:1200,height:900});await clean(alice);await a.screenshot({path:'/tmp/monkey-crew-ui-tribe-desktop.png',fullPage:true});await a.setViewportSize({width:390,height:844});
  await b.emulateMedia({reducedMotion:'reduce'});
  const moving=await b.locator('.social-boss-event').evaluate(el=>[el,...el.querySelectorAll('*')].filter(n=>getComputedStyle(n).animationName!=='none'&&getComputedStyle(n).animationDuration.split(',').some(t=>parseFloat(t)>0)).length);
  assert.equal(moving,0,'Tribe event respects reduced motion');await b.emulateMedia({reducedMotion:'no-preference'});
  await hostedCrewLayout(b);await clean(bob);await b.screenshot({path:'/tmp/monkey-crew-ui-hosted-mobile.png',fullPage:true});
  await hostedCrewLayout(a);await clean(alice);

  // Use the real upgrade control, then wait for another client to receive the new crew.
  await b.locator('[data-action="tab"][data-id="team"]').click();
  await b.evaluate(()=>{__socialDebug.state.run.party.forEach(id=>__socialDebug.state.collection[id].training=100000);__socialDebug.render();});
  const ownBefore=await b.locator('[data-team-power]').getAttribute('data-team-power');
  await b.locator('[data-action="monkey-actions"]').first().click();await b.locator('[data-action="companion-details"]').click();
  await b.locator('[data-action="train"]').click();
  await b.waitForFunction(()=>__socialDebug.state.run.monkey.level===7);
  await a.waitForFunction(()=>__socialDebug.social.model.members.find(p=>p.uid==='bob-online')?.team?.[0]?.level===7);
  const upgradedBob=await assertCrew(a,bob.uid),outpacedAlice=await assertCrew(a,alice.uid);
  assert.ok(upgradedBob.power>tiedBob.power);assert.equal(upgradedBob.rank,1);assert.equal(outpacedAlice.rank,2);
  assert.deepEqual(await a.locator('[data-crew-scope="tribe"]').evaluateAll(nodes=>nodes.map(n=>n.dataset.crewId)),[bob.uid,alice.uid]);
  const own=await b.evaluate(async()=>{
   const C=await import('/game/monkey-trails/core.mjs'),P=await import('/game/monkey-trails/power.mjs'),s=__socialDebug.state,p=s.run.monkey,ranking=P.rankMonkeys(C.sanctuary(s),p=>C.equipped(s,p));
   return {total:Number(document.querySelector('[data-team-power]').dataset.teamPower),expectedTotal:P.teamPower(C.team(s).map(p=>({...p,gear:C.equipped(s,p)}))),power:Number(document.querySelector('.companion-power [data-monkey-power]').dataset.monkeyPower),expectedPower:P.monkeyPower(p,C.equipped(s,p)),rank:Number(document.querySelector('.companion-power [data-roster-rank]').dataset.rosterRank),expectedRank:ranking.find(r=>r.monkey.id===p.id).rank,slots:document.querySelectorAll('.team-slot [data-monkey-power]').length};
  });
  assert.ok(own.total>Number(ownBefore));assert.equal(own.total,own.expectedTotal);assert.equal(own.power,own.expectedPower);assert.equal(own.rank,own.expectedRank);assert.equal(own.slots,3);
  await clean(bob);await b.screenshot({path:'/tmp/monkey-crew-ui-team-mobile.png',fullPage:true});
  await b.locator('[data-action="tab"][data-id="sanctuary"]').click();await b.locator('[data-action="collection-filter"][data-id="owned"]').click();
  await b.locator('#collection-sort').selectOption('power');
  const rosterPower=await b.locator('.collection-card .collection-power').evaluateAll(nodes=>nodes.map(n=>({power:Number(n.dataset.monkeyPower),rank:Number(n.dataset.rosterRank)})));
  assert.equal(rosterPower.length,3);assert.deepEqual(rosterPower.map(r=>r.power),rosterPower.map(r=>r.power).sort((x,y)=>y-x));assert.equal(rosterPower[0].rank,1);
  await clean(bob);await b.screenshot({path:'/tmp/monkey-crew-ui-sanctuary-mobile.png',fullPage:true});
  await b.locator('[data-action="tab"][data-id="social"]').click();report.push('starting-three portraits, tied tribe ranks, live upgrades, roster power sorting and responsive event graphics');console.log('Passed crew power UI');

  await form(a,'gift',{toId:bob.uid,asset:'berry',amount:2});await b.locator('[data-social-action="dismiss-reward"]').waitFor();assert.equal(docs.get(`players/${bob.uid}`).wallet.items.berry,2);await notices(b);
  await form(a,'offer-trade',{toId:bob.uid,giveAsset:'fruit',giveAmount:3,wantAsset:'berry',wantAmount:1});
  await b.locator('[data-social-action="accept-trade"]').waitFor();await b.locator('[data-social-action="accept-trade"]').click();
  await b.waitForFunction(()=>!__socialDebug.social.actionTask);await a.waitForFunction(()=>__socialDebug.social.model.offers.some(o=>o.status==='accepted'));
  assert.equal(docs.get(`players/${alice.uid}`).wallet.items.berry,1);assert.equal(docs.get(`players/${bob.uid}`).wallet.fruit,3);await notices(a);await notices(b);report.push('gifts and atomic tribe trades');console.log('Passed tribe gifts/trades');

  // Offline member reward delivery: the fixture boss is already near defeat.
  const t=clone(docs.get(`tribes/${code}`));t.boss.hp=1;docs.set(`tribes/${code}`,t);revision++;
  await bob.page.evaluate(()=>__socialDebug.social.stop());
  await a.waitForTimeout(150);await a.locator('#view [data-social-action="start-raid"]').click();await a.locator('#dialog-content [data-social-action="start-raid"][data-lead-confirmed="true"]').click();
  await a.locator('[data-social-action="raid-move"]').first().waitFor();
  for(let turn=0;turn<10;turn++){
   await a.locator('[data-social-action="raid-move"][data-id="strike"]').click();
   await a.waitForFunction(expected=>__socialDebug.social.model.raid?.turns===expected,turn+1);
  }
  await a.locator('[data-social-action="submit-raid"]').click();await a.waitForFunction(()=>__socialDebug.social.model.raid?.submitted===true);
  assert.equal(docs.get(`tribes/${code}`).boss.level,2);assert.ok(rows(`receipts/${bob.uid}/entries`,[['type','==','raid']]).length===1);
  await a.locator('[data-social-action="return"]').click();await a.waitForFunction(()=>!__socialDebug.social.model.raid);
  await a.locator('[data-action="tab"][data-id="social"]').click();
  assert.equal(await a.locator('#view [data-social-action="start-raid"]').isDisabled(),true,'Hourly cooldown is visible after returning');
  assert.ok(await a.locator('.social-event-availability').textContent());
  await b.evaluate(()=>__socialDebug.social.start());await b.waitForFunction(()=>__socialDebug.social.model.status==='ready');
  await b.locator('.social-reward-notice').filter({hasText:'Your tribe defeated boss 1'}).waitFor();await notices(a);await notices(b);report.push('ten-turn raid and offline member reward delivery');console.log('Passed raid and offline receipts');

  await a.locator('[data-action="tab"][data-id="social"]').click();await b.locator('[data-action="tab"][data-id="social"]').click();
  // A nearby discoverability opt-in shows a top option even without a map pin.
  await a.locator('[data-social-action="discoverability"]').check();await b.locator('[data-social-action="discoverability"]').check();
  await a.waitForFunction(()=>__socialDebug.social.model.nearby.length===1);await b.waitForFunction(()=>__socialDebug.social.model.nearby.length===1);
  await a.waitForFunction(()=>__socialDebug.social.model.nearby[0]?.team?.length===3);await b.waitForFunction(()=>__socialDebug.social.model.nearby[0]?.team?.length===3);
  const nearbyBob=await assertCrew(a,bob.uid,'nearby'),nearbyAlice=await assertCrew(b,alice.uid,'nearby');assert.match(nearbyBob.rankLabel,/Nearby #1/);assert.match(nearbyAlice.rankLabel,/Nearby #1/);
  assert.match(await a.locator('#social-banner').innerText(),/nearby player/);report.push('nearby online discovery');console.log('Passed nearby discovery');

  const beforeA=await expedition(a),beforeB=await expedition(b);
  const fruitBefore={a:(await a.evaluate(()=>__socialDebug.state.run.fruit))+docs.get(`players/${alice.uid}`).wallet.fruit,b:(await b.evaluate(()=>__socialDebug.state.run.fruit))+docs.get(`players/${bob.uid}`).wallet.fruit};
  const xpBefore=await Promise.all([a,b].map(p=>p.evaluate(()=>__socialDebug.state.run.party.reduce((n,id)=>n+__socialDebug.state.collection[id].training,0))));
  await b.locator('[data-action="settings"]').click();assert.equal(await b.locator('#inspect-dialog').evaluate(el=>el.open),true);
  await a.locator(`[data-social-action="challenge"][data-id="${bob.uid}"]`).first().click();await a.locator('#dialog-content [data-social-action="challenge"][data-lead-confirmed="true"]').click();
  await b.waitForFunction(()=>document.querySelector('#social-menu-overlay')?.matches(':popover-open'));
  const overlayAccept=b.locator('#social-menu-overlay [data-social-action="accept-challenge"]');await overlayAccept.waitFor();await overlayAccept.click();await b.locator('#dialog-content [data-social-action="accept-challenge"][data-lead-confirmed="true"]').click();
  await a.waitForFunction(()=>__socialDebug.social.model.duel?.status==='active');await b.waitForFunction(()=>__socialDebug.social.model.duel?.status==='active');
  assert.equal(await b.locator('#inspect-dialog').evaluate(el=>el.open),false,'Accepting the challenge closes the Settings dialog');
  assert.match(await b.locator('#social-banner').innerText(),/New challenges are paused/);
  assert.equal(await b.locator('[data-social-action="accept-challenge"]:visible').count(),0);
  const did=(await a.evaluate(()=>__socialDebug.social.model.duel.id));
  assert.equal(await a.locator('[data-social-action="duel-move"][data-id="strike"]').isEnabled(),true);
  assert.equal(await b.locator('[data-social-action="duel-move"][data-id="strike"]').isDisabled(),true);
  await a.locator('[data-social-action="duel-move"][data-id="strike"]').click();
  await b.waitForFunction(()=>__socialDebug.social.model.duel?.revision===1);await a.waitForFunction(()=>__socialDebug.social.model.duel?.revision===1);
  assert.equal(await a.locator('[data-social-action="duel-move"][data-id="strike"]').isDisabled(),true);
  await b.locator('[data-social-action="duel-move"][data-id="strike"]').click();await a.waitForFunction(()=>__socialDebug.social.model.duel?.revision===2);await b.waitForFunction(()=>__socialDebug.social.model.duel?.revision===2);
  await b.locator('[data-social-action="surrender"]').click();await a.waitForFunction(()=>__socialDebug.social.model.duel?.status==='finished');await b.waitForFunction(()=>__socialDebug.social.model.duel?.status==='finished');
  assert.equal(docs.get(`duels/${did}`).state.winner,alice.uid);assert.ok(rows(`receipts/${alice.uid}/entries`,[['type','==','duel']]).at(-1).payload.xp>0);assert.ok(rows(`receipts/${bob.uid}/entries`,[['type','==','duel']]).at(-1).payload.xp>0);
  assert.deepEqual(await expedition(a),beforeA);assert.deepEqual(await expedition(b),beforeB);
  await a.evaluate(()=>__socialDebug.social.queueReceipts());await b.evaluate(()=>__socialDebug.social.queueReceipts());
  for(const [index,p]of [a,b].entries())assert.ok(await p.evaluate(()=>__socialDebug.state.run.party.reduce((n,id)=>n+__socialDebug.state.collection[id].training,0))>xpBefore[index],'Both players receive local XP');
  assert.equal((await a.evaluate(()=>__socialDebug.state.run.fruit))+docs.get(`players/${alice.uid}`).wallet.fruit-fruitBefore.a,10);
  assert.equal((await b.evaluate(()=>__socialDebug.state.run.fruit))+docs.get(`players/${bob.uid}`).wallet.fruit-fruitBefore.b,-10);
  await notices(a);await notices(b);
  await a.locator('[data-social-action="return"]').click();await b.locator('[data-social-action="return"]').click();
  await a.waitForFunction(()=>!__socialDebug.social.model.me?.activeDuel);await b.waitForFunction(()=>!__socialDebug.social.model.me?.activeDuel);
  await a.locator('[data-action="tab"][data-id="social"]').click();await b.locator('[data-action="tab"][data-id="social"]').click();
  await a.waitForFunction(()=>__socialDebug.social.model.members.some(p=>p.uid==='bob-online'));
  await a.waitForFunction(uid=>[...document.querySelectorAll(`[data-social-action="challenge"][data-id="${uid}"]`)].some(el=>!el.disabled),'bob-online');
  assert.equal(await a.locator(`[data-social-action="challenge"][data-id="${bob.uid}"]`).first().isEnabled(),true);report.push('challenge overlay above Settings, synchronized alternating duel, safe restoration and immediate rematch');
  // A reward arriving while an expedition has ended waits for its next run.
  await notices(a);await notices(b);await b.locator('[data-action="tab"][data-id="walk"]').click();
  await b.evaluate(async()=>{const s=__socialDebug.state;s.run.phase='dead';for(const id of s.run.party)s.collection[id].hp=0;__socialDebug.render();await __socialDebug.social.heartbeat(true);});
  const player=clone(docs.get(`players/${alice.uid}`));player.lastRaidAt=0;docs.set(`players/${alice.uid}`,player);
  const nextBoss=clone(docs.get(`tribes/${code}`));nextBoss.boss.hp=1;docs.set(`tribes/${code}`,nextBoss);revision++;
  await a.waitForFunction(()=>__socialDebug.social.model.me?.lastRaidAt===0);
  await a.locator('#view [data-social-action="start-raid"]').click();await a.locator('#dialog-content [data-social-action="start-raid"][data-lead-confirmed="true"]').click();await a.locator('[data-social-action="raid-move"]').first().waitFor();
  for(let turn=0;turn<10;turn++){await a.locator('[data-social-action="raid-move"][data-id="strike"]').click();await a.waitForFunction(expected=>__socialDebug.social.model.raid?.turns===expected,turn+1);}
  await a.locator('[data-social-action="submit-raid"]').click();await a.waitForFunction(()=>__socialDebug.social.model.raid?.submitted===true);
  await b.waitForFunction(()=>__socialDebug.social.model.receipts.some(r=>r.type==='raid'&&r.title.includes('boss 2')));
  const queuedId=rows(`receipts/${bob.uid}/entries`,[['type','==','raid']]).find(r=>r.title.includes('boss 2')).id;
  assert.equal(await b.evaluate(id=>!!__socialDebug.state.social?.applied[`bob-online:${id}`],queuedId),false);
  assert.equal(await b.locator('.social-reward-notice').filter({hasText:'Your tribe defeated boss 2'}).count(),0);
  await b.locator('[data-action="restart"]').click();await b.waitForFunction(id=>!!__socialDebug.state.social?.applied[`bob-online:${id}`],queuedId);
  await b.locator('.social-reward-notice').filter({hasText:'Your tribe defeated boss 2'}).waitFor();
  assert.ok(await b.evaluate(()=>__socialDebug.state.run.party.reduce((n,id)=>n+__socialDebug.state.collection[id].training,0))>0,'The queued reward is applied after restarting');
  await a.locator('[data-social-action="return"]').click();await notices(a);await notices(b);
  report.push('ended-run rewards deliver on the next expedition');
  await clean(alice);await clean(bob);
  const escaped=await b.evaluate(async()=>{
   const {renderSocialView,renderSocialBanner,renderRewardNotices,emblemSVG}=await import('/game/monkey-trails/social-ui.mjs');
   const model=structuredClone(__socialDebug.social.model),hostile='<img src=x onerror="window.__xss=1">';
   model.tribe.name=hostile;model.members=model.members.map(p=>({...p,name:hostile,team:p.team?.map(m=>({...m,nickname:hostile}))}));model.challenges=[{id:'hostile',from:'alice-online',to:'bob-online',fromName:hostile,status:'pending'}];
   const box=document.createElement('div');box.innerHTML=renderSocialView(model)+renderSocialBanner(model)+renderRewardNotices([{id:'bad',uid:'bob-online',title:hostile,payload:{fruit:1,xp:1,items:{}}}])+emblemSVG({color:'red" onload="window.__xss=1',symbol:'<script>'});
   return {injected:box.querySelectorAll('[onerror],[onload],script,img[src="x"]').length,text:box.textContent.includes(hostile)};
  });
  assert.deepEqual(escaped,{injected:0,text:true});report.push('hostile names and emblem values stay inert');
  const partial=await b.evaluate(async()=>{
   const {renderSocialView}=await import('/game/monkey-trails/social-ui.mjs'),model=structuredClone(__socialDebug.social.model),box=document.createElement('div');
   model.me={...model.me,team:model.me.team.slice(0,1)};model.members=[model.me];model.nearby=[];model.challenges=[];model.tribe.boss.hp=0;
   box.innerHTML=renderSocialView(model);const crew=box.querySelector('[data-crew-scope="tribe"]');
   return {slots:crew.querySelectorAll('.social-crew-slot').length,empty:crew.querySelectorAll('.social-crew-slot.is-empty').length,zeroHealth:box.querySelector('.social-boss-event .social-health i')?.style.width};
  });
  assert.deepEqual(partial,{slots:3,empty:2,zeroHealth:'0%'});report.push('partial crews retain three visible slots and zero boss HP stays zero');
  }else{const seed=await openClient(browser,{uid:'seed-online',name:'Seed Trail',width:390});clients.push(seed);await form(seed.page,'create-tribe',{name:'Moonleaf Tribe'});await seed.page.locator('.tribe-code strong').waitFor();code=await seed.page.locator('.tribe-code strong').innerText();}
  const carol=await openClient(browser,{uid:'carol-online',name:'Carol Trail',width:360}),dana=await openClient(browser,{uid:'dana-online',name:'Dana Trail',width:320});clients.push(carol,dana);
  const c=carol.page,d=dana.page;await d.emulateMedia({reducedMotion:'reduce'});
  await form(c,'directory-search',{search:'moonleaf',availableOnly:'true'});await c.locator(`[data-directory-tribe="${code}"]`).waitFor();
  await c.locator(`[data-social-action="view-tribe"][data-id="${code}"]`).first().click();await c.locator('.social-public-profile').waitFor();
  const publicFields=await c.evaluate(()=>JSON.stringify({tribe:__socialDebug.social.model.publicTribe,members:__socialDebug.social.model.publicMembers}));assert.doesNotMatch(publicFields,/saveId|wallet|location|route/);
  await c.locator('[data-social-action="close-tribe-profile"]').click();
  await form(c,'create-tribe',{name:'Approval Explorers',admissionMode:'approval',acceptingApplications:'true'});await c.waitForFunction(()=>!!__socialDebug.social.model.me?.tribeId);await c.locator('.tribe-code strong').waitFor();const approval=await c.locator('.tribe-code strong').innerText();
  await form(d,'directory-search',{search:'approval explorers',availableOnly:'true'});const requestButton=d.locator(`[data-social-action="request-tribe"][data-id="${approval}"]`);await requestButton.click();await d.locator('.social-application-status.is-pending').waitFor();
  await c.locator('[data-social-action="accept-application"]').click();await d.waitForFunction(()=>__socialDebug.social.model.tribe?.name==='Approval Explorers');await d.locator('.social-application-status.is-accepted').waitFor();
  await d.locator('[data-social-action="leave-tribe"]').click();await d.locator('[data-social-action="leave-tribe"][data-leave-confirmed="true"]').click();await d.waitForFunction(()=>!__socialDebug.social.model.me?.tribeId);await d.locator('.social-directory').waitFor();
  await form(d,'directory-search',{search:'approval explorers',availableOnly:'true'});await d.locator(`[data-social-action="request-tribe"][data-id="${approval}"]`).click();await c.locator('[data-social-action="reject-application"]').click();await d.locator('.social-application-status.is-rejected').waitFor();
  await form(c,'update-emblem',{acceptingApplications:'false'});await c.waitForFunction(()=>__socialDebug.social.model.tribe?.acceptingApplications===false);await form(d,'directory-search',{search:'approval explorers',availableOnly:'true'});await d.waitForFunction(id=>__socialDebug.social.model.directoryStatus==='ready'&&!__socialDebug.social.model.directory.some(t=>t.id===id),approval);assert.equal(await d.locator(`[data-directory-tribe="${approval}"]`).count(),0);
  await d.locator('[data-social-action="ranking-metric"][data-id="power"]').click();await d.waitForFunction(()=>__socialDebug.social.model.rankingMetric==='power'&&__socialDebug.social.model.rankingsStatus==='ready');
  await clean(carol);await clean(dana);report.push('directory search, private public profiles, leader approval/rejection, safe leave, paused-admissions filter and power ranking at 320px with reduced motion');
  console.log(JSON.stringify({checks:report,clients:clients.length,pageErrors:0,mobileOverflow:0},null,2));
 }finally{for(const c of clients)await c.context.close();await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
