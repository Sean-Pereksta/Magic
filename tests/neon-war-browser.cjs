/* Actual game/WebGL integration checks. Hooks are injected only by this server. */
const assert=require('node:assert/strict'),fs=require('node:fs'),http=require('node:http'),path=require('node:path'),{chromium}=require('playwright');
const repo=path.resolve(__dirname,'..'),out=process.env.NEON_WAR_REPORT||path.resolve(repo,'../war-browser');fs.mkdirSync(out,{recursive:true});
const hooks=`window.__WAR_TEST__={game,player,get bots(){return bots},get player2(){return player2},get conquest(){return conquest},get campaign(){return campaignRun},get map(){return warMap},get models(){return neonModelRenderer},get gl(){return gl},get nav(){return nav},reset,startArenaMatch,draw,updateWar,updateBot,physics,kill,equip,tryFire,rayWorld,rayEntity,actorModelPath,sameTeam,spawn,tacticalPointInfo,tacticalCanTraverse,endMatch,setClock:n=>warClock=n,getClock:()=>warClock,setInteract:()=>warInteract=true,progress:survivalProgress};`;
// Send real Chromium touch events, including pointer capture and cancellation,
// instead of invoking a gameplay handler or dispatching synthetic PointerEvents.
async function holdTouchButton(page,session,selector,property){
 const button=page.locator(selector);await button.waitFor({state:'visible'});
 const box=await button.boundingBox();assert(box,selector+' has a visible touch target');
 const x=box.x+box.width/2,y=box.y+box.height/2,viewport=page.viewportSize();
 assert(x>0&&y>0&&x<viewport.width&&y<viewport.height,selector+' is inside the mobile viewport');
 await session.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x,y,id:1,radiusX:2,radiusY:2,force:1}]});
 try{
  await page.waitForTimeout(550);
  // Keep the finger down until the browser has processed its 450ms timer. A
  // delayed software-rendering frame must not release/cancel a valid hold.
  await page.waitForFunction(key=>__WAR_TEST__.player[key]===true,property,{timeout:3000});
 }finally{await session.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});}
}
const server=http.createServer((req,res)=>{try{const pathname=decodeURIComponent(new URL(req.url,'http://localhost').pathname),file=path.resolve(repo,'.'+(pathname.endsWith('/')?pathname+'index.html':pathname));if(!file.startsWith(repo+path.sep))throw Error();let data=fs.readFileSync(file);if(file.endsWith('neon-arena.js'))data=Buffer.from(data.toString().replace(/\}\)\(\);\s*$/,hooks+'})();'));res.writeHead(200,{'Content-Type':({'.js':'text/javascript','.mjs':'text/javascript','.html':'text/html','.json':'application/json','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp','.glb':'model/gltf-binary'}[path.extname(file)]||'application/octet-stream')});res.end(data);}catch{res.writeHead(404);res.end('Missing');}});
(async()=>{await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||'C:/Program Files/Google/Chrome/Application/chrome.exe',args:['--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});const report=[];
try{
 for(const device of (process.env.NEON_WAR_GAMEPLAY_ONLY?[{name:'desktop',width:1280,height:800}]:[{name:'desktop',width:1280,height:800},{name:'mobile',width:390,height:844,hasTouch:true,isMobile:true},{name:'split',width:1280,height:800}])){
  const context=await browser.newContext({viewport:{width:device.width,height:device.height},hasTouch:device.hasTouch,isMobile:device.isMobile});const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto(origin+'/game/neon-arena/');await page.waitForFunction(()=>!!window.__WAR_TEST__,null,{timeout:15000}).catch(e=>{throw Error('Startup: '+errors.join(' | ')+e.message)});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'no horizontal overflow');
  if(device.name==='desktop')assert.equal(await page.evaluate(async()=>{for(const m of NeonCampaign.catalog()){const image=new Image();image.src=m.thumbnail;await image.decode();if(!image.naturalWidth)throw Error('Empty mission preview '+m.id);}return NeonCampaign.catalog().length;}),10,'all distinct mission briefing graphics decode');
  for(const map of (process.env.NEON_WAR_MAP?[process.env.NEON_WAR_MAP]:['rail_yard_nine','whiskerhold','floodgate','cold_storage','clockwork','night_market'])){
   await page.evaluate(({map,split,touch})=>{document.getElementById('mode').value='conquest';document.getElementById('mode').dispatchEvent(new Event('change'));document.getElementById('map').value=map;document.getElementById('map').dispatchEvent(new Event('change'));document.getElementById('secondPlayer').value=split?'keyboard':'off';const input=document.getElementById('p1Input');input.value=touch?'touch':'keyboard';input.dispatchEvent(new Event('change',{bubbles:true}));}, {map,split:device.name==='split',touch:device.name==='mobile'});
   await page.evaluate(()=>__WAR_TEST__.startArenaMatch());
   const result=await page.evaluate(()=>{const t=__WAR_TEST__;if(!t.game.started||t.game.over)return {failed:document.getElementById('mapDescription').textContent};t.draw();return {map:t.game.map,mode:t.game.mode,input:t.game.p1Input,actors:1+t.bots.length,teams:[...new Set([t.player,...t.bots].map(a=>a.team))],finite:[t.player,...t.bots].every(a=>Number.isFinite(a.x+a.y+a.z)),paths:[t.player,...t.bots].every(a=>t.actorModelPath(a).startsWith('factions/')),gl:t.gl.getError(),parsed:t.models.stats().parsed,split:!!t.player2,sectors:t.conquest.sectors.length};});
   assert(!result.failed,JSON.stringify(result));assert.equal(result.map,map);assert.equal(result.mode,'conquest');assert.equal(result.input,device.name==='mobile'?'touch':'keyboard');assert.equal(result.finite,true);assert.equal(result.paths,true);assert.equal(result.gl,0);assert.equal(result.sectors,5);assert.equal(result.split,device.name==='split');assert.equal(result.teams.length,2);
   result.spacing=await page.evaluate(()=>{const t=__WAR_TEST__,a=[t.player,...t.bots];return {initial:a.map(b=>({id:b.entityId,x:b.x,y:b.y,z:b.z,d:Math.hypot(b.x-t.player.x,b.z-t.player.z)})),min:Math.min(...a.flatMap((b,i)=>a.slice(i+1).map(c=>Math.hypot(c.x-b.x,c.z-b.z))))};});
   assert(result.spacing.min>1.2,device.name+' '+map+' initial spawn pairs must remain over 1.2m apart: '+JSON.stringify(result.spacing));
   await page.waitForTimeout(300);
   result.after=await page.evaluate(()=>{
    const t=__WAR_TEST__,actors=[t.player,...t.bots].filter(a=>!a.dead),humans=actors.filter(a=>a.human);
    return {humans:humans.length,actors:actors.map(b=>({id:b.entityId,human:!!b.human,x:b.x,y:b.y,z:b.z,d:Math.hypot(b.x-t.player.x,b.z-t.player.z)})),minHumanDistance:Math.min(...humans.flatMap(h=>actors.filter(a=>a!==h).map(a=>Math.hypot(a.x-h.x,a.z-h.z))))};
   });
   assert.equal(result.after.humans,device.name==='split'?2:1,'spacing must check each active human');
   assert(result.after.minHumanDistance>1.0,device.name+' '+map+' actors must stay over 1m from either human after 300ms: '+JSON.stringify(result.after));
   await page.screenshot({path:path.join(out,device.name+'-'+map+'.png')});
   await page.evaluate(()=>{__WAR_TEST__.endMatch('Integration check');document.getElementById('returnMenu').click();});report.push({device:device.name,...result});
  }
  // Both factions and every staged campaign start with real models and objectives.
  if(device.name==='desktop'&&!process.env.NEON_WAR_MAP)for(const faction of ['cats','mice']){
   await page.evaluate(faction=>{const p=NeonCampaign.freshProgress();for(const m of NeonCampaign.catalog(faction))p.completed[m.id]={stars:1,bestTime:30,completions:1,difficulty:'normal'};__WAR_TEST__.progress.saveCampaign(p);document.getElementById('warFaction').value=faction;document.getElementById('warFaction').dispatchEvent(new Event('change',{bubbles:true}));NeonWarUI.refreshProgress();},faction);
   const missions=await page.evaluate(f=>NeonCampaign.catalog(f).map(m=>m.id),faction);
   for(const mission of missions){await page.evaluate(m=>{document.getElementById('mode').value='campaign';document.getElementById('mode').dispatchEvent(new Event('change'));document.getElementById('warMission').value=m;document.getElementById('warMission').dispatchEvent(new Event('change',{bubbles:true}));},mission);await page.evaluate(()=>__WAR_TEST__.startArenaMatch());const result=await page.evaluate(()=>{const t=__WAR_TEST__;t.draw();return {started:t.game.started,over:t.game.over,id:t.campaign?.missionId,mode:t.game.mode,map:t.game.map,gl:t.gl.getError(),objective:NeonCampaign.currentObjective(t.campaign)?.title,failed:document.getElementById('mapDescription').textContent};});assert.equal(result.id,mission,JSON.stringify(result));assert.equal(result.gl,0);assert.equal(result.over,false);
    const play=await page.evaluate(()=>{const t=__WAR_TEST__;t.game.started=false;let ticks=0,orders=0;const stages=new Set();while(t.campaign.status==='active'&&ticks++<2200){const o=NeonCampaign.currentObjective(t.campaign),p=NeonCampaign.objectivePoint(t.campaign,t.map.anchors);stages.add(o.type);for(const b of t.bots){if(b.assaultOrder)orders++;if(b.team!==t.player.team)b.dead=true;else if(b.escort)Object.assign(b,{x:p.x,y:p.y,z:p.z});}Object.assign(t.player,{x:p.x,y:p.y,z:p.z,dead:false});t.setClock(t.getClock()+.1);t.setInteract();t.updateWar(.1);}return {status:t.campaign.status,index:t.campaign.index,stages:[...stages],orders,progress:t.progress.getCampaign().completed[t.campaign.missionId],objective:NeonCampaign.currentObjective(t.campaign),position:t.player,point:NeonCampaign.objectivePoint(t.campaign,t.map.anchors)};});assert.equal(play.status,'complete',JSON.stringify({mission,play}));assert(play.progress,'completion persisted');if(play.stages.includes('defend'))assert(play.orders>0,'counterattack squads receive actual defense position orders');
    await page.evaluate(()=>{document.getElementById('returnMenu').click();});report.push({device:device.name,mission,...result,completed:play.status,stages:play.stages});}
  }
  // Exercise the actual touch UI in each faction's first mission. These cases
  // check gestures and stealth safety, not scripted campaign completion.
  if(device.name==='mobile'&&!process.env.NEON_WAR_MAP){
   const touchSession=await context.newCDPSession(page);
   try{for(const faction of ['cats','mice']){
    const mission=await page.evaluate(faction=>{
     const byId=id=>document.getElementById(id);
     __WAR_TEST__.progress.saveCampaign(NeonCampaign.freshProgress());
     byId('mode').value='campaign';byId('mode').dispatchEvent(new Event('change'));
     byId('warFaction').value=faction;byId('warFaction').dispatchEvent(new Event('change',{bubbles:true}));
     NeonWarUI.refreshProgress();const mission=NeonCampaign.catalog(faction)[0].id;
     byId('warMission').value=mission;byId('warMission').dispatchEvent(new Event('change',{bubbles:true}));
     byId('warResume').checked=false;byId('secondPlayer').value='off';
     byId('p1Input').value='touch';byId('p1Input').dispatchEvent(new Event('change',{bubbles:true}));
     return mission;
    },faction);
    await page.evaluate(()=>__WAR_TEST__.startArenaMatch());
    await page.locator('#warTrigger').waitFor({state:'visible'});
    const result=await page.evaluate(()=>{
     const t=__WAR_TEST__;t.draw();return {started:t.game.started,over:t.game.over,id:t.campaign?.missionId,mode:t.game.mode,input:t.game.p1Input,map:t.game.map,split:!!t.player2,safeDefault:t.game.weaponSafe,safeLabel:document.getElementById('warTrigger').textContent,crouching:!!t.player.crouching,aiming:!!t.player.aiming,paths:[t.player,...t.bots].every(a=>t.actorModelPath(a).startsWith('factions/')),gl:t.gl.getError(),parsed:t.models.stats().parsed};
    });
    assert.equal(result.started,true);assert.equal(result.over,false);assert.equal(result.id,mission);assert.equal(result.mode,'campaign');assert.equal(result.input,'touch');assert.equal(result.split,false);assert.equal(result.safeDefault,true);assert.match(result.safeLabel,/WEAPON SAFE/);assert.equal(result.paths,true);assert.equal(result.gl,0);assert.equal(result.crouching,false);assert.equal(result.aiming,false);
    await page.locator('#warTrigger').tap();
    await page.waitForFunction(()=>!__WAR_TEST__.game.weaponSafe&&document.getElementById('warTrigger').textContent.includes('WEAPON LIVE'));
    result.safeLiveToggle=true;
    // Return to safe while testing posture to avoid automatic fire changing the
    // scenario merely because the player is aiming toward a distant guard.
    await page.locator('#warTrigger').tap();await page.waitForFunction(()=>__WAR_TEST__.game.weaponSafe);
    await holdTouchButton(page,touchSession,'#mobileSlide','crouching');
    await page.waitForFunction(()=>__WAR_TEST__.player.crouching===true);
    result.longHoldCrouch=true;
    await holdTouchButton(page,touchSession,'#mobileReload','aiming');
    await page.waitForFunction(()=>__WAR_TEST__.player.aiming===true);
    result.longHoldAim=true;
    const final=await page.evaluate(()=>{const t=__WAR_TEST__;t.draw();return {over:t.game.over,gl:t.gl.getError(),safe:t.game.weaponSafe,crouching:t.player.crouching,aiming:t.player.aiming};});
    assert.equal(final.over,false);assert.equal(final.gl,0);assert.equal(final.safe,true);assert.equal(final.crouching,true);assert.equal(final.aiming,true);
    await page.screenshot({path:path.join(out,'mobile-campaign-'+faction+'.png')});
    await page.evaluate(()=>{__WAR_TEST__.endMatch('Mobile campaign control check');document.getElementById('returnMenu').click();});
    report.push({device:device.name,faction,mission,...result,controls:final});
   }}finally{await touchSession.detach();}
  }
  assert.deepEqual(errors,[],device.name+' runtime errors');await context.close();
 }
 if(!process.env.NEON_WAR_GAMEPLAY_ONLY&&!process.env.NEON_WAR_MAP)assert.equal(report.length,30,'the full matrix includes 18 Conquest, ten desktop campaign and two mobile campaign cases');
 fs.writeFileSync(path.join(out,'results.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({passed:report.length,report:path.join(out,'results.json')},null,2));
}finally{await browser.close();server.close();}
})().catch(e=>{console.error(e);server.close();process.exitCode=1;});
