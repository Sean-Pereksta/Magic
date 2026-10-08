/* Integrated-world artwork QA: occupied village, actual crossing movement and
 * a generated human fortress. Fixtures use live state and ordinary game updates. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{chromium}=require('playwright');
const root=path.resolve(__dirname,'..'),output=process.env.QA_ARTIFACT_DIR;
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{
  const page=await browser.newPage({viewport:{width:1600,height:1100},deviceScaleFactor:1}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<style>html,body{margin:0;background:#101f1c}canvas{display:block;width:1600px;height:1100px}</style><canvas id="game"></canvas>');
  const manifest={},sources={},assetRoot=path.join(root,'assets/visual');
  for(const family of fs.readdirSync(assetRoot).filter(x=>fs.existsSync(path.join(assetRoot,x,'manifest.json')))){
   manifest[family]=JSON.parse(fs.readFileSync(path.join(assetRoot,family,'manifest.json')));
   for(const a of manifest[family].atlases)sources[a.id]='data:image/'+path.extname(a.file).slice(1)+';base64,'+fs.readFileSync(path.join(assetRoot,a.file)).toString('base64');
  }
  await page.evaluate(b=>window.ATS_VISUAL_BUNDLE=b,{manifest,sources});
  const build=fs.readFileSync(path.join(root,'build.cjs'),'utf8'),parts=[...build.match(/const parts=\[([^\]]+)\]/)[1].matchAll(/'([^']+)'/g)].map(m=>m[1]).filter(n=>n!=='app'&&!n.endsWith('-ui'));
  for(const name of parts)await page.addScriptTag({content:fs.readFileSync(path.join(root,name+'.js'),'utf8')});
  await page.evaluate(()=>ATSVisualAssets.ready);assert.equal(await page.evaluate(()=>ATSVisualAssets.status),'ready');
  await page.evaluate(()=>{
   window.worldQA={
    game(seed){const g=new ATSGame(seed,'wanderer');g.nextSpawnAt=g.nextDirectorAt=g.nextConvoyAt=g.nextFieldOperation=g.nextRegionalOperation=g.nextMajorOffensive=g.nextReinforcementAt=1e9;g.raidTimer=g.heliTimer=1e9;return g},
    renderer(g,x,y,zoom=1){const r=new ATSRenderer(document.getElementById('game'));r.quality='high';r.camera.x=x;r.camera.y=y;r.camera.zoom=zoom;r.lastWorld=g.world;return r},
    signature(g){return JSON.stringify({actors:[g.king,...g.apes,...g.humans,...g.vehicles,...g.helis].map(a=>[a.id,a.x,a.y,a.hp,a.state,a.settlementId]),villages:g.settlements.map(s=>[s.id,s.population,s.food,s.wood,...s.huts.map(h=>[h.id,h.stage,h.hp]),...s.projects.map(p=>[p.id,p.stage,p.progress])]),objects:[...g.world.objects.values()].map(o=>[o.id,o.hp,o.dead,o.solid])})},
    draw(g,r,title,subtitle){const before=this.signature(g);for(let i=0;i<24;i++)r.draw(g,0);const pure=before===this.signature(g);const c=r.ctx;c.fillStyle='rgba(9,22,19,.86)';c.fillRect(24,22,1110,80);c.font='700 26px system-ui';c.fillStyle='#e4d8af';c.fillText(title,43,56);c.font='14px system-ui';c.fillStyle='#b9cdb8';c.fillText(subtitle,43,84);return pure},
    clearPlot(g,p){for(const id of p.treeIds||[]){const t=g.world.objects.get(id);if(t){t.dead=true;t.solid=false;t.hp=0;t.clearedBy='qa-construction'}}p.treeIds=[];g.world.navRevision++},
   };
  });
  const village=await page.evaluate(()=>{
   const q=worldQA,g=q.game('WORLD-ART-VILLAGE');let origin=null;
   for(let y=-700;y<800&&!origin;y+=140)for(let x=-700;x<800&&!origin;x+=140)if(!g.world.terrain(x,y).water&&!g.world.blocked(x,y,25)&&!g.world.getSites(x,y,300).some(s=>!s.cleared&&s.guards>0))origin={x,y};
   if(!origin)throw new Error('No safe founding plot');Object.assign(g.king,origin);g.food=1000;
   for(let i=0;i<30;i++){const p=g.findOpen(origin.x+Math.cos(i*2.4)*60,origin.y+Math.sin(i*2.4)*60);g.makeApe(p.x,p.y,'follow')}
   g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);if(!g.command('settleAll'))throw new Error('Village founding failed');
   const s=g.settlements[0];Object.assign(s,{food:1000,wood:1000,radius:390,developedRadius:390,birthTimer:-100000,_economyAt:1e9});g.colonies.layout(s);
   const construction=[];for(let stage=0;stage<5;stage++){
    const p=g.colonies.queue(s,'hut');if(!p)throw new Error('No hut plot '+stage);q.clearPlot(g,p);const h=s.huts.find(h=>h.id===p.structureId);
    Object.assign(p,{stage,progress:stage/4,work:p.totalWork*stage/4});Object.assign(h,{stage,progress:stage/4});
    if(stage===4){g.colonies.complete(s,p);p.done=true;p.completedAt=g.time}s.projects=s.projects.filter(p=>!p.done);construction.push(h);
   }
   for(const kind of ['garden','storage','workShelter','cooking','training','spearTower']){const p=g.colonies.queue(s,kind);if(!p)continue;q.clearPlot(g,p);g.colonies.complete(s,p);p.done=true}s.projects=s.projects.filter(p=>!p.done);
   // A developed settlement has already harvested its communal clearing.
   // Keep perimeter woodland and all building footprints/navigation intact.
   for(const tree of g.world.getObjects(s.x,s.y,270))if(tree.type==='tree'&&!tree.dead&&Math.hypot(tree.x-s.x,tree.y-s.y)<245){tree.dead=true;tree.solid=false;tree.hp=0;tree.clearedBy=s.id}
   g.world.navRevision++;
   g.world.syncSettlementBuildings(s,g);g.syncIndexes();g.refreshSettlements();g.colonies.assignJobs(s,g.colonies.members(s));const before=g.apes.map(a=>[a.x,a.y]);
   for(let i=0;i<480;i++)g.update(1/60,{});
   const r=q.renderer(g,s.x,s.y,1.1),pure=q.draw(g,r,'A LIVING APE SETTLEMENT','30 assigned residents • foundations, frames, half-built walls, finishing and occupied homes');
   window.qaVillage={g,r,s};return{population:s.population,apes:g.apes.length,stages:construction.map(h=>h.stage),moved:g.apes.filter((a,i)=>Math.hypot(a.x-before[i][0],a.y-before[i][1])>1).length,facilities:s.facilities.length,structures:s.structures.length,renderPure:pure};
  });
  assert.equal(village.apes,30);assert.equal(village.population,30);assert.deepEqual(village.stages,[0,1,2,3,4]);assert.ok(village.moved>0);assert.equal(village.renderPure,true);
  const screenshot=async name=>{if(output){fs.mkdirSync(output,{recursive:true});await page.screenshot({path:path.join(output,name+'.png')})}};
  await screenshot('world-settlement-construction');
  const crossing=await page.evaluate(()=>{
   const q=worldQA,g=q.game('WORLD-ART-RIVER'),w=g.world,b=w.crossingsNear(1300,1300,3000).find(b=>b.type==='stone')||w.crossing(0,0);
   w.ensure(b.x,b.y,1100);Object.assign(g.king,{x:b.x+120,y:b.maxY+170});g.viewRadius=750;
   for(let i=0;i<50;i++){const p=g.findOpen(b.x-120+(i%5)*23,b.minY-170-Math.floor(i/5)*15);g.makeApe(p.x,p.y,'follow')}
   g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.command('recallField');const r=q.renderer(g,b.x,b.y,1.35),start=g.apes.map(a=>[a.x,a.y]);
   window.qaCrossing={g,w,b,r,start,lanes:new Set(),waterEntries:0,maxOnDeck:0};return{type:b.type,width:b.width,apes:g.apes.length};
  });assert.equal(crossing.apes,50);assert.ok(crossing.width>=104);
  const middle=await page.evaluate(()=>{
   const q=worldQA,{g,w,b,r,lanes}=qaCrossing;let best=null;
   for(let frame=0;frame<540;frame++){
    g.update(1/60,{});const onDeck=g.apes.filter(a=>a.y>=b.minY&&a.y<=b.maxY&&a.x>=b.minX&&a.x<=b.maxX).length;
    qaCrossing.maxOnDeck=Math.max(qaCrossing.maxOnDeck,onDeck);
    for(const a of g.apes){if(w.waterBlocked(a.x,a.y,10))qaCrossing.waterEntries++;if(a._navCrossing?.phase===1)lanes.add(Math.round(a._navCrossing.far.x-b.x))}
    if(onDeck>=10){best={frame,onDeck};break}
   }
   const pure=q.draw(g,r,'THE HORDE USES A REAL RIVER CROSSING','Fifty recalled apes • generated stone deck • original water collision and actual navigation updates');return{...best,renderPure:pure,maxOnDeck:qaCrossing.maxOnDeck};
  });assert.ok(middle.maxOnDeck>0);assert.equal(middle.renderPure,true);await screenshot('world-crossing-in-motion');
  const arrived=await page.evaluate(()=>{
   const q=worldQA,{g,w,b,r,lanes,start}=qaCrossing;
   for(let frame=0;frame<1200;frame++){
    g.update(1/60,{});for(const a of g.apes){if(w.waterBlocked(a.x,a.y,10))qaCrossing.waterEntries++;if(a._navCrossing?.phase===1)lanes.add(Math.round(a._navCrossing.far.x-b.x))}
    if(g.apes.every(a=>a.y>b.maxY+18))break;
   }
   const pure=q.draw(g,r,'THE HORDE REACHES THE FAR BANK','The same fifty apes remain in the simulation; the bridge artwork matches their legal crossing corridor');
   return{apes:g.apes.length,crossed:g.apes.filter(a=>a.y>b.maxY+18).length,waterEntries:qaCrossing.waterEntries,lanes:lanes.size,moved:g.apes.filter((a,i)=>Math.hypot(a.x-start[i][0],a.y-start[i][1])>50).length,renderPure:pure};
  });assert.equal(arrived.apes,50);assert.ok(arrived.crossed>=45,JSON.stringify(arrived));assert.equal(arrived.waterEntries,0);assert.ok(arrived.lanes>=2);assert.equal(arrived.renderPure,true);await screenshot('world-crossing-far-bank');
  const military=await page.evaluate(()=>{
   const q=worldQA,g=q.game('military-bases'),w=g.world;let chosen=null;
   for(let cy=-24;cy<=24&&!chosen;cy++)for(let cx=-24;cx<=24&&!chosen;cx++){const p=w._sitePlan(cx,cy);if(p?.type==='regionalCommand')chosen={p,cx,cy}}
   if(!chosen)throw new Error('No regional command blueprint');w._generateChunk(chosen.cx,chosen.cy);const site=w.sites.get(chosen.p.id);w.ensure(site.x,site.y,1100);g.viewRadius=1100;
   Object.assign(g.king,{x:site.x,y:site.y+(site.extentY||site.radius-74)+160});
   for(let i=0;i<8;i++){const point=g.findOpen(site.x+80+i%4*32,site.y-30+Math.floor(i/4)*35);g.makeHuman(point.x,point.y,site)}
   for(const kind of ['truck','apc','tank'])g.makeMilitaryVehicle(site,kind,{x:site.x,y:site.y+1000});
   g.helis.push({id:'heli-qa-world',kind:'gunship',x:site.x+170,y:site.y-170,dir:.5,phase:5,hp:360,maxHp:360,spotX:site.x+180,spotY:site.y-90,armed:true,shootTimer:10,life:110,siteId:site.id,target:{x:site.x+100,y:site.y+70}});
   g.syncIndexes();for(let i=0;i<30;i++)g.update(1/60,{});
   const r=q.renderer(g,site.x,site.y,.8),pure=q.draw(g,r,'HUMAN REGIONAL COMMAND','Generated fortified base • illustrated barracks, command building and garages • live armor and gunship');
   const objects=site.objects.map(id=>w.objects.get(id)).filter(Boolean);window.qaMilitary={g,r,site};return{type:site.type,humans:g.humans.length,vehicles:g.vehicles.length,helis:g.helis.length,command:objects.filter(o=>o.commandCenter).length,garages:objects.filter(o=>o.repairBay).length,barracks:objects.filter(o=>o.type==='barracks').length,renderPure:pure};
  });assert.equal(military.type,'regionalCommand');assert.ok(military.vehicles>=3);assert.equal(military.helis,1);assert.ok(military.command>0&&military.garages>0&&military.barracks>0);assert.equal(military.renderPure,true);await screenshot('world-human-regional-command');
  assert.deepEqual(errors,[]);const report={village,crossing,middle,arrived,military,errors};if(output)fs.writeFileSync(path.join(output,'world-art-browser.json'),JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
