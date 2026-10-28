const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {assetPath,enablePixelInspection}=require('./helpers/cloudflare-assets.cjs');
const repo=path.resolve(__dirname,'../../..');
const expose=`window.tcCampaign={run:source=>eval(source), engine:window.TurncraftExpansion, catalog:CATALOG};resize();`;
const stubs=`const initializeApp=()=>({}),getApps=()=>[],getApp=()=>({}),getAuth=()=>({currentUser:null}),getFirestore=()=>({}),saveLobbyResult=async()=>{},rememberLobby=()=>{},selectedUsername=()=> 'CampaignTester',turncraftRankFor=()=>({}),restoreFirebaseUser=async()=>({}),firebaseError=e=>e.message,completeFirestoreMatch=async()=>{};`;
const source=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8')
  .replace(/^import .*https:\/\/www.gstatic.com.*;$/gm,'')
  .replace(/^import .*from '\.\/turncraft\/shattered-front\/(?:profile|firebase-connection|lobby-store)\.mjs';$/gm,'')
  .replace("(() => {\n'use strict';",stubs+"\n(() => {\n'use strict';")
  .replace("(() => {\r\n'use strict';",stubs+"\n(() => {\n'use strict';")
  .replace(/\nboot\(\);/,`\n${expose}\nwindow.TurncraftStartup.complete();`)
  .replace('<script type="module" src="turncraft/shattered-front/menu.mjs"></script>','');
async function setup(viewport={width:1440,height:900}){
  const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH,args:['--no-sandbox']});
  const page=await browser.newPage({viewport}),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.addInitScript(enablePixelInspection);
  await page.route('**/*',route=>{
    const art=assetPath(route.request().url());if(art)return route.fulfill({path:art,headers:{'Access-Control-Allow-Origin':'*'}});
    const pathname=decodeURIComponent(new URL(route.request().url()).pathname);if(pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});
    const file=path.resolve(repo,'.'+pathname);if(file.startsWith(repo+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file,contentType:file.endsWith('.mjs')||file.endsWith('.js')?'application/javascript':undefined});
    return route.fulfill({status:404,body:''});
  });
  await page.goto('http://turncraft.test/game/turncraft.html');try{await page.waitForFunction(()=>!!window.tcCampaign,{}, {timeout:10000});assert.deepEqual(errors,[]);}catch(e){await browser.close();throw Error(e.message+'; browser errors: '+errors.join('; '));}return {browser,page,errors};
}

test('all24 real mission adapters open, honor stage locks, complete objectives and restore their actual entities',async()=>{
  const {browser,page,errors}=await setup();
  try{
    const report=await page.evaluate(()=>{
      const rows=[];
      for(const mission of tcCampaign.catalog.missions){
        tcCampaign.engine.startCampaign(mission.id,{seed:591,factionId:['frontier','ascendant','brood'][mission.index%3],difficulty:'easy'});
        let state=tcCampaign.engine.getState();
        if(state.operation.stage!==0)throw Error(mission.id+' wrong opening stage');
        const locked=state.objectives.find(o=>o.type==='destroy'&&o.stage>0);
        if(locked){const before=tcCampaign.run('entityById('+locked.entityId+')?.hp');tcCampaign.run('applyDamage(entityById('+locked.entityId+'),90000,playerUnits(0)[0])');if(tcCampaign.run('entityById('+locked.entityId+')?.hp')!==before)throw Error(mission.id+' future target is vulnerable');}
        let restored=false;
        for(let n=0;n<900&&tcCampaign.engine.isActive();n++){
          state=tcCampaign.engine.getState();
          for(const o of state.objectives.filter(o=>o.stage===state.operation.stage)){
            if(o.type==='destroy')tcCampaign.run('(()=>{const target=entityById('+o.entityId+');if(target)applyDamage(target,90000,playerUnits(0)[0])})()');
            else if(o.type==='escort')tcCampaign.run('(()=>{const e=entityById('+o.entityId+');if(e){e.x='+o.to.x+';e.y='+o.to.y+';}})()');
            else if(['capture','discover','hold','rescue'].includes(o.type))tcCampaign.run('(()=>{for(const e of entities.filter(e=>e.alive!==false&&e.faction===1&&dist(e,{x:'+o.x+',y:'+o.y+'})<4))applyDamage(e,90000,playerUnits(0)[0]);const type='+JSON.stringify(o.workers?'worker':'combat')+"==='worker'?FACTIONS[chosenFaction].worker:({frontier:'rifleman',ascendant:'zealot',brood:'roach'}[chosenFaction]);if(!entities.some(e=>e.alive!==false&&e.faction===0&&e.type===type&&dist(e,{x:"+o.x+',y:'+o.y+'})<1))addEntity(\'unit\',type,0,'+o.x+','+o.y+');})()');
          }
          tcCampaign.run('for(const e of entities.filter(e=>e.alive!==false&&e.sfWave))applyDamage(e,90000,playerUnits(0)[0])');
          tcCampaign.engine.tick(1);
          if(!restored&&tcCampaign.engine.isActive()&&tcCampaign.engine.getState().operation.stage>0){const save=tcCampaign.engine.checkpoint();tcCampaign.engine.abandon();tcCampaign.engine.resume(save);if(tcCampaign.engine.getState().operation.stage!==save.operation.stage)throw Error(mission.id+' stage restore failed');restored=true;}
        }
        state=tcCampaign.engine.getState();if(state.phase!=='victory')throw Error(mission.id+' stalled at '+state.stageTitle);
        rows.push({id:mission.id,phase:state.phase,economy:state.enemyEconomy,stages:state.operation.completedStages.length});
      }
      return rows;
    });
    assert.equal(report.length,24);assert.equal(report.filter(r=>r.economy).length,5);assert.ok(report.every(r=>r.phase==='victory'));assert.deepEqual(errors,[]);
  }finally{await browser.close();}
});

test('mission HUD keeps focus, details and radio nodes stable; dialogue dismisses and logs without blocking play',async()=>{
  for(const viewport of [{width:1440,height:900},{width:390,height:844}]){
    const {browser,page,errors}=await setup(viewport);
    try{
      await page.evaluate(()=>{tcCampaign.engine.startCampaign('amber-wastes-4',{seed:3});document.querySelector('#sfBattleHud details').open=true;window.savedDetails=document.querySelector('#sfBattleHud details');window.savedRadio=document.getElementById('sfDialogue');for(let n=0;n<40;n++)tcCampaign.engine.tick(.5);});
      assert.equal(await page.evaluate(()=>savedDetails===document.querySelector('#sfBattleHud details')&&savedDetails.open&&savedRadio===document.getElementById('sfDialogue')),true);
      const bounds=await page.locator('#sfBattleHud').boundingBox();assert.ok(bounds.x>=0&&bounds.x+bounds.width<=viewport.width+1);
      await page.locator('#sfDialogue button').filter({hasText:'Skip dialogue'}).click();assert.equal(await page.locator('#sfDialogue').isVisible(),false);
      await page.locator('#sfBattleHud details').nth(1).locator('summary').click();assert.match(await page.locator('#sfBattleHud details').nth(1).textContent(),/refinery/i);
      await page.evaluate(()=>tcCampaign.run("selectEntity(playerUnits(0).find(e=>!isWorker(e)));"));assert.ok(await page.locator('#selectionName').textContent());assert.deepEqual(errors,[]);
    }finally{await browser.close();}
  }
});

test('authored tactical choices change real ambush pressure, engine weapons, sector paths and dock targeting',async()=>{
  const {browser,page,errors}=await setup();
  try{
    const report=await page.evaluate(()=>{
      const tc=tcCampaign,step=n=>{for(let i=0;i<n;i++){window.campaignClock+=1000;tc.engine.tick(1);}},place=id=>{const o=tc.engine.getState().objectives.find(o=>o.id===id);tc.run(`addEntity('unit','rifleman',0,${o.x},${o.y})`);},kill=id=>{const o=tc.engine.getState().objectives.find(o=>o.id===id);tc.run(`applyDamage(entityById(${o.entityId}),90000,playerUnits(0)[0])`);step(1);};
      window.campaignClock=100000;tc.run('nowMs=()=>window.campaignClock');
      tc.engine.startCampaign('ironwood-basin-1',{seed:12,factionId:'frontier'});place('scouts');const rescueBefore=tc.run('playerUnits(0).length');step(7);const rescue={noBase:!tc.run('playerStructures(0).some(isBase)'),before:rescueBefore,after:tc.run('playerUnits(0).length'),completed:tc.engine.getState().objectives.find(o=>o.id==='scouts').completed};
      const ambush=[];
      for(const choice of ['north','south']){
        tc.engine.startCampaign('ironwood-basin-1',{seed:12,factionId:'frontier'});for(const id of ['route-north','route-center','route-south'])place(id);step(3);
        tc.run('for(const e of playerUnits(0)){e.x=3;e.y=15;}');place('ambush-'+choice);step(9);
        const selected=tc.engine.getState().operation.ambushRoute;step(7);const state=tc.engine.getState();ambush.push({choice,selected,routes:state.wavePlan.routes,target:tc.run('playerUnits(1).find(e=>e.sfWave)?.attackMoveDestination')});
      }
      const engineWeapons=[];
      for(const faction of ['frontier','ascendant','brood']){
        tc.engine.startCampaign('ironwood-basin-3',{seed:12,factionId:faction});for(const id of ['control-north','control-south'])place(id);step(13);place('engine');step(23);
        const damage=tc.run(`(()=>{tileVisibleToFaction=()=>true;const weapon=entities.find(e=>e.sfDefensiveEngine),enemy=addEntity('unit','zergling',1,weapon.x+1,weapon.y);const hp=enemy.hp;fireAt(weapon,enemy);window.campaignClock+=1000;resolveProjectileImpacts();return {type:weapon.type,weapon:!!attackDef(weapon),powered:structurePowered(weapon),before:hp,after:enemy.hp};})()`);engineWeapons.push({faction,...damage});
      }
      tc.engine.startCampaign('ashglass-expanse-4',{seed:12,factionId:'frontier'});step(16);const initialPlan=tc.engine.getState().wavePlan;
      const closed=tc.run('groundBlocked(29,7,playerUnits(0)[0])');kill('generator-a');const opened=tc.run('groundBlocked(29,7,playerUnits(0)[0])'),centralClosed=tc.run('groundBlocked(29,15,playerUnits(0)[0])');
      for(let i=0;i<100&&tc.engine.getState().wave<2;i++){tc.run('for(const e of playerUnits(1).filter(e=>e.sfWave))applyDamage(e,90000,playerUnits(0)[0])');step(1);}
      const reducedPlan=tc.engine.getState().wavePlan,save=tc.engine.checkpoint();tc.engine.abandon();tc.engine.resume(save);const restoredGate=tc.run('groundBlocked(29,7,playerUnits(0)[0])');
      // Older checkpoints have no gate metadata or newly authored targets. Keep destroyed sectors open and materialize the later target on stage entry.
      const legacy=tc.engine.checkpoint();delete legacy.operation.sectorsReady;delete legacy.operation.closedSectors;legacy.objectives=legacy.objectives.filter(o=>o.id!=='generator-b');legacy.simulation.entities=legacy.simulation.entities.filter(e=>e.sfObjectiveId!=='generator-b');legacy.simulation.temporaryObstacles=legacy.simulation.temporaryObstacles.filter(o=>!o.sfSector);legacy.simulation.terrain=tc.run('campaignLayout("ashglass-expanse",12).terrain');tc.engine.abandon();tc.engine.resume(legacy);const migratedOpen=tc.run('groundBlocked(29,7,playerUnits(0)[0])');kill('generator-c');const migratedTarget=tc.engine.getState().objectives.find(o=>o.id==='generator-b'),migratedTargetExists=tc.run('!!entityById('+migratedTarget.entityId+')');kill('generator-b');const migratedVictory=tc.engine.getState().phase==='victory';
      tc.engine.startCampaign('sundered-coast-2',{seed:12,factionId:'frontier'});place('causeway');step(17);const docks=tc.engine.getState().objectives.filter(o=>o.id.includes('dock'));tc.run(`entityById(${docks[0].entityId}).hp=1000;entityById(${docks[1].entityId}).hp=250;`);step(7);const weakSouth=tc.engine.getState().operation.assaultTargetId;tc.run(`entityById(${docks[0].entityId}).hp=100;entityById(${docks[1].entityId}).hp=1200;`);step(4);const weakNorth=tc.engine.getState().operation.assaultTargetId;
      return {rescue,ambush,engineWeapons,sectors:{closed,opened,centralClosed,restoredGate,migratedOpen,migratedTargetExists,migratedVictory,before:initialPlan.composition.length,after:reducedPlan.composition.length,routes:reducedPlan.routes},weakSouth,weakNorth,docks};
    });
    assert.deepEqual(report.ambush.map(r=>r.routes),[[0],[2]]);for(const row of report.ambush){assert.equal(row.selected.name,row.choice);assert.equal(row.target.x,row.selected.x);assert.equal(row.target.y,row.selected.y);}
    assert.ok(report.rescue.noBase&&report.rescue.completed);assert.equal(report.rescue.after-report.rescue.before,3);
    assert.ok(report.engineWeapons.every(row=>row.weapon&&row.powered&&row.after<row.before),JSON.stringify(report.engineWeapons));
    assert.deepEqual({closed:report.sectors.closed,opened:report.sectors.opened,centralClosed:report.sectors.centralClosed,restoredGate:report.sectors.restoredGate},{closed:true,opened:false,centralClosed:true,restoredGate:false});assert.ok(report.sectors.after<report.sectors.before);assert.equal(report.sectors.routes.includes(3),false);
    assert.equal(report.sectors.migratedOpen,false);assert.ok(report.sectors.migratedTargetExists&&report.sectors.migratedVictory);
    assert.equal(report.weakSouth,report.docks[1].entityId);assert.equal(report.weakNorth,report.docks[0].entityId);assert.deepEqual(errors,[]);
  }finally{await browser.close();}
});
