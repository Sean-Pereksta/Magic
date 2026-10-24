/* End-to-end gameplay regression. node game/circuitbound/tests/gameplay-browser.cjs
   The first section uses an unchanged seeded generated world. The later quarry is an
   explicit deterministic terrain fixture; all inventory, crafting, power, combat and
   campaign progress still come from the real game. No production debug API is added. */
'use strict';
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {pathToFileURL}=require('node:url');
const {playwright,launchOptions}=require('./browser-runtime.cjs');
const root=path.resolve(__dirname,'../../..'),shots=path.resolve(process.env.CIRCUITBOUND_SCREENSHOTS||path.join(root,'../work/circuitbound-gameplay'));
fs.mkdirSync(shots,{recursive:true});
const injected=String.raw`
  globalThis.testAPI={
    read(){return {player:{...player},inventory:{...inventory},stats:{...stats},time:gameTime,elapsed:survival.elapsed,chapter:survival.chapter,journal:survival.getJournal(),hud:survival.getHud(),enemies:survival.enemies.map(e=>({...e})),metrics:{...engine.metrics},drops:engine.drops.map(d=>({...d})),power:{supply:engine.supply,load:engine.load}};},
    tile(x,y){return {id:get(x,y),name:defs[get(x,y)].name,meta:{...(meta.get(idx(x,y))||{})}};},
    screen(x,y){return {x:(x+.5)*TILE-camera.x,y:(y+.5)*TILE-camera.y};},
    aim(x,y){Object.assign(pointer,{tx:x,ty:y,wx:(x+.5)*TILE,wy:(y+.5)*TILE,x:(x+.5)*TILE-camera.x,y:(y+.5)*TILE-camera.y});},
    select(key,mode='build'){const found=items.findIndex(i=>i.key===key);if(found<0)throw Error('Unknown item '+key);selected=found;inputMode=mode;manualRotation=true;renderHotbar();ui.refresh();},
    advance(seconds,fight=false){for(let left=seconds;left>1e-7;left-=.05){const dt=Math.min(.05,left);if(modalOpen)throw Error('Simulation paused by dialog');gameTime+=dt;for(let part=dt;part>0;part-=.016)movePlayer(Math.min(.016,part));mineStep(dt);simulationStep(dt);if(fight){const p={x:player.x+player.w/2,y:player.y+player.h/2},e=survival.enemies.filter(e=>e.hp>0&&Math.hypot(e.x-p.x,e.y-p.y)<65).sort((a,b)=>Math.hypot(a.x-p.x,a.y-p.y)-Math.hypot(b.x-p.x,b.y-p.y))[0];if(e)survival.attack(e.x,e.y);const above=survival.enemies.some(e=>e.hp>0&&e.y<p.y-40&&Math.hypot(e.x-p.x,e.y-p.y)<125);if(this.jumpCombat&&above)keys.Space=true;if(this.serviceLadder){const low=survival.enemies.some(e=>e.hp>0&&e.y>TILE*55&&Math.abs(e.x-p.x)<TILE*6),high=survival.enemies.some(e=>e.hp>0&&e.y<TILE*54&&Math.abs(e.x-p.x)<TILE*6),target=TILE*(high?53:low?55:53);keys.KeyS=p.y<target-4;keys.KeyW=p.y>target+4;}if(player.health<42)survival.heal();}}if(seconds>.1)tick(last);return this.read();},
    walk(x,limit=15){const target=(x+.5)*TILE-player.w/2;let old=player.x,stuck=0;for(let n=0;n<limit/.05;n++){const delta=target-player.x;if(Math.abs(delta)<5&&Math.abs(player.vx)<12)break;keys.KeyD=delta>5;keys.KeyA=delta< -5;if(Math.abs(player.x-old)<.1)stuck++;else stuck=0;old=player.x;if(stuck>4)keys.Space=true;this.advance(.05,true);}keys.KeyD=keys.KeyA=false;this.advance(.15,true);if(Math.abs(target-player.x)>12)throw Error('Walk failed to '+x+' at '+player.x/TILE+','+player.y/TILE);return this.read();},
    climb(y,limit=15){const target=y*TILE-player.h/2;for(let n=0;n<limit/.05;n++){const delta=target-player.y;if(Math.abs(delta)<5)break;keys.KeyS=delta>0;keys.KeyW=delta<0;this.advance(.05,true);}keys.KeyS=keys.KeyW=false;this.advance(.1,true);if(Math.abs(player.y-target)>16)throw Error('Climb failed to '+y+' at '+player.x/TILE+','+player.y/TILE);return this.read();},
    craft(key,count=1){const recipe=recipes.find(r=>r.key===key);for(let i=0;i<count;i++)if(!craft(recipe))throw Error('Cannot craft '+key+' from '+JSON.stringify(inventory));return this.read();},
    place(key,x,y,rot=0){if(get(x,y)!==T.AIR)throw Error('Occupied placement '+x+','+y+' '+defs[get(x,y)].name);this.select(key);rotation=rot;manualRotation=true;this.aim(x,y);const before=inventory[key]||0;useOrPlace();if((inventory[key]||0)!==before-1||get(x,y)!==items[selected].tile)throw Error('Placement failed '+key+' '+x+','+y+' reach='+distanceToTile(x,y));return this.tile(x,y);},
    mine(x,y){const original=get(x,y);if(!original)throw Error('Nothing to mine '+x+','+y);this.select('stone','mine');this.aim(x,y);pointer.down=true;pointer.button=0;for(let n=0;n<110&&get(x,y)===original;n++)this.advance(.05,true);releasePointer(false);if(get(x,y)!==T.AIR)throw Error('Mining failed '+x+','+y+' reach='+distanceToTile(x,y));this.advance(.4,true);return this.read();},
    take(x,y){this.aim(x,y);interactItems();return this.read();},
    insert(key,count,x,y){this.select(key);this.aim(x,y);const before=inventory[key]||0;keys.ShiftLeft=true;for(let i=0;i<count;i++)interactItems();keys.ShiftLeft=false;if((inventory[key]||0)!==before-count)throw Error('Insertion failed '+key+' at '+x+','+y+' '+JSON.stringify({before,after:inventory[key],target:this.tile(x,y),crusher:this.tile(45,54),smelter:this.tile(46,54),player,hud:survival.getHud()}));return this.read();},
    service(enabled){this.serviceLadder=!!enabled;keys.KeyW=keys.KeyS=false;},
    combatJump(enabled){this.jumpCombat=!!enabled;},
    checkpoint(){const result=survival.setCheckpoint();if(!result)throw Error('Checkpoint failed '+JSON.stringify({player,near:survival.nearestMachine(player.x,player.y,true),toast:document.getElementById('toast').textContent}));return result;},
    approachEnemy(id){for(let n=0;n<400;n++){const e=survival.enemies.find(e=>e.id===id);if(!e||e.hp<=0)break;const dx=e.x-(player.x+player.w/2),dy=e.y-(player.y+player.h/2);if(Math.hypot(dx,dy)<54)break;keys.KeyD=dx>0;keys.KeyA=dx<0;if(Math.abs(player.vx)<5)keys.Space=true;this.advance(.05,false);}keys.KeyD=keys.KeyA=false;this.advance(.1,false);return this.read();},
    weapon(key){return survival.selectWeapon(key);},
    attack(x,y){return survival.attack(x*TILE,y*TILE);},
    spawn(){const e=survival.findSpawn('beetle');return e?{...e}:null;},
    save(){saveGame();return localStorage.getItem(SAVE_KEY);},
    quarry(){let carved=0,ores=0;for(let x=14;x<=100;x++)for(let y=43;y<=57;y++){const id=get(x,y);if(id>=1&&id<=8){set(x,y,T.AIR);carved++;}}for(let x=14;x<=100;x++)for(let y=58;y<=73;y++)if(get(x,y)<=8)set(x,y,T.STONE);for(let x=20;x<=39;x++)for(let y=52;y<=55;y++){set(x,y,y===52?T.COPPER:T.IRON);ores++;}for(const x of [58,60])for(let y=55;y<=60;y++){set(x,y,T.IRON);ores++;}simulateNetworks();return {carved,oreTiles:ores,description:'Carved connected quarry x14..100 y43..57, stone floor58 and solid terrain below; copper/iron ceiling veins and two real six-block drill deposits. Existing machines and all inventory preserved.'};}
  };
`;
async function prepare(context,saved){
  await context.addInitScript(({saved})=>{let seed=875312;Math.random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296;};window.requestAnimationFrame=()=>0;if(saved)localStorage.setItem('circuitbound-save-v1',saved);},{saved});
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.route('http://circuitbound.test/**',async route=>{const file=path.resolve(root,'.'+new URL(route.request().url()).pathname);if(!file.startsWith(root+path.sep)||!fs.existsSync(file)){await route.fulfill({status:404,body:''});return;}let body=fs.readFileSync(file);if(file.endsWith(path.join('circuitbound','app.js')))body=Buffer.from(body.toString().replace(/\}\)\(\);\s*$/,injected+'\n})();'));await route.fulfill({contentType:{'.html':'text/html','.css':'text/css','.js':'text/javascript'}[path.extname(file)]||'text/plain',body});});
  await page.goto('http://circuitbound.test/game/circuitbound.html');await page.waitForFunction(()=>window.testAPI);
  if(await page.locator('#welcomePanel').isVisible())await page.locator('#startBtn').click();return{page,errors};
}
const act=(page,method,...args)=>page.evaluate(({method,args})=>testAPI[method](...args),{method,args});
const read=page=>act(page,'read');
async function phase(page,label){const s=await read(page);console.log(label,JSON.stringify({pos:[+(s.player.x/24).toFixed(2),+(s.player.y/24).toFixed(2)],time:+s.elapsed.toFixed(1),chapter:s.chapter,resources:{copper:s.inventory.copper,iron:s.inventory.iron,wood:s.inventory.wood,stone:s.inventory.stone,ammo:s.inventory.ammo},stats:s.stats}));}
(async()=>{const browser=await playwright.chromium.launch(launchOptions);try{
 const context=await browser.newContext({viewport:{width:1440,height:900}}),{page,errors}=await prepare(context);
 let s=await read(page),sawWarning=false,sawAttack=false;const raidsBefore=0;assert.equal(s.stats.copperMined,0);const startX=s.player.x;
 await page.keyboard.down('d');await page.keyboard.down('Space');await act(page,'advance',.1);await page.keyboard.up('Space');await act(page,'advance',.8);await page.keyboard.up('d');assert.ok((await read(page)).player.x>startX+60,'Normal-world keyboard moves the player over the outpost machines');
 await act(page,'walk',17);await act(page,'climb',36.5);const beforeClimb=(await read(page)).player.y;
 await page.keyboard.down('w');await act(page,'advance',.4);await page.keyboard.up('w');assert.ok((await read(page)).player.y<beforeClimb-30,'Existing ladder climbs with actual keyboard input');await act(page,'climb',36.5);
 for(let x=18;x<=22;x++)await act(page,'place','platform',x,38);
 await act(page,'walk',20);for(let x=23;x<=25;x++)await act(page,'place','platform',x,38);await act(page,'walk',24);for(let x=26;x<=27;x++)await act(page,'place','platform',x,38);
 await act(page,'select','stone','mine');
 for(let x=20;x<=27;x++){const target=await act(page,'screen',x,35);await page.mouse.move(target.x,target.y);await page.mouse.down();await act(page,'advance',1.4);await page.mouse.up();assert.equal((await act(page,'tile',x,35)).id,0,'Normal generated copper is removed by hold-to-mine');await act(page,'advance',.4);}
 await act(page,'walk',20);await act(page,'walk',26);s=await read(page);assert.equal(s.stats.copperMined,8);assert.ok(s.inventory.copper>=20,'Normal mining yields real collected copper');
 const mobileSave=await act(page,'save');await page.screenshot({path:path.join(shots,'01-normal-copper.png')});await phase(page,'normal movement, mining, platforms and climbing');
 await act(page,'craft','reinforcedPickaxe');await act(page,'walk',17);await act(page,'climb',28.5);await page.keyboard.down('Space');await act(page,'advance',.2);await page.keyboard.up('Space');await act(page,'walk',9);
 // The outpost repair uses the same component placement rules as player builds.
 await act(page,'place','wire',9,27);await act(page,'place','generator',11,28);await act(page,'advance',1.5);
 s=await read(page);assert.equal(s.stats.coreReached,true,'Outpost core is restored through real power');assert.equal(s.chapter,2,'Working machinery and core complete chapter one');await act(page,'take',14,28);await act(page,'craft','miningArmor');
 const fixture=await act(page,'quarry');console.log('EXPLICIT TERRAIN FIXTURE',JSON.stringify(fixture));
 await act(page,'walk',17);await act(page,'climb',40);
 for(let y=43;y<=45;y++)await act(page,'place','ladder',17,y);await act(page,'climb',44);for(let y=46;y<=48;y++)await act(page,'place','ladder',17,y);
 await act(page,'climb',47);const lower=(await read(page)).player.y;await page.keyboard.down('w');await act(page,'advance',.5);await page.keyboard.up('w');assert.ok((await read(page)).player.y<lower-40,'New ladder sections support climbing back up');await act(page,'climb',47);
 await act(page,'walk',20);await act(page,'advance',2);
 for(let x=20;x<=39;x++){await act(page,'walk',x);for(let y=55;y>=52;y--)await act(page,'mine',x,y);if(x===20){await act(page,'craft','hammer');await act(page,'weapon','hammer');}}
 for(let x=40;x<45;x++){await act(page,'walk',x);await act(page,'mine',x,58);}
 await act(page,'craft','medkit',3);await phase(page,'real quarry mining and equipment');
 // Powered drill -> hopper -> independently driven conveyor -> storage.
 await act(page,'craft','generator',3);await act(page,'craft','motor');await act(page,'craft','drill');await act(page,'craft','hopper');await act(page,'craft','storage');
 const miner=[['generator',58,52,0],['motor',58,53,1],['drill',58,54,1],['generator',59,54,0],['hopper',57,54,2],['conveyor',56,54,2],['storage',55,54,2],['motor',56,53,1],['generator',56,52,0]];
 await act(page,'walk',55);for(const p of miner)await act(page,'place',...p);await act(page,'advance',1,true);assert.equal(await act(page,'checkpoint'),true);
  const beforeFull=await read(page);await act(page,'insert','dirt',28,55,54);await act(page,'insert','stone',16,55,54);await act(page,'insert','iron',20,55,54);await act(page,'insert','iron',8,57,54);await act(page,'insert','iron',8,56,54);await act(page,'advance',3,true);
 assert.equal((await read(page)).metrics.mined,0,'Full storage and transport buffers stop the drill before consuming terrain');assert.equal((await act(page,'tile',58,54)).meta.powered,true);assert.match((await act(page,'tile',58,54)).meta.machineFault,/OUTPUT BLOCKED/);assert.equal((await act(page,'tile',58,55)).name,'Iron Ore');
 await act(page,'take',55,54);await act(page,'take',57,54);await act(page,'take',56,54);const afterFull=await read(page);assert.equal(afterFull.inventory.dirt,beforeFull.inventory.dirt);assert.equal(afterFull.inventory.stone,beforeFull.inventory.stone);
 await act(page,'advance',20,true);s=await read(page);assert.equal(s.metrics.mined,6,'Drill consumes six real ore tiles');assert.equal((await act(page,'tile',55,54)).meta.contents.iron,6,'Ore traverses hopper and powered conveyor into storage');
 await page.screenshot({path:path.join(shots,'02-powered-miner.png')});
 await act(page,'take',55,54);
 for(const p of [...miner].reverse())await act(page,'mine',p[1],p[2]);await act(page,'craft','ladder');await act(page,'walk',58);await act(page,'advance',1,true);for(let y=57;y<=60;y++)await act(page,'place','ladder',58,y);await act(page,'climb',57);
 await act(page,'walk',57);for(const p of miner)await act(page,'place',p[0],p[1]+2,p[2],p[3]);await act(page,'advance',20,true);
 s=await read(page);assert.equal(s.metrics.mined,12);assert.equal(s.chapter,3,'Real route, depth and twelve automatically mined ore unlock the next region');assert.ok(s.journal.unlocks.includes('Crystal Hollows'));
 await act(page,'walk',57);await act(page,'take',57,54);for(const p of [...miner].reverse())await act(page,'mine',p[1]+2,p[2]);await act(page,'craft','ladder');await act(page,'walk',60);await act(page,'advance',1,true);for(let y=57;y<=60;y++)await act(page,'place','ladder',60,y);await act(page,'climb',57);await phase(page,'automation and chapter unlock');if((await read(page)).hud.raid.state==='warning'){sawWarning=true;await page.screenshot({path:path.join(shots,'04-raid-warning.png')});}
 // Begin defense with the legitimately earned chapter reward, then manufacture its replacement ammunition.
 await act(page,'craft','crusher');await act(page,'craft','smelter');await act(page,'craft','turret');
 const processing=[['generator',45,52,0],['motor',45,53,1],['crusher',45,54,0],['generator',45,55,0],['hopper',44,54,0],['smelter',46,54,0],['copperBlock',46,55,0],['storage',47,54,0],['copperBlock',47,55,0],['turret',48,54,0],['generator',48,55,0]];
 await act(page,'craft','ladder');await act(page,'walk',42.85);for(let y=53;y<=57;y++)await act(page,'place','ladder',43,y);await act(page,'climb',53);for(const p of processing)await act(page,'place',...p);await act(page,'insert','ammo',8,48,54);await act(page,'advance',1,true);await act(page,'checkpoint');await act(page,'climb',53);await act(page,'service',true);
 
 await act(page,'insert','iron',8,44,54);await act(page,'advance',25,true);
 assert.equal((await act(page,'tile',47,54)).meta.contents.ironIngot,8,'Crusher and smelter manufacture eight ingots from eight real iron ore');
 if((await read(page)).hud.raid.state==='attack'){sawAttack=true;await page.screenshot({path:path.join(shots,'05-base-attack.png')});}await page.screenshot({path:path.join(shots,'03-processing-and-defense.png')});await act(page,'take',47,54);
 await act(page,'mine',47,54);await act(page,'craft','ammoFabricator');await act(page,'place','ammoFabricator',47,54);await act(page,'insert','ironIngot',4,47,54);
 await act(page,'insert','iron',8,44,54);await act(page,'advance',25,true);
 for(let wait=0;wait<12&&(await act(page,'tile',46,54)).meta.totalProduced<16;wait++){
   for(const [key,x,y,rot] of [['generator',45,52,0],['motor',45,53,1],['generator',45,55,0],['generator',48,55,0],['copperBlock',46,55,0],['copperBlock',47,55,0]])if((await act(page,'tile',x,y)).id===0){if(!((await read(page)).inventory[key]>0))await act(page,'craft',key);await act(page,'place',key,x,y,rot);console.log('Repaired raid damage using recovered/manufactured '+key+'.');}
   await act(page,'advance',5,true);
 }
 await act(page,'advance',5,true);
 if((await act(page,'tile',46,54)).meta.totalProduced<16)console.log('FACTORY_BLOCKAGE',JSON.stringify(await Promise.all([[45,52],[45,53],[45,54],[46,54],[47,54]].map(([x,y])=>act(page,'tile',x,y)))));
 assert.equal((await act(page,'tile',46,54)).meta.totalProduced,16,'Sixteen real ore become sixteen smelted ingots');
 assert.equal((await act(page,'tile',47,54)).meta.totalProduced,48,'Twelve ingots become forty-eight real ammunition rounds after four ingots craft the fabricator');
 const turretState=await act(page,'tile',48,54),ammoState=await act(page,'tile',47,54);s=await read(page);
 assert.equal((turretState.meta.contents?.ammo||0)+(ammoState.meta.outputs?.ammo||0)+s.metrics.shots,56,'Every earned/manufactured round is stored, buffered or actually fired');
 await act(page,'service',false);await act(page,'walk',50);
 let spawned=null;for(let tries=0;tries<16;tries++){const enemy=await act(page,'spawn');if(enemy&&enemy.x/24>=14&&enemy.x/24<=35&&enemy.y/24>=56&&enemy.y/24<58){spawned=enemy;break;}}
 assert.ok(spawned,'Enemy uses the real spawn manager on the connected quarry floor');
 await page.locator('[data-mode="combat"]').click();
 for(let swing=0;swing<4&&(await read(page)).enemies.some(e=>e.id===spawned.id);swing++){await act(page,'approachEnemy',spawned.id);s=await read(page);const foe=s.enemies.find(e=>e.id===spawned.id);if(!foe)break;const enemyTarget=await act(page,'screen',foe.x/24-.5,foe.y/24-.5);await page.mouse.move(enemyTarget.x,enemyTarget.y);await page.keyboard.press('q');await act(page,'advance',.7,false);}
 assert.equal((await read(page)).enemies.some(e=>e.id===spawned.id),false,'Actual Q attack defeats the naturally spawned beetle');
 await act(page,'walk',42.85);await act(page,'climb',53);await act(page,'service',true);await phase(page,'processing, manufactured ammunition and Q combat');
 // Keep the genuine input hopper supplied while the normal scheduler produces a warned raid.
 // Warning and attack were observed while constructing and operating this same factory.
 for(let loop=0;loop<180;loop++){
   s=await read(page);if(s.hud.raid.state==='warning'){if(!sawWarning){await page.screenshot({path:path.join(shots,'04-raid-warning.png')});sawWarning=true;}}
   if(s.hud.raid.state==='attack'){if(!sawAttack){await page.screenshot({path:path.join(shots,'05-base-attack.png')});sawAttack=true;}}
   const hopper=await act(page,'tile',44,54),room=8-Object.values(hopper.meta.contents||{}).reduce((a,b)=>a+b,0),feed=Math.min(room,s.inventory.iron||0);
   if(feed>0)await act(page,'insert','iron',feed,44,54);
   if((s.stats.raidsSurvived||0)>raidsBefore&&sawWarning&&sawAttack)break;
   await act(page,'advance',3,true);
 }
 s=await read(page);assert.ok(sawWarning&&sawAttack,'The normal elapsed-time spawn manager warns and starts a real base attack');
 assert.ok((s.stats.raidsSurvived||0)>raidsBefore,'Every real raid enemy is defeated, rather than waiting for a retreat');
 assert.ok(s.metrics.shots>0,'Powered turret consumes ammunition and fires');assert.ok((s.stats.defenseKills||0)>=3,'Connected defense machinery defeats real enemies');assert.ok(s.chapter>=4,'Combat and defense progress unlock the Industrial Network');
 await page.screenshot({path:path.join(shots,'06-factory-defended.png')});await phase(page,'full survival/automation loop completed');
 assert.deepEqual(errors,[],'Desktop gameplay produces no browser exceptions');
 // A genuine touch-capable browser context resumes the unchanged normal-world save.
 const mobileContext=await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true}),mobile=await prepare(mobileContext,mobileSave),phone=mobile.page,cdp=await mobileContext.newCDPSession(phone);
 async function touchHold(selector,seconds){const box=await phone.locator(selector).boundingBox();assert.ok(box,'Touch control exists');await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:box.x+box.width/2,y:box.y+box.height/2}]});await act(phone,'advance',seconds);await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});}
 const mobileStart=await read(phone);await touchHold('[data-touch="left"]',.55);assert.ok((await read(phone)).player.x<mobileStart.player.x-30,'Actual held touch movement changes player position');
 await act(phone,'walk',24);await phone.locator('[data-mode="mine"]').tap();const copperTarget=await act(phone,'screen',24,34);await phone.touchscreen.tap(copperTarget.x,copperTarget.y);await touchHold('[data-touch="mine"]',1.5);await act(phone,'advance',.5);
 assert.equal((await act(phone,'tile',24,34)).id,0,'Actual touch hold mines normal generated copper');assert.ok((await read(phone)).inventory.copper>mobileStart.inventory.copper,'Touch-mined resources are collected');
 await act(phone,'walk',17);const shaftBottom=(await read(phone)).player.y;await touchHold('[data-touch="jump"]',.8);assert.ok((await read(phone)).player.y<shaftBottom-70,'Actual up touch climbs from the bottom of the normal shaft');const top=(await read(phone)).player.y;await touchHold('[data-touch="down"]',.6);assert.ok((await read(phone)).player.y>top+40,'Actual down touch control descends a ladder');
 const down=(await read(phone)).player.y;await touchHold('[data-touch="jump"]',.4);assert.ok((await read(phone)).player.y<down-30,'Actual up touch control climbs the ladder');
 await act(phone,'select','ladder','mine');const ladderTarget=await act(phone,'screen',18,40);await phone.touchscreen.tap(ladderTarget.x,ladderTarget.y);await phone.locator('[data-mode="build"]').tap();const laddersBefore=(await read(phone)).inventory.ladder;await touchHold('[data-touch="place"]',.05);
 assert.equal((await act(phone,'tile',18,40)).name,'Ladder','Actual touch placement builds the targeted section');assert.equal((await read(phone)).inventory.ladder,laddersBefore-1,'Touch construction consumes its real inventory component');
 await act(phone,'craft','hammer');await phone.locator('[data-mode="combat"]').tap();const firstWeapon=(await read(phone)).hud.weapon;await phone.locator('#weaponBtn').tap();assert.notEqual((await read(phone)).hud.weapon,firstWeapon,'Actual touch cycles among owned weapons');const medicalBefore=(await read(phone)).inventory.medkit;await phone.locator('#healBtn').tap();assert.equal((await read(phone)).inventory.medkit,medicalBefore,'Actual heal touch preserves supplies at full health');assert.match(await phone.locator('#toast').innerText(),/Health is full/);
 await phone.screenshot({path:path.join(shots,'07-mobile-actions.png')});assert.deepEqual(mobile.errors,[],'Actual mobile gameplay produces no browser exceptions');
 await mobileContext.close();await context.close();
 // Finally open the actual files with no routing, source instrumentation, server or injected test state.
 const directContext=await browser.newContext({viewport:{width:1440,height:900}}),directPage=await directContext.newPage(),fileErrors=[];
 directPage.on('pageerror',error=>fileErrors.push(error.message));await directPage.goto(pathToFileURL(path.join(root,'game/circuitbound.html')).href);
 await directPage.waitForFunction(()=>document.querySelectorAll('#hotbar .slot').length>20&&typeof CircuitEngineering==='object'&&typeof CircuitSurvival==='object'&&typeof CircuitInterface==='object');
 if(await directPage.locator('#welcomePanel').isVisible())await directPage.locator('#startBtn').click();
 assert.equal(await directPage.evaluate(()=>typeof window.testAPI),'undefined','Direct-file smoke uses the unmodified production application');
 assert.equal(await directPage.locator('.modal.show').count(),0,'Direct-file startup returns to playable gameplay');
 assert.deepEqual(fileErrors,[],'Direct-file startup has no browser exceptions');await directPage.screenshot({path:path.join(shots,'08-direct-file-startup.png')});await directContext.close();
 console.log('Circuitbound full browser gameplay passed: unchanged seeded start; resource-conserving terrain-fixture quarry, powered mining/logistics, manufacturing/ammunition, real melee/turret kills, warned raid survival and campaign unlock; genuine touch movement/mining/climbing/placement and uninstrumented direct-file startup.');
 }finally{await browser.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
