/* Browser-only integration assertions. Uses the root harness's closure-scoped eval hook. */
module.exports=async function combatBrowserChecks(page,exec,check,assert){
 const fixture=String.raw`
  function fresh(mode='CLASSIC'){resetArcadeCore(mode);uiPaused=false;pausedForChoice=false;enemies=[];hazards=[];powerups=[];arcade.tasks=[];arcade.props=[];arcade.bossWave=true;wavePause=999;spawnTimer=999;stats.waveTarget=999;player.inv=999;}
  function tick(count,dt=.005){for(let n=0;n<count;n++)update(dt);}
  function target(type='goblin',dx=24,dy=0){spawnEnemy(type);const e=enemies[enemies.length-1];Object.assign(e,{x:player.x+dx,y:player.y+dy,spd:0,cooldown:999,stun:1,state:'move'});return e;}
  function must(ok,message){if(!ok)throw new Error(message);}
 `;
 const run=body=>exec('(function(){'+fixture+body+'})()');

 await check('all eight sprite weapons synchronize anticipation, contact and recovery',async()=>{
  const result=await run(String.raw`
   const checked=[];
   for(const id of Object.keys(WEAPONS)){
    fresh();player.weapon=id;player.equipment.weapon=id;const e=target();doSlash();const a=player.attack;
    must(a&&a.phase==='anticipate'&&!e.dead,id+': input cannot hit before animation');
    doSlash();must(player.attack===a,id+': repeated input cannot restart a swing');
    update(.005);must(!e.dead&&a.phase==='anticipate',id+': anticipation must be harmless');
    let sawActive=false,sawRecover=false;
    for(let n=0;n<120;n++){update(.005);sawActive=sawActive||a.phase==='active';sawRecover=sawRecover||a.phase==='recover';}
    must(sawActive&&sawRecover&&e.dead,id+': contact/recovery phases must occur');
    must(!player.attack&&stats.kills===1,id+': a swing hits each target once and returns to idle');
    const art=OHKAssets.lookup('weapon.'+id);must(art&&art.animations,id+': real matching weapon sheet must exist');
    checked.push(id);
   }
   return checked;
  `);assert.equal(result.length,8);
 });

 await check('four directional swing sectors and late-windup Perfect Strikes',async()=>{
  const result=await run(String.raw`
   for(let dir=0;dir<4;dir++){
    fresh();player.faceDir=dir;const a=dir*Math.PI/2,front=target('goblin',Math.cos(a)*24,Math.sin(a)*24),back=target('goblin',-Math.cos(a)*24,-Math.sin(a)*24);
    doSlash();tick(100);must(front.dead&&!back.dead,'Slash '+dir+' must hit only its visible facing sector');
   }
   fresh();const e=target();Object.assign(e,{state:'windup',windup:.5,stateTimer:.09});doSlash();tick(20);
   must(e.dead&&stats.perfectStrikes===1&&arcade.slowmo>0,'Late enemy windup must produce bonus Perfect Strike and slow motion');
   return {perfect:stats.perfectStrikes,score:stats.score};
  `);assert.equal(result.perfect,1);assert(result.score>500);
 });

 await check('all seven bosses require readable vulnerable windows and die to one timed hit',async()=>{
  const result=await run(String.raw`
   const names=['ogre','warlock','behemoth','wyrm','king','necroking','titan'],checked=[];
   for(let index=0;index<names.length;index++){
    fresh();stats.wave=7;stats.bosses=index;spawnBoss();const e=enemies[enemies.length-1];
    must(e.type===names[index],'Boss sequence must include '+names[index]);
    Object.assign(e,{x:player.x+24,y:player.y,spd:0,cooldown:999,stun:5,entrance:0,state:'move'});
    must(!canStrikeEnemy(e),'Moving '+e.type+' must be armored');doSlash();tick(100);must(!e.dead,e.type+' cannot die before its opening');
    e.state='recover';e.stateTimer=1;must(canStrikeEnemy(e),e.type+' recovery must be vulnerable');doSlash();tick(22);
    must(e.dead&&e.deathLife>0,e.type+' must die on one exposed strike and retain death animation');
    must(stats.bosses===index+1,'Boss victory should count once');must(OHKAssets.lookup('boss.'+e.type),e.type+' needs a real boss sheet');checked.push(e.type);
   }
   return checked;
  `);assert.equal(result.length,7);
 });

 await check('independent equipment upgrades combine and 30-kill chains trigger Knight Frenzy',async()=>{
  const result=await run(String.raw`
   fresh();for(const item of ['quick-boots','royal-score','flame-sword','shadow-knight'])choices.find(c=>c.item===item)[2]();
   must(player.equipment.boots==='gold'&&player.equipment.helmet==='crown'&&player.equipment.weapon==='flame'&&player.equipment.armor==='shadow'&&player.equipment.cape==='shadow','Equipment slots must combine independently');
   fresh();for(let n=0;n<30;n++){target();doSlash();tick(60);}
   must(stats.combo===30&&stats.bestCombo===30&&player.frenzy>0,'30 kills must activate Knight Frenzy');must(stats.achievements.includes('frenzy'),'Frenzy achievement must unlock');
   return {combo:stats.combo,multiplier:stats.highestMultiplier};
  `);assert.equal(result.combo,30);assert(result.multiplier>=2);
 });

 await check('all 30 weighted pickups apply distinct effects without leaking temporary stats',async()=>{
  const result=await run(String.raw`
   const before=[];
   must(PICKUP_POOL.length===30&&PICKUP_POOL.filter(p=>p[0].includes('-')).length===20,'Exactly 20 new pickups extend the original 10');
   for(const [kind,weight]of PICKUP_POOL){fresh();collectPower({kind,x:player.x,y:player.y});must(stats.pickups===1&&weight>0,kind+' needs a live positive-weight handler');must(OHKAssets.lookup('pickup.'+kind)||OHKAssets.lookup('pickup.'+({shield:'royal-shield',big:'giant-sword',freeze:'ice-crystal',boom:'fire-crown',gold:'gold',haste:'knight-wings',nuke:'thunder-strike',crown:'royal-banner',sand:'time-clock',ghost:'phantom-cloak'}[kind]||kind)),kind+' needs an actual animated icon');before.push(kind);}
   fresh();collectPower({kind:'big',x:200,y:100});collectPower({kind:'haste',x:200,y:100});must(player.buffs.giant>0&&player.buffs.haste>0,'Temporary transformations must be timed buffs');must(upgrades.speed===1&&upgrades.slash===1,'Temporary pickups must not permanently mutate base stats');
   return before;
  `);assert.equal(result.length,30);
 });

 await check('all seven arcade events start and introduce their challenge',async()=>{
  const result=await run(String.raw`
   const checked=[];for(const name of ARCADE_EVENTS){fresh();arcade.bossWave=false;startArcadeEvent(name);update(.25);must(arcade.event===name&&arcade.eventLeft<15,name+' must advance on simulation time');
    if(name==='GOBLIN STAMPEDE')must(enemies.length>=3,'Stampede must spawn goblins');
    if(name==='BOMB PANIC')must(enemies.some(e=>e.type==='bomb'),'Bomb Panic must spawn recognizable bomb patterns');
    if(name==='GOLDEN CHICKEN')must(enemies.some(e=>e.bonusTarget==='chicken'),'Golden Chicken must create the catchable target');
    if(name==='ROYAL DUEL')must(enemies.length===1&&enemies[0].duelist,'Royal Duel must remain a single telegraphed duel');
    if(name==='CURSED TREASURE')must(powerups.some(p=>p.cursed),'Cursed Treasure must create a chest with a consequence');checked.push(name);
   }return checked;
  `);assert.equal(result.length,7);
 });

 await check('every third wave offers three working graphical upgrade choices and resumes',async()=>{
  await run(String.raw`
   fresh();stats.wave=3;arcade.bossWave=false;nextWave();tick(130);must(pausedForChoice&&choice.style.display==='flex','Wave 3 must pause for upgrade cards');must(cards.children.length===3,'Three rarity cards must appear');
   must([...cards.children].every(c=>c.dataset.item&&c.dataset.rarity),'Every upgrade needs item and rarity metadata');return true;
  `);
  await page.waitForFunction(()=>document.querySelectorAll('#cards canvas').length===3);
  await page.waitForFunction(()=>[...document.querySelectorAll('#cards canvas')].every(c=>{try{return c.getContext('2d').getImageData(0,0,c.width,c.height).data.some((v,i)=>i%4===3&&v>0);}catch(_){return false;}}),{timeout:10000});
  await page.locator('#cards .card').first().click();assert.equal(await exec('pausedForChoice'),false);assert.equal(await exec('stats.wave'),4);
 });

 await check('Mayhem time expiration ends the run even when shields remain',async()=>{
  const result=await run(String.raw`
   fresh('60 SECOND');upgrades.shield=5;stats.minuteLeft=.001;update(.01);must(gameOver&&!running&&stats.minuteLeft===0,'Mayhem must end at zero');must(upgrades.shield===5,'Clock expiration must not spend shields');must(stats.achievements.includes('mayhem-complete'),'Completed challenge must unlock a cosmetic');return {gameOver,shields:upgrades.shield};
  `);assert.equal(result.gameOver,true);assert.equal(result.shields,5);
 });

 await check('restart cancels old delayed attacks, buff reversals and death menus',async()=>{
  const result=await run(String.raw`
   fresh();let stale=0;arcade.tasks.push({fn:()=>stale++,left:.01,token:arcade.runToken});collectPower({kind:'haste',x:200,y:100});player.inv=0;upgrades.shield=0;die();resetArcadeCore('CLASSIC');uiPaused=false;enemies=[];wavePause=999;spawnTimer=999;tick(100);
   must(stale===0&&upgrades.speed===1&&!player.buffs.haste&&!gameOver&&running,'Restart must discard all prior-run state');return stale;
  `);assert.equal(result,0);
  await page.waitForTimeout(700);assert.equal(await exec('running'),true);assert.equal(await exec('gameOver'),false);assert.equal(await exec('overlay.style.display'),'none');
 });
};
