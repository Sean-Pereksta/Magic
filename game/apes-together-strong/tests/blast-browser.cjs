/* Actual impact, airborne bodies and survivor recovery on desktop and mobile. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{for(const mobile of [false,true]){
  const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<meta name="viewport" content="width=device-width,initial-scale=1"><style>html,body{margin:0;background:#071216}canvas{display:block;width:100vw;height:100vh}</style><canvas id="game"></canvas>');
  for(const name of ['world','navigation','settlements','forces','sim','render','render-details'])await page.addScriptTag({content:fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8')});
  const impact=await page.evaluate(mobile=>{
   const g=new ATSGame('COLORFUL-BLASTS','survival');g.time=20;g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.effects=[];g.corpses=[];g.king.hp=g.king.maxHp=100000;g.king.x=g.king.y=0;
   // Isolate the real blast pipeline in open terrain. Collision obstruction
   // and crowded combat remain covered by simulation tests.
   g.world.getObjects=()=>[];g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.actorBlocked=g.world.blocked=g.world.waterBlocked=()=>false;g.world.lineClear=()=>true;
   const r=new ATSRenderer(document.getElementById('game'));r.quality='high';r.camera.zoom=mobile?1:1.3;r.draw(g,0);r.camera.x=r.camera.y=0;
   const points=(mobile?[[195,230],[195,430],[195,630]]:[[340,465],[720,465],[1100,465]]).map(([x,y],i)=>({...r.unproject(x,y),sx:x,sy:y,type:['grenade','shell','airstrike'][i],label:['GRENADES','TANK SHOTS','AIR STRIKES'][i],radius:[66,104,120][i]}));
   for(const p of points){for(let i=0;i<9;i++){const angle=i/9*Math.PI*2,a=g.makeApe(p.x+Math.cos(angle)*46,p.y+Math.sin(angle)*46,'follow');a.hp=a.maxHp=400;}const corpse=g.makeApe(p.x+18,p.y+25,'follow');g.hurt(corpse,1000,p);const doomed=g.makeApe(p.x-22,p.y+20,'follow');doomed.hp=1;}
   g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild([]);
   function colors(){const data=r.ctx.getImageData(0,0,r.canvas.width,r.canvas.height).data;let warm=0;for(let i=0;i<data.length;i+=4)if(data[i]>150&&data[i]>data[i+1]*1.14&&data[i+1]>65)warm++;return warm;}
   function draw(){r.draw(g,0);const c=r.ctx;c.textAlign='center';c.fillStyle='#ecdba9';c.font='700 '+(mobile?18:26)+'px system-ui';c.fillText('BLASTS & RECOVERY',r.w/2,mobile?43:62);c.font=(mobile?11:14)+'px system-ui';c.fillStyle='#afc6b5';c.fillText('Explosions throw apes and fallen bodies. Survivors stand again.',r.w/2,mobile?65:90);c.font='700 '+(mobile?12:15)+'px system-ui';for(const p of points){c.fillStyle='#e9d6a3';c.fillText(p.label,p.sx,p.sy+(mobile?92:142));}}
   for(let i=0;i<10;i++)draw();const before=colors();for(const p of points)g.forces.blast({...p,damage:10});g.cannonShake=0;
   function step(seconds){for(let t=0;t<seconds-.0001;t+=1/60){g.time+=1/60;g.updateBlastReaction(g.king,1/60);for(const a of g.apes)if(a.hp>0)g.updateBlastReaction(a,1/60);for(const c of g.corpses){g.updateBlastReaction(c,1/60);c.life-=1/60;}for(const e of g.effects)e.life-=1/60;g.effects=g.effects.filter(e=>e.life>0);}}
   step(.23);draw();window.blastPreview={g,r,points,step,draw};return {types:g.effects.filter(e=>e.type==='explosion').map(e=>e.blastKind),survivors:g.apes.filter(a=>a.hp>0&&a.blastReaction?.stage==='flight'&&a.blastZ>20).length,bodies:g.corpses.filter(a=>a.blastReaction?.stage==='flight'&&a.blastZ>20).length,warm:colors(),before};
  },mobile);
  assert.deepEqual(impact.types,['grenade','shell','airstrike']);assert.equal(impact.survivors,27);assert.equal(impact.bodies,6);assert.ok(impact.warm>impact.before+250,JSON.stringify(impact));
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'blasts-mobile.png':'blasts-desktop.png')});}
  const recovery=await page.evaluate(()=>{const {g,step,draw}=window.blastPreview;step(.7);draw();return {standingUp:g.apes.filter(a=>a.hp>0&&a.blastReaction?.stage==='gettingUp').length,landed:g.corpses.filter(a=>a.blastReaction?.stage==='landed').length};});
  assert.equal(recovery.standingUp,27);assert.equal(recovery.landed,6);
  if(process.env.QA_ARTIFACT_DIR)await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'recovery-mobile.png':'recovery-desktop.png')});
  const resumed=await page.evaluate(()=>{const {g,step,draw}=window.blastPreview;step(1.5);draw();return g.apes.filter(a=>a.hp>0&&a.blastReaction).length;});assert.equal(resumed,0);assert.deepEqual(errors,[]);
  console.log(JSON.stringify({mobile,...impact,...recovery,resumed}));await page.close();
 }}finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1});
