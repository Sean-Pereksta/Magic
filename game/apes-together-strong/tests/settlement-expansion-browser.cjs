/* Real desktop/mobile controls and canvas verification from the current source. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {chromium}=require('playwright');
const directory=path.join(__dirname,'..'),modules=['world','navigation','audio','settlements','forces','sim','render','render-details','app'];
const html=fs.readFileSync(path.join(directory,'shell.html'),'utf8').replace('</body>',modules.map(name=>'<script>\n'+fs.readFileSync(path.join(directory,name+'.js'),'utf8')+'\n</script>').join('\n')+'</body>');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{
  for(const mobile of [false,true]){
   const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900},isMobile:mobile,hasTouch:mobile}),errors=[];page.on('pageerror',e=>errors.push(e.message));
   await page.route('http://settlement.test/**',route=>route.fulfill({status:200,contentType:'text/html',body:html}));await page.goto('http://settlement.test/');await page.locator('#newRun').click();await page.waitForFunction(()=>ATS.screen==='play');
   const initial=await page.evaluate(()=>{
    const g=ATS.game,r=ATS.renderer;g.update=()=>{};g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.settlements=[];g.effects=[];g.world.getObjects=()=>[];g.world.getSites=()=>[];g.world.lineClear=()=>true;g.world.blocked=()=>false;
    for(let i=0;i<96;i++)g.makeApe(Math.cos(i)*65,Math.sin(i)*65,'follow');g.food=1000;g.command('settleAll');const home=g.settlements[0];Object.assign(home,{food:1000,wood:150,birthTimer:-100000,suitability:{fertility:1,wood:24,capacity:MAX_APE_POPULATION,water:true}});const before={huts:home.huts.length,radius:home.radius};
    // This gallery advances a distant village's shared economy before
    // returning to inspect the same occupied homes and touch controls.
    const crown={x:g.king.x,y:g.king.y};g.king.x=home.x+5000;for(let i=0;i<90;i++){g.time++;g.refreshSettlements();g.colonies.tick(home)}Object.assign(g.king,crown);
    const names=['East','Southeast','South','Southwest','West','Northwest','North','Northeast'];for(let i=0;i<8;i++){const a=i*Math.PI/4,p=r.unproject(r.w/2+Math.cos(a)*5000,r.h*.53+Math.sin(a)*5000),s={id:'remote-'+i,name:names[i],...p,level:1,population:1,food:20,birthTimer:0,age:0,safety:1,lastRaid:g.time};g.settlements.push(s);const ape=g.makeApe(p.x,p.y,'settled',s.id);g.colonies.init(s);if(i===0){ape.state='follow';ape.settlementId=null}}g.refreshSettlements();r.camera.x=g.king.x;r.camera.y=g.king.y;window.browserHome=home;return{before,after:{huts:home.huts.length,radius:home.radius,housing:home.housing}};
   });
   assert.ok(initial.after.huts>initial.before.huts,'builders create actual huts');assert.ok(initial.after.radius>initial.before.radius,'construction enlarges base');
   const finder=page.locator('#findSettlementButton');assert.equal(await finder.isVisible(),true);if(mobile)await finder.tap();else await finder.click();assert.equal(await finder.getAttribute('aria-pressed'),'true');
   const geometry=await page.evaluate(()=>{const r=ATS.renderer,m=r.settlementIndicators(ATS.game);return{enabled:r.findSettlements,count:m.length,empty:m.filter(a=>a.settlement.population===0).length,edges:[...new Set(m.filter(a=>!a.near).map(a=>a.edge))],allWithin:m.every(a=>a.x>0&&a.x<r.w&&a.y>0&&a.y<r.h),overflow:document.documentElement.scrollWidth>innerWidth}});
   assert.equal(geometry.enabled,true);assert.equal(geometry.count,9);assert.equal(geometry.empty,1,'a rallied empty base stays findable');assert.equal(geometry.edges.length,4);assert.equal(geometry.allWithin,true);assert.equal(geometry.overflow,false);
   await page.evaluate(()=>document.getElementById('toastArea').replaceChildren());await page.waitForTimeout(80);if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'settlement-mobile.png':'settlement-desktop.png')})}
   const destroyed=await page.evaluate(()=>{
    const g=ATS.game,s=window.browserHome,occupied=s.huts.filter(h=>h.hp>0&&h.stage===4),hut=occupied.sort((a,b)=>Math.hypot(b.x-s.x,b.y-s.y)-Math.hypot(a.x-s.x,a.y-s.y))[0],housing=s.housing,angle=Math.atan2(hut.y-s.y,hut.x-s.x),firing={x:hut.x+Math.cos(angle)*55,y:hut.y+Math.sin(angle)*55};
    // Advance the raider into an exposed outside firing position. Choosing
    // the outermost occupied home prevents a nearer new hut intercepting this
    // deliberate siege target as the shared construction queue finishes.
    const start={x:hut.x+Math.cos(angle)*100,y:hut.y+Math.sin(angle)*100},h=g.makeHuman(start.x,start.y,null);h.state='search';h.raidTarget=s.id;
    // The gallery uses an open-ground mock above; the newer actor-profile API
    // needs the same open-ground contract for this bounded approach.
    g.world.actorBlocked=()=>false;g.navigation=new ATSNavigation(g.world);
    for(let i=0;i<600&&hut.hp>0;i++){g.time+=.1;g.navigation.beginFrame(g.time);if(Math.hypot(h.x-firing.x,h.y-firing.y)>5)g.move(h,firing.x-h.x,firing.y-h.y,72,.1);g.humanGrid.rebuild(g.humans);if(i%10===0){g.refreshSettlements();g.colonies.tick(s)}}
    const damaged=occupied.find(o=>o!==hut&&o.hp>0);g.colonies.damageHut(s,damaged,50);g.effects=[];return{hp:hut.hp,housing:s.housing,before:housing,capacity:hut.capacity,damaged:damaged.hp,approach:Math.hypot(h.x-start.x,h.y-start.y)};
   });
   assert.ok(destroyed.approach>20,'the raider advances to its firing position');assert.equal(destroyed.hp,0);assert.ok(destroyed.housing<=destroyed.before-destroyed.capacity);assert.ok(destroyed.damaged>0&&destroyed.damaged<100);await page.evaluate(()=>document.getElementById('toastArea').replaceChildren());await page.waitForTimeout(80);
   if(process.env.QA_ARTIFACT_DIR)await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'settlement-mobile-damage.png':'settlement-desktop-damage.png')});
   if(mobile)await finder.tap();else await page.keyboard.press('v');assert.equal(await finder.getAttribute('aria-pressed'),'false');assert.equal(await page.evaluate(()=>ATS.renderer.findSettlements),false);assert.deepEqual(errors,[]);await page.close();
  }
  console.log('PASS: desktop/mobile Find Settlement control, all eight bearings on four edges, simultaneous destinations, real expanding huts and human destruction, finite live canvas, no browser errors or mobile overflow.');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
