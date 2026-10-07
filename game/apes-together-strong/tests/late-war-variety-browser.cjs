/* Focused real-canvas checks for the final late-war variety bundle. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {pathToFileURL}=require('node:url'),{chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{for(const mobile of [false,true]){
  const page=await browser.newPage({viewport:mobile?{width:390,height:844}:{width:1440,height:900}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto(pathToFileURL(path.resolve(__dirname,'../../apes-together-strong.html')).href);await page.waitForFunction(()=>window.ATS?.screen==='menu');
  const result=await page.evaluate(mobile=>{
   const canvas=document.createElement('canvas');canvas.style.cssText='position:fixed;inset:0;width:100vw;height:100vh;z-index:9999;background:#071216';document.body.append(canvas);
   const r=new ATSRenderer(canvas),g=new ATSGame('LATE-WAR-VARIETY','survival');r.time=20;r.quality='high';r.actorSpriteBudget=200;r.camera.zoom=1;
   const armor=[['tank',null],['tank','veteran'],['tank','siege'],['tank','ironclad'],['ifv',null],['ifv','sentinel']].map(([kind,variant],i)=>{const v={id:'variety-'+i,x:0,y:0,dir:0,turretDir:-.2,variant};g.forces.initVehicle(v,kind,0);return v});
   const roles=['guard','rifleman','heavy','assault','commando','juggernaut'],troops=roles.map((role,i)=>{const h=g.makeHuman(0,0,null);g.forces.assign(h,null,role);h.state='patrol';h.suspicion=0;h.dir=0;return h});
   const hash=canvas=>{const data=canvas.getContext('2d').getImageData(0,0,canvas.width,canvas.height).data;let n=2166136261;for(let i=0;i<data.length;i++)n=Math.imul(n^data[i],16777619);return n>>>0};
   const signature=draw=>{const image=document.createElement('canvas');image.width=300;image.height=210;const c=image.getContext('2d');c.translate(150,170);draw(c);return hash(image)};
   const armorHashes=armor.map(v=>signature(c=>r.drawVehicle(c,{...v,label:''}))),troopHashes=troops.map(h=>signature(c=>r.drawHuman(c,h)));
   // All supported hulls remain distinct and fit the established cache budget.
   for(const kind of ['apc','truck'])r.drawVehicle(r.ctx,{kind,hp:100,maxHp:100,dir:0});
   const hullHashes=[...r.vehicleSprites.values()].map(hash);
   for(const h of troops)r.drawHumanSprite(r.ctx,h);
   const roleKeys=[...r.humanSprites.keys()];
   // The Ironclad's warning uses its larger physical impact radius.
   const ellipses=[],ellipse=r.ctx.ellipse.bind(r.ctx);r.ctx.ellipse=(...args)=>{ellipses.push(args);ellipse(...args)};
   r.drawThreats({time:20,vehicles:[{...armor[3],cannonTarget:{x:0,y:0,start:19,duration:2}}],humans:[],forces:{hazards:[]}});r.ctx.ellipse=ellipse;
   const radius=ATSVehicleVariants.ironclad.cannonRadius,accurateWarning=ellipses.some(a=>Math.abs(a[2]-radius*.8*Math.SQRT2)<.001&&Math.abs(a[3]-radius*.42*Math.SQRT2)<.001);
   const c=r.ctx;c.setTransform(r.dpr,0,0,r.dpr,0,0);c.fillStyle='#071216';c.fillRect(0,0,r.w,r.h);
   c.textAlign='center';c.font='700 '+(mobile?22:30)+'px system-ui';c.fillStyle='#ead8ad';c.fillText('THE WAR ESCALATES',r.w/2,mobile?36:54);c.font=(mobile?10:14)+'px system-ui';c.fillStyle='#aac0b1';c.fillText('Stronger armor. Specialist infantry. Larger armies.',r.w/2,mobile?58:82);
   for(let i=0;i<armor.length;i++){
    const v=armor[i],x=mobile?98+i%2*194:i<4?210+i*340:i===4?420:1020,y=mobile?190+Math.floor(i/2)*165:i<4?285:510,scale=mobile?.7:1.15;
    c.save();c.translate(x,y);c.scale(scale,scale);r.drawVehicle(c,v);c.restore();c.font='700 '+(mobile?9:13)+'px system-ui';c.fillStyle='#dccfa8';c.fillText(v.label,x,y+(mobile?32:47));c.font=(mobile?9:12)+'px system-ui';c.fillStyle='#8fa89b';c.fillText(v.maxHp+' HP',x,y+(mobile?46:64));
   }
   for(let i=0;i<troops.length;i++){
    const h=troops[i],x=mobile?60+i%3*127:190+i*212,y=mobile?666+Math.floor(i/3)*116:775;
    c.save();c.translate(x,y);c.scale(mobile?1.25:1.9,mobile?1.25:1.9);r.drawHumanSprite(c,h);c.restore();c.font='700 '+(mobile?9:12)+'px system-ui';c.fillStyle=i>=3?'#eed3a0':'#b6c8b8';const title=ATSHumanRoles[h.role].label.replace('Armored assault infantry','Armored assault').replace('Military rifleman','Rifleman').replace('Heavy assault gunner','Heavy gunner').replace('Juggernaut gunner','Juggernaut');c.fillText(title,x,y+23);c.font=(mobile?9:11)+'px system-ui';c.fillStyle='#8fa89b';c.fillText(h.maxHp+' HP',x,y+39);
   }
   window.lateWarGallery={r,g,armor,troops};return {mobile,cache:r.vehicleSprites.size,hullHashes,armorHashes,troopHashes,roleKeys,accurateWarning,labels:armor.map(v=>v.label)};
  },mobile);
  assert.equal(result.cache,8);assert.equal(new Set(result.hullHashes).size,8);assert.equal(new Set(result.armorHashes).size,6);assert.equal(new Set(result.troopHashes).size,6);assert.equal(result.accurateWarning,true);
  for(const role of ['assault','commando','juggernaut'])assert.ok(result.roleKeys.some(key=>key.includes(':'+role+':')),role+' keeps a separate cached equipment sprite');
  assert.ok(result.labels.includes('Ironclad assault tank'));assert.ok(result.labels.includes('Sentinel fighting vehicle'));assert.deepEqual(errors,[]);
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,mobile?'late-war-variety-mobile.png':'late-war-variety-desktop.png')});}
  console.log(JSON.stringify({mobile,cache:result.cache,distinctHulls:new Set(result.hullHashes).size,specialRoles:3,accurateWarning:result.accurateWarning}));await page.close();
 }}finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1});
