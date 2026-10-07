/* Six-species art, real canvas silhouettes, action paths and bounded appearance caches. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {chromium}=require('playwright');
const SPECIES=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'];
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--disable-dev-shm-usage']});
 try{
  const page=await browser.newPage({viewport:{width:1600,height:1050},deviceScaleFactor:1}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<style>html,body{margin:0;background:#071216}canvas{display:block;width:1600px;height:1050px}</style><canvas id="game"></canvas>');
  for(const name of ['world','navigation','settlements','forces','sim','render','render-details'])await page.addScriptTag({content:fs.readFileSync(path.join(__dirname,'..',name+'.js'),'utf8')});
  const report=await page.evaluate(species=>{
   const ensure=(value,message)=>{if(!value)throw new Error(message)},canvas=document.getElementById('game'),r=new ATSRenderer(canvas);
   r.time=10.2;r.quality='high';r.camera.zoom=1;r.actorSpriteBudget=1000;
   const actor=(id,fields={})=>({id:'primate-'+id,species:id,coatVariant:0,x:0,y:0,dir:0,phase:0,bodyScale:1,age:240,hp:120,maxHp:120,state:'follow',attackTimer:0,moving:false,...fields});
   const pixelData=(image)=>{
    const {width:w,height:h}=image,data=image.getContext('2d').getImageData(0,0,w,h).data,mask=new Uint8Array(w*h);let count=0,xsum=0,ysum=0,minX=w,minY=h,maxX=-1,maxY=-1,hash=2166136261;
    for(let p=0;p<w*h;p++){const i=p*4;for(let k=0;k<4;k++)hash=Math.imul(hash^data[i+k],16777619);if(data[i+3]>128){const x=p%w,y=Math.floor(p/w);mask[p]=1;count++;xsum+=x;ysum+=y;minX=Math.min(minX,x);minY=Math.min(minY,y);maxX=Math.max(maxX,x);maxY=Math.max(maxY,y)}}
    return{canvas:image,mask,hash:hash>>>0,count,cx:count?xsum/count:0,cy:count?ysum/count:0,width:maxX-minX+1,height:maxY-minY+1,minX,minY,maxX,maxY};
   };
   const difference=(a,b)=>{let union=0,different=0;for(let i=0;i<a.mask.length;i++){if(a.mask[i]||b.mask[i])union++;if(a.mask[i]!==b.mask[i])different++}return union?different/union:0};
   function snapshot(a,options={}){
    const image=document.createElement('canvas');image.width=320;image.height=260;const c=image.getContext('2d'),old={quality:r.quality,zoom:r.camera.zoom,time:r.time,reduced:r.reducedMotion,ctx:r.ctx,project:r.project};
    r.quality=options.low?'low':'high';r.camera.zoom=options.low?.45:1;r.time=options.time??10.2;r.reducedMotion=!!options.reduced;r.actorSpriteBudget=options.budget??1000;
    if(options.corpse){r.ctx=c;r.camera.zoom=1;r.project=()=>({x:160,y:200});r.drawCorpses({corpses:[a]})}
    else{c.translate(160,200);if(options.atlas)r.drawApeSprite(c,a);else r.drawApe(c,a,!!options.king,0)}
    r.quality=old.quality;r.camera.zoom=old.zoom;r.time=old.time;r.reducedMotion=old.reduced;r.ctx=old.ctx;r.project=old.project;const pixels=pixelData(image);
    ensure(pixels.count>0,'rendered snapshot is empty: '+a.species);ensure(pixels.minX>2&&pixels.minY>2&&pixels.maxX<image.width-3&&pixels.maxY<image.height-3,'snapshot clips an actor: '+a.species);return pixels;
   }
   const adults=species.map(id=>snapshot(actor(id))),silhouettes=[];
   for(let i=0;i<species.length;i++)for(let j=i+1;j<species.length;j++){
    const diff=difference(adults[i],adults[j]);ensure(diff>.025,'species share an indistinguishable silhouette: '+species[i]+'/'+species[j]+' ('+diff+')');silhouettes.push({a:species[i],b:species[j],difference:diff});
   }
   const states=[];
   for(let i=0;i<species.length;i++){
    const id=species[i],a=actor(id),young=snapshot({...a,state:'young',age:7,hp:60,maxHp:60}),worker=snapshot({...a,state:'settled',activity:'hauling timber',carrying:'wood'}),emptyWorker=snapshot({...a,state:'settled',activity:'hauling timber',carrying:false}),attack=snapshot({...a,attackTimer:.3,animation:{kind:'hook',start:10,duration:.5}});
    const flight=snapshot({...a,blastReaction:{stage:'flight',height:65,rotation:.55,start:10,duration:.7},blastZ:65,blastSpin:.55});
    const gettingUp=snapshot({...a,blastReaction:{stage:'gettingUp',height:0,rotation:0,start:10,getUpAt:10,recoverAt:10.8,recoveryDuration:.8,recoveryElapsed:.2}});
    const corpse={...a,type:'ape',hp:0,actorAge:240,actorState:'follow',age:2,life:30,fallVariant:0},fallen=snapshot(corpse,{corpse:true}),flyingBody=snapshot({...corpse,blastReaction:{stage:'flight',height:65,rotation:.55,start:10,duration:.7}},{corpse:true});
    const cached=snapshot(a,{atlas:true}),shade=snapshot({...a,coatVariant:2}),small=snapshot(a,{low:true,reduced:true});
    ensure(young.count>adults[i].count*.2&&young.count<adults[i].count*.75,'young scale for '+id);
    ensure(worker.hash!==emptyWorker.hash&&worker.count>0,'worker carrying detail for '+id);
    ensure(difference(attack,adults[i])>.015,'an attack must change the limb pose for '+id);
    ensure(flight.cy<adults[i].cy-20&&flight.count>0,'airborne living actor for '+id);
    ensure(difference(gettingUp,adults[i])>.04,'visible survivor recovery pose for '+id);
    ensure(fallen.count>0&&flyingBody.cy<fallen.cy-15,'ground and flying corpses for '+id);
    ensure(difference(cached,adults[i])<.16,'the sprite atlas must preserve '+id+' geometry');
    ensure(shade.hash!==adults[i].hash&&difference(shade,adults[i])<.025,'coat shades preserve species silhouette for '+id);
    ensure(small.count>40,'low-detail silhouette remains visible for '+id);
    for(const kind of ['hook','overhead','slam','uppercut','backhand','tackle'])for(const dir of [0,Math.PI]){
     const pose=snapshot({...a,dir,attackTimer:.3,animation:{kind,start:10,duration:.5}});ensure(pose.count>adults[i].count*.55&&difference(pose,adults[i])>.015,'visible '+kind+' pose for '+id+' facing '+dir);
    }
    const youngCached=snapshot({...a,state:'young',age:7,hp:60,maxHp:60},{atlas:true});ensure(difference(young,youngCached)<.01,'young atlas fallback preserves juvenile geometry for '+id);
    const climbing=snapshot({...a,climbingVehicleId:'test-tank',climbUntil:11});ensure(climbing.count>adults[i].count*.6&&difference(climbing,adults[i])>.1,'visible raised species arms while climbing for '+id);
    for(const reaction of [{stage:'flight',height:55,rotation:.4},{stage:'gettingUp',height:0,getUpAt:10,recoverAt:10.8,recoveryDuration:.8,recoveryElapsed:.2}]){
     const smaller=snapshot({...a,bodyScale:.8,blastReaction:reaction}),larger=snapshot({...a,bodyScale:1.2,blastReaction:reaction}),ratio=larger.count/smaller.count;ensure(ratio>1.7&&ratio<2.8,'living body scale survives '+reaction.stage+' for '+id);
    }
    const smallerBody=snapshot({...corpse,bodyScale:.8,blastReaction:{stage:'flight',height:55,rotation:.4}},{corpse:true}),largerBody=snapshot({...corpse,bodyScale:1.2,blastReaction:{stage:'flight',height:55,rotation:.4}},{corpse:true});ensure(largerBody.count/smallerBody.count>1.7,'airborne corpse preserves body scale for '+id);
    states.push({species:id,adultPixels:adults[i].count,youngPixels:young.count,workerPixels:worker.count,attackDifference:difference(attack,adults[i]),atlasDifference:difference(cached,adults[i]),flightLift:adults[i].cy-flight.cy,corpseLift:fallen.cy-flyingBody.cy});
   }
   const king=snapshot({...actor('gorilla'),id:'king',hp:160,maxHp:160},{king:true});ensure(king.height>adults[0].height*1.2&&king.count>adults[0].count*1.4,'crowned silverback stays visibly larger');
   const canonicalCacheChecks=[];
   for(const id of species)for(const scout of [false,true]){
    const a=actor(id,{state:scout?'scout':'follow'}),body={...a,type:'ape',hp:0,actorAge:240,age:2,life:30},living=[],dead=[];
    for(const detail of [3,0]){r.detailLevel=detail;r.quality=detail?'low':'high';r.camera.zoom=detail?.45:1;r.apeSprites.clear();r.corpseSprites.clear();r.actorSpriteBudget=1000;r.drawApeSprite(r.ctx,a);living.push(pixelData(r.apeSprites.get(id+':0:'+scout+':6')));r.drawCorpses({corpses:[body]});dead.push(pixelData(r.corpseSprites.get('ape::'+id+':0:false:1')))}
    ensure(living[0].hash===living[1].hash,'living atlas bakes full species detail even at low quality: '+id+' scout '+scout);ensure(dead[0].hash===dead[1].hash,'corpse atlas bakes full species detail even at low quality: '+id);canonicalCacheChecks.push(id+':'+scout);
   }
   r.time=0;r.reducedMotion=false;r.detailLevel=0;r.camera.zoom=1;r.quality='high';
   const spriteBounds=[];
   function checkSpriteEdges(image,key){const c=image.getContext('2d'),data=c.getImageData(0,0,image.width,image.height).data;let edgePixels=0;
    for(let y=0;y<image.height;y++)for(let x=0;x<image.width;x++)if((x===0||y===0||x===image.width-1||y===image.height-1)&&data[(y*image.width+x)*4+3]>0)edgePixels++;
    ensure(edgePixels===0,'cached sprite touches its transparent edge: '+key);const p=pixelData(image);ensure(p.count>0,'cached sprite is empty: '+key);spriteBounds.push({key,width:p.width,height:p.height});
   }
   for(const id of species)for(let coat=0;coat<3;coat++)for(const scout of [false,true])for(let frame=0;frame<7;frame++){
    r.actorSpriteBudget=1000;const a=actor(id,{coatVariant:coat,state:scout?'scout':'follow',moving:frame!==6,phase:(frame+.25)/6*Math.PI*2});r.drawApeSprite(r.ctx,a);ensure(r.apeSprites.size<=256,'bounded six-species atlas');
    const key=id+':'+coat+':'+scout+':'+frame,sprite=r.apeSprites.get(key);ensure(sprite,'every species/scout/frame cache entry exists: '+key);checkSpriteEdges(sprite,key);
    for(const dir of [0,Math.PI]){const exact={...a,dir,phase:(frame+.0001)/6*Math.PI*2,_atlas:true},direct=snapshot(exact,{time:0}),cached=snapshot(exact,{time:0,atlas:true});ensure(difference(direct,cached)<.025,'sprite preserves uncached limbs and tail: '+key+' facing '+dir+' ('+difference(direct,cached)+')')}
   }
   const atlasKeys=Array.from(r.apeSprites.keys());ensure(r.apeSprites.size===252,'the complete finite appearance set fits without eviction churn');for(const id of species)ensure(atlasKeys.some(key=>key.startsWith(id+':')),'atlas key includes '+id);
   for(let i=0;i<600;i++){r.actorSpriteBudget=1000;r.drawApeSprite(r.ctx,actor(species[i%6],{coatVariant:i%3,bodyScale:.88+i%23*.01,fur:'#'+(i*2177%16777216).toString(16).padStart(6,'0')}));ensure(r.apeSprites.size===252,'body variation and legacy fur do not create unbounded appearance keys')}
   for(const id of species)for(let coat=0;coat<3;coat++)for(const young of [false,true])for(const facing of [0,Math.PI]){
    const body={...actor(id,{coatVariant:coat,dir:facing}),type:'ape',hp:0,actorAge:young?7:240,actorState:young?'young':'follow',age:2,life:30,fallVariant:0};r.actorSpriteBudget=1000;r.drawCorpses({corpses:[body]});ensure(r.corpseSprites.size<=64,'bounded species and coat corpse cache');
    const key='ape::'+id+':'+coat+':'+young+':'+(facing===0?1:-1);checkSpriteEdges(r.corpseSprites.get(key),key);
    const cached=snapshot(body,{corpse:true}),savedCache=r.corpseSprites;r.corpseSprites=new Map();const uncached=snapshot(body,{corpse:true,budget:0});r.corpseSprites=savedCache;ensure(difference(cached,uncached)<.06&&Math.abs(cached.count/uncached.count-1)<.08&&Math.abs(cached.width-uncached.width)<=3&&Math.abs(cached.height-uncached.height)<=3,'corpse sprite preserves uncached species body after raster rotation: '+key+' ('+difference(cached,uncached)+')');
   }
   ensure(r.corpseSprites.size===64,'the stress scene exercises corpse cache eviction');
   const fullAtlasEntries=r.apeSprites.size,maximumCorpseEntries=r.corpseSprites.size;
   const g=new ATSGame('PRIMATE-CANVAS-INTEGRATION','survival');g.time=10.2;g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.effects=[];g.corpses=species.map((id,i)=>({...actor(id,{x:(i-3)*45,y:130,dir:i%2?Math.PI:0}),type:'ape',hp:0,actorAge:240,actorState:'follow',age:2,life:30,fallVariant:0}));g.king.x=g.king.y=0;
   g.world.getObjects=()=>[];g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',water:false});g.world.lineClear=()=>true;g.getLights=()=>[];
   for(let i=0;i<108;i++){const a=g.makeApe((i%18-9)*40,(Math.floor(i/18)-3)*60,'follow');Object.assign(a,actor(species[i%6],{id:a.id,x:a.x,y:a.y,moving:true,coatVariant:i%3}))}
   const special=[];
   for(let i=0;i<6;i++){
    const base=g.apes[i];base.attackTimer=.3;base.animation={kind:'hook',start:10,duration:.5};special.push(base.id);
    Object.assign(g.apes[i+6],{activity:'hauling timber',carrying:'wood'});special.push(g.apes[i+6].id);
    Object.assign(g.apes[i+12],{blastReaction:{stage:'flight',height:55,rotation:.4,start:10,duration:.7},blastZ:55});special.push(g.apes[i+12].id);
    Object.assign(g.apes[i+18],{blastReaction:{stage:'gettingUp',height:0,start:10,getUpAt:10,recoverAt:10.8,recoveryDuration:.8,recoveryElapsed:.2}});special.push(g.apes[i+18].id);
    Object.assign(g.apes[i+24],{state:'young',age:7,hp:60,maxHp:60});
    Object.assign(g.apes[i+30],{climbingVehicleId:'test-tank',climbUntil:11});special.push(g.apes[i+30].id);
   }
   const live=new Set(),atlasCalls=new Set(),drawApe=r.drawApe.bind(r),drawApeSprite=r.drawApeSprite.bind(r),cacheSet=r.cacheSet.bind(r);let builds=0,totalBuilds=0;
   r.drawApe=function(c,a,...args){live.add(a.id);return drawApe(c,a,...args)};
   r.drawApeSprite=function(c,a){atlasCalls.add(a.id);return drawApeSprite(c,a)};
   r.cacheSet=function(cache,key,value,limit){if((cache===this.apeSprites||cache===this.corpseSprites)&&!cache.has(key))builds++;return cacheSet(cache,key,value,limit)};
   const integrated=[];
   for(const reduced of [false,true])for(const zoom of [1,.45]){
    r.apeSprites.clear();r.corpseSprites.clear();live.clear();atlasCalls.clear();
    r.reducedMotion=reduced;r.camera.zoom=zoom;r.quality=zoom<.65?'low':'high';let maximumBuilds=0;
    for(let frame=0;frame<6;frame++){g.time+=1/60;builds=0;r.draw(g,0);totalBuilds+=builds;maximumBuilds=Math.max(maximumBuilds,builds);ensure(builds<=8,'appearance variants must share the frame sprite-build budget')}
    ensure(g.performance.counters.renderLosTests<=96&&g.performance.counters.renderRays<=96,'canvas perception budget');ensure(g.performance.counters.visibleActors>=100,'all six species remain visible at low zoom');
    if(reduced)ensure(g.apes.every(a=>r.actorFrame(a,a.phase)===6),'reduced motion freezes the cached walking cycle');
    ensure(maximumBuilds>0&&r.corpseSprites.size>0,'cold living and corpse caches actually build in each rendering setting');
    for(const id of special)ensure(live.has(id)&&!atlasCalls.has(id),'worker, attack and blast actor bypasses idle atlas at zoom '+zoom+' reduced '+reduced+': '+id);
    for(let i=24;i<30;i++)ensure(live.has(g.apes[i].id),'young actor preserves direct juvenile drawing at zoom '+zoom);
    integrated.push({reduced,zoom,visible:g.performance.counters.visibleActors,maximumBuilds});
   }
   ensure(totalBuilds>8,'the integration exercises repeated cold-cache building and frame turnover');
   const labels=['Gorilla','Chimpanzee','Orangutan','Gibbon','Mandrill','Capuchin'];
   function showcase(actions=false){
    const c=r.ctx;r.quality='high';r.camera.zoom=1;r.time=10.2;r.reducedMotion=false;c.setTransform(1,0,0,1,0,0);c.fillStyle='#0c1e22';c.fillRect(0,0,r.w,r.h);
    const gradient=c.createLinearGradient(0,0,0,r.h);gradient.addColorStop(0,'#173135');gradient.addColorStop(1,'#071b20');c.fillStyle=gradient;c.fillRect(0,0,r.w,r.h);
    c.textAlign='left';c.fillStyle='#ead8a7';c.font='700 34px system-ui';c.fillText(actions?'PRIMATES IN MOTION':'SAVE THE APE · LIVING PRIMATE KINGDOM',48,58);
    c.fillStyle='#a9c4b8';c.font='17px system-ui';c.fillText(actions?'Distinct bodies through combat, flight, recovery and fallen poses':'Six species · adult, worker and young silhouettes · one growing kingdom',48,88);
    for(let i=0;i<6;i++){
     const x=48+i%3*507,y=126+Math.floor(i/3)*350;c.fillStyle='#183538';c.beginPath();c.roundRect(x,y,490,330,20);c.fill();c.strokeStyle='#385b59';c.lineWidth=1;c.stroke();
     c.textAlign='left';c.fillStyle='#e4cf9e';c.font='700 23px system-ui';c.fillText(labels[i],x+24,y+39);c.fillStyle='#8fae9f';c.font='13px system-ui';c.fillText(species[i].toUpperCase(),x+24,y+61);
     const a=actor(species[i]);c.save();c.translate(x+140,y+248);c.scale(2.7,2.7);r.drawApe(c,a,false,0);c.restore();
     if(actions){
      c.save();c.translate(x+315,y+185);c.scale(1.7,1.7);r.drawApe(c,{...a,attackTimer:.3,animation:{kind:'hook',start:10,duration:.5}},false,0);c.restore();
      c.save();c.translate(x+395,y+292);c.scale(1.4,1.4);r.drawApe(c,{...a,blastReaction:{stage:'flight',height:55,rotation:.5,start:10}},false,0);c.restore();
      c.save();c.translate(x+295,y+268);c.scale(1.05,1.05);r.drawApe(c,{...a,blastReaction:{stage:'gettingUp',height:0,start:10,getUpAt:10,recoverAt:10.8,recoveryDuration:.8,recoveryElapsed:.2}},false,0);c.restore();
      const oldProject=r.project;r.project=()=>({x:x+430,y:y+286});r.drawCorpses({corpses:[{...a,type:'ape',hp:0,actorAge:240,age:2,life:30}]});r.project=oldProject;
     }else{
      c.save();c.translate(x+326,y+193);c.scale(1.7,1.7);r.drawApe(c,{...a,state:'settled',carrying:'wood',activity:'hauling timber'},false,0);c.restore();
      c.save();c.translate(x+370,y+270);c.scale(1.8,1.8);r.drawApe(c,{...a,state:'young',age:7,hp:60,maxHp:60},false,0);c.restore();
     }
     c.fillStyle='#a7c0b1';c.font='13px system-ui';c.fillText('ADULT',x+111,y+290);c.fillText(actions?'ATTACK · FLIGHT · GET UP · FALLEN':'WORKER · YOUNG',x+(actions?225:289),y+320);
    }
    c.fillStyle='#143034';c.beginPath();c.roundRect(48,850,1504,161,20);c.fill();c.save();c.translate(177,998);c.scale(1.3,1.3);r.drawApe(c,{...actor('gorilla'),id:'king',hp:160,maxHp:160},true,0);c.restore();
    c.textAlign='left';c.fillStyle='#e4cf9e';c.font='700 24px system-ui';c.fillText('The crowned silverback',300,906);c.fillStyle='#a9c4b8';c.font='16px system-ui';c.fillText('A stronger silhouette, expressive faces, hands and feet.',300,939);c.fillText('Species and coat shades are cosmetic; every ape keeps its role and abilities.',300,967);
   }
   window.primatePreview={r,g,showcase};showcase();return{silhouettes,states,kingPixels:king.count,canonicalCacheChecks:canonicalCacheChecks.length,checkedSpriteBounds:spriteBounds.length,fullAtlasEntries,maximumCorpseEntries,atlas:r.apeSprites.size,corpses:r.corpseSprites.size,totalBuilds,integrated};
  },SPECIES);
  assert.equal(report.silhouettes.length,15);assert.equal(report.states.length,6);assert.equal(report.fullAtlasEntries,252);assert.equal(report.maximumCorpseEntries,64);assert.ok(report.atlas<=256&&report.corpses<=64);assert.deepEqual(errors,[]);
  if(process.env.QA_ARTIFACT_DIR){fs.mkdirSync(process.env.QA_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,'primate-six-species.png')});await page.evaluate(()=>primatePreview.showcase(true));await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,'primate-action-states.png')})}
  await page.setViewportSize({width:390,height:844});
  report.mobile=await page.evaluate(()=>{const {r,g}=primatePreview;r.canvas.style.width='390px';r.canvas.style.height='844px';r.resize();r.camera.zoom=.65;r.reducedMotion=true;r.quality='low';for(let i=0;i<g.apes.length;i++){g.apes[i].x=(i%12-6)*18;g.apes[i].y=(Math.floor(i/12)-4)*25}r.draw(g,0);const visible=new Set(g.apes.filter(a=>r.visible(r.project(a.x,a.y),100*r.camera.zoom)).map(a=>a.species));if(visible.size!==6||g.performance.counters.visibleActors<100)throw Error('all six species remain visible in the mobile integration');return{width:r.w,height:r.h,species:visible.size,visible:g.performance.counters.visibleActors}});
  assert.equal(report.mobile.species,6);assert.deepEqual(errors,[]);
  if(process.env.QA_ARTIFACT_DIR)await page.screenshot({path:path.join(process.env.QA_ARTIFACT_DIR,'primate-mobile.png')});
  console.log(JSON.stringify(report));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
