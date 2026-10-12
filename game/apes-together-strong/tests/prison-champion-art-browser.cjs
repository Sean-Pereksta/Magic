'use strict';
// Source-module gallery exercises the final production wrappers and real Canvas.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const root=path.resolve(__dirname,'..'),output=process.env.QA_ARTIFACT_DIR||path.resolve(root,'../../../prison-champion-qa');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
 try{
  const page=await browser.newPage({viewport:{width:1640,height:1770},deviceScaleFactor:1}),errors=[],requests=[];page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(/^https?:/.test(r.url()))requests.push(r.url())});
  await page.setContent('<body style="margin:0;background:#142b26"><canvas id="gallery" style="width:1640px;height:1770px"></canvas></body>');
  const manifest={},sources={},assetRoot=path.join(root,'assets/visual');
  for(const family of['characters','environment','equipment','held-items','settlements','vehicles','outfits','armor-sprites']){
   const m=manifest[family]=JSON.parse(fs.readFileSync(path.join(assetRoot,family,'manifest.json')));
   if(family==='outfits'){m.atlases=m.atlases.filter(a=>['outfits-shields-tools','outfits-tribal-structures'].includes(a.id));m.characterFrames={}}
   for(const a of m.atlases)if(family!=='armor-sprites'||/-base$|-tier3$/.test(a.id))sources[a.id]='data:image/'+path.extname(a.file).slice(1)+';base64,'+fs.readFileSync(path.join(assetRoot,a.file)).toString('base64');
  }
  await page.evaluate(bundle=>window.ATS_VISUAL_BUNDLE=bundle,{manifest,sources});
  const parts=[...fs.readFileSync(path.join(root,'build.cjs'),'utf8').match(/const parts=\[([^\]]+)\]/)[1].matchAll(/'([^']+)'/g)].map(m=>m[1]).filter(n=>n!=='app'&&!n.endsWith('-ui'));
  for(const part of parts)await page.addScriptTag({content:fs.readFileSync(path.join(root,part+'.js'),'utf8')});
  await page.evaluate(()=>ATSVisualAssets.ready);
  const report=await page.evaluate(async()=>{
   const kinds=ATSCharacterArt.species,names=['E','SE','S','SW','W','NW','N','NE'];await ATSVisualAssets.preload(kinds.map(s=>'characters-armor-'+s+'-tier3'));
   const r=new ATSRenderer(document.getElementById('gallery')),c=r.ctx;r.time=10;r.quality='high';r.reducedMotion=true;r.detailLevel=0;r.camera.zoom=1;
   c.fillStyle='#142b26';c.fillRect(0,0,1640,1770);c.fillStyle='#ead4a3';c.font='700 25px system-ui';c.fillText('PRISON CHAMPIONS',26,37);c.fillStyle='#b5c7ae';c.font='14px system-ui';c.fillText('Existing monkey species • full armor • illustrated back-mounted standard • eight facings',26,61);
   const angle=d=>{const t=d*Math.PI/4;return Math.atan2(Math.sin(t)/.42-Math.cos(t)/.8,Math.sin(t)/.42+Math.cos(t)/.8)},specs={gorilla:'wallbreaker',chimpanzee:'shockRaider',orangutan:'rescueBearer',gibbon:'skyRunner',mandrill:'guardCaptain',capuchin:'alarmSpoiler'};let poses=0;const facings=new Set();
   for(let row=0;row<kinds.length;row++)for(let d=0;d<8;d++){
    const kind=kinds[row],a={id:'gallery-'+kind+'-'+d,species:kind,hp:550,maxHp:550,age:120,bodyScale:1,dir:angle(d),phase:0,state:'follow',champion:{archetype:specs[kind],prisonChampion:true}},before=JSON.stringify(a),p=ATSCharacterArt.resolve(a,10);
    if(p.atlas!=='armor-'+kind+'-tier3')throw Error('Champion armor missing for '+kind);facings.add(p.direction);
    const x=d*205,y=80+row*240;c.strokeStyle='#2e4c3d';c.strokeRect(x+4,y+4,197,231);c.fillStyle='#adc3ac';c.font='13px system-ui';c.fillText(kind+' '+names[d],x+14,y+24);
    c.save();c.translate(x+98,y+212);c.scale(1.35,1.35);r.drawApe(c,a,false);c.restore();if(before!==JSON.stringify(a))throw Error('Drawing changed actor');poses++;
   }
   c.fillStyle='#ead4a3';c.font='700 16px system-ui';c.fillText('Low detail stays recognizable',26,1553);r.quality='low';r.detailLevel=4;r.camera.zoom=.5;
   for(let i=0;i<kinds.length;i++){const a={id:'low-'+i,species:kinds[i],hp:550,maxHp:550,age:120,bodyScale:1,dir:angle(i%8),state:'follow',champion:{archetype:specs[kinds[i]],prisonChampion:true}};c.save();c.translate(70+i*140,1732);c.scale(1.3,1.3);r.drawApe(c,a,false);c.restore();c.fillStyle='#adc3ac';c.font='12px system-ui';c.fillText(kinds[i],30+i*140,1754)}
   r.quality='high';r.detailLevel=0;r.camera.zoom=1;c.fillStyle='#ead4a3';c.font='700 16px system-ui';c.fillText('Normal monkeys + 2 champions in a cage',920,1553);
   for(const [n,championCount] of[[0,0],[1,2]]){c.save();c.translate(1035+n*350,1737);c.scale(1.9,1.9);r.drawObject(c,{id:'gallery-cage-'+n,type:'cage',prisonKind:'fortress',prisonReward:'reinforcements',count:18,championCount,championRemaining:championCount,w:85,h:65,hp:100,maxHp:100});c.restore()}
   const flag=ATSChampionArt.standardImage('#d2a85d'),data=flag.getContext('2d').getImageData(0,0,flag.width,flag.height).data;let flagPixels=0;for(let i=3;i<data.length;i+=4)if(data[i]>0)flagPixels++;
   return {poses,facings:facings.size,standards:r.championStandardDraws,flagPixels,cache:ATSChampionArt.status(),failures:ATSVisualAssets.failures};
  });
  assert.equal(report.poses,48);assert.equal(report.facings,8);assert.ok(report.standards>=56);assert.ok(report.flagPixels>2000);assert.deepEqual(report.failures,[]);assert.deepEqual(errors,[]);assert.deepEqual(requests,[]);
  fs.mkdirSync(output,{recursive:true});await page.screenshot({path:path.join(output,'prison-champion-gallery.png')});fs.writeFileSync(path.join(output,'prison-champion-gallery.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({output,...report}));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
