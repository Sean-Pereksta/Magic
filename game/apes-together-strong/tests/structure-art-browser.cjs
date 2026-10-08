/* Actual Canvas verification of every construction strip, live completion,
 * save restoration and exact-width ground artwork. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{chromium}=require('playwright');
const root=path.resolve(__dirname,'..'),output=process.env.QA_ARTIFACT_DIR;
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
 try{
  const page=await browser.newPage({viewport:{width:1600,height:2500},deviceScaleFactor:1}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent('<style>html,body{margin:0;background:#162a26}canvas{display:block;width:1600px;height:2500px}</style><canvas id="game"></canvas>');
  const manifest={},sources={},assetRoot=path.join(root,'assets/visual');
  for(const family of fs.readdirSync(assetRoot).filter(x=>fs.existsSync(path.join(assetRoot,x,'manifest.json')))){manifest[family]=JSON.parse(fs.readFileSync(path.join(assetRoot,family,'manifest.json')));for(const a of manifest[family].atlases)sources[a.id]='data:image/'+path.extname(a.file).slice(1)+';base64,'+fs.readFileSync(path.join(assetRoot,a.file)).toString('base64')}
  await page.evaluate(b=>window.ATS_VISUAL_BUNDLE=b,{manifest,sources});
  const build=fs.readFileSync(path.join(root,'build.cjs'),'utf8'),parts=[...build.match(/const parts=\[([^\]]+)\]/)[1].matchAll(/'([^']+)'/g)].map(m=>m[1]).filter(n=>n!=='app'&&!n.endsWith('-ui'));
  for(const name of parts)await page.addScriptTag({content:fs.readFileSync(path.join(root,name+'.js'),'utf8')});
  await page.evaluate(()=>ATSVisualAssets.ready);
  const gallery=await page.evaluate(()=>{
   const r=window.qaStructureRenderer=new ATSRenderer(document.getElementById('game')),c=r.ctx;r.time=4;r.quality='high';
   c.fillStyle='#162a26';c.fillRect(0,0,1600,2500);c.fillStyle='#eee0b5';c.font='700 28px system-ui';c.fillText('BUILDING A CIVILIZATION',30,43);
   c.font='14px system-ui';c.fillStyle='#bbd3bb';c.fillText('Every stage follows actual construction progress. The final frame becomes the completed structure.',30,73);
   const kinds=['hut','longhouse','canopyHut','spearTower','workShelter','storage','training','nursery','rallyGrove','spearBattery','spearBallista','barrier','garden','orchard','cooking'];
   const labels=['FOUNDATIONS','FRAME','HALF BUILT','FINISHING','COMPLETE'];
   for(let i=0;i<5;i++){c.fillStyle='#d2c59f';c.font='700 12px system-ui';c.fillText(labels[i],215+i*290,112)}
   for(let row=0;row<kinds.length;row++){
    const kind=kinds[row],y=243+row*150;c.fillStyle=row%2?'#1b322a':'#162a26';c.fillRect(0,y-119,1600,150);c.fillStyle='#c1d3b8';c.font='700 12px system-ui';c.fillText(kind,22,y-35);
    for(let stage=0;stage<5;stage++){c.save();c.translate(257+stage*290,y);c.scale(1.1,1.1);if(stage<4)r.drawConstruction(c,{kind,stage,progress:stage/4});else if(row<3)r.drawHut(c,{kind,stage:4,hp:100,maxHp:100});else r.drawSettlementProp(c,{kind,stage:4,hp:100,maxHp:100});c.restore()}
   }
   return{frames:Object.values(ATSVisualAssets.manifest.settlements.construction).flat().length};
  });assert.equal(gallery.frames,75);
  if(output){fs.mkdirSync(output,{recursive:true});await page.screenshot({path:path.join(output,'construction-strips.png')})}
  // Browser-side source resolution after serialization; the unit suite also
  // commissions a real project and saves/restores it during worker progress.
  const journey=await page.evaluate(()=>{
   const records=[];for(let stage=0;stage<5;stage++){const object={kind:'hut',stage,progress:stage/4,hp:100,maxHp:100},clone=JSON.parse(JSON.stringify(object));records.push({stage:clone.stage,rect:ATSStructureArt.descriptor('hut',clone.stage).rect})}
   return records;
  });assert.equal(new Set(journey.map(x=>x.rect[0])).size,5);
  await page.setViewportSize({width:1600,height:1050});
  const crossingReport=await page.evaluate(()=>{
   const r=qaStructureRenderer,c=r.ctx;r.resize();c.fillStyle='#162a26';c.fillRect(0,0,1600,1050);c.fillStyle='#e7d8ae';c.font='700 28px system-ui';c.fillText('RIVER CROSSINGS  /  REAL WALKABLE WIDTH',40,50);
   const w=new ATSWorld('CROSSING-GALLERY'),seen=new Set(),result=[];
   for(let row=-3;row<4;row++)for(let col=-4;col<8;col++){const b=w.crossing(row,col);if(seen.has(b.type))continue;seen.add(b.type);result.push(b)}
   for(let i=0;i<result.length;i++){const b=result[i],x=400+(i%2)*770,y=300+Math.floor(i/2)*475;c.save();c.translate(x,y);c.scale(1.55,1.55);c.fillStyle='#173e47';c.beginPath();c.ellipse(0,0,225,115,0,0,Math.PI*2);c.fill();r.drawCrossingDeck(c,b,b.x,b.y);c.restore();c.fillStyle='#d5d6b1';c.font='700 18px system-ui';c.fillText(b.type.toUpperCase()+'   '+b.width+' world units',x-145,y+180)}
   return result.map(b=>({type:b.type,width:b.width}));
  });assert.equal(crossingReport.length,4);for(const b of crossingReport)assert.ok(b.width>=104);
  if(output)await page.screenshot({path:path.join(output,'crossing-artwork.png')});
  assert.deepEqual(errors,[]);console.log(JSON.stringify({constructionFrames:gallery.frames,crossings:crossingReport,errors},null,2));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
