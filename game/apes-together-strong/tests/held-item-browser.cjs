'use strict';
// Actual Canvas raster QA: every held item, all eight facings, animated sockets.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const root=path.resolve(__dirname,'..');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',args:['--allow-file-access-from-files']});
 const page=await browser.newPage({viewport:{width:1680,height:1780},deviceScaleFactor:1}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 const manifest={},sources={};for(const family of fs.readdirSync(path.join(root,'assets','visual')).filter(f=>fs.existsSync(path.join(root,'assets','visual',f,'manifest.json')))){const m=JSON.parse(fs.readFileSync(path.join(root,'assets','visual',family,'manifest.json'),'utf8'));manifest[family]=m;for(const a of m.atlases)sources[a.id]='data:image/'+path.extname(a.file).slice(1)+';base64,'+fs.readFileSync(path.join(root,'assets','visual',a.file)).toString('base64')}
 await page.setContent('<body style="margin:0;background:#183027;color:#e2d4b3;font:15px system-ui"><canvas id="gallery" width="1680" height="1780"></canvas></body>');
 await page.evaluate(async ({manifest,sources})=>{const imgs=new Map();await Promise.all(Object.entries(sources).map(([id,src])=>new Promise((resolve,reject)=>{const image=new Image();image.onload=()=>{imgs.set(id,image);resolve()};image.onerror=reject;image.src=src})));window.ATSVisualAssets={manifest,get:id=>imgs.get(id)};}, {manifest,sources});
 const parts=[...fs.readFileSync(path.join(root,'build.cjs'),'utf8').match(/const parts=\[([^\]]+)\]/)[1].matchAll(/'([^']+)'/g)].map(m=>m[1]).filter(n=>n!=='app'&&n!=='visual-assets'&&!n.endsWith('-ui'));
 for(const module of parts)await page.addScriptTag({content:fs.readFileSync(path.join(root,module+'.js'),'utf8')});
 const report=await page.evaluate(()=>{
  const r=new ATSRenderer(document.getElementById('gallery'));Object.assign(r,{time:10,detailLevel:0,quality:'high',reducedMotion:true});r.camera.zoom=1;const c=r.ctx;c.fillStyle='#183027';c.fillRect(0,0,1680,1780);
  const rows=[['Rifle aim',{type:'human',kind:'rifle',role:'rifleman',state:'combat',aiming:true}],['Riot shield',{type:'human',kind:'pistol',role:'shield',state:'patrol'}],['Scout spear',{species:'gibbon',state:'scout',equipment:{spear:true}}],['Maul attack',{species:'gorilla',champion:{archetype:'wallbreaker'},animation:{kind:'overhead',start:9.9,duration:.6}}],['Moving torch',{species:'orangutan',moving:true,equipment:{torch:true}}],['Radio',{type:'human',kind:'pistol',role:'officer',state:'radio'}],['Wood carrier',{species:'chimpanzee',carrying:'wood'}],['Spear + champion',{species:'capuchin',champion:{archetype:'alarmSpoiler'},equipment:{spear:true,torch:true}}]];
  const names=['E','SE','S','SW','W','NW','N','NE'],angles=names.map((_,n)=>{const t=n*Math.PI/4;return Math.atan2(Math.sin(t)/.42-Math.cos(t)/.8,Math.sin(t)/.42+Math.cos(t)/.8)});
  const checks=[];
  rows.forEach(([label,fields],y)=>{for(let d=0;d<8;d++){
   const a={id:'qa'+y+'-'+d,hp:100,maxHp:100,species:'gorilla',phase:0,bodyScale:1,state:'follow',dir:angles[d],...fields};
   c.fillStyle='#8eab98';c.font='13px system-ui';c.fillText(label+' '+names[d],d*210+12,y*215+23);c.strokeStyle='#325344';c.strokeRect(d*210+2,y*215+30,205,175);
   c.save();c.translate(d*210+105,y*215+181);c.scale(2,2);if(a.type==='human')r.drawHuman(c,a);else r.drawApe(c,a,false,0);c.restore();
   const pose=ATSCharacterArt.resolve(a,r.time,{human:a.type==='human',reducedMotion:true});checks.push({pose:pose.direction,ids:ATSHeldItemArt.resolve(a,pose).map(x=>x.id)});
  }});
  const moving={id:'run',type:'human',kind:'rifle',hp:100,moving:true,phase:0,dir:0};r.reducedMotion=false;
  const points=[10,10.15,10.3,10.45].map(t=>{const p=ATSCharacterArt.resolve(moving,t);return ATSHeldItemArt.sockets(moving,p,60).right});
  return {status:ATSHeldItemArt.status(),checks,draws:r.heldItemArtworkDraws,points,pixels:Array.from(c.getImageData(0,0,1680,1780).data).filter((_,i)=>i%4===3).reduce((n,a)=>n+(a>0),0)};
 });
 assert.deepEqual(errors,[]);assert.equal(report.status.ready,true);assert.equal(report.status.frames,41);assert.equal(report.checks.length,64);assert.ok(report.draws>=64);assert.ok(new Set(report.points.map(p=>JSON.stringify(p))).size>1,'moving hands must follow actual stride frames');
 const output=process.env.HELD_ITEM_QA_OUTPUT||path.join(root,'tests','held-item-gallery.png');await page.screenshot({path:output});
 const integrated=await page.evaluate(()=>{
  const r=new ATSRenderer(document.getElementById('gallery'));r.quality='high';const g=new ATSGame('HELD-EQUIPMENT-QA','survival');g.apes=[];g.humans=[];g.vehicles=[];g.helis=[];g.corpses=[];g.effects=[];g.king.x=g.king.y=0;g.time=10;
  g.world.getObjects=()=>[];g.world.getSites=()=>[];g.world.terrain=()=>({biome:'forest',road:true});g.world.lineClear=()=>true;
  const types=Object.keys(ATSChampionClasses);for(let i=0;i<48;i++){const a=g.makeApe((i%8-3.5)*55,(Math.floor(i/8)-2.5)*65,'scout');a.equipment=i%3?{spear:true}:{torch:true};a.champion={archetype:types[i%types.length]};a.moving=true;if(i%8===0)a.carrying='wood'}
  for(const [i,role]of Object.keys(ATSHumanRoles).entries()){const h=g.makeHuman((i%8-3.5)*68,(Math.floor(i/8)-1)*90);g.forces.assign(h,null,role);h.moving=true;h.dir=i*Math.PI/4}g.syncIndexes();
  const snapshot=()=>JSON.stringify([...g.apes,...g.humans].map(a=>({id:a.id,x:a.x,y:a.y,hp:a.hp,state:a.state,equipment:a.equipment,champion:a.champion,carrying:a.carrying}))),before=snapshot();
  for(let i=0;i<40;i++){g.time=10+i/60;r.draw(g,1/60)}
  const unchanged=before===snapshot();const a=g.apes.find(a=>!a.carrying);r.time=10;const pose=ATSCharacterArt.resolve(a,10),expected=ATSHeldItemArt.resolve(a,pose).length,prior=r.heldItemArtworkDraws;r.drawApe(r.ctx,a,false,0);const actual=r.heldItemArtworkDraws-prior;
  const normalGet=ATSVisualAssets.get;ATSVisualAssets.get=id=>id==='held-combat'?null:normalGet(id);const humanBefore=r.characterArtworkDraws;r.drawHuman(r.ctx,g.humans[0]);ATSVisualAssets.get=normalGet;
  return {apes:g.apes.length,humans:g.humans.length,frames:40,stateUnchanged:unchanged,duplicateFree:actual===expected,expectedItems:expected,actualItems:actual,missingHeldUsesLegacy:r.characterArtworkDraws===humanBefore,rasterDraws:r.heldItemArtworkDraws};
 });
 assert.ok(integrated.stateUnchanged);assert.ok(integrated.duplicateFree);assert.ok(integrated.missingHeldUsesLegacy);assert.deepEqual(errors,[]);await page.screenshot({path:output.replace(/\.png$/,'-integrated.png')});await browser.close();console.log(JSON.stringify({pass:true,frames:report.status.frames,poses:report.checks.length,rasterDraws:report.draws,movingHandPositions:report.points,screenshot:output,integrated},null,2));
})().catch(e=>{console.error(e);process.exitCode=1});
