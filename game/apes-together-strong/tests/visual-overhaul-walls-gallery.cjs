const fs=require('node:fs'),path=require('node:path');
const {chromium}=require('playwright');
const source=path.resolve(__dirname,'..');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
 try{
  const page=await browser.newPage({viewport:{width:1440,height:1080},deviceScaleFactor:1});
  await page.setContent('<html><body style="margin:0;background:#102426"><canvas id="board" width="1440" height="1080" style="width:1440px;height:1080px"></canvas></body></html>');
  const manifest=JSON.parse(fs.readFileSync(path.join(source,'assets/visual/environment/manifest.json'))),sources={};
  for(const a of manifest.atlases)sources[a.id]='data:image/png;base64,'+fs.readFileSync(path.join(source,'assets/visual',a.file)).toString('base64');
  await page.evaluate(data=>window.ATS_VISUAL_BUNDLE=data,{manifest:{environment:manifest},sources});
  for(const name of ['visual-assets','render','render-details','siege-render','graphics','environment-art'])await page.addScriptTag({content:fs.readFileSync(path.join(source,name+'.js'),'utf8')});
  await page.evaluate(()=>ATSVisualAssets.ready);
  const result=await page.evaluate(()=>{
   const r=new ATSRenderer(document.getElementById('board')),c=r.ctx; r.quality='high';r.time=8;r.camera.zoom=1;r.actorSpriteBudget=1000;
   const wall=(id,x,y,w,h,tier=2)=>({id,type:'wall',x,y,w,h,wallTier:tier,visualHeight:tier>=3?76:50,hp:500,maxHp:500,solid:true});
   const gate=(id,x,y,w,h,state)=>({...wall(id,x,y,w,h,3),type:'gate',gateState:state,forcedOpen:state==='open',solid:state==='closed',dead:state==='destroyed',hp:state==='destroyed'?0:500});
   const chain=(axis,tier=2)=>[-80,0,80].map((n,i)=>wall('chain'+i,axis==='x'?n:0,axis==='y'?n:0,axis==='x'?81:18,axis==='y'?81:18,tier));
   const doorway=(axis,state)=>[-100,100].map((n,i)=>wall('doorwall'+i,axis==='x'?n:0,axis==='y'?n:0,axis==='x'?100:18,axis==='y'?100:18,3)).concat(gate('gate',0,0,axis==='x'?100:18,axis==='y'?100:18,state));
   const corner=()=>[wall('a',-40,0,160,18,3),wall('b',32,63,18,144,3)];
   const scenes=[['Connected timber · X axis',chain('x')],['Connected timber · Y axis',chain('y')],['Stone corner + end caps',corner()],['Closed security gate',doorway('x','closed')],['Open gate · X axis',doorway('x','open')],['Open gate · Y axis',doorway('y','open')],['Walkable top + vine access',corner().map(o=>({...o,walkable:true,walkHeight:76,climbAccess:true}))],['Destroyed gate · clear breach',doorway('x','destroyed')],['True service gap stays open',[wall('gapA',-80,0,100,18),wall('gapB',80,0,100,18)]]];
   c.fillStyle='#102426';c.fillRect(0,0,1440,1080);const layouts=[];
   for(let n=0;n<scenes.length;n++){
    const [title,objects]=scenes[n],col=n%3,row=Math.floor(n/3),ox=col*480+240,oy=row*360+236;
    c.save();c.fillStyle='#decea3';c.font='17px system-ui';c.fillText(title,col*480+22,row*360+34);c.fillStyle='#8baba0';c.font='11px system-ui';c.fillText('Original material artwork · unchanged collision footprint',col*480+22,row*360+55);c.restore();
    r._environmentArt={pool:[],cursor:0,observations:new WeakMap(),cageLayouts:new WeakMap(),wallLayouts:new WeakMap(),wallLookupBudget:1000,wallTextures:new Map(),wallTexturePixels:0,wallTextureBudget:1000,focus:[],legacyEffects:[],world:{navRevision:1,chunkRevision:1,getObjects:()=>objects}};
    const sorted=objects.slice().sort((a,b)=>(a.x+a.y)-(b.x+b.y));
    for(const o of sorted){const x=(o.x-o.y)*.8,y=(o.x+o.y)*.42;c.save();c.translate(ox+x*1.17,oy+y*1.17);c.scale(1.17,1.17);r.drawObject(c,o);c.restore();layouts.push(o.type==='gate'?ATSEnvironmentArt.gateLayout(o):ATSEnvironmentArt.wallLayout(o,objects));}
    // Discreet footprint outline verifies gate clearance without filling it.
    c.save();c.translate(ox,oy+3);c.scale(1.17,1.17);c.strokeStyle='#83a89644';c.lineWidth=.6;
    for(const o of objects){const hx=o.w/2,hy=o.h/2;c.beginPath();for(const [i,[x,y]] of [[-hx,-hy],[hx,-hy],[hx,hy],[-hx,hy]].entries()){const px=((x+o.x)-(y+o.y))*.8,py=((x+o.x)+(y+o.y))*.42;i?c.lineTo(px,py):c.moveTo(px,py)}c.closePath();c.stroke()}c.restore();
   }
   return {panels:scenes.length,layouts};
  });
  const out=process.env.QA_ARTIFACT_DIR||path.resolve(__dirname,'../../../work/qa-reports/wall-connectivity');fs.mkdirSync(out,{recursive:true});await page.screenshot({path:path.join(out,'wall-contact-sheet.png')});fs.writeFileSync(path.join(out,'wall-layouts.json'),JSON.stringify(result,null,2));console.log(JSON.stringify({panels:result.panels,screenshot:path.join(out,'wall-contact-sheet.png')}));
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exit(1)});
