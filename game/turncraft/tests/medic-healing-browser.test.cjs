const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const manifest=require('../assets/animation-manifest.json');
const root=path.resolve(__dirname,'../../..');

test('painted Medic atlases decode with isolated transparent gutters, 64 unique poses and visible body-attached beam in browser',async()=>{
  const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
  try{
    const page=await browser.newPage({viewport:{width:600,height:400}});
    await page.setContent('<canvas width="320" height="240"></canvas>');
    await page.addScriptTag({path:path.join(root,'game/turncraft/animation.js')});
    const data=await page.evaluate(async({manifest,body,effects})=>{
      async function decode(data){const im=new Image();im.src='data:image/webp;base64,'+data;await im.decode();return im}
      const unit=await decode(body),fx=await decode(effects),canvas=document.querySelector('canvas'),ctx=canvas.getContext('2d'),checker=document.createElement('canvas');checker.width=128;checker.height=160;const check=checker.getContext('2d'),hashes=[];let borders=0,minimum=Infinity;
      for(let row=0;row<8;row++)for(let col=0;col<8;col++){
        check.clearRect(0,0,128,160);check.drawImage(unit,col*128,row*160,128,160,0,0,128,160);const pixels=check.getImageData(0,0,128,160).data;let hash=2166136261,opaque=0;
        for(let p=0;p<pixels.length;p++){hash=Math.imul(hash^pixels[p],16777619);if(p%4===3&&pixels[p]){opaque++;const pos=(p-3)/4,x=pos%128,y=Math.floor(pos/128);if(x<4||x>=124||y<4||y>=156)borders++}}
        hashes.push(hash>>>0);minimum=Math.min(minimum,opaque);
      }
      const cache={setBudget(){},setResolution(){},setCropRepairs(){},setLocalFallbacks(){},beginFrame(){},evict(){},sourceScale:()=>1,get:file=>file.includes('/effects/')?fx:unit};
      const runtime=new TurncraftAnimation.SpriteRuntime({manifest,cache}),healer={id:1,type:'medic',kind:'unit',alive:true,faction:0,x:1,y:2,px:1,py:2,order:'idle',healOrder:'idle',healTargetId:2,healStartedAt:1000,healUntil:2400,healRange:3},patient={id:2,type:'rifleman',kind:'unit',alive:true,faction:0,x:4,y:2,px:4,py:2};
      runtime.beginFrame([healer,patient],1350);ctx.fillStyle='#10273a';ctx.fillRect(0,0,320,240);ctx.save();ctx.translate(72,120);runtime.draw(ctx,healer,1350,{size:48});ctx.restore();runtime.drawState(ctx,'medic','healPrepare',{x:216,y:133,size:48,elapsedMs:0,direction:'W'});
      const before=ctx.getImageData(128,80,40,60).data.slice();const draws=runtime.drawHealingEffects(ctx,1350,{size:48,worldToScreen:(x,y)=>({x:x*48,y:y*48})}),after=ctx.getImageData(128,80,40,60).data;let changed=0;
      for(let i=0;i<after.length;i++)if(before[i]!==after[i])changed++;
      healer.order='stop';runtime.beginFrame([healer,patient],1351);const stopped=runtime.drawHealingEffects(ctx,1351,{size:48,worldToScreen:(x,y)=>({x:x*48,y:y*48})});
      return{unit:[unit.naturalWidth,unit.naturalHeight],fx:[fx.naturalWidth,fx.naturalHeight],unique:new Set(hashes).size,borders,minimum,draws,changed,stopped};
    },{manifest,body:fs.readFileSync(path.join(root,'units/medic-healing.webp')).toString('base64'),effects:fs.readFileSync(path.join(root,'effects/medic-healing.webp')).toString('base64')});
    assert.deepEqual(data.unit,[1024,1280]);assert.deepEqual(data.fx,[1024,256]);assert.equal(data.unique,64);assert.equal(data.borders,0);assert.ok(data.minimum>2000);assert.equal(data.draws,1);assert.ok(data.changed>20,'visible beam pixels connect the units');assert.equal(data.stopped,0);
    if(process.env.TURNCRAFT_HEALING_SCREENSHOT)await page.screenshot({path:process.env.TURNCRAFT_HEALING_SCREENSHOT});
  }finally{await browser.close()}
});
