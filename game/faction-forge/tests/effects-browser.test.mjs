import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,mkdir} from 'node:fs/promises';
import {resolve,extname,join} from 'node:path';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
let chromium;try{({chromium}=require('playwright'));}catch{}
const root=resolve(import.meta.dirname,'..');

test('physical missiles, storms, BloodBurst and every unit effect render on desktop/mobile with bounded canvas surfaces',{skip:!chromium},async()=>{
  const server=createServer(async(req,res)=>{try{const path=resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!path.startsWith(root+require('node:path').sep))throw Error('path');res.setHeader('Content-Type',extname(path)==='.html'?'text/html':'text/javascript');res.end(await readFile(path));}catch{res.writeHead(404);res.end();}});
  await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    const page=await browser.newPage({viewport:{width:1050,height:570},deviceScaleFactor:2}),errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.goto(base+'/renderer.js');await page.setContent(`<style>*{box-sizing:border-box}body{margin:0;padding:28px;background:#080f1c;color:#d8e4f4;font:14px system-ui}h1{font-size:22px;margin:0 0 8px}p{color:#9cafc8;margin:0 0 22px}.boards{display:flex;gap:26px;flex-wrap:wrap}.frame{width:288px;max-width:100%;height:425px}.frame h2{font-size:12px;letter-spacing:.16em;margin:0 0 14px;color:#d8c497}.board{position:relative;width:280px;height:388px;border:1px solid #628a7260;border-radius:12px;background:repeating-linear-gradient(0deg,transparent 0 51px,#517b6638 51px 55px,transparent 55px 56px),repeating-linear-gradient(90deg,#203e33 0 51px,#304c4038 51px 55px,#233d34 55px 56px);overflow:hidden}.tag{position:absolute;padding:5px 7px;border:1px solid #abc9e24d;border-radius:4px;background:#101d2bd9;color:#d8e8fa;z-index:12;font-size:10px;transform:translate(-50%,-50%)}@media(max-width:600px){body{padding:15px}.boards{gap:16px}.frame{height:425px}}</style><h1>Faction Forge · combat effects</h1><p>Actual source and target positions · separate presentation clock · existing artwork stays intact</p><div class="boards"><section class="frame"><h2>VANIR · PHYSICAL MISSILE</h2><div class="board" id="vanir"><div class="layer"></div><span class="tag" style="left:26px;top:362px">Rockets</span><span class="tag" style="left:250px;top:82px">Impact target</span></div></section><section class="frame"><h2>AESIR · STATIC TEMPEST</h2><div class="board" id="aesir"><div class="layer"></div><span class="tag" style="left:26px;top:306px">Storm Seer</span><span class="tag" style="left:138px;top:194px">Bound target</span></div></section><section class="frame"><h2>HELIONS · BLOODBURST</h2><div class="board" id="helions"><div class="layer"></div><span class="tag" style="left:26px;top:306px">BloodBurst</span><span class="tag" style="left:138px;top:194px">Splash center</span></div></section></div>`);
    const result=await page.evaluate(async base=>{
      const {createBattleRenderer}=await import(base+'/renderer.js'),{UNIT_EFFECTS}=await import(base+'/effects.js'),{COMBOS}=await import(base+'/combos.js');
      let clock=0;window.renderers=[];
      for(const [id,unitType] of [['vanir','vrocket'],['aesir','storm'],['helions','blood']]){
        const layer=document.querySelector('#'+id+' .layer'),renderer=createBattleRenderer({layer,cell:52,gap:4,columns:5,rows:7,now:()=>clock,assets:{enabled:()=>false},intensity:'high',audio:{settings:{audio:false,volume:0},configure:()=>({audio:false,volume:0}),play:()=>false,clear(){},destroy(){},unlock:async()=>false}});
        renderer.event({id:id+'attack',type:'combat',kind:'attack',unitType,sourceId:'source',targetId:'target',x1:0,y1:6,x2:id==='vanir'?4:2,y2:id==='vanir'?1:3,radius:1,battleRank:3,targets:unitType==='storm'?[{id:'chain1',x:3,y:3},{id:'chain2',x:4,y:4}]:[]});
        if(id==='aesir')renderer.event({id:'tempest',type:'combat',kind:'combo',comboId:'static-tempest',name:'Static Tempest',unitType:'storm',sourceId:'source',targetId:'target',x1:0,y1:6,x2:2,y2:3,radius:1,targets:[{id:'chain1',x:3,y:3}]});
        clock=id==='vanir'?80:id==='aesir'?160:185;renderer.step(clock);clock=0;window.renderers.push(renderer);
      }
      const pixels=Array.from(document.querySelectorAll('.ff-battle-effects')).map(canvas=>{const data=canvas.getContext('2d').getImageData(0,0,canvas.width,canvas.height).data;let count=0;for(let i=3;i<data.length;i+=4)if(data[i]>0)count++;return count;});
      const renderer=renderers[0],before=renderer.stats.active;
      const immutable=Object.freeze({id:'immutable',type:'combat',kind:'attack',unitType:'vrocket',x1:1,y1:6,x2:4,y2:1,sourceId:'s',targetId:'t',radius:1,at:123});renderer.event(immutable);renderer.event(immutable);const once=renderer.stats.active;renderer.event(immutable);
      for(const unitType of Object.keys(UNIT_EFFECTS)){renderer.event({id:unitType+'test',type:'combat',kind:'attack',unitType,x1:0,y1:5,x2:3,y2:2,radius:1});renderer.event({id:unitType+'ability',type:'combat',kind:'ability',unitType,x1:0,y1:5,x2:3,y2:2,radius:1});}
      for(const combo of COMBOS)renderer.event({id:combo.id,type:'combat',kind:'combo',comboId:combo.id,name:combo.name,x1:0,y1:5,x2:3,y2:2,radius:1,targets:[{id:'extra',x:4,y:2}]});
      for(const kind of ['lightning-field','blood-field','rupture','rocket-explosion','precision-hit','evolution-echo','rail-impact','shockwave','ward-break'])renderer.event({id:'signature-'+kind,type:'combat',kind,unitType:'rifleman',x1:0,y1:5,x2:3,y2:2,radius:1,targets:[{id:'extra',x:4,y:2,amount:2}]});
      clock=250;renderer.step(clock);const bounded=renderer.stats.active,after=renderer.stats;
      renderer.configure({intensity:'low',reducedMotion:true,screenFlash:false});for(let i=0;i<300;i++)renderer.event({id:'load'+i,type:'combat',kind:'attack',unitType:'vrocket',x1:0,y1:6,x2:4,y2:1,radius:1});clock=500;renderer.step(clock);const low=renderer.stats.active;
      renderer.clear();const cleared=renderer.stats.active;
      // Restore the missile portrait for a human visual check.
      renderer.configure({intensity:'high',reducedMotion:false});clock=0;renderer.event({id:'portrait',type:'combat',kind:'attack',unitType:'vrocket',x1:0,y1:6,x2:4,y2:1,radius:1});clock=80;renderer.step(clock);
      // Missing evolved art must retain the working lower-tier animated sheet.
      const host=document.createElement('div');host.innerHTML='<div class="troop-art"></div>';document.body.appendChild(host);
      const sheet=document.createElement('canvas');sheet.width=sheet.height=128;sheet.getContext('2d').fillStyle='#00ff00';sheet.getContext('2d').fillRect(0,0,128,128);
      const requested=[],spec={frameWidth:128,frameHeight:128,columns:1,pivot:[.5,.9375],animations:{idle:{frames:[0],duration:900}}};
      const lower=createBattleRenderer({now:()=>clock,assets:{enabled:()=>true,unit:async(key,tier)=>{requested.push(tier);if(tier===2)throw Error('missing evolution');return tier===1?{image:sheet,spec}:null;}}});
      const unit={id:'rank-fallback',type:'phaser',x:1,y:4,hp:8,forgeTier:1,lvl:1};lower.renderUnit(host,unit);await new Promise(r=>setTimeout(r,0));lower.renderUnit(host,unit);const working=host.querySelector('canvas');unit.lvl=3;const retainedWhileLoading=lower.renderUnit(host,unit);await new Promise(r=>setTimeout(r,0));const retainedAfterFailure=lower.renderUnit(host,unit);const lowerPixel=[...working.getContext('2d').getImageData(64,64,1,1).data];lower.destroy();host.remove();
      return {pixels,canvases:document.querySelectorAll('.ff-battle-effects').length,before,once,bounded,after,low,cleared,dprWidths:[...document.querySelectorAll('.ff-battle-effects')].map(c=>c.width),immutable,requested,retainedWhileLoading,retainedAfterFailure,lowerPixel};
    },base);
    assert.equal(result.canvases,6);assert.ok(result.pixels.every(n=>n>=0));assert.ok(result.pixels[1]>150,'missile is physically painted');assert.ok(result.pixels[3]>500,'clouds and lightning painted');assert.ok(result.pixels[5]>500,'layered BloodBurst blast painted');assert.ok(result.bounded<=420);assert.ok(result.low<=130);assert.equal(result.cleared,0);assert.ok(result.dprWidths.every(width=>width<=414),'canvas pixel ratio is capped at 1.5');
    assert.deepEqual(result.requested,[1,3,2,1]);assert.equal(result.retainedWhileLoading,true);assert.equal(result.retainedAfterFailure,true);assert.deepEqual(result.lowerPixel,[0,255,0,255]);
    if(process.env.FACTION_FORGE_VISUAL_DIR){await mkdir(process.env.FACTION_FORGE_VISUAL_DIR,{recursive:true});await page.screenshot({path:join(process.env.FACTION_FORGE_VISUAL_DIR,'battle-effects-desktop.png'),fullPage:true});}
    await page.setViewportSize({width:390,height:844});assert.ok(await page.evaluate(()=>document.body.scrollWidth<=innerWidth));
    if(process.env.FACTION_FORGE_VISUAL_DIR)await page.screenshot({path:join(process.env.FACTION_FORGE_VISUAL_DIR,'battle-effects-mobile.png'),fullPage:true});
    await page.evaluate(()=>{for(const renderer of renderers)renderer.destroy();});assert.equal(await page.locator('.ff-battle-effects').count(),0);assert.deepEqual(errors,[]);
  }finally{await browser?.close();await new Promise(r=>server.close(r));}
});
