import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,mkdir} from 'node:fs/promises';
import {resolve,extname,join} from 'node:path';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
let chromium;try{({chromium}=require('playwright'));}catch{}
const root=resolve(import.meta.dirname,'..');

test('Rifleman follow-up leaves its source separately and small statuses stay attached to recipients',{skip:!chromium},async()=>{
  const server=createServer(async(req,res)=>{try{const file=resolve(root,'.'+new URL(req.url,'http://localhost').pathname);if(!file.startsWith(root+require('node:path').sep))throw Error('path');res.setHeader('Content-Type','text/javascript');res.end(await readFile(file));}catch{res.writeHead(404);res.end();}});
  await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`;let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox']});
    const page=await browser.newPage({viewport:{width:1180,height:520}}),errors=[];page.on('pageerror',error=>errors.push(error.message));
    await page.goto(base+'/renderer.js');await page.setContent(`<style>*{box-sizing:border-box}body{margin:0;padding:24px;background:#0a1220;color:#e6edf6;font:14px system-ui}h1{font-size:23px;margin:0 0 6px}p{color:#aabbd0;margin:0 0 18px}.scenes{display:flex;gap:25px}.scene h2{font-size:13px;font-weight:600}.board{position:relative;width:336px;height:336px;overflow:hidden;border:1px solid #4b6079;border-radius:8px;background:repeating-linear-gradient(0deg,transparent 0 63px,#49657630 63px 68px),repeating-linear-gradient(90deg,#192c36 0 63px,#49657630 63px 68px)}.body{position:absolute;width:64px;height:64px;display:grid;place-items:center;font-size:38px;filter:drop-shadow(0 2px 2px #000);z-index:12}.body small{position:absolute;bottom:0;font:10px system-ui;color:#dce8f5;background:#09121bc9;padding:1px 4px}.layer{position:absolute;inset:0}</style><h1>Faction Forge · attached combat effects</h1><p>Two distinct rounds leave the Rifleman. Shields and status cues stay with affected troops.</p><div class="scenes">${['Primary round · 20 ms','Bonus round · 110 ms','Shielded allies · poisoned enemy'].map((title,i)=>`<section class="scene"><h2>${title}</h2><div class="board" id="scene${i}"><div class="layer"></div></div></section>`).join('')}</div>`);
    const report=await page.evaluate(async base=>{
      const {createBattleRenderer}=await import(base+'/renderer.js');
      const audio=()=>({settings:{audio:false,volume:0},configure:()=>({audio:false,volume:0}),play:()=>false,clear(){},destroy(){},unlock:async()=>false});
      window.reviewRenderers=[];const reports=[];
      for(let i=0;i<3;i++){
        let time=0;const board=document.querySelector('#scene'+i),renderer=createBattleRenderer({layer:board.querySelector('.layer'),columns:5,rows:5,now:()=>time,assets:{enabled:()=>false},audio:audio()});
        const units=[{id:'rifle',type:'rifleman',p:0,x:0,y:4,hp:10,maxhp:10,lvl:2,evolution:'a'},{id:'ally',type:'vguard',p:0,x:2,y:4,hp:12,maxhp:12,lvl:1},{id:'target',type:'vguard',p:1,x:4,y:1,hp:15,maxhp:15,lvl:1}];
        const hosts=new Map();for(const unit of units){const host=document.createElement('div');host.className='body';host.style.left=unit.x*68+'px';host.style.top=unit.y*68+'px';host.innerHTML=`<div class="troop-art">${unit.type==='rifleman'?'🔫':'🛡️'}</div><small>${unit.id==='rifle'?'Rifleman':unit.id==='ally'?'Ally':'Enemy'}</small>`;board.append(host);hosts.set(unit.id,host);renderer.renderUnit(host,unit);}
        if(i<2){
          const event={type:'combat',kind:'attack',unitType:'rifleman',sourceId:'rifle',targetId:'target',x1:0,y1:4,x2:4,y2:1,radius:0};renderer.event({...event,id:'primary'});renderer.event({...event,id:'bonus',kind:'evolution-echo',followUp:true});time=i===0?20:110;renderer.step(time);
          reports.push({shots:renderer.projectiles,chip:hosts.get('rifle').querySelector('.ff-unit-upgrade')?.textContent});
        }else{
          const wall=Date.now();units[0].comboShield=2;units[0].comboShieldUntil=wall+2000;units[1].comboShield=3;units[1].comboShieldUntil=wall+2000;units[2].statusPoisonUntil=wall+2000;units[2].bindUntil=wall+1000;
          for(const unit of units)renderer.renderUnit(hosts.get(unit.id),unit);
          renderer.event({id:'shield',type:'combat',kind:'shield',unitType:'shieldcap',sourceId:'rifle',x1:0,y1:4,x2:0,y2:4,radius:3,targets:units.slice(0,2).map(u=>({id:u.id,x:u.x,y:u.y,amount:2}))});
          renderer.paintFields({units,toxic:[{id:'poison-area',x:4,y:1,radius:1,until:wall+2000}]},wall);time=150;renderer.step(time);
          reports.push({attachments:renderer.attachments,labels:hosts.get('target').getAttribute('aria-label')});
        }
        reviewRenderers.push(renderer);
      }
      return reports;
    },base);
    assert.equal(report[0].chip,'II · BURST');assert.equal(report[0].shots.length,2);assert.equal(report[0].shots.find(s=>s.followUp).start,75);
    assert.equal(report[1].shots.length,1);assert.equal(report[1].shots[0].followUp,true);assert.equal(report[1].shots[0].sourceId,'rifle');assert.equal(report[1].shots[0].x,32);assert.equal(report[1].shots[0].y,304);assert.equal(report[1].shots[0].x2,304);assert.equal(report[1].shots[0].y2,100);
    assert.ok(report[2].attachments.every(e=>e.radius<=64*.4));assert.match(report[2].labels,/Poisoned.*Bound/);assert.ok(report[2].attachments.some(e=>e.family==='unit-shield'&&e.sourceId==='ally'&&e.x===168&&e.y===304));
    if(process.env.FACTION_FORGE_VISUAL_DIR){await mkdir(process.env.FACTION_FORGE_VISUAL_DIR,{recursive:true});await page.screenshot({path:join(process.env.FACTION_FORGE_VISUAL_DIR,'rifleman-burst-attachments.png'),fullPage:true});}
    assert.deepEqual(errors,[]);await page.evaluate(()=>reviewRenderers.forEach(renderer=>renderer.destroy()));
  }finally{await browser?.close();await new Promise(r=>server.close(r));}
});

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
