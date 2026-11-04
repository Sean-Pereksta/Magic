'use strict';
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const {instrument,sourcePath} = require('./deep-sea-harness.cjs');

function getPlaywright(){
  return require(process.env.DEEP_SEA_PLAYWRIGHT || 'playwright');
}
const percentile=(xs,p)=>xs.length?[...xs].sort((a,b)=>a-b)[Math.min(xs.length-1,Math.floor(xs.length*p))]:0;
const summary=xs=>({count:xs.length,medianMs:+percentile(xs,.5).toFixed(2),p95Ms:+percentile(xs,.95).toFixed(2),maxMs:+Math.max(0,...xs).toFixed(2)});

async function main(){
  const baseline=process.argv.includes('--baseline');
  const target=process.env.DEEP_SEA_SOURCE || sourcePath;
  const label=baseline?'baseline':'living';
  const out=process.env.DEEP_SEA_OUTPUTS;
  const html=instrument(fs.readFileSync(target,'utf8'));
  const browser=await getPlaywright().chromium.launch({headless:true,...(process.env.DEEP_SEA_BROWSER?{executablePath:process.env.DEEP_SEA_BROWSER}:{})});
  const results=[];
  try{
    for(const mobile of [false,true]){
      const context=await browser.newContext({viewport:mobile?{width:390,height:844}:{width:1280,height:720},deviceScaleFactor:mobile?2:1,isMobile:mobile,hasTouch:mobile});
      const page=await context.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
      await page.addInitScript(()=>{let seed=42;Math.random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};});
      // The game is self-contained: fulfill a synthetic URL without external network.
      await page.route('**/deep-sea',route=>route.fulfill({status:200,contentType:'text/html',body:html}));
      await page.goto('http://deep-sea.test/deep-sea',{waitUntil:'load'});
      if(out && !baseline){fs.mkdirSync(out,{recursive:true});await page.screenshot({path:path.join(out,`deep-sea-menu-${mobile?'mobile':'desktop'}.png`)});}
      await page.locator('#startBtn').click();
      await page.locator('.upgradeCard').first().click();
      assert.equal(await page.evaluate(()=>__deep.state.mode),'play','start and choose evolution');
      if(!baseline){
        assert.equal(await page.locator('body').evaluate(el=>el.classList.contains('mobile-play')),mobile,'correct desktop/touch controls');
        if(!mobile)assert.equal(await page.locator('#hud').isVisible(),true,'desktop HUD stays visible');
      }
      await page.waitForTimeout(700);
      if(out){fs.mkdirSync(out,{recursive:true});await page.screenshot({path:path.join(out,`deep-sea-${label}-${mobile?'mobile':'desktop'}.png`)});}
      await page.evaluate(()=>{const g=__deep;g.state.player.mods.predatorResist=100;g.state.player.nextUpgradeAt=1e9;});
      const before=await page.evaluate(()=>__deep.state.player.yaw);
      if(mobile){
        await page.locator('#game').dispatchEvent('pointerdown',{pointerId:1,pointerType:'touch',clientX:65,clientY:670,isPrimary:true});
        await page.locator('#game').dispatchEvent('pointermove',{pointerId:1,pointerType:'touch',clientX:105,clientY:670,isPrimary:true});
        await page.waitForTimeout(200);
        await page.locator('#game').dispatchEvent('pointerup',{pointerId:1,pointerType:'touch',clientX:105,clientY:670,isPrimary:true});
      }else{
        await page.keyboard.down('Shift');await page.waitForTimeout(250);await page.keyboard.up('Shift');
        assert.ok(await page.evaluate(()=>__deep.state.player.sprintEnergy<100),'keyboard sprint drains energy');
        if(!baseline){
          const heading=await page.evaluate(()=>__deep.state.player.yaw);
          await page.keyboard.down('j');await page.waitForTimeout(150);await page.keyboard.up('j');
          assert.ok(Math.abs(await page.evaluate(()=>__deep.state.player.yaw)-heading)>.03,'J steers without pointer lock');
          const pitch=await page.evaluate(()=>__deep.state.player.pitch);
          await page.keyboard.down('i');await page.waitForTimeout(150);await page.keyboard.up('i');
          assert.ok(Math.abs(await page.evaluate(()=>__deep.state.player.pitch)-pitch)>.01,'I steers vertically');
        }
      }
      if(mobile)assert.ok(Math.abs(await page.evaluate(()=>__deep.state.player.yaw)-before)>.03,'touch stick changes heading');
      await page.evaluate(()=>{
        __deep.renderSamples.length=0;__deep.updateSamples.length=0;
        window.__rafTimes=[];let last=performance.now();
        function sample(t){__rafTimes.push(t-last);last=t;if(__rafTimes.length<240)requestAnimationFrame(sample);}requestAnimationFrame(sample);
      });
      await page.waitForTimeout(4500);
      const data=await page.evaluate(()=>({render:[...__deep.renderSamples],updates:[...__deep.updateSamples],raf:__rafTimes,fish:__deep.state.fish.length,mode:__deep.state.mode,edible:__deep.state.fish.filter(f=>f.size<__deep.state.player.size&&__deep.dist3(f.pos,__deep.state.player.pos)<100).length,score:__deep.state.player.fishEaten,perf:__deep.state.perf||null}));
      assert.equal(data.mode,'play');assert.equal(errors.length,0,errors.join('\n'));
      results.push({label,device:mobile?'mobile':'desktop',viewport:mobile?'390x844@2':'1280x720@1',render:summary(data.render),simulation:summary(data.updates),frameInterval:summary(data.raf.filter(x=>x>0)),fish:data.fish,edibleWithin100:data.edible,fishEaten:data.score,perf:data.perf,errors});
      if(!baseline){
        await page.evaluate(()=>document.exitPointerLock?.());
        await page.locator('#pauseBtn').click();
        const paused=await page.evaluate(()=>({mode:__deep.state.mode,time:__deep.state.t}));
        assert.equal(paused.mode,'paused');await page.waitForTimeout(200);
        assert.equal(await page.evaluate(()=>__deep.state.t),paused.time,'pause freezes world clock');
        await page.locator('#resumeBtn').click();assert.equal(await page.evaluate(()=>__deep.state.mode),'play');
        await page.locator('#qualitySelect').selectOption('fast');
        const fast=await page.evaluate(()=>({quality:__deep.state.quality,width:document.querySelector('#game').width}));
        assert.equal(fast.quality,0);
        await page.locator('#qualitySelect').selectOption('vivid');
        const vivid=await page.evaluate(()=>({quality:__deep.state.quality,width:document.querySelector('#game').width}));
        assert.equal(vivid.quality,2);assert.ok(vivid.width>fast.width,'quality changes rendering resolution');
        await page.locator('#qualitySelect').selectOption('auto');
        await page.evaluate(()=>{
          const g=__deep,p=g.state.player;
          p.pos={x:2290,y:g.waterColumnY(2290,0,-1370,24),z:0};p.yaw=0;p.pitch=0;
          p.vel={x:0,y:0,z:0};p.combo=0;p.comboUntil=0;p.nextUpgradeAt=p.fishEaten+4;
          g.state.fish=[];for(let i=0;i<g.MAX_FISH;i++)g.spawnFish(false);
          for(let i=0;i<48;i++)g.updateObstacles(.016);
          g.updatePlants(.016);g.updateBubbles(.016);g.updateWaterParticles(.016);
          g.rebuild();g.updateCamera(.4);g.updatePredatorPanic(.016);g.updateUI();g.state.mode='preview';g.render();
        });
        await page.waitForTimeout(150);
        if(out)await page.screenshot({path:path.join(out,`deep-sea-abyss-${mobile?'mobile':'desktop'}.png`)});
        assert.equal(errors.length,0,errors.join('\n'));
      }
      await context.close();
    }
    const report={testedAt:new Date().toISOString(),source:target,results};
    if(out)fs.writeFileSync(path.join(out,`deep-sea-${label}-performance.json`),JSON.stringify(report,null,2)+'\n');
    console.log(JSON.stringify(report,null,2));
  }finally{await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
