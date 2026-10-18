'use strict';
// Strategic movement through the real game collision/navigation pipeline.
// The local server alone injects observation/stepping hooks. No production hooks,
// actor teleporting, path substitutes, capture shortcuts, or human input are used.
const assert=require('node:assert/strict'),fs=require('node:fs'),http=require('node:http'),path=require('node:path');
const {chromium}=require('playwright');
const repo=path.resolve(__dirname,'..'),out=path.resolve(process.env.NEON_FRONTLINE_REPORT||path.join(repo,'../frontline-ai-browser'));
const maps=(process.env.NEON_FRONTLINE_MAPS||'cold_storage,floodgate').split(',').map(s=>s.trim()).filter(Boolean);
const seconds=Math.max(15,Math.min(90,Number(process.env.NEON_FRONTLINE_SECONDS)||60));
const timeoutMs=Math.max(30000,Math.min(300000,Number(process.env.NEON_FRONTLINE_TIMEOUT_MS)||180000));
const hooks=`
window.__FRONTLINE_TEST__={
  game,startArenaMatch,
  begin(){
    if(!game.started||game.over||!conquest)throw Error('Conquest did not start: '+el('mapDescription').textContent);
    // The idle observer remains at the authored spawn and cannot be targeted or capture a flag.
    player.spawnGrace=1e6;player.vx=player.vy=player.vz=0;
    const observer={x:player.x,y:player.y,z:player.z},initial={};
    for(const team of [1,2]){
      const units=bots.filter(b=>!b.human&&b.team===team),order=warObjective(units[0]);
      initial[team]={goal:{x:order.x,y:order.y,z:order.z},actors:units.map(a=>({id:a.entityId,x:a.x,y:a.y,z:a.z})),distance:units.reduce((sum,a)=>sum+Math.hypot(a.x-order.x,a.z-order.z),0)/units.length};
    }
    this.observer=observer;this.initial=initial;this.closest={1:initial[1].distance,2:initial[2].distance};this.captures=[];this.ticks=0;
    return {map:game.map,navPoints:nav.length,initial,observer,actors:bots.length};
  },
  step(count=90,dt=1/30){
    for(let tick=0;tick<count&&!game.over;tick++){
      player.spawnGrace=1e6;warClock+=dt;updateWar(dt);
      for(const event of conquest.events)if(event.type==='captured')this.captures.push({...event,time:warClock});
      for(const b of [...bots])updateBot(b,dt);
      updateProjectiles(dt);
      // This check exercises simulation, not GPU/effect lifetime performance.
      particles.length=shots.length=lasers.length=impacts.length=muzzleFlashes.length=ultimateFx.length=0;
      for(const team of [1,2]){
        const units=bots.filter(b=>!b.human&&!b.dead&&b.team===team),goal=this.initial[team].goal;
        if(units.length)this.closest[team]=Math.min(this.closest[team],units.reduce((sum,a)=>sum+Math.hypot(a.x-goal.x,a.z-goal.z),0)/units.length);
      }
      this.ticks++;
    }
    return this.snapshot();
  },
  snapshot(){return {map:game.map,elapsed:warClock,ticks:this.ticks,over:game.over,initial:this.initial,closest:this.closest,captures:this.captures,observer:this.observer,player:{x:player.x,y:player.y,z:player.z,dead:player.dead},sectors:conquest.sectors.map(s=>({id:s.id,owner:s.owner,progress:s.progress,contested:s.contested})),navigation:humanBotController().stats(),actors:bots.map(a=>({id:a.entityId,team:a.team,x:a.x,y:a.y,z:a.z,dead:a.dead,hp:a.hp,tactic:a.tacticalState,role:a.squadRole,target:a.target,waypoint:a.route}))};}
};`;
const server=http.createServer((req,res)=>{
  try{
    const pathname=decodeURIComponent(new URL(req.url,'http://localhost').pathname);
    if(pathname==='/favicon.ico'){res.writeHead(204);res.end();return;}
    const file=path.resolve(repo,'.'+(pathname.endsWith('/')?pathname+'index.html':pathname));
    if(!file.startsWith(repo+path.sep))throw Error('Outside repository');
    let data=fs.readFileSync(file);
    if(file.endsWith('neon-arena.js')){const source=data.toString();assert.match(source,/\}\)\(\);\s*$/);data=Buffer.from(source.replace(/\}\)\(\);\s*$/,hooks+'})();'));}
    res.writeHead(200,{'Cache-Control':'no-store','Content-Type':({'.js':'text/javascript','.mjs':'text/javascript','.html':'text/html','.json':'application/json','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp','.png':'image/png','.glb':'model/gltf-binary'}[path.extname(file)]||'application/octet-stream')});res.end(data);
  }catch{res.writeHead(404);res.end('Missing local fixture');}
});
function bounded(promise,ms,label){let timer;return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(label+' exceeded '+ms+'ms')),ms);})]).finally(()=>clearTimeout(timer));}
async function run(){
  fs.mkdirSync(out,{recursive:true});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));let browser;const report=[];
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||process.env.CHROME_PATH||(process.platform==='win32'?'C:/Program Files/Google/Chrome/Application/chrome.exe':undefined),args:['--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
    const origin='http://127.0.0.1:'+server.address().port;
    for(const map of maps){
      const context=await browser.newContext({viewport:{width:960,height:640},deviceScaleFactor:1}),page=await context.newPage(),errors=[];
      page.on('pageerror',e=>errors.push(e.message));
      await page.addInitScript(()=>{
        let seed=7135461;Math.random=()=>((seed=(Math.imul(seed,1664525)+1013904223)>>>0)/4294967296);
        window.requestAnimationFrame=()=>0;
        HTMLCanvasElement.prototype.requestPointerLock=function(){};
        window.NEON_ASSET_CONFIG={ASSET_BASE_URL:'',UI_ASSET_BASE_URL:'./assets',EXPANSION_ASSET_BASE_URL:'./assets',EXPANSION_FALLBACK_BASE_URL:'./assets',version:''};
      });
      await page.route('**/*',route=>route.request().url().startsWith(origin+'/')?route.continue():route.abort());
      let latest;
      try{
        await bounded((async()=>{
          await page.goto(origin+'/game/neon-arena/');await page.waitForFunction(()=>!!window.__FRONTLINE_TEST__,null,{timeout:15000});
          await page.evaluate(map=>{
            if(!NeonWarMaps.get(map))throw Error('Unknown test map '+map);
            const set=(id,value)=>{const node=document.getElementById(id);node.value=value;node.dispatchEvent(new Event('change',{bubbles:true}));};
            set('mode','conquest');set('map',map);set('secondPlayer','off');set('p1Input','keyboard');set('difficulty','normal');set('visualQuality','low');set('warBudget','8');set('warBase','4');set('warPerFlag','0');
          },map);
          await page.evaluate(()=>__FRONTLINE_TEST__.startArenaMatch());
          const initial=await page.evaluate(()=>__FRONTLINE_TEST__.begin());assert.ok(initial.navPoints>20,map+': real authored navigation was built');
          for(let batch=0;batch<Math.ceil(seconds/3);batch++){
            latest=await page.evaluate(()=>__FRONTLINE_TEST__.step());
            const both=[1,2].every(team=>latest.captures.some(event=>event.team===team));
            if(both||latest.over)break;
          }
          assert.deepEqual(errors,[],map+': no runtime exceptions');
          assert.equal(latest.player.dead,false,map+': observer survives');
          assert.deepEqual({x:latest.player.x,y:latest.player.y,z:latest.player.z},latest.observer,map+': no human movement or shepherding');
          assert.ok(latest.actors.every(a=>Number.isFinite(a.x+a.y+a.z)),map+': real physics positions remain finite');
          for(const team of [1,2]){
            const improvement=initial.initial[team].distance-latest.closest[team];
            assert.ok(improvement>=Math.min(4,initial.initial[team].distance*.2),map+': faction '+team+' must close its initial objective distance; '+JSON.stringify({improvement,initial:initial.initial[team].distance,closest:latest.closest[team]}));
            assert.ok(latest.captures.some(event=>event.team===team),map+': faction '+team+' must acquire a public flag using real movement/capture; '+JSON.stringify(latest));
          }
          report.push(latest);console.log('PASS '+map+': both factions advanced and captured territory in '+latest.elapsed.toFixed(1)+' simulated seconds');
        })(),timeoutMs,map+' strategic movement');
      }catch(error){
        fs.writeFileSync(path.join(out,map+'-failure.json'),JSON.stringify({message:error.message,errors,latest},null,2));throw error;
      }finally{await context.close();}
    }
    fs.writeFileSync(path.join(out,'results.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({passed:report.length,report:path.join(out,'results.json')}));
  }finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
}
run().catch(error=>{console.error(error);process.exitCode=1;});
