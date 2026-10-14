// Real Chromium/WebGL integration with deterministic simulation steps. This is an
// input/rendering regression, not a frame-rate benchmark or real-device test.
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),http=require('node:http'),path=require('node:path');
const {chromium}=require('playwright');
const repo=path.resolve(__dirname,'..'),fixtures=process.env.NEON_ASSET_FIXTURES;
const hooks=`window.__SLIDE_TEST__={game,player,get player2(){return player2},mobileInput,controller,reset,updatePlayer,physics,draw,spawn,requestHumanSlide,requestHumanJump,actorEyePosition,resetHumanJump,readPlayerPad,updateMobileControlVisibility,updateHumanMovement,buildMap,topAtPoint,get ramps(){return arenaRamps},get map(){return survivalMapRuntime},get gl(){return gl},get models(){return neonModelRenderer}};`;
function serve(req,res){try{
  const url=new URL(req.url,'http://localhost'),remote=fixtures&&url.pathname.startsWith('/assets/'),base=remote?path.resolve(fixtures):repo;
  const file=path.resolve(base,'.'+(remote?url.pathname.slice(7):url.pathname.endsWith('/')?url.pathname+'index.html':url.pathname));
  if(!file.startsWith(base+path.sep))throw Error('Outside test root');let data=fs.readFileSync(file);
  if(path.basename(file)==='neon-arena.js')data=Buffer.from(data.toString().replace(/\}\)\(\);\s*$/,hooks+'})();'));
  res.writeHead(200,{'Content-Type':({'.js':'text/javascript','.mjs':'text/javascript','.json':'application/json','.html':'text/html','.css':'text/css','.webp':'image/webp','.png':'image/png','.glb':'model/gltf-binary'})[path.extname(file)]||'application/octet-stream','Access-Control-Allow-Origin':'*'});res.end(data);
}catch{res.writeHead(404);res.end('Not found')}}
async function main(){
  const server=http.createServer(serve);await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
  const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  try{for(const touch of [false,true]){
    const context=await browser.newContext({viewport:touch?{width:390,height:844}:{width:1280,height:800},hasTouch:touch,isMobile:touch}),page=await context.newPage(),errors=[];
    page.on('pageerror',e=>errors.push(e.message));
    await page.addInitScript(base=>{window.NEON_ASSET_CONFIG={ASSET_BASE_URL:base,UI_ASSET_BASE_URL:'./assets',version:''};window.requestAnimationFrame=()=>0;HTMLCanvasElement.prototype.requestPointerLock=function(){}},fixtures?origin+'/assets':'');
    await page.goto(origin+'/game/neon-arena/');await page.waitForFunction(()=>!!window.__SLIDE_TEST__);
    await page.evaluate(async touch=>{const t=__SLIDE_TEST__;await t.map.load('breach_ring',{mobile:touch});document.getElementById('botCount').value='0';document.getElementById('secondPlayer').value='keyboard';document.getElementById('p1Input').value=touch?'touch':'keyboard';t.reset();t.updateMobileControlVisibility(true);document.getElementById('menu').style.display='none';const p=t.player;p.x=0;p.z=-53;p.y=1.86;p.yaw=0;p.pitch=0;p.vx=p.vy=p.vz=0;p.onGround=true;t.resetHumanJump(p);t.player2.x=8;t.player2.z=-53;t.player2.y=1.86;t.player2.onGround=true;t.resetHumanJump(t.player2)},touch);
    // Actual browser key/pointer events feed the same shared movement path.
    if(touch){await page.evaluate(()=>{__SLIDE_TEST__.mobileInput.moveForward=1;__SLIDE_TEST__.updatePlayer(1/60)});const b=await page.locator('#mobileSlide').boundingBox();assert(b&&b.x>=0&&b.y>=0&&b.x+b.width<=390&&b.y+b.height<=844,'slide fits the portrait viewport');await page.locator('#mobileSlide').tap()}
    else{await page.keyboard.down('w');await page.evaluate(()=>{for(let i=0;i<6;i++)__SLIDE_TEST__.updatePlayer(1/60)});await page.keyboard.down('Shift')}
    const slide=await page.evaluate(()=>{const t=__SLIDE_TEST__;for(let i=0;i<6;i++)t.updatePlayer(1/60);t.draw();return {timer:t.player.slideTimer,pose:t.player.slidePose,eye:t.actorEyePosition(t.player).y,y:t.player.y,p2:t.player2.slideTimer,error:t.gl.getError()}});
    assert(slide.timer>.5&&slide.pose>.7,JSON.stringify({touch,...slide}));assert(slide.eye<slide.y-.5);assert.equal(slide.p2,0);assert.equal(slide.error,0);
    if(!touch){await page.keyboard.down('Space');await page.evaluate(()=>__SLIDE_TEST__.updatePlayer(1/60));assert.equal(await page.evaluate(()=>__SLIDE_TEST__.player.slideTimer),0);assert((await page.evaluate(()=>__SLIDE_TEST__.player.vy))>22);await page.keyboard.up('Space');await page.keyboard.up('Shift');await page.keyboard.up('w')}
    // Two standard gamepad objects maintain independent button-edge history.
    const pads=await page.evaluate(()=>{const t=__SLIDE_TEST__,buttons=Array.from({length:16},(_,i)=>({pressed:i===10,value:i===10?1:0})),pad={index:0,axes:[0,-1,0,0],buttons};for(const p of [t.player,t.player2]){p.dead=false;p.onGround=true;p.vx=0;p.vz=18.4;t.resetHumanJump(p)}t.readPlayerPad(1,t.player,pad);t.updatePlayer(1/60);const first=t.player.slideTimer;t.readPlayerPad(1,t.player,pad);const repeated=t.player.slideRequest;t.readPlayerPad(2,t.player2,{...pad,index:1});return {first,repeated,p2Request:t.player2.slideRequest,p1Prev:t.controller.players[1].prev.slide,p2Prev:t.controller.players[2].prev.slide}});
    assert(pads.first>0);assert.equal(pads.repeated,0);assert(pads.p2Request>0);assert(pads.p1Prev&&pads.p2Prev);
    assert.deepEqual(errors,[]);console.log(JSON.stringify({device:touch?'mobile portrait':'desktop split-screen',slide:'tap, momentum, lowered view, independent pad edges',webgl:'real',errors:0}));await context.close();
  }
    const rampReports=[],page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.addInitScript(base=>{window.NEON_ASSET_CONFIG={ASSET_BASE_URL:base,UI_ASSET_BASE_URL:'./assets',version:''};window.requestAnimationFrame=()=>0;HTMLCanvasElement.prototype.requestPointerLock=function(){}},fixtures?origin+'/assets':'');
    await page.goto(origin+'/game/neon-arena/');await page.waitForFunction(()=>!!window.__SLIDE_TEST__);
    const rampCases=['grid','pastel','reactor','terminal','foundry','outpost','breach_ring','derelict_station','xenofall_wastes','black_ice_cryolab','living_hive','drowned_powerworks','orbital_graveyard','swarm_heart'].map(id=>({id,mode:'dm'})).concat(['terminal','foundry','outpost'].map(id=>({id,mode:'frag'})));
    for(const {id,mode} of rampCases){
      const result=await page.evaluate(async({id,mode})=>{
        const t=__SLIDE_TEST__,standard=['grid','pastel','reactor','terminal','foundry','outpost'].includes(id);document.getElementById('secondPlayer').value='keyboard';document.getElementById('botCount').value='0';document.getElementById('mode').value=mode;document.getElementById('map').value=standard?id:'grid';await t.map.load(standard?({grid:'breach_ring',pastel:'black_ice_cryolab',reactor:'drowned_powerworks',terminal:'derelict_station',foundry:'drowned_powerworks',outpost:'xenofall_wastes'})[id]:'breach_ring',{mobile:true});t.reset();
        if(!standard){await t.map.load(id,{mobile:true});t.game.mode='survivor';t.game.survivalMap=id;t.game.survivalElapsed=0;t.game.shopOpen=false;t.buildMap()}
        const ramps=standard?t.ramps:t.map.current.ramps,failures=[];let cases=0;
        for(let ri=0;ri<ramps.length;ri++)for(const fps of [60,30,10])for(const [slot,actor]of [[1,t.player],[2,t.player2]])for(const lane of [-.30,0,.30])for(const uphill of [true,false]){
          const r=ramps[ri],axis=r.axis,cross=axis==='x'?'z':'x',length=(axis==='x'?r.w:r.d)*1.45,width=(axis==='x'?r.d:r.w)*1.45,ascending=r.toHeight>r.fromHeight?1:-1,low=r[axis]*1.45-ascending*length/2,high=r[axis]*1.45+ascending*length/2,direction=ascending*(uphill?1:-1),start=(uphill?low:high)-direction*.3,end=(uphill?high:low)+direction*.3,startHeight=uphill?Math.min(r.fromHeight,r.toHeight):Math.max(r.fromHeight,r.toHeight),targetHeight=uphill?Math.max(r.fromHeight,r.toHeight):Math.min(r.fromHeight,r.toHeight);
          actor.x=axis==='x'?start:r.x*1.45+lane*width;actor.z=axis==='z'?start:r.z*1.45+lane*width;actor.y=startHeight+1.86;actor.vx=actor.vy=actor.vz=0;actor.onGround=true;actor.dead=false;actor.controllerFireHeld=false;actor.weapon='pistol';actor.yaw=axis==='x'?direction*Math.PI/2:direction===1?0:Math.PI;actor.pitch=0;t.resetHumanJump(actor);
          let maxError=0,maxLateral=0,steps=0,airSteps=0;const maxSteps=Math.ceil((length+3)/18.4*fps*3),dt=1/fps;
          while((end-actor[axis])*direction>.02&&steps++<maxSteps){t.updateHumanMovement(actor,dt,0,1);const fraction=(actor[axis]-r[axis]*1.45)/length+.5;if(fraction>=0&&fraction<=1){const expected=r.fromHeight+(r.toHeight-r.fromHeight)*fraction;maxError=Math.max(maxError,Math.abs(actor.y-1.86-expected));if(!actor.onGround)airSteps++;}maxLateral=Math.max(maxLateral,Math.abs(actor[cross]-(r[cross]*1.45+lane*width)));}
          cases++;const remaining=(end-actor[axis])*direction,finalError=Math.abs(actor.y-1.86-targetHeight);if(remaining>.15||maxError>.23||maxLateral>.38||finalError>.22||airSteps>2)failures.push({ramp:ri,fps,slot,lane,uphill,remaining,maxError,maxLateral,finalError,airSteps,feet:actor.y-1.86,position:actor[axis],end});
        }
        t.draw();return {map:id,mode:t.game.mode,ramps:ramps.length,cases,failures,gl:t.gl.getError()};
      },{id,mode});
      rampReports.push(result);assert.equal(result.gl,0);console.log(JSON.stringify({...result,failures:result.failures.length,examples:result.failures.slice(0,2),walking:'up/down; 60/30/10 FPS; P1/P2; center/left/right; no jump'}));
    }
    if(process.env.NEON_RAMP_REPORT)fs.writeFileSync(process.env.NEON_RAMP_REPORT,JSON.stringify(rampReports,null,2));assert.equal(rampReports.reduce((n,r)=>n+r.failures.length,0),0,'all authored ramps must be walkable; see map reports');assert.deepEqual(errors,[],'ramp runtime errors');await page.close();
  }finally{await browser.close();await new Promise(r=>server.close(r))}
}
main().catch(e=>{console.error(e);process.exitCode=1});
