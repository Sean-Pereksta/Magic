'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),http=require('node:http'),{chromium}=require('playwright');
const repo=path.resolve(__dirname,'..'),fixtures=process.env.NEON_ASSET_FIXTURES,output=process.env.NEON_QA_OUTPUT;
const hooks=`window.__NEON_ALLY__={game,player,get player2(){return player2},get bots(){return bots},get nav(){return nav},get renderer(){return neonModelRenderer},get viewer(){return viewEnt},draw,updateHud,equip,lineClear,allyHighlightForView};`;
const server=http.createServer((req,res)=>{try{const url=new URL(req.url,'http://localhost').pathname,isFixture=url.startsWith('/fixture-assets/'),base=isFixture?fixtures:repo,file=path.resolve(base,'.'+(isFixture?url.slice('/fixture-assets'.length):url.endsWith('/')?url+'index.html':url));if(!file.startsWith(path.resolve(base)+path.sep))throw Error('outside');let data=fs.readFileSync(file);if(path.basename(file)==='neon-arena.js')data=Buffer.from(data.toString().replace(/\}\)\(\);\s*$/,hooks+'})();'));res.writeHead(200,{'Content-Type':({'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.json':'application/json','.css':'text/css','.webp':'image/webp','.png':'image/png','.svg':'image/svg+xml','.glb':'model/gltf-binary'})[path.extname(file)]||'application/octet-stream','Access-Control-Allow-Origin':'*'});res.end(data)}catch{res.writeHead(404);res.end('not found')}});
async function main(){
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  try{
    const shaderPage=await browser.newPage();await shaderPage.setContent('<canvas id="test" width="256" height="256"></canvas>');await shaderPage.addScriptTag({url:origin+'/game/neon-arena/neon-models.js'});
    const shader=await shaderPage.evaluate(()=>{
      const canvas=document.querySelector('#test'),gl=canvas.getContext('webgl',{antialias:false,preserveDrawingBuffer:true});if(!gl)throw Error('WebGL unavailable');
      const plane=(w,h,color)=>{const positions=[[-w/2,0,0],[w/2,0,0],[w/2,h,0],[-w/2,0,0],[w/2,h,0],[-w/2,h,0]],vertices=new Float32Array(positions.flatMap(p=>[...p,0,0,-1,...color,0,0,0]));return {path:'fixture',height:h,bodyHeight:h,bounds:{min:[-w/2,0,0],max:[w/2,h,0]},parts:[{key:'body',pivot:[0,0,0],vertices}],vertices,authorScale:1};};
      const human=plane(1.4,2.39,[.3,.3,.3]),wall=plane(8,6,[.65,.12,.12]),renderer=NeonModels.createRenderer(gl,{cache:{get:p=>p==='wall'?wall:human,status:()=>({})}});
      const actor={x:0,y:1.86,z:1,hp:100,armor:0,onGround:true,vx:0,vz:0,yaw:0},camera={position:[0,1.2,-5],right:[1,0,0],up:[0,1,0],forward:[0,0,1],aspect:1};
      const render=(highlight,occluded=false,includeActor=true)=>{gl.viewport(0,0,256,256);gl.clearColor(.02,.03,.05,1);gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);renderer.beginView(camera,1);if(occluded)renderer.drawActor('wall',{...actor,z:0},{height:6,animate:false});if(includeActor)renderer.drawActor('human',actor,{allyHighlight:highlight,animate:false});const pixels=new Uint8Array(256*256*4);gl.readPixels(0,0,256,256,gl.RGBA,gl.UNSIGNED_BYTE,pixels);return pixels;};
      const neutral=render(false),highlighted=render(true),wallOnly=render(false,true,false),behindWall=render(true,true),center=(128*256+128)*4;
      let wallDifferences=0;for(let i=0;i<wallOnly.length;i++)if(wallOnly[i]!==behindWall[i])wallDifferences++;
      const result={neutral:[...neutral.slice(center,center+3)],highlighted:[...highlighted.slice(center,center+3)],wallDifferences,error:gl.getError()};renderer.dispose();return result;
    });
    assert.equal(shader.error,0);assert.ok(shader.highlighted[1]>shader.neutral[1]+20&&shader.highlighted[2]>shader.neutral[2]+20,'cyan material visibly distinguishes a friend');assert.equal(shader.wallDifferences,0,'highlighted actor contributes no pixels behind a solid wall');await shaderPage.close();
    const page=await browser.newPage({viewport:{width:1440,height:900}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.addInitScript(base=>{window.NEON_ASSET_CONFIG={ASSET_BASE_URL:base,UI_ASSET_BASE_URL:'./assets',CHARACTER_ASSET_VERSION:'',version:''};HTMLCanvasElement.prototype.requestPointerLock=function(){};},origin+'/fixture-assets');
    await page.goto(origin+'/game/neon-arena/');await page.waitForFunction(()=>!!window.__NEON_ALLY__);await page.locator('[data-mode="tdm"]').click();
    await page.evaluate(()=>{document.querySelector('#map').value='grid';document.querySelector('#secondPlayer').value='keyboard';document.querySelector('#botCount').value='6'});await page.locator('#start').click();
    await page.waitForFunction(()=>__NEON_ALLY__.game.started&&!__NEON_ALLY__.game.over||document.querySelector('#mapDescription').textContent.startsWith('ASSET LOAD FAILED'),null,{timeout:30000});
    const failure=await page.locator('#mapDescription').innerText();assert.ok(!failure.startsWith('ASSET LOAD FAILED'),failure);
    const report=await page.evaluate(()=>{
      const t=__NEON_ALLY__;t.game.over=true;const actors=t.bots.filter(b=>!b.human),ally=actors[0],enemy=actors[1];if(!ally||!enemy||!t.player2)throw Error('Local TDM actors missing');
      const ground=t.nav.filter(n=>Math.abs(n.y-1.86)<.1),clear=(from,to)=>t.lineClear(from.x,from.y-.05,from.z,to.x,to.y-.6,to.z);let scene=null;
      for(const camera of ground){const visible=ground.filter(n=>{const d=Math.hypot(n.x-camera.x,n.z-camera.z);return d>7&&d<15&&clear(camera,n)});for(let i=0;i<visible.length&&!scene;i++)for(let j=i+1;j<visible.length;j++){const a=visible[i],b=visible[j],aa=Math.atan2(a.x-camera.x,a.z-camera.z),bb=Math.atan2(b.x-camera.x,b.z-camera.z),gap=Math.abs(Math.atan2(Math.sin(aa-bb),Math.cos(aa-bb)));if(gap>.48&&gap<.9){scene={camera,a,b,yaw:Math.atan2(Math.sin(aa)+Math.sin(bb),Math.cos(aa)+Math.cos(bb))};break;}}if(scene)break;}
      if(!scene)throw Error('No safe screenshot engagement lane');
      for(const b of t.bots)b.dead=true;
      const place=(actor,node,team)=>Object.assign(actor,{x:node.x,y:node.y,z:node.z,team,dead:false,vx:0,vy:0,vz:0,onGround:true,faceYaw:scene.yaw+Math.PI,yaw:scene.yaw+Math.PI,hp:100,armor:50,spawnGrace:0,slidePose:0});
      place(t.player,scene.camera,1);t.player.yaw=scene.yaw;t.player.pitch=.04;place(t.player2,scene.camera,2);t.player2.yaw=scene.yaw;t.player2.pitch=.04;
      place(ally,scene.a,1);place(enemy,scene.b,2);t.equip(t.player,'assault',false);t.equip(ally,'assault',false);t.equip(enemy,'assault',false);
      const calls=[],original=t.renderer.drawActor.bind(t.renderer);t.renderer.drawActor=(path,actor,options)=>{calls.push({viewer:t.viewer.entityId,actor:actor.entityId,highlight:!!options?.allyHighlight});return original(path,actor,options)};
      t.game.secondPlayer=true;t.updateHud();t.draw();
      const relative={p1Ally:calls.some(c=>c.viewer==='p1'&&c.actor===ally.entityId&&c.highlight),p1Enemy:calls.some(c=>c.viewer==='p1'&&c.actor===enemy.entityId&&c.highlight),p2Ally:calls.some(c=>c.viewer==='p2'&&c.actor===enemy.entityId&&c.highlight),p2Enemy:calls.some(c=>c.viewer==='p2'&&c.actor===ally.entityId&&c.highlight)};
      t.game.secondPlayer=false;t.player2.dead=true;t.game.over=false;window.requestAnimationFrame=()=>0;t.updateHud();t.draw();return {relative,scene,gl:document.querySelector('canvas').getContext('webgl')?.getError?.()||0};
    });
    assert.deepEqual(report.relative,{p1Ally:true,p1Enemy:false,p2Ally:true,p2Enemy:false},'each local view highlights its own team');assert.deepEqual(errors,[]);assert.equal(report.gl,0);
    if(output){fs.mkdirSync(output,{recursive:true});await page.screenshot({path:path.join(output,'tdm-allies.png')});}
    console.log(JSON.stringify({shader,splitView:report.relative,screenshot:output?path.join(output,'tdm-allies.png'):null},null,2));
  }finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
}
main().catch(error=>{console.error(error);process.exitCode=1;server.close()});
