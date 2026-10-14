'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),http=require('node:http'),{chromium}=require('playwright');
const repo=path.resolve(__dirname,'..'),fixtures=process.env.NEON_ASSET_FIXTURES,output=process.env.NEON_QA_OUTPUT;
const hooks=`window.__NEON_PROJECTILE__={game,player,get bots(){return bots},get nav(){return nav},get projectiles(){return projectiles},get renderer(){return projectileModelRenderer},get gl(){return gl},draw,updateHud,equip,tryFire,rayWorld,updatePunchProjectile,updateHarpoonProjectile,clearShots(){projectiles=[];shots=[];}};`;
const server=http.createServer((req,res)=>{try{const url=new URL(req.url,'http://localhost').pathname,fixture=url.startsWith('/fixture-assets/'),base=fixture?fixtures:repo,file=path.resolve(base,'.'+(fixture?url.slice('/fixture-assets'.length):url.endsWith('/')?url+'index.html':url));if(!file.startsWith(path.resolve(base)+path.sep))throw Error('outside');let data=fs.readFileSync(file);if(path.basename(file)==='neon-arena.js')data=Buffer.from(data.toString().replace(/\}\)\(\);\s*$/,hooks+'})();'));res.writeHead(200,{'Content-Type':({'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.json':'application/json','.css':'text/css','.webp':'image/webp','.png':'image/png','.svg':'image/svg+xml','.glb':'model/gltf-binary'})[path.extname(file)]||'application/octet-stream','Access-Control-Allow-Origin':'*'});res.end(data)}catch{res.writeHead(404);res.end('not found')}});
async function main(){
  await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
  const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  try{
    if(output)fs.mkdirSync(output,{recursive:true});
    const gallery=await browser.newPage({viewport:{width:1220,height:760}});
    await gallery.setContent('<style>body{margin:0;padding:24px;background:#0b111b;color:#c5d9e5;font:14px system-ui}h1{font-size:22px;margin:0 0 20px}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}figure{margin:0;background:#111c2a;border:1px solid #334458;border-radius:9px;overflow:hidden}canvas{display:block;width:100%;height:280px}figcaption{padding:10px 14px;color:#9fb5c9}</style><h1>Neon Breach · true 3D projectile meshes</h1><div class="grid"></div>');
    await gallery.addScriptTag({url:origin+'/game/neon-arena/neon-models.js'});await gallery.addScriptTag({url:origin+'/game/neon-arena/neon-projectile-models.js'});
    const meshReport=await gallery.evaluate(()=>{
      const normalize=a=>{const n=Math.hypot(...a);return a.map(v=>v/n)},cross=(a,b)=>[a[1]*b[2]-a[2]*b[1],a[2]*b[0]-a[0]*b[2],a[0]*b[1]-a[1]*b[0]];
      const camera=position=>{const forward=normalize([-position[0],1-position[1],-position[2]]),right=normalize(cross([0,1,0],forward)),up=cross(forward,right);return {position,right,up,forward,aspect:280/374}};
      const views=[{fist:'Front · four knuckles',harpoon:'Front · pointed spearhead',p:[0,1,1.9]},{fist:'Side · curled fingers and thumb',harpoon:'Side · barbs and metal shaft',p:[1.9,1,0]},{fist:'Three-quarter · wrist and cuff',harpoon:'Three-quarter · collars and tail fins',p:[-1.4,1.65,-1.7]}],report={};
      for(const kind of ['punch','harpoon']){
        const masks=[];
        for(const view of views){
          const figure=document.createElement('figure'),canvas=document.createElement('canvas'),caption=document.createElement('figcaption');canvas.width=374;canvas.height=280;caption.textContent=(kind==='punch'?'Fist':'Harpoon')+' — '+(kind==='punch'?view.fist:view.harpoon);figure.append(canvas,caption);document.querySelector('.grid').append(figure);
          const gl=canvas.getContext('webgl',{antialias:true,preserveDrawingBuffer:true}),renderer=NeonProjectileModels.createRenderer(gl),p={kind,x:0,y:1,z:0,dx:0,dy:0,dz:1,life:1,state:'out',color:kind==='punch'?'#ff4fd8':'#bdfcff'};
          gl.viewport(0,0,374,280);gl.clearColor(.025,.04,.065,1);gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);renderer.beginView(camera(view.p));(kind==='punch'?renderer.drawPunch:renderer.drawHarpoon)(p);
          const pixels=new Uint8Array(374*280*4);gl.readPixels(0,0,374,280,gl.RGBA,gl.UNSIGNED_BYTE,pixels);const mask=new Uint8Array(374*280);let count=0,minX=374,maxX=0,minY=280,maxY=0;
          for(let i=0;i<mask.length;i++)if(pixels[i*4]+pixels[i*4+1]+pixels[i*4+2]>70){mask[i]=1;count++;const x=i%374,y=Math.floor(i/374);minX=Math.min(minX,x);maxX=Math.max(maxX,x);minY=Math.min(minY,y);maxY=Math.max(maxY,y);}
          masks.push(mask);(report[kind]||=[]).push({pixels:count,width:maxX-minX,height:maxY-minY,error:gl.getError(),triangles:renderer.stats().triangles});
        }
        let difference=0;for(let i=0;i<masks[0].length;i++)if(masks[0][i]!==masks[1][i])difference++;report[kind+'PerspectiveDifference']=difference;
      }
      // A separate full-wall test shares the character/world projection and
      // tests the actual opaque projectile and linked cable GPU passes.
      const hidden=document.createElement('canvas');hidden.width=hidden.height=256;const gl=hidden.getContext('webgl',{antialias:false,preserveDrawingBuffer:true}),pm=NeonProjectileModels.createRenderer(gl);
      const positions=[[-8,0,0],[8,0,0],[8,8,0],[-8,0,0],[8,8,0],[-8,8,0]],vertices=new Float32Array(positions.flatMap(p=>[...p,0,0,-1,.5,.1,.1,0,0,0])),wall={height:8,bodyHeight:8,bounds:{min:[-8,0,0],max:[8,8,0]},vertices,parts:[{key:'body',pivot:[0,0,0],vertices}],authorScale:1},wm=NeonModels.createRenderer(gl,{cache:{get:()=>wall,status:()=>({})}}),cam={position:[0,1,-5],right:[1,0,0],up:[0,1,0],forward:[0,0,1],aspect:1};
      const wallOnly=()=>{gl.viewport(0,0,256,256);gl.clearColor(.025,.04,.065,1);gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);wm.beginView(cam,1);wm.drawActor('wall',{x:0,y:1.86,z:0,onGround:true,hp:100,armor:0},{height:8,animate:false});};
      const read=()=>{const p=new Uint8Array(256*256*4);gl.readPixels(0,0,256,256,gl.RGBA,gl.UNSIGNED_BYTE,p);return p};wallOnly();const baseline=read();wallOnly();pm.beginView(cam);pm.drawPunch({kind:'punch',x:-.5,y:1,z:3,dx:0,dy:0,dz:1,state:'out',life:1},{muzzle:{x:-.5,y:1,z:1.5}});pm.drawHarpoon({kind:'harpoon',x:.5,y:1,z:3,dx:0,dy:0,dz:1,life:1},{muzzle:{x:.5,y:1,z:1.5}});const covered=read();let differences=0;for(let i=0;i<covered.length;i++)if(covered[i]!==baseline[i])differences++;
      report.wallDifferences=differences;report.wallError=gl.getError();
      const returning={kind:'punch',x:0,y:1,z:2,dx:0,dy:0,dz:1,state:'back',life:1},matrix=NeonProjectileModels.projectileMatrix(returning,{x:0,y:1,z:-2});report.returnFacing=NeonProjectileModels.transform(matrix,[0,0,1])[2]<returning.z;
      return report;
    });
    for(const kind of ['punch','harpoon']){assert.ok(meshReport[kind].every(view=>view.pixels>80&&view.error===0),kind+' visible3D surfaces');assert.ok(meshReport[kind+'PerspectiveDifference']>150,kind+' silhouette changes with real camera perspective');}
    assert.equal(meshReport.wallDifferences,0,'wall fully occludes both3D models and chains');assert.equal(meshReport.wallError,0);assert.equal(meshReport.returnFacing,true);
    if(output)await gallery.screenshot({path:path.join(output,'projectile-meshes.png')});await gallery.close();
    const page=await browser.newPage({viewport:{width:1440,height:900}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.addInitScript(base=>{window.NEON_ASSET_CONFIG={ASSET_BASE_URL:base,UI_ASSET_BASE_URL:'./assets',CHARACTER_ASSET_VERSION:'',version:''};HTMLCanvasElement.prototype.requestPointerLock=function(){};},origin+'/fixture-assets');
    await page.goto(origin+'/game/neon-arena/');await page.waitForFunction(()=>!!window.__NEON_PROJECTILE__);await page.locator('[data-mode="dm"]').click();await page.evaluate(()=>{document.querySelector('#map').value='grid';document.querySelector('#secondPlayer').value='off';document.querySelector('#botCount').value='0'});await page.locator('#start').click();
    await page.waitForFunction(()=>__NEON_PROJECTILE__.game.started||document.querySelector('#mapDescription').textContent.startsWith('ASSET LOAD FAILED'),null,{timeout:30000});assert.ok(!(await page.locator('#mapDescription').innerText()).startsWith('ASSET LOAD FAILED'));
    const flight=await page.evaluate(()=>{
      window.requestAnimationFrame=()=>0;const t=__NEON_PROJECTILE__;t.game.over=true;for(const b of t.bots)b.dead=true;const ground=t.nav.filter(n=>Math.abs(n.y-1.86)<.1);let scene=null;
      for(const from of ground){for(const to of ground){const dx=to.x-from.x,dz=to.z-from.z,d=Math.hypot(dx,dz);if(d<14||d>24)continue;const direction={x:dx/d,y:0,z:dz/d};if(!t.rayWorld(from,direction,14,false,.9)){scene={from,direction};break;}}if(scene)break;}
      if(!scene)throw Error('No physical showcase firing lane');Object.assign(t.player,{...scene.from,y:1.86,yaw:Math.atan2(scene.direction.x,scene.direction.z),pitch:0,vx:0,vy:0,vz:0,onGround:true,dead:false,hp:100,armor:50,spawnGrace:0,slidePose:0});
      t.clearShots();t.equip(t.player,'punch',false);t.player.cool=t.player.reload=0;t.tryFire(t.player,true);const p=t.projectiles.at(-1);t.updatePunchProjectile(p,.09);t.game.over=false;t.updateHud();t.draw();
      return {kind:p.kind,state:p.state,z:p.z,renderer:t.renderer.stats(),gl:t.gl.getError(),scene};
    });
    assert.equal(flight.kind,'punch');assert.equal(flight.state,'out');assert.ok(flight.renderer.drawCalls>=2);assert.equal(flight.gl,0);if(output)await page.screenshot({path:path.join(output,'punch-flight.png')});
    const returning=await page.evaluate(()=>{const t=__NEON_PROJECTILE__,p=t.projectiles.find(p=>p.kind==='punch');t.updatePunchProjectile(p,.5);if(p.state!=='back')throw Error('Fist did not turn around');const before={x:p.x,y:p.y,z:p.z};t.updatePunchProjectile(p,.05);t.draw();return {state:p.state,moved:Math.hypot(p.x-before.x,p.y-before.y,p.z-before.z),gl:t.gl.getError()};});assert.equal(returning.state,'back');assert.ok(returning.moved>0);assert.equal(returning.gl,0);if(output)await page.screenshot({path:path.join(output,'punch-return.png')});
    const harpoon=await page.evaluate(()=>{const t=__NEON_PROJECTILE__;t.clearShots();t.equip(t.player,'harpoon',false);t.player.cool=t.player.reload=0;t.tryFire(t.player,true);const p=t.projectiles.at(-1);t.updateHarpoonProjectile(p,.075);t.player.x+=Math.cos(t.player.yaw)*1.3;t.player.z-=Math.sin(t.player.yaw)*1.3;t.draw();return {kind:p.kind,life:p.life,draws:t.renderer.stats().drawCalls,gl:t.gl.getError()};});assert.equal(harpoon.kind,'harpoon');assert.ok(harpoon.life>0&&harpoon.draws>=2);assert.equal(harpoon.gl,0);if(output)await page.screenshot({path:path.join(output,'harpoon-flight.png')});
    const latched=await page.evaluate(()=>{const t=__NEON_PROJECTILE__,p=t.projectiles.find(p=>p.kind==='harpoon');t.updateHarpoonProjectile(p,.9);const a=t.player.harpoonVisualAnchor;t.draw();return {hit:!!a,point:a?Math.hypot(a.x-p.x,a.y-p.y,a.z-p.z):null,draws:t.renderer.stats().drawCalls,gl:t.gl.getError()};});assert.equal(latched.hit,true);assert.equal(latched.point,0);assert.ok(latched.draws>=2);assert.equal(latched.gl,0);if(output)await page.screenshot({path:path.join(output,'harpoon-latched.png')});assert.deepEqual(errors,[]);
    console.log(JSON.stringify({meshes:meshReport,flight,returning,harpoon,latched,errors},null,2));
  }finally{await browser.close();await new Promise(r=>server.close(r));}
}
main().catch(error=>{console.error(error);process.exitCode=1;server.close()});
