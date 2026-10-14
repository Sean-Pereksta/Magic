'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const models=require('../game/neon-arena/neon-projectile-models.js');
const arena=fs.readFileSync(path.resolve(__dirname,'../game/neon-arena/neon-arena.js'),'utf8');
function glMock(){
  let current=null;const state={draws:[],ranges:[],enabled:[],uniforms:{},shaders:[],subUploads:[]};
  const gl={VERTEX_SHADER:1,FRAGMENT_SHADER:2,COMPILE_STATUS:3,LINK_STATUS:4,ARRAY_BUFFER:5,STATIC_DRAW:6,DYNAMIC_DRAW:7,TRIANGLES:8,FLOAT:9,DEPTH_TEST:10,LEQUAL:11,BLEND:12,CULL_FACE:13,
    createProgram:()=>({}),createShader:type=>({type}),shaderSource:(shader,source)=>state.shaders.push({type:shader.type,source}),compileShader(){},getShaderParameter:()=>true,getShaderInfoLog:()=>'',attachShader(){},linkProgram(){},getProgramParameter:()=>true,deleteShader(){},
    getAttribLocation:(_,name)=>['aPosition','aNormal','aColor','aEmissive'].indexOf(name),getUniformLocation:(_,name)=>name,createBuffer:()=>({}),bindBuffer:(_,buffer)=>{current=buffer},
    bufferData:(_,data)=>{current.capacity=typeof data==='number'?data:data.byteLength},bufferSubData:(_,offset,data)=>{assert.ok(offset+data.byteLength<=current.capacity);assert.ok([...data].every(Number.isFinite));state.subUploads.push(data.length)},
    useProgram(){},enable:value=>state.enabled.push(value),disable:value=>assert.notEqual(value,gl.DEPTH_TEST),depthFunc:value=>assert.equal(value,gl.LEQUAL),depthMask:value=>assert.equal(value,true),depthRange:(a,b)=>state.ranges.push([a,b]),
    uniform3fv:(name,value)=>{assert.ok([...value].every(Number.isFinite));state.uniforms[name]=[...value]},uniform1f:(name,value)=>{assert.ok(Number.isFinite(value));state.uniforms[name]=value},uniformMatrix4fv:(name,transpose,value)=>{assert.equal(transpose,false);assert.ok([...value].every(Number.isFinite));state.uniforms[name]=[...value]},
    enableVertexAttribArray(){},vertexAttribPointer:(_,size,type,normalized,stride)=>{assert.equal(size,3);assert.equal(type,gl.FLOAT);assert.equal(normalized,false);assert.equal(stride,48)},
    drawArrays:(mode,start,count)=>{assert.equal(mode,gl.TRIANGLES);assert.equal(start,0);assert.ok(count*48<=current.capacity);state.draws.push({count,uniforms:structuredClone(state.uniforms)})},deleteBuffer(){},deleteProgram(){} };
  return {gl,state};
}
const camera={position:[0,1,-5],right:[1,0,0],up:[0,1,0],forward:[0,0,1],aspect:.75};
test('the fist is a closed hand with four separate curled fingers, four knuckles, a wrapped thumb and cuff',()=>{
  const fist=models.MESHES.fist,names=fist.parts.map(p=>p.name);
  assert.equal(names.filter(name=>name.startsWith('curled_finger_')).length,4);assert.equal(names.filter(name=>name.startsWith('knuckle_plate_')).length,4);
  for(const name of ['palm_shell','wrapped_thumb','thumb_armor','wrist','cuff_steel'])assert.ok(names.includes(name));
  assert.ok(fist.vertexCount>2500&&fist.vertexCount<6000,'authored curved surfaces stay within a practical browser budget');
  const thumb=fist.parts.find(p=>p.name==='wrapped_thumb'),xs=[];for(let i=thumb.start*12;i<(thumb.start+thumb.count)*12;i+=12)xs.push(fist.vertices[i]);
  assert.ok(Math.max(...xs)>.65&&Math.min(...xs)<.25,'thumb bends across the curled fingers');
});
test('harpoon geometry has a sharp forward tip, backward barbs, collars and a long metal shaft',()=>{
  const mesh=models.MESHES.harpoon,names=mesh.parts.map(p=>p.name),zs=[];for(let i=2;i<mesh.vertices.length;i+=12)zs.push(mesh.vertices[i]);
  for(const name of ['spearhead','backward_barb_-1','backward_barb_1','shaft','anchor_power_ring'])assert.ok(names.includes(name));
  assert.ok(Math.abs(Math.max(...zs))<1e-6,'the actual projectile position is the spear tip');assert.ok(Math.min(...zs)<-1.4);
});
test('every authored surface has finite geometry and usable unit normals',()=>{
  for(const [name,mesh] of Object.entries(models.MESHES)){
    assert.equal(mesh.vertices.length%36,0,name);assert.ok([...mesh.vertices].every(Number.isFinite),name);
    for(let i=0;i<mesh.vertices.length;i+=12)assert.ok(Math.abs(Math.hypot(mesh.vertices[i+3],mesh.vertices[i+4],mesh.vertices[i+5])-1)<1e-5,name+' normal');
  }
});
test('projectile transforms follow actual flight and return directions and place the tip at the collision record',()=>{
  const p={kind:'harpoon',x:3,y:2,z:8,dx:1,dy:1,dz:0},matrix=models.projectileMatrix(p);
  assert.deepEqual(models.transform(matrix,[0,0,0]),[3,2,8]);
  const front=models.transform(matrix,[0,0,1]);assert.ok(front[0]>3.7&&front[1]>2.7&&Math.abs(front[2]-8)<1e-6);
  const punch={kind:'punch',x:0,y:1,z:10,dx:0,dy:0,dz:1,state:'back'},back=models.projectileMatrix(punch,{x:0,y:1,z:0});
  assert.ok(models.transform(back,[0,0,1])[2]<10,'returning fist faces toward its real owner');
  for(const direction of [[0,1,0],[0,-1,0],[0,0,0]])assert.ok([...models.directionMatrix([0,0,0],direction)].every(Number.isFinite));
});
test('rendering uses normal world depth, bounded batched chains and does not mutate authoritative projectiles',()=>{
  const {gl,state}=glMock(),renderer=models.createRenderer(gl),p={kind:'punch',x:0,y:1,z:18,dx:0,dy:0,dz:1,state:'out',life:1,damage:92,color:'#ff4fd8',owner:{hp:100}},before=structuredClone(p);
  renderer.beginView(camera,{quality:'low'});renderer.drawPunch(p,{muzzle:{x:0,y:1,z:0}});
  assert.deepEqual(p,before);assert.equal(state.draws.length,2,'the entire linked chain is one draw, plus the fist');assert.equal(renderer.stats().maxChainLinks,24);
  assert.ok(state.subUploads[0]<=models.MESHES.link.vertices.length*24);assert.deepEqual(state.ranges,[[0,1]]);assert.ok(state.enabled.includes(gl.DEPTH_TEST));
  renderer.beginView({...camera,position:[4,1,-5]},{quality:'high'});renderer.drawHarpoon({...p,kind:'harpoon'},{muzzle:{x:0,y:1,z:0}});
  assert.deepEqual(state.draws.at(-1).uniforms.uCamera,[4,1,-5],'a second local view receives its own perspective');assert.equal(renderer.stats().staticMeshes,3);
  renderer.dispose();
});
test('the production world pass reads live projectile and actual latch records with no 2D heads',()=>{
  assert.match(arena,/renderProjectileModels\(importedModelCamera\(\)\)/);assert.match(arena,/renderer\.drawPunch\(p,\{muzzle\}\)/);assert.match(arena,/renderer\.drawHarpoon\(p,\{muzzle\}\)/);
  assert.ok(!arena.includes('drawPunchProjectiles2D'));assert.ok(!arena.includes('drawHarpoonProjectiles2D'));
  const latches=arena.slice(arena.indexOf('function latchHarpoonEnemy('),arena.indexOf('function updateHarpoonProjectile('));assert.ok(!latches.includes('shots.push('),'latches use their real3D cable instead of a duplicate2D beam');
  assert.match(arena,/offset:\{x:p\.x-target\.x,y:p\.y-target\.y,z:p\.z-target\.z\}/,'latched head follows its real impact offset');
});
test('punch collision, range turnaround, snapback and harpoon sweep physics remain authoritative',()=>{
  const extract=(from,to)=>arena.slice(arena.indexOf('function '+from+'('),arena.indexOf('function '+to+'(',arena.indexOf('function '+from+'(')));
  const calls=[],context={Math,performance:{now:()=>1000},WEAPONS:{punch:{damage:92,range:21.75,knockback:27,wallDamage:150,spread:.004},harpoon:{damage:58,range:58,spread:.002}},projectiles:[],
    dirFrom:()=>({x:0,y:0,z:1}),actorEyePosition:a=>({x:a.x,y:a.y,z:a.z}),muzzlePoint:a=>({x:a.x,y:a.y,z:a.z}),ultimateActive:()=>false,hasUpgrade:()=>false,addMuzzleFlash(){},targetsFor:()=>[],isHumanActor:()=>true,
    rayWorld:()=>null,rayEntity:()=>null,latchHarpoonWorld:(p,hit)=>calls.push(['world',p.z,hit.dist]),latchHarpoonEnemy:()=>calls.push(['enemy']),burst(){},addFloorDecal(){},damageStructure:()=>calls.push(['wall']),addUltimateImpactVisual(){}};
  vm.createContext(context);vm.runInContext(extract('firePunchGun','punchSeismicShockwave')+extract('updatePunchProjectile','firePrismSplit')+extract('fireHarpoon','friendlyHarpoonStructure')+extract('updateHarpoonProjectile','firePunchGun'),context);
  const owner={x:0,y:1,z:0,hp:100};context.firePunchGun(owner,true);const fist=context.projectiles[0];assert.equal(fist.damage,92);assert.equal(fist.range,21.75);
  context.updatePunchProjectile(fist,.1);assert.ok(Math.abs(fist.z-6.8)<1e-9);fist.traveled=21.7;context.updatePunchProjectile(fist,.01);assert.equal(fist.state,'back');
  const before=fist.z;fist.snapback=true;context.updatePunchProjectile(fist,.01);assert.ok(Math.abs(before-fist.z-1.28)<1e-9);
  context.fireHarpoon(owner,true);const harpoon=context.projectiles.at(-1);context.rayWorld=()=>({dist:1.25});context.updateHarpoonProjectile(harpoon,.1);
  assert.equal(harpoon.z,1.25);assert.equal(harpoon.life,0);assert.deepEqual(calls.at(-1),['world',1.25,1.25]);
});
