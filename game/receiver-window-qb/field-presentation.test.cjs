'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const THREE=require('./vendor/three.min.js'),F=require('./field-presentation.js');

function canvasFactory(){
 const canvases=[];
 const factory=()=>{
  const canvas={width:0,height:0},context={digest:2166136261,text:[],calls:0};
  for(const method of ['fillRect','clearRect','beginPath','moveTo','lineTo','stroke','strokeText','fillText'])context[method]=function(...args){
   const string=[method,...args,this.fillStyle,this.strokeStyle,this.lineWidth,this.font].join('|');
   for(let i=0;i<string.length;i++)this.digest=Math.imul(this.digest^string.charCodeAt(i),16777619)>>>0;
   this.calls++;if(method==='fillText')this.text.push(args[0]);
  };
  canvas.context=context;canvas.getContext=kind=>kind==='2d'?context:null;canvases.push(canvas);return canvas;
 };
 factory.canvases=canvases;return factory;
}
function fixture(options={}){
 const scene=new THREE.Scene(),turf=new THREE.Mesh(new THREE.PlaneGeometry(52,130),new THREE.MeshStandardMaterial({color:0x225522})),endzone=new THREE.Mesh(new THREE.PlaneGeometry(52,10),new THREE.MeshStandardMaterial({color:0x123456}));
 const originalTurf=turf.material,originalEndzone=endzone.material,factory=canvasFactory();scene.add(turf,endzone);
 const field=F.create({THREE,scene,turf,endzone,canvasFactory:factory,...options});
 return {scene,turf,endzone,originalTurf,originalEndzone,factory,field,close(){field.dispose();turf.geometry.dispose();endzone.geometry.dispose();originalTurf.dispose();originalEndzone.dispose();}};
}
function centers(mesh){const matrix=new THREE.Matrix4(),v=new THREE.Vector3(),result=[];for(let i=0;i<mesh.count;i++){mesh.getMatrixAt(i,matrix);result.push(v.setFromMatrixPosition(matrix).clone());}return result;}

test('field paint keeps existing five-yard lines and adds bounded one-yard detail',()=>{
 const f=fixture();try{
  const {field}=f;assert.deepEqual(field.world,F.defaults);assert.equal(field.stats.interiorHashes,80);assert.equal(field.stats.sidelineTicks,80);
  assert.equal(field.stats.totalDraws,11);assert.ok(field.stats.totalInstances<220);assert.equal(field.stats.textures,3);
  const hashes=field.group.getObjectByName('One-yard interior hash marks');
  for(const point of centers(hashes)){const yard=(point.z+60)/2;assert.equal(yard,Math.round(yard));assert.notEqual(yard%5,0);assert.ok(point.z>-60&&point.z<40);assert.ok(Math.abs(point.x)<=9);}
  const apron=field.group.getObjectByName('Broad painted boundary apron'),matrix=new THREE.Matrix4(),center=new THREE.Vector3(),size=new THREE.Vector3(),rotation=new THREE.Quaternion();
  apron.getMatrixAt(0,matrix);matrix.decompose(center,rotation,size);assert.equal(center.z,-15);assert.equal(size.z,110);assert.equal(center.z-size.z/2,-70);assert.equal(center.z+size.z/2,40);
  const numbers=field.group.getObjectByName('Crisp sideline yard numbers');assert.equal(numbers.geometry.getAttribute('position').count,40);assert.equal(numbers.geometry.index.count,60);assert.ok(numbers.geometry.boundingBox.max.z<40);
  field.group.traverse(o=>{if(o.isMesh){o.geometry.computeBoundingBox();for(const value of [...o.geometry.boundingBox.min.toArray(),...o.geometry.boundingBox.max.toArray()])assert.ok(Number.isFinite(value),o.name);}});
 }finally{f.close();}assert.equal(F.sharedTextureCount,0);
});

test('low quality retains the goal post and readable essentials with eight draws',()=>{
 const f=fixture();try{
  const high=f.field.stats;assert.equal(high.draws,11);
  f.field.quality('medium');assert.equal(f.field.stats.draws,10);
  f.field.quality('low');assert.equal(f.field.stats.draws,8);assert.ok(f.field.group.getObjectByName('Goal post').visible);assert.ok(f.field.group.getObjectByName('Goal-post tubes').isInstancedMesh);
  const allocation=[high.geometries,high.materials,high.textures];
  for(let i=0;i<100;i++){f.field.quality(i%3);f.field.theme({primary:i%2?'#134d7b':'#7b2c13',secondary:'#e9e7dd',wet:i%2===0});}
  assert.deepEqual([f.field.stats.geometries,f.field.stats.materials,f.field.stats.textures],allocation);assert.equal(F.sharedTextureCount,2);
 }finally{f.close();}
});

test('theme updates tint and wet roughness without touching external materials',()=>{
 const f=fixture();try{
  assert.notEqual(f.turf.material,f.originalTurf);assert.notEqual(f.endzone.material,f.originalEndzone);
  const map=f.turf.material.map;assert.ok(map.isCanvasTexture);assert.equal(map.image.width,512);assert.equal(map.image.height,1024);assert.equal(map.anisotropy,1);
  f.field.theme({jersey:'#a32425',accent:'#fff9ee',fieldColor:'#427b44',name:'River\nCity',wet:true});
  assert.equal(f.endzone.material.color.getHexString(),'a32425');assert.equal(f.turf.material.roughness,.79);assert.equal(f.originalTurf.color.getHexString(),'225522');assert.equal(f.originalEndzone.color.getHexString(),'123456');
  const word=f.field.group.getObjectByName('End-zone identity').material.map,version=word.version;
  assert.equal(word.image.context.text.at(-1),'RIVER CITY');f.field.theme({name:'River City'});assert.equal(word.version,version);
  f.field.theme({wet:false});assert.equal(f.turf.material.roughness,.99);assert.equal(f.turf.material.color.getHexString(),'427b44');assert.equal(f.turf.material.map,map);
  f.field.theme({primary:'not a color',secondary:{},name:5});assert.equal(f.endzone.material.color.getHexString(),'a32425');
  f.field.theme({primary:NaN,secondary:Infinity,turfColor:-3});assert.equal(f.endzone.material.color.getHexString(),'a32425');assert.equal(f.turf.material.color.getHexString(),'427b44');
 }finally{f.close();}
});

test('turf and number atlases share ownership while end-zone identity stays independent',()=>{
 const a=fixture(),b=fixture();
 try{
  assert.equal(F.sharedTextureCount,2);assert.equal(a.turf.material.map,b.turf.material.map);
  assert.equal(a.field.group.getObjectByName('Crisp sideline yard numbers').material.map,b.field.group.getObjectByName('Crisp sideline yard numbers').material.map);
  assert.notEqual(a.field.group.getObjectByName('End-zone identity').material.map,b.field.group.getObjectByName('End-zone identity').material.map);
  assert.equal(a.factory.canvases.length,3);assert.equal(b.factory.canvases.length,1);
  let released=0;const shared=a.turf.material.map;shared.addEventListener('dispose',()=>released++);
  a.field.dispose();assert.equal(released,0);assert.equal(F.sharedTextureCount,2);assert.equal(b.turf.material.map,shared);
  b.field.dispose();assert.equal(released,1);assert.equal(F.sharedTextureCount,0);
 }finally{a.close();b.close();}
});

test('seeded turf is repeatable across complete teardown and recreation',()=>{
 const first=fixture(),digest=first.turf.material.map.image.context.digest,calls=first.turf.material.map.image.context.calls;first.close();
 const second=fixture();try{assert.equal(second.turf.material.map.image.context.digest,digest);assert.equal(second.turf.material.map.image.context.calls,calls);assert.ok(calls>34000);}finally{second.close();}
});

test('dispose restores caller materials and releases every owned resource exactly once',()=>{
 const f=fixture(),resources=new Map();const track=resource=>{if(!resource||resources.has(resource))return;resources.set(resource,0);resource.addEventListener('dispose',()=>resources.set(resource,resources.get(resource)+1));};
 f.field.group.traverse(o=>{if(o.isMesh){if(o.isInstancedMesh)track(o);track(o.geometry);track(o.material);track(o.material.map);}});track(f.turf.material);track(f.turf.material.map);track(f.endzone.material);
 let originalDisposals=0;f.originalTurf.addEventListener('dispose',()=>originalDisposals++);f.originalEndzone.addEventListener('dispose',()=>originalDisposals++);
 f.field.dispose();f.field.dispose();assert.equal(f.turf.material,f.originalTurf);assert.equal(f.endzone.material,f.originalEndzone);assert.equal(originalDisposals,0);assert.ok(!f.scene.children.includes(f.field.group));assert.equal(f.field.stats.draws,0);assert.equal(F.sharedTextureCount,0);
 for(const count of resources.values())assert.equal(count,1);assert.equal(f.field.theme({}),false);assert.equal(f.field.quality('high'),false);f.close();
});

test('defensive world normalization prevents unbounded marking allocation',()=>{
 for(const raw of [null,{}, {startZ:-100,goalZ:-50}, {startZ:1e300,goalZ:-1e300,endZ:-1e300,halfWidth:1e300,unitsPerYard:.00000001}]){
  const world=F.normalizeWorld(raw);assert.ok(Object.values(world).every(Number.isFinite));assert.ok(world.endZ<world.goalZ&&world.goalZ<world.startZ);assert.ok(world.unitsPerYard>=.25);assert.ok((world.startZ-world.goalZ)/world.unitsPerYard<=200);
  const f=fixture({world});try{assert.ok(f.field.stats.interiorHashes<=320);assert.ok(f.field.stats.totalInstances<800);}finally{f.close();}
 }
 const f=fixture({world:{startZ:12,goalZ:-28,endZ:-32,halfWidth:14,unitsPerYard:1},canvasFactory:()=>null});try{assert.equal(f.field.stats.textures,0);assert.equal(f.field.stats.interiorHashes,64);assert.ok(!f.field.group.getObjectByName('Crisp sideline yard numbers'));assert.ok(f.field.group.getObjectByName('Goal-post tubes'));}finally{f.close();}
});

test('browser and CommonJS expose the same static API without adding render loops',()=>{
 const context={};vm.runInNewContext(fs.readFileSync(require.resolve('./field-presentation.js'),'utf8'),context);
 assert.equal(typeof context.QBFieldGraphics.create,'function');assert.equal(typeof context.QBFieldGraphics.normalizeWorld,'function');assert.deepEqual(JSON.parse(JSON.stringify(context.QBFieldGraphics.defaults)),F.defaults);
 assert.throws(()=>F.create({}),/requires THREE/);assert.equal(F.sharedTextureCount,0);
 const f=fixture({world:null});f.close();assert.equal(F.sharedTextureCount,0);
});
