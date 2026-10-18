const test=require('node:test'),assert=require('node:assert/strict');
const Assets=require('./player-assets.js');
const pack=()=>({schemaVersion:1,assets:[{id:'speed-wr',path:'models/receivers/speed-wr.glb',lods:{low:'models/receivers/speed-wr-low.glb'}},{id:'safety',path:'models/defenders/safety.glb'}]});
const response=data=>({ok:true,json:async()=>data});
function resource(extra={}){return {disposed:0,dispose(){this.disposed++;},...extra};}
function material(texture){return resource({map:texture,color:{value:'blue'},clone(){const m=material(this.map);m.color.value=this.color.value;return m;}});}
function scene(nodes){return {nodes,traverse(fn){fn(this);nodes.forEach(fn);},removeFromParent(){this.removed=true;}};}
function clip(values=[0,0,0]){return {name:'sprint',tracks:[{values:Float32Array.from(values)}],clone(){return clip(this.tracks[0].values);}};}
function matrix(elements=[1,0,0,1]){return {elements:elements.slice(),clone(){return matrix(this.elements);}};}
function gltf(sharedGeometry){const texture=resource({isTexture:true}),geometry=sharedGeometry||resource(),mat=material(texture),bone={rotation:0},mesh={geometry,material:mat,skeleton:{bones:[bone],boneInverses:[matrix()]},morphTargetInfluences:[0]};return {scene:scene([mesh]),animations:[clip()],mesh,texture,geometry,mat};}
function clone(source){return scene(source.nodes.map(o=>({...o,skeleton:o.skeleton?{bones:o.skeleton.bones.map(b=>({...b})),boneInverses:o.skeleton.boneInverses}:undefined,morphTargetInfluences:o.morphTargetInfluences?.slice()})));}
function loader(options={}){return Assets.create({baseURL:'https://cdn.example/football',fetch:async()=>response(pack()),loadGLTF:async()=>gltf(),clone,...options});}

test('unconfigured preparation performs zero network or clone operations',async()=>{
  let calls=0;const fail=()=>{calls++;throw Error('unexpected dependency use');};
  const assets=Assets.create({fetch:fail,loadGLTF:fail,clone:fail});
  assert.equal(assets.enabled,false);assert.equal(await assets.getManifest(),null);assert.equal(await assets.load('speed-wr'),null);assert.equal(calls,0);
});
test('base URLs are explicit HTTP(S) or relative directories and cannot carry credentials or executable schemes',async()=>{
  for(const baseURL of ['javascript:alert(1)','data:text/plain,test','file:///C:/models','//other.example/models','https://user:pass@cdn.example','https://cdn.example/models?redirect=x','https://cdn.example/#x','models\\elsewhere','models/%2e%2e/private'])assert.throws(()=>Assets.create({baseURL}),baseURL);
  for(const baseURL of ['./assets','../assets','/football','http://localhost:8080/football/']){
    const urls=[],a=loader({baseURL,fetch:async url=>{urls.push(url);return response(pack());},loadGLTF:async url=>{urls.push(url);return gltf();}});
    const instance=await a.load('speed-wr',{lod:'low'});
    assert.deepEqual(urls,[a.baseURL+'manifest.json',a.baseURL+'models/receivers/speed-wr-low.glb']);instance.release();a.dispose();
  }
});
test('manifest paths cannot escape the configured directory, including encoded traversal and alternate origins',async()=>{
  for(const bad of ['../secret.glb','models/../../secret.glb','/secret.glb','https://other.example/p.glb','//other.example/p.glb','models\\secret.glb','models/%2e%2e/p.glb','models/%252e%252e/p.glb','models/a.glb?x=1','models/a.glb#x','models//a.glb']){
    let loads=0;const data=pack();data.assets[0].path=bad;
    const a=loader({fetch:async()=>response(data),loadGLTF:async()=>{loads++;return gltf();}});
    await assert.rejects(a.load('speed-wr'),/path|GLB/);assert.equal(loads,0,bad);assert.equal(a.baseURL,'https://cdn.example/football/');a.dispose();
  }
});
test('bad or missing manifests can be retried without changing configuration or caching invalid data',async()=>{
  let reads=0;const a=loader({fetch:async()=>{reads++;if(reads===1)return {ok:false};if(reads===2)return response({schemaVersion:2,assets:[]});return response(pack());}});
  await assert.rejects(a.getManifest(),/could not be loaded/);await assert.rejects(a.getManifest(),/Unsupported/);
  const manifest=await a.getManifest();assert.equal(await a.getManifest(),manifest);assert.equal(reads,3);assert.ok(Object.isFrozen(manifest.assets[0]));
  await assert.rejects(a.load('missing'),/Unknown football asset/);await assert.rejects(a.load('speed-wr',{lod:'ultra'}),/LOD/);assert.equal(a.cachedTemplates,0);assert.equal(a.baseURL,'https://cdn.example/football/');a.dispose();
});
test('concurrent instances share one template but isolate bones, morph weights and uniform materials',async()=>{
  let loads=0,reads=0;const source=gltf();const a=loader({fetch:async()=>{reads++;return response(pack());},loadGLTF:async()=>{loads++;return source;}});
  const [one,two]=await Promise.all([a.load('speed-wr'),a.load('speed-wr')]);
  assert.equal(loads,1);assert.equal(reads,1);const [x,y]=[one.scene.nodes[0],two.scene.nodes[0]];
  x.material.color.value='red';x.skeleton.bones[0].rotation=1;x.morphTargetInfluences[0]=1;
  x.skeleton.boneInverses[0].elements[0]=99;assert.equal(y.skeleton.boneInverses[0].elements[0],1);assert.equal(source.mesh.skeleton.boneInverses[0].elements[0],1);
  one.animations[0].tracks[0].values[0]=5;assert.equal(two.animations[0].tracks[0].values[0],0);assert.equal(source.animations[0].tracks[0].values[0],0);
  assert.equal(y.material.color.value,'blue');assert.equal(y.skeleton.bones[0].rotation,0);assert.equal(y.morphTargetInfluences[0],0);assert.equal(source.mat.color.value,'blue');
  assert.equal(x.geometry,y.geometry);assert.equal(x.geometry,source.geometry);assert.equal(x.material.map,source.texture);assert.notEqual(x.material,y.material);
  one.release();one.release();assert.equal(x.material.disposed,1);assert.equal(source.geometry.disposed,0);assert.equal(source.texture.disposed,0);
  a.dispose();assert.equal(source.geometry.disposed,0,'a live second instance still uses shared geometry');two.release();assert.equal(source.geometry.disposed,1);assert.equal(source.texture.disposed,1);assert.equal(source.mat.disposed,1);
});
test('bounded LRU cache defers disposal of evicted templates until their instances release',async()=>{
  const sources=[],a=loader({maxTemplates:1,loadGLTF:async()=>{const source=gltf();sources.push(source);return source;}});
  const first=await a.load('speed-wr'),second=await a.load('safety');assert.equal(a.cachedTemplates,1);assert.equal(sources[0].geometry.disposed,0);
  first.release();assert.equal(sources[0].geometry.disposed,1);
  const third=await a.load('speed-wr');assert.equal(sources.length,3);assert.equal(a.cachedTemplates,1);second.release();third.release();a.dispose();assert.ok(sources.every(s=>s.geometry.disposed===1));
});
test('templates sharing a geometry cannot dispose it while another template still owns it',async()=>{
  const shared=resource(),a=loader({maxTemplates:1,loadGLTF:async()=>gltf(shared)});
  const first=await a.load('speed-wr'),second=await a.load('safety');first.release();assert.equal(shared.disposed,0);second.release();assert.equal(shared.disposed,0);a.dispose();assert.equal(shared.disposed,1);
});
test('failed GLB loads and malformed scenes are evicted and can be retried',async()=>{
  let loads=0;const a=loader({loadGLTF:async()=>{loads++;if(loads===1)throw Error('temporary network failure');if(loads===2)return {scene:null};return gltf();}});
  await assert.rejects(a.load('speed-wr'),/temporary/);assert.equal(a.cachedTemplates,0);
  await assert.rejects(a.load('speed-wr'),/usable scene/);assert.equal(a.cachedTemplates,0);
  const instance=await a.load('speed-wr');assert.equal(loads,3);instance.release();a.dispose();
});
test('unsafe clones are rejected without disposing template bones or detaching the original scene',async()=>{
  const source=gltf();source.mesh.skeleton.boneTexture=resource();
  const a=loader({loadGLTF:async()=>source,clone:s=>s});await assert.rejects(a.load('speed-wr'),/independent scene/);assert.equal(source.scene.removed,undefined);assert.equal(source.mesh.skeleton.boneTexture.disposed,0);a.dispose();assert.equal(source.mesh.skeleton.boneTexture.disposed,1);
  const other=gltf(),b=loader({loadGLTF:async()=>other,clone:s=>scene(s.nodes.map(o=>({...o,skeleton:{bones:o.skeleton.bones}})))});
  await assert.rejects(b.load('speed-wr'),/reused a template skeleton/);assert.equal(other.mat.disposed,0);b.dispose();
});
test('disposing an in-flight loader releases its eventual template and prevents activation',async()=>{
  let finish;const source=gltf(),a=loader({loadGLTF:()=>new Promise(resolve=>{finish=resolve;})}),pending=a.load('speed-wr');
  while(!finish)await new Promise(resolve=>setImmediate(resolve));a.dispose();finish(source);await assert.rejects(pending,/disposed/);assert.equal(source.geometry.disposed,1);assert.equal(source.texture.disposed,1);await assert.rejects(a.getManifest(),/disposed/);
});
