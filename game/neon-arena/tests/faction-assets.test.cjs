'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),vm=require('node:vm');
const factions=require('../neon-factions.js'),models=require('../neon-models.js'),root=path.resolve(__dirname,'..');
const manifest=JSON.parse(fs.readFileSync(path.join(root,'assets/factions/manifest.json'),'utf8'));
const bytes=new Map(manifest.models.map(m=>[m.file,fs.readFileSync(path.join(root,'assets',m.file))]));
const parsed=new Map([...bytes].map(([p,b])=>[p,models.parseGLB(b,p)]));
const near=(a,b,message)=>assert.ok(Math.abs(a-b)<1e-5,`${message}: ${a} != ${b}`);
function jsonGLB(bytes){const length=bytes.readUInt32LE(12);return JSON.parse(bytes.subarray(20,20+length).toString().trim());}
function assets(config={}){
  const context=vm.createContext({URL,console,location:{href:'https://game.test/repo/game/neon-arena/'},NEON_ASSET_CONFIG:config});context.window=context;
  for(const file of ['neon-factions.js','neon-assets-config.js','neon-assets.js'])vm.runInContext(fs.readFileSync(path.join(root,file),'utf8'),context);
  return context.NeonAssets;
}
function recordingGL(){
  let currentBuffer=null,id=0;const events={uploads:[],draws:[],uniforms:{},ranges:[]};
  const gl={VERTEX_SHADER:1,FRAGMENT_SHADER:2,COMPILE_STATUS:3,LINK_STATUS:4,ARRAY_BUFFER:5,STATIC_DRAW:6,FLOAT:7,TRIANGLES:8,DEPTH_TEST:9,LEQUAL:10,BLEND:11,CULL_FACE:12,
    createShader:()=>({}),shaderSource(){},compileShader(){},getShaderParameter:()=>true,deleteShader(){},createProgram:()=>({}),attachShader(){},linkProgram(){},getProgramParameter:()=>true,deleteProgram(){},
    getAttribLocation:(_,name)=>['aPosition','aNormal','aColor','aEmissive'].indexOf(name),getUniformLocation:(_,name)=>name,
    createBuffer:()=>({id:++id}),bindBuffer:(_,buffer)=>{currentBuffer=buffer;},bufferData:(_,vertices)=>{currentBuffer.vertices=vertices;events.uploads.push(currentBuffer);},deleteBuffer(){},
    useProgram(){},enable(){},disable(){},depthFunc(){},enableVertexAttribArray(){},vertexAttribPointer(){},
    uniform3fv:(name,v)=>{events.uniforms[name]=[...v];},uniform1f:(name,v)=>{events.uniforms[name]=v;},uniformMatrix4fv:(name,_,v)=>{assert.ok([...v].every(Number.isFinite));events.uniforms[name]=[...v];},
    drawArrays:(_,start,count)=>{assert.equal(start,0);assert.ok(count>0);events.draws.push({buffer:currentBuffer,matrix:events.uniforms.uModel.slice(),ally:events.uniforms.uAllyHighlight});},depthRange:(a,b)=>events.ranges.push([a,b])
  };return {gl,events};
}
function renderer(){const {gl,events}=recordingGL();const cache={get:p=>parsed.get(p),status:()=>({}),preload:async p=>p.map(k=>parsed.get(k))};const render=models.createRenderer(gl,{cache});render.beginView({position:[0,1.2,-5],right:[1,0,0],up:[0,1,0],forward:[0,0,1],aspect:1},1);return {render,events};}
const actor=(extra={})=>({x:0,y:1.86,z:0,yaw:0,faceYaw:0,pitch:0,vx:0,vz:0,onGround:true,hp:100,armor:50,...extra});

test('twelve original faction operators and six weapons have deployed geometry, portraits and matching manifest hashes',()=>{
  assert.equal(factions.characters.length,12);assert.equal(factions.weapons.length,6);assert.equal(manifest.models.length,18);
  assert.equal(factions.characters.filter(c=>c.faction==='cats').length,6);assert.equal(factions.characters.filter(c=>c.faction==='mice').length,6);
  const shapes=[];
  for(const entry of manifest.models){
    const b=bytes.get(entry.file),j=jsonGLB(b),m=parsed.get(entry.file);assert.equal(b.length,entry.bytes);assert.equal(crypto.createHash('sha256').update(b).digest('hex'),entry.sha256);
    assert.equal(j.asset.version,'2.0');assert.equal(j.buffers.length,1);assert.equal(j.buffers[0].uri,undefined);assert.ok(!j.images?.length&&!j.skins?.length&&!j.extensionsRequired?.length);
    assert.ok(j.meshes.every(m=>m.primitives.every(p=>p.attributes.NORMAL!==undefined)));assert.ok(j.materials.every(m=>(m.alphaMode||'OPAQUE')==='OPAQUE'));
    assert.ok(entry.bytes<500000,entry.id+' bounded download');assert.ok(m.vertexCount<24000,entry.id+' bounded GPU geometry');assert.ok(m.vertexCount>7000,entry.id+' detailed geometry');
    assert.equal(m.parts.reduce((n,p)=>n+p.vertices.length,0),m.vertices.length);
    for(let i=0;i<m.vertices.length;i+=12){assert.ok([...m.vertices.subarray(i,i+12)].every(Number.isFinite));near(Math.hypot(...m.vertices.subarray(i+3,i+6)),1,entry.id+' unit normal');}
    const positions=new Float32Array(m.vertexCount*3);for(let i=0,k=0;i<m.vertices.length;i+=12,k+=3)positions.set(m.vertices.subarray(i,i+3),k);
    shapes.push(crypto.createHash('sha256').update(Buffer.from(positions.buffer)).digest('hex'));
    assert.ok(fs.readFileSync(path.join(root,'assets',entry.portrait),'utf8').includes('actual model preview'));
  }
  assert.equal(new Set(shapes).size,18,'all variants change geometry, not only materials');
});

test('animal anatomy, explicit support-hand sides and nine rigid groups follow authored pivots',()=>{
  for(const c of factions.characters){const m=parsed.get(c.model),classified=new Map(m.parts.flatMap(p=>p.names.map(n=>[n,p.key])));
    assert.deepEqual(m.parts.map(p=>p.key).sort(),['body','head','armL','armR','legL','legR','tail','earL','earR'].sort());
    assert.ok([...classified.keys()].some(n=>/^head_whisker_/.test(n)));assert.ok([...classified.keys()].some(n=>/^snout_/.test(n)));assert.ok([...classified.keys()].some(n=>/^tail_/.test(n)));
    for(const [name,group] of classified){
      const side=/_([LR])_\d+$/.exec(name)?.[1];if(side&&/^(glove|forearm|upper_arm|shoulder|elbow)_/.test(name))assert.equal(group,'arm'+side,name+' crosses center without changing limbs');
      if(/^ear_/.test(name))assert.equal(group,'ear'+side,name);if(/^head_|^helmet_|^snout_/.test(name))assert.equal(group,'head',name);
    }
    assert.equal(m.rig.holdPose,true);assert.deepEqual(m.rig.weaponGrip,[.32,1.43,.43]);assert.deepEqual(m.parts.find(p=>p.key==='tail').pivot,m.rig.tailPivot);
    const fit=models.actorFit(m,actor());near(fit.scale[1]*m.bodyHeight,2.39,'common body height');near(m.bounds.min[1]*fit.scale[1]+fit.offset[1],0,'soles stay grounded');
    assert.ok(m.height>m.bodyHeight,'ears extend beyond the body without shrinking the torso');assert.deepEqual(m.anchors.head,[0,2.15,.12]);
  }
});

test('sculpted animal heads have outward-facing authored normals',()=>{
  for(const c of factions.characters){const b=bytes.get(c.model),j=jsonGLB(b),jsonLength=b.readUInt32LE(12),binStart=28+jsonLength;
    const head=j.nodes.find(n=>/^head_\d+$/.test(n.name)),primitive=j.meshes[head.mesh].primitives[0];
    function read(index){const a=j.accessors[index],v=j.bufferViews[a.bufferView];return Array.from({length:a.count},(_,i)=>[0,1,2].map(k=>b.readFloatLE(binStart+v.byteOffset+(a.byteOffset||0)+i*12+k*4)));}
    const positions=read(primitive.attributes.POSITION),normals=read(primitive.attributes.NORMAL);
    // The widest X slices of both animal heads have an unambiguous outward
    // direction, independent of their sculpted muzzle/chin and axial caps.
    for(let i=0;i<positions.length;i++)if(Math.abs(positions[i][0])>.14)assert.ok(positions[i][0]*normals[i][0]>0,c.id+' head cheek normal points outward');
  }
});

test('all operator trigger hands line up with the separately equipped weapon grip',()=>{
  for(const c of factions.characters){const j=jsonGLB(bytes.get(c.model)),m=parsed.get(c.model),hand=j.nodes.find(n=>/^glove_R_\d+$/.test(n.name)).translation;
    const origin=m.rig.weaponGrip,target=[origin[0],origin[1]-.205*.7,origin[2]-.14*.7];
    assert.ok(Math.hypot(...hand.map((v,i)=>v-target[i]))<.04,c.id+' trigger-hand placement');
    const support=j.nodes.find(n=>/^glove_L_\d+$/.test(n.name)).translation;
    assert.ok(support[2]>origin[2]&&support[1]>hand[1],c.id+' support hand extends toward the handguard');
  }
});

test('all catalog paths resolve to existing local files and weapon aliases share the canonical URL',()=>{
  const api=assets();for(const p of factions.assetPaths){assert.ok(fs.existsSync(path.join(root,'assets',p)),p);assert.equal(api.url(p).split('?')[0],'https://game.test/repo/game/neon-arena/assets/'+p);}
  for(const w of factions.weapons){assert.equal(api.url('weapons/'+w.id+'.glb'),api.url(w.model));assert.equal(api.weapons[w.id],w.model);assert.equal(api.localModelURL(w.model),'https://game.test/repo/game/neon-arena/assets/'+w.model);}
  for(const c of factions.characters)assert.equal(api.characters[c.id],c.model);
  for(const invalid of ['factions/cats/../secret.glb','factions/cats/cat_missing.glb','factions/mice/cat_assault.glb','weapons/new/../../secret.glb'])assert.equal(api.url(invalid),'');
  assert.match(api.url('weapons/assault.glb'),/^https:\/\/pub-/,'original hosted arsenal is retained');
});

test('a failed optional expansion CDN transparently loads the exact bundled model',async()=>{
  const api=assets({EXPANSION_ASSET_BASE_URL:'https://cdn.test/expansion'}),p=factions.characters[0].model,requests=[];
  const cache=new models.ModelCache({resolve:api.url,fallback:api.localModelURL,fetch:async url=>{requests.push(url);if(url.startsWith('https://cdn.test'))throw Error('offline');const b=bytes.get(p);return {ok:true,arrayBuffer:async()=>b.buffer.slice(b.byteOffset,b.byteOffset+b.byteLength)};}});
  await cache.preload([p,p]);assert.equal(requests.length,2);assert.equal(requests[1],api.localModelURL(p));assert.equal(cache.status().parsed,1);
});

test('ear aiming follows the head, tails articulate and nearby animation shares GPU buffers',()=>{
  const p=factions.characters[0].model,m=parsed.get(p),{render,events}=renderer();const first=actor({pitch:.32,vz:4,walk:2});
  render.drawActor(p,first,{animate:true,animationBudget:2,now:1,allyHighlight:true});const indices=Object.fromEntries(m.parts.map((part,i)=>[part.key,i])),head=events.draws[indices.head].matrix;
  for(const key of ['earL','earR']){const pivot=m.parts[indices[key]].pivot,expected=models.transform(head,pivot),actual=models.transform(events.draws[indices[key]].matrix,pivot);actual.forEach((v,i)=>near(v,expected[i],key+' inherits head aiming'));}
  assert.notDeepEqual(events.draws[indices.tail].matrix,models.compose());assert.equal(events.draws[0].ally,.35,'subtle allied rim preserves faction materials');
  const uploads=events.uploads.length;render.drawActor(p,actor({x:2}),{animate:true,animationBudget:2,now:1});render.drawActor(p,actor({x:4}),{animate:true,animationBudget:2,now:1});
  assert.equal(events.uploads.length,uploads,'identical actors share buffers');assert.equal(events.draws.length,19,'two nine-part actors and one budgeted static actor');assert.equal(render.stats().animatedActors,2);
});

test('crouch scales visual targets once and leaves the feet at the support surface',()=>{
  const p=factions.characters[0].model,{render,events}=renderer();render.drawActor(p,actor({crouchPose:1,slidePose:1}),{animate:false,now:1});const matrix=events.draws[0].matrix;
  near(models.transform(matrix,[0,0,0])[1],0,'feet');near(models.transform(matrix,[0,2.15,0])[1],2.15*.73*Math.cos(.11),'slide and crouch share posture rather than compounding');
});

test('new reloads remove magazines, top loaders lift up, bolts cycle and shotgun pumps move independently',()=>{
  for(const w of factions.weapons){const m=parsed.get(w.model),{render,events}=renderer();render.drawWeapon(w.model,models.compose(),{reload:.5,recoil:1},true);
    const draws=Object.fromEntries(m.parts.map((p,i)=>[p.key,events.draws[i].matrix]));draws.body.forEach((v,i)=>near(v,models.compose()[i],w.id+' receiver stays fixed'));assert.ok(draws.bolt[14]<-.10,w.id+' bolt cycles');
    if(w.id==='clawbreaker'){assert.ok(draws.pump[14]<-.10);assert.equal(draws.magazine,undefined,'tube magazine stays mounted');}
    else{const mag=m.parts.find(p=>p.key==='magazine'),at=models.transform(draws.magazine,mag.pivot);if(w.id==='purr90')assert.ok(at[1]>mag.pivot[1],w.id+' top loading');else assert.ok(at[1]<mag.pivot[1]-.30,w.id+' magazine removed');}
    assert.deepEqual(events.ranges,[[0,.12],[0,1]],'foreground depth restored');
  }
});
