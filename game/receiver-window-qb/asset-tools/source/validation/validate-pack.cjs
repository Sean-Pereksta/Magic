#!/usr/bin/env node
'use strict';
// Independent asset acceptance checks. This does not exercise football gameplay.
// Usage: npm ci && node validate-pack.cjs /path/to/extracted/pack
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const validator = require('gltf-validator');
const defaultRoot=[path.join(__dirname,'..','pack'),path.join(__dirname,'..','..')].find(p=>fs.existsSync(path.join(p,'manifest.json')))||path.join(__dirname,'..','pack');
const root = path.resolve(process.argv[2] || defaultRoot);
const reportDir = path.join(root, 'validation');
const onlyPattern=process.argv[3]==='--only'?process.argv[4]:null;
const requiredAnimationGroups = {
  locomotion: /idle|walk|jog|run/i, sprint: /sprint/i, acceleration: /accel/i,
  plant: /plant/i, cut: /cut/i, jump: /jump/i, highPoint: /high.?point/i,
  oneHand: /one.?hand/i, backShoulder: /back.?shoulder/i, diveCatch: /div.*catch|catch.*div/i,
  toeTap: /toe.?tap/i, standingSwat: /standing.?swat|swat.?stand/i,
  jumpingSwat: /jump.*swat|swat.*jump/i, divingBreakup: /div.*break|div.*swat|div.*pbu/i,
  interception: /intercept/i, tackle: /tackl/i, fall: /fall/i, recovery: /recover/i,
  celebration: /celebrat/i,
};
const failures = [], warnings = [];
const check = (ok, text, bucket = failures) => { if (!ok) bucket.push(text); };
function filesIn(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir, {withFileTypes:true}).flatMap(e => e.isDirectory() ? filesIn(path.join(dir,e.name)) : [path.join(dir,e.name)]);
}
function readGLB(file) {
  const data = fs.readFileSync(file);
  if (data.length < 20 || data.readUInt32LE(0) !== 0x46546c67) throw Error('Invalid GLB magic/length');
  if (data.readUInt32LE(4) !== 2 || data.readUInt32LE(8) !== data.length) throw Error('GLB version or declared length mismatch');
  let json, bin, cursor=12;
  while(cursor < data.length) {
    const length=data.readUInt32LE(cursor), type=data.readUInt32LE(cursor+4);
    if(length % 4 || cursor+8+length > data.length) throw Error('Invalid GLB chunk alignment/length');
    const chunk=data.subarray(cursor+8,cursor+8+length);
    if(type===0x4e4f534a) json=JSON.parse(chunk.toString('utf8').trim());
    if(type===0x004e4942) bin=chunk;
    cursor += 8+length;
  }
  if(!json || !bin) throw Error('Expected JSON and embedded binary chunks');
  return {data,json,bin};
}
const components={5120:[1,'getInt8'],5121:[1,'getUint8'],5122:[2,'getInt16'],5123:[2,'getUint16'],5125:[4,'getUint32'],5126:[4,'getFloat32']};
const sizes={SCALAR:1,VEC2:2,VEC3:3,VEC4:4,MAT2:4,MAT3:9,MAT4:16};
function accessorReader(gltf,bin) {
  const cache=new Map();
  return function read(index) {
    if(cache.has(index)) return cache.get(index);
    const a=gltf.accessors[index];
    if(!a) throw Error('Accessor does not exist: '+index);
    const bv=a.bufferView===undefined?null:gltf.bufferViews[a.bufferView], [width,fn]=components[a.componentType]||[];
    if(!width || !sizes[a.type]) throw Error('Unknown accessor type');
    const count=sizes[a.type], stride=bv?.byteStride||width*count;
    const offset=(bv?.byteOffset||0)+(a.byteOffset||0);
    const view=new DataView(bin.buffer,bin.byteOffset,bin.byteLength);
    const values=new Float64Array(a.count*count);
    if(bv)for(let i=0;i<a.count;i++) for(let c=0;c<count;c++) {
      let v=view[fn](offset+i*stride+c*width,true);
      if(a.normalized) {
        if(a.componentType===5120) v=Math.max(v/127,-1);
        if(a.componentType===5121) v/=255;
        if(a.componentType===5122) v=Math.max(v/32767,-1);
        if(a.componentType===5123) v/=65535;
      }
      values[i*count+c]=v;
    }
    if(a.sparse){
      const s=a.sparse,ib=gltf.bufferViews[s.indices.bufferView],vb=gltf.bufferViews[s.values.bufferView];
      const [iw,ifn]=components[s.indices.componentType],io=(ib.byteOffset||0)+(s.indices.byteOffset||0),vo=(vb.byteOffset||0)+(s.values.byteOffset||0);
      for(let i=0;i<s.count;i++){
        const at=view[ifn](io+i*iw,true);
        for(let c=0;c<count;c++){
          let v=view[fn](vo+(i*count+c)*width,true);
          if(a.normalized){if(a.componentType===5120)v=Math.max(v/127,-1);if(a.componentType===5121)v/=255;if(a.componentType===5122)v=Math.max(v/32767,-1);if(a.componentType===5123)v/=65535;}
          values[at*count+c]=v;
        }
      }
    }
    const r={accessor:a,values,components:count}; cache.set(index,r); return r;
  };
}
function imageInfo(gltf,bin,image) {
  if(image.uri) return {embedded:false,uri:image.uri};
  const b=gltf.bufferViews[image.bufferView];
  const bytes=bin.subarray(b.byteOffset||0,(b.byteOffset||0)+b.byteLength);
  const png=bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]));
  return {embedded:true,mimeType:image.mimeType,bytes:bytes.length,png,width:png?bytes.readUInt32BE(16):null,height:png?bytes.readUInt32BE(20):null};
}
async function validate(file) {
  const relative=path.relative(root,file).replaceAll('\\','/');
  const localErrors=[], localWarnings=[];
  const assert=(ok,message) => {if(!ok) localErrors.push(message);};
  const {data,json:g,bin}=readGLB(file), read=accessorReader(g,bin);
  const official=await validator.validateBytes(new Uint8Array(data), {uri:relative,maxIssues:10000});
  assert(official.issues.numErrors===0,`Khronos errors: ${official.issues.numErrors}`);
  assert((g.buffers||[]).every(b=>!b.uri),'All buffers embedded');
  const isPlayer=relative.startsWith('models/');
  const nodes=g.nodes||[], meshes=g.meshes||[], materials=g.materials||[];
  const geometryHash=crypto.createHash('sha256');
  let vertexCount=0, triangles=0, invalidWeights=0, invalidIndices=0, invalidNormals=0, badMorphs=0, degenerateTriangles=0;
  const morphNames=new Set(), meshSummaries=[], weightedJoints=new Set();
  for(const [mi,m] of meshes.entries()) {
    const targetNames=m.extras?.targetNames||[];
    targetNames.forEach(n=>morphNames.add(n));
    let mv=0,mt=0;
    for(const p of m.primitives||[]) {
      const pos=read(p.attributes.POSITION), normal=p.attributes.NORMAL===undefined?null:read(p.attributes.NORMAL);
      geometryHash.update(Buffer.from(pos.values.buffer,pos.values.byteOffset,pos.values.byteLength));
      assert(pos.components===3,'POSITION must be VEC3');
      assert([...pos.values].every(Number.isFinite),'Finite vertex positions');
      assert(normal && normal.accessor.count===pos.accessor.count,'Normals for all vertices');
      if(normal) for(let i=0;i<normal.values.length;i+=3) {
        const len=Math.hypot(normal.values[i],normal.values[i+1],normal.values[i+2]);
        if(!Number.isFinite(len)||Math.abs(len-1)>0.02) invalidNormals++;
      }
      assert((p.mode??4)===4,'Triangle primitive mode');
      if(p.indices!==undefined) {
        const idx=read(p.indices);
        for(const v of idx.values) if(v<0||v>=pos.accessor.count||!Number.isInteger(v)) invalidIndices++;
        for(let i=0;i<idx.values.length;i+=3){
          const ia=idx.values[i]*3,ib=idx.values[i+1]*3,ic=idx.values[i+2]*3,v=pos.values;
          const ax=v[ib]-v[ia],ay=v[ib+1]-v[ia+1],az=v[ib+2]-v[ia+2],bx=v[ic]-v[ia],by=v[ic+1]-v[ia+1],bz=v[ic+2]-v[ia+2];
          const cx=ay*bz-az*by,cy=az*bx-ax*bz,cz=ax*by-ay*bx;
          if(cx*cx+cy*cy+cz*cz<1e-22)degenerateTriangles++;
        }
        mt += idx.values.length/3;
      } else mt += pos.accessor.count/3;
      mv+=pos.accessor.count;
      if(p.attributes.WEIGHTS_0!==undefined) {
        const weights=read(p.attributes.WEIGHTS_0), joints=read(p.attributes.JOINTS_0);
        assert(weights.accessor.count===pos.accessor.count && joints.accessor.count===pos.accessor.count,'Skin accessor counts');
        const jointLimit=Math.max(0,...(g.skins||[]).map(s=>s.joints.length));
        for(let i=0;i<weights.values.length;i+=4) {
          const w=Array.from(weights.values.subarray(i,i+4));
          if(w.some(x=>!Number.isFinite(x)||x<0||x>1)||Math.abs(w.reduce((a,b)=>a+b,0)-1)>0.0002) invalidWeights++;
          if(Array.from(joints.values.subarray(i,i+4)).some(x=>x<0||x>=jointLimit||!Number.isInteger(x))) invalidWeights++;
          for(let j=0;j<4;j++)if(w[j]>0.00001)weightedJoints.add(joints.values[i+j]);
        }
      } else if(isPlayer) assert(false,'Player primitives must be skinned');
      if(targetNames.length) assert((p.targets||[]).length===targetNames.length,'Morph names/target count');
      for(const t of p.targets||[]) for(const [attribute,ai] of Object.entries(t)) {
        const ar=read(ai);
        if(ar.accessor.count!==pos.accessor.count||ar.components!==3||Array.from(ar.values).some(x=>!Number.isFinite(x))) badMorphs++;
        assert(['POSITION','NORMAL','TANGENT'].includes(attribute),'Valid morph attribute');
      }
    }
    vertexCount+=mv;triangles+=mt;
    meshSummaries.push({name:m.name,vertices:mv,triangles:mt,morphs:targetNames});
  }
  assert(invalidWeights===0,`${invalidWeights} invalid skin weights or joint indices`);
  assert(invalidIndices===0,`${invalidIndices} invalid geometry indices`);
  assert(invalidNormals===0,`${invalidNormals} non-unit/invalid normals`);
  assert(badMorphs===0,`${badMorphs} malformed morph targets`);
  if(degenerateTriangles)localWarnings.push(`${degenerateTriangles} zero-area or near-zero-area triangles`);
  const skins=(g.skins||[]).map(s=>({name:s.name,jointCount:s.joints.length,jointNames:s.joints.map(i=>nodes[i]?.name)}));
  for(const s of g.skins||[]) {
    assert(s.joints.every(i=>nodes[i]),'Skin joints exist');
    if(s.inverseBindMatrices!==undefined) assert(read(s.inverseBindMatrices).accessor.count===s.joints.length,'Inverse bind matrix count');
  }
  const names=nodes.map(n=>n.name||'');
  const fingers=['thumb','index','middle','ring','pinky'];
  if(isPlayer) {
    assert(skins.length>0,'Player skin exists');
    assert(skins.some(s=>s.jointCount>=50),'At least 50 joints for articulated body and fingers');
    assert(morphNames.size>=5,'At least five physique morph parameters');
    for(const [side,suffix] of [['left','.L'],['right','.R']]) for(const finger of fingers) assert(names.some(n=>(n.toLowerCase().includes(side)||n.endsWith(suffix))&&n.toLowerCase().includes(finger)),`${side} ${finger} joint`);
    const weightedNames=(g.skins?.[0]?.joints||[]).filter((_,i)=>weightedJoints.has(i)).map(i=>nodes[i]?.name||'');
    for(const suffix of ['.L','.R'])for(const finger of fingers)assert(weightedNames.some(n=>n.endsWith(suffix)&&n.toLowerCase().includes(finger)),`${finger}${suffix} is bound to actual geometry`);
    for(const re of [/jersey/i,/skin/i,/helmet|equipment/i,/glove|accessor/i]) assert(materials.some(m=>re.test(m.name||'')),`Separate material semantic ${re}`);
  }
  const animations=[];
  for(const animation of g.animations||[]) {
    let end=0,start=Infinity,channelCount=0,animatedChannels=0;
    for(const channel of animation.channels||[]) {
      const sampler=animation.samplers[channel.sampler], input=read(sampler.input), output=read(sampler.output);
      const times=input.values;
      assert(times.length>1,`${animation.name}: animation has >1 time sample`);
      for(let i=0;i<times.length;i++) assert(Number.isFinite(times[i])&&times[i]>=0&&(i===0||times[i]>times[i-1]),`${animation.name}: finite increasing nonnegative times`);
      assert(Array.from(output.values).every(Number.isFinite),`${animation.name}: finite output`);
      let varies=false;
      for(let c=0;c<output.components;c++)for(let k=c+output.components;k<output.values.length;k+=output.components)if(Math.abs(output.values[k]-output.values[c])>0.00001){varies=true;break;}
      if(varies)animatedChannels++;
      assert(nodes[channel.target.node],`${animation.name}: target node exists`);
      if(channel.target.path==='rotation') for(let i=0;i<output.values.length;i+=4) assert(Math.abs(Math.hypot(...output.values.subarray(i,i+4))-1)<0.01,`${animation.name}: unit quaternion`);
      end=Math.max(end,times.at(-1)); start=Math.min(start,times[0]);channelCount++;
    }
    assert(end>0&&channelCount>0,`${animation.name}: positive duration and channels`);
    assert(animatedChannels>0,`${animation.name}: has at least one animated channel with actual motion`);
    animations.push({name:animation.name,start:Number.isFinite(start)?start:null,duration:end,channels:channelCount,animatedChannels});
  }
  if(isPlayer) for(const [group,re] of Object.entries(requiredAnimationGroups)) assert(animations.some(a=>re.test(a.name)),`Required animation group: ${group}`);
  const images=(g.images||[]).map(i=>imageInfo(g,bin,i));
  if(isPlayer) assert(images.length>0,'Player includes actual embedded texture image');
  assert(images.every(i=>i.embedded),'Texture images embedded');
  assert(images.every(i=>i.bytes>64&&i.width>1&&i.height>1),'Texture images have dimensions and real data');
  const summary={file:relative,sha256:crypto.createHash('sha256').update(data).digest('hex'),baseGeometrySha256:geometryHash.digest('hex'),bytes:data.length,
    isPlayer,vertexCount,triangles,meshes:meshSummaries,materials:materials.map(m=>({name:m.name,alphaMode:m.alphaMode||'OPAQUE',doubleSided:!!m.doubleSided,textured:!!m.pbrMetallicRoughness?.baseColorTexture})),
    skins,morphNames:[...morphNames],animations,images,invalidWeights,invalidIndices,invalidNormals,badMorphs,degenerateTriangles,
    errors:localErrors,warnings:localWarnings,khronos:{version:validator.version(),errors:official.issues.numErrors,warnings:official.issues.numWarnings,infos:official.issues.numInfos,messages:official.issues.messages}};
  fs.writeFileSync(path.join(reportDir,path.basename(file,'.glb')+'.json'),JSON.stringify(summary,null,2)+'\n');
  localErrors.forEach(e=>failures.push(relative+': '+e));
  localWarnings.forEach(e=>warnings.push(relative+': '+e));
  if(official.issues.numWarnings) warnings.push(`${relative}: ${official.issues.numWarnings} Khronos warning(s), see asset report`);
  return summary;
}
(async()=>{
  fs.mkdirSync(reportDir,{recursive:true});
  const files=filesIn(root).filter(f=>f.toLowerCase().endsWith('.glb')&&(!onlyPattern||f.replaceAll('\\','/').endsWith(onlyPattern)));
  check(files.length>0,'No GLBs found');
  const results=[];
  for(const file of files) {
    try {results.push(await validate(file)); console.log('Validated '+path.relative(root,file));}
    catch(error) {failures.push(path.relative(root,file)+': '+error.stack);}
  }
  const players=results.filter(r=>r.isPlayer);
  const primaryPlayers=players.filter(r=>!/_lod\d+\.glb$/i.test(r.file));
  if(!onlyPattern)check(primaryPlayers.length>=16,'At least 16 primary player archetypes');
  check(new Set(players.map(p=>p.sha256)).size===players.length,'Archetypes are distinct binary assets');
  check(new Set(primaryPlayers.map(p=>p.baseGeometrySha256)).size===primaryPlayers.length,'Primary archetypes have distinct base vertex geometry');
  if(!onlyPattern)for(const folder of ['models/quarterbacks','models/receivers','models/defenders','models/other-positions','animations','equipment','textures','source']) check(filesIn(path.join(root,folder)).length>0,'Nonempty required folder '+folder);
  if(!onlyPattern)for(const f of ['manifest.json','README.md']) check(fs.existsSync(path.join(root,f)),'Required '+f);
  if(!onlyPattern&&fs.existsSync(path.join(root,'manifest.json'))) {
    try {
      const manifest=JSON.parse(fs.readFileSync(path.join(root,'manifest.json'),'utf8'));
      const archetypes=manifest.archetypes||[];
      check(archetypes.length>=16,'Manifest includes at least 16 archetypes');
      check(new Set(archetypes.map(a=>a.id)).size===archetypes.length,'Unique manifest archetype IDs');
      for(const a of archetypes) {
        const model=results.find(r=>r.file===a.model),lod=results.find(r=>r.file===a.lod1);
        check(!!model,`Manifest model exists: ${a.model}`);
        check(!!lod,`Manifest LOD exists: ${a.lod1}`);
        if(model&&lod)check(lod.triangles<model.triangles,`LOD reduces triangles: ${a.id}`);
        if(model)for(const name of a.morph_targets||[])check(model.morphNames.includes(name),`Manifest morph exported: ${a.id}/${name}`);
        if(model)for(const name of a.clips||[])check(model.animations.some(c=>c.name===name),`Manifest clip exported: ${a.id}/${name}`);
      }
      for(const result of results)for(const clip of result.animations){
        const expected=manifest.animations?.[clip.name]?.duration_seconds;
        if(expected!==undefined)check(Math.abs(clip.duration-expected)<=1/30+0.0001,`Clip duration matches 30fps manifest: ${result.file}/${clip.name} expected ${expected}, exported ${clip.duration}`);
        check(clip.start===null||Math.abs(clip.start)<0.0001,`Clip starts at zero: ${result.file}/${clip.name}`);
      }
    }catch(error){failures.push('Manifest parsing: '+error.message);}
  }
  const report={generatedAt:new Date().toISOString(),scope:'Offline GLB format, mesh, rig, morph, animation and packaging checks only. Gameplay, visual quality, IK accuracy and device performance require separate checks.',
    partial:!!onlyPattern,passed:failures.length===0,glbCount:results.length,archetypeCount:primaryPlayers.length,playerModelCountIncludingLODs:players.length,totalBytes:results.reduce((n,r)=>n+r.bytes,0),
    khronosErrors:results.reduce((n,r)=>n+r.khronos.errors,0),khronosWarnings:results.reduce((n,r)=>n+r.khronos.warnings,0),
    failures,warnings,assets:results.map(({file,sha256,bytes,vertexCount,triangles,morphNames,animations,skins,khronos,errors})=>({file,sha256,bytes,vertexCount,triangles,morphNames,animationCount:animations.length,jointCount:skins[0]?.jointCount,khronosErrors:khronos.errors,khronosWarnings:khronos.warnings,passed:errors.length===0}))};
  fs.writeFileSync(path.join(reportDir,onlyPattern?'partial-summary.json':'summary.json'),JSON.stringify(report,null,2)+'\n');
  console.log(JSON.stringify({passed:report.passed,archetypeCount:report.archetypeCount,glbCount:report.glbCount,khronosErrors:report.khronosErrors,khronosWarnings:report.khronosWarnings,failures:report.failures},null,2));
  process.exitCode=report.passed?0:1;
})().catch(e=>{console.error(e);process.exitCode=1;});
