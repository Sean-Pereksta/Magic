import { mkdir,writeFile,readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { buildBattlePack } from './battle-model-source.mjs';

const base=fileURLToPath(new URL('../',import.meta.url)),runtime=path.join(base,'assets','battle','v1');
const destination=process.argv[2]&&path.resolve(process.argv[2]);
const pack=buildBattlePack();
await mkdir(runtime,{recursive:true});
await writeFile(path.join(runtime,'pack.json'),JSON.stringify(pack));

function quaternion([x,y,z]){
  const cx=Math.cos(x/2),sx=Math.sin(x/2),cy=Math.cos(y/2),sy=Math.sin(y/2),cz=Math.cos(z/2),sz=Math.sin(z/2);
  return [sx*cy*cz+cx*sy*sz,cx*sy*cz-sx*cy*sz,cx*cy*sz+sx*sy*cz,cx*cy*cz-sx*sy*sz];
}
function gltfFor(model,id){
  const chunks=[],views=[],accessors=[],meshes=[],materialNames=Object.keys(pack.materials);let offset=0;
  const accessor=(values,type)=>{
    const a=new Float32Array(values),buffer=Buffer.from(a.buffer),stride={SCALAR:1,VEC3:3,VEC4:4}[type],index=accessors.length;
    views.push({buffer:0,byteOffset:offset,byteLength:buffer.length});chunks.push(buffer);offset+=buffer.length;
    const mins=Array(stride).fill(Infinity),maxs=Array(stride).fill(-Infinity);
    values.forEach((v,i)=>{mins[i%stride]=Math.min(mins[i%stride],v);maxs[i%stride]=Math.max(maxs[i%stride],v);});
    accessors.push({bufferView:views.length-1,componentType:5126,count:values.length/stride,type,min:mins,max:maxs});return index;
  };
  const meshIds=new Map();
  const nodes=model.nodes.map(n=>{
    const node={name:n.name,translation:n.position,rotation:quaternion(n.rotation),scale:n.scale};
    if(n.mesh){const name=n.mesh==='heraldry'?'crest_ashen':n.mesh,key=`${name}:${n.material}`;if(!meshIds.has(key)){
      const geo=pack.meshes[name];meshIds.set(key,meshes.length);meshes.push({name:key,primitives:[{attributes:{POSITION:accessor(geo.positions,'VEC3'),NORMAL:accessor(geo.normals,'VEC3')},material:Math.max(0,materialNames.indexOf(n.material))}]});
    }node.mesh=meshIds.get(key);}return node;
  });
  const nodeIndex=Object.fromEntries(model.nodes.map((n,i)=>[n.name,i])),roots=[];
  model.nodes.forEach((n,i)=>{if(n.parent){const parent=nodes[nodeIndex[n.parent]];parent.children||=[];parent.children.push(i);}else roots.push(i);});
  const animations=[];
  for(const clipName of model.clips){
    const clip=pack.animations[clipName],times=Array.from({length:9},(_,i)=>i/8*clip.duration),samplers=[],channels=[],input=accessor(times,'SCALAR');
    model.nodes.forEach((node,index)=>{
      const name=node.name,isLeg=/^(thigh|horseLeg)[A-Z]+$/.test(name),isArm=/^arm[LR]$/.test(name);
      const affected=(['walk','run','gallop'].includes(clipName)&&(isLeg||isArm))||(clipName==='attack'&&name==='armR')||(clipName==='block'&&name==='armL')||(['shoot','reload'].includes(clipName)&&isArm)||(clipName==='fire'&&name==='beam')||(clipName==='death'&&!node.parent)||(clipName==='idle'&&name==='head')||(clipName==='setup'&&name==='beam');
      if(!affected)return;
      const values=times.flatMap((time,i)=>{const r=[...node.rotation],phase=i/8;
        if(['walk','run','gallop'].includes(clipName))r[0]+=Math.sin(phase*Math.PI*2)*(clip.swing||.55)*(name.endsWith('R')?-1:1);
        if(clipName==='attack')r[0]+=-2.4+Math.sin(phase*Math.PI)*2.8;
        if(clipName==='block')r[0]-=Math.sin(phase*Math.PI)*1.1;
        if(clipName==='shoot')r[0]-=Math.sin(phase*Math.PI)*1.5;
        if(clipName==='reload')r[0]-=.7+Math.sin(phase*Math.PI*4)*.3;
        if(clipName==='fire')r[0]+=Math.sin(phase*Math.PI)*2.2;
        if(clipName==='setup')r[0]+=(1-phase)*.6;
        if(clipName==='death')r[0]+=phase*1.5;
        if(clipName==='idle')r[1]+=Math.sin(phase*Math.PI*2)*.035;
        return quaternion(r);
      });
      samplers.push({input,output:accessor(values,'VEC4'),interpolation:'LINEAR'});channels.push({sampler:samplers.length-1,target:{node:index,path:'rotation'}});
    });
    if(channels.length)animations.push({name:clipName,channels,samplers});
  }
  return{json:{asset:{version:'2.0',generator:'Iron Thrones original articulated low-poly asset authoring pipeline',copyright:'CC0-1.0'},scene:0,scenes:[{nodes:roots}],nodes,meshes,materials:materialNames.map(name=>({name,doubleSided:true,pbrMetallicRoughness:{baseColorFactor:[...[1,3,5].map(i=>parseInt(pack.materials[name].slice(i,i+2),16)/255),1],metallicFactor:['steel','silver','darkSteel'].includes(name)?.55:0,roughnessFactor:.8}})),animations,buffers:[{uri:`${id}.bin`,byteLength:offset}],bufferViews:views,accessors,extras:{campaignTroop:id,crewCountedSeparately:false,houseTintMaterial:'house',front:'+Z',units:'meters'}},binary:Buffer.concat(chunks)};
}
const readme=`# Iron Thrones: Lead the Battle — original 3D asset package\n\nVersion 1. All models, animation curves, heraldry, and sounds in this package were authored for this implementation and are released under CC0-1.0. No third-party model downloads or CDN scripts are required.\n\nUpload the **contents** of this directory to the existing R2 bucket, preserving paths. The runtime URL is battle/v1/pack.json. It is also included with the game as an offline fallback. Set globalThis.IRON_THRONES_BATTLE_ASSET_BASE before entering battle to opt into a hosted mirror. R2 must allow CORS GET from the game origin. A mirror failure falls back to the local pack; failure of both preserves the pending encounter and exposes Retry / Resolve.\n\n## Contents\n- battle/v1/pack.json: complete runtime geometry, articulated node hierarchies, material palette, 12 House emblems, and clip definitions.\n- battle/v1/models/: glTF 2.0 models with external binary buffers and animation tracks, one model for every campaign troop plus the horse. Import these into Blender or any glTF-compatible editor.\n- battle/v1/textures/: editable SVG House standards; runtime shields and banners use matching authored silhouette meshes and House cloth colors.\n- battle/v1/animations/clips.json: clip timing metadata. Each glTF contains playable rotation tracks. Runtime blends the same articulated nodes against simulation action progress.\n- battle/v1/sounds/: original synthesized WAV cues for melee, arrows, engines, and reinforcements.\n- battle/v1/manifest.json: file sizes and SHA-256 checksums.\n\n## Accounting and quality\nEach horse and rider is one campaign troop. Engine crew is visual only and never manufactures troop records. All 15 roster types retain their exact keys, including the legacy siege model. Models are original, stylized low-poly figures: sculpted contour surfaces, articulated limbs, recognisable helmets, equipment and horses. No visible placeholder capsules are used. Engine frames and buildings appropriately use timber/stone beams.\n\n## Rebuild\nRun node game/iron-throne/tools/generate-battle-assets.mjs <output-directory> from the repository. It regenerates the shipped runtime pack as well as this reusable asset bundle. Runtime model/animation source is game/iron-throne/tools/battle-model-source.mjs.\n`;
if(destination){
  const root=path.join(destination,'battle','v1');
  for(const dir of ['models','textures','animations','sounds'])await mkdir(path.join(root,dir),{recursive:true});
  await writeFile(path.join(root,'pack.json'),JSON.stringify(pack));
  const files=['pack.json'];
  for(const [id,model]of Object.entries(pack.models)){
    const {json,binary}=gltfFor(model,id);await writeFile(path.join(root,'models',`${id}.gltf`),JSON.stringify(json));await writeFile(path.join(root,'models',`${id}.bin`),binary);files.push(`models/${id}.gltf`,`models/${id}.bin`);
  }
  for(const [id,house]of Object.entries(pack.houses)){
    const geo=pack.meshes[house.mesh],ps=geo.positions;let paths='';for(let i=0;i<ps.length;i+=9)paths+=`<path d="M${128+ps[i]*70} ${128-ps[i+1]*70}L${128+ps[i+3]*70} ${128-ps[i+4]*70}L${128+ps[i+6]*70} ${128-ps[i+7]*70}Z"/>`;
    await writeFile(path.join(root,'textures',`${id}-heraldry.svg`),`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 256 320"><title>${house.name}</title><path fill="${house.color}" d="M8 8H248V220L128 310L8 220Z"/><g fill="#eee0b4">${paths}</g></svg>`);files.push(`textures/${id}-heraldry.svg`);
  }
  await writeFile(path.join(root,'animations','clips.json'),JSON.stringify(pack.animations,null,2));files.push('animations/clips.json');
  for(const [id,duration,freq]of [['melee',.16,180],['arrow',.19,900],['siege',.8,55],['reinforcement',1.2,330]]){
    const rate=22050,count=Math.round(rate*duration),wav=Buffer.alloc(44+count*2);wav.write('RIFF');wav.writeUInt32LE(36+count*2,4);wav.write('WAVEfmt ',8);wav.writeUInt32LE(16,16);wav.writeUInt16LE(1,20);wav.writeUInt16LE(1,22);wav.writeUInt32LE(rate,24);wav.writeUInt32LE(rate*2,28);wav.writeUInt16LE(2,32);wav.writeUInt16LE(16,34);wav.write('data',36);wav.writeUInt32LE(count*2,40);
    let seed=42;for(let i=0;i<count;i++){seed=(Math.imul(seed,1664525)+1013904223)>>>0;const t=i/rate,noise=seed/4294967296*2-1,envelope=Math.pow(1-i/count,2),sample=id==='reinforcement'?Math.sin(t*freq*Math.PI*2)*(1+.3*Math.sin(t*6)):Math.sin(t*(freq*(1-t/duration*.5))*Math.PI*2)*.4+noise*.6;wav.writeInt16LE(Math.round(sample*envelope*13000),44+i*2);}await writeFile(path.join(root,'sounds',`${id}.wav`),wav);files.push(`sounds/${id}.wav`);
  }
  const manifest={version:1,format:pack.format,license:pack.license,basePath:'battle/v1',models:Object.keys(pack.models),houses:Object.keys(pack.houses),files:[]};
  for(const file of files){const data=await readFile(path.join(root,file));manifest.files.push({path:`battle/v1/${file}`,bytes:data.length,sha256:createHash('sha256').update(data).digest('hex')});}
  await writeFile(path.join(root,'manifest.json'),JSON.stringify(manifest,null,2));await writeFile(path.join(destination,'README.md'),readme);await writeFile(path.join(destination,'LICENSE.txt'),'CC0 1.0 Universal. Original assets authored for Iron Thrones. To the extent possible under law, all copyright and related rights to these assets are waived. https://creativecommons.org/publicdomain/zero/1.0/\n');
  await writeFile(path.join(runtime,'manifest.json'),JSON.stringify({version:1,format:pack.format,license:pack.license,models:Object.keys(pack.models),houses:Object.keys(pack.houses),files:manifest.files.filter(f=>f.path.endsWith('/pack.json'))},null,2));
  console.log(`Generated ${Object.keys(pack.models).length} articulated models, ${Object.keys(pack.houses).length} House emblems, clips, sounds, and manifest in ${destination}`);
}else console.log(`Generated runtime model pack: ${path.join(runtime,'pack.json')}`);
