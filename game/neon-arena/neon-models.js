/* Native, dependency-free renderer for the supplied static glTF 2.0 art pack.
 * Geometry/materials and GPU buffers are shared by URL; entities only own a pose.
 * Authored vertex normals preserve curved surfaces and sharp bevels; the original
 * assets without normals use flat face normals. Unsupported/compressed/textured
 * files fail closed to the game's original art.
 */
(function(root){
'use strict';
const FACTION_WEAPONS=['whisker','clawbreaker','purr90','burrower','squeakshot','thunderpaw'];
const WEAPONS=['pistol','assault','sniper','smg','shotgun','burst','arc','rocket','mortar','punch','harpoon','plasma','laser','mender',...FACTION_WEAPONS];
const FACTION_CHARACTERS=['cat_assault','cat_heavy','cat_scout','cat_marksman','cat_stalker','cat_officer','mouse_rifleman','mouse_runner','mouse_engineer','mouse_sniper','mouse_shieldbearer','mouse_commander'];
const HUMANS=['player_one','player_two','bot_breacher','bot_sentinel','bot_vanguard','bot_flanker','bot_demolisher'];
const UNLOCKABLE_HUMANS=['character_bulwark','character_ronin','character_warden','character_revenant','character_praetorian','character_overlord'];
const HUMAN_HEIGHT=2.39;
const ALIENS=['blue','yellow','red','green','stripe','purple','flyer','climber','rammerBoss','spitterBoss','matriarchBoss','shieldBoss','burrowerBoss','commanderBoss'];
// Boss proportions are already baked into the shipped GLBs. Remove that authoring
// scale before applying the simulation's alienScale, so bosses aren't scaled twice.
const AUTHOR_SCALE={rammerBoss:1.92,spitterBoss:1.85,matriarchBoss:1.88,shieldBoss:1.96,burrowerBoss:1.90,commanderBoss:1.83};
const identity=()=>new Float32Array([1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]);
function multiply(a,b){const out=new Float32Array(16);for(let c=0;c<4;c++)for(let r=0;r<4;r++)for(let k=0;k<4;k++)out[c*4+r]+=a[k*4+r]*b[c*4+k];return out}
function transform(m,p){return [m[0]*p[0]+m[4]*p[1]+m[8]*p[2]+m[12],m[1]*p[0]+m[5]*p[1]+m[9]*p[2]+m[13],m[2]*p[0]+m[6]*p[1]+m[10]*p[2]+m[14]]}
function compose(position=[0,0,0],rotation=[0,0,0],scale=[1,1,1]){
  const [x,y,z]=rotation,cx=Math.cos(x),sx=Math.sin(x),cy=Math.cos(y),sy=Math.sin(y),cz=Math.cos(z),sz=Math.sin(z),m=identity();
  m[0]=(cy*cz+sy*sx*sz)*scale[0];m[1]=cx*sz*scale[0];m[2]=(-sy*cz+cy*sx*sz)*scale[0];
  m[4]=(-cy*sz+sy*sx*cz)*scale[1];m[5]=cx*cz*scale[1];m[6]=(sy*sz+cy*sx*cz)*scale[1];
  m[8]=sy*cx*scale[2];m[9]=-sx*scale[2];m[10]=cy*cx*scale[2];m.set(position,12);return m;
}
function nodeMatrix(n){if(n.matrix)return new Float32Array(n.matrix);const [x,y,z,w]=n.rotation||[0,0,0,1],s=n.scale||[1,1,1],t=n.translation||[0,0,0],m=identity();m[0]=(1-2*y*y-2*z*z)*s[0];m[1]=(2*x*y+2*w*z)*s[0];m[2]=(2*x*z-2*w*y)*s[0];m[4]=(2*x*y-2*w*z)*s[1];m[5]=(1-2*x*x-2*z*z)*s[1];m[6]=(2*y*z+2*w*x)*s[1];m[8]=(2*x*z+2*w*y)*s[2];m[9]=(2*y*z-2*w*x)*s[2];m[10]=(1-2*x*x-2*y*y)*s[2];m.set(t,12);return m}
function pivotMatrix(p,angles,offset=[0,0,0]){const m=compose([0,0,0],angles);const r=transform(m,p);m[12]=p[0]-r[0]+offset[0];m[13]=p[1]-r[1]+offset[1];m[14]=p[2]-r[2]+offset[2];return m}
const unit=v=>Number.isFinite(v)?Math.max(0,Math.min(1,v)):0;
const finite3=v=>Array.isArray(v)&&v.length===3&&v.every(Number.isFinite);
// Model-local socket: +Z points down the barrel. The caller applies its exact
// held/viewmodel matrix, so flashes follow ADS, reload tilt and split-screen.
function weaponMuzzle(model){
  if(finite3(model?.rig?.muzzle))return model.rig.muzzle.slice();
  if(finite3(model?.anchors?.muzzle))return model.anchors.muzzle.slice();
  const b=model?.bounds;if(!finite3(b?.min)||!finite3(b?.max))return [0,0,1];
  return [(b.min[0]+b.max[0])/2,(b.min[1]+b.max[1])/2,b.max[2]];
}
function shotRGB(value){
  if(finite3(value))return value.map(unit);
  if(typeof value==='string'&&/^#[\da-f]{6}$/i.test(value))return [1,3,5].map(i=>parseInt(value.slice(i,i+2),16)/255);
  return [1,.72,.34];
}
function mechanicalCycle(age,start,rise,hold,fall){
  const t=age-start;if(t<0||t>=rise+hold+fall)return 0;
  if(t<rise)return t/rise;if(t<rise+hold)return 1;
  const x=(t-rise-hold)/fall;return 1-x*x*(3-2*x);
}
// An explicit shotAge opts into time-based animation. Cooldown-only callers
// retain their original poses; idle equipped guns never emit a false pulse.
function weaponVisualState(path,state={},now=0){
  const id=path.split('/').pop().replace(/\.glb$/,''),recoil=unit(state.recoil),charge=unit(state.charge),timed=Number.isFinite(state.shotAge)&&state.shotAge>=0;
  const fallback={timed:false,bolt:recoil,pump:recoil,barrel:recoil,pulse:0,emissive:0,energyAngle:Math.sin(now*8)*charge*.03,color:shotRGB(state.shotColor)};
  if(!timed)return fallback;
  const age=state.shotAge,heavy=['shotgun','clawbreaker','thunderpaw','rocket','mortar'].includes(id),precision=['sniper','squeakshot'].includes(id),rapid=['smg','purr90'].includes(id),energy=['arc','plasma','laser','mender','punch'].includes(id),suppressed=id==='burrower';
  const duration=heavy?.10:precision?.085:rapid?.045:energy?.075:.06,pulse=Math.pow(Math.max(0,1-age/duration),2)*(suppressed?.25:1);
  const bolt=mechanicalCycle(age,0,.012,rapid?.008:.02,precision?.12:rapid?.038:.065),pump=mechanicalCycle(age,.045,.045,.035,.17),barrel=mechanicalCycle(age,0,.008,.008,heavy?.12:.075);
  return {timed:true,bolt,pump,barrel,pulse,emissive:Math.min(1.4,charge*.85+unit(state.heat)*.22+pulse*.5),energyAngle:Math.sin(now*8)*(charge+pulse*.35)*.03,color:shotRGB(state.shotColor)};
}
function groupFor(name,center,weapon,mechanical=false){
  if(weapon&&mechanical){if(/^bolt_|^slide_/.test(name))return 'bolt';if(/^pump(?:_|$)/.test(name))return 'pump';if(/^magazine_tube/.test(name))return 'body';if(/^magazine(?:_|$)/.test(name))return 'magazine';return 'body';}
  if(weapon){if(/magazine|mag_glow|ammo_block|cylinder|chamber/.test(name))return 'magazine';if(/barrel|muzzle|slide|hammer|bolt|pump/.test(name))return 'barrel';if(/coil|capacitor|plasma|crystal|core|energy|power_fin/.test(name))return 'energy';return 'body'}
  const explicitSide=/(?:^|_)(L|R)(?:_|$)/.exec(name),side=explicitSide?explicitSide[1]:center[0]<0?'L':'R';
  if(/^ear(?:_|$)/.test(name))return 'ear'+side;
  if(/^wing(?:_|$)/.test(name))return 'wing'+side;
  if(/^(?:glove|foreclaw|claw_tip|upper_arm|forearm|shoulder)(?:_|$)/.test(name))return 'arm'+side;
  // The detailed pack adds muscles, joints and complete arm/jaw meshes. Attach
  // them to the same rigid limb as their armor, rather than leaving stationary
  // skin behind when its limb swings. Match new families by semantic prefix so
  // torso equipment containing similar words keeps its original body grouping.
  if(/upper_leg|lower_leg|thigh|shin|knee|boot|foot|talon/.test(name)||/^(?:calf|ankle|rear_digging_leg)(?:_|$)/.test(name))return 'leg'+side;
  if(/shoulder|upper_arm|elbow|forearm|glove|foreclaw|claw_tip|impact_shoulder/.test(name)||/^arm(?:_|$)/.test(name))return 'arm'+side;
  if(/tail/.test(name))return 'tail';
  if(/helmet|visor|neck_guard|^head|snout|lower_jaw|luminous_eye|beak|crown_horn|front_horn/.test(name)||/^(?:jaw|acid_mouth)(?:_|$)/.test(name))return 'head';
  return 'body';
}
function authoredNormals(normals,positions,matrix){
  if(normals.width!==3||normals.count!==positions.count)throw Error('Invalid GLB normal dimensions');
  const a=[matrix[0],matrix[1],matrix[2]],b=[matrix[4],matrix[5],matrix[6]],c=[matrix[8],matrix[9],matrix[10]],cross=(p,q)=>[p[1]*q[2]-p[2]*q[1],p[2]*q[0]-p[0]*q[2],p[0]*q[1]-p[1]*q[0]],ca=cross(b,c),cb=cross(c,a),cc=cross(a,b),det=a[0]*ca[0]+a[1]*ca[1]+a[2]*ca[2];
  if(!Number.isFinite(det)||Math.abs(det)<1e-10)throw Error('Singular GLB normal transform');
  const out=new Float32Array(normals.data.length);
  for(let i=0;i<normals.data.length;i+=3){const x=normals.data[i],y=normals.data[i+1],z=normals.data[i+2],unit=Math.hypot(x,y,z);if(!Number.isFinite(unit)||Math.abs(unit-1)>.025)throw Error('GLB normals must be nonzero unit vectors');let nx=(ca[0]*x+cb[0]*y+cc[0]*z)/det,ny=(ca[1]*x+cb[1]*y+cc[1]*z)/det,nz=(ca[2]*x+cb[2]*y+cc[2]*z)/det;const length=Math.hypot(nx,ny,nz);if(!Number.isFinite(length)||length<1e-10)throw Error('Invalid transformed GLB normal');out[i]=nx/length;out[i+1]=ny/length;out[i+2]=nz/length;}
  return out;
}
function parseGLB(input,path=''){
  const data=input instanceof ArrayBuffer?input:input.buffer.slice(input.byteOffset,input.byteOffset+input.byteLength),v=new DataView(data);
  if(data.byteLength<20||v.getUint32(0,true)!==0x46546c67||v.getUint32(4,true)!==2||v.getUint32(8,true)!==data.byteLength)throw Error('Invalid GLB header');
  let json,bin;for(let offset=12;offset<data.byteLength;){if(offset+8>data.byteLength)throw Error('Invalid GLB chunk');const length=v.getUint32(offset,true),type=v.getUint32(offset+4,true);offset+=8;if(offset+length>data.byteLength)throw Error('Truncated GLB');if(type===0x4e4f534a)json=JSON.parse(new TextDecoder().decode(new Uint8Array(data,offset,length)).trim());if(type===0x004e4942)bin=new DataView(data,offset,length);offset+=length;}
  if(!json||!bin||json.asset?.version!=='2.0'||json.extensionsRequired?.length||json.images?.length||json.skins?.length)throw Error('Unsupported GLB feature');
  if(json.buffers?.length!==1||json.buffers[0].uri)throw Error('GLB must embed its geometry');
  const types={SCALAR:1,VEC2:2,VEC3:3,VEC4:4},components={5120:[1,'getInt8'],5121:[1,'getUint8'],5122:[2,'getInt16'],5123:[2,'getUint16'],5125:[4,'getUint32'],5126:[4,'getFloat32']};
  function accessor(index){const a=json.accessors?.[index],b=json.bufferViews?.[a?.bufferView],c=components[a?.componentType],width=types[a?.type];if(!a||!b||!c||!width||a.sparse||b.buffer!==0)throw Error('Unsupported GLB accessor');const size=c[0]*width,stride=b.byteStride||size,start=(b.byteOffset||0)+(a.byteOffset||0),end=start+(a.count-1)*stride+size;if(a.count<1||stride<size||end>bin.byteLength||end>(b.byteOffset||0)+b.byteLength)throw Error('Invalid accessor bounds');const out=new Float32Array(a.count*width);for(let i=0;i<a.count;i++)for(let k=0;k<width;k++){let value=bin[c[1]](start+i*stride+k*c[0],true);if(a.normalized&&a.componentType!==5126)value=a.componentType===5120?Math.max(-1,value/127):a.componentType===5122?Math.max(-1,value/32767):value/(a.componentType===5121?255:65535);if(!Number.isFinite(value))throw Error('Non-finite GLB geometry');out[i*width+k]=value;}return {data:out,width,count:a.count};}
  const groups=new Map(),all=[],bounds={min:[Infinity,Infinity,Infinity],max:[-Infinity,-Infinity,-Infinity]},anchors={},weapon=path.startsWith('weapons/'),socketBounds=new Map();let nodeCount=0;
  function visit(index,parent,ancestors){const n=json.nodes?.[index];if(!n||ancestors.has(index))throw Error('Invalid GLB hierarchy');const branch=new Set(ancestors);branch.add(index);const world=multiply(parent,nodeMatrix(n));if(n.mesh!==undefined){const mesh=json.meshes?.[n.mesh];if(!mesh)throw Error('Invalid GLB mesh');nodeCount++;for(const primitive of mesh.primitives){if((primitive.mode??4)!==4||primitive.extensions)throw Error('Only uncompressed GLB triangles are supported');const positions=accessor(primitive.attributes.POSITION);if(positions.width!==3)throw Error('Invalid GLB positions');const normals=primitive.attributes.NORMAL===undefined?null:authoredNormals(accessor(primitive.attributes.NORMAL),positions,world);const indices=primitive.indices===undefined?Array.from({length:positions.count},(_,i)=>i):accessor(primitive.indices).data;if(indices.length%3)throw Error('Invalid GLB triangles');const material=json.materials?.[primitive.material]||{},base=material.pbrMetallicRoughness?.baseColorFactor||[.6,.7,.8,1],emissive=material.emissiveFactor||[0,0,0];if((material.alphaMode||'OPAQUE')!=='OPAQUE'||(base[3]??1)<1)throw Error('Transparent models require a different pass');const name=n.name||mesh.name||'',center=[world[12],world[13],world[14]],key=groupFor(name,center,weapon,json.asset.extras?.neonBreach?.mechanicalParts);if(/^head_\d+$/.test(name))anchors.head=center;if(/^(torso|chest_core)_\d+$/.test(name))anchors.core=center;let group=groups.get(key);if(!group){group={key,vertices:[],center:[],names:[]};groups.set(key,group)}group.center.push(center);group.names.push(name);
      const socket=weapon&&(/muzzle_bore/i.test(name)?'bore':/muzzle/i.test(name)?'muzzle':/barrel/i.test(name)?'barrel':null);if(socket&&!socketBounds.has(socket))socketBounds.set(socket,{min:[Infinity,Infinity,Infinity],max:[-Infinity,-Infinity,-Infinity]});
      for(let i=0;i<indices.length;i+=3){const pts=[];for(let k=0;k<3;k++){const ind=indices[i+k];if(!Number.isInteger(ind)||ind<0||ind>=positions.count)throw Error('Invalid GLB index');const p=transform(world,positions.data.subarray(ind*3,ind*3+3));pts.push(p);for(let j=0;j<3;j++){bounds.min[j]=Math.min(bounds.min[j],p[j]);bounds.max[j]=Math.max(bounds.max[j],p[j]);if(socket){const b=socketBounds.get(socket);b.min[j]=Math.min(b.min[j],p[j]);b.max[j]=Math.max(b.max[j],p[j]);}}}
        const a=pts[0],b=pts[1],c=pts[2],ab=b.map((q,j)=>q-a[j]),ac=c.map((q,j)=>q-a[j]),normal=[ab[1]*ac[2]-ab[2]*ac[1],ab[2]*ac[0]-ab[0]*ac[2],ab[0]*ac[1]-ab[1]*ac[0]],len=Math.hypot(...normal);if(len<1e-9)continue;for(let j=0;j<3;j++)normal[j]/=len;for(let k=0;k<3;k++){const n=normals?normals.subarray(indices[i+k]*3,indices[i+k]*3+3):normal,vertex=[...pts[k],...n,...base.slice(0,3),...emissive];group.vertices.push(...vertex);all.push(...vertex);}
      }
    }}for(const child of n.children||[])visit(child,world,branch);}
  for(const n of json.scenes?.[json.scene||0]?.nodes||[])visit(n,identity(),new Set());if(!all.length||all.length>2e6)throw Error('Empty or oversized GLB');
  const height=bounds.max[1]-bounds.min[1],kind=path.replace(/^characters\/alien_|\.glb$/g,''),authorScale=AUTHOR_SCALE[kind]||1;
  const authoredRig=json.asset.extras?.neonBreach,rig=authoredRig&&Number.isFinite(authoredRig.bodyHeight)&&authoredRig.bodyHeight>0?authoredRig:null;
  const anchor=(key,fallback)=>Array.isArray(rig?.[key])&&rig[key].length===3&&rig[key].every(Number.isFinite)?rig[key]:fallback;
  if(rig){anchors.head=anchor('aimHead',anchors.head);anchors.core=anchor('aimBody',anchors.core);}
  const muzzle=socketBounds.get('bore')||socketBounds.get('muzzle')||socketBounds.get('barrel');if(muzzle)anchors.muzzle=[(muzzle.min[0]+muzzle.max[0])/2,(muzzle.min[1]+muzzle.max[1])/2,muzzle.max[2]];
  const parts=[...groups.values()].map(g=>{const c=g.center.reduce((s,p)=>s.map((q,i)=>q+p[i]),[0,0,0]).map(q=>q/g.center.length),side=g.key.endsWith('L')?-1:1;
    const pivot=rig&&g.key.startsWith('leg')?anchor('hip',[.32,1.10,0]).map((v,i)=>i===0?Math.abs(v)*side:v):rig&&g.key.startsWith('arm')?anchor('shoulder',[.62,1.78,.02]).map((v,i)=>i===0?Math.abs(v)*side:v):rig&&g.key==='head'?anchor('headPivot',[0,1.94,.08]):g.key.startsWith('leg')?[side*(weapon?0:.25)*authorScale,(path.includes('/alien_')?.70:.84)*authorScale,0]:g.key.startsWith('arm')?[side*.42*authorScale,(path.includes('/alien_')?1.02:1.48)*authorScale,.02]:g.key.startsWith('wing')?[side*.40*authorScale,1.02*authorScale,-.05]:g.key==='head'?[0,height*.72,.08]:g.key==='tail'?anchor('tailPivot',[0,.8*authorScale,-.6*authorScale]):g.key.startsWith('ear')?anchor('earPivot',[.22,2.32,0]).map((v,i)=>i===0?Math.abs(v)*side:v):c;return {key:g.key,pivot,vertices:new Float32Array(g.vertices),names:g.names};});
  return {path,bounds,height,bodyHeight:rig?.bodyHeight||height,rig:authoredRig||{},authorScale,weapon,nodeCount,anchors,parts,vertices:new Float32Array(all),vertexCount:all.length/12};
}
function actorFit(model,actor,eyeHeight=1.86,height=HUMAN_HEIGHT){
  if(!actor.alien){const scale=height/(model.bodyHeight||model.height);return {scale:[scale,scale,scale],offset:[0,-model.bounds.min[1]*scale,0]};}
  const gameplayScale=actor.alienScale||1,scale=gameplayScale/model.authorScale,head=model.anchors.head||[0,.97*model.authorScale,.85*model.authorScale],headRadius=(actor.r||.6)*1.10*.62;
  // Alien eyeY is a legacy targeting reference, not a height-scaled human camera.
  // Fit the head to that existing sphere while keeping the soles on the support
  // surface. Establish that legacy fit before applying the arena's visual size
  // multiplier, so larger raptors grow vertically as well as horizontally.
  const targetHeight=Math.max(.30,eyeHeight-.45*gameplayScale+headRadius*.18),scaleY=targetHeight/Math.max(.1,head[1]-model.bounds.min[1]);
  const visualScale=Number.isFinite(actor.alienVisualScale)&&actor.alienVisualScale>0?actor.alienVisualScale:1;
  return {scale:[scale*visualScale,scaleY*visualScale,scale*visualScale],offset:[-head[0]*scale*visualScale,-model.bounds.min[1]*scaleY*visualScale,-head[2]*scale*visualScale]};
}
class ModelCache{
  constructor(options={}){this.resolve=options.resolve||((p)=>root.NeonAssets?.url?.(p)||'');this.fallback=options.fallback||((p)=>root.NeonAssets?.localModelURL?.(p)||root.NeonAssets?.localCharacterURL?.(p)||'');this.fetch=options.fetch||root.fetch?.bind(root);this.now=options.now||(()=>Date.now());this.maxConcurrent=options.maxConcurrent||2;this.records=new Map();this.queue=[];this.active=0;this.stats={requests:0,parsed:0,failed:0};}
  get(path,priority=false){const url=this.resolve(path);if(!url||!this.fetch)return null;let r=this.records.get(url);if(r?.model)return r.model;if(r?.state==='failed'&&this.now()-r.failedAt>60000){this.records.delete(url);r=null;}if(!r){r={path,url,state:'queued',model:null};this.records.set(url,r);priority?this.queue.unshift(r):this.queue.push(r);this.pump();}return null;}
  pump(){while(this.active<this.maxConcurrent&&this.queue.length){const r=this.queue.shift();r.state='loading';this.active++;this.stats.requests++;
    const read=async url=>{const abort=typeof AbortController!=='undefined'?new AbortController():null,timer=abort?setTimeout(()=>abort.abort(),12000):null;try{const response=await this.fetch(url,{mode:'cors',credentials:'omit',signal:abort?.signal});if(!response.ok)throw Error('Model HTTP '+response.status);return parseGLB(await response.arrayBuffer(),r.path);}finally{if(timer)clearTimeout(timer);}};
    Promise.resolve().then(async()=>{try{return await read(r.url);}catch(error){const local=this.fallback(r.path);if(!local||local===r.url)throw error;return read(local);}}).then(model=>{r.model=model;r.state='ready';this.stats.parsed++;}).catch(error=>{r.state='failed';r.failedAt=this.now();r.error=String(error?.message||error);this.stats.failed++;}).finally(()=>{this.active--;this.pump();});}}
  status(){return {active:this.active,queued:this.queue.length,...this.stats};}
  async preload(paths,options={}){
    const entries=[...new Set(paths)].map(path=>{const url=this.resolve(path);if(!url)throw Error('Model hosting is not configured for '+path);if(options.retry&&this.records.get(url)?.state==='failed')this.records.delete(url);this.get(path);return this.records.get(url);});
    const started=this.now();
    while(true){const failed=entries.find(r=>r.state==='failed');if(failed)throw Error('Unable to load '+failed.path+': '+failed.error);const ready=entries.filter(r=>r.state==='ready').length;options.onProgress?.(ready,entries.length);if(ready===entries.length)return entries.map(r=>r.model);if(this.now()-started>(options.timeout||120000))throw Error('Model loading timed out. Retry to continue.');await new Promise(resolve=>setTimeout(resolve,80));}
  }
}
function shader(gl,type,source){const s=gl.createShader(type);gl.shaderSource(s,source);gl.compileShader(s);if(!gl.getShaderParameter(s,gl.COMPILE_STATUS))throw Error(gl.getShaderInfoLog(s));return s}
const sharedCache=new ModelCache();
function createRenderer(gl,options={}){
  const cache=options.cache||(options.fetch||options.resolve?new ModelCache(options):sharedCache),gpu=new Map(),poses=new WeakMap(),corpses=[];
  const program=gl.createProgram(),vs=shader(gl,gl.VERTEX_SHADER,'attribute vec3 aPosition;attribute vec3 aNormal;attribute vec3 aColor;attribute vec3 aEmissive;uniform mat4 uModel;uniform vec3 uCamera;uniform vec3 uRight;uniform vec3 uUp;uniform vec3 uForward;uniform float uAspect;varying vec3 vColor;varying vec3 vNormal;varying vec3 vEmissive;varying vec3 vViewDir;varying vec3 vLocalPosition;void main(){vec3 world=(uModel*vec4(aPosition,1.0)).xyz;vec3 delta=world-uCamera;vec3 camera=vec3(dot(delta,uRight),dot(delta,uUp),dot(delta,uForward));float near=0.055;float far=180.0;float A=(far+near)/(far-near);float B=-2.0*far*near/(far-near);gl_Position=vec4(camera.x*1.44*uAspect,camera.y*1.44,A*camera.z+B,camera.z);vColor=aColor;vNormal=normalize(cross(uModel[1].xyz,uModel[2].xyz)*aNormal.x+cross(uModel[2].xyz,uModel[0].xyz)*aNormal.y+cross(uModel[0].xyz,uModel[1].xyz)*aNormal.z);vEmissive=aEmissive;vViewDir=-delta;vLocalPosition=aPosition;}');
  const fs=shader(gl,gl.FRAGMENT_SHADER,'precision mediump float;varying vec3 vColor;varying vec3 vNormal;varying vec3 vEmissive;uniform float uFlash;uniform float uShotPulse;uniform float uEmissiveBoost;uniform vec3 uShotOrigin;uniform vec3 uShotColor;uniform float uAmbient;uniform float uExposure;uniform vec3 uSunColor;uniform vec3 uSunDirection;uniform vec3 uTeamTint;uniform float uTeamTintAmount;uniform float uAllyHighlight;varying vec3 vViewDir;varying vec3 vLocalPosition;void main(){float sun=max(0.0,dot(normalize(vNormal),normalize(uSunDirection)));float accent=step(0.001,dot(vEmissive,vEmissive))*uTeamTintAmount;vec3 base=mix(vColor,uTeamTint,accent*0.78);vec3 glow=mix(vEmissive,uTeamTint*length(vEmissive),accent);vec3 lit=base*(vec3(uAmbient)+uSunColor*0.44*sun)*uExposure+glow*(0.65+uEmissiveBoost);if(uShotPulse>0.001){float reach=max(0.0,1.0-length(vLocalPosition-uShotOrigin)*1.25);lit+=uShotColor*uShotPulse*reach*0.72;}float ally=clamp(uAllyHighlight,0.0,1.0);float rim=pow(1.0-abs(dot(normalize(vNormal),normalize(vViewDir))),2.0);vec3 friendly=vec3(0.08,0.9,1.0);lit=mix(lit,friendly,ally*0.22)+friendly*rim*ally*0.5;gl_FragColor=vec4(mix(lit,vec3(0.62,0.93,1.0),uFlash),1.0);}');
  gl.attachShader(program,vs);gl.attachShader(program,fs);gl.linkProgram(program);if(!gl.getProgramParameter(program,gl.LINK_STATUS))throw Error(gl.getProgramInfoLog(program));gl.deleteShader(vs);gl.deleteShader(fs);
  const attr=['aPosition','aNormal','aColor','aEmissive'].map(n=>gl.getAttribLocation(program,n)),u=Object.fromEntries(['uModel','uCamera','uRight','uUp','uForward','uAspect','uFlash','uShotPulse','uEmissiveBoost','uShotOrigin','uShotColor','uTeamTint','uTeamTintAmount','uAllyHighlight','uAmbient','uExposure','uSunColor','uSunDirection'].map(n=>[n,gl.getUniformLocation(program,n)]));
  let drawCalls=0,animatedActors=0,actorCount=0,lastNow=0;
  function buffer(vertices){const b=gl.createBuffer();gl.bindBuffer(gl.ARRAY_BUFFER,b);gl.bufferData(gl.ARRAY_BUFFER,vertices,gl.STATIC_DRAW);return {buffer:b,count:vertices.length/12};}
  function resource(model){let r=gpu.get(model);if(!r){r={full:buffer(model.vertices),parts:model.parts.map(p=>({...p,...buffer(p.vertices)}))};gpu.set(model,r);}return r;}
  function beginView(camera,now=performance.now()/1000,lighting=null){lastNow=now;drawCalls=animatedActors=actorCount=0;gl.useProgram(program);for(const [name,val] of [['uCamera',camera.position],['uRight',camera.right],['uUp',camera.up],['uForward',camera.forward]])gl.uniform3fv(u[name],val);gl.uniform1f(u.uAspect,camera.aspect);gl.uniform1f(u.uAllyHighlight,0);gl.uniform1f(u.uShotPulse,0);gl.uniform1f(u.uEmissiveBoost,0);gl.uniform3fv(u.uShotOrigin,[0,0,0]);gl.uniform3fv(u.uShotColor,[1,.72,.34]);gl.uniform1f(u.uAmbient,lighting?.ambient??.56);gl.uniform1f(u.uExposure,lighting?.exposure??1);gl.uniform3fv(u.uSunColor,lighting?.sunColor||[1,1,1]);gl.uniform3fv(u.uSunDirection,lighting?.sunDirection||[-.45,.82,.38]);gl.enable(gl.DEPTH_TEST);gl.depthFunc(gl.LEQUAL);gl.disable(gl.BLEND);gl.disable(gl.CULL_FACE);}
  function drawBuffer(b,m,flash){gl.uniformMatrix4fv(u.uModel,false,m);gl.uniform1f(u.uFlash,flash);gl.bindBuffer(gl.ARRAY_BUFFER,b.buffer);for(let i=0;i<attr.length;i++){gl.enableVertexAttribArray(attr[i]);gl.vertexAttribPointer(attr[i],3,gl.FLOAT,false,48,i*12);}gl.drawArrays(gl.TRIANGLES,0,b.count);drawCalls++;}
  function actorPose(actor,now){let p=poses.get(actor);if(!p){p={hp:actor.hp,armor:actor.armor,yaw:actor.faceYaw??actor.yaw??0,now,ground:actor.onGround,flashUntil:0,landUntil:0};poses.set(actor,p);}const dt=Math.min(.08,Math.max(0,now-p.now));if(actor.hp<p.hp||actor.armor<p.armor)p.flashUntil=now+.13;if(actor.onGround&&!p.ground)p.landUntil=now+.13;const yaw=actor.faceYaw??actor.yaw??0,delta=Math.atan2(Math.sin(yaw-p.yaw),Math.cos(yaw-p.yaw));p.yaw+=delta*Math.min(1,dt*18);p.now=now;p.hp=actor.hp;p.armor=actor.armor;p.ground=actor.onGround;return p;}
  function actorPart(part,actor,p,now,model){const key=part.key,side=key.endsWith('L')?-1:1,speed=Math.hypot(actor.vx||0,actor.vz||0),moving=Math.min(1,speed/7),walk=(actor.walk||now*speed*2.6)*(actor.alien?.20:.28),stride=Math.sin(walk+(side<0?Math.PI:0)),jump=!actor.onGround&&!actor.flying,attack=Math.min(1,(actor.attackAnim||0)/.28),fire=Math.min(1,(actor.cool||0)/.12),reload=actor.reload>0?Math.sin(Math.PI*Math.max(0,Math.min(1,1-actor.reload/(actor.reloadTotal||2)))):0,slide=actor.alien?0:(actor.slidePose||0);let angles=[0,0,0],offset=[0,0,0];
    if(key.startsWith('leg')){angles[0]=actor.climbing?stride*.65:jump?-.26+side*.14:stride*.55*moving;angles[0]=angles[0]*(1-slide)+(side<0?-.62:.78)*slide;angles[2]=slide*side*.08;}
    if(key.startsWith('arm')){angles[0]=model.path.includes('/alien_')?(actor.climbing?-1.35+stride*.5:-attack*.9-stride*.3*moving):((model.rig?.holdPose?0:-.85)-fire*.13+reload*.8-stride*.12*moving);angles[2]=reload*side*.23+slide*side*.10;angles[0]-=slide*.20;}
    if(key.startsWith('wing'))angles[2]=side*(Math.sin((actor.tailPhase||now*7)*1.8)*.38+.08);
    if(key==='tail'){angles[1]=Math.sin((actor.tailPhase||now*3)+walk*.15)*(.12+moving*.15);angles[0]=-slide*.45;}
    if(key.startsWith('ear'))angles[2]=side*Math.sin(now*2.3+side)*.045+side*moving*.035;
    if(key==='head')angles[0]=actor.bossState==='windup'?.24+Math.sin(now*20)*.035:-attack*.15+(actor.stagger>0?Math.sin(now*24)*.08:0);
    if(key==='body'&&actor.chargeTimer>0)offset[1]=Math.sin(now*12)*.025*model.authorScale;if(key==='head'){angles[0]-=slide*.12;angles[0]-=Math.max(-.50,Math.min(.50,actor.pitch||0));angles[1]=Math.max(-.42,Math.min(.42,Math.atan2(Math.sin((actor.yaw??p.yaw)-p.yaw),Math.cos((actor.yaw??p.yaw)-p.yaw))));}if(key==='body'&&!actor.alien){const hit=Math.max(0,(actor.hitReactUntil||0)-now)/.18;angles[0]+=Math.min(1,hit)*.06;angles[1]+=(actor.hitReactYaw||0)*Math.min(1,hit)*.025;}
    if(key.startsWith('ear')){const head=model.parts.find(p=>p.key==='head'),headMatrix=head?actorPart(head,actor,p,now,model):identity();return multiply(headMatrix,pivotMatrix(part.pivot,angles,offset));}
    return pivotMatrix(part.pivot,angles,offset);
  }
  function drawActor(path,actor,options={}){
    const model=cache.get(path);if(!model)return false;const now=options.now??lastNow,p=actorPose(actor,now),r=resource(model),fit=actorFit(model,actor,options.eyeHeight||1.86,options.height||HUMAN_HEIGHT),speed=Math.hypot(actor.vx||0,actor.vz||0),death=options.deathProgress||0,slide=actor.alien?0:(actor.slidePose||0),crouch=actor.alien?0:(actor.crouchPose??(actor.crouching||actor.crouch?1:0)),landing=Math.max(0,p.landUntil-now)/.13,windup=actor.bossState==='windup',lean=(actor.climbing?-.12:windup?.07:actor.bossState==='charge'?-.08:0)+(actor.stagger>0?Math.sin(now*26)*.022:0),bob=actor.onGround?Math.abs(Math.sin((actor.walk||now*speed*2.6)*(actor.alien?.20:.28)))*Math.min(.045,speed*.002):0;
    const matrix=multiply(compose([actor.x,actor.y-(options.eyeHeight||1.86)+bob-landing*.045-death*.18,actor.z],[lean+death*1.35+slide*.11,p.yaw,0]),compose(fit.offset,[0,0,0],[fit.scale[0],fit.scale[1]*(1-landing*.035)*(1-Math.max(slide,crouch)*.27),fit.scale[2]])),flash=Math.max(0,(p.flashUntil-now)/.13)*.55+(actor.shieldFlash>0?.18:0),animate=!!options.animate&&animatedActors<(options.animationBudget??12);
    gl.uniform1f(u.uShotPulse,0);gl.uniform1f(u.uEmissiveBoost,0);gl.uniform1f(u.uAllyHighlight,!actor.alien&&options.allyHighlight?(model.rig?.holdPose?.35:1):0);
    const tint=!actor.alien?options.teamTint:null;gl.uniform3fv(u.uTeamTint,tint||[1,1,1]);gl.uniform1f(u.uTeamTintAmount,tint?1:0);
    if(animate){animatedActors++;for(const part of r.parts)drawBuffer(part,multiply(matrix,actorPart(part,actor,p,now,model)),flash);}else drawBuffer(r.full,matrix,flash);actorCount++;return true;
  }
  function drawWeapon(path,matrix,state={},foreground=false){
    const model=cache.get(path,true);if(!model)return false;const r=resource(model),shot=weaponVisualState(path,state,lastNow);
    gl.uniform1f(u.uAllyHighlight,0);gl.uniform3fv(u.uTeamTint,state.finishTint||[1,1,1]);gl.uniform1f(u.uTeamTintAmount,state.finishTint?1:0);
    gl.uniform1f(u.uShotPulse,shot.pulse);gl.uniform1f(u.uEmissiveBoost,shot.emissive);gl.uniform3fv(u.uShotOrigin,weaponMuzzle(model));gl.uniform3fv(u.uShotColor,shot.color);
    if(foreground)gl.depthRange(0,.12);
    for(const part of r.parts){let angles=[0,0,0],offset=[0,0,0];
      if(part.key==='magazine'&&state.reload>0){const t=Math.max(0,Math.min(1,state.reload));offset[1]=-Math.sin(Math.PI*t)*.36;if(model.rig?.mechanicalParts){offset[0]=Math.sin(Math.PI*t)*.10;angles[2]=Math.sin(Math.PI*t)*.22;if(/purr90/.test(path))offset[1]*=-.65;}if(path.includes('pistol'))angles[2]=Math.sin(Math.PI*state.reload)*.8;}
      if(part.key==='barrel')offset[2]=-shot.barrel*.10;if(part.key==='bolt')offset[2]=-shot.bolt*.12;if(part.key==='pump')offset[2]=-shot.pump*(shot.timed?.16:.11);if(part.key==='energy')angles[2]=shot.energyAngle;
      drawBuffer(part,multiply(matrix,pivotMatrix(part.pivot,angles,offset)),0);
    }
    if(foreground)gl.depthRange(0,1);return true;
  }
  function rememberDeath(path,actor){if(!cache.get(path))return;corpses.push({path,actor:{...actor,onGround:true,vx:0,vz:0},time:performance.now()/1000});while(corpses.length>12)corpses.shift();}
  function recentDeaths(now=lastNow){while(corpses.length&&now-corpses[0].time>.62)corpses.shift();return corpses;}
  function dispose(){for(const r of gpu.values()){gl.deleteBuffer(r.full.buffer);for(const p of r.parts)gl.deleteBuffer(p.buffer);}gpu.clear();corpses.length=0;gl.deleteProgram(program);}
  return {cache,beginView,drawActor,drawWeapon,rememberDeath,recentDeaths,get:(p)=>cache.get(p),preload:(paths,opts)=>cache.preload(paths,opts),stats:()=>({drawCalls,animatedActors,actorCount,gpuModels:gpu.size,...cache.status()}),dispose};
}
// Menu portrait uses the actual model, shares downloads with the arena, and stops
// drawing when hidden. It is intentionally independent of gameplay and HUD state.
function initMenuCharacter(){
  const canvas=root.document?.getElementById('menuCharacter');if(!canvas||canvas.dataset.modelPreview)return;canvas.dataset.modelPreview='true';
  const gl=canvas.getContext('webgl',{alpha:true,antialias:true,premultipliedAlpha:false});if(!gl)return;let renderer;try{renderer=createRenderer(gl);}catch{return;}
  let previous=0,lastLoad=0,drag=null;const actor={x:0,y:1.86,z:0,yaw:Math.PI+.25,faceYaw:Math.PI+.25,vx:0,vz:0,onGround:true,hp:100,armor:50,walk:0};
  canvas.style.touchAction='pan-y';canvas.style.cursor='grab';canvas.setAttribute('aria-label','3D character preview. Drag left or right to rotate.');
  canvas.addEventListener('pointerdown',event=>{drag={id:event.pointerId,x:event.clientX,yaw:actor.yaw};canvas.setPointerCapture?.(event.pointerId);canvas.style.cursor='grabbing';});
  canvas.addEventListener('pointermove',event=>{if(drag&&event.pointerId===drag.id){actor.yaw=actor.faceYaw=drag.yaw+(event.clientX-drag.x)*.012;}});
  const endDrag=()=>{drag=null;canvas.style.cursor='grab';};canvas.addEventListener('pointerup',endDrag);canvas.addEventListener('pointercancel',endDrag);
  root.document.addEventListener('neon-war-selection',()=>{lastLoad=0;});
  function frame(now){root.requestAnimationFrame(frame);const menu=root.document.getElementById('menu');if(root.document.hidden||!menu||menu.style.display==='none'||now-previous<42)return;previous=now;
    const choice=root.NeonWarUI?.selection?.(),operator=choice&&root.NeonFactions?.characterById?.[choice.character],gun=choice&&root.NeonFactions?.weaponById?.[choice.weapon];
    const selected=operator?.model||root.NeonSurvivalProgress?.store?.getEquippedCharacterModelPath?.()||'characters/player_one.glb',weaponPath=gun?.model||'weapons/assault.glb';
    const caption=canvas.parentElement?.querySelector('.menuHeroCaption'),name=operator?.name||root.NeonSurvivalProgress?.store?.getEquippedCharacter?.()?.name||'Breacher';if(caption&&caption.textContent!==name.toUpperCase()+' // READY')caption.textContent=name.toUpperCase()+' // READY';
    if(now-lastLoad>500){if(renderer.get(selected)){canvas.classList.add('model-ready');canvas.parentElement?.classList.add('model-ready');}renderer.get(weaponPath);lastLoad=now;}if(!canvas.offsetWidth)return;
    const width=Math.round(canvas.clientWidth*Math.min(1.5,root.devicePixelRatio||1)),height=Math.round(canvas.clientHeight*Math.min(1.5,root.devicePixelRatio||1));if(width<=0||height<=0)return;if(canvas.width!==width||canvas.height!==height){canvas.width=width;canvas.height=height;}gl.viewport(0,0,width,height);gl.clearColor(0,0,0,0);gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);renderer.beginView({position:[0,1.13,-3.55],right:[1,0,0],up:[0,1,0],forward:[0,0,1],aspect:height/width},now/1000);
    if(renderer.drawActor(selected,actor,{animate:true,animationBudget:1,now:now/1000})){canvas.classList.add('model-ready');canvas.parentElement?.classList.add('model-ready');const sy=Math.sin(actor.yaw),cy=Math.cos(actor.yaw),forward=operator?.43:.48,side=operator?.32:.34,matrix=compose([sy*forward+cy*side,operator?1.43:1.32,cy*forward-sy*side],[0,actor.yaw,0],[.70,.70,.70]);renderer.drawWeapon(weaponPath,matrix);}
  }
  root.requestAnimationFrame(frame);
}
root.NeonModels={WEAPONS,FACTION_WEAPONS,FACTION_CHARACTERS,HUMANS,UNLOCKABLE_HUMANS,HUMAN_HEIGHT,ALIENS,AUTHOR_SCALE,parseGLB,actorFit,ModelCache,sharedCache,createRenderer,compose,multiply,transform,pivotMatrix,weaponMuzzle,weaponVisualState,initMenuCharacter};
if(typeof module!=='undefined'&&module.exports)module.exports=root.NeonModels;
if(root.document){if(root.document.readyState==='loading')root.document.addEventListener('DOMContentLoaded',initMenuCharacter,{once:true});else initMenuCharacter();}
})(typeof window!=='undefined'?window:globalThis);
