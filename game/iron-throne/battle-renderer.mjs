import {loadBattleAssets,releaseBattleAssets} from './battle-assets.mjs';
import {battleGroundHeight,battlefieldClimate,TERRAIN_STEP} from './battle-terrain.mjs';
import {createBattleSky} from './battle-sky.mjs';
import {drawCastleItems} from './battle-castle-view.mjs';
export {battleGroundHeight};

const TAU=Math.PI*2;
const clamp=(x,a,b)=>Math.max(a,Math.min(b,x));
const color=hex=>[1,3,5].map(i=>parseInt((hex||'#aaaaaa').slice(i,i+2),16)/255);
const ID=()=>new Float32Array([1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]);
function multiply(a,b){const m=new Float32Array(16);for(let c=0;c<4;c++)for(let r=0;r<4;r++)m[c*4+r]=a[r]*b[c*4]+a[4+r]*b[c*4+1]+a[8+r]*b[c*4+2]+a[12+r]*b[c*4+3];return m;}
function transform(p=[0,0,0],r=[0,0,0],s=[1,1,1]){
  const [x,y,z]=r,cx=Math.cos(x),sx=Math.sin(x),cy=Math.cos(y),sy=Math.sin(y),cz=Math.cos(z),sz=Math.sin(z);
  const m=new Float32Array([cy*cz*s[0],(sx*sy*cz+cx*sz)*s[0],(-cx*sy*cz+sx*sz)*s[0],0,-cy*sz*s[1],(-sx*sy*sz+cx*cz)*s[1],(cx*sy*sz+sx*cz)*s[1],0,sy*s[2],-sx*cy*s[2],cx*cy*s[2],0,...p,1]);return m;
}
function cross(a,b){return[a[1]*b[2]-a[2]*b[1],a[2]*b[0]-a[0]*b[2],a[0]*b[1]-a[1]*b[0]];}
function normalize(a){const n=Math.hypot(...a)||1;return a.map(x=>x/n);}
function rayBox(eye,ray,lo,hi){
  let start=0,end=Infinity;
  for(let i=0;i<3;i++){
    if(Math.abs(ray[i])<1e-7){if(eye[i]<lo[i]||eye[i]>hi[i])return Infinity;continue;}
    const a=(lo[i]-eye[i])/ray[i],b=(hi[i]-eye[i])/ray[i];
    start=Math.max(start,Math.min(a,b));end=Math.min(end,Math.max(a,b));
    if(end<start)return Infinity;
  }
  return start;
}
const dot=(a,b)=>a.reduce((n,x,i)=>n+x*b[i],0);
const mix=(a,b,t)=>a+(b-a)*clamp(t,0,1);
const hash=value=>{let n=2166136261;for(const c of String(value))n=Math.imul(n^c.charCodeAt(0),16777619);return n>>>0;};
const CORPSE_LIMIT=160,DETAILED_CORPSE_LIMIT=24,BLOOD_LIMIT=96,BLOOD_DISTANCE=32,BLOOD_LIFETIME=.85;
const carriesPolearm=u=>!u.sidearm&&(['spear','lance'].includes(u.weapon)||!u.weapon&&['spearman','cavalry','knight'].includes(u.type));
export function polearmPitch(u,pose=battleActionPose(u)){
  if(u.status==='dead')return .27;
  if(u.action?.kind==='attack'||u.braced)return pose.shaft;
  return u.mounted?1.20:.34;
}

// The pole stays in a plane outside the helmet. Wrist roll and animated arm
// twist cannot sweep the shaft across the face during recovery or a death fall.
export function polearmFrame(rider,hand,head,pitch){
  const grip=new Float32Array(rider),right=normalize([rider[0],rider[1],rider[2]]);
  const lateral=(hand[12]-head[12])*right[0]+(hand[13]-head[13])*right[1]+(hand[14]-head[14])*right[2];
  const clearance=Math.max(0,.43-lateral);
  for(let i=0;i<3;i++)grip[12+i]=hand[12+i]+right[i]*clearance;
  return multiply(grip,transform([0,0,0],[pitch,0,0]));
}

// The rig faces +Z. Weapon meshes point up their local Y axis, so the hand mount
// must counter the arm rotation to put the actual tip forward at impact.
export function battleActionPose(unit){
  const a=unit.action,weapon=unit.sidearm?'sword':unit.weapon,thrust=['spear','lance'].includes(weapon);
  let arm=-.25,forearm=-.25,shaft=thrust?1.25:.12,twist=0,stage='idle',extension=0;
  if(a?.kind==='attack'){
    const windup=Math.max(.04,a.windup??a.duration*.4),elapsed=a.elapsed||0,contactWindow=Math.min(.14,windup*.45),preparation=clamp(elapsed/(windup-contactWindow),0,1);
    if(elapsed<windup-contactWindow){stage='windup';arm=mix(-.25,thrust?-.65:-2.15,preparation);forearm=mix(-.25,-.65,preparation);shaft=mix(thrust?1.25:.12,thrust?1.45:-.8,preparation);twist=-.25*preparation;extension=thrust?-.12*preparation:0;}
    else if(elapsed<=windup+.09){const t=clamp((elapsed-windup+contactWindow)/contactWindow,0,1);stage='strike';arm=mix(thrust?-.65:-2.15,thrust?-1.4:-.9,t);forearm=mix(-.65,-.08,t);shaft=mix(thrust?1.45:-.8,thrust?Math.PI/2:1.8,t);twist=mix(-.25,.22,t);extension=thrust?mix(-.12,.28,t):0;}
    else {const t=clamp((elapsed-windup-.09)/Math.max(.1,(a.recovery??a.duration-windup)-.09),0,1);stage='recovery';arm=mix(thrust?-1.4:-.9,-.25,t);forearm=mix(-.08,-.25,t);shaft=mix(thrust?Math.PI/2:1.8,thrust?1.25:.12,t);twist=mix(.22,0,t);extension=thrust?mix(.28,0,t):0;}
  }else if(unit.braced){arm=-.85;forearm=-.25;shaft=Math.PI/2;}
  return{stage,arm,forearm,weapon:shaft-arm-forearm,shaft,twist,extension};
}
function lookAt(eye,target){const z=normalize(eye.map((v,i)=>v-target[i])),x=normalize(cross([0,1,0],z)),y=cross(z,x);return new Float32Array([x[0],y[0],z[0],0,x[1],y[1],z[1],0,x[2],y[2],z[2],0,-dot(x,eye),-dot(y,eye),-dot(z,eye),1]);}
function perspective(aspect){const f=1/Math.tan(Math.PI/6),near=.15,far=420;return new Float32Array([f/aspect,0,0,0,0,f,0,0,0,0,(far+near)/(near-far),-1,0,0,2*far*near/(near-far),0]);}
function shader(gl,type,source){const s=gl.createShader(type);gl.shaderSource(s,source);gl.compileShader(s);if(!gl.getShaderParameter(s,gl.COMPILE_STATUS)){const message=gl.getShaderInfoLog(s);gl.deleteShader(s);throw new Error(message);}return s;}
function surfaceNoise(gl,seed){
  const size=128,bytes=new Uint8Array(size*size);
  let state=hash(seed);
  for(let i=0;i<bytes.length;i++){state=(Math.imul(state,1664525)+1013904223)>>>0;bytes[i]=state>>>24;}
  const texture=gl.createTexture();gl.bindTexture(gl.TEXTURE_2D,texture);
  gl.texImage2D(gl.TEXTURE_2D,0,gl.LUMINANCE,size,size,0,gl.LUMINANCE,gl.UNSIGNED_BYTE,bytes);
  gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MAG_FILTER,gl.LINEAR);gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MIN_FILTER,gl.LINEAR_MIPMAP_LINEAR);
  gl.generateMipmap(gl.TEXTURE_2D);return texture;
}
function program(gl,instanced){
  const vertex=shader(gl,gl.VERTEX_SHADER,`precision highp float;
  attribute vec3 aPosition;attribute vec3 aNormal;
  ${instanced?'attribute vec4 i0;attribute vec4 i1;attribute vec4 i2;attribute vec4 i3;attribute vec3 iColor;':'uniform mat4 uModel;uniform vec3 uColor;'}
  uniform mat4 uViewProjection;uniform vec3 uEye,uSun;varying vec3 vColor,vWorld,vNormal;varying float vFog;
  void main(){mat4 m=${instanced?'mat4(i0,i1,i2,i3)':'uModel'};vec4 w=m*vec4(aPosition,1.);mat3 basis=mat3(m);
    vec3 n=normalize(basis*(aNormal/max(vec3(.0001),vec3(dot(basis[0],basis[0]),dot(basis[1],basis[1]),dot(basis[2],basis[2])))));
    float lit=max(0.,dot(n,uSun));vec3 light=vec3(.46,.49,.54)+lit*mix(vec3(.69,.63,.50),vec3(.65),uSun.y);
    vColor=${instanced?'iColor':'uColor'}*light;vWorld=w.xyz;vNormal=n;vFog=clamp((distance(w.xz,uEye.xz)-65.)/175.,0.,.86);gl_Position=uViewProjection*w;}`);
  const fragment=shader(gl,gl.FRAGMENT_SHADER,`precision highp float;
    varying vec3 vColor,vWorld,vNormal;varying float vFog;uniform vec3 uHorizon;
    uniform float uSurface,uFort,uPhase;uniform vec4 uFeet;uniform sampler2D uNoise;
    float noise(vec2 p){return texture2D(uNoise,(p+.5)/128.).r;}
    void main(){
      vec3 tint=vColor;
      if(uSurface==1.){
        vec2 p=vWorld.xz;float fleck=noise(p*1.75),grain=noise(p*8.)-.5,light=.89+.16*fleck+.055*grain;
        tint*=light;
        if(uFort>0.){
          float lane=abs(p.x-2.4*sin(p.y/17.+uPhase)*min(1.,abs(p.y-12.)/20.));
          float road=(1.-smoothstep(2.7,4.5,lane+(fleck-.5)*1.15))*(1.-smoothstep(37.,40.,p.y));
          vec2 f=vec2(.85,.58);
          float earth=1.-smoothstep(3.3,5.2,min(length((p-uFeet.xy)*f),length((p-uFeet.zw)*f))+(fleck-.5)*1.2);
          tint=mix(tint,vec3(.42,.335,.23)*light,max(road,earth)*.90);
        }
      }else if(uSurface==2.){
        vec2 p=abs(vNormal.y)>.6?vWorld.xz:vec2(abs(vNormal.x)>.6?vWorld.z:vWorld.x,vWorld.y);
        float row=floor(p.y/.53);vec2 uv=vec2((p.x+mod(row,2.)*.65)/1.30,p.y/.53);
        vec2 seam=min(fract(uv),1.-fract(uv));
        float mortar=smoothstep(.01,.045,min(seam.x,seam.y))*.15;
        tint*=.81+mortar*(1.-vFog*.75)+.08*noise(floor(uv))+.05*noise(p*6.);
      }
      gl_FragColor=vec4(mix(tint,uHorizon,vFog),1.);
    }`);
  const p=gl.createProgram();gl.attachShader(p,vertex);gl.attachShader(p,fragment);gl.linkProgram(p);gl.deleteShader(vertex);gl.deleteShader(fragment);
  if(!gl.getProgramParameter(p,gl.LINK_STATUS))throw new Error(gl.getProgramInfoLog(p));return p;
}

export async function createBattleRenderer(canvas,battle,options={}) {
  const pack=await loadBattleAssets(options);
  const gl=canvas.getContext('webgl',{antialias:true,alpha:false,powerPreference:'high-performance',preserveDrawingBuffer:false});
  if(!gl)throw new Error('This browser could not start WebGL. Resolve this battle automatically or try another browser.');
  const ext=gl.getExtension('ANGLE_instanced_arrays'),p=program(gl,!!ext),buffers=new Map(),instances=new Map(),staticInstances=new Map();
  const staticBuffers=new Map(),structureInstances=new Map(),structureBuffers=new Map();let structureKey='',targetBatch=instances;
  const noiseTexture=surfaceNoise(gl,battle.seed);
  let resolutionScale=1,lastFrameTime=0,slowFrames=0,fastFrames=0;
  const climate=battle.layout.climate||battlefieldClimate(battle.seed), sky=createBattleSky(gl,climate,shader);
  const uSun=gl.getUniformLocation(p,'uSun'),uHorizon=gl.getUniformLocation(p,'uHorizon');
  const uSurface=gl.getUniformLocation(p,'uSurface'),uFort=gl.getUniformLocation(p,'uFort'),uPhase=gl.getUniformLocation(p,'uPhase'),uFeet=gl.getUniformLocation(p,'uFeet');
  let disposed=false,lost=false,previousBattle=battle,lastEye=[0,20,-30],lastTarget=[0,0,0],lastAspect=1,frame=0;
  let frameStats={bloodParticles:0,corpses:0,detailedCorpses:0,attacking:0,deploymentZones:0};
  const recentHits=new Map();
  const stains=[],stainedEvents=new WeakSet();
  let bloodLevel='full';
  function setBlood(value){
    bloodLevel=['off','reduced','full'].includes(value)?value:'full';
    const limit={off:0,reduced:18,full:64}[bloodLevel];
    if(stains.length>limit)stains.splice(0,stains.length-limit);
  }
  setBlood(options.blood);
  const attribute=name=>gl.getAttribLocation(p,name),uniform=name=>gl.getUniformLocation(p,name);
  const aPos=attribute('aPosition'),aNormal=attribute('aNormal'),uVP=uniform('uViewProjection'),uEye=uniform('uEye'),uModel=uniform('uModel'),uColor=uniform('uColor');
  const iAttr=ext?[0,1,2,3].map(i=>attribute('i'+i)):[],iColor=ext?attribute('iColor'):-1,instanceBuffer=gl.createBuffer();
  const palette=Object.fromEntries(Object.entries(pack.materials).map(([k,v])=>[k,color(v)]));
  const houseColor=owner=>color(pack.houses[owner]?.color||'#ac9f7c');
  // Original procedural cues need no network request. Audio starts only after a gesture.
  let audioContext=null,audioMuted=options.sound===false,lastSoundAt=-1;
  const heardEvents=new WeakSet(),heardShots=new Set(),audioSurface=canvas.closest('dialog')||canvas;
  const unlockAudio=()=>{if(audioMuted)return;try{const Audio=globalThis.AudioContext||globalThis.webkitAudioContext;if(!audioContext&&Audio)audioContext=new Audio();audioContext?.resume().catch(()=>{});}catch{}};
  audioSurface.addEventListener('pointerdown',unlockAudio);
  function cue(kind,distance=0){
    if(audioMuted||audioContext?.state!=='running'||audioContext.currentTime-lastSoundAt<.065)return;
    lastSoundAt=audioContext.currentTime;
    const oscillator=audioContext.createOscillator(),gain=audioContext.createGain(),frequency=({wood:104,stone:57,siege:65,arrow:880,block:1120,brace:570,reinforcements:330})[kind]||210,duration=kind==='siege'||kind==='reinforcements'?.6:kind==='block'?.32:.15;
    oscillator.type=['reinforcements','block','wood'].includes(kind)?'triangle':'sawtooth';oscillator.frequency.setValueAtTime(frequency,lastSoundAt);oscillator.frequency.exponentialRampToValueAtTime(frequency*(kind==='reinforcements'?1.5:kind==='block'?.82:.35),lastSoundAt+duration);
    gain.gain.setValueAtTime(.035/(1+distance/12),lastSoundAt);gain.gain.exponentialRampToValueAtTime(.001,lastSoundAt+duration);oscillator.connect(gain);gain.connect(audioContext.destination);oscillator.start();oscillator.stop(lastSoundAt+duration+.01);oscillator.onended=()=>{oscillator.disconnect();gain.disconnect();};
  }
  function upload(name,mesh){
    const positions=gl.createBuffer(),normals=gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER,positions);gl.bufferData(gl.ARRAY_BUFFER,new Float32Array(mesh.positions),gl.STATIC_DRAW);
    gl.bindBuffer(gl.ARRAY_BUFFER,normals);gl.bufferData(gl.ARRAY_BUFFER,new Float32Array(mesh.normals),gl.STATIC_DRAW);
    buffers.set(name,{positions,normals,count:mesh.positions.length/3});
  }
  for(const [name,mesh]of Object.entries(pack.meshes))upload(name,mesh);
  upload('masonry',pack.meshes.box);
  const spire={positions:[],normals:[]};
  for(let i=0;i<8;i++){
    const a=i*TAU/8,b=(i+1)*TAU/8,points=[[Math.cos(a),0,Math.sin(a)],[0,1,0],[Math.cos(b),0,Math.sin(b)]];
    const n=normalize(cross(points[1].map((v,j)=>v-points[0][j]),points[2].map((v,j)=>v-points[0][j])));
    for(const p of points){spire.positions.push(...p);spire.normals.push(...n);}
  }
  upload('spire',spire);
  const disc={positions:[],normals:[]};for(let i=0;i<16;i++){const a=i*TAU/16,b=(i+1)*TAU/16;disc.positions.push(0,0,0,Math.sin(a),0,Math.cos(a),Math.sin(b),0,Math.cos(b));disc.normals.push(0,1,0,0,1,0,0,1,0);}upload('shadow',disc);
  const annulus={positions:[],normals:[]};for(let i=0;i<32;i++){const a=i*TAU/32,b=(i+1)*TAU/32,point=(angle,r)=>[Math.sin(angle)*r,0,Math.cos(angle)*r];for(const triangle of [[point(a,.965),point(a,1.035),point(b,1.035)],[point(a,.965),point(b,1.035),point(b,.965)]])for(const v of triangle){annulus.positions.push(...v);annulus.normals.push(0,1,0);}}upload('selectionRing',annulus);
  const drop={positions:[],normals:[]},dropPoints=[[0,.7,0],[-.5,-.3,.35],[.5,-.3,.35],[0,-.3,-.55]];
  for(const ids of [[0,1,2],[0,2,3],[0,3,1],[1,3,2]]){const [a,b,c]=ids.map(i=>dropPoints[i]),n=normalize(cross(b.map((v,i)=>v-a[i]),c.map((v,i)=>v-a[i])));for(const point of [a,b,c]){drop.positions.push(...point);drop.normals.push(...n);}}upload('bloodDrop',drop);
  function add(name,matrix,tint,target=targetBatch){if(!buffers.has(name))return;let items=target.get(name);if(!items){items=[];target.set(name,items);}items.push(...matrix,...tint);}
  function object(name,position,scale,tint,rotation=[0,0,0],target=targetBatch){add(name,transform(position,rotation,scale),tint,target);}
  function cacheInstances(source,cache){
    if(!ext)return;
    for(const buffer of cache.values())gl.deleteBuffer(buffer);
    cache.clear();
    for(const [name,data] of source){
      const buffer=gl.createBuffer();gl.bindBuffer(gl.ARRAY_BUFFER,buffer);
      gl.bufferData(gl.ARRAY_BUFFER,new Float32Array(data),gl.STATIC_DRAW);cache.set(name,buffer);
    }
  }
  const terrainColor=battle.layout.terrain==='desert'?[.65,.55,.34]:battle.layout.terrain==='forest'?[.25,.35,.24]:[.38,.43,.28];
  const height=(x,z)=>battleGroundHeight(battle.layout,x,z);
  function corpseFloor(unit,snapToStep=false){
    const ground=height(unit.x,unit.z),wall=previousBattle.layout.walls?.find(w=>w.id===unit.wallId&&w.hp>0);
    if(wall)return ground+(wall.height||6);
    for(const stair of previousBattle.layout.stairs||[]){
      const w=previousBattle.layout.walls?.find(w=>w.id===stair.wallId&&w.hp>0);if(!w)continue;
      const dx=stair.topX-stair.x,dz=stair.topZ-stair.z,length=dx*dx+dz*dz,t=((unit.x-stair.x)*dx+(unit.z-stair.z)*dz)/length;
      if(t>=0&&t<=1&&Math.hypot(unit.x-stair.x-dx*t,unit.z-stair.z-dz*t)<(stair.width||3)/2)return mix(height(stair.x,stair.z),height(stair.topX,stair.topZ)+(w.height||6),snapToStep?Math.ceil(t*12)/12:t);
    }
    return ground;
  }
  function ground(){
    const groups=Array.from({length:7},()=>({positions:[],normals:[]})),w=battle.layout.width||160,d=battle.layout.depth||160,step=TERRAIN_STEP;
    const road=z=>2.4*Math.sin(z/17+climate.phase)*Math.min(1,Math.abs(z-12)/20);
    for(let x=-w/2;x<w/2;x+=step)for(let z=-d/2;z<d/2;z+=step){
      const cx=x+step/2,cz=z+step/2,noise=Math.sin(cx*.18+cz*.07+climate.phase)*.6+Math.cos(cz*.21-cx*.12)*.4;
      let material=Math.min(3,Math.floor((noise+1)*2));
      if(battle.layout.bridges?.length&&Math.abs(cz)<7)material=5;
      const group=groups[material],pos=group.positions,normals=group.normals;
      const vs=[[x,height(x,z),z],[x+step,height(x+step,z),z],[x,height(x,z+step),z+step],[x+step,height(x+step,z+step),z+step]];
      for(const ids of [[0,2,1],[1,2,3]]){const [a,b,c]=ids.map(i=>vs[i]),n=normalize(cross(b.map((x,i)=>x-a[i]),c.map((x,i)=>x-a[i])));for(const v of [a,b,c]){pos.push(...v);normals.push(...n);}}
    }
    const wear=[[.47,.42,.30],[.43,.34,.24],[.49,.40,.29]];
    groups.forEach((group,i)=>{if(!group.positions.length)return;upload('terrain'+i,group);add('terrain'+i,ID(),wear[i-4]||terrainColor.map(c=>c*(.87+i*.05)),staticInstances);});
    const verge={positions:[],normals:[]};
    for(let edge=0;edge<4;edge++){
      const corner=(t,out)=>{const side=edge%2?d:w,a=(t-.5)*(side+2*out),b=((edge%2?w:d)/2+out)*(edge>1?-1:1);return edge%2?[b,a]:[a,b];};
      for(let n=0;n<64;n++){
        const vertices=[corner(n/64,0),corner((n+1)/64,0),corner(n/64,150),corner((n+1)/64,150)];
        for(const i of [0,2,1,1,2,3]){
          const [x,z]=vertices[i];verge.positions.push(x,i<2?height(x,z)-.018:-3,z);verge.normals.push(0,1,0);
        }
      }
    }
    upload('meadow',verge);add('meadow',ID(),terrainColor.map(c=>c*.91),staticInstances);
    if(battle.layout.fortified){
      // Narrow wheel ruts follow the cart road through the gate into the yard.
      for(let z=-d/2;z<38;z+=step)for(const s of [-1,1]){
        const x=road(z)+s*1.08,dx=road(z+step)-road(z),dy=height(x+dx,z+step)-height(x,z);
        add('box',multiply(transform([x+dx/2,height(x,z)+dy/2+.035,z+step/2],[0,Math.atan2(dx,step),0]),transform([0,0,0],[-Math.atan2(dy,step),0,0],[.28,.025,Math.hypot(dx,step,dy)+.08])),[.29,.23,.17],staticInstances);
      }
    }
    for(let i=0;i<74;i++){
      const a=i*TAU/74,band=1.02+.13*Math.sin(3*a+climate.phase),x=Math.sin(a)*w*band,z=Math.cos(a)*d*band;
      object('stone',[x,-4,z],[85,45+27*Math.sin(a*4+climate.phase),70],[.36,.43,.40],[0,a,0],staticInstances);
      if(battle.layout.terrain==='forest'||Math.cos(a*6+climate.phase)>.35){
        for(let j=0;j<3;j++)object('pine',[x*.72+j*2,-1,z*.72],[1.8,2+(i+j)%3*.4,1.8],[.24,.34,.28],[0,a+j,0],staticInstances);
      }
      if(battle.layout.fortified&&z>70&&i%3===0){
        object('box',[x*.67,0,z*.72],[7,4,5],[.48,.48,.42],[0,a,0],staticInstances);
        object('roof',[x*.67,4,z*.72],[4,2.5,3],[.41,.34,.28],[0,a,0],staticInstances);
      }
    }
    for(const o of battle.layout.obstacles||[]){
      const y=height(o.x,o.z),r=o.radius||2;
      if(o.kind==='tree'){
        object('shaft',[o.x,y,o.z],[r*5,3.5,r*5],[.30,.23,.15],[0,0,0],staticInstances);
        object(o.id?.length%2?'pine':'foliage',[o.x,y+2,o.z],[r*.9,(o.height||7)/4,r*.9],[.19+(o.x%7)*.005,.31,.20],[0,o.x,0],staticInstances);
      }else if(o.kind==='rock')object('stone',[o.x,y+(o.height||4)*.46,o.z],[(o.width||r*2)/.4,(o.height||4)/.44,(o.depth||r*2)/.36],[.42,.45,.43],[0,0,0],staticInstances);
      else if(o.kind==='building'){
        object('box',[o.x,y,o.z],[o.width||5,o.height||5,o.depth||5],[.65,.59,.46],[0,0,0],staticInstances);
        object('roof',[o.x,y+(o.height||5),o.z],[(o.width||5)*.6,2,(o.depth||5)*.6],[.34,.22,.17],[0,0,0],staticInstances);
        for(const side of [-1,1])object('box',[o.x+side*(o.width||5)*.28,y+1.8,o.z-(o.depth||5)*.51],[.7,.9,.08],[.16,.20,.18],[0,0,0],staticInstances);
      }else if(o.kind==='water')object('box',[o.x,battle.layout.climate?-.43:-.12,o.z],[o.width||r*2,.12,o.depth||r*2],[.25,.45,.53],[0,0,0],staticInstances);
    }
    const garrison=battle.armies.find(a=>a.side===1);
    drawCastleItems(battle.layout.castle,(...args)=>object(...args,staticInstances),height,houseColor(garrison?.owner));
    if(battle.layout.bridges?.length||battle.layout.terrain==='river'){
      // Bridge positions come from legal routes, not decorative obstructions.
      for(const bridge of battle.layout.bridges||[{x:0,z:0,width:10,depth:16}]){
        object('box',[bridge.x,-.19,bridge.z],[bridge.width||10,.2,bridge.depth||16],[.47,.37,.25],[0,0,0],staticInstances);
        for(const s of [-1,1])object('beam',[bridge.x+s*(bridge.width||10)/2,.6,bridge.z-(bridge.depth||16)/2],[.8,bridge.depth||16,.8],[.35,.28,.2],[Math.PI/2,0,0],staticInstances);
      }
    }
  }
  ground();cacheInstances(staticInstances,staticBuffers);
  function wall(w,isGate=w.kind==='gate'){
    const state=w.state||((w.hp||0)<=0?'breached':'intact'),ratio=clamp((w.hp??w.maxHp)/(w.maxHp||100),0,1),base=height(w.x,w.z);
    if(state==='breached'||state==='destroyed'||ratio===0){
      const alongX=w.width>w.depth,center=alongX?w.x:w.z,span=alongX?w.width:w.depth,opening=w.breach;
      if(opening&&!isGate){
        const impact=alongX?opening.x:opening.z;
        for(const [lo,hi] of [[center-span/2,impact-opening.width/2],[impact+opening.width/2,center+span/2]]){
          if(hi<=lo)continue;
          object('masonry',[alongX?(lo+hi)/2:w.x,base,alongX?w.z:(lo+hi)/2],[alongX?hi-lo:w.width,2,alongX?w.depth:hi-lo],[.46,.46,.40]);
        }
      }
      for(let i=0;i<9;i++){
        const offset=(i-4)*(opening?.width||span)/9,x=(opening?.x??w.x)+(alongX?offset:(i%2-.5)),z=(opening?.z??w.z)+(alongX?(i%2-.5):offset);
        object('stone',[x,height(x,z)+.05,z],[1.4+(i%3)*.4,.5+(i%2)*.4,1.2],isGate?[.34,.25,.16]:[.45,.44,.39],[0,i*1.3,.3]);
      }
      return;
    }
    const h=w.height||6,timber=w.material==='timber',tint=isGate?[.37,.25,.16]:timber?[.47,.34,.21]:w.layer==='inner'?[.60,.60,.55]:[.52,.54,.48];
    object(isGate||timber?'box':'masonry',[w.x,base,w.z],[w.width||8,h,w.depth||2],tint);
    if(isGate){
      for(let i=-2;i<=2;i++)object('beam',[w.x+i*w.width/5.3,base,w.z-w.depth*.51],[.7,h,.7],[.22,.17,.13]);
      for(const y of [.26,.70])object('box',[w.x,base+h*y,w.z-w.depth*.52],[w.width*.97,.18,.10],[.28,.31,.30]);
      object('box',[w.x,base,w.z-w.depth*.52],[.12,h,.12],[.15,.12,.09]);
    }else if(timber){
      const across=w.width>w.depth,span=across?w.width:w.depth;
      for(let n=.26;n<span;n+=.58){
        const x=w.x+(across?n-span/2:Math.sign(w.x)*(w.width/2-.06)),z=w.z+(across?(w.id.endsWith('wall-rear')?1:-1)*(w.depth/2-.06):n-span/2);
        object('shaft',[x,base,z],[11,h+.18,11],[.49+(Math.floor(n)%3)*.015,.35,.22]);
        object('roof',[x,base+h+.10,z],[.27,.65,.28],[.57,.43,.29]);
      }
    }
    // Keep the inner wall top walkable: merlons occupy only the outside edge.
    else if((w.depth||2)>(w.width||8)){const outer=w.x<0?-1:1;for(let z=-(w.depth||2)/2+.7;z<(w.depth||2)/2;z+=2)if(ratio>.6||Math.round(z)%3)object('box',[w.x+outer*((w.width||2)/2-.26),base+h,w.z+z],[.52,1.1,1],[.48,.50,.45]);}
    else {const outer=w.id.endsWith('wall-rear')?1:-1;for(let x=-(w.width||8)/2+.7;x<(w.width||8)/2;x+=2)if(ratio>.6||Math.round(x)%3)object('box',[w.x+x,base+h,w.z+outer*((w.depth||2)/2-.26)],[1,1.1,.52],[.48,.50,.45]);}
    if(ratio<.8)for(let i=0;i<3;i++)object('stone',[w.x+(i-1)*1.3,base+.15,w.z-1.5],[1.5,.8,1.4],[.45,.45,.4]);
  }
  function renderLadder(l,next){
    const wall=next.layout.walls.find(w=>w.id===l.wallId)||next.layout.walls.find(w=>Math.abs(l.x-w.x)<=w.width/2&&Math.abs(l.z-w.z)<=w.depth/2);
    const foot=l.foot||{x:l.x,z:l.z-3.4},top=l.landing||{x:l.x,z:l.z};
    if(!wall||wall.hp<=0){
      for(const s of [-1,1])object('beam',[foot.x+s*.46,height(foot.x,foot.z)+.1,foot.z],[.65,2.6+s*.3,.65],[.39,.29,.20],[Math.PI/2,s*.21,0]);
      return;
    }
    const base=height(foot.x,foot.z)+.04,rise=height(top.x,top.z)+wall.height-base,dz=wall.z-wall.depth/2-.14-foot.z,dx=top.x-foot.x,length=Math.hypot(dx,rise,dz),yaw=Math.atan2(dx,dz);
    const root=transform([foot.x,base,foot.z],[0,yaw,0]),slope=Math.acos(rise/length);
    for(const side of [-1,1])add('beam',multiply(root,transform([side*.5,0,0],[slope,0,0],[.65,length+.28,.65])),palette.wood);
    const rungs=l.rungs||12;
    for(let i=1;i<=rungs;i++)add('beam',multiply(root,transform([-.53,rise*i/rungs,Math.hypot(dx,dz)*i/rungs],[0,0,-Math.PI/2],[.47,1.06,.47])),[.65,.48,.29]);
  }
  function ramStroke(action){
    if(action?.kind!=='ramStrike')return 0;
    const t=action.elapsed,impact=action.windup;
    if(t<impact*.78)return mix(0,-.52,t/(impact*.78));
    if(t<impact)return mix(-.52,.20,(t-impact*.78)/(impact*.22));
    return mix(.20,0,(t-impact)/Math.max(.05,action.duration-impact));
  }
  function renderRam(unit,time){
    const base=transform([unit.x,unit.y||0,unit.z],[0,unit.facing||0,0]);
    const part=(mesh,offset,scale,tint,rotation=[0,0,0])=>add(mesh,multiply(base,transform(offset,rotation,scale)),tint);
    const wood=[.47,.32,.19],iron=[.30,.33,.33],tire=[.18,.20,.20],roll=(unit.rolledDistance||0)/.57;
    // The carriage stays on its wheels, including when every pusher lets go.
    for(const z of [-1.44,1.44]){
      part('beam',[-1.13,.57,z],[1.26,2.26,1.26],iron,[0,0,-Math.PI/2]);
      for(const side of [-1,1]){
        const x=side*1.02;
        part('wheel',[x,.57,z],[1.5,1.5,1.5],tire,[0,0,Math.PI/2]);
        part('wheel',[x+side*.095,.57,z],[1.23,.6,1.23],wood,[0,0,Math.PI/2]);
        for(let i=0;i<3;i++){
          const a=roll+i*Math.PI/3;
          part('beam',[x+side*.14,.57-.41*Math.cos(a),z-.41*Math.sin(a)],[.47,.82,.47],iron,[a,0,0]);
        }
      }
    }
    for(const side of [-1,1]){
      part('box',[side*.76,.68,0],[.27,.27,4.55],wood);
      for(const z of [-1.38,1.24])part('beam',[side*.76,.90,z],[1.15,1.38,1.15],wood,[0,0,side*.22]);
    }
    part('roof',[0,2.16,-.05],[1.12,.55,2.12],[.33,.29,.23]);
    for(const z of [-1.75,0,1.75]){
      part('beam',[-1.42,1.10,z],[.62,2.84,.62],wood,[0,0,-Math.PI/2]);
    }
    const stroke=ramStroke(unit.action),beamY=unit.crewCount>=4?1.27:1.05;
    part('shaft',[0,beamY,-2.31+stroke],[12.5,4.52,12.5],wood,[Math.PI/2,0,0]);
    part('box',[0,beamY-.31,2.30+stroke],[.69,.64,.60],iron);
    for(const z of [-1.33,1.20]){
      part('shaft',[0,beamY,z-.09+stroke],[13.2,.18,13.2],iron,[Math.PI/2,0,0]);
      for(const side of [-1,1])part('shaft',[side*.24,beamY,z+stroke],[1.7,.92,1.7],[.21,.23,.23],[-stroke*.8,0,0]);
    }
  }
  function siegeTargets(next,kind){
    if(kind==='wall'||kind==='breach'){
      const surfaces=kind==='breach'?[...next.layout.walls,next.layout.gate]:next.layout.walls;
      return surfaces.filter(s=>s?.hp>0&&(kind==='breach'||s.kind!=='gate')).map(s=>({id:s.id,x:s.x,z:s.z,y:height(s.x,s.z),width:s.width,depth:s.depth,h:s.height+1}));
    }
    if(kind==='ladder')return next.layout.ladders.filter(l=>next.layout.walls.some(w=>w.hp>0&&(w.id===l.wallId||(!l.wallId&&Math.abs(l.x-w.x)<w.width/2&&Math.abs(l.z-w.z)<w.depth/2)))).map(l=>{
      const foot=l.foot||{x:l.x,z:l.z-3.4},top=l.landing||l;
      return {id:l.id,x:foot.x,z:(foot.z+top.z)/2,y:height(foot.x,foot.z),width:1.55,depth:Math.abs(top.z-foot.z)+.25,h:l.height||6,ladder:true};
    });
    if(kind==='ram')return next.units.filter(u=>u.type==='ram'&&u.status==='active'&&u.side===next.playerSide).map(u=>({id:u.id,x:u.x,y:u.y,z:u.z,width:2.9,depth:5.6,h:2.72,ram:true}));
    return [];
  }
  function climbingLimbs(unit){
    const l=previousBattle.layout.ladders.find(l=>l.id===unit.action.ladderId);
    if(!l)return null;
    const foot=l.foot||l,rise=unit.y-height(foot.x,foot.z),spacing=l.height/(l.rungs||12);
    // Each hand or boot stays on its rung for half a cycle, then reaches past
    // its partner. Moving backward through the cycle also works on recall.
    const reach=(side,arm)=>{
      const stride=2*spacing,start=arm?2.02:.64,offset=side*spacing;
      const cycle=(rise+start-offset)/stride,phase=cycle-Math.floor(cycle);
      const step=clamp((phase-.5)*2,0,1),lift=step*step*(3-2*step);
      const rung=Math.min(l.height,(Math.floor(cycle)+lift)*stride+offset);
      const down=(arm?1.48:.96)-(rung-rise),forward=(arm?.30:.21)+Math.sin(step*Math.PI)*.13;
      const upper=arm?.36:.44,lower=arm?.30:.42,d=Math.hypot(down,forward);
      const bend=Math.acos(clamp((d*d-upper*upper-lower*lower)/(2*upper*lower),-1,1))*(arm?-1:1);
      return [Math.atan2(-forward,down)-Math.atan2(lower*Math.sin(bend),upper+lower*Math.cos(bend)),bend];
    };
    return {armL:reach(0,true),armR:reach(1,true),thighL:reach(1,false),thighR:reach(0,false)};
  }
  function animatedNodes(unit,time){
    if(unit.type==='ram'){renderRam(unit,time);return;}
    const model=pack.models[unit.type]||pack.models.levy,frames=new Map(),ownerTint=houseColor(unit.owner);
    const mounted=unit.family==='mounted'&&unit.mounted!==false,dismounted=unit.family==='mounted'&&!mounted;
    const equip=pack.weapons?.[unit.sidearm?'sword':unit.weapon];
    const nodes=equip?[...model.nodes.filter(n=>n.name!=='weapon'&&n.parent!=='weapon'),...equip]:model.nodes;
    const dead=unit.status==='dead',action=unit.action,moving=(unit.speed||0)>.15||action?.kind==='stairs',progress=action?clamp(action.elapsed/Math.max(.01,action.duration||1),0,1):0;
    const carrying=unit.atHandle&&unit.order?.kind==='crew';
    const ram=carrying&&previousBattle.units.find(u=>u.id===unit.order.ramId);
    const phase=carrying?time*8:time*(mounted?13:9)+(hash(unit.id)%17),walk=moving?Math.sin(phase)*.55:Math.sin(time*2)*.015;
    const facing=dead?(unit.deathFacing??unit.facing??0):action?.kind==='stairs'?Math.atan2(action.targetX-action.fromX,action.targetZ-action.fromZ):unit.facing||0;
    const fall=dead?clamp((time-(unit.deathTime??time-1))/.55,0,1):0,tilt=fall*(mounted?Math.PI/2:1.55),side=hash(unit.id)%2?1:-1;
    const bodyY=dead?mix(unit.y??height(unit.x,unit.z),corpseFloor(unit),fall):unit.y??height(unit.x,unit.z);
    const root=multiply(transform([unit.x,bodyY+fall*(mounted?.48:.23),unit.z],[0,facing,0]),transform([0,0,0],mounted?[0,0,side*tilt]:[tilt,0,side*.12*fall]));
    const pose=battleActionPose(unit),flinch=recentHits.get(unit.id)||0,polearm=carriesPolearm(unit);
    const limbs=action?.kind==='climb'&&action.phase==='rungs'&&climbingLimbs(unit);
    for(const node of nodes){
      const pos=[...node.position],rot=[...node.rotation],name=node.name;
      if((carrying||action?.kind==='climb')&&(name==='weapon'||node.parent==='weapon'||name==='shield'||name==='emblem'))continue;
      if(['shield','emblem'].includes(name)&&unit.shield===false)continue;
      if(dismounted){
        if(name.startsWith('horse')||name.startsWith('ear')||['saddle','tail','mane'].includes(name))continue;
        if(name==='rider')pos[1]=0;
        if(/^thigh[LR]$/.test(name)||/^shin[LR]$/.test(name))rot.fill(0);
      }
      if(dead){
        if(/^arm[LR]$/.test(name)){rot[0]=-.25;rot[2]=name.endsWith('R')?-.6:.7;}
        if(/^forearm[LR]$/.test(name))rot[0]=-.4;
        if(/^thigh[LR]$/.test(name)&&!mounted){rot[0]=name.endsWith('R')?.12:-.08;rot[2]=name.endsWith('R')?-.12:.12;}
        if(name==='weapon')rot[0]=1.6;
      }else{
        if(/^thigh[LR]$/.test(name)&&!mounted)rot[0]+=walk*(name.endsWith('R')?-1:1);
        if(/^shin[LR]$/.test(name)&&!mounted)rot[0]+=Math.max(0,-walk*(name.endsWith('R')?-1:1))*.7;
        if(/^arm[LR]$/.test(name))rot[0]-=walk*(name.endsWith('R')?-1:1)*.65;
        if(/^horseLeg[FB][LR]$/.test(name))rot[0]+=Math.sin(phase+(name.includes('FR')||name.includes('BL')?Math.PI:0))*(moving?.6:.01);
        if(name.startsWith('wheel')&&moving)rot[1]+=time*unit.speed*2;
        if(name==='tail')rot[2]+=Math.sin(time*3)*.18;
        if(name==='cape'||name.startsWith('horseCloth'))rot[0]+=Math.sin(time*5+unit.x)*.1;
        if(unit.blocking&&name==='armL'){rot[0]=-1.1;rot[2]=-.35;}
        if(name==='armR'&&!['shoot','climb','siege'].includes(action?.kind)){rot[0]=pose.arm;rot[2]=.12-pose.twist;}
        if(name==='forearmR'&&!['shoot','climb','siege'].includes(action?.kind))rot[0]=pose.forearm;
        if(name==='weapon'&&!['shoot','siege'].includes(action?.kind)){rot[0]=pose.weapon;pos[1]+=pose.extension;}
        if(action?.kind==='climb'){
          if(/^arm[LR]$/.test(name))rot[0]=limbs?limbs[name][0]:-1.6;
          if(/^forearm[LR]$/.test(name))rot[0]=limbs?limbs['arm'+name.slice(-1)][1]:-.72;
          if(/^thigh[LR]$/.test(name))rot[0]=limbs?limbs[name][0]:name.endsWith('L')?-1.35:-.42;
          if(/^shin[LR]$/.test(name))rot[0]=limbs?limbs['thigh'+name.slice(-1)][1]:.92;
          if(name==='rider')rot[0]=limbs?.07:.35;
        }
        if(carrying){
          const raised=ram?.crewCount>=4,thrust=ramStroke(ram?.action);
          if(/^arm[LR]$/.test(name)){rot[0]=raised?-1.02:-.7;rot[2]=name.endsWith('R')?.13:-.13;}
          if(/^forearm[LR]$/.test(name))rot[0]=raised?-.43:-.6;
          if(name==='rider'){rot[0]=raised?.28+thrust*.24:.10;pos[2]+=thrust*.45;pos[1]-=raised?.12:.04;}
        }
        if(action?.kind==='stairs'){
          if(name==='rider')rot[0]=action.ascending?-.12:.08;
          if(/^shin[LR]$/.test(name))rot[0]+=.15;
        }
        if(action?.kind==='attack'){
          if(name==='rider')rot[1]=pose.twist;
          if(name==='torso')rot[0]=-.08+Math.abs(pose.twist)*.2;
        }
        if(action?.kind==='shoot'){
          const reloading=action.elapsed>(action.windup||0);
          if(name==='armR')rot[0]=reloading?-.6:-1.55;
          if(name==='armL'){rot[0]=reloading?-.7:-1.3;rot[2]=reloading?-.1:-.5;}
          if(name==='weapon')rot[0]=reloading?.4:1.55;
          if(name==='forearmL')rot[0]=reloading?Math.sin(progress*10)*.5:-1.3;
        }
        if(action?.kind==='siege'){
          if(name==='beam') {if(unit.type==='ram')pos[2]+=Math.sin(progress*TAU)*.65;else rot[0]=-.85+Math.sin(progress*Math.PI)*2.2;}
          if(name==='ramHead')pos[2]+=Math.sin(progress*TAU)*.65;
          if(name.includes('crew')&&name.includes('arm'))rot[0]=-1.2+Math.sin(progress*TAU)*.3;
        }
        if(name==='rider')pos[1]+=moving?Math.abs(Math.sin(phase))*(mounted?.08:.035):Math.sin(time*2)*.015;
        if(name==='rider'&&flinch){rot[0]-=flinch*.22;pos[2]-=flinch*.08;}
        if(name==='armL'&&unit.blocking&&flinch){rot[0]-=flinch*.55;pos[2]-=.12*flinch;}
      }
      if(polearm&&name==='armR'){pos[0]=.43;rot[2]=Math.max(.07,rot[2]);}
      const matrix=polearm&&name==='weapon'
        ? polearmFrame(frames.get('rider')||root,frames.get('handR')||root,frames.get('head')||root,polearmPitch(unit,pose))
        : multiply(node.parent?frames.get(node.parent)||root:root,transform(pos,rot,node.scale));
      frames.set(node.name,matrix);
      if(node.mesh){const mesh=node.mesh==='heraldry'?(pack.houses[unit.owner]?.mesh||'crest_ashen'):node.mesh;add(mesh,matrix,node.material==='house'?ownerTint:node.material==='accent'?palette.gold:palette[node.material]||palette.cloth);}
    }
    // A standard marks the actual participating commander; it never counts as a troop.
    if(unit.id===previousBattle.commanderId&&!dead){
      const standard=multiply(root,transform([-.45,1.1,-.3]));add('shaft',multiply(standard,transform([0,0,0],[0,0,0],[1,2.5,1])),palette.wood);
      add('banner',multiply(standard,transform([0,2.4,0],[0,Math.sin(time*2)*.08,0])),ownerTint);
      add(pack.houses[unit.owner]?.mesh||'crest_ashen',multiply(standard,transform([.4,2.16,.09],[0,0,0],[.15,.15,.15])),palette.ivory);
    }
  }
  // A single instanced annulus replaces 24 articulated beam segments per marker.
  function ring(x,y,z,r,tint){object('selectionRing',[x,y,z],[r,1,r],tint);}
  function deploymentOutlines(next){
    if(next.phase!=='deployment')return;
    for(const zone of next.layout.deploymentZones||[]){
      if(![zone.minX,zone.maxX,zone.minZ,zone.maxZ].every(Number.isFinite)||zone.minX>=zone.maxX||zone.minZ>=zone.maxZ)continue;
      const tint=zone.side===next.playerSide?[.35,.74,.76]:[.86,.38,.31],corners=[[zone.minX,zone.minZ],[zone.maxX,zone.minZ],[zone.maxX,zone.maxZ],[zone.minX,zone.maxZ]];
      for(let edge=0;edge<4;edge++){
        const a=corners[edge],b=corners[(edge+1)%4],segments=Math.ceil(Math.hypot(b[0]-a[0],b[1]-a[1])/5);
        for(let i=0;i<segments;i++){
          const x=mix(a[0],b[0],i/segments),z=mix(a[1],b[1],i/segments),endX=mix(a[0],b[0],(i+1)/segments),endZ=mix(a[1],b[1],(i+1)/segments),y=height(x,z)+.075,endY=height(endX,endZ)+.075,length=Math.hypot(endX-x,endY-y,endZ-z);
          add('beam',multiply(transform([x,y,z],[0,Math.atan2(endX-x,endZ-z),0]),transform([0,0,0],[Math.acos((endY-y)/length),0,0],[1,length,.5])),tint);
        }
      }
      frameStats.deploymentZones++;
    }
  }
  function distantCorpse(unit){
    const mounted=unit.family==='mounted'&&unit.mounted!==false,y=corpseFloor(unit),root=multiply(transform([unit.x,y+.23,unit.z],[0,unit.deathFacing??unit.facing??0,0]),transform([0,0,0],[1.55,0,.1]));
    if(unit.family==='siege'){object('box',[unit.x,y+.05,unit.z],[2.5,.35,2],palette.wood,[0,unit.facing||0,.1]);for(const sign of [-1,1])object('beam',[unit.x+sign,y+.2,unit.z-1.5],[1.5,3,1.5],palette.wood,[Math.PI/2,0,sign*.3]);return;}
    const part=(name,p,s,tint)=>add(name,multiply(root,transform(p,[0,0,0],s)),tint);
    // A grounded human silhouette preserves fallen bodies beyond the articulated budget.
    part('torso',[0,.9,0],[1.15,1.1,1],houseColor(unit.owner));part('head',[0,1.72,0],[1,1,1],palette.skin);
    for(const sign of [-1,1]){part('thigh',[sign*.17,.85,0],[1,1.6,1],palette.cloth);part('limb',[sign*.37,1.36,0],[1,1.5,1],palette.leather);}
    if(mounted)object('horseBody',[unit.x+.7,y+.30,unit.z],[1.1,1.4,1.1],palette.horse,[0,unit.deathFacing??unit.facing??0,Math.PI/2]);
  }
  function distantSoldier(u,time){
    const mount=u.mounted, root=transform([u.x,u.y,u.z],[0,u.facing||0,0]);
    const part=(mesh,p,s,tint,rotation=[0,0,0])=>add(mesh,multiply(root,transform(p,rotation,s)),tint);
    const y=mount?1:0,cos=Math.cos(time*8+u.x)*Math.min(.26,(u.speed||0)*.09);
    if(mount){part('horseBody',[0,1.22,0],[1,1,1],palette.horse);part('horseHead',[0,1.75,.89],[1,1,1],palette.horse);}
    const owner=houseColor(u.owner);
    part('torso',[0,y+1.03,0],[1.22,1.08,1],owner);part('head',[0,y+1.76,0],[1,1,1],palette.skin);part('helm',[0,y+1.8,0],[1.06,1.05,1],palette.steel);
    for(const s of [-1,1])part('limb',[s*.16,y+.92,0],[1,2.45,1],palette.leather,[cos*s,0,0]);
    if(u.shield)part('shield',[-.34,y+1.17,.15],[1,1,1],owner);
    if(!u.sidearm&&['bow','crossbow'].includes(u.weapon))part('bow',[.31,y+1.21,.28],[1,1,1],palette.wood,u.weapon==='crossbow'?[0,0,Math.PI/2]:[0,0,0]);
    else if(carriesPolearm(u)){
      const grip=polearmFrame(root,multiply(root,transform([.46,y+.86,.12])),root,polearmPitch(u));
      add('shaft',multiply(grip,transform([0,-.13,0],[0,0,0],[1.2,2.5,1.2])),palette.wood);
      add('blade',multiply(grip,transform([0,2.32,0],[0,0,0],[1,.5,1])),palette.silver);
    }
    else part('blade',[.33,y+.78,.12],[1,1,1],palette.steel,[u.action?.kind==='attack'?1.65:.4,0,0]);
  }
  function distantEngine(u){
    const root=transform([u.x,u.y,u.z],[0,u.facing||0,0]);
    const part=(mesh,p,s,tint,r=[0,0,0])=>add(mesh,multiply(root,transform(p,r,s)),tint);
    const ram=u.type==='ram';
    part('box',[0,.58,0],[1.65,.32,ram?4.45:3.25],palette.wood);
    for(const x of [-1,1])for(const z of [-1.35,1.35])part('wheel',[x,.53,z],[1.34,1.25,1.34],palette.darkSteel,[0,0,Math.PI/2]);
    if(ram){
      part('roof',[0,2.14,-.05],[1.12,.55,2.12],palette.wood);
      part('shaft',[0,1.23,-2.28+ramStroke(u.action)],[12,4.8,12],palette.wood,[Math.PI/2,0,0]);
      part('box',[0,.95,2.25+ramStroke(u.action)],[.70,.61,.65],palette.steel);
    }else{
      part('box',[0,.85,0],[.38,u.type==='trebuchet'?2.68:1.52,.48],palette.wood);
      part('beam',[0,u.type==='trebuchet'?3.3:2.1,0],[1.35,2.45,1.35],palette.wood,[-.7,0,0]);
    }
  }
  function impactEffects(next){
    let burstCount=0;
    for(const event of [...(next.events||[])].reverse()){
      const age=next.time-event.time;if(age<0||age>BLOOD_LIFETIME||!Number.isFinite(event.x)||!Number.isFinite(event.z))continue;
      const eyeDistance=Math.hypot(event.x-lastEye[0],(event.y??height(event.x,event.z)+1)-lastEye[1],event.z-lastEye[2]);
      if(eyeDistance>BLOOD_DISTANCE)continue;
      const target=next.units.find(u=>u.id===(event.unitId||event.targetId)),damage=event.damageApplied??event.damage??0;
      const blood=event.type==='hit'&&damage>0&&!event.blocked&&(event.targetFamily??target?.family)!=='siege';
      if(blood&&bloodLevel!=='off'&&burstCount<(bloodLevel==='reduced'?4:12)){
        burstCount++;
        const seed=hash(event.eventId??`${event.time}:${event.unitId}:${event.sourceId}`),baseY=event.y??((target?.y??height(event.x,event.z))+(target?.mounted?1.7:1.1)),groundY=corpseFloor({x:event.x,z:event.z,wallId:target?.wallId},true);
        if(!stainedEvents.has(event)){
          stainedEvents.add(event);stains.push({x:event.x,z:event.z,y:groundY,time:event.time,wallId:target?.wallId,seed});
          if(stains.length>(bloodLevel==='reduced'?18:64))stains.shift();
        }
        for(let i=0;i<(bloodLevel==='reduced'?3:8)&&frameStats.bloodParticles<BLOOD_LIMIT;i++){
          const random=n=>((Math.imul(seed+i*719+n*103,48271)>>>0)%1000)/1000,a=random(1)*TAU,speed=.6+random(2)*1.8;
          const vx=Math.sin(a)*speed+(event.directionX||0)*1.25,vz=Math.cos(a)*speed+(event.directionZ||0)*1.25,vy=.8+random(3)*1.4;
          const y=Math.max(groundY+.035,baseY+vy*age-4.9*age*age),size=(.06+random(4)*.065)*(1-age/BLOOD_LIFETIME*.6);
          object('bloodDrop',[event.x+vx*age,y,event.z+vz*age],[size,size*(y<=groundY+.04?.2:1.7),size],[.52+random(5)*.16,.025,.018],[age*8,a,age*3]);frameStats.bloodParticles++;
        }
      }
      if(age>.45)continue;
      if(!heardEvents.has(event)){heardEvents.add(event);if(['hit','block','brace','ramImpact','siegeImpact','breach','reinforcements'].includes(event.type))cue(event.material||(['siegeImpact','breach'].includes(event.type)?'siege':event.type),eyeDistance);}
      if(['block','brace','ramImpact','siegeImpact','breach'].includes(event.type)){
        const heavy=['ramImpact','siegeImpact','breach'].includes(event.type),wood=event.material==='wood',size=(heavy?.68:.12)*(1-age/.5);
        for(let i=0;i<(heavy?7:3);i++){
          const a=i*2.4,spread=age*(heavy?3:1),x=event.x+Math.sin(a)*spread,y=(event.y??height(event.x,event.z)+(heavy?.5:1.3))+age*(2+i%3)-age*age*5,z=event.z+Math.cos(a)*spread;
          object(wood?'beam':heavy?'stone':'bloodDrop',[x,y,z],wood?[size,size*.8,size]:[size,size,size],wood?[.72,.52,.30]:heavy?[.57,.55,.47]:[.95,.82,.50],[age*7,a,age*4]);
          if(heavy&&!wood&&i<3)object('stone',[x,y+.15,z],[size*2,size*.7,size*1.5],[.66,.62,.54],[0,a,0]);
        }
      }
    }
    for(const s of stains){
      const age=next.time-s.time;
      if(age<.18||age>35||Math.hypot(s.x-lastEye[0],s.z-lastEye[2])>BLOOD_DISTANCE)continue;
      if(s.wallId&&!next.layout.walls.some(w=>w.id===s.wallId&&w.hp>0))continue;
      const radius=(.16+(s.seed%12)/60)*(1-.45*age/35);
      object('shadow',[s.x,s.y+.055,s.z],[radius,1,radius*.62],[.32,.075,.045],[0,s.seed%6,0]);
      frameStats.stains++;
    }
  }
  function resize(){if(disposed)return;const rect=canvas.getBoundingClientRect(),cap=previousBattle.maxActive||150,dpr=Math.min(globalThis.devicePixelRatio||1,cap>=300?1.15:1.75)*resolutionScale,w=Math.max(1,Math.round(rect.width*dpr)),h=Math.max(1,Math.round(rect.height*dpr));if(canvas.width!==w||canvas.height!==h){canvas.width=w;canvas.height=h;}gl.viewport(0,0,w,h);lastAspect=w/h;}
  function tuneResolution(cap){
    const now=performance.now(),elapsed=now-lastFrameTime;lastFrameTime=now;
    if(cap<150||elapsed<4||elapsed>350)return;
    slowFrames=elapsed>38?slowFrames+1:Math.max(0,slowFrames-2);
    fastFrames=elapsed<19?fastFrames+1:0;
    if(slowFrames>=12){resolutionScale=Math.max(cap>=300?.68:.80,resolutionScale-.10);slowFrames=0;}
    if(fastFrames>=100){resolutionScale=Math.min(1,resolutionScale+.05);fastFrames=0;}
  }
  function drawStructures(next){
    // Only rebuild these batches when visible fortification geometry changes.
    const surfaces=[...(next.layout.walls||[]),next.layout.gate].filter(Boolean);
    const key=surfaces.map(w=>{
      const damage=w.hp<=0?3:w.hp/w.maxHp<.6?2:w.hp/w.maxHp<.8?1:0;
      return [w.id,w.x,w.z,w.width,w.depth,w.height,w.material,damage,w.breach?.x,w.breach?.z,w.breach?.width].join(',');
    }).join('|')+'-'+(next.layout.stairs?.length||0)+'-'+(next.layout.ladders?.length||0);
    if(key===structureKey)return;
    structureKey=key;structureInstances.clear();targetBatch=structureInstances;
    for(const w of next.layout.walls||[])wall(w);if(next.layout.gate)wall(next.layout.gate,true);
    for(const stair of next.layout.stairs||[]){
      const w=next.layout.walls.find(w=>w.id===stair.wallId);if(!w||w.hp<=0)continue;
      const dx=stair.topX-stair.x,dz=stair.topZ-stair.z,length=Math.hypot(dx,dz)||stair.depth||6,yaw=Math.atan2(dx,dz),steps=12;
      const base=height(stair.x,stair.z),rise=height(stair.topX,stair.topZ)+(w.height||stair.height||6)-base;
      for(let i=1;i<=steps;i++){
        const t=(i-.5)/steps;
        object('box',[stair.x+dx*t,base,stair.z+dz*t],[stair.width||3,rise*i/steps,length/steps+.015],i%2?[.53,.55,.49]:[.49,.51,.45],[0,yaw,0]);
      }
      // A small landing joins the staircase and the parapet without a gap.
      object('box',[stair.topX,base,stair.topZ],[stair.width||3,rise,.6],[.53,.55,.49],[0,yaw,0]);
    }
    for(const ladder of next.layout.ladders||[]) renderLadder(ladder,next);
    targetBatch=instances;cacheInstances(structureInstances,structureBuffers);
  }
  function renderTroops(next,camera,tactical,vp){
    const viewDistance=unit=>Math.hypot(unit.x-lastEye[0],unit.z-lastEye[2]);
    const corpses=next.units.filter(u=>u.status==='dead'&&viewDistance(u)<180).sort((a,b)=>viewDistance(a)-viewDistance(b)).slice(0,CORPSE_LIMIT);
    const active=next.units.filter(u=>u.status==='active'),large=active.length>=100;
    const visible=active.filter(u=>{
      if(!large||u.id===next.commanderId)return true;
      const x=u.x,y=u.y+1.5,z=u.z,w=vp[3]*x+vp[7]*y+vp[11]*z+vp[15],margin=u.family==='siege'?9:4;
      return w+margin>0&&Math.abs(vp[0]*x+vp[4]*y+vp[8]*z+vp[12])<=w+margin&&Math.abs(vp[1]*x+vp[5]*y+vp[9]*z+vp[13])<=w+margin;
    });
    const budget=active.length>=300?24:active.length>=150?36:52;
    const score=u=>viewDistance(u)-(u.id===next.commanderId?1000:u.atHandle||u.action?.kind==='climb'?10:0);
    const detailed=new Set(visible.filter(u=>viewDistance(u)<56||u.id===next.commanderId).sort((a,b)=>score(a)-score(b)).slice(0,large?budget:Infinity));
    frameStats.culledTroops=active.length-visible.length;
    const selected=new Set(camera.selected||[]);
    for(const unit of [...visible,...corpses]){
      // Rendering detail never changes simulation membership or casualties.
      const distance=viewDistance(unit),dead=unit.status==='dead';
      if(distance>220)continue;
      object('shadow',[unit.x,(dead?corpseFloor(unit):unit.wallId||unit.action?.kind==='stairs'?unit.y:height(unit.x,unit.z))+.025,unit.z],[unit.family==='siege'?2:unit.family==='mounted'&&unit.mounted!==false?1.25:.6,1,dead?1.4:unit.family==='siege'?2:unit.family==='mounted'&&unit.mounted!==false?1.6:.4],terrainColor.map(c=>c*.66));
      if(dead){frameStats.corpses++;if(frameStats.detailedCorpses<DETAILED_CORPSE_LIMIT&&distance<65){animatedNodes(unit,next.time);frameStats.detailedCorpses++;}else distantCorpse(unit);}
      else {
        if(detailed.has(unit)){animatedNodes(unit,next.time);frameStats.detailedTroops++;}
        else if(unit.family==='siege'){distantEngine(unit);frameStats.silhouettes++;}
        else {distantSoldier(unit,next.time);frameStats.silhouettes++;}
        if(unit.action?.kind==='attack')frameStats.attacking++;
      }
      if(unit.status==='active'){
        if(tactical||unit.id===next.commanderId)ring(unit.x,(unit.y||0)+.035,unit.z,unit.family==='siege'?2:unit.family==='mounted'?1:.55,unit.side===next.playerSide?[.40,.79,.83]:[.82,.34,.27]);
        if(unit.selected||selected.has(unit.id))ring(unit.x,(unit.y||0)+.07,unit.z,1.2,[.95,.84,.34]);
      }
    }
  }
  function render(next,camera={}){
    if(disposed)return;if(lost)throw new Error('The 3D graphics context was lost. Your battle checkpoint is safe; retry or finish automatically.');
    previousBattle=next;tuneResolution(next.maxActive||150);resize();instances.clear();
    const commander=next.units.find(u=>u.id===next.commanderId),tactical=camera.mode==='tactical'||!commander||commander.status==='dead';
    const target=camera.target||{x:commander?.x||0,y:commander?.y||0,z:commander?.z||0},yaw=camera.yaw??commander?.facing??0;
    const pitch=clamp(camera.pitch??(tactical?.9:.32),tactical?.45:.04,1.52),distance=clamp(camera.distance??(tactical?70:7),tactical?12:3,150);
    lastTarget=Array.isArray(target)?target:[target.x||0,(target.y||0)+(tactical?0:1.5),target.z||0];
    lastEye=[lastTarget[0]-Math.sin(yaw)*Math.cos(pitch)*distance,lastTarget[1]+Math.sin(pitch)*distance,lastTarget[2]-Math.cos(yaw)*Math.cos(pitch)*distance];
    frameStats={bloodParticles:0,stains:0,corpses:0,detailedCorpses:0,attacking:0,deploymentZones:0,detailedTroops:0,silhouettes:0,culledTroops:0};recentHits.clear();
    for(const event of next.events||[]){const age=next.time-event.time;if((event.type==='block'||event.type==='hit'&&(event.damageApplied??event.damage)>0)&&age>=0&&age<.22)recentHits.set(event.unitId||event.targetId,1-age/.22);}
    const vp=multiply(perspective(lastAspect),lookAt(lastEye,lastTarget));
    deploymentOutlines(next);
    drawStructures(next);
    for(const ladder of next.layout.ladders||[])if(ladder.held&&next.layout.walls.some(w=>w.id===ladder.wallId&&w.hp>0))ring(ladder.x,height(ladder.x,ladder.z)+ladder.height+.09,ladder.z,.95,[.98,.39,.20]);
    renderTroops(next,camera,tactical,vp);
    for(const shot of next.projectiles||[]){
      if(!heardShots.has(shot.id)){heardShots.add(shot.id);cue(shot.kind==='stone'?'siege':'arrow',Math.hypot(shot.x-lastTarget[0],shot.z-lastTarget[2]));}
      const dx=shot.targetX-shot.startX,dz=shot.targetZ-shot.startZ,heading=Math.atan2(dx,dz);
      if(shot.kind==='stone')object('stone',[shot.x,shot.y,shot.z],[1.2,1.2,1.2],[.42,.42,.38],[next.time*4,heading,0]);
      else {
        const t=clamp(shot.elapsed/shot.duration,0,1),slope=(shot.targetY-shot.startY)+.22*Math.PI*Math.cos(t*Math.PI),pitch=Math.atan2(Math.hypot(dx,dz),slope);
        add('shaft',multiply(transform([shot.x,shot.y,shot.z],[0,heading,0]),transform([0,0,0],[pitch,0,0],[.6,shot.kind==='bolt'?.55:1,.6])),[.74,.65,.43]);
      }
    }
    impactEffects(next);
    if(camera.targetKind){
      if(camera.targetKind==='ground'&&camera.groundTarget){
        const {x,z}=camera.groundTarget;ring(x,height(x,z)+.07,z,1.15,[1,.82,.27]);
      }
      for(const s of siegeTargets(next,camera.targetKind)){
        const radius=s.ram?2:s.ladder?1.3:Math.min(s.width,s.depth)*.64, y=s.y+(s.ram?1.7:s.ladder?0:s.h-.97);
        ring(s.x,y+.09,s.z,radius,[1,.77,.22]);
        if(!s.ram&&!s.ladder){
          object('box',[s.x,y+.03,s.z],[s.width,.065,s.depth],[.91,.69,.28]);
        }
      }
    }
    if(tactical){
      for(const u of next.units.filter(u=>u.status==='active'&&u.side===next.playerSide&&Number.isFinite(u.order?.x))){
        const dest=u.order;if(Number.isFinite(dest.x)&&Number.isFinite(dest.z)){
          ring(dest.x,height(dest.x,dest.z)+.08,dest.z,.55,[.95,.83,.4]);
          object('blade',[dest.x,height(dest.x,dest.z)+.08,dest.z],[.8,1.4,.8],[.95,.83,.4],[Math.PI/2,dest.facing||0,0]);
        }
      }
      for(const a of next.armies.filter(a=>!a.arrived)){if(a.side===next.playerSide&&a.entry)ring(a.entry.x,.08,a.entry.z,3,[.4,.7,.85]);}
    }
    gl.clearColor(...sky.horizon,1);gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);gl.enable(gl.DEPTH_TEST);gl.disable(gl.CULL_FACE);gl.useProgram(p);gl.uniformMatrix4fv(uVP,false,vp);gl.uniform3fv(uEye,lastEye);gl.uniform3fv(uSun,climate.sun);gl.uniform3fv(uHorizon,sky.horizon);
    if(ext){ext.vertexAttribDivisorANGLE(aPos,0);ext.vertexAttribDivisorANGLE(aNormal,0);}
    gl.activeTexture(gl.TEXTURE0);gl.bindTexture(gl.TEXTURE_2D,noiseTexture);
    gl.uniform1f(uFort,next.layout.fortified?1:0);gl.uniform1f(uPhase,climate.phase);
    const feet=(next.layout.ladders||[]).slice(0,2).flatMap(l=>[l.x,l.z-5]);while(feet.length<4)feet.push(1000);
    gl.uniform4fv(uFeet,feet);
    for(const [batch,cache]of [[staticInstances,staticBuffers],[structureInstances,structureBuffers],[instances,null]])for(const [name,data]of batch){
      const geo=buffers.get(name);if(!geo||!data.length)continue;
      gl.uniform1f(uSurface,name==='masonry'?2:name.startsWith('terrain')?1:0);
      gl.bindBuffer(gl.ARRAY_BUFFER,geo.positions);gl.enableVertexAttribArray(aPos);gl.vertexAttribPointer(aPos,3,gl.FLOAT,false,0,0);
      gl.bindBuffer(gl.ARRAY_BUFFER,geo.normals);gl.enableVertexAttribArray(aNormal);gl.vertexAttribPointer(aNormal,3,gl.FLOAT,false,0,0);
      if(ext){
        gl.bindBuffer(gl.ARRAY_BUFFER,cache?cache.get(name):instanceBuffer);
        if(!cache)gl.bufferData(gl.ARRAY_BUFFER,new Float32Array(data),gl.DYNAMIC_DRAW);
        for(let i=0;i<4;i++){gl.enableVertexAttribArray(iAttr[i]);gl.vertexAttribPointer(iAttr[i],4,gl.FLOAT,false,76,i*16);ext.vertexAttribDivisorANGLE(iAttr[i],1);}
        gl.enableVertexAttribArray(iColor);gl.vertexAttribPointer(iColor,3,gl.FLOAT,false,76,64);ext.vertexAttribDivisorANGLE(iColor,1);
        ext.drawArraysInstancedANGLE(gl.TRIANGLES,0,geo.count,data.length/19);
      }else for(let i=0;i<data.length;i+=19){gl.uniformMatrix4fv(uModel,false,new Float32Array(data.slice(i,i+16)));gl.uniform3fv(uColor,data.slice(i+16,i+19));gl.drawArrays(gl.TRIANGLES,0,geo.count);}
    }
    // Attributes enabled by the world program may address a much smaller final mesh.
    if(ext)for(const index of [...iAttr,iColor])gl.disableVertexAttribArray(index);
    gl.disableVertexAttribArray(aPos);gl.disableVertexAttribArray(aNormal);
    const forward=normalize(lastTarget.map((v,i)=>v-lastEye[i])),right=normalize(cross(forward,[0,1,0])),up=cross(right,forward),tan=Math.tan(Math.PI/6);
    sky.draw(forward,right.map(v=>v*tan*lastAspect),up.map(v=>v*tan),next.time,ext);
    frame++;
  }
  function pick(clientX,clientY,pickOptions={}){
    const rect=canvas.getBoundingClientRect(),nx=(clientX-rect.left)/rect.width*2-1,ny=1-(clientY-rect.top)/rect.height*2;
    const forward=normalize(lastTarget.map((v,i)=>v-lastEye[i])),right=normalize(cross(forward,[0,1,0])),up=cross(right,forward),tan=Math.tan(Math.PI/6);
    const ray=normalize(forward.map((v,i)=>v+right[i]*nx*tan*lastAspect+up[i]*ny*tan));
    const location=d=>lastEye.map((v,k)=>v+ray[k]*d);
    // The first surface along the ray is important on the back slope of a ridge.
    let d=Infinity;
    for(let t=.15;t<360;t+=1.5){
      const [x,y,z]=location(t);
      if(Math.abs(x)>previousBattle.layout.width/2||Math.abs(z)>previousBattle.layout.depth/2||y>height(x,z))continue;
      let lo=Math.max(0,t-1.5),hi=t;
      for(let i=0;i<9;i++){const mid=(lo+hi)/2,[mx,my,mz]=location(mid);if(my>height(mx,mz))lo=mid;else hi=mid;}
      d=hi;break;
    }
    const point=Number.isFinite(d)?location(d):null;
    if(point)point[1]=height(point[0],point[2]);
    if(pickOptions.targetKind){
      const kind=pickOptions.targetKind;
      const distanceTo=s=>rayBox(lastEye,ray,[s.x-s.width/2,s.y,s.z-s.depth/2],[s.x+s.width/2,s.y+s.h,s.z+s.depth/2]);
      // A covering wall should intercept the aim before a target behind it.
      let cover=d;
      for(const s of siegeTargets(previousBattle,'breach'))cover=Math.min(cover,distanceTo(s));
      for(const o of previousBattle.layout.obstacles||[]){
        if(o.kind==='water')continue;
        cover=Math.min(cover,distanceTo({x:o.x,y:height(o.x,o.z),z:o.z,width:o.width||o.radius*2,depth:o.depth||o.radius*2,h:o.height}));
      }
      if(kind==='ground')return point&&cover>=d?{x:point[0],y:point[1],z:point[2],unitId:null}:null;
      let picked=null,nearest=cover+.02;
      for(const s of siegeTargets(previousBattle,kind)){
        const t=distanceTo(s);
        if(t<=nearest&&Number.isFinite(t)){picked=s;nearest=t;}
      }
      if(!picked)return null;
      const [x,y,z]=location(nearest);
      return{x,y,z,targetId:picked.id,unitId:picked.ram?picked.id:null};
    }
    // Select elevated wall defenders at their visible bodies, not the ground behind them.
    let picked=null,nearest=d;
    for(const unit of pickOptions.groundOnly?[]:previousBattle.units){
      if(unit.status!=='active')continue;
      const radius=unit.family==='siege'?1.8:unit.mounted?1.2:.75,center=[unit.x,(unit.y||0)+(unit.mounted?1.5:.95),unit.z],offset=center.map((v,i)=>v-lastEye[i]),along=dot(offset,ray);
      if(along<=0||along>=nearest)continue;
      if(dot(offset,offset)-along*along<=radius*radius){picked=unit;nearest=along;}
    }
    if(picked)return{x:picked.x,y:picked.y||0,z:picked.z,unitId:picked.id};
    if(!point)return null;
    const [x,y,z]=point;
    if(Math.abs(x)>previousBattle.layout.width/2||Math.abs(z)>previousBattle.layout.depth/2)return null;
    if(pickOptions.groundOnly)return{x,y,z,unitId:null};
    const unit=previousBattle.units.filter(u=>u.status==='active').sort((a,b)=>Math.hypot(a.x-x,a.z-z)-Math.hypot(b.x-x,b.z-z))[0];
    return{x,y,z,unitId:unit&&Math.hypot(unit.x-x,unit.z-z)<2.4?unit.id:null};
  }
  const contextLost=event=>{event.preventDefault();lost=true;};canvas.addEventListener('webglcontextlost',contextLost);
  const resizeObserver=typeof ResizeObserver!=='undefined'?new ResizeObserver(resize):null;resizeObserver?.observe(canvas);
  function dispose(){if(disposed)return;disposed=true;resizeObserver?.disconnect();canvas.removeEventListener('webglcontextlost',contextLost);audioSurface.removeEventListener('pointerdown',unlockAudio);audioContext?.close().catch(()=>{});audioContext=null;heardShots.clear();for(const b of buffers.values()){gl.deleteBuffer(b.positions);gl.deleteBuffer(b.normals);}for(const cache of [staticBuffers,structureBuffers]){for(const buffer of cache.values())gl.deleteBuffer(buffer);cache.clear();}gl.deleteTexture(noiseTexture);sky.dispose();gl.deleteBuffer(instanceBuffer);gl.deleteProgram(p);buffers.clear();instances.clear();staticInstances.clear();structureInstances.clear();releaseBattleAssets();}
  resize();return{render,pick,resize,dispose,setBlood,setMuted(value){audioMuted=!!value;if(audioMuted)audioContext?.suspend().catch(()=>{});},get stats(){return{drawCalls:instances.size+staticInstances.size+structureInstances.size,frames:frame,resolutionScale,instanced:!!ext,models:Object.keys(pack.models).length,...frameStats};}};
}
