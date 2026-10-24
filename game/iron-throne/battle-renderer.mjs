import {loadBattleAssets,releaseBattleAssets} from './battle-assets.mjs';

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
const dot=(a,b)=>a.reduce((n,x,i)=>n+x*b[i],0);
function lookAt(eye,target){const z=normalize(eye.map((v,i)=>v-target[i])),x=normalize(cross([0,1,0],z)),y=cross(z,x);return new Float32Array([x[0],y[0],z[0],0,x[1],y[1],z[1],0,x[2],y[2],z[2],0,-dot(x,eye),-dot(y,eye),-dot(z,eye),1]);}
function perspective(aspect){const f=1/Math.tan(Math.PI/6),near=.15,far=420;return new Float32Array([f/aspect,0,0,0,0,f,0,0,0,0,(far+near)/(near-far),-1,0,0,2*far*near/(near-far),0]);}
function shader(gl,type,source){const s=gl.createShader(type);gl.shaderSource(s,source);gl.compileShader(s);if(!gl.getShaderParameter(s,gl.COMPILE_STATUS)){const message=gl.getShaderInfoLog(s);gl.deleteShader(s);throw new Error(message);}return s;}
function program(gl,instanced){
  const vertex=shader(gl,gl.VERTEX_SHADER,`precision highp float;
  attribute vec3 aPosition;attribute vec3 aNormal;
  ${instanced?'attribute vec4 i0;attribute vec4 i1;attribute vec4 i2;attribute vec4 i3;attribute vec3 iColor;':'uniform mat4 uModel;uniform vec3 uColor;'}
  uniform mat4 uViewProjection;uniform vec3 uEye;varying vec3 vColor;varying float vFog;
  void main(){mat4 m=${instanced?'mat4(i0,i1,i2,i3)':'uModel'};vec4 w=m*vec4(aPosition,1.);vec3 n=normalize(mat3(m)*aNormal);float light=.48+.52*max(0.,dot(n,normalize(vec3(-.45,.8,.25))));vColor=${instanced?'iColor':'uColor'}*light;vFog=clamp((distance(w.xyz,uEye)-65.)/170.,0.,.82);gl_Position=uViewProjection*w;}`);
  const fragment=shader(gl,gl.FRAGMENT_SHADER,`precision mediump float;varying vec3 vColor;varying float vFog;void main(){gl_FragColor=vec4(mix(vColor,vec3(.48,.60,.65),vFog),1.);}`);
  const p=gl.createProgram();gl.attachShader(p,vertex);gl.attachShader(p,fragment);gl.linkProgram(p);gl.deleteShader(vertex);gl.deleteShader(fragment);
  if(!gl.getProgramParameter(p,gl.LINK_STATUS))throw new Error(gl.getProgramInfoLog(p));return p;
}

// Matches the terrain height function used by the simulation, supplied when available.
export function battleGroundHeight(layout,x,z){
  if(layout.terrain==='hills')return 2.8*Math.sin(x/25)+2.3*Math.cos(z/23);
  return 0;
}

export async function createBattleRenderer(canvas,battle,options={}) {
  const pack=await loadBattleAssets(options);
  const gl=canvas.getContext('webgl',{antialias:true,alpha:false,powerPreference:'high-performance',preserveDrawingBuffer:false});
  if(!gl)throw new Error('This browser could not start WebGL. Resolve this battle automatically or try another browser.');
  const ext=gl.getExtension('ANGLE_instanced_arrays'),p=program(gl,!!ext),buffers=new Map(),instances=new Map(),staticInstances=new Map();
  let disposed=false,lost=false,previousBattle=battle,lastEye=[0,20,-30],lastTarget=[0,0,0],lastAspect=1,frame=0;
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
    const oscillator=audioContext.createOscillator(),gain=audioContext.createGain(),frequency=kind==='siege'?65:kind==='arrow'?880:kind==='reinforcements'?330:210,duration=kind==='siege'?.6:kind==='reinforcements'?.6:.12;
    oscillator.type=kind==='reinforcements'?'triangle':'sawtooth';oscillator.frequency.setValueAtTime(frequency,lastSoundAt);oscillator.frequency.exponentialRampToValueAtTime(frequency*(kind==='reinforcements'?1.5:.35),lastSoundAt+duration);
    gain.gain.setValueAtTime(.035/(1+distance/12),lastSoundAt);gain.gain.exponentialRampToValueAtTime(.001,lastSoundAt+duration);oscillator.connect(gain);gain.connect(audioContext.destination);oscillator.start();oscillator.stop(lastSoundAt+duration+.01);oscillator.onended=()=>{oscillator.disconnect();gain.disconnect();};
  }
  function upload(name,mesh){
    const positions=gl.createBuffer(),normals=gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER,positions);gl.bufferData(gl.ARRAY_BUFFER,new Float32Array(mesh.positions),gl.STATIC_DRAW);
    gl.bindBuffer(gl.ARRAY_BUFFER,normals);gl.bufferData(gl.ARRAY_BUFFER,new Float32Array(mesh.normals),gl.STATIC_DRAW);
    buffers.set(name,{positions,normals,count:mesh.positions.length/3});
  }
  for(const [name,mesh]of Object.entries(pack.meshes))upload(name,mesh);
  const disc={positions:[],normals:[]};for(let i=0;i<16;i++){const a=i*TAU/16,b=(i+1)*TAU/16;disc.positions.push(0,0,0,Math.sin(a),0,Math.cos(a),Math.sin(b),0,Math.cos(b));disc.normals.push(0,1,0,0,1,0,0,1,0);}upload('shadow',disc);
  function add(name,matrix,tint,target=instances){if(!buffers.has(name))return;let items=target.get(name);if(!items){items=[];target.set(name,items);}items.push(...matrix,...tint);}
  function object(name,position,scale,tint,rotation=[0,0,0],target=instances){add(name,transform(position,rotation,scale),tint,target);}
  const terrainColor=battle.layout.terrain==='desert'?[.65,.55,.34]:battle.layout.terrain==='forest'?[.25,.35,.24]:[.38,.43,.28];
  const height=(x,z)=>battleGroundHeight(battle.layout,x,z);
  function ground(){
    const groups=Array.from({length:4},()=>({positions:[],normals:[]})),w=battle.layout.width||160,d=battle.layout.depth||160,step=5;
    for(let x=-w/2;x<w/2;x+=step)for(let z=-d/2;z<d/2;z+=step){
      const group=groups[Math.abs(Math.floor(Math.sin(x*.15+z*.07+(Number(battle.seed)||0)%20)*1.6+1.7))%4],pos=group.positions,normals=group.normals;
      const vs=[[x,height(x,z),z],[x+step,height(x+step,z),z],[x,height(x,z+step),z+step],[x+step,height(x+step,z+step),z+step]];
      for(const ids of [[0,2,1],[1,2,3]]){const [a,b,c]=ids.map(i=>vs[i]),n=normalize(cross(b.map((x,i)=>x-a[i]),c.map((x,i)=>x-a[i])));for(const v of [a,b,c]){pos.push(...v);normals.push(...n);}}
    }
    groups.forEach((group,i)=>{upload('terrain'+i,group);add('terrain'+i,ID(),terrainColor.map(c=>c*(.94+i*.025)),staticInstances);});
    // Low exterior ridges frame the arena without adding simulation obstacles.
    for(let i=0;i<48;i++){const a=i*TAU/48,x=Math.sin(a)*(w*.62),z=Math.cos(a)*(d*.62);object('stone',[x,-3,z],[12,8+(i%5)*2,12],[.36,.42,.40],[0,a,0],staticInstances);}
    for(const o of battle.layout.obstacles||[]){
      const y=height(o.x,o.z),r=o.radius||2;
      if(o.kind==='tree'){
        object('shaft',[o.x,y,o.z],[r*5,3.5,r*5],[.30,.23,.15],[0,0,0],staticInstances);
        object(o.id?.length%2?'pine':'foliage',[o.x,y+2,o.z],[r*.9,(o.height||7)/4,r*.9],[.19+(o.x%7)*.005,.31,.20],[0,o.x,0],staticInstances);
      }else if(o.kind==='rock')object('stone',[o.x,y,o.z],[(o.width||r*2)*2.4,(o.height||4)*3,(o.depth||r*2)*2.4],[.42,.45,.43],[0,0,0],staticInstances);
      else if(o.kind==='building'){
        object('box',[o.x,y,o.z],[o.width||5,o.height||5,o.depth||5],[.65,.59,.46],[0,0,0],staticInstances);
        object('roof',[o.x,y+(o.height||5),o.z],[(o.width||5)*.6,2,(o.depth||5)*.6],[.34,.22,.17],[0,0,0],staticInstances);
        for(const side of [-1,1])object('box',[o.x+side*(o.width||5)*.28,y+1.8,o.z-(o.depth||5)*.51],[.7,.9,.08],[.16,.20,.18],[0,0,0],staticInstances);
      }else if(o.kind==='water')object('box',[o.x,-.12,o.z],[o.width||r*2,.12,o.depth||r*2],[.25,.45,.53],[0,0,0],staticInstances);
    }
    if(battle.layout.bridges?.length||battle.layout.terrain==='river'){
      // Bridge positions come from legal routes, not decorative obstructions.
      for(const bridge of battle.layout.bridges||[{x:0,z:0,width:10,depth:16}]){
        object('box',[bridge.x,.03,bridge.z],[bridge.width||10,.18,bridge.depth||16],[.47,.37,.25],[0,0,0],staticInstances);
        for(const s of [-1,1])object('beam',[bridge.x+s*(bridge.width||10)/2,.6,bridge.z-(bridge.depth||16)/2],[.8,bridge.depth||16,.8],[.35,.28,.2],[Math.PI/2,0,0],staticInstances);
      }
    }
  }
  ground();
  function wall(w,isGate=false){
    const state=w.state||((w.hp||0)<=0?'breached':'intact'),ratio=clamp((w.hp??w.maxHp)/(w.maxHp||100),0,1),base=height(w.x,w.z);
    if(state==='breached'||state==='destroyed'||ratio===0){
      for(let i=0;i<5;i++)object('stone',[w.x+(i-2)*(w.width||8)/5,base-.05,w.z+(i%2-.5)],[(w.width||8)/7,1+(i%2),1.2],[.42,.43,.39],[0,i*1.3,.3]);
      return;
    }
    const h=w.height||6,tint=isGate?[.31,.22,.14]:[.52,.54,.48];
    object('box',[w.x,base,w.z],[w.width||8,h,w.depth||2],tint);
    if(isGate){for(let i=-2;i<=2;i++)object('beam',[w.x+i*.55,base,w.z-(w.depth||2)*.51],[.7,h,.7],[.16,.19,.18]);}
    // Keep the inner wall top walkable: merlons occupy only the outside edge.
    else if((w.depth||2)>(w.width||8)){const outer=w.x<0?-1:1;for(let z=-(w.depth||2)/2+.7;z<(w.depth||2)/2;z+=2)if(ratio>.6||Math.round(z)%3)object('box',[w.x+outer*((w.width||2)/2-.26),base+h,w.z+z],[.52,1.1,1],[.48,.50,.45]);}
    else {const outer=w.id==='wall-rear'?1:-1;for(let x=-(w.width||8)/2+.7;x<(w.width||8)/2;x+=2)if(ratio>.6||Math.round(x)%3)object('box',[w.x+x,base+h,w.z+outer*((w.depth||2)/2-.26)],[1,1.1,.52],[.48,.50,.45]);}
    if(ratio<.8)for(let i=0;i<3;i++)object('stone',[w.x+(i-1)*1.3,base+.15,w.z-1.5],[1.5,.8,1.4],[.45,.45,.4]);
  }
  function animatedNodes(unit,time){
    const model=pack.models[unit.type]||pack.models.levy,frames=new Map(),ownerTint=houseColor(unit.owner);
    const mounted=unit.family==='mounted'&&unit.mounted!==false,dismounted=unit.family==='mounted'&&!mounted;
    const equip=unit.id===previousBattle.commanderId&&pack.weapons?.[unit.weapon];
    const nodes=equip?[...model.nodes.filter(n=>n.name!=='weapon'&&n.parent!=='weapon'),...equip]:model.nodes;
    const dead=unit.status==='dead',action=unit.action,moving=(unit.speed||0)>.15||action?.kind==='stairs',progress=action?clamp(action.elapsed/Math.max(.01,action.duration||1),0,1):0;
    const phase=time*(mounted?13:9)+(String(unit.id).length%5),walk=moving?Math.sin(phase)*.55:Math.sin(time*2)*.015;
    const facing=action?.kind==='stairs'?Math.atan2(action.targetX-action.fromX,action.targetZ-action.fromZ):unit.facing||0;
    const root=transform([unit.x,(unit.y??height(unit.x,unit.z))+(dead?.18:0),unit.z],[dead?1.45:0,facing,0], [1,1,1]);
    for(const node of nodes){
      const pos=[...node.position],rot=[...node.rotation],name=node.name;
      if(dismounted){
        if(name.startsWith('horse')||name.startsWith('ear')||['saddle','tail','mane'].includes(name))continue;
        if(name==='rider')pos[1]=0;
        if(/^thigh[LR]$/.test(name)||/^shin[LR]$/.test(name))rot.fill(0);
      }
      if(!dead){
        if(/^thigh[LR]$/.test(name)&&!mounted)rot[0]+=walk*(name.endsWith('R')?-1:1);
        if(/^shin[LR]$/.test(name)&&!mounted)rot[0]+=Math.max(0,-walk*(name.endsWith('R')?-1:1))*.7;
        if(/^arm[LR]$/.test(name))rot[0]-=walk*(name.endsWith('R')?-1:1)*.65;
        if(/^horseLeg[FB][LR]$/.test(name))rot[0]+=Math.sin(phase+(name.includes('FR')||name.includes('BL')?Math.PI:0))*(moving?.6:.01);
        if(name.startsWith('wheel')&&moving)rot[1]+=time*unit.speed*2;
        if(name==='tail')rot[2]+=Math.sin(time*3)*.18;
        if(name==='cape'||name.startsWith('horseCloth'))rot[0]+=Math.sin(time*5+unit.x)*.1;
        if(unit.blocking&&name==='armL'){rot[0]=-1.1;rot[2]=-.35;}
        if(unit.braced&&name==='armR'){rot[0]=-.7;rot[2]=-.2;}
        if(action?.kind==='climb'){
          if(name==='armL'||name==='armR')rot[0]=-2.6+walk*(name==='armR'?1:-1);
          if(name==='thighL'||name==='thighR')rot[0]=-.4+walk;
        }
        if(action?.kind==='stairs'){
          if(name==='rider')rot[0]=action.ascending?-.12:.08;
          if(/^shin[LR]$/.test(name))rot[0]+=.15;
        }
        if(action?.kind==='attack'){
          const swing=Math.sin(progress*Math.PI);
          if(name==='armR'){rot[0]=-2.4+swing*2.8;rot[2]=-.45+swing*.8;}
          if(name==='forearmR')rot[0]=-.7;
          if(name==='torso')rot[1]=-.25+swing*.5;
        }
        if(action?.kind==='shoot'){
          const reloading=action.elapsed>(action.windup||0);
          if(name==='armR')rot[0]=reloading?-.6:-1.55;
          if(name==='armL'){rot[0]=reloading?-.7:-1.3;rot[2]=reloading?-.1:-.5;}
          if(name==='weapon')rot[0]=reloading?-1.8:-.1;
          if(name==='forearmL')rot[0]=reloading?Math.sin(progress*10)*.5:-1.3;
        }
        if(action?.kind==='siege'){
          if(name==='beam') {if(unit.type==='ram')pos[2]+=Math.sin(progress*TAU)*.65;else rot[0]=-.85+Math.sin(progress*Math.PI)*2.2;}
          if(name==='ramHead')pos[2]+=Math.sin(progress*TAU)*.65;
          if(name.includes('crew')&&name.includes('arm'))rot[0]=-1.2+Math.sin(progress*TAU)*.3;
        }
        if(name==='rider')pos[1]+=moving?Math.abs(Math.sin(phase))*(mounted?.08:.035):Math.sin(time*2)*.015;
      }
      const matrix=multiply(node.parent?frames.get(node.parent)||root:root,transform(pos,rot,node.scale));frames.set(node.name,matrix);
      if(node.mesh){const mesh=node.mesh==='heraldry'?(pack.houses[unit.owner]?.mesh||'crest_ashen'):node.mesh;add(mesh,matrix,node.material==='house'?ownerTint:node.material==='accent'?palette.gold:palette[node.material]||palette.cloth);}
    }
    // A standard marks the actual participating commander; it never counts as a troop.
    if(unit.id===previousBattle.commanderId&&!dead){
      const standard=multiply(root,transform([-.45,1.1,-.3]));add('shaft',multiply(standard,transform([0,0,0],[0,0,0],[1,2.5,1])),palette.wood);
      add('banner',multiply(standard,transform([0,2.4,0],[0,Math.sin(time*2)*.08,0])),ownerTint);
      add(pack.houses[unit.owner]?.mesh||'crest_ashen',multiply(standard,transform([.4,2.16,.09],[0,0,0],[.15,.15,.15])),palette.ivory);
    }
  }
  function ring(x,y,z,r,tint){for(let i=0;i<24;i++){const a=i*TAU/24;object('beam',[x+Math.sin(a)*r,y,z+Math.cos(a)*r],[.22,r*TAU/24,.22],tint,[Math.PI/2,0,Math.PI/2-a]);}}
  function resize(){if(disposed)return;const rect=canvas.getBoundingClientRect(),dpr=Math.min(globalThis.devicePixelRatio||1,1.75),w=Math.max(1,Math.round(rect.width*dpr)),h=Math.max(1,Math.round(rect.height*dpr));if(canvas.width!==w||canvas.height!==h){canvas.width=w;canvas.height=h;}gl.viewport(0,0,w,h);lastAspect=w/h;}
  function render(next,camera={}){
    if(disposed)return;if(lost)throw new Error('The 3D graphics context was lost. Your battle checkpoint is safe; retry or finish automatically.');
    previousBattle=next;resize();instances.clear();for(const [key,value]of staticInstances)instances.set(key,[...value]);
    const commander=next.units.find(u=>u.id===next.commanderId),tactical=camera.mode==='tactical'||!commander||commander.status==='dead';
    const target=camera.target||{x:commander?.x||0,y:commander?.y||0,z:commander?.z||0},yaw=camera.yaw??commander?.facing??0;
    const pitch=clamp(camera.pitch??(tactical?.9:.32),tactical?.45:.04,1.52),distance=clamp(camera.distance??(tactical?70:7),tactical?12:3,150);
    lastTarget=Array.isArray(target)?target:[target.x||0,(target.y||0)+(tactical?0:1.5),target.z||0];
    lastEye=[lastTarget[0]-Math.sin(yaw)*Math.cos(pitch)*distance,lastTarget[1]+Math.sin(pitch)*distance,lastTarget[2]-Math.cos(yaw)*Math.cos(pitch)*distance];
    const vp=multiply(perspective(lastAspect),lookAt(lastEye,lastTarget));
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
    for(const l of next.layout.ladders||[]) {
      const active=l.active||l.progress>0;const slope=active?-.32:-Math.PI/2;
      for(const s of [-1,1])object('beam',[l.x+s*.5,.1,l.z-(active?2:4)],[.35,l.height||6,.35],[.52,.37,.21],[slope,0,0]);
      for(let i=1;i<=8;i++)object('beam',[l.x-.5,active?i*.7:.1,l.z-(active?2:4)-i*(active?.23:.7)],[.25,1,.25],[.52,.37,.21],[0,0,-Math.PI/2]);
    }
    for(const unit of next.units){
      if(unit.status==='reserve'||unit.status==='escaped')continue;
      // Rendering detail never changes simulation membership or casualties.
      const distance=Math.hypot(unit.x-lastEye[0],unit.z-lastEye[2]);
      if(distance>220)continue;
      object('shadow',[unit.x,(unit.wallId||unit.action?.kind==='stairs'?unit.y:height(unit.x,unit.z))+.025,unit.z],[unit.family==='siege'?2:unit.family==='mounted'&&unit.mounted!==false?1.25:.6,1,unit.family==='siege'?2:unit.family==='mounted'&&unit.mounted!==false?1.6:.4],terrainColor.map(c=>c*.66));
      animatedNodes(unit,distance>75?Math.floor(next.time*6)/6:next.time);
      if(unit.status==='active'){
        if(tactical||unit.id===next.commanderId)ring(unit.x,(unit.y||0)+.035,unit.z,unit.family==='siege'?2:unit.family==='mounted'?1:.55,unit.side===next.playerSide?[.40,.79,.83]:[.82,.34,.27]);
        if(unit.selected||camera.selected?.includes?.(unit.id))ring(unit.x,(unit.y||0)+.07,unit.z,1.2,[.95,.84,.34]);
      }
    }
    for(const shot of next.projectiles||[]){
      if(!heardShots.has(shot.id)){heardShots.add(shot.id);cue(shot.kind==='stone'?'siege':'arrow',Math.hypot(shot.x-lastTarget[0],shot.z-lastTarget[2]));}
      const dx=shot.targetX-shot.startX,dz=shot.targetZ-shot.startZ,heading=Math.atan2(dx,dz);
      if(shot.kind==='stone')object('stone',[shot.x,shot.y,shot.z],[1.2,1.2,1.2],[.42,.42,.38],[next.time*4,heading,0]);
      else object('shaft',[shot.x,shot.y,shot.z],[.6,shot.kind==='bolt'?.55:1,.6],[.74,.65,.43],[Math.PI/2,heading,0]);
    }
    for(const event of (next.events||[]).slice(-10)){
      const age=next.time-event.time;if(age<0||age>.45||!Number.isFinite(event.x))continue;
      if(!heardEvents.has(event)){heardEvents.add(event);if(['hit','block','brace','siegeImpact','breach','reinforcements'].includes(event.type))cue(['siegeImpact','breach'].includes(event.type)?'siege':event.type,Math.hypot(event.x-lastTarget[0],event.z-lastTarget[2]));}
      if(['hit','block','brace','siegeImpact','breach'].includes(event.type)){
        const heavy=['siegeImpact','breach'].includes(event.type),size=(heavy?.8:.09)*(1-age/.5);
        for(let i=0;i<3;i++)object('stone',[event.x+Math.sin(i*2+age)*age*(heavy?5:1),height(event.x,event.z)+(heavy?.5:1.3)+age,event.z+Math.cos(i*2)*age],[size,size,size],heavy?[.48,.45,.37]:[.95,.82,.50]);
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
    gl.clearColor(.48,.60,.65,1);gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);gl.enable(gl.DEPTH_TEST);gl.disable(gl.CULL_FACE);gl.useProgram(p);gl.uniformMatrix4fv(uVP,false,vp);gl.uniform3fv(uEye,lastEye);
    for(const [name,data]of instances){
      const geo=buffers.get(name);if(!geo||!data.length)continue;
      gl.bindBuffer(gl.ARRAY_BUFFER,geo.positions);gl.enableVertexAttribArray(aPos);gl.vertexAttribPointer(aPos,3,gl.FLOAT,false,0,0);
      gl.bindBuffer(gl.ARRAY_BUFFER,geo.normals);gl.enableVertexAttribArray(aNormal);gl.vertexAttribPointer(aNormal,3,gl.FLOAT,false,0,0);
      if(ext){
        gl.bindBuffer(gl.ARRAY_BUFFER,instanceBuffer);gl.bufferData(gl.ARRAY_BUFFER,new Float32Array(data),gl.DYNAMIC_DRAW);
        for(let i=0;i<4;i++){gl.enableVertexAttribArray(iAttr[i]);gl.vertexAttribPointer(iAttr[i],4,gl.FLOAT,false,76,i*16);ext.vertexAttribDivisorANGLE(iAttr[i],1);}
        gl.enableVertexAttribArray(iColor);gl.vertexAttribPointer(iColor,3,gl.FLOAT,false,76,64);ext.vertexAttribDivisorANGLE(iColor,1);
        ext.drawArraysInstancedANGLE(gl.TRIANGLES,0,geo.count,data.length/19);
      }else for(let i=0;i<data.length;i+=19){gl.uniformMatrix4fv(uModel,false,new Float32Array(data.slice(i,i+16)));gl.uniform3fv(uColor,data.slice(i+16,i+19));gl.drawArrays(gl.TRIANGLES,0,geo.count);}
    }
    frame++;
  }
  function pick(clientX,clientY){
    const rect=canvas.getBoundingClientRect(),nx=(clientX-rect.left)/rect.width*2-1,ny=1-(clientY-rect.top)/rect.height*2;
    const forward=normalize(lastTarget.map((v,i)=>v-lastEye[i])),right=normalize(cross(forward,[0,1,0])),up=cross(right,forward),tan=Math.tan(Math.PI/6);
    const ray=normalize(forward.map((v,i)=>v+right[i]*nx*tan*lastAspect+up[i]*ny*tan));
    if(ray[1]>=-.0001)return null;
    let d=-lastEye[1]/ray[1],point;
    for(let i=0;i<4;i++){point=lastEye.map((v,k)=>v+ray[k]*d);d=(height(point[0],point[2])-lastEye[1])/ray[1];}
    point=lastEye.map((v,k)=>v+ray[k]*d);
    // Select elevated wall defenders at their visible bodies, not the ground behind them.
    let picked=null,nearest=d;
    for(const unit of previousBattle.units){
      if(unit.status!=='active')continue;
      const radius=unit.family==='siege'?1.8:unit.mounted?1.2:.75,center=[unit.x,(unit.y||0)+(unit.mounted?1.5:.95),unit.z],offset=center.map((v,i)=>v-lastEye[i]),along=dot(offset,ray);
      if(along<=0||along>=nearest)continue;
      if(dot(offset,offset)-along*along<=radius*radius){picked=unit;nearest=along;}
    }
    if(picked)return{x:picked.x,y:picked.y||0,z:picked.z,unitId:picked.id};
    const [x,y,z]=point;
    if(Math.abs(x)>previousBattle.layout.width/2||Math.abs(z)>previousBattle.layout.depth/2)return null;
    const unit=previousBattle.units.filter(u=>u.status==='active').sort((a,b)=>Math.hypot(a.x-x,a.z-z)-Math.hypot(b.x-x,b.z-z))[0];
    return{x,y,z,unitId:unit&&Math.hypot(unit.x-x,unit.z-z)<2.4?unit.id:null};
  }
  const contextLost=event=>{event.preventDefault();lost=true;};canvas.addEventListener('webglcontextlost',contextLost);
  const resizeObserver=typeof ResizeObserver!=='undefined'?new ResizeObserver(resize):null;resizeObserver?.observe(canvas);
  function dispose(){if(disposed)return;disposed=true;resizeObserver?.disconnect();canvas.removeEventListener('webglcontextlost',contextLost);audioSurface.removeEventListener('pointerdown',unlockAudio);audioContext?.close().catch(()=>{});audioContext=null;heardShots.clear();for(const b of buffers.values()){gl.deleteBuffer(b.positions);gl.deleteBuffer(b.normals);}gl.deleteBuffer(instanceBuffer);gl.deleteProgram(p);buffers.clear();instances.clear();staticInstances.clear();releaseBattleAssets();}
  resize();return{render,pick,resize,dispose,setMuted(value){audioMuted=!!value;if(audioMuted)audioContext?.suspend().catch(()=>{});},get stats(){return{drawCalls:instances.size,frames:frame,instanced:!!ext,models:Object.keys(pack.models).length};}};
}
