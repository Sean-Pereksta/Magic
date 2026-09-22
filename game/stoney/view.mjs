import {RULES,center,hash,random,isOnline,finite,trapWindow,roundKey,distance,spawnPoint} from './core.mjs';

export async function createView(canvas,markers){
  const THREE=await import('https://cdn.jsdelivr.net/npm/three@0.160.1/build/three.module.js');
  return new DungeonView(THREE,canvas,markers);
}
class DungeonView {
  constructor(T,canvas,markers){
    this.T=T;this.markers=markers;this.renderer=new T.WebGLRenderer({canvas,antialias:true,powerPreference:'high-performance'});
    this.dpr=Math.min(globalThis.devicePixelRatio||1,matchMedia('(pointer:coarse)').matches?1.25:1.75);this.renderer.setPixelRatio(this.dpr);this.renderer.outputColorSpace=T.SRGBColorSpace;
    this.scene=new T.Scene();this.scene.background=new T.Color(0x070c15);this.scene.fog=new T.FogExp2(0x070c15,.022);
    this.camera=new T.PerspectiveCamera(75,1,.05,220);this.scene.add(new T.HemisphereLight(0xcfe3ff,0x17121b,.9));
    const dir=new T.DirectionalLight(0xdbeafe,.55);dir.position.set(8,18,7);this.scene.add(dir);
    this.torch=new T.PointLight(0xffe6b8,14,18,2);this.scene.add(this.torch);
    this.world=new T.Group();this.scene.add(this.world);this.players=new Map();this.enemies=new Map();this.traps=new Map();this.markerNodes=new Map();this.resources=new Set();this.frameAvg=16;this.lastQuality=0;
    this.v=new T.Vector3();this.matrix=new T.Matrix4();this.reduced=matchMedia('(prefers-reduced-motion:reduce)').matches;this.resize();
  }
  resource(value){this.resources.add(value);return value;}
  material(options){return this.resource(new this.T.MeshStandardMaterial(options));}
  mesh(geo,mat){return new this.T.Mesh(this.resource(geo),mat);}
  resize(){this.renderer.setSize(innerWidth,innerHeight,false);this.camera.aspect=innerWidth/innerHeight;this.camera.updateProjectionMatrix();}
  disposeGroup(group){this.world.remove(group);group.traverse(o=>{o.dispose?.();for(const r of [o.geometry,...(Array.isArray(o.material)?o.material:[o.material])])if(r&&this.resources.has(r)){r.dispose?.();this.resources.delete(r);}});}
  clearWorld(){this.world.traverse(o=>{if(o.isInstancedMesh)o.dispose();});this.world.clear();for(const r of this.resources)r.dispose?.();this.resources.clear();this.players.clear();this.enemies.clear();this.traps.clear();for(const n of this.markerNodes.values())n.remove();this.markerNodes.clear();}
  build(m,key){
    this.clearWorld();this.maze=m;this.key=key;const T=this.T;
    const c=document.createElement('canvas');c.width=c.height=256;const g=c.getContext('2d'),rnd=random(m.seed);
    g.fillStyle='#263343';g.fillRect(0,0,256,256);for(let y=0;y<8;y++)for(let x=-1;x<5;x++){const v=30+Math.floor(rnd()*22);g.fillStyle=`rgb(${v},${v+8},${v+16})`;g.fillRect(x*64+(y%2)*32+2,y*32+2,60,28);}for(let i=0;i<2000;i++){g.fillStyle=`rgba(180,200,220,${rnd()*.10})`;g.fillRect(rnd()*256,rnd()*256,1,1);}
    const texture=this.resource(new T.CanvasTexture(c));texture.colorSpace=T.SRGBColorSpace;texture.wrapS=texture.wrapT=T.RepeatWrapping;
    const walls=this.material({map:texture,roughness:.96}),box=this.resource(new T.BoxGeometry(1,1,1));
    const wallMesh=new T.InstancedMesh(box,walls,m.boxes.length);
    m.boxes.forEach((b,i)=>{this.matrix.compose(new T.Vector3(b.x,RULES.wallHeight/2,b.z),new T.Quaternion(),new T.Vector3(b.sx,RULES.wallHeight,b.sz));wallMesh.setMatrixAt(i,this.matrix);});
    wallMesh.instanceMatrix.needsUpdate=true;wallMesh.computeBoundingSphere();this.world.add(wallMesh);
    const floor=this.mesh(new T.PlaneGeometry(m.w*m.size,m.h*m.size+RULES.roomDepth),this.material({color:0x8896a5,roughness:1}));floor.rotation.x=-Math.PI/2;floor.position.z=RULES.roomDepth/2;this.world.add(floor);
    const ceiling=this.mesh(new T.PlaneGeometry(m.w*m.size,m.h*m.size+RULES.roomDepth),this.material({color:0x111b2c,roughness:1}));ceiling.rotation.x=Math.PI/2;ceiling.position.set(0,RULES.wallHeight,RULES.roomDepth/2);this.world.add(ceiling);
    // Emissive fixtures use one instanced draw, not dozens of point lights.
    const positions=[];for(let i=0;i<m.cells.length;i++)if(i%4===0&&m.neighbors[i].length>1)positions.push(center(m,i));positions.push(spawnPoint(m));
    const lamps=new T.InstancedMesh(this.resource(new T.BoxGeometry(.45,.06,.7)),this.material({color:0xffedc4,emissive:0xffd99a,emissiveIntensity:1.6}),positions.length);
    positions.forEach((p,i)=>{this.matrix.makeTranslation(p.x,RULES.wallHeight-.08,p.z);lamps.setMatrixAt(i,this.matrix);});lamps.computeBoundingSphere();this.world.add(lamps);
    const entrance=center(m,m.entrance.y*m.w+m.entrance.x),exitMat=this.material({color:0x34d399,emissive:0x065f46,emissiveIntensity:1});
    for(const dx of [-m.size*.46,m.size*.46]){const post=this.mesh(new T.BoxGeometry(.16,2.9,.18),exitMat);post.position.set(entrance.x+dx,1.45,m.h*m.size/2+.12);this.world.add(post);}
    const lintel=this.mesh(new T.BoxGeometry(m.size,.14,.18),exitMat);lintel.position.set(entrance.x,2.94,m.h*m.size/2+.12);this.world.add(lintel);
    // Deterministic corner props retain the dungeon's procedural character.
    const props=[];for(let i=0;i<m.cells.length;i++)if(hash(`${m.seed}:${i}`)%31===0){const p=center(m,i);props.push({x:p.x+.95,z:p.z+.95});}
    const crates=new T.InstancedMesh(this.resource(new T.BoxGeometry(.5,.5,.5)),this.material({color:0x735631,roughness:1}),props.length);props.forEach((p,i)=>{this.matrix.makeTranslation(p.x,.25,p.z);crates.setMatrixAt(i,this.matrix);});if(props.length)crates.computeBoundingSphere();this.world.add(crates);
    this.crown=new T.Group();const gold=this.material({color:0xffd54a,emissive:0xb77900,emissiveIntensity:.7,metalness:.6,roughness:.3});
    const ring=this.mesh(new T.TorusGeometry(.3,.065,8,20),gold);ring.rotation.x=Math.PI/2;this.crown.add(ring);
    for(let i=0;i<6;i++){const spike=this.mesh(new T.ConeGeometry(.09,.28,4),gold),angle=i*Math.PI/3;spike.position.set(Math.sin(angle)*.27,.12,Math.cos(angle)*.27);this.crown.add(spike);}
    this.world.add(this.crown);
  }
  playerModel(){const T=this.T,g=new T.Group(),mat=this.material({color:0x60a5fa,emissive:0x12386b,emissiveIntensity:.4,roughness:.8});const body=this.mesh(new T.CapsuleGeometry(.3,.65,4,10),mat);body.position.y=.85;g.add(body);const head=this.mesh(new T.SphereGeometry(.22,10,8),mat);head.position.y=1.53;g.add(head);return{group:g,mat,x:0,z:0,initialized:false};}
  enemyModel(type){
    const T=this.T,g=new T.Group(),ghost=type==='ghost',mat=this.material({color:ghost?0xc4f1ff:0x7c1734,emissive:ghost?0x26748b:0x50021b,emissiveIntensity:.8,roughness:.7,transparent:ghost,opacity:ghost?.8:1});
    const body=this.mesh(new T.SphereGeometry(ghost?.42:.48,14,10),mat);body.scale.y=ghost?1.3:1.05;body.position.y=1.25;g.add(body);
    const tail=this.mesh(new T.ConeGeometry(.4,.85,12),mat);tail.rotation.z=Math.PI;tail.position.y=.55;g.add(tail);
    const eyeMat=this.material({color:ghost?0xffffff:0xffddbe,emissive:ghost?0x70e7ff:0xff321d,emissiveIntensity:2});
    for(const sign of [-1,1]){const eye=this.mesh(new T.SphereGeometry(.055,8,6),eyeMat);eye.position.set(sign*.14,1.34,-.39);g.add(eye);if(!ghost){const horn=this.mesh(new T.ConeGeometry(.105,.45,7),mat);horn.position.set(sign*.32,1.81,0);horn.rotation.z=sign*-.35;g.add(horn);const wing=this.mesh(new T.ConeGeometry(.45,.7,3),mat);wing.scale.z=.12;wing.rotation.z=sign*Math.PI/2;wing.position.set(sign*.7,1.1,.05);g.add(wing);}}
    const tell=this.mesh(new T.RingGeometry(.65,.77,24),this.material({color:0xff8a4b,emissive:0xf54b16,emissiveIntensity:1,transparent:true,opacity:.7,side:T.DoubleSide}));tell.rotation.x=-Math.PI/2;tell.position.y=.04;g.add(tell);
    return{group:g,body,tail,tell,mat,x:0,z:0,initialized:false};
  }
  trapModel(t){const T=this.T,g=new T.Group(),fog=t.type==='fog';
    const mat=this.material({color:fog?0xa5b8cc:0xff8b25,emissive:fog?0x324c69:0xff4000,emissiveIntensity:fog?.2:1.3,transparent:true,opacity:fog?.22:.9,depthWrite:!fog});
    const mesh=this.mesh(fog?new T.SphereGeometry(2.7,12,8):new T.ConeGeometry(.55,1.6,9),mat);mesh.position.y=fog?1.3:.8;g.add(mesh);g.position.set(t.x,0,t.z);this.world.add(g);return{group:g,mesh};
  }
  marker(id,text,p,highlight=false){
    let el=this.markerNodes.get(id);if(!el){el=document.createElement('div');el.className='world-marker';this.markers.append(el);this.markerNodes.set(id,el);}
    el.classList.toggle('crowned',highlight);this.v.set(p.x,2.05,p.z).applyMatrix4(this.camera.matrixWorldInverse);const inFront=this.v.z<0;this.v.set(p.x,2.05,p.z).project(this.camera);
    let x=(this.v.x*.5+.5)*innerWidth,y=(-this.v.y*.5+.5)*innerHeight;
    const edge=!inFront||x<70||x>innerWidth-70||y<100||y>innerHeight-185;
    if(edge){const angle=Math.atan2(p.x-this.camera.position.x,-(p.z-this.camera.position.z))+this.camera.rotation.y;x=innerWidth/2+Math.sin(angle)*(innerWidth/2-80);y=innerHeight/2-Math.cos(angle)*(innerHeight/2-125);}
    el.style.left=`${Math.max(72,Math.min(innerWidth-72,x))}px`;el.style.top=`${Math.max(105,Math.min(innerHeight-170,y))}px`;
    const label=`${highlight?'👑 ':edge?'➤ ':''}${text}`;if(el.textContent!==label)el.textContent=label;el.classList.toggle('edge',edge);
  }
  render(room,me,dt){
    if(!room.maze)return;if(this.key!==roundKey(room.state))this.build(room.maze,roundKey(room.state));const now=room.now(),T=this.T,s=room.state;
    this.camera.position.set(me.x,me.y??RULES.playerHeight,me.z);this.camera.rotation.set(me.pitch||0,me.yaw||0,0,'YXZ');this.camera.updateMatrixWorld();this.torch.position.copy(this.camera.position);
    const seen=new Set(),markerSeen=new Set();for(const p of room.players()){
      if(p.uid===room.uid||p.stoneyRole==='dm'||!isOnline(p,now)||p.alive===false)continue;seen.add(p.uid);
      let o=this.players.get(p.uid);if(!o){o=this.playerModel();this.world.add(o.group);this.players.set(p.uid,o);}
      if(!o.initialized||o.life!==p.life||distance(o,p)>10){o.x=p.x;o.z=p.z;o.initialized=true;o.life=p.life;}
      const k=1-Math.exp(-dt*10);o.x+=(p.x-o.x)*k;o.z+=(p.z-o.z)*k;o.group.position.set(o.x,0,o.z);o.group.rotation.y=finite(p.yaw);
      const crowned=s.carrierId===p.uid;o.mat.color.setHex(crowned?0xffd54a:0x60a5fa);o.mat.emissive.setHex(crowned?0x805400:0x12386b);
      this.marker(p.uid,`${p.name} · ${Math.round(distance(me,o))}m`,o,crowned);markerSeen.add(p.uid);
    }
    for(const [id,o]of this.players)if(!seen.has(id)){this.disposeGroup(o.group);this.players.delete(id);}
    const enemySeen=new Set();if(room.world?.round===roundKey(s))for(const e of room.world.enemies||[]){
      enemySeen.add(e.id);let o=this.enemies.get(e.id);if(!o){o=this.enemyModel(e.type);this.world.add(o.group);this.enemies.set(e.id,o);}
      if(!o.initialized){o.x=e.x;o.z=e.z;o.initialized=true;}
      const k=1-Math.exp(-dt*11);o.x+=(e.x-o.x)*k;o.z+=(e.z-o.z)*k;o.group.position.set(o.x,this.reduced?0:Math.sin(now*.004+(hash(e.id)%10))*.09,o.z);
      const target=room.players().find(p=>p.uid===e.targetId);if(target)o.group.rotation.y=Math.atan2(-(target.x-o.x),-(target.z-o.z));o.tell.visible=e.mode==='windup'||now<finite(e.windupUntil);o.tail.scale.y=this.reduced?1:1+Math.sin(now*.007)*.06;
    }
    for(const [id,o]of this.enemies)if(!enemySeen.has(id)){this.disposeGroup(o.group);this.enemies.delete(id);}
    const trapSeen=new Set();let fogOn=false;
    for(const t of room.activeTraps()){
      const window=trapWindow(t,s),armed=now>=window.armedAt;
      if(t.type==='fire'||t.type==='fog'){trapSeen.add(t.id);let o=this.traps.get(t.id);if(!o){o=this.trapModel(t);this.traps.set(t.id,o);}o.group.visible=s.phase==='play';o.mesh.material.opacity=armed?(t.type==='fog'?.22:.9):.18;if(t.type==='fire'&&!this.reduced)o.mesh.scale.set(1+Math.sin(now*.012)*.08,1+Math.sin(now*.016)*.12,1);if(t.type==='fog'&&armed&&distance(me,t)<7)fogOn=true;}
      else if(!armed&&s.phase==='play'){this.marker(`spawn-${t.id}`,'⚠ Enemy arriving',t,true);markerSeen.add(`spawn-${t.id}`);}
    }
    for(const[id,o]of this.traps)if(!trapSeen.has(id)){this.disposeGroup(o.group);this.traps.delete(id);}
    const carrier=s.carrierId===room.uid?me:room.players().find(p=>p.uid===s.carrierId),crown=carrier||(!s.carrierId?s.crown:null);
    this.crown.visible=!!crown&&s.phase==='play'&&s.carrierId!==room.uid;if(crown){this.crown.position.set(crown.x,carrier?2.1:1.05,crown.z);if(!this.reduced)this.crown.rotation.y+=dt*1.3;}
    if(!s.carrierId&&s.crownDiscovered&&s.crown&&s.phase==='play'){this.marker('dropped-crown','Dropped crown',s.crown,true);markerSeen.add('dropped-crown');}
    for(const[id,n]of this.markerNodes)if(!markerSeen.has(id)){n.remove();this.markerNodes.delete(id);}
    document.getElementById('fog').style.opacity=fogOn?'.85':'0';this.renderer.render(this.scene,this.camera);
    this.frameAvg=this.frameAvg*.98+dt*1000*.02;if(now-this.lastQuality>5000&&this.frameAvg>27&&this.dpr>1){this.dpr=Math.max(1,this.dpr-.2);this.renderer.setPixelRatio(this.dpr);this.lastQuality=now;this.resize();}
  }
  close(){this.clearWorld();this.renderer.dispose();}
}

/** Cached static map; live positions/crown are drawn over it at HUD cadence. */
export function makeMap(canvas,maze){
  const background=document.createElement('canvas');background.width=canvas.width;background.height=canvas.height;const b=background.getContext('2d'),ctx=canvas.getContext('2d'),pad=20,size=Math.min((canvas.width-pad*2)/maze.w,(canvas.height-pad*2)/(maze.h+RULES.roomDepth/maze.size)),x0=(canvas.width-maze.w*size)/2,y0=pad;
  b.fillStyle='#eef3f8';b.fillRect(0,0,canvas.width,canvas.height);b.strokeStyle='#203048';b.lineWidth=Math.max(1.7,size*.075);b.beginPath();for(let y=0;y<maze.h;y++)for(let x=0;x<maze.w;x++){const w=maze.cells[y*maze.w+x].walls,px=x0+x*size,py=y0+y*size;if(w&1){b.moveTo(px,py);b.lineTo(px+size,py);}if(w&8){b.moveTo(px,py);b.lineTo(px,py+size);}if(y===maze.h-1&&w&4){b.moveTo(px,py+size);b.lineTo(px+size,py+size);}if(x===maze.w-1&&w&2){b.moveTo(px+size,py);b.lineTo(px+size,py+size);}}b.stroke();b.fillStyle='#0f766e';b.font='bold 13px system-ui';b.textAlign='center';b.fillText('ENTRANCE / RESPAWN',x0+(maze.entrance.x+.5)*size,y0+maze.h*size+26);
  const point=p=>({x:x0+(p.x+maze.w*maze.size/2)/maze.size*size,y:y0+(p.z+maze.h*maze.size/2)/maze.size*size});
  return {cell(clientX,clientY){const rect=canvas.getBoundingClientRect(),x=Math.floor(((clientX-rect.left)*canvas.width/rect.width-x0)/size),y=Math.floor(((clientY-rect.top)*canvas.height/rect.height-y0)/size);return x>=0&&y>=0&&x<maze.w&&y<maze.h?center(maze,y*maze.w+x):null;},draw(room){ctx.drawImage(background,0,0);const now=room.now(),s=room.state;
    for(const t of room.activeTraps()){if((t.type==='ghost'||t.type==='demon')&&room.world?.round===roundKey(s)&&room.world.enemies?.some(e=>e.id===t.id))continue;const p=point(t);ctx.globalAlpha=now<trapWindow(t,s).armedAt?.4:1;ctx.font=`${Math.max(14,size*.7)}px system-ui`;ctx.textAlign='center';ctx.fillText({fire:'🔥',fog:'🌫️',ghost:'👻',demon:'😈'}[t.type]||'?',p.x,p.y+size*.22);}ctx.globalAlpha=1;
    for(const e of room.world?.round===roundKey(s)?room.world.enemies||[]:[]){const p=point(e);ctx.fillStyle=e.type==='demon'?'#be123c':'#0891b2';ctx.beginPath();ctx.arc(p.x,p.y,Math.max(3,size*.17),0,Math.PI*2);ctx.fill();}
    for(const p of room.players()){if(p.stoneyRole==='dm'||!isOnline(p,now))continue;const q=point(p);if(p.alive!==false){ctx.fillStyle='rgba(220,38,38,.06)';ctx.strokeStyle='rgba(220,38,38,.22)';ctx.beginPath();ctx.arc(q.x,q.y,size*2,0,Math.PI*2);ctx.fill();ctx.stroke();}ctx.fillStyle=p.uid===s.carrierId?'#d99809':p.alive===false?'#8793a6':'#2563eb';ctx.beginPath();ctx.arc(q.x,q.y,size*.2,0,Math.PI*2);ctx.fill();ctx.font='bold 11px system-ui';ctx.textAlign='center';ctx.fillText(`${p.uid===s.carrierId?'👑 ':''}${p.name}`,q.x,Math.min(canvas.height-5,q.y+size*.55));}
    if(!s.carrierId&&s.crown){const q=point(s.crown);ctx.font=`${Math.max(18,size*.85)}px system-ui`;ctx.textAlign='center';ctx.fillText('👑',q.x,q.y+size*.25);}
  }};
}
