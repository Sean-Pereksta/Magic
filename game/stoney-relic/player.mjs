import * as THREE from 'https://cdn.jsdelivr.net/npm/three@0.160.1/build/three.module.js';
import {RelicSession} from './network.mjs';
import {RULES,finite,fresh,living,distance,clamp,hash,runKey,trapWindow,usableTrap,remotePosition,hazardAt} from './core.mjs';
import {$,text,element,color,roster,updateHUD,notifications,drawMap,fatal,lifecycle} from './ui.mjs';

async function main(){
  const qs=new URLSearchParams(location.search),session=new RelicSession({gameId:(qs.get('gameId')||'').trim(),name:qs.get('username')});
  const touch=matchMedia('(pointer:coarse)').matches;document.body.classList.toggle('touch',touch);
  const renderer=new THREE.WebGLRenderer({antialias:!touch,powerPreference:'high-performance'});
  renderer.setPixelRatio(Math.min(devicePixelRatio||1,touch?1.35:1.75));renderer.setSize(innerWidth,innerHeight);renderer.outputColorSpace=THREE.SRGBColorSpace;
  document.body.prepend(renderer.domElement);renderer.domElement.setAttribute('aria-label','Stoney’s Relic dungeon');
  const scene=new THREE.Scene();scene.background=new THREE.Color(0x090f1b);scene.fog=new THREE.FogExp2(0x090f1b,.018);
  scene.add(new THREE.HemisphereLight(0xc0d9ff,0x23201d,1.15));
  const fill=new THREE.DirectionalLight(0xe4eeff,.4);fill.position.set(5,12,3);scene.add(fill);
  const camera=new THREE.PerspectiveCamera(75,innerWidth/innerHeight,.05,180),world=new THREE.Group(),actors=new THREE.Group();scene.add(world,actors);
  const lamps=Array.from({length:touch?3:5},()=>{const light=new THREE.PointLight(0xffdea5,11,15,2);scene.add(light);return light;});
  const remotes=new Map(),hazards=new Map(),labels=new Map();let points=[],crownMesh=null,maze=null,lastRun='',spawnSeq=-1;
  let pos={x:0,z:0},yaw=0,pitch=0,vx=0,vz=0,jump=0,vy=0,paused=true,stopped=false,last=performance.now(),lastUI=0,lastLights=0;
  const resetSticks=[],keys=new Set(),sticks={move:{x:0,y:0},look:{x:0,y:0}},screen=$('screen');let screenMode='';
  function resetInput(){keys.clear();for(const reset of resetSticks)reset();for(const key of Object.keys(sticks)){sticks[key]={x:0,y:0};const node=$(`${key}-stick`);if(node)node.querySelector('i').style.transform='';}vx=vz=0;}
  function dispose(root){const geometries=new Set(),materials=new Set(),textures=new Set();root.traverse(o=>{if(o.geometry)geometries.add(o.geometry);for(const m of [].concat(o.material||[])){materials.add(m);if(m.map)textures.add(m.map);}});for(const x of [...geometries,...materials,...textures])x.dispose();root.clear();}
  function mesh(geometry,material,parent=world,x=0,y=0,z=0){const m=new THREE.Mesh(geometry,material);m.position.set(x,y,z);parent.add(m);return m;}
  const solid=(c,more={})=>new THREE.MeshLambertMaterial({color:c,...more});
  function texture(kind){const c=document.createElement('canvas');c.width=c.height=128;const g=c.getContext('2d');g.fillStyle=kind==='wall'?'#526071':'#818998';g.fillRect(0,0,128,128);
    g.strokeStyle=kind==='wall'?'#293242':'#545e6d';g.lineWidth=3;
    if(kind==='wall')for(let row=0;row<4;row++){g.beginPath();g.moveTo(0,row*32);g.lineTo(128,row*32);g.stroke();for(let col=-1;col<3;col++){const x=col*64+(row%2)*32;g.beginPath();g.moveTo(x,row*32);g.lineTo(x,(row+1)*32);g.stroke();}}
    else for(let i=0;i<4;i++){g.strokeRect(i*32,0,32,128);g.strokeRect(0,i*32,128,32);}
    for(let i=0;i<500;i++){g.fillStyle=`rgba(0,0,0,${(i%5)*.02})`;g.fillRect(hash(i)%128,hash(`r${i}`)%128,2,2);}
    const t=new THREE.CanvasTexture(c);t.colorSpace=THREE.SRGBColorSpace;t.wrapS=t.wrapT=THREE.RepeatWrapping;t.repeat.set(kind==='wall'?2:18,kind==='wall'?3:18);return t;
  }
  function build(){
    dispose(world);dispose(actors);remotes.clear();hazards.clear();for(const n of labels.values())n.remove();labels.clear();points=[];
    const wallMat=solid(0xffffff,{map:texture('wall')}),walls=new THREE.InstancedMesh(new THREE.BoxGeometry(1,1,1),wallMat,maze.boxes.length),dummy=new THREE.Object3D();
    maze.boxes.forEach((b,i)=>{dummy.position.set(b.x,3.5,b.z);dummy.scale.set(b.sx,7,b.sz);dummy.updateMatrix();walls.setMatrixAt(i,dummy.matrix);});walls.instanceMatrix.needsUpdate=true;walls.computeBoundingSphere();world.add(walls);
    const floor=mesh(new THREE.PlaneGeometry(maze.w*maze.size,maze.h*maze.size+9),solid(0xffffff,{map:texture('floor')}),world,0,0,4.5);floor.rotation.x=-Math.PI/2;
    const ceil=mesh(new THREE.PlaneGeometry(maze.w*maze.size,maze.h*maze.size+9),solid(0x1a2435,{side:THREE.DoubleSide}),world,0,7,4.5);ceil.rotation.x=Math.PI/2;
    const entry=maze.center(maze.entry),arch=solid(0x64bf91);
    // An actual open arch; the previous solid "frame" visually blocked the doorway.
    mesh(new THREE.BoxGeometry(.18,2.8,.18),arch,world,entry.x-maze.size*.43,1.4,maze.maxZ+.22);
    mesh(new THREE.BoxGeometry(.18,2.8,.18),arch,world,entry.x+maze.size*.43,1.4,maze.maxZ+.22);
    mesh(new THREE.BoxGeometry(maze.size*.9,.18,.18),arch,world,entry.x,2.85,maze.maxZ+.22);
    for(let y=1;y<maze.h;y+=4)for(let x=1;x<maze.w;x+=4)points.push({...maze.center(y*maze.w+x),y:5.7});points.push({...maze.spawn(),y:4});
    const bulbs=new THREE.InstancedMesh(new THREE.SphereGeometry(.17,8,6),new THREE.MeshBasicMaterial({color:0xffe8b2}),points.length);points.forEach((p,i)=>{dummy.position.set(p.x,p.y,p.z);dummy.scale.set(1,1,1);dummy.updateMatrix();bulbs.setMatrixAt(i,dummy.matrix);});bulbs.computeBoundingSphere();world.add(bulbs);
    crownMesh=new THREE.Group();const gold=new THREE.MeshStandardMaterial({color:0xffd568,emissive:0x8e600c,emissiveIntensity:.5,metalness:.55,roughness:.35});
    mesh(new THREE.CylinderGeometry(.28,.3,.13,12),gold,crownMesh);
    for(let i=0;i<5;i++){const a=i*Math.PI*2/5;mesh(new THREE.ConeGeometry(.08,.27,5),gold,crownMesh,Math.cos(a)*.24,.17,Math.sin(a)*.24);}actors.add(crownMesh);
  }
  function playerMesh(id){const group=new THREE.Group();mesh(new THREE.CapsuleGeometry(.31,.66,4,10),solid(color(id)),group,0,.84,0);mesh(new THREE.SphereGeometry(.2,10,8),solid(0xeac5a7),group,0,1.44,0);mesh(new THREE.BoxGeometry(.28,.09,.12),solid(0x182535),group,0,1.48,-.16);actors.add(group);return group;}
  function trapMesh(t){const group=new THREE.Group(),kind=t.type;
    if(kind==='fire'){for(let i=0;i<3;i++){const flame=mesh(new THREE.ConeGeometry(.32-i*.05,1.35-i*.18,7),new THREE.MeshBasicMaterial({color:i%2?0xffdb76:0xff7044,transparent:true,opacity:.85}),group,(i-1)*.3,.6,0);flame.rotation.z=(i-1)*.17;}}
    if(kind==='fog')mesh(new THREE.SphereGeometry(2.3,12,8),solid(0xb5cde4,{transparent:true,opacity:.2,depthWrite:false}),group,0,1.5,0);
    if(kind==='ghost'||kind==='demon'){
      const material=solid(kind==='ghost'?0xbceefa:0x8b3450,{transparent:kind==='ghost',opacity:kind==='ghost'?.8:1});mesh(new THREE.SphereGeometry(.48,12,10),material,group,0,1.3,0);mesh(new THREE.ConeGeometry(.49,.9,10),material,group,0,.78,0);
      const eye=new THREE.MeshBasicMaterial({color:kind==='ghost'?0x112a47:0xffd384});for(const x of [-.18,.18])mesh(new THREE.SphereGeometry(.09,8,6),eye,group,x,1.38,-.42);
      if(kind==='demon')for(const x of [-.32,.32])mesh(new THREE.ConeGeometry(.14,.42,6),solid(0xe0af99),group,x,1.8,0);
    }
    group.position.set(t.x,0,t.z);actors.add(group);return group;
  }
  function marker(id,label,p,isCrown=false){let node=labels.get(id);if(!node){node=element('div','marker');$('markers').append(node);labels.set(id,node);}node.classList.toggle('crown',isCrown);node.dataset.seen='1';
    const v=new THREE.Vector3(p.x,2.1,p.z).project(camera),bearing=Math.atan2(-(p.x-pos.x),-(p.z-pos.z))-yaw,angle=Math.atan2(Math.sin(bearing),Math.cos(bearing));
    let x=(v.x*.5+.5)*innerWidth,y=(-v.y*.5+.5)*innerHeight;const off=v.z>1||v.z<-1||x<55||x>innerWidth-55||y<110||y>innerHeight-165;
    if(off){x=innerWidth/2-Math.sin(angle)*(innerWidth/2-70);y=innerHeight/2-Math.cos(angle)*Math.max(10,innerHeight/2-135);}
    x=clamp(x,60,Math.max(60,innerWidth-60));y=clamp(y,110,Math.max(110,innerHeight-145));
    const arrows=['↑','↖','←','↙','↓','↘','→','↗'];const arrow=arrows[(Math.round(angle/(Math.PI/4))+8)%8];
    text(node,`${isCrown?'👑 ':''}${off?arrow+' ':''}${label} · ${Math.round(distance(pos,p))}m`);const half=Math.min(innerWidth/2-8,node.getBoundingClientRect().width/2+8);x=clamp(x,half,innerWidth-half);y=Math.max(Math.min(200,innerHeight*.42),y);node.style.transform=`translate(${x}px,${y}px) translate(-50%,-50%)`;
  }
  function overlay(mode,title,description,play=false){screen.hidden=!mode;if(screenMode===mode)return;screenMode=mode;if(!mode)return;screen.replaceChildren(element('h1','',title),element('p','',description));if(play){const b=element('button','primary','Enter dungeon');b.onclick=async()=>{paused=false;try{if(!touch)await renderer.domElement.requestPointerLock?.();b.blur();}catch{paused=true;}screenMode='';};screen.append(b);}if(mode==='sync'){const retry=element('button','','Reconnect');retry.onclick=()=>location.reload();screen.append(retry);}const home=element('button','','Back to lobby');home.onclick=()=>location.assign('/');screen.append(home);}
  function draw(now){if(stopped)return;const dt=Math.min(.033,(now-last)/1000);last=now;const time=session.now(),s=session.s,p=session.self;
    if(session.maze&&(session.maze!==maze||runKey(s)!==lastRun)){maze=session.maze;lastRun=runKey(s);build();spawnSeq=-1;}
    if(session.pose&&session.pose.spawnSeq!==spawnSeq){spawnSeq=session.pose.spawnSeq;pos={x:session.pose.x,z:session.pose.z};yaw=finite(session.pose.yaw);vx=vz=vy=jump=0;}
    const activeRound=s?.phase==='play'&&session.synchronized&&p?.runId===runKey(s)&&session.pose?.runId===runKey(s)&&!document.hidden&&p?.alive!==false;const playable=activeRound&&!paused&&(touch||document.pointerLockElement===renderer.domElement);
    const previous={...pos};
    if(playable&&maze&&!session.actions.get('caught')?.busy){
      yaw-=sticks.look.x*2.8*dt;pitch=clamp(pitch-sticks.look.y*2.2*dt,-1.35,1.35);
      let mx=touch?sticks.move.x:Number(keys.has('d')||keys.has('arrowright'))-Number(keys.has('a')||keys.has('arrowleft'));
      let mz=touch?-sticks.move.y:Number(keys.has('w')||keys.has('arrowup'))-Number(keys.has('s')||keys.has('arrowdown'));
      const magnitude=Math.hypot(mx,mz);if(magnitude>1){mx/=magnitude;mz/=magnitude;}
      const speed=keys.has('shift')||touch&&magnitude>.86?5.4:3.9,sy=Math.sin(yaw),cy=Math.cos(yaw),accel=Math.min(1,18*dt);
      vx+=((mx*cy-mz*sy)*speed-vx)*accel;vz+=((-mx*sy-mz*cy)*speed-vz)*accel;
      pos=maze.move(pos,vx*dt,vz*dt);if(keys.has(' ')&&jump===0)vy=3.6;vy-=10.5*dt;jump=Math.max(0,jump+vy*dt);if(!jump)vy=0;
    }else vx=vz=0;
    camera.position.set(pos.x,1.72+jump,pos.z);camera.rotation.set(pitch,yaw,0,'YXZ');camera.updateMatrixWorld();
    session.setPosition({...pos,yaw,vx,vz});
    if(maze){
      const seen=new Set();for(const [id,other] of roster(session)){if(id===session.uid||!living(other,time,s))continue;seen.add(id);let object=remotes.get(id);if(!object){object=playerMesh(id);object.position.set(other.x,0,other.z);remotes.set(id,object);}const target=remotePosition(other,time,maze),current={x:object.position.x,z:object.position.z},alpha=1-Math.exp(-dt*12);
        const next=distance(current,target)>5?target:maze.move(current,(target.x-current.x)*alpha,(target.z-current.z)*alpha);object.position.set(next.x,0,next.z);object.rotation.y=finite(other.yaw);object.visible=!other.hidden;}
      for(const [id,object] of remotes)if(!seen.has(id)){actors.remove(object);dispose(object);remotes.delete(id);}
      let fog=false;const active=new Set();for(const [id,t] of session.traps){if(!usableTrap(t,s,time))continue;active.add(id);let object=hazards.get(id);if(!object){object=trapMesh(t);hazards.set(id,object);}const window=trapWindow(t,s),armed=s.phase==='play'&&time>=window.armedAt;
        const e=session.enemies[id],target=e||t,current={x:object.position.x,z:object.position.z};
        const alpha=1-Math.exp(-dt*16),next=distance(current,target)>5?target:maze.move(current,(target.x-current.x)*alpha,(target.z-current.z)*alpha,.25);
        object.position.set(next.x,['ghost','demon'].includes(t.type)?Math.sin(now*.003+hash(id))*.1:0,next.z);object.visible=s.phase==='play';object.scale.setScalar(armed?1:.65+.12*Math.sin(now*.014));
        if(e&&(e.vx||e.vz))object.rotation.y=Math.atan2(-e.vx,-e.vz);if(t.type==='fire')object.scale.y*=1+.09*Math.sin(now*.018);
        if(t.type==='fog'&&armed&&distance(pos,t)<6.5)fog=true;
      }
      for(const [id,object] of hazards)if(!active.has(id)){actors.remove(object);dispose(object);hazards.delete(id);}
      $('fog').style.opacity=fog?'1':'0';
      const holder=s?.carrierId===session.uid?pos:session.players.get(s?.carrierId),crown=s?.carrierId?holder:s?.crown;
      crownMesh.visible=s?.phase==='play'&&!!crown&&s?.carrierId!==session.uid&&(!s?.carrierId||fresh(holder,time));if(crownMesh.visible){const visual=remotes.get(s.carrierId);crownMesh.position.set(visual?.position.x??crown.x,s.carrierId?2.04:1+Math.sin(now*.003)*.1,visual?.position.z??crown.z);crownMesh.rotation.y=now*.001;}
      if(activeRound){const self={...p,...pos,hidden:false,updatedAt:time,serverAt:time};const drawnEnemies=Object.fromEntries([...hazards].filter(([id])=>session.enemies[id]).map(([id,object])=>[id,object.position]));const hit=hazardAt(self,previous,s,session.traps,drawnEnemies,maze,time);if(hit)void session.playerAction('caught',hit);else if(playable&&s.carrierId===session.uid&&maze.inRoom(pos))void session.playerAction('escape');else if(playable&&!s.carrierId&&s.crown&&distance(pos,s.crown)<=1.6)void session.playerAction('pickup');}
      if(p?.alive===false&&time>=finite(p.deadUntil)&&s?.phase==='play')void session.playerAction('respawn');
      if(now-lastLights>400){lastLights=now;const nearby=[...points].sort((a,b)=>distance(pos,a)-distance(pos,b));lamps.forEach((light,i)=>{const point=nearby[i];light.visible=!!point;if(point)light.position.set(point.x,point.y,point.z);});}
    }
    if(now-lastUI>120){lastUI=now;updateHUD(session,pos);if(maze)drawMap($('radar'),session,{origin:pos});for(const n of labels.values())n.dataset.seen='0';
      if(s?.phase==='play'&&maze){for(const [id,other] of roster(session)){if(id!==session.uid&&living(other,time,s)&&!other.hidden)marker(id,other.name,remotes.get(id)?.position||other,s.carrierId===id);}if(s.carrierId===session.uid)marker('exit','EXIT',maze.spawn(),true);}
      for(const [id,node] of labels)if(node.dataset.seen!=='1'){node.remove();labels.delete(id);}
      if(!session.synchronized)overlay('sync','Connecting to the dungeon',session.status+' — gameplay waits for confirmed state.');
      else if(!s||s.phase==='setup')overlay('setup','The Dungeon Master is preparing the maze','The doors open when the DM is ready or the setup timer ends.');
      else if(s.phase==='ended')overlay(`ended:${s.winner}`,s.winner==='players'?'🏆 Players win!':'😈 Dungeon Master wins!',s.winner==='players'?`${s.winnerName||'A teammate'} escaped with the crown.`:'The five-minute timer expired.');
      else if(p?.alive===false)overlay(`dead:${Math.ceil((p.deadUntil-time)/1000)}`,'Caught — returning to the entrance',`Respawning at the entrance in ${Math.max(0,Math.ceil((p.deadUntil-time)/1000))} seconds. You will have three seconds of protection.`);
      else if(paused||!touch&&document.pointerLockElement!==renderer.domElement)overlay('paused','Find the crown. Escape together.',touch?'Left stick: move; push fully to sprint. Right stick: look. Gold markers identify the crown carrier. Multiplayer continues while this menu is open.':'WASD or arrows: move · Shift: sprint · Space: jump · Esc: pause. Teammate labels remain visible through walls. Multiplayer continues while this menu is open.',true);
      else overlay('');
    }
    renderer.render(scene,camera);requestAnimationFrame(draw);
  }
  window.addEventListener('keydown',e=>{if(['INPUT','TEXTAREA','BUTTON'].includes(document.activeElement?.tagName))return;keys.add(e.key.toLowerCase());if([' ','ArrowUp','ArrowDown','ArrowLeft','ArrowRight'].includes(e.key))e.preventDefault();});
  window.addEventListener('keyup',e=>keys.delete(e.key.toLowerCase()));window.addEventListener('blur',resetInput);document.addEventListener('visibilitychange',resetInput);
  document.addEventListener('mousemove',e=>{if(document.pointerLockElement!==renderer.domElement||paused)return;yaw-=e.movementX*.0022;pitch=clamp(pitch-e.movementY*.0022,-1.35,1.35);});
  document.addEventListener('pointerlockchange',()=>{if(!document.pointerLockElement){paused=true;resetInput();}});
  $('menu').onclick=()=>{paused=true;resetInput();document.exitPointerLock?.();};
  $('fullscreen').onclick=async()=>{try{if(document.fullscreenElement)await document.exitFullscreen();else await document.documentElement.requestFullscreen();}catch(error){session.report('Fullscreen',error);}};
  for(const key of ['move','look']){const node=$(`${key}-stick`),knob=node.querySelector('i');let pointer=null;
    const move=e=>{if(pointer!==e.pointerId)return;const r=node.getBoundingClientRect(),radius=r.width*.36,dx=(e.clientX-r.left-r.width/2)/radius,dy=(e.clientY-r.top-r.height/2)/radius,n=Math.max(1,Math.hypot(dx,dy));sticks[key]={x:dx/n,y:dy/n};knob.style.transform=`translate(${sticks[key].x*radius}px,${sticks[key].y*radius}px)`;};
    node.addEventListener('pointerdown',e=>{if(pointer!==null)return;pointer=e.pointerId;node.setPointerCapture(pointer);move(e);e.preventDefault();});node.addEventListener('pointermove',move);
    const end=e=>{if(e.pointerId!==pointer)return;pointer=null;sticks[key]={x:0,y:0};knob.style.transform='';};for(const event of ['pointerup','pointercancel','lostpointercapture'])node.addEventListener(event,end);resetSticks.push(()=>{const id=pointer;pointer=null;if(id!==null&&node.hasPointerCapture(id))node.releasePointerCapture(id);});
  }
  window.addEventListener('resize',()=>{renderer.setSize(innerWidth,innerHeight);camera.aspect=innerWidth/innerHeight;camera.updateProjectionMatrix();});
  const off=notifications(session);lifecycle(session,()=>{stopped=true;off();dispose(world);dispose(actors);renderer.dispose();});
  await session.start();requestAnimationFrame(draw);
}
main().catch(fatal);
