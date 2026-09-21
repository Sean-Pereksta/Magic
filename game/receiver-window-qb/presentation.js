/* Lightweight procedural details, shared stadium geometry and bounded decal lifetime. */
(function(root){
'use strict';
const F=root.QBFranchise,T=root.THREE;
const textures=new Map();
function decal(text,color='#ffffff'){
  const key=text+'|'+color;let item=textures.get(key);
  if(!item){const c=document.createElement('canvas');c.width=128;c.height=128;const x=c.getContext('2d');x.clearRect(0,0,128,128);x.textAlign='center';x.textBaseline='middle';x.font='bold 88px "Segoe UI Emoji", "Apple Color Emoji", system-ui';x.fillStyle=color;x.fillText(text,64,67,120);const texture=new T.CanvasTexture(c);texture.colorSpace=T.SRGBColorSpace;item={texture,refs:0};textures.set(key,item);}
  item.refs++;return {key,texture:item.texture};
}
function addDecal(root,text,color,x,y,z,w,h,rotation=0){
  const entry=decal(text,color),mesh=new T.Mesh(new T.PlaneGeometry(w,h),new T.MeshBasicMaterial({map:entry.texture,transparent:true,depthWrite:false,polygonOffset:true,polygonOffsetFactor:-1}));
  mesh.position.set(x,y,z);mesh.rotation.y=rotation;root.add(mesh);(root.userData.decals||=[]).push(entry.key);return mesh;
}
function retain(root){for(const key of root.userData.decals||[]){const entry=textures.get(key);if(entry)entry.refs++;}}
function release(root){for(const key of root.userData.decals||[]){const entry=textures.get(key);if(entry&&--entry.refs<=0){entry.texture.dispose();textures.delete(key);}}root.userData.decals=[];}
function dress(root,look,parts){
  const {body,helmet,head,faceguard,hands,legs,shins,hips,forearms}=parts;
  const seed=F.hash(look.identityId||look.number||'player');
  const material=color=>new T.MeshStandardMaterial({color,roughness:.6});
  const pants=material(look.pantsColor||look.jerseyPrimary),socks=material(look.sockColor||look.jerseyAccent),gloves=material(look.accentColor||look.jerseyAccent);
  hips.material=pants;for(const leg of legs)leg.material=pants;for(const shin of shins)shin.material=socks;for(const hand of hands)hand.material=gloves;
  const bulk=.92+(look.strength||55)*.0014;body.scale.z=bulk;for(const arm of parts.arms)arm.scale.x=bulk;
  const headRig=new T.Group();headRig.position.set(0,1.88,0);root.add(headRig);
  for(const piece of [head,helmet,faceguard]){piece.position.y-=1.88;headRig.add(piece);}root.userData.headRig=headRig;
  helmet.scale.set(1,seed%3===0?1.08:1,seed%3===1?1.06:1);
  if(seed%2===0){const guard=faceguard.clone();guard.position.y-=.07;headRig.add(guard);}
  if(seed%3===0){const visor=new T.Mesh(new T.SphereGeometry(.246,8,4,0,Math.PI,Math.PI*.3,Math.PI*.3),material('#172638'));visor.position.set(0,.06,.04);headRig.add(visor);}
  if(seed%2){for(const arm of forearms)arm.material=material(look.jerseyAccent);}
  for(const hand of hands){const band=new T.Mesh(new T.TorusGeometry(.085,.023,4,8),material(look.secondaryColor||look.jerseyAccent));band.rotation.x=Math.PI/2;band.position.y=.055;hand.add(band);}
  if(seed%3!==0){const towel=new T.Mesh(new T.PlaneGeometry(.14,.3),material(look.secondaryColor||'#ffffff'));towel.position.set(-.22,.74,.24);root.add(towel);}
  const number=String(look.number||11);
  addDecal(root,number,look.numberColor||'#ffffff',0,1.26,.28,.40,.35);
  addDecal(root,number,look.numberColor||'#ffffff',0,1.28,-.28,.43,.38,Math.PI);
  addDecal(root,look.logo||'🏈','#ffffff',.24,1.53,.235,.17,.17);
  for(const side of [-1,1]){
    const logo=addDecal(root,look.logo||'🏈','#ffffff',side*.292,2.02,.015,.24,.24,side*Math.PI/2);
    logo.position.y-=1.88;headRig.add(logo);
  }
}
function animate(a,dt,ball){
  const u=a.mesh.userData,rig=u.visualRig;if(!rig)return;
  const speed=a.velocity?.length()||0,ratio=Math.min(1,speed/(a.maxSpeed||8)),last=a.visualSpeed??speed;
  a.visualAcceleration=dt>0?T.MathUtils.lerp(a.visualAcceleration||0,(speed-last)/Math.max(dt,.001),Math.min(1,dt*9)):0;a.visualSpeed=speed;
  const plant=Math.min(1,a.plantPose||0),cut=a.visualCutSide||1,phase=a.runPhase||0;
  rig.rotation.x+=T.MathUtils.clamp(a.visualAcceleration*.014,-.12,.17)+ratio*.07;
  rig.rotation.z+=plant*cut*.12;
  rig.position.y+=Math.abs(Math.sin(phase))*ratio*.028-plant*.035;
  u.body.rotation.y+=Math.sin(phase)*ratio*.065;
  if(u.headRig){
    let yaw=-Math.sin(phase)*ratio*.045,pitch=-ratio*.035;
    if(a.trackingBall||a.ballSeen){const target=ball.position.clone();a.mesh.worldToLocal(target);yaw=T.MathUtils.clamp(Math.atan2(target.x,target.z),-.65,.65);pitch=T.MathUtils.clamp(-Math.atan2(target.y-1.9,Math.hypot(target.x,target.z)),-.4,.3);}
    u.headRig.rotation.y=T.MathUtils.lerp(u.headRig.rotation.y,yaw,Math.min(1,dt*9));u.headRig.rotation.x=pitch;
  }
  if(a.stiffArmTime>0&&a.hasBall){const t=1-a.stiffArmTime/.42,envelope=Math.sin(Math.PI*Math.min(1,t));rig.rotation.z-=envelope*.15;rig.rotation.y-=envelope*.1;u.hands[1].position.set(.26,1.26,.27);}
  if(a.contactReaction>0){a.contactReaction=Math.max(0,a.contactReaction-dt);const p=Math.sin(Math.PI*a.contactReaction/(a.contactDuration||.7)),side=a.contactSide||1;rig.rotation.z+=side*p*(a.contactPower>.8?1.15:.48);rig.rotation.y+=side*p*.6;rig.position.y-=p*(a.contactPower>.8?.55:.16);}
  if(a.hurdleTime>0){const flight=T.MathUtils.clamp((a.jumpY||0)/.9,0,1);rig.rotation.x-=flight*.15;for(const h of u.hands)h.position.y+=flight*.25;}
  if(a.stumbleTime>0){const weight=a.style==='Power Receiver'?.7:1,p=Math.sin(a.stumbleTime/.48*Math.PI);rig.rotation.x+=p*.12*weight;u.hands[0].position.set(-.25,1.08,.48);}
}
function stadium(scene){
  const group=new T.Group();scene.add(group);const metal=new T.MeshStandardMaterial({color:0x788391,roughness:.85}),dark=new T.MeshStandardMaterial({color:0x18232e}),team=new T.MeshStandardMaterial({color:0x235fba}),orange=new T.MeshBasicMaterial({color:0xff882e});
  const geo=new T.BoxGeometry(1,1,1),people=new T.CapsuleGeometry(.22,.8,2,4),head=new T.SphereGeometry(.19,6,4),skin=new T.MeshStandardMaterial({color:0xb17c59});
  const structures=[];
  function box(x,y,z,w,h,d,mat=metal,parent=group){const m=new T.Mesh(geo,mat);m.position.set(x,y,z);m.scale.set(w,h,d);parent.add(m);return m;}
  for(const side of [-1,1]){
    for(const z of [-12,2,16])box(side*28.5,.7,z,.9,.18,7);
    box(side*29.5,.08,2,2,.06,34,team); // coaching box
    for(let i=0;i<14;i++){const x=side*(28+(i%2)*1.2),z=-17+i*2.6;const person=new T.Mesh(people,i%4===0?dark:team);person.position.set(x,.78,z);group.add(person);const h=new T.Mesh(head,skin);h.position.set(x,1.58,z);group.add(h);}
    for(const z of [-45,30]){box(side*28,.65,z,.55,1.3,.55,dark);box(side*27.6,1.1,z,.35,.28,.5,dark);} // photographers
    const tunnel=new T.Group();box(side*38,3,-43,9,6,7,dark,tunnel);box(side*33.2,5.9,-43,.2,.4,7,team,tunnel);group.add(tunnel);structures.push(tunnel);
    box(side*27.7,.65,-34,.08,1.3,14,team);
  }
  const marker=new T.Group();box(27.3,1.2,0,.1,2.4,.1,orange,marker);box(27.3,2.45,0,.7,.4,.13,orange,marker);group.add(marker);
  const chain=new T.Group();for(const z of [0,50])box(28.2,1,z,.08,2,.08,orange,chain);box(28.2,.2,25,.04,.04,50,orange,chain);group.add(chain);
  const crowd=new T.InstancedMesh(new T.BoxGeometry(.45,.8,.4),team,480);const dummy=new T.Object3D();
  for(let i=0;i<480;i++){const side=i<240?-1:1,j=i%240;dummy.position.set(side*(30+Math.floor(j/60)*2),2.8+Math.floor(j/60)*1.7,-64+(j%60)*1.8);dummy.updateMatrix();crowd.setMatrixAt(i,dummy.matrix);crowd.setColorAt(i,new T.Color(i%4===0?'#ece4cf':i%3===0?'#4f6178':'#244970'));}group.add(crowd);
  const bowl=new T.Group();for(let i=0;i<4;i++)box(0,3+i*2,-83-i*2,66,.8,2,metal,bowl);group.add(bowl);
  const skyline=new T.Group();for(let i=0;i<12;i++)box(-58+i*10,8+(i%3)*3,-96,6,16+(i%3)*6,7,dark,skyline);group.add(skyline);
  const roof=new T.Group();for(const side of [-1,1])box(side*38,14,-10,20,.5,120,metal,roof);group.add(roof);
  const canvas=document.createElement('canvas');canvas.width=1024;canvas.height=256;const ctx=canvas.getContext('2d'),texture=new T.CanvasTexture(canvas);texture.colorSpace=T.SRGBColorSpace;
  const scoreboard=new T.Mesh(new T.PlaneGeometry(19,4.75),new T.MeshBasicMaterial({map:texture}));scoreboard.position.set(0,10,-78);group.add(scoreboard);box(0,5,-78,.5,10,.5,dark);
  const banner=addDecal(group,'🐺','#ffffff',-28,3,-26,2,2,Math.PI/2);let key='',phase=0,opponent=null;
  return {
    theme(round){opponent=F.opponent(round);team.color.set(opponent.primary);skyline.visible=opponent.venue===3;roof.visible=opponent.venue===5;bowl.visible=opponent.venue===1||opponent.venue===5;structures.forEach(s=>s.visible=opponent.venue!==0);crowd.count=opponent.venue===0?160:opponent.venue===4?240:480;key='';},
    updateMarkers(spot,gain){marker.position.z=gain;marker.visible=gain>-60;chain.position.z=spot-50;},
    score(home,away,a,b,round){const next=[home.name,home.logo,away.name,away.logo,a,b,round].join('|');if(next===key)return;key=next;ctx.fillStyle='#081320';ctx.fillRect(0,0,1024,256);ctx.fillStyle='#a9bacd';ctx.font='28px system-ui';ctx.textAlign='center';ctx.fillText('ROUND '+round+'  •  RECEIVER WINDOW',512,43);ctx.fillStyle='#ffffff';ctx.font='bold 68px "Segoe UI Emoji", system-ui';ctx.fillText(home.logo+'  '+a+'  —  '+b+'  '+away.logo,512,137);ctx.font='28px system-ui';ctx.fillText(home.name+'  /  '+away.name,512,210);texture.needsUpdate=true;},
    animate(dt,low){phase+=dt;if(low)return;crowd.position.y=Math.sin(phase*2)*.035;banner.rotation.z=Math.sin(phase*1.5)*.025;},
    group,crowd
  };
}
root.QBPresentation={dress,retain,release,animate,stadium,get textureCount(){return textures.size;}};
})(globalThis);
