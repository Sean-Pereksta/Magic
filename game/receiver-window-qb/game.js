
(()=>{
'use strict';
let gameTime=performance.now();
let pendingCatch=null,contactStep=1/120,ballTumble=0,ballSpin=0,receiverDrop=false;
const previousBallAxis=new THREE.Vector3(0,0,1);
const P=QBProgression,V=QBVariety,F=QBFranchise,R=globalThis.QBPresentation;
let studio=null,stadiumDetails=null,playLog=null,previewPlayIndex=null;
let snapDefense=null,snapMemory=V.tendencies([]),attemptPending=false,throwBobbles=0,releaseTiming=[];
let menuOpen=true,saveOpen=false,cloudBusy=false,activeSlot=0,transition=null,replay=null,replayFrames=[],replayEligible=false,replayRecording=false,replaySample=0,throwDown=1,replayPool=[],replayInterval=1/30;
const SLOT_KEY='receiverWindowQB_slots_v2';
let slots=[];
try{slots=JSON.parse(localStorage.getItem(SLOT_KEY)||'[]');if(!Array.isArray(slots))slots=[];}catch{}
slots=slots.slice(0,3);
try{activeSlot=Math.max(0,Math.min(2,Number(localStorage.getItem('receiverWindowQB_activeSlot'))||0))}catch{}
const escapeHTML=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const $=id=>document.getElementById(id);
const canvas=$('game');
let renderer;try{renderer=new THREE.WebGLRenderer({canvas,antialias:true,powerPreference:'high-performance'})}catch{ $('startCard').querySelector('p').textContent='3D graphics could not start. Enable hardware acceleration or open this game in a WebGL-capable browser.';return;}
renderer.setPixelRatio(Math.min(devicePixelRatio||1,1.7));renderer.shadowMap.enabled=true;renderer.shadowMap.type=THREE.PCFSoftShadowMap;renderer.outputColorSpace=THREE.SRGBColorSpace;
const scene=new THREE.Scene();scene.background=new THREE.Color(0x7fb7d6);scene.fog=new THREE.Fog(0x7fb7d6,78,155);
const camera=new THREE.PerspectiveCamera(67,innerWidth/innerHeight,.08,220);camera.position.set(0,2.25,44);let yaw=0,pitch=-.08;
function applyCamera(){camera.rotation.order='YXZ';camera.rotation.y=yaw;camera.rotation.x=pitch;camera.rotation.z=0}applyCamera();
const ambient=new THREE.HemisphereLight(0xe8f6ff,0x31542a,2.15);scene.add(ambient);const sun=new THREE.DirectionalLight(0xffffff,2.2);sun.position.set(-35,55,25);sun.castShadow=true;sun.shadow.mapSize.set(1024,1024);sun.shadow.camera.left=-65;sun.shadow.camera.right=65;sun.shadow.camera.top=90;sun.shadow.camera.bottom=-25;scene.add(sun);

// Field — exactly 50 yards from midfield (z=40) to the goal line (z=-60).
const FIELD_START_Z=40,GOAL_LINE_Z=-60,ENDZONE_BACK_Z=-70,WORLD_PER_YARD=2;
const turf=new THREE.Mesh(new THREE.PlaneGeometry(54,130),new THREE.MeshStandardMaterial({color:0x24723c,roughness:.95}));turf.rotation.x=-Math.PI/2;turf.position.z=-5;turf.receiveShadow=true;scene.add(turf);
const endzone=new THREE.Mesh(new THREE.PlaneGeometry(54,10),new THREE.MeshStandardMaterial({color:0x163f7a,roughness:.9}));endzone.rotation.x=-Math.PI/2;endzone.position.set(0,.006,-65);scene.add(endzone);
const lineMat=new THREE.MeshBasicMaterial({color:0xffffff,transparent:true,opacity:.72});for(let z=FIELD_START_Z;z>=GOAL_LINE_Z;z-=10){const line=new THREE.Mesh(new THREE.PlaneGeometry(52,.12),lineMat);line.rotation.x=-Math.PI/2;line.position.set(0,.014,z);scene.add(line)}for(const x of[-26,26]){const s=new THREE.Mesh(new THREE.PlaneGeometry(.14,120),lineMat);s.rotation.x=-Math.PI/2;s.position.set(x,.014,-10);scene.add(s)}for(let z=35;z>=-55;z-=10){for(const x of[-9,9]){const h=new THREE.Mesh(new THREE.PlaneGeometry(.9,.09),lineMat);h.rotation.x=-Math.PI/2;h.position.set(x,.015,z);scene.add(h)}}
const standMat=new THREE.MeshStandardMaterial({color:0x27333a,roughness:1});for(const side of[-1,1]){const stand=new THREE.Mesh(new THREE.BoxGeometry(18,10,120),standMat);stand.position.set(side*39,5,-10);scene.add(stand)}
const losLine=new THREE.Mesh(new THREE.PlaneGeometry(52,.20),new THREE.MeshBasicMaterial({color:0x45b9ff,transparent:true,opacity:.92}));losLine.rotation.x=-Math.PI/2;losLine.position.y=.026;scene.add(losLine);
const gainLine=new THREE.Mesh(new THREE.PlaneGeometry(52,.20),new THREE.MeshBasicMaterial({color:0xffd34f,transparent:true,opacity:.96}));gainLine.rotation.x=-Math.PI/2;gainLine.position.y=.028;scene.add(gainLine);

// Materials and player models. Feet are separate visual/physical cues for plant direction.
const offenseMat=new THREE.MeshStandardMaterial({color:0xf2f4f7,roughness:.5}), offenseAccent=new THREE.MeshStandardMaterial({color:0x1767c7,roughness:.45}), defenseMat=new THREE.MeshStandardMaterial({color:0xa8162b,roughness:.5}), skinMat=new THREE.MeshStandardMaterial({color:0x80563d,roughness:.65}), blackMat=new THREE.MeshStandardMaterial({color:0x17191b,roughness:.72}), whiteMat=new THREE.MeshStandardMaterial({color:0xf4f4f4,roughness:.6}), ballMat=new THREE.MeshStandardMaterial({color:0x6c331d,roughness:.46}), laceMat=new THREE.MeshStandardMaterial({color:0xf3eee7,roughness:.5}), shadowMat=new THREE.MeshBasicMaterial({color:0x000000,transparent:true,opacity:.18,depthWrite:false});
const playerGeometryCache=new Map();
function playerGeometry(key,create){
  if(!playerGeometryCache.has(key)){const geometry=create();geometry.userData.sharedPlayer=true;playerGeometryCache.set(key,geometry);}
  return playerGeometryCache.get(key);
}
function makePlayer(offense=true,appearance=null){
  const look={...appearance,...normalizeAppearance(appearance,offense)},root=new THREE.Group();
  const jerseyMat=(offense?offenseMat:defenseMat).clone();jerseyMat.color.set(look.jerseyPrimary);
  const accentMat=(offense?offenseAccent:blackMat).clone();accentMat.color.set(look.jerseyAccent);
  const headMat=skinMat.clone();headMat.color.set(look.skinColor);
  const helmetMat=(offense?offenseAccent:whiteMat).clone();helmetMat.color.set(look.helmetColor);
  const cleatMat=blackMat.clone();cleatMat.color.set(look.cleatColor);
  const body=new THREE.Mesh(playerGeometry('body',()=>{const g=new THREE.SphereGeometry(1,12,10);g.scale(.40,.53,.26);return g}),jerseyMat);body.position.y=1.24;body.castShadow=true;root.add(body);
  const stripe=new THREE.Mesh(playerGeometry('stripe',()=>new THREE.BoxGeometry(.70,.12,.035)),accentMat);stripe.position.set(0,1.31,.255);root.add(stripe);
  const head=new THREE.Mesh(playerGeometry('head',()=>new THREE.SphereGeometry(.26,12,10)),headMat);head.position.y=1.88;head.castShadow=true;root.add(head);
  const helmet=new THREE.Mesh(playerGeometry('helmet',()=>new THREE.SphereGeometry(.29,12,8,0,Math.PI*2,0,Math.PI*.63)),helmetMat);helmet.position.y=1.98;root.add(helmet);
  // Shared, low-poly jointed limbs; rendered capsules are also the contact shapes.
  const arms=[],forearms=[],hands=[],legs=[],shins=[],feet=[];
  function limb(radius,length,material){
    const part=new THREE.Mesh(playerGeometry(`limb-${radius}-${length}`,()=>new THREE.CapsuleGeometry(radius,length-2*radius,3,6)),material);
    part.userData.contactRadius=radius;part.userData.boneLength=length;part.castShadow=true;root.add(part);return part;
  }
  for(const side of [-1,1]){
    const shoulder=new THREE.Mesh(playerGeometry('shoulder',()=>new THREE.SphereGeometry(.22,8,6)),jerseyMat);
    shoulder.scale.set(1,.72,1);shoulder.position.set(side*.36,1.48,0);root.add(shoulder);
    arms.push(limb(.085,.44,jerseyMat));forearms.push(limb(.075,.46,headMat));
    const hand=new THREE.Mesh(playerGeometry('hand',()=>new THREE.SphereGeometry(.105,8,7)),whiteMat);
    hand.position.set(side*.53,.86,.02);hand.userData.contactRadius=.105;hand.userData.boneLength=.21;
    hand.castShadow=true;root.add(hand);hands.push(hand);
    legs.push(limb(.12,.45,jerseyMat));shins.push(limb(.095,.45,accentMat));
    const foot=new THREE.Mesh(playerGeometry('foot',()=>new THREE.CapsuleGeometry(.095,.23,3,6)),cleatMat);
    foot.rotation.x=Math.PI/2;foot.position.set(side*.22,.12,.08);root.add(foot);feet.push(foot);
  }
  const hips=new THREE.Mesh(playerGeometry('hips',()=>new THREE.SphereGeometry(.29,10,6)),jerseyMat);
  hips.scale.set(1,.65,.8);hips.position.y=.91;root.add(hips);
  const faceguard=new THREE.Mesh(playerGeometry('faceguard',()=>new THREE.TorusGeometry(.23,.022,4,12,Math.PI)),blackMat);
  faceguard.position.set(0,1.87,.18);faceguard.rotation.z=Math.PI;root.add(faceguard);
  R?.dress(root,look,{body,head,helmet,faceguard,hands,arms,forearms,legs,shins,hips});
  const visualRig=new THREE.Group();
  for(const part of [...root.children])visualRig.add(part);
  root.add(visualRig);root.userData.visualRig=visualRig;
  root.scale.set(look.scaleX,look.scaleY,look.scaleZ);
  const shadow=new THREE.Mesh(playerGeometry('shadow',()=>new THREE.CircleGeometry(.5,14)),shadowMat);shadow.rotation.x=-Math.PI/2;shadow.position.y=.016;shadow.scale.set(Math.max(.86,look.scaleX*.98),1,Math.max(.86,look.scaleZ*.98));root.add(shadow);let trackRing=null;if(offense){trackRing=new THREE.Mesh(playerGeometry('ring',()=>new THREE.RingGeometry(.62,.82,24)),new THREE.MeshBasicMaterial({color:0x62ff9c,transparent:true,opacity:.82,side:THREE.DoubleSide,depthWrite:false}));trackRing.rotation.x=-Math.PI/2;trackRing.position.y=.025;trackRing.visible=false;root.add(trackRing)}root.userData.arms=arms;root.userData.forearms=forearms;root.userData.shins=shins;root.userData.hands=hands;root.userData.feet=feet;root.userData.legs=legs;root.userData.body=body;root.userData.trackRing=trackRing;root.userData.appearance=look;return root;
}
function makeBall(){
  const root=new THREE.Group();const g=new THREE.SphereGeometry(.19,16,10);g.scale(1,1,1.75);const m=new THREE.Mesh(g,ballMat);m.castShadow=true;root.add(m);
  const lace=new THREE.Mesh(new THREE.BoxGeometry(.035,.025,.38),laceMat);lace.position.set(0,.17,0);root.add(lace);const spin=new THREE.Group();spin.add(m,lace);root.add(spin);root.visible=false;scene.add(root);root.userData.mesh=m;root.userData.spin=spin;return root;
}
const ball=makeBall();

// Pre-snap route art stays on the field until the snap.
const routeVisuals=new THREE.Group();scene.add(routeVisuals);const routeColors=[0x62d9ff,0xffd166,0x7cffae,0xff82c6];
function clearRouteVisuals(){while(routeVisuals.children.length){const o=routeVisuals.children[0];o.geometry?.dispose();o.material?.dispose();routeVisuals.remove(o)}}
function drawRouteVisuals(){clearRouteVisuals();receivers.forEach((r,i)=>{const pts=r.path.map(p=>new THREE.Vector3(p.x,.055,p.z));const line=new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts),new THREE.LineBasicMaterial({color:routeColors[i],transparent:true,opacity:.92}));routeVisuals.add(line);if(pts.length>1){const end=pts[pts.length-1],prev=pts[pts.length-2],dir=end.clone().sub(prev).normalize();const arrow=new THREE.Mesh(new THREE.ConeGeometry(.36,.85,10),new THREE.MeshBasicMaterial({color:routeColors[i],transparent:true,opacity:.95}));arrow.position.copy(end).add(new THREE.Vector3(0,.16,0));arrow.quaternion.setFromUnitVectors(new THREE.Vector3(0,1,0),dir.clone().setY(0).normalize());arrow.rotateX(Math.PI/2);routeVisuals.add(arrow)}});routeVisuals.visible=true}

// Green trajectory preview.
const arcMat=new THREE.LineBasicMaterial({color:0x62ff9c,transparent:true,opacity:.9});const arcGeo=new THREE.BufferGeometry();const arcPositions=new Float32Array(64*3);arcGeo.setAttribute('position',new THREE.BufferAttribute(arcPositions,3).setUsage(THREE.DynamicDrawUsage));const arcLine=new THREE.Line(arcGeo,arcMat);arcLine.visible=false;scene.add(arcLine);
const landRing=new THREE.Mesh(new THREE.RingGeometry(.45,.66,28),new THREE.MeshBasicMaterial({color:0x62ff9c,transparent:true,opacity:.8,side:THREE.DoubleSide}));landRing.rotation.x=-Math.PI/2;landRing.visible=false;scene.add(landRing);

const routeOptions=[
  {name:'Go',key:'G'},{name:'Slant',key:'S'},{name:'Out',key:'O'},{name:'Post',key:'P'},{name:'Corner',key:'C'},
  {name:'Drag',key:'D'},{name:'Dig',key:'I'},{name:'Comeback',key:'B'},{name:'Flat',key:'F'},{name:'Wheel',key:'W'},{name:'Fade',key:'A'},{name:'Curl',key:'U'},{name:'Double Move',key:'M'},{name:'Stick',key:'T'},{name:'Bubble',key:'E'},{name:'Tunnel',key:'N'},{name:'Lead',key:'L'}
];
const routePool=routeOptions.map(r=>r.name);
const routeByKey=Object.fromEntries(routeOptions.map(r=>[r.key,r.name]));
const receiverByKey={X:0,H:1,Y:2,Z:3};
const plays=[
  {name:'Four Verticals',routes:['Go','Go','Go','Go']},
  {name:'Mesh',routes:['Drag','Post','Corner','Drag']},
  {name:'Levels',routes:['Dig','Drag','Dig','Go']},
  {name:'Smash',routes:['Curl','Corner','Corner','Curl']},
  {name:'Dagger',routes:['Double Move','Dig','Post','Go']},
  {name:'Drive',routes:['Dig','Drag','Post','Out']},
  {name:'Crossfire',routes:['Post','Slant','Slant','Corner']},
  {name:'Sideline',routes:['Fade','Wheel','Wheel','Fade']}
];
plays.push(...V.concepts);
let selectedPlay=0,playCategory='Suggested',pumpReadyAt=0,formationMotion=null;
plays.slice(0,8).forEach((p,i)=>p.category=[0,4,7].includes(i)?'Deep Shots':'Intermediate');
const defenses=[
  {id:'off',name:'OFF MAN',desc:'Cushion at the snap; attack the space before corners close.',corner:'off',safety:'patient'},
  {id:'underzone',name:'UNDERNEATH ZONE',desc:'Corners guard short lanes and pass crossing receivers between zones.',corner:'zone',safety:'patient'},
  {id:'deep',name:'DEEP ZONE',desc:'Deep landmarks protect vertical throws; underneath space is available.',corner:'deepzone',safety:'patient'},
  {id:'bracket',name:'SAFETY HELP',desc:'A safety shades the featured receiver while corners play man.',corner:'off',safety:'bracket'},
  {id:'disguise',name:'ROTATING ZONE',desc:'A two-high shell rotates gradually after the snap.',corner:'zone',safety:'rotate'},
  {id:'press',name:'PRESS MAN',desc:'Corners crowd releases. One wrong hip turn can open a sharp cut.',corner:'press',safety:'single'},
  {id:'twohigh',name:'TWO-HIGH PATIENT',desc:'Safeties stay square and protect depth until the ball declares itself.',corner:'under',safety:'patient'},
  {id:'robber',name:'ROBBER',desc:'A middle safety reads your eyes while corners trail routes.',corner:'trail',safety:'robber'},
  {id:'match',name:'MATCH ZONE',desc:'DBs pattern-match only after routes enter their vision and leverage.',corner:'match',safety:'match'},
  {id:'pressure',name:'PRESSURE MAN',desc:'Aggressive man coverage with a fast single-high safety hunting deep throws.',corner:'allout',safety:'aggressive'}
];
const baseOpponents=[
  {name:'Rookie Secondary',skill:0.00,schemes:['twohigh']},
  {name:'Saturday Ballhawks',skill:.13,schemes:['twohigh','press']},
  {name:'Red Zone Raiders',skill:.25,schemes:['press','twohigh','robber']},
  {name:'Route Jumpers',skill:.38,schemes:['press','robber','match']},
  {name:'Coverage Lab',skill:.50,schemes:['twohigh','robber','match']},
  {name:'No-Fly Unit',skill:.64,schemes:['press','match','pressure']},
  {name:'All-Pro DBs',skill:.80,schemes:['robber','match','pressure']},
  {name:'Championship Defense',skill:.92,schemes:['press','twohigh','robber','match','pressure']}
];
const endlessAdjectives=['Iron','Night','Storm','Prime','Viper','Titan','Phantom','Cobalt','Crimson','Onyx','Apex','Metro','Crown','Blitz','Steel','Summit','Arctic','Solar','Copper','Royal','Wild','Silver','Golden','Scarlet','Coastal','Granite','Midnight','Thunder','Emerald','Frost','Desert','Obsidian'];
const endlessNouns=['Ballhawks','Shadows','Sentinels','Lockdown','Wardens','Jackals','Falcons','Guard','Hunters','Legion','Cyclones','Reapers','Stalkers','Titans','Rangers','Dragons'];
function opponentForRound(round){
  if(round<=baseOpponents.length)return baseOpponents[round-1];
  const i=round-baseOpponents.length-1,count=endlessAdjectives.length*endlessNouns.length,season=Math.floor(i/count),name=`${endlessAdjectives[i%endlessAdjectives.length]} ${endlessNouns[Math.floor(i/endlessAdjectives.length)%endlessNouns.length]}${season?' · League '+(season+1):''}`;
  const skill=Math.min(.98,.86+(round-baseOpponents.length)*.012);
  const schemeSets=[['press','robber','match'],['twohigh','match','pressure'],['press','twohigh','pressure'],['robber','match','pressure'],['press','twohigh','robber','match','pressure']];
  return{name,skill,schemes:schemeSets[i%schemeSets.length]};
}
const SAVE_KEY='receiverWindowQB_franchise_v1';
const skinTones=[{name:'Deep',color:'#4d2f22'},{name:'Rich',color:'#6a432d'},{name:'Warm Brown',color:'#87583e'},{name:'Golden Brown',color:'#a36d4b'},{name:'Tan',color:'#c28a66'},{name:'Light',color:'#deb394'}];
const cleatThemes=[{name:'Black',color:'#181b20'},{name:'White',color:'#f1f4f7'},{name:'Lime',color:'#b4ff4b'},{name:'Gold',color:'#ffc547'},{name:'Red',color:'#df5d5d'},{name:'Ice',color:'#6de5ff'}];
const sizeProfiles=[{name:'Compact',sx:.93,sy:.95,sz:.93},{name:'Balanced',sx:1,sy:1,sz:1},{name:'Tall',sx:.96,sy:1.08,sz:.96},{name:'Big',sx:1.08,sy:1.03,sz:1.06},{name:'Lean',sx:.90,sy:1.05,sz:.90}];
const offenseBlueThemes=[{name:'Royal Blue',primary:'#1759cf',accent:'#f5fbff',helmet:'#0f3f97'},{name:'Navy Ice',primary:'#173b83',accent:'#6ad8ff',helmet:'#eef7ff'},{name:'Powder Blue',primary:'#4b8fdd',accent:'#f6fbff',helmet:'#1f58a8'},{name:'Midnight Blue',primary:'#142a60',accent:'#9ae7ff',helmet:'#eef4ff'},{name:'Electric Blue',primary:'#1d79de',accent:'#dff7ff',helmet:'#0f4e94'}];
const opponentUniformThemes=[{name:'Crimson Gold',primary:'#a71f31',accent:'#f1c76a',helmet:'#231316'},{name:'Emerald Black',primary:'#12724e',accent:'#d7ffe7',helmet:'#101716'},{name:'Purple Storm',primary:'#5d39a4',accent:'#d1c7ff',helmet:'#181520'},{name:'Orange Night',primary:'#cf6b14',accent:'#fff0cf',helmet:'#20140c'},{name:'Steel Red',primary:'#6f7d92',accent:'#f35f67',helmet:'#1c232a'},{name:'Onyx Teal',primary:'#21252e',accent:'#4be2d5',helmet:'#0b0e12'},{name:'Sunset Maroon',primary:'#7e2136',accent:'#ff9966',helmet:'#2a1217'},{name:'Volt Green',primary:'#315f22',accent:'#c9ff58',helmet:'#141910'}];
function choice(arr){return arr[randInt(0,arr.length-1)]}
function buildReceiverAppearance(){const skin=choice(skinTones),cleat=choice(cleatThemes),size=choice(sizeProfiles),jersey=choice(offenseBlueThemes);return{skinColor:skin.color,skinName:skin.name,cleatColor:cleat.color,cleatName:cleat.name,sizeName:size.name,scaleX:size.sx,scaleY:size.sy,scaleZ:size.sz,jerseyPrimary:jersey.primary,jerseyAccent:jersey.accent,helmetColor:jersey.helmet,jerseyName:jersey.name}}
function opponentUniformForRound(round){const u=F.opponent(round);return {...u,accent:u.secondary,helmet:u.helmet,name:opponentForRound(round).name};}
function buildDefenderAppearance(round){const skin=choice(skinTones),cleat=choice(cleatThemes),size=choice(sizeProfiles),jersey=opponentUniformForRound(round);return{skinColor:skin.color,skinName:skin.name,cleatColor:jersey.shoes,cleatName:'Team',sizeName:size.name,scaleX:size.sx,scaleY:size.sy,scaleZ:size.sz,jerseyPrimary:jersey.primary,jerseyAccent:jersey.secondary,helmetColor:jersey.helmet,jerseyName:jersey.name,pantsColor:jersey.pants,sockColor:jersey.socks,numberColor:jersey.number,accentColor:jersey.accent,secondaryColor:jersey.secondary,logo:jersey.logo,number:20+(defenders.length||0),identityId:'defender-'+round+'-'+defenders.length}}
function normalizeAppearance(a,offense=true){if(!a||typeof a!=='object')return offense?buildReceiverAppearance():buildDefenderAppearance(1);const color=v=>typeof v==='string'&&/^#[0-9a-f]{6}$/i.test(v)?v:null; a={...a}; for(const k of ['skinColor','cleatColor','jerseyPrimary','jerseyAccent','helmetColor'])a[k]=color(a[k]);for(const k of ['skinName','cleatName','sizeName','jerseyName'])a[k]=escapeHTML(String(a[k]||'').slice(0,30));return{skinColor:a.skinColor||choice(skinTones).color,skinName:a.skinName||'Balanced',cleatColor:a.cleatColor||choice(cleatThemes).color,cleatName:a.cleatName||'Black',sizeName:a.sizeName||'Balanced',scaleX:Number.isFinite(Number(a.scaleX))?THREE.MathUtils.clamp(Number(a.scaleX),.85,1.16):1,scaleY:Number.isFinite(Number(a.scaleY))?THREE.MathUtils.clamp(Number(a.scaleY),.85,1.16):1,scaleZ:Number.isFinite(Number(a.scaleZ))?THREE.MathUtils.clamp(Number(a.scaleZ),.85,1.16):1,jerseyPrimary:a.jerseyPrimary||((offense?choice(offenseBlueThemes):choice(opponentUniformThemes)).primary),jerseyAccent:a.jerseyAccent||((offense?choice(offenseBlueThemes):choice(opponentUniformThemes)).accent),helmetColor:a.helmetColor||((offense?choice(offenseBlueThemes):choice(opponentUniformThemes)).helmet),jerseyName:a.jerseyName||(offense?'Blue Kit':'Road Kit')}}
const firstNames=['Jalen','Marcus','Darius','Tyrell','Devin','Malik','Tre','Andre','Jordan','Cameron','Nico','Xavier','Bryce','Kendrick','Zay','Keon','Jaylen','Trey','Calvin','Miles','Roman','Isaiah','Chris','Avery','Quentin','Darnell','Rashad','Elijah','Micah','Troy','Marvin','Donovan','Cedric','Terrance','Demarcus','Lance','Cole','Khalil','Tavian','Amari','Jaxon','Noah','Corey','Brandon','Desmond','Sterling','Jamal','Dante','Marlon','Reggie'];
const lastNames=['Banks','Mercer','Coleman','Vaughn','Price','Holloway','Sims','Maddox','Bennett','Cross','Hayes','Rowe','McCall','Jefferson','Foster','Reed','Brooks','Knight','Wells','Parker','Grant','Ellis','Rhodes','Pierce','Hampton','Carter','Bishop','Stone','Fleming','Moss','Rivers','Daniels','Turner','Cobb','Murray','Fields','Dawson','Wallace','Sharp','Harris','Monroe','Burke','Gaines','Lewis','Watkins','Dean','Riley','Owens','Ford','Marshall'];
const archetypes=[
  {name:'Blocking',b:{speed:61,cutting:72,turning:68,evasion:55,catching:68,strength:93}},
  {name:'Burner',b:{speed:86,cutting:64,turning:58,evasion:66,catching:61,strength:46}},
  {name:'Route Tech',b:{speed:67,cutting:86,turning:84,evasion:67,catching:72,strength:54}},
  {name:'Possession',b:{speed:58,cutting:70,turning:74,evasion:62,catching:89,strength:86}},
  {name:'Open-Field',b:{speed:75,cutting:76,turning:78,evasion:88,catching:61,strength:55}},
  {name:'Balanced',b:{speed:73,cutting:73,turning:73,evasion:73,catching:73,strength:72}},
  {name:'Comeback Artist',b:{speed:63,cutting:72,turning:91,evasion:64,catching:78,strength:63}},
  {name:'Raw Athlete',b:{speed:88,cutting:54,turning:55,evasion:79,catching:50,strength:82}},
  {name:'Sure Hands',b:{speed:64,cutting:65,turning:68,evasion:58,catching:94,strength:69}},
  {name:'Power Slot',b:{speed:62,cutting:66,turning:69,evasion:58,catching:83,strength:94}}
];
function randInt(a,b){return Math.floor(a+Math.random()*(b-a+1))}
function clampRating(v){return P.rating(v)}
function playerOverall(p){return Math.round(P.stats.reduce((n,k)=>n+(p[k]||50),0)/P.stats.length)}
function newReceiver(market=false){
  const a=archetypes[randInt(0,archetypes.length-1)],j=market?15:13,p={id:`wr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,8)}`,name:`${firstNames[randInt(0,firstNames.length-1)]} ${lastNames[randInt(0,lastNames.length-1)]}`,archetype:a.name,trainings:0};
  for(const k of ['speed','cutting','turning','evasion','catching','strength'])p[k]=clampRating(a.b[k]+randInt(-j,j));
  p.appearance=buildReceiverAppearance();p.chemistry={xp:0,concepts:Object.create(null)};P.migratePlayer(p);
  const buildStrength={Big:7,Tall:2,Balanced:0,Compact:-3,Lean:-4}[p.appearance.sizeName]||0;p.strength=clampRating(p.strength+buildStrength);
  if(market)return P.recruit(p);
  p.price=Math.max(140,Math.round((145+Math.max(0,playerOverall(p)-48)*12+randInt(-35,55))/10)*10);return p;
}
function freshMarket(){return Array.from({length:5},()=>newReceiver(true))}
function defaultFranchise(){return{cash:250,round:1,wins:0,team:Array.from({length:4},()=>newReceiver(false)),market:freshMarket()}}
function fallbackReceiverStrength(p){const arch=archetypes.find(a=>a.name===p?.archetype),sizeBonus={Big:7,Tall:2,Balanced:0,Compact:-3,Lean:-4}[p?.appearance?.sizeName]||0,catchInfluence=((Number(p?.catching)||50)-50)*.08;return clampRating((arch?.b?.strength||65)+sizeBonus+catchInfluence)}
function normalizeReceiver(p){if(!p||typeof p!=='object')return newReceiver(false);for(const k of ['speed','cutting','turning','evasion','catching'])p[k]=clampRating(Number(p[k])||50);p.strength=Number.isFinite(Number(p.strength))?clampRating(Number(p.strength)):fallbackReceiverStrength(p);p.trainings=Math.max(0,Number(p.trainings)||0);p.name=String(p.name||'').slice(0,80)||`${firstNames[randInt(0,firstNames.length-1)]} ${lastNames[randInt(0,lastNames.length-1)]}`;p.archetype=String(p.archetype||'Prospect').slice(0,40);p.id=p.id||`wr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,8)}`;p.price=Math.max(100,Number(p.price)||200);p.appearance=normalizeAppearance(p.appearance,true);P.migratePlayer(p);return p}
let localSaveAvailable=true;
// Reprice saved listings without rerolling recruits or raising legacy bargain prices.
function normalizeMarketReceiver(p){p=normalizeReceiver(p);p.price=Math.min(p.price,P.signingPrice(playerOverall(p)));return p}
function loadFranchise(){try{const raw=localStorage.getItem(SAVE_KEY);if(!raw)return defaultFranchise();const f=JSON.parse(raw);f.cash=Math.max(0,Number(f.cash)||0);f.round=Math.max(1,Number(f.round)||1);f.wins=Math.max(0,Number(f.wins)||0);f.team=(Array.isArray(f.team)?f.team:[]).slice(0,4).map(normalizeReceiver);while(f.team.length<4)f.team.push(newReceiver(false));f.market=(Array.isArray(f.market)?f.market:[]).slice(0,5).map(normalizeMarketReceiver);while(f.market.length<5)f.market.push(newReceiver(true));return f}catch(err){localSaveAvailable=false;return defaultFranchise()}}
let franchise;
try{franchise=slots[activeSlot]?P.validateSave(slots[activeSlot].data):loadFranchise()}catch{franchise=loadFranchise()}
P.normalizeCompetition(franchise);F.normalize(franchise);
franchise.team.forEach(normalizeReceiver);franchise.market.forEach(normalizeMarketReceiver);
function saveFranchise(){
  const stamp=Date.now();franchise.updatedAt=stamp;
  slots[activeSlot]={name:slots[activeSlot]?.name||`Franchise ${activeSlot+1}`,updatedAt:stamp,data:JSON.parse(JSON.stringify(franchise))};
  try{localStorage.setItem(SLOT_KEY,JSON.stringify(slots));localStorage.setItem('receiverWindowQB_activeSlot',String(activeSlot));localSaveAvailable=true}catch{localSaveAvailable=false}
  updateSavePill();renderMenu();
}
function updateSavePill(){const el=$('savePill');if(el){el.textContent=localSaveAvailable?'LOCAL SAVE ✓':'LOCAL SAVE FAILED';el.style.color=localSaveAvailable?'#bdf8d2':'#ff8c91'}}
franchise.scouting=V.cleanHistory(franchise.scouting);
let currentDefense=defenses[0],currentOpponentUniform=opponentUniformForRound(activeRound()),receivers=[],defenders=[];
let tournamentStage=franchise.round-1,seriesOffense=0,seriesDefense=0,playNumber=1,score=0,catches=0,drops=0,ints=0,playState='dead';
let browsedOpponent=1;
function activeRound(){return P.matchRound(franchise)}
let selectedRosterIndex=0,managerLocked=false,audibleReceiverIndex=null;
let ballVel=new THREE.Vector3(),ballPrev=new THREE.Vector3(),ballLive=false,throwTime=0,spiralQuality=1,duckPhase=0,lastTime=performance.now(),snapTime=0,nextCount=0,charging=false,chargeStart=0,chargePower=0,messageTimer=0,resultFlashTimer=0;
let throwClock=8,loftBias=0,keyLoft=false,keyBullet=false;
const predictedLanding=new THREE.Vector3();let predictedFlightTime=0;
let ballSpotYards=0,down=1,lineToGainYards=25,snapSpotYards=0,ballCarrier=null,tackler=null,tackleTimer=0,tackleSpotYards=0;
function worldZForYards(yards){return FIELD_START_Z-THREE.MathUtils.clamp(yards,0,50)*WORLD_PER_YARD}
function yardsForWorldZ(z){return THREE.MathUtils.clamp((FIELD_START_Z-z)/WORLD_PER_YARD,0,50)}
function downLabel(){return down===1?'1st':down===2?'2nd':down===3?'3rd':'4th'}

function playerLook(p){const shape=V.physique(p),id=franchise.identity,u=id[id.active],index=franchise.team.indexOf(p);return {...p.appearance,scaleX:shape.x,scaleY:shape.y,scaleZ:shape.z,sizeName:p.size>=75?'Powerful':p.size<45?'Compact':'Balanced',jerseyPrimary:u.jersey,jerseyAccent:u.secondary,helmetColor:u.helmet,cleatColor:u.shoes,pantsColor:u.pants,sockColor:u.socks,numberColor:u.number,accentColor:u.accent,secondaryColor:u.secondary,logo:id.logo,number:index>=0?11+index*7:10+F.hash(p.id)%80,identityId:p.id,strength:Math.min(140,p.strength)};}

function playerTier(p){const n=playerOverall(p);return n>=97?'generational':n>=90?'elite':n>=80?'star':'prospect';}
function statLabel(k){return({speed:'SPD',cutting:'CUT',turning:'TRN',evasion:'EVA',catching:'CAT',strength:'STR',athleticism:'ATH',size:'SIZE',tricks:'TRICK'})[k]}
function previewFigureHTML(a){return globalThis.QBFranchiseUI?QBFranchiseUI.uniformSVG({name:'Receiver',logo:a.logo||'🏈'},{helmet:a.helmetColor,jersey:a.jerseyPrimary,secondary:a.jerseyAccent,pants:a.pantsColor||a.jerseyPrimary,socks:a.sockColor||a.jerseyAccent,shoes:a.cleatColor,number:a.numberColor||'#ffffff',accent:a.accentColor||a.jerseyAccent},a.number||11):'';}

function lookSummary(p){const a=playerLook(p);const inches=Math.round(69+(a.scaleY-.9)*35);return `${Math.floor(inches/12)}′${inches%12}″ · ${a.sizeName} build · IQ ${F.awareness(p)} · Chemistry ${F.chemistryLevel(p)}`}
function swatchLineHTML(a){return `<div class="swatchLine"><span class="swatchGroup"><span class="swatch" style="background:${a.jerseyPrimary}"></span>Jersey</span><span class="swatchGroup"><span class="swatch" style="background:${a.jerseyAccent}"></span>Accent</span><span class="swatchGroup"><span class="swatch" style="background:${a.helmetColor}"></span>Helmet</span><span class="swatchGroup"><span class="swatch" style="background:${a.cleatColor}"></span>Cleats</span></div>`}
function trainingCost(p){return 90+p.trainings*55}
function rosterCardHTML(p,i){const cost=trainingCost(p),max=p.trainings>=5,a=playerLook(p);return `<div data-tier="${playerTier(p)}" class="playerCard ${i===selectedRosterIndex?'selected':''}" data-roster="${i}" tabindex="0" role="button" aria-label="Select ${escapeHTML(p.name)} as replacement slot"><div class="playerHeader">${previewFigureHTML(a)}<div class="playerIdentity"><div class="playerName">${['X','H','Y','Z'][i]} · ${escapeHTML(p.name)}</div><div class="playerSub">${escapeHTML(V.playstyle(p))}${V.signature(p)?' · '+V.signature(p):''} · OVR ${playerOverall(p)} · ${Object.values(p.trainingByStat).reduce((a,b)=>a+b,0)} sessions</div><div class="lookLine">${lookSummary(p)}</div><div class="traitBadges">${studio?.traits(p)||''}</div></div></div><div class="statRow"><span>Speed</span><b>${p.speed}</b><span>Cutting</span><b>${p.cutting}</b><span>Turning</span><b>${p.turning}</b><span>Evasion</span><b>${p.evasion}</b><span>Catch</span><b>${p.catching}</b><span>Strength</span><b>${p.strength}</b><span>Athleticism</span><b>${p.athleticism}</b><span>Size</span><b>${p.size}</b><span>Tricks</span><b>${p.tricks}</b></div><button data-player-stats="${escapeHTML(p.id)}" class="ghost">CAREER & CHEMISTRY</button><div class="trainGrid">${P.stats.map(k=>`<button data-train-slot="${i}" data-stat="${k}" ${p.trainingByStat[k]>=5?'disabled':''} title="$${90+p.trainingByStat[k]*55} · ${p.trainingByStat[k]}/5 sessions">+ ${statLabel(k)}</button>`).join('')}</div><div class="costLine">Training $90–310 per attribute · 5 sessions each · Prestige ${p.prestige||0}</div><button data-prestige="${i}" ${P.stats.some(k=>p.trainingByStat[k]>=5)?'':'disabled'}>PRESTIGE · ${P.prestigeCost(p).toLocaleString()}</button><div class="costLine">Keep ratings. Renew five sessions per attribute. Next prestige costs more.</div></div>`}
function marketCardHTML(p,i){const a=playerLook(p);return `<div data-tier="${playerTier(p)}" class="playerCard"><div class="playerHeader">${previewFigureHTML(a)}<div class="playerIdentity"><div class="playerName">${escapeHTML(p.name)}</div><div class="playerSub">${escapeHTML(V.playstyle(p))}${V.signature(p)?' · '+V.signature(p):''} · OVR ${playerOverall(p)}</div><div class="lookLine">${lookSummary(p)}</div>${swatchLineHTML(a)}</div></div><div class="statRow"><span>Speed</span><b>${p.speed}</b><span>Cutting</span><b>${p.cutting}</b><span>Turning</span><b>${p.turning}</b><span>Evasion</span><b>${p.evasion}</b><span>Catch</span><b>${p.catching}</b><span>Strength</span><b>${p.strength}</b><span>Athleticism</span><b>${p.athleticism}</b><span>Size</span><b>${p.size}</b><span>Tricks</span><b>${p.tricks}</b></div><div class="costLine">${escapeHTML(p.rarity||'Prospect')} · Signing cost: ${p.price}</div><button class="signBtn" data-sign="${i}">SIGN · REPLACE ${['X','H','Y','Z'][selectedRosterIndex]}</button></div>`}
function renderManager(){renderOpponentBrowser();$('cashPill').textContent=`$${franchise.cash}`;$('roundPill').textContent=`Round ${franchise.round} · ${franchise.wins} wins`;$('rosterGrid').innerHTML=franchise.team.map(rosterCardHTML).join('');$('marketGrid').innerHTML=franchise.market.map(marketCardHTML).join('');$('managerStatus').dataset.uniform=`Next opponent uniform: ${opponentUniformForRound(activeRound()).name}`;updateSavePill();document.querySelectorAll('[data-player-stats]').forEach(btn=>btn.onclick=e=>{e.stopPropagation();studio?.player(btn.dataset.playerStats);});document.querySelectorAll('[data-roster]').forEach(el=>el.addEventListener('click',()=>{selectedRosterIndex=Number(el.dataset.roster);renderManager()}));document.querySelectorAll('[data-train-slot]').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();trainReceiver(Number(btn.dataset.trainSlot),btn.dataset.stat)}));document.querySelectorAll('[data-prestige]').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();prestigeReceiver(Number(btn.dataset.prestige))}));document.querySelectorAll('[data-sign]').forEach(btn=>btn.addEventListener('click',()=>signReceiver(Number(btn.dataset.sign))))}
function prestigeReceiver(slot){const p=franchise.team[slot];if(!p)return;const result=P.prestige(p,franchise.cash);if(!result.ok){$('managerStatus').textContent=result.reason;return}franchise.cash=result.cash;$('managerStatus').textContent=`${p.name} prestiged! Five sessions per attribute renewed; ratings retained.`;saveFranchise();renderManager()}
function trainReceiver(slot,stat){const p=franchise.team[slot];if(!p)return;const result=P.train(p,stat,franchise.cash,randInt(4,8));if(!result.ok){$('managerStatus').textContent=result.reason;return}franchise.cash=result.cash;$('managerStatus').textContent=`${p.name}: ${statLabel(stat)} +${result.gain}.`;saveFranchise();renderManager()}
function signReceiver(index){const p=franchise.market[index];if(!p)return;if(franchise.cash<p.price){$('managerStatus').textContent=`You need $${p.price-franchise.cash} more to sign ${escapeHTML(p.name)}.`;return}const old=franchise.team[selectedRosterIndex];franchise.cash-=p.price;franchise.team[selectedRosterIndex]=p;franchise.market[index]=newReceiver(true);$('managerStatus').textContent=`Signed ${escapeHTML(p.name)} to ${['X','H','Y','Z'][selectedRosterIndex]}, replacing ${old.name}.`;saveFranchise();renderManager()}
function openManager(status='Manage your four starters before the next matchup.',locked=false){managerLocked=locked;browsedOpponent=activeRound();playState=locked?'manager':playState;document.exitPointerLock?.();$('managerStatus').textContent=status;$('managerLayer').style.display='flex';$('continueBtn').textContent=locked?'CONTINUE TO NEXT MATCHUP':'RETURN TO FIELD';renderManager()}
function closeManager(){$('cloudOffer').hidden=true;if(managerLocked){managerLocked=false;playState='dead';$('managerLayer').style.display='none';resetDrive();setupPlay(true)}else {$('managerLayer').style.display='none';if(!menuOpen)setupPlay(false)}}
function canSelectOpponent(){return !transition&&!replay&&!cloudBusy&&!saveOpen&&(managerLocked||(!franchise.matchInProgress&&seriesOffense===0&&seriesDefense===0&&down===1&&ballSpotYards===0));}
function renderOpponentBrowser(){
  browsedOpponent=THREE.MathUtils.clamp(browsedOpponent,1,franchise.round);
  const round=browsedOpponent,opp=opponentForRound(round),record=franchise.opponentResults[round],beaten=round<franchise.round;
  const wins=record?.wins??(beaten?1:0),losses=record?.losses||0;
  studio?.manager(round);
  $('opponentName').textContent=`${F.opponent(round).logo} ${round}. ${opp.name}`;
  $('opponentRecord').textContent=`${beaten?'DEFEATED':franchise.matchInProgress&&round===activeRound()?'IN PROGRESS':'CAMPAIGN OPPONENT'} · ${wins}W / ${losses}L · ${V.identity(round).name}`;
  const reward=P.payout(true,round,3),cash=beaten?Math.floor(reward/5):reward;
  $('opponentPrize').textContent=`Win prize: $${cash.toLocaleString()}${beaten?' · 1/5 cash (20%), including scoring bonus':' · Full cash'} · 520 distinct teams, then new leagues`;
  $('selectOpponent').textContent=round===activeRound()?'SELECTED':beaten?'PLAY REMATCH':'CONTINUE CAMPAIGN';
  $('selectOpponent').disabled=!canSelectOpponent()||round===activeRound();
  $('opponentPrevious').disabled=franchise.round<=1;$('opponentNext').disabled=franchise.round<=1;
  $('opponentSelectionNote').textContent=canSelectOpponent()?`Campaign progress: Round ${franchise.round}. Rematches preserve progression and use the original opponent difficulty.`:'Finish this matchup before changing opponents.';
}
function browseOpponent(step){browsedOpponent=((browsedOpponent-1+step+franchise.round)%franchise.round)+1;renderOpponentBrowser();}
function selectOpponent(){
  if(!canSelectOpponent()||!Number.isInteger(browsedOpponent)||browsedOpponent<1||browsedOpponent>franchise.round)return;
  franchise.rematchRound=browsedOpponent<franchise.round?browsedOpponent:null;
  franchise.matchInProgress=false;franchise.scouting=[];franchise.pumpMemory=[];franchise.conceptMemory=[];franchise.matchWeather=null;franchise.lastCoverage=null;snapDefense=null;
  seriesOffense=0;seriesDefense=0;resetDrive();setupPlay(true);renderOpponentBrowser();
  $('managerStatus').textContent=`Selected ${opponentForRound(activeRound()).name}. Continue to the field to start.`;
}
$('opponentPrevious').onclick=()=>browseOpponent(-1);$('opponentNext').onclick=()=>browseOpponent(1);$('selectOpponent').onclick=selectOpponent;
function endMatchup(won){
  const playedRound=activeRound(),rematch=playedRound<franchise.round;
  const firstMatch=!franchise.wins&&!franchise.losses&&franchise.round===1;
  const offerCloud=firstMatch&&!franchise.cloudSaveOffered;
  const opp=opponentForRound(playedRound),payout=P.matchPayout(franchise,won,seriesOffense);
  F.finishGame(franchise,{won,opponent:opp.name,round:playedRound,score:[seriesOffense,seriesDefense]});
  franchise.cash+=payout;
  const record=franchise.opponentResults[playedRound]||(franchise.opponentResults[playedRound]={wins:rematch?1:0,losses:0});
  record[won?'wins':'losses']++;
  if(won){franchise.wins++;if(!rematch)franchise.round++;}else franchise.losses=(franchise.losses||0)+1;
  franchise.rematchRound=null;franchise.matchInProgress=false;
  franchise.scouting=[];franchise.pumpMemory=[];franchise.conceptMemory=[];franchise.matchWeather=null;franchise.lastCoverage=null;
  tournamentStage=franchise.round-1;
  const status=`MATCHUP ${won?'WON':'LOST'} vs ${opp.name} · +$${payout}${won?' victory':' participation'} & scoring payout. ${rematch?'Rematch paid 20%. Campaign remains at':won?'Next':'Retry'} Round ${franchise.round}.`;
  seriesOffense=0;seriesDefense=0;resetDrive();franchise.market=freshMarket();checkpoint();
  scheduleResult(()=>{
    openManager(status,true);
    if(offerCloud){franchise.cloudSaveOffered=true;saveFranchise();$('cloudOffer').hidden=false;}
  },1400);
}

function resetDrive(){ballSpotYards=0;down=1;lineToGainYards=25;snapSpotYards=0;ballCarrier=null;tackler=null;tackleTimer=0;tackleSpotYards=0}
function updateFieldMarkers(){stadiumDetails?.updateMarkers(worldZForYards(ballSpotYards),worldZForYards(lineToGainYards));losLine.position.z=worldZForYards(ballSpotYards);gainLine.position.z=worldZForYards(lineToGainYards);gainLine.visible=lineToGainYards<50;}


function pathFor(type,start){
  return rawPathFor(type,start).map(p=>p.set(THREE.MathUtils.clamp(p.x,-24.7,24.7),0,Math.max(ENDZONE_BACK_Z+1,p.z)));
}
function rawPathFor(type,start){const x=start.x,z=start.z;switch(type){
case'Option':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-10)];
case'Bubble Left':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x-5,0,z+2.5),new THREE.Vector3(x-6,0,z+3)];
case'Slip':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+2,0,z-2),new THREE.Vector3(x+3,0,z+2)];
case'Bubble Wheel':{const side=x>=0?1:-1;return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+side*5,0,z+2),new THREE.Vector3(x+side*7,0,z-5),new THREE.Vector3(x+side*7,0,z-42)];}
case'Tunnel Go':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-3),new THREE.Vector3(x-Math.sign(x)*5,0,z+1),new THREE.Vector3(x-Math.sign(x)*6,0,z-40)];
case'Return':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+5,0,z-3),new THREE.Vector3(-16,0,z-6),new THREE.Vector3(-20,0,z-25)];
case'Bubble':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+5,0,z+2.5),new THREE.Vector3(x+6,0,z+3)];
case'Tunnel':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-2),new THREE.Vector3(x-Math.sign(x)*6,0,z+2)];
case'Lead':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-3),new THREE.Vector3(x,0,z-5)];
case'Stick':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-9),new THREE.Vector3(x+Math.sign(x)*2,0,z-9)];
case'Fade':{const side=x>=0?1:-1;return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+side*2,0,z-7),new THREE.Vector3(x+side*5,0,z-24),new THREE.Vector3(x+side*6,0,z-48)]}
case'Curl':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-17),new THREE.Vector3(x*.92,0,z-14)];
case'Double Move':{const side=x>=0?1:-1;return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-9),new THREE.Vector3(x-side*3.8,0,z-12),new THREE.Vector3(x,0,z-20),new THREE.Vector3(x+side*2,0,z-45)]}
case'Go':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+(x>0?1.4:-1.4),0,z-20),new THREE.Vector3(x+(x>0?2.8:-2.8),0,z-50)];
case'Slant':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x*.72,0,z-6),new THREE.Vector3(x*.12,0,z-20),new THREE.Vector3(-x*.23,0,z-38)];
case'Out':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-10),new THREE.Vector3(x+(x>0?10:-10),0,z-14),new THREE.Vector3(x+(x>0?15:-15),0,z-21)];
case'Post':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x*.92,0,z-13),new THREE.Vector3(x*.35,0,z-30),new THREE.Vector3(0,0,z-49)];
case'Corner':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x*.94,0,z-13),new THREE.Vector3(x+(x>0?8:-8),0,z-31),new THREE.Vector3(x+(x>0?14:-14),0,z-47)];
case'Drag':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x*.65,0,z-4),new THREE.Vector3(-x*.45,0,z-8),new THREE.Vector3(-x,0,z-12)];
case'Dig':return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-12),new THREE.Vector3(x*.18,0,z-17),new THREE.Vector3(-x*.58,0,z-17)];
case'Comeback':{const side=x>=0?1:-1;return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+side*.7,0,z-17),new THREE.Vector3(x+side*1.5,0,z-25),new THREE.Vector3(x+side*6.8,0,z-19),new THREE.Vector3(x+side*12,0,z-13)]}
case'Flat':{const side=x>=0?1:-1;return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+side*5.5,0,z-2.5),new THREE.Vector3(x+side*12,0,z-5),new THREE.Vector3(x+side*18,0,z-8)]}
case'Wheel':{const side=x>=0?1:-1;return[new THREE.Vector3(x,0,z),new THREE.Vector3(x+side*6.5,0,z-3.5),new THREE.Vector3(x+side*10,0,z-9),new THREE.Vector3(x+side*10.8,0,z-28),new THREE.Vector3(x+side*11.5,0,z-48)]}
default:return[new THREE.Vector3(x,0,z),new THREE.Vector3(x,0,z-18),new THREE.Vector3(x,0,z-40)];}}
function routePosition(points,distance){let left=distance;for(let i=0;i<points.length-1;i++){const a=points[i],b=points[i+1],len=a.distanceTo(b);if(len<.00001)continue;if(left<=len)return a.clone().lerp(b,left/len);left-=len}const a=points[points.length-2],b=points[points.length-1],dir=b.clone().sub(a).normalize();return b.clone().addScaledVector(dir,left)}
function clearPlayers(){replayObjects=null;for(const a of [...receivers,...defenders]){scene.remove(a.mesh);R?.release(a.mesh);const geos=new Set(),mats=new Set();a.mesh.traverse(o=>{if(o.geometry&&!o.geometry.userData.sharedPlayer)geos.add(o.geometry);if(o.material&&o.material!==shadowMat)mats.add(o.material)});geos.forEach(g=>g.dispose());mats.forEach(m=>m.dispose())}receivers=[];defenders=[]}
function currentSkill(){return opponentForRound(activeRound()).skill}
function chooseDefense(){
  const opp=opponentForRound(activeRound()),identity=V.identity(activeRound());
  if(!snapDefense){
    snapMemory=V.tendencies(franchise.scouting);
    const id=V.chooseCoverage(identity,snapMemory,franchise.lastCoverage);
    snapDefense=defenses.find(d=>d.id===id);franchise.lastCoverage=id;
  }
  currentDefense=snapDefense;currentOpponentUniform=opponentUniformForRound(activeRound());
  $('defenseName').textContent=`R${activeRound()}${franchise.rematchRound?" REMATCH":""} · ${opp.name} · ${identity.name}`;
  $('defenseDesc').textContent=`${currentDefense.id==='disguise'?'Two-high shell — read the rotation.':currentDefense.name} · ${matchWeather().name}`;
}
function recordAttempt(receiver=null){
  if(!attemptPending)return;attemptPending=false;
  const nearest=receiver||receivers.reduce((best,r)=>!best||r.mesh.position.distanceToSquared(ball.position)<best.mesh.position.distanceToSquared(ball.position)?r:best,null);
  if(!nearest)return;
  if(playLog)playLog.target=nearest.profile.id;
  franchise.scouting=V.remember(franchise.scouting,{depth:(worldZForYards(snapSpotYards)-ball.position.z)/WORLD_PER_YARD,target:receivers.indexOf(nearest),route:nearest.route});
}

function resetAim(){yaw=0;pitch=-.08;camera.position.set(0,2.25,worldZForYards(ballSpotYards)+14);applyCamera()}
function setupPlay(increment=false){
  formationMotion=null;pumpReadyAt=0;playLog=null;
  pendingCatch=null;ballTumble=0;ballSpin=0;receiverDrop=false;ball.userData.spin.rotation.set(0,0,0);
  if(increment){playNumber++;snapDefense=null;}attemptPending=false;throwBobbles=0;transition=null;replayFrames=[];replayPool=[];replayInterval=1/30;replayRecording=false;replayEligible=false;applyFieldTheme();clearPlayers();clearRouteVisuals();ball.visible=false;ballLive=false;ballCarrier=null;tackler=null;tackleTimer=0;arcLine.visible=false;landRing.visible=false;charging=false;chargePower=0;loftBias=0;throwClock=8;predictedFlightTime=0;updateCharge();updateThrowClock();resetAim();chooseDefense();updateFieldMarkers();
  const play=plays[selectedPlay],xs=play.xs||[-18,-6,6,18],skill=currentSkill(),losZ=worldZForYards(ballSpotYards);
  for(let i=0;i<4;i++){
    const start=new THREE.Vector3(xs[i],0,losZ-.9+(play.depths?.[i]??(i%2)*.35)),profile=franchise.team[i],mesh=makePlayer(true,playerLook(profile));mesh.position.copy(start);scene.add(mesh);
    const speed=V.movement(profile).speed*(V.signature(profile)==='Deep Threat'?1.015:1),catchReach=THREE.MathUtils.lerp(.94,1.08,P.effective(profile.catching)/100);
    receivers.push({mesh,label:['X','H','Y','Z'][i],profile,style:V.playstyle(profile),screenTarget:play.screen===i,route:play.routes[i],path:pathFor(play.routes[i],start),speed,maxSpeed:speed,catchReach,signature:V.signature(profile),distance:0,start,velocity:new THREE.Vector3(0,0,-speed*.15),impactVel:new THREE.Vector3(),heading:new THREE.Vector3(0,0,-1),history:[],shoveCooldown:0,shoveSlow:0,stagger:0,shoveAnim:0,jumpY:0,jumpVel:0,jumpCooldown:0,trackingBall:false,burst:0,catchPose:0,plantPose:0,comebackActive:false,comebackPlant:0,underthrowDifficulty:0,runPhase:Math.random()*Math.PI*2,runIntensity:0,hasBall:false});
  }
  receivers.forEach((r,i)=>{
    const m=makePlayer(false,buildDefenderAppearance(activeRound()));scene.add(m);let start;if(currentDefense.corner==='press'||currentDefense.corner==='allout')start=new THREE.Vector3(r.start.x+(Math.random()-.5)*.8,0,r.start.z-1.6);else start=new THREE.Vector3(r.start.x+(Math.random()-.5)*1.6,0,r.start.z-5.5-Math.random()*1.7);m.position.copy(start);
    defenders.push(makeDefender(m,'corner',i,skill));
  });
  let sets;
  if(currentDefense.safety==='robber')sets=[[0,losZ-18,'robber'],[-14,losZ-29,'safety']];
  else if(currentDefense.safety==='single')sets=[[0,losZ-30,'safety']];
  else if(currentDefense.safety==='aggressive')sets=[[0,losZ-24,'safety']];
  else sets=[[-12,losZ-28,'safety'],[12,losZ-28,'safety']];
  for(const [x,z,kind] of sets){const m=makePlayer(false,buildDefenderAppearance(activeRound()));m.position.set(x,0,Math.max(ENDZONE_BACK_Z+2,z-snapMemory.depth-V.identity(activeRound()).depth));scene.add(m);defenders.push(makeDefender(m,kind,null,skill))}
  for(const a of [...receivers,...defenders])animatePlayerContact(a,0);
  snapSpotYards=ballSpotYards;playState='call';snapTime=0;nextCount=0;audibleReceiverIndex=null;$('audiblePanel').style.display='none';drawRouteVisuals();$('headline').textContent='CALL THE PLAY';$('detail').textContent=`${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)} from the ${Math.round(50-ballSpotYards)}. Selected: ${plays[selectedPlay].name}. Tap a receiver or press X / H / Y / Z to audible.`;$('playCallPanel').style.display='block';$('snapBtn').disabled=false;renderRoutes();updateScore();updatePlayButtons();checkpoint();
}
function beginCountdown(){
  if(inputBlocked()||playState!=='call')return;playLog=F.newPlay(plays[selectedPlay].name);franchise.matchInProgress=true;checkpoint();closeAudible(true);playState='countdown';snapTime=gameTime+2200;nextCount=3;$('snapBtn').disabled=true;showMessage('READY',`${plays[selectedPlay].name} locked in — routes stay drawn until the snap.`,650);
}
function makeDefender(mesh,kind,target,skill){
  const growth=P.defenseProgress(activeRound()),identity=V.identity(activeRound());
  const reaction=THREE.MathUtils.lerp(.34,.085,skill)+(Math.random()*.06-.03);const turnRate=THREE.MathUtils.lerp(3.7,7.3,skill);const accel=THREE.MathUtils.lerp(13,24,skill);const maxSpeed=THREE.MathUtils.lerp(7.4,9.05,skill)+(kind==='safety'?.15:0);
  const sizeName=mesh.userData?.appearance?.sizeName||'Balanced',sizeStrength={Big:8,Tall:3,Balanced:0,Compact:-3,Lean:-4}[sizeName]||0,strength=clampRating(50+skill*37+randInt(-9,9)+sizeStrength+(kind==='safety'?2:0));
  return{mesh,kind,target,zoneX:mesh.position.x,pursuitRole:0,tackleCooldown:0,diveTime:0,diveRecovery:0,technique:V.tackleTechnique(activeRound(),skill),strength:(strength+identity.strength)*V.weather(matchWeather().name).contact,fakeUntil:0,velocity:new THREE.Vector3(),impactVel:new THREE.Vector3(),heading:new THREE.Vector3(0,0,-1),reaction:Math.max(.06,reaction),turnRate:turnRate+growth.turn,accel:accel+growth.accel,maxSpeed:maxSpeed+growth.speed+identity.speed,jumpVelocity:growth.jump,depth: growth.depth,smartDeep:Math.random()<growth.smartChance,hands:growth.hands+identity.hands,commit:0,lastDesired:new THREE.Vector3(),shoveCooldown:0,shoveSlow:0,stagger:0,shoveAnim:0,jumpY:0,jumpVel:0,jumpCooldown:0,swatPose:0,ballSeen:false,plantPose:0,runPhase:Math.random()*Math.PI*2,runIntensity:0};
}
function renderRoutes(){
  $('routes').innerHTML=`<div id="playName">${plays[selectedPlay].name} · AUDIBLES ON</div>`+receivers.map((r,i)=>`<div class="routeRow audibleTap" data-audible-receiver="${i}"><span class="routeName">${r.label} · ${escapeHTML(r.profile.name.split(' ')[0])}</span><span class="routeType">${r.route}${r.audibled?' *':''} · ${escapeHTML(r.style||V.playstyle(r.profile))}</span></div>`).join('');
  document.querySelectorAll('[data-audible-receiver]').forEach(el=>el.addEventListener('pointerdown',e=>{e.stopPropagation();if(playState==='call')openAudible(Number(el.dataset.audibleReceiver))}));
}
function setAudibleRing(){receivers.forEach((r,i)=>{const ring=r.mesh.userData.trackRing;if(ring)ring.visible=playState==='call'&&i===audibleReceiverIndex})}
function renderAudiblePanel(){
  if(audibleReceiverIndex==null||!receivers[audibleReceiverIndex])return;const selected=receivers[audibleReceiverIndex];
  $('audibleTitle').textContent=`AUDIBLE · ${selected.label} · ${selected.profile.name}`;
  $('audibleReceivers').innerHTML=receivers.map((r,i)=>`<button class="audibleReceiverBtn ${i===audibleReceiverIndex?'selected':''}" data-audible-slot="${i}">${r.label}<br>${r.route}</button>`).join('');
  $('audibleRouteGrid').innerHTML=routeOptions.map(opt=>`<button class="audibleRouteBtn ${selected.route===opt.name?'current':''}" data-audible-route="${opt.name}"><span class="key">${opt.key}</span><span>${opt.name}</span></button>`).join('');
  document.querySelectorAll('[data-audible-slot]').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();openAudible(Number(btn.dataset.audibleSlot))}));
  document.querySelectorAll('[data-audible-route]').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();assignAudibleRoute(btn.dataset.audibleRoute)}));
}
function openAudible(index){
  if(playState!=='call'||!receivers[index])return;audibleReceiverIndex=index;$('audiblePanel').style.display='block';setAudibleRing();renderAudiblePanel();
  showMessage('AUDIBLE READY',`${receivers[index].label} · ${receivers[index].profile.name.split(' ')[0]} selected. Choose any route or press its letter.`,900);
}
function closeAudible(clearSelection=true){if(clearSelection)audibleReceiverIndex=null;$('audiblePanel').style.display='none';setAudibleRing()}
function assignAudibleRoute(route){
  if(playState!=='call'||audibleReceiverIndex==null||!routePool.includes(route))return;const r=receivers[audibleReceiverIndex],slot=audibleReceiverIndex;r.route=route;r.screenTarget=false;r.optionRead=true;r.path=pathFor(route,r.start);r.audibled=true;drawRouteVisuals();renderRoutes();
  showMessage('AUDIBLE SET',`${r.label} · ${r.profile.name.split(' ')[0]} → ${route}.`,900);closeAudible(true);
}
// Decisions occur only on explicitly designated option routes, before the throw.
function readOptionRoute(r){
  if(r.route!=='Option'||r.optionRead||ballLive||playState!=='live')return;
  const skill=Math.min(1,(P.effective(r.profile.cutting)+P.effective(r.profile.turning))/200);
  if(r.distance<(6+(1-skill)*5)*(1-F.chemistry(r.profile,plays[selectedPlay].name))+(1-F.awareness(r.profile)/100)*.3)return;
  const d=defenders.filter(d=>d.kind==='corner').sort((a,b)=>a.mesh.position.distanceToSquared(r.mesh.position)-b.mesh.position.distanceToSquared(r.mesh.position))[0];
  if(!d)return;
  const pos=r.mesh.position,deep=d.mesh.position.z<pos.z-4,under=d.mesh.position.z>pos.z-1,inside=Math.abs(d.mesh.position.x)<Math.abs(pos.x);
  // Lower route ratings read later and may choose the safe curl instead.
  const route=Math.random()>skill*.8+.18?'Curl':deep?'Curl':under?'Go':inside?'Out':'Dig';
  r.optionRead=true;r.route=route;r.path=pathFor(route,pos);r.distance=0;
  if(route==='Curl')r.path=[pos.clone(),pos.clone().add(new THREE.Vector3(0,0,-2)),pos.clone().add(new THREE.Vector3(0,0,-1))];
  showMessage('OPTION READ',`${r.label}: ${route.toUpperCase()}`,600);renderRoutes();
}
function adjustFormation(kind){
  if(inputBlocked()||playState!=='call'||audibleReceiverIndex==null)return;
  const r=receivers[audibleReceiverIndex],side=Math.sign(r.start.x)||1,base=plays[selectedPlay].xs?.[audibleReceiverIndex]??[-18,-6,6,18][audibleReceiverIndex];
  if(kind==='motion'){
    if(!['Motion','Bunch / Stack','Trick Plays'].includes(V.category(plays[selectedPlay]))){showMessage('FIXED RELEASE','Motion is available in Motion, Bunch / Stack and Trick Plays.',900);return;}
    formationMotion={slot:audibleReceiverIndex,to:THREE.MathUtils.clamp(-r.start.x*.55,-12,12)};showMessage('MOTION SET',`${r.label} crosses before the snap.`,700);return;
  }
  const next=THREE.MathUtils.clamp(r.start.x+side*(kind==='wide'?2:-2),Math.max(-23,base-4),Math.min(23,base+4));
  if(receivers.some(a=>a!==r&&Math.abs(a.start.x-next)<1.3&&Math.abs(a.start.z-r.start.z)<1.3))return;
  r.start.x=next;r.mesh.position.x=next;r.path=pathFor(r.route,r.start);r.distance=0;r.history=[];
  drawRouteVisuals();showMessage('ALIGNMENT SET',`${r.label}: ${kind==='wide'?'wider release':'tighter release'}`,650);
}
function pumpFake(){
  if(inputBlocked()||playState!=='live'||ballLive||gameTime<pumpReadyAt)return false;
  const aim=new THREE.Vector3();camera.getWorldDirection(aim);
  const target=receivers.reduce((best,r)=>{const direction=r.mesh.position.clone().add(new THREE.Vector3(0,1.5,0)).sub(camera.position).normalize();const score=direction.dot(aim);return !best||score>best.score?{r,score}:best;},null);
  if(!target)return false;
  const slot=receivers.indexOf(target.r),concept=plays[selectedPlay].name,memory=(Array.isArray(franchise.pumpMemory)?franchise.pumpMemory:[]).slice(-64),identity=V.identity(activeRound());
  const repetitions=(franchise.conceptMemory||[]).filter(x=>x===concept).length;
  const chance=V.pumpChance(memory,slot,concept,identity.discipline??.5)*Math.exp(-Math.max(0,repetitions-1)*.15);
  franchise.pumpMemory=[...memory,{target:slot,concept}].slice(-64);pumpReadyAt=gameTime+1100;
  // Never freeze a defender. At most one nearby DB takes a short false step.
  const d=defenders.filter(d=>d.mesh.position.distanceTo(target.r.mesh.position)<18&&d.fakeUntil<=gameTime).sort((a,b)=>a.mesh.position.distanceToSquared(target.r.mesh.position)-b.mesh.position.distanceToSquared(target.r.mesh.position))[0];
  let bite=false;
  if(d&&Math.random()<chance){d.fakeTarget=target.r.mesh.position.clone().addScaledVector(target.r.velocity,.16);d.fakeUntil=gameTime+220+chance*200;d.plantPose=.5;bite=true;}
  flashResult(bite?'PUMP — DEFENDER BIT':'PUMP — COVERAGE HELD',bite,550);checkpoint();return true;
}
$('pumpBtn').onclick=pumpFake;
for(const kind of ['wide','tight','motion'])$('formation'+kind).onclick=()=>adjustFormation(kind);
function findReceiverAtScreen(clientX,clientY){
  if(playState!=='call'||!receivers.length)return-1;let best=-1,bestDist=Infinity;for(let i=0;i<receivers.length;i++){const p=receivers[i].mesh.position.clone();p.y+=1.15;p.project(camera);if(p.z<-1||p.z>1)continue;const sx=(p.x*.5+.5)*innerWidth,sy=(-p.y*.5+.5)*innerHeight,dist=Math.hypot(clientX-sx,clientY-sy);if(dist<bestDist){bestDist=dist;best=i}}const radius=matchMedia('(pointer:coarse)').matches?76:50;return bestDist<=radius?best:-1;
}
function updateThrowClock(){const el=$('throwClock');el.textContent=`THROW CLOCK · ${throwClock.toFixed(1)}`;el.classList.toggle('urgent',throwClock<=2.25&&playState==='live')}
function updateScore(){const team=franchise.identity;stadiumDetails?.score(team,{...F.opponent(activeRound()),name:opponentForRound(activeRound()).name},seriesOffense,seriesDefense,activeRound());const toGo=Math.max(0,Math.ceil(lineToGainYards-ballSpotYards)),opp=opponentForRound(activeRound());$('score').textContent=`${team.logo} ${seriesOffense}–${seriesDefense} ${F.opponent(activeRound()).logo}`;$('tournamentLine').textContent=`Round ${activeRound()}${franchise.rematchRound?" REMATCH · 20% CASH":""} · Best of 5 · ${opp.name} · $${franchise.cash}`;$('subscore').textContent=`${downLabel()} & ${toGo} · Ball: ${Math.round(50-ballSpotYards)} yd line · Drive ${Math.round(ballSpotYards)}/50 · ${catches} catches · ${ints} INT`}
function showMessage(head,detail,ms=1300){$('headline').textContent=head;$('detail').textContent=detail||'';messageTimer=gameTime+ms}
function flashResult(text,good=true,ms=900){const el=$('resultFlash');el.textContent=text;el.className=`${good?'good':'bad'} show`;resultFlashTimer=gameTime+ms}
function suggestedPlays(){return plays.map((p,i)=>({i,...F.suggestion(p,{down,toGo:lineToGainYards-ballSpotYards,spot:ballSpotYards})})).sort((a,b)=>b.score-a.score).slice(0,6).map(p=>p.i);}
function diagramPaths(p){return p.routes.map((route,i)=>pathFor(route,new THREE.Vector3((p.xs||[-18,-6,6,18])[i],0,30+(p.depths?.[i]||0))));}
function updatePlayButtons(){
  const p=plays[selectedPlay],suggested=suggestedPlays();$('schemeHint').textContent=playCategory==='Suggested'?F.suggestion(p,{down,toGo:lineToGainYards-ballSpotYards,spot:ballSpotYards}).reason:p.hint||'Balanced spacing. Audible individual receivers to suit the coverage.';
  document.querySelectorAll('.playBtn[data-play]').forEach(b=>{b.classList.toggle('selected',Number(b.dataset.play)===selectedPlay);b.hidden=playCategory==='Suggested'?!suggested.includes(Number(b.dataset.play)):V.category(plays[Number(b.dataset.play)])!==playCategory;});
  document.querySelectorAll('[data-category]').forEach(b=>{b.classList.toggle('selected',b.dataset.category===playCategory);b.setAttribute('aria-selected',String(b.dataset.category===playCategory));});
  if(studio)$('selectedPlayDetail').innerHTML=studio.detail(p,selectedPlay);
}
function buildPlayButtons(){
  ['Suggested',...V.categories].forEach(category=>{const b=document.createElement('button');b.textContent=category;b.dataset.category=category;b.setAttribute('role','tab');b.onclick=()=>{if(playState!=='call')return;playCategory=category;updatePlayButtons();};$('playTabs').appendChild(b);});
  plays.forEach((p,i)=>{const b=document.createElement('button');b.className='playBtn';b.dataset.play=i;b.innerHTML=`${globalThis.QBFranchiseUI?QBFranchiseUI.diagram(p,i,diagramPaths(p)):''}<span>${escapeHTML(p.name)}</span><small>${V.category(p)}</small>`;b.title=p.hint||'Choose a concept, then adjust individual routes.';b.addEventListener('click',e=>{e.stopPropagation();if(playState!=='call')return;selectedPlay=i;setupPlay(false)});$('playGrid').appendChild(b)});updatePlayButtons();
}

buildPlayButtons();

// Receivers run with real velocity/acceleration, so contact changes their path and momentum instead of being erased next frame.
function getBallLanding(pos=ball.position,vel=ballVel){
  if(!ballLive)return null;const y=Math.max(.02,pos.y);const disc=vel.y*vel.y+2*9.81*y;const t=(vel.y+Math.sqrt(Math.max(0,disc)))/9.81;if(!isFinite(t)||t<=0)return null;return{point:new THREE.Vector3(pos.x+vel.x*t,.02,pos.z+vel.z*t),time:t};
}
function updateJump(a,dt,shouldJump=false){
  a.landingPose=Math.max(0,(a.landingPose||0)-dt*4);
  a.jumpCooldown=Math.max(0,(a.jumpCooldown||0)-dt);
  if(shouldJump&&(a.jumpY||0)<.03&&a.jumpCooldown<=0){
    a.jumpVel=a.profile?P.traits(a.profile).jumpVelocity:(a.jumpVelocity||3.25);
    a.jumpCooldown=a.jumpVel/9.81*2+.16;a.takeoffPose=1;
  }
  a.takeoffPose=Math.max(0,(a.takeoffPose||0)-dt*7);
  if(a.jumpY>0||a.jumpVel>0){
    a.jumpY=Math.max(0,(a.jumpY||0)+a.jumpVel*dt-4.905*dt*dt);a.jumpVel-=9.81*dt;
    if(a.jumpY<=0){a.jumpY=0;a.jumpVel=0;a.landingPose=1;}
  }
  a.mesh.position.y=a.jumpY||0;
}
// Start the jump before arrival, so the hands meet a high pass near the apex.
function wantsHighPoint(a){
  if(!ballLive||!(a.profile?a.trackingBall:a.ballSeen)||a.jumpY>.03||a.jumpCooldown>0)return false;
  const jump=a.profile?P.traits(a.profile).jumpVelocity:(a.jumpVelocity||3.25),apex=jump/9.81;
  const standing=2.22*a.mesh.scale.y,peak=standing+jump*jump/19.62;
  const athletic=a.profile?Math.min(1,P.effective(a.profile.athleticism)/100):currentSkill();
  const time=apex*(.88+athletic*.12),point=ball.position.clone().addScaledVector(ballVel,time);
  point.y-=4.905*time*time;
  const future=a.mesh.position.clone().addScaledVector(a.velocity,time);
  const gap=Math.hypot(point.x-future.x,point.z-future.z);
  return point.y>standing-.12&&point.y<=peak+.12&&gap<.65+athletic*.4;
}

function ballApproach(point,maxTime=1.15){
  if(!ballLive||ballVel.lengthSq()<.01)return null;const rel=point.clone().sub(ball.position),t=THREE.MathUtils.clamp(rel.dot(ballVel)/ballVel.lengthSq(),0,maxTime);const p=ball.position.clone().addScaledVector(ballVel,t);p.y-=.5*9.81*t*t;return{time:t,dist:p.distanceTo(point),point:p};
}

// Receiver awareness: look for the FIRST catchable point on the actual flight, not just where the ball hits the turf.
// This keeps normal route running intact until a pass enters a reasonable route/receiver neighborhood, then lets the
// receiver attack the catch window. Underthrows can be pursued backward, but the plant/turn cost below still matters.
function receiverBallPlan(r){
  if(!ballLive||ballVel.lengthSq()<.01)return null;
  const landing=getBallLanding(),maxT=Math.min(3.25,landing?landing.time:3.25);
  const jump=P.traits(r.profile).jumpVelocity,highReach=2.4*r.mesh.scale.y+jump*jump/19.62;
  let best=null,inStride=null;
  for(let t=.01;t<=maxT;t+=.04){
    const p=ball.position.clone().addScaledVector(ballVel,t);p.y-=.5*9.81*t*t;
    if(p.y<.24||p.y>highReach)continue;
    const dx=p.x-r.mesh.position.x,dz=p.z-r.mesh.position.z,flatDist=Math.hypot(dx,dz);
    const toPoint=new THREE.Vector3(dx,0,dz),facing=toPoint.lengthSq()>.001?r.heading.dot(toPoint.clone().normalize()):1;

    // Check a route corridor that includes several yards behind the receiver as well as where his route is headed.
    // That is what makes a sensible comeback possible without making unrelated cross-field throws magnetize everyone.
    let routeGap=Infinity;
    const lookAhead=r.maxSpeed*Math.min(t,2.15);
    for(const off of[-8,-4,0,lookAhead*.45,lookAhead]){
      const rp=routePosition(r.path,Math.max(0,r.distance+off));
      routeGap=Math.min(routeGap,Math.hypot(p.x-rp.x,p.z-rp.z));
    }
    const futureRoute=routePosition(r.path,r.distance+lookAhead),futureGap=Math.hypot(p.x-futureRoute.x,p.z-futureRoute.z);
    const inReceiverNeighborhood=routeGap<(r.style==='Deep Threat'?13:11.8)||futureGap<(r.style==='Deep Threat'?16:14.8)||(flatDist<7.0&&routeGap<16.5);
    // Once acquired, route progress must not veto a physically reachable pass.
    if(!inReceiverNeighborhood&&!r.ballPursuit)continue;

    // A hard 180 still costs time. Forward/side pursuit gets a little more usable burst than a full comeback.
    const turnSkill=P.effective((r.profile?.turning||50))/100,turnFactorBase=facing<-.60?.72:facing<-.15?.82:facing<.35?.92:1,turnFactor=THREE.MathUtils.clamp(turnFactorBase+((turnSkill-.5)*.12)*(facing<.35?1:.25),.64,1.04);
    const usableReach=(r.maxSpeed*(P.traits(r.profile).pursuitBurst+.02)*t+1.02)*turnFactor;
    if(flatDist>usableReach)continue;

    const difficulty=(flatDist/Math.max(.01,usableReach))*1.15+routeGap*.035+Math.max(0,-facing)*.16+t*.035;
    const plan={point:p,time:t,routeGap,facing,flatDist,difficulty};
    const strideGap=Math.hypot(dx-r.velocity.x*t,dz-r.velocity.z*t);
    // A catchable ball already meeting his momentum needs no braking, burst or comeback.
    if(r.velocity.length()>r.maxSpeed*.35&&facing>0&&p.y>=1.0&&strideGap<.55){
      if(!inStride||strideGap<inStride.strideGap)inStride={...plan,strideGap,strideSpeed:r.velocity.length(),strideHeading:r.velocity.clone().setY(0).normalize()};
    }
    // Earliest reachable window is preferred; among near-identical windows prefer the cleaner route fit.
    if(!best||t<best.time-.055||(Math.abs(t-best.time)<=.055&&difficulty<best.difficulty))best=plan;
  }
  const plan=inStride||best;
  if(plan){r.ballPursuit={...plan,expires:throwTime+plan.time};return plan;}
  // Bridge brief sampling/reachability gaps, but never chase an expired catch window.
  const previous=r.ballPursuit;
  if(previous&&previous.expires>throwTime){
    const time=previous.expires-throwTime,point=ball.position.clone().addScaledVector(ballVel,time);
    point.y-=.5*9.81*time*time;
    if(point.y>=.1&&point.y<=highReach){
      const to=point.clone().sub(r.mesh.position).setY(0),flatDist=to.length();
      return {point,time,flatDist,routeGap:previous.routeGap,facing:flatDist>.001?r.heading.dot(to.normalize()):1};
    }
  }
  r.ballPursuit=null;
  return null;
}
function defenderSeesBall(d){
  if(!ballLive)return false;const eye=d.mesh.position.clone().add(new THREE.Vector3(0,1.72,0)),to=ball.position.clone().sub(eye),dist=to.length();if(dist<.01)return true;const flat=to.clone().setY(0);if(flat.lengthSq()<.01)return true;flat.normalize();const facing=d.heading.dot(flat),skill=currentSkill();return facing>THREE.MathUtils.lerp(.18,-.12,skill)||(dist<THREE.MathUtils.lerp(3.0,5.8,skill)&&throwTime>d.reaction*.55);
}
// Predict a nearby intercept, not the eventual ground landing. The receiver's
// physical pursuit remains authoritative; this only aims bounded arms/hands.
function receiverHandTarget(a){
  if(!ballLive||!(a.profile?a.trackingBall:a.ballSeen)||a.hasBall)return null;
  const chest=a.mesh.position.clone().add(new THREE.Vector3(0,1.35*a.mesh.scale.y,0));
  let best=null,bestDistance=Infinity;
  for(let t=0;t<=.2401;t+=.04){
    const point=ball.position.clone().addScaledVector(ballVel,t);point.y-=4.905*t*t;
    const future=chest.clone().addScaledVector(a.velocity,t);
    const distance=point.distanceToSquared(future);
    if(distance<bestDistance){bestDistance=distance;best=point;}
  }
  if(bestDistance>9)return null;
  const space=a.mesh.userData.visualRig||a.mesh;
  a.mesh.updateMatrixWorld(true);
  return space.worldToLocal(best);
}
function trackReceiverHands(a,dt){
  const hands=a.mesh.userData.hands||[],arms=a.mesh.userData.arms||[];
  const target=receiverHandTarget(a);
  if(!target){a.handTargets=null;return;}
  a.catchPose=Math.max(a.catchPose||0,.7);
  if(!a.handTargets)a.handTargets=hands.map(h=>h.position.clone());
  for(let i=0;i<hands.length;i++){
    const side=i===0?-1:1,shoulder=new THREE.Vector3(side*.48,1.48,.02);
    const wanted=target.clone();wanted.x+=side*.10;
    const offset=wanted.clone().sub(shoulder),maxReach=.89;
    if(offset.length()>maxReach)offset.setLength(maxReach);
    wanted.copy(shoulder).add(offset);
    const delta=wanted.clone().sub(a.handTargets[i]),step=Math.max(0,dt)*(a.profile?5+Math.min(1,P.effective(a.profile.athleticism)/100)*3:5+currentSkill()*3);
    if(delta.length()>step)delta.setLength(step);
    a.handTargets[i].add(delta);hands[i].position.copy(a.handTargets[i]);
    // Orient the existing capsule along shoulder-to-hand, without stretching it.
    if(arms[i]){
      const direction=hands[i].position.clone().sub(shoulder);
      arms[i].position.copy(shoulder).addScaledVector(direction,.5);
      if(direction.lengthSq()>.0001)arms[i].quaternion.setFromUnitVectors(new THREE.Vector3(0,1,0),direction.normalize());
    }
  }
}
function animatePlayerContact(a,dt){
  if(a.finishPose){poseFinishPlayer(a);return;}
  a.shoveAnim=Math.max(0,(a.shoveAnim||0)-dt);const arms=a.mesh.userData.arms||[],hands=a.mesh.userData.hands||[],feet=a.mesh.userData.feet||[],legs=a.mesh.userData.legs||[];const shove=a.shoveAnim>0?Math.sin((1-a.shoveAnim/.34)*Math.PI):0,reach=THREE.MathUtils.clamp(Math.max(a.catchPose||0,a.swatPose||0),0,1),plant=THREE.MathUtils.clamp(a.plantPose||0,0,1),run=THREE.MathUtils.clamp(a.runIntensity||0,0,1),stride=Math.sin(a.runPhase||0)*.70*run;
  for(let i=0;i<arms.length;i++){const side=i===0?-1:1,armRun=reach>.05?0:(i===0?stride:-stride)*.58;arms[i].rotation.y=0;arms[i].rotation.x=shove*1.18-reach*.72+armRun;arms[i].rotation.z=side*.08*(1-shove*.5)+side*reach*.19;arms[i].position.set(side*(.53-reach*.11),1.22+reach*.44,shove*.27+reach*.22)}
  for(let i=0;i<hands.length;i++){const side=i===0?-1:1;hands[i].position.set(side*(.53-reach*.23*(a.catchReach||1)),.86+reach*1.18*(a.catchReach||1),reach*.44*(a.catchReach||1)+shove*.18)}
  for(let i=0;i<legs.length;i++){legs[i].rotation.x=(i===0?stride:-stride);legs[i].position.z=(i===0?stride:-stride)*.10}
  for(let i=0;i<feet.length;i++){const side=i===0?-1:1,step=(i===0?stride:-stride);feet[i].rotation.x=step*.48;feet[i].rotation.y=side*plant*.58;feet[i].position.z=-.11+plant*(i===0?.10:-.08)+step*.18;feet[i].position.y=.16+Math.max(0,-step)*.05*run}
  const body=a.mesh.userData.body;
  const move=a.jukeAnim>0?Math.sin(Math.PI*(1-a.jukeAnim/(a.jukeDuration||.56))):0,side=a.jukeSide||1;
  const airborne=Math.min(1,(a.jumpY||0)*2),swat=(a.swatPose||0)>.2;
  if(body){body.rotation.x=shove*.12-plant*.16-run*.06-airborne*.16;
    body.rotation.z=plant*.035+move*side*.48;
    body.rotation.y=0;
    body.position.y=1.24-move*.10;}
  for(let i=0;i<legs.length;i++){legs[i].rotation.z=move*(i===0?-.35:.35);legs[i].rotation.x-=airborne*.55;}
  for(let i=0;i<feet.length;i++)feet[i].position.x=(i===0?-1:1)*(.22+move*.18);
  for(let i=0;i<arms.length;i++){
    arms[i].rotation.z+=move*side*.55;
    if(airborne>0&&reach>.2){arms[i].position.y+=airborne*.14;arms[i].rotation.x-=airborne*.28;}
    if(swat){arms[i].rotation.x-=i===0?.6:.15;arms[i].rotation.z+=i===0?-.35:.25;}
  }
  const rig=a.mesh.userData.visualRig;
  if(rig){
    const progress=a.jukeAnim>0?1-a.jukeAnim/(a.jukeDuration||.56):0;
    const fooled=!a.profile&&a.fakeUntil>gameTime;
    const stumble=fooled?Math.sin(Math.min(1,(a.fakeUntil-gameTime)/650)*Math.PI):0;
    rig.rotation.y=a.jukeMove==='SPIN'&&a.jukeAnim>0?side*progress*Math.PI*2:side*move*.35;
    rig.rotation.y+=Math.sin(a.runPhase||0)*run*.055;
    const stumbleRun=(a.stumbleTime||0)/.48;
    rig.rotation.y+=(a.stumbleSide||1)*Math.sin(stumbleRun*Math.PI)*.12;
    rig.rotation.z=(a.stumbleSide||1)*Math.sin(stumbleRun*Math.PI)*.23+side*move*.16+(a.fakeSide||1)*stumble*.25;
    rig.position.y=-move*.12-stumble*.10-(a.landingPose||0)*.15-(a.takeoffPose||0)*.06;
    if(fooled){for(let i=0;i<arms.length;i++)arms[i].rotation.z+=(i===0?-1:1)*stumble*.7;}
  }
  // Distinct one-arm swat, two-hand high point, and secured-ball tuck.
  for(let i=0;i<hands.length;i++){
    if(swat){hands[i].position.y+=i===0?.22:-.14;hands[i].position.x+=i===0?-.14:.18;}
    if(a.hasBall){hands[i].position.set(i===0?.12:.42,1.18,.32);if(a.securedHands)hands[i].position.lerp(a.securedHands[i],Math.max(0,(a.catchStyleTime||0)/.6));arms[i].rotation.x=-.75;arms[i].position.y=1.18;}
  }
  if(a.hasBall&&a.catchStyleTime>0){
    a.catchStyleTime=Math.max(0,a.catchStyleTime-dt);const pose=Math.sin(Math.PI*a.catchStyleTime/.6);
    if(rig){rig.rotation.x=a.catchStyle==='LOW CATCH'?pose*.35:a.catchStyle==='HIGH POINT'?-pose*.18:0;
      if(a.catchStyle==='BACK SHOULDER'||a.catchStyle==='CATCH AND TURN')rig.rotation.y+=pose*.5;
      if(a.catchStyle==='OVER THE SHOULDER')rig.rotation.y-=pose*.3;}
    if(a.catchStyle==='TOE TAP')for(const foot of feet){foot.position.z=-.1;foot.rotation.x=pose*.55;}
  }else if(rig)rig.rotation.x=(a.landingPose||0)*.16-airborne*.10+Math.sin((a.stumbleTime||0)/.48*Math.PI)*.32;
  if(a.catchDive){
    a.catchDive.time+=dt;const t=Math.min(1,a.catchDive.time/.22);
    if(rig){rig.rotation.x=t*1.12;rig.position.y=-t*.16;}
    a.catchPose=1;
  }
  R?.animate(a,dt,ball);
  trackReceiverHands(a,dt);
  if(a.stiffArmTime>0&&a.stiffTarget&&a.hasBall){
    a.mesh.updateMatrixWorld(true);const local=(rig||a.mesh).worldToLocal(a.stiffTarget.mesh.position.clone().add(new THREE.Vector3(0,1.35,0)));
    hands[0].position.lerp(local,Math.sin(Math.min(1,a.stiffArmTime/.42)*Math.PI)*.85);
  }
  poseJointedLimbs(a);
}

// Two-bone IK keeps elbows/knees bent and every hand attached to a real arm.
function poseBone(mesh,start,end){
  mesh.position.copy(start).add(end).multiplyScalar(.5);
  const direction=end.clone().sub(start).normalize();
  mesh.quaternion.setFromUnitVectors(new THREE.Vector3(0,1,0),direction);
}
function bendJoint(start,end,upper,lower,bend){
  const delta=end.clone().sub(start),distance=THREE.MathUtils.clamp(delta.length(),.02,upper+lower-.001);
  delta.normalize();end.copy(start).addScaledVector(delta,distance);
  const along=(upper*upper-lower*lower+distance*distance)/(2*distance);
  bend.addScaledVector(delta,-bend.dot(delta));
  if(bend.lengthSq()<.001)bend.set(0,0,1).addScaledVector(delta,-delta.z);
  return start.clone().addScaledVector(delta,along).addScaledVector(bend.normalize(),Math.sqrt(Math.max(0,upper*upper-along*along)));
}
function poseJointedLimbs(a){
  const {hands,arms,forearms,legs,shins,feet}=a.mesh.userData;
  if(!forearms||!shins)return;
  const run=a.runIntensity||0,air=Math.min(1,(a.jumpY||0)*3),land=a.landingPose||0;
  for(let i=0;i<2;i++){
    const side=i?1:-1,shoulder=new THREE.Vector3(side*.48,1.48,.02),hand=hands[i].position;
    if(!a.handTargets&&!a.hasBall&&!a.catchDive){
      const swing=Math.sin((a.runPhase||0)+i*Math.PI)*run;
      if(a.blockPose>0)hand.set(side*.32,1.35,.65);
      else if((a.catchPose||a.swatPose||0)<.2)hand.set(side*.51,1.05+Math.max(0,swing)*.16,swing*.32+.10);
    }
    const elbow=bendJoint(shoulder,hand,.44,.46,new THREE.Vector3(side*.7,-.65,-.55));
    poseBone(arms[i],shoulder,elbow);poseBone(forearms[i],elbow,hand);
    const phase=(a.runPhase||0)+i*Math.PI,stride=Math.sin(phase)*run*(1-air),plant=THREE.MathUtils.clamp(a.plantPose||0,0,1),planted=i===(a.visualCutSide>0?1:0);
    const hip=new THREE.Vector3(side*.21,.94,0),ankle=new THREE.Vector3(side*(.22+(a.jukeAnim>0?.08:0)),.15+Math.max(0,-stride)*.24+air*(i?.15:.26)+land*.10,stride*.34-air*.16);
    if(plant>.15&&planted&&air<.1){ankle.y=.15;ankle.x+=side*plant*.12;ankle.z*=1-plant*.65;}
    if(a.hurdleTime>0&&air>0){const tuck=Math.min(1,(a.jumpY||0)*2.2);ankle.y+=tuck*(i?.22:.40);ankle.z+=tuck*(i?-.16:.25);}
    const knee=bendJoint(hip,ankle,.45,.45,new THREE.Vector3(side*.06,0,1));
    poseBone(legs[i],hip,knee);poseBone(shins[i],knee,ankle);
    feet[i].position.copy(ankle);feet[i].position.z+=.07;
    feet[i].rotation.x=Math.PI/2+stride*.32+air*.3;
  }
}

function actorStrength(a){return THREE.MathUtils.clamp(a?.profile?(P.effective(a.profile.strength)||50)/100+P.traits(a.profile).bodyBonus+(a.signature==='Contact Balance'?.025:0)+(a.style==='Power Receiver'?.025:0):(a?.strength||50)/100,.01,1)}
function nearestCornerContact(r,maxDist=1.24){let best=null,bestDist=maxDist;for(const d of defenders){if(d.kind!=='corner')continue;const dist=Math.hypot(d.mesh.position.x-r.mesh.position.x,d.mesh.position.z-r.mesh.position.z);if(dist<bestDist){best=d;bestDist=dist}}return best?{player:best,dist:bestDist}:null}
function receiverContactFactors(r){const hit=nearestCornerContact(r);if(!hit)return{speed:1,accel:1,edge:0};const edge=actorStrength(r)-actorStrength(hit.player);return{speed:THREE.MathUtils.clamp(.92+edge*.42,.66,1.03),accel:THREE.MathUtils.clamp(.82+edge*.42,.54,1.04),edge}}
function contestedStrengthBonus(receiver,defender,distance){if(!receiver||!defender||distance>=1.45)return 0;const rs=actorStrength(receiver),ds=actorStrength(defender),closeness=distance<.85?1:.62;return THREE.MathUtils.clamp(((rs-ds)*.24+(rs-.5)*.06+(receiver.profile?Math.max(0,P.effective(receiver.profile.size)-50)*.001:0))*closeness,-.11,.10)}

function updateReceiver(r,dt,now){
  readOptionRoute(r);
  updateTricks(r,dt,false);
  r.shoveCooldown=Math.max(0,r.shoveCooldown-dt);r.shoveSlow=Math.max(0,r.shoveSlow-dt);r.stagger=Math.max(0,r.stagger-dt);r.burst=Math.max(0,(r.burst||0)-dt);r.runIntensity=THREE.MathUtils.lerp(r.runIntensity||0,Math.min(1,r.velocity.length()/r.maxSpeed),Math.min(1,dt*10));r.runPhase=(r.runPhase||0)+dt*Math.max(6,r.velocity.length()*2.25);r.catchPose=Math.max(0,(r.catchPose||0)-dt*4.8);r.comebackPlant=Math.max(0,(r.comebackPlant||0)-dt);r.plantPose=r.comebackPlant>0?Math.min(1,r.comebackPlant/.18):Math.max(0,(r.plantPose||0)-dt*5.5);
  if(!ballLive){r.ballPursuit=null;r.comebackActive=false;r.comebackPlant=0;r.underthrowDifficulty=0}
  r.distance+=r.velocity.length()*dt;let target=routePosition(r.path,r.distance+1.45);
  let settle=1;
  if(['Curl','Stick','Bubble','Bubble Left','Tunnel','Slip','Lead','Option'].includes(r.route)&&!ballLive){
    const end=r.path[r.path.length-1];let length=0;for(let i=1;i<r.path.length;i++)length+=r.path[i].distanceTo(r.path[i-1]);
    if(r.distance>length-2){target.copy(end);settle=THREE.MathUtils.clamp(r.mesh.position.distanceTo(end)/1.8,0,1);}
  }r.trackingBall=false;let stridePlan=null;
  if(ballLive){
    const plan=receiverBallPlan(r);
    if(plan){
      r.trackingBall=true;r.burst=Math.max(r.burst,.38);predictedLanding.copy(plan.point);predictedFlightTime=plan.time;
      stridePlan=plan.strideHeading?plan:null;
      const underthrown=!stridePlan&&plan.facing<-.10;
      if(underthrown&&!r.comebackActive){
        r.comebackActive=true;
        const turnSkill=P.effective((r.profile?.turning||50))/100;r.comebackPlant=THREE.MathUtils.clamp((.14+r.velocity.length()*.012+(-plan.facing)*.075)*THREE.MathUtils.lerp(1.16,.78,turnSkill),.12,.34);
        r.underthrowDifficulty=THREE.MathUtils.clamp(.06+(-plan.facing)*.16+plan.routeGap*.010,.06,.30);
      }else if(!underthrown&&r.comebackActive&&r.comebackPlant<=0){
        r.comebackActive=false;r.underthrowDifficulty=0;
      }
      // During the plant he has to gather himself; immediately afterward he attacks the earliest reachable catch point.
      if(stridePlan){r.comebackActive=false;r.comebackPlant=0;r.underthrowDifficulty=0;target.copy(r.mesh.position).add(stridePlan.strideHeading);}
      else if(r.comebackActive&&r.comebackPlant>0)target.copy(r.mesh.position).addScaledVector(r.heading,.38);else target.copy(plan.point);
    }
  }
  if(r.blockAim&&!r.trackingBall&&(playState==='run'||r.route==='Lead')){target.copy(r.blockAim);settle=THREE.MathUtils.clamp(target.distanceTo(r.mesh.position)/1.2,0,1);}
  if(!ballLive&&!r.blockAim&&['Route Technician','Possession Receiver'].includes(r.style)&&['Curl','Stick','Option'].includes(r.route)&&settle<1){
    const near=defenders.find(d=>d.mesh.position.distanceTo(target)<3);if(near)target.x=THREE.MathUtils.clamp(target.x+Math.sign(target.x-near.mesh.position.x||1)*(1+F.awareness(r.profile)*.006),-24,24);
  }
  const desired=target.clone().sub(r.mesh.position);desired.y=0;if(desired.lengthSq()>.0001)desired.normalize();else desired.copy(r.heading);
  const current=r.heading.clone().normalize(),angle=Math.acos(THREE.MathUtils.clamp(current.dot(desired),-1,1)),cross=current.x*desired.z-current.z*desired.x,sign=cross<0?-1:1,turnRating=P.effective((r.profile?.turning||50))/100,turnRateBase=r.comebackPlant>0?2.25:r.comebackActive?8.8:(r.trackingBall?12.6:(r.profile?3.8+6*turnRating:7.7)),turnRate=turnRateBase*V.weather(matchWeather().name).cut*THREE.MathUtils.lerp(.76,1.22,turnRating)*(r.signature==='Route Artist'?1.04:1),turn=Math.min(angle,turnRate*dt)*sign,c=Math.cos(turn),ss=Math.sin(turn);r.heading.set(current.x*c-current.z*ss,0,current.x*ss+current.z*c).normalize();
  if(angle>.45&&!r.trackingBall){r.plantPose=Math.max(r.plantPose,Math.min(1,angle/1.8));r.visualCutSide=sign;}
  const familiarity=F.chemistry(r.profile,plays[selectedPlay].name),release=r.style==='Deep Threat'&&r.distance<8?1.035:1;
  const contact=receiverContactFactors(r),cutPenalty=THREE.MathUtils.clamp(angle/(Math.PI*.7),0,1),cutRating=P.effective((r.profile?.cutting||50))/100,slow=(r.shoveSlow>0?.68:1)*(r.stagger>0?.78:1),burst=r.trackingBall?P.traits(r.profile).pursuitBurst*(1+familiarity):1,plantSlow=r.comebackPlant>0?.27:1,speedCutLoss=THREE.MathUtils.lerp(.48,.19,cutRating),accelCutLoss=THREE.MathUtils.lerp(.40,.16,cutRating),desiredSpeed=(stridePlan?Math.min(stridePlan.strideSpeed,r.maxSpeed*burst):r.maxSpeed*burst)*(1-speedCutLoss*cutPenalty)*slow*plantSlow*contact.speed*settle,desiredVel=r.heading.clone().multiplyScalar(desiredSpeed),accel=(r.trackingBall?34:11+16*cutRating)*release*(1+familiarity)*(1-accelCutLoss*cutPenalty)*(r.stagger>0?.62:1)*contact.accel,maxDv=accel*dt,dv=desiredVel.sub(r.velocity);if(dv.length()>maxDv)dv.setLength(maxDv);r.velocity.add(dv);if(r.comebackPlant>0)r.velocity.multiplyScalar(Math.pow(.16,dt));r.mesh.position.addScaledVector(r.velocity,dt);r.mesh.position.addScaledVector(r.impactVel,dt);r.impactVel.multiplyScalar(Math.pow(.055,dt));
  const face=Math.atan2(r.heading.x,r.heading.z),delta=Math.atan2(Math.sin(face-r.mesh.rotation.y),Math.cos(face-r.mesh.rotation.y));r.mesh.rotation.y+=THREE.MathUtils.clamp(delta,-11.5*dt,11.5*dt);
  let shouldJump=false;if(ballLive){const catchPoint=r.mesh.position.clone().add(new THREE.Vector3(0,1.92*r.mesh.scale.y,0)),approach=ballApproach(catchPoint,.9);if(r.trackingBall&&approach&&approach.time<.58&&approach.dist<1.52)r.catchPose=1;shouldJump=wantsHighPoint(r)}updateJump(r,dt,shouldJump);
  if(ballLive&&!stridePlan&&!r.catchDive&&r.trackingBall&&r.jumpY<.03){
    const point=ball.position.clone().addScaledVector(ballVel,.20);point.y-=4.905*.04;
    const to=point.clone().sub(r.mesh.position),gap=Math.hypot(to.x,to.z);
    if(point.y>.20&&point.y<.85&&gap>.8&&gap<P.traits(r.profile).diveReach&&to.setY(0).normalize().dot(r.heading)>.25){
      r.catchDive={time:0,direction:to.clone()};r.velocity.addScaledVector(to,1.4);r.jumpCooldown=.7;
    }
  }
  if(r.catchDive&&r.catchDive.time>.55){r.catchDive=null;r.landingPose=1;}
  animatePlayerContact(r,dt);const ring=r.mesh.userData.trackRing;if(ring){ring.visible=r.trackingBall;ring.rotation.z+=dt*2.8}
  if(!r.history.length||now-r.history[r.history.length-1].t>=30){
    const sample=r.history.length>=32?r.history.shift():{p:new THREE.Vector3(),v:new THREE.Vector3()};
    sample.t=now;sample.p.copy(r.mesh.position);sample.v.copy(r.velocity);r.history.push(sample);
  }
}

function observedReceiver(r,delayMs,now){let s=r.history[0]||{p:r.mesh.position,v:r.velocity};for(let i=r.history.length-1;i>=0;i--){if(r.history[i].t<=now-delayMs){s=r.history[i];break}}return s}
function getDeepThreat(side,delay,now){let best=null;for(const r of receivers){const o=observedReceiver(r,delay,now);if(side===0||Math.sign(o.p.x||side)===side||Math.abs(o.p.x)<5){if(!best||o.p.z<best.p.z)best=o}}return best}

// DB logic only uses delayed observed position/velocity and scheme leverage. It never reads route names or route paths.
function defenderTarget(d,now){
  if(d.fakeUntil>gameTime&&d.fakeTarget)return d.fakeTarget.clone();
  const repeatedScreens=V.cleanHistory(franchise.scouting).filter(p=>['Bubble','Bubble Left','Tunnel','Slip'].includes(p.route)).length;
  const delay=d.reaction*1000*(1-Math.min(.3,repeatedScreens*.045)),skill=currentSkill();
  // React to a delayed shallow release and clustered lead runners, never to a called route.
  if(playState==='live'&&throwClock<7.5&&repeatedScreens>1){
    const shallow=receivers.map(r=>observedReceiver(r,delay,now)).filter(o=>o.p.z>worldZForYards(snapSpotYards)-3&&Math.abs(o.v.x)>1.5);
    const seen=shallow.find(o=>o.p.distanceTo(d.mesh.position)<10);
    if(seen&&d.kind==='corner'&&throwClock<7.4+Math.min(.25,repeatedScreens*.04))return seen.p.clone().addScaledVector(seen.v,.18);
  }
  if(playState==='run'&&ballCarrier){
    const distance=d.mesh.position.distanceTo(ballCarrier.mesh.position);
    const o=observedReceiver(ballCarrier,delay,now);
    // At arm's length, use local contact awareness instead of chasing a stale point behind the runner.
    const position=distance<4?ballCarrier.mesh.position.clone():o.p.clone().addScaledVector(o.v,Math.min(d.reaction,.22));
    const velocity=distance<4?ballCarrier.velocity:o.v,dx=position.x-d.mesh.position.x,dz=position.z-d.mesh.position.z;
    const intercept=V.pursuitTime(dx,dz,velocity.x,velocity.z,d.maxSpeed);
    const lead=distance<4?.10+d.technique*.12:Math.max(.18,intercept*(.8+d.technique*.2));
    const target=position.addScaledVector(velocity,lead);
    // First defender closes; support protects the upfield shoulder instead of joining the same chase line.
    if(distance>4&&d.pursuitRole>0){
      const side=d.zoneX>=ballCarrier.mesh.position.x?1:-1;
      target.x+=side*(d.pursuitRole===1?.8:1.8);
      target.z-=d.pursuitRole===1?.7:1.5;
    }
    target.x=THREE.MathUtils.clamp(target.x,-25.4,25.4);target.z=Math.max(GOAL_LINE_Z-.5,target.z);return target;
  }
  const seesBall=ballLive&&defenderSeesBall(d),los=worldZForYards(snapSpotYards);
  // Recognize a receiver waiting behind the line from delayed observations, not play metadata.
  if(d.kind==='corner'&&now-snapTime>1700-skill*650){
    const near=receivers.map(r=>observedReceiver(r,delay,now)).find(o=>o.p.z>los-.5&&Math.abs(o.p.x-d.zoneX)<8&&o.v.length()<3);
    if(near)return near.p.clone().addScaledVector(near.v,.12);
  }
  if(d.kind==='corner'&&(currentDefense.corner==='zone'||currentDefense.corner==='deepzone')){
    const depth=currentDefense.corner==='deepzone'?20:8;
    let target=new THREE.Vector3(d.zoneX,0,los-depth-snapMemory.depth),closest=null,gap=Infinity;
    for(const r of receivers){const o=observedReceiver(r,delay,now),distance=o.p.distanceTo(target);if(Math.abs(o.p.x-d.zoneX)<9&&distance<gap){closest=o;gap=distance;}}
    if(closest&&gap<18){target.lerp(closest.p,currentDefense.corner==='zone'?.67:.5);target.addScaledVector(closest.v,.15+skill*.15);}
    if(seesBall){const approach=ballApproach(target,1.0);if(approach&&approach.dist<12)target.lerp(approach.point,.65);}
    target.z=Math.max(ENDZONE_BACK_Z+1,target.z);return target;
  }
  if(d.kind==='corner'){
    const r=receivers[d.target],o=observedReceiver(r,delay,now);let target=o.p.clone(),v=o.v.clone();v.y=0;
    const speed=v.length();if(speed>.2)v.multiplyScalar(1/speed);
    if(currentDefense.corner==='press'){target.addScaledVector(v,.3+skill*.35);target.z-=.8}
    if(currentDefense.corner==='under'){target.z+=1.5;target.x*=.95}
    if(currentDefense.corner==='off')target.z-=1.8;
    if(!ballLive)target.addScaledVector(v,(snapMemory.anticipation[d.target]||0)+(['Gamblers','Ball Hawks'].includes(V.identity(activeRound()).name)?.7:0));
    if(currentDefense.corner==='trail'){target.z+=.7}
    if(currentDefense.corner==='match'){target.addScaledVector(v,.35+skill*.85);target.z+=o.p.z<-4?.45:.1;target.x*=o.p.z<-5?.96:1}
    if(currentDefense.corner==='allout'){target.addScaledVector(v,.8+skill*1.05);target.z-=.3}
    // Better levels and route-jumping schemes try to occupy the receiver's path using only observed movement.
    if(!ballLive&&(currentDefense.corner==='match'||currentDefense.corner==='allout'||currentDefense.id==='robber'))target.addScaledVector(v,THREE.MathUtils.lerp(.15,1.45,skill));
    if(!ballLive&&d.smartDeep&&o.v.z<-1){
      target.z=Math.min(target.z,o.p.z-d.depth);
      target.x=o.p.x+o.v.x*Math.min(.45,d.reaction+.15);
      target.z=Math.max(ENDZONE_BACK_Z+1,target.z);
    }
    if(seesBall){const land=getBallLanding(),ballTarget=land?land.point:ball.position,toBall=ballTarget.clone().sub(d.mesh.position),range=THREE.MathUtils.lerp(8.5,18.0,skill);if(toBall.length()<range||throwTime>THREE.MathUtils.lerp(.65,.20,skill))target.lerp(ballTarget,THREE.MathUtils.lerp(.28,.78,skill))}
    return target;
  }
  if(d.kind==='safety'){
    const side=d.zoneX<0?-1:1;
    const best=receivers.reduce((a,r,i)=>playerOverall(r.profile)>playerOverall(receivers[a].profile)?i:a,0);
    const help=snapMemory.attention>.2?snapMemory.help:best;
    const bracket=(currentDefense.safety==='bracket'||snapMemory.attention>.4)&&Math.sign(receivers[help].start.x)===side;
    const th=bracket?observedReceiver(receivers[help],delay,now):getDeepThreat(side,delay,now);if(!th)return d.mesh.position.clone();let target=th.p.clone();
    if(currentDefense.safety==='patient'&&!seesBall){target.x=THREE.MathUtils.clamp(target.x,-20,20);target.z-=7.2}
    else if(currentDefense.safety==='aggressive'&&!seesBall){target.x*=.45;target.z-=4.5}
    else if(seesBall){const land=getBallLanding();target.lerp(land?land.point:ball.position,THREE.MathUtils.lerp(.45,.84,skill))}else{target.x*=.72;target.z-=6}
    if(!seesBall){
      target.z-=snapMemory.depth+V.identity(activeRound()).depth;
      if(bracket){target.x=th.p.x;target.z=th.p.z-4-snapMemory.depth;}
      if(currentDefense.safety==='rotate'){
        if(side<0){target.x*=.4;target.z=Math.min(los-16,th.p.z-9);}
        else {target.x=THREE.MathUtils.clamp(th.p.x,-12,12);target.z=los-10;}
      }
    }
    target.z=Math.max(ENDZONE_BACK_Z+1,target.z);return target;
  }
  if(d.kind==='robber'){
    if(seesBall){const land=getBallLanding();return land?land.point.clone():ball.position.clone();}
    // The robber reads QB gaze with delay, plus actual inside receiver movement—not the called play.
    const look=new THREE.Vector3();camera.getWorldDirection(look);const gazeX=THREE.MathUtils.clamp(camera.position.x+look.x*28,-9,9);let inside=0,count=0;
    for(const r of receivers){const o=observedReceiver(r,delay,now);if(Math.abs(o.p.x)<12){inside+=o.p.x;count++}}
    return new THREE.Vector3(THREE.MathUtils.lerp(gazeX,count?inside/count:0,.35),0,los-12-snapMemory.depth);
  }
  return d.mesh.position.clone();
}
function updateDefender(d,target,dt){
  d.tackleCooldown=Math.max(0,(d.tackleCooldown||0)-dt);
  d.catchPose=Math.max(0,(d.catchPose||0)-dt*5);
  if(d.diveTime>0){
    // A launched tackle has committed momentum. It cannot home in on a receiver's next cut.
    d.diveTime=Math.max(0,d.diveTime-dt);d.mesh.position.addScaledVector(d.velocity,dt);
    const progress=1-d.diveTime/.28;d.mesh.position.y=Math.sin(progress*Math.PI)*.28;
    d.mesh.rotation.x=0;d.finishPose={pitch:Math.sin(Math.min(1,progress*2)*Math.PI/2)*1.12,roll:0,yaw:0,drop:progress*.25,fold:.6,wrapTarget:ballCarrier};
    d.mesh.rotation.y=Math.atan2(d.heading.x,d.heading.z);d.catchPose=1;animatePlayerContact(d,dt);
    if(d.diveTime===0){d.diveRecovery=.58-.2*d.technique;d.velocity.multiplyScalar(.25);}
    return;
  }
  if(d.diveRecovery>0){
    d.diveRecovery=Math.max(0,d.diveRecovery-dt);d.mesh.rotation.x=0;const settle=Math.min(1,d.diveRecovery/.3);d.finishPose={pitch:1.25*settle,roll:.18*settle,drop:.5*settle,fold:.6*settle};
    d.mesh.position.y=0;d.velocity.multiplyScalar(Math.pow(.04,dt));d.mesh.position.addScaledVector(d.velocity,dt);
    animatePlayerContact(d,dt);return;
  }
  d.finishPose=null;d.mesh.rotation.x=0;
  d.shoveCooldown=Math.max(0,d.shoveCooldown-dt);d.shoveSlow=Math.max(0,d.shoveSlow-dt);d.stagger=Math.max(0,d.stagger-dt);d.swatPose=Math.max(0,(d.swatPose||0)-dt*5.2);d.ballSeen=defenderSeesBall(d);const pos=d.mesh.position,desired=target.clone().sub(pos);desired.y=0;if(desired.lengthSq()<.002){d.velocity.multiplyScalar(Math.pow(.1,dt));updateJump(d,dt,false);animatePlayerContact(d,dt);return}desired.normalize();
  const current=d.heading.clone().normalize();let angle=Math.acos(THREE.MathUtils.clamp(current.dot(desired),-1,1));
  // Planting: a DB pointed the wrong way cannot instantly rotate 90–180 degrees. Big changes reduce usable speed and acceleration.
  const cross=current.x*desired.z-current.z*desired.x,sign=cross<0?-1:1,maxTurn=d.turnRate*V.weather(matchWeather().name).cut*dt*(d.fakeUntil>gameTime?.65:1),turn=Math.min(angle,maxTurn)*sign;const c=Math.cos(turn),ss=Math.sin(turn);d.heading.set(current.x*c-current.z*ss,0,current.x*ss+current.z*c).normalize();
  const plantPenalty=THREE.MathUtils.clamp(angle/(Math.PI*.75),0,1);d.plantPose=plantPenalty>.48?THREE.MathUtils.clamp((plantPenalty-.48)*1.6,0,.85):Math.max(0,(d.plantPose||0)-dt*4.5);const slow=(d.shoveSlow>0?.67:1)*(d.stagger>0?.76:1),pursuitBoost=d.blockTime>0?d.blockSlow:1,targetSpeed=d.maxSpeed*pursuitBoost*(1-.50*plantPenalty)*slow,desiredVel=d.heading.clone().multiplyScalar(targetSpeed),maxDv=d.accel*(1-.45*plantPenalty)*(d.stagger>0?.62:1)*dt,delta=desiredVel.sub(d.velocity);if(delta.length()>maxDv)delta.setLength(maxDv);d.velocity.add(delta);pos.addScaledVector(d.velocity,dt);pos.addScaledVector(d.impactVel,dt);d.impactVel.multiplyScalar(Math.pow(.055,dt));d.mesh.rotation.y=Math.atan2(d.heading.x,d.heading.z);
  d.runIntensity=THREE.MathUtils.lerp(d.runIntensity||0,playState==='run'?1:.78,Math.min(1,dt*11));d.runPhase=(d.runPhase||0)+dt*Math.max(playState==='run'?10:6,d.velocity.length()*(playState==='run'?3.0:2.2));let shouldJump=false;if(ballLive){const handTarget=pos.clone().add(new THREE.Vector3(0,2.00*d.mesh.scale.y,0)),approach=ballApproach(handTarget,.72);if(d.ballSeen&&approach&&approach.time<.42&&approach.dist<THREE.MathUtils.lerp(1.02,1.38,currentSkill()))d.swatPose=1;shouldJump=wantsHighPoint(d)}updateJump(d,dt,shouldJump);animatePlayerContact(d,dt);
}


// Physical collisions + hand-fighting. STR controls who keeps momentum, who gets displaced, and how effective a shove is.
function shovePlayer(shover,target,nx,nz){
  const shoveStr=actorStrength(shover),targetStr=actorStrength(target),edge=THREE.MathUtils.clamp(shoveStr-targetStr,-.75,.75),edge01=(edge+.75)/1.5;
  const force=(2.35+shoveStr*1.95+Math.random()*.65)*THREE.MathUtils.lerp(.72,1.22,edge01),selfRetention=THREE.MathUtils.lerp(.69,.88,edge01),targetRetention=THREE.MathUtils.lerp(.82,.61,edge01);
  shover.shoveCooldown=.75+Math.random()*.5;target.shoveCooldown=.55;shover.shoveSlow=THREE.MathUtils.lerp(.34,.18,edge01);target.stagger=.16+shoveStr*.15+Math.max(0,edge)*.18+Math.random()*.08;shover.shoveAnim=.34;target.shoveAnim=Math.max(target.shoveAnim||0,.12);
  shover.velocity.multiplyScalar(selfRetention);target.velocity.multiplyScalar(targetRetention);shover.impactVel.add(new THREE.Vector3(-nx,0,-nz).multiplyScalar(THREE.MathUtils.lerp(.78,.38,edge01)));target.impactVel.add(new THREE.Vector3(nx,0,nz).multiplyScalar(force));
}
function solvePlayerCollisions(dt){
  const all=[...receivers,...defenders];for(let a=0;a<all.length;a++)for(let b=a+1;b<all.length;b++){
    const A=all[a],B=all[b];if((A.hurdleTime>0&&A.hurdleTarget===B&&A.jumpY>.48&&(B.diveTime>0||B.divingThisStep||B.diveRecovery>0))||(B.hurdleTime>0&&B.hurdleTarget===A&&B.jumpY>.48&&(A.diveTime>0||A.divingThisStep||A.diveRecovery>0)))continue;const dx=B.mesh.position.x-A.mesh.position.x,dz=B.mesh.position.z-A.mesh.position.z,dist=Math.hypot(dx,dz),min=.40*(A.mesh.scale.x+B.mesh.scale.x);if(dist>0&&dist<min){const overlap=min-dist,nx=dx/dist,nz=dz/dist,crossTeam=!!A.profile!==!!B.profile,sA=crossTeam?actorStrength(A):.5,sB=crossTeam?actorStrength(B):.5,total=Math.max(.01,sA+sB),moveA=overlap*(sB/total),moveB=overlap*(sA/total);A.mesh.position.x-=nx*moveA;A.mesh.position.z-=nz*moveA;B.mesh.position.x+=nx*moveB;B.mesh.position.z+=nz*moveB;if(A.velocity)A.velocity.multiplyScalar(THREE.MathUtils.lerp(.98,.89,moveA/Math.max(.001,overlap)));if(B.velocity)B.velocity.multiplyScalar(THREE.MathUtils.lerp(.98,.89,moveB/Math.max(.001,overlap)))}
  }
  for(const r of receivers)for(const d of defenders){if(d.kind!=='corner')continue;const dx=d.mesh.position.x-r.mesh.position.x,dz=d.mesh.position.z-r.mesh.position.z,dist=Math.hypot(dx,dz);if(dist<1.10&&dist>.05&&r.jumpY<.08&&d.jumpY<.08&&r.shoveCooldown<=0&&d.shoveCooldown<=0){const press=currentDefense.corner==='press'||currentDefense.corner==='allout',rate=r.trackingBall?2.8:(press?1.8:1.0);if(Math.random()<dt*rate){const nx=dx/dist,nz=dz/dist,strengthEdge=actorStrength(r)-actorStrength(d),baseInitiative=r.trackingBall?.68:.50,receiverInitiates=Math.random()<THREE.MathUtils.clamp(baseInitiative+strengthEdge*.32,.20,.86);if(receiverInitiates)shovePlayer(r,d,nx,nz);else shovePlayer(d,r,-nx,-nz)}}}
}

function centerThrowDirection(){
  // Use the camera's true forward vector so the football follows the exact center reticle.
  // This avoids any projection/unprojection mismatch between the HUD crosshair and the 3D throw ray.
  camera.updateMatrixWorld(true);
  const dir=new THREE.Vector3();
  camera.getWorldDirection(dir);
  return dir.normalize();
}
function throwStats(held){
  const power=THREE.MathUtils.clamp(.12+held/1.55,0,1);let quality;if(held<.22)quality=THREE.MathUtils.lerp(.35,.72,held/.22);else if(held<=1.22)quality=THREE.MathUtils.lerp(.82,1,1-Math.abs(held-.68)/.54);else quality=THREE.MathUtils.lerp(.80,.28,THREE.MathUtils.clamp((held-1.22)/.58,0,1));return{power,quality:THREE.MathUtils.clamp(quality,.25,1)};
}
function currentHeld(){return Math.min(1.8,(gameTime-chargeStart)/1000)}
function startCharge(){if(playState!=='live'||ballLive||charging)return;charging=true;chargeStart=gameTime}
function stopCharge(){if(!charging)return;charging=false;if(playState==='live'&&!ballLive)throwBall();else{arcLine.visible=false;landRing.visible=false}}
function updateCharge(){const pct=Math.round(chargePower*100);$('charge').style.width=`${pct}%`;const shape=loftBias>.28?'LOFT':loftBias<-.28?'BULLET':'BALANCED';if(!charging){$('throwQuality').textContent=`${shape} · PERFECT SPIRAL WINDOW`;$('throwQuality').style.color='var(--good)';return}const q=throwStats(currentHeld()).quality;$('throwQuality').textContent=`${shape} · ${q>.86?'TIGHT SPIRAL':q>.62?'STABLE BALL':'DUCK / WOBBLE RISK'}`;$('throwQuality').style.color=q>.86?'#62ff9c':q>.62?'#ffe174':'#ff756f'}
function idealLaunchVelocity(power){
  const aim=centerThrowDirection();
  // Preserve the exact horizontal aim angle. W/S only changes vertical launch angle and speed.
  const flat=new THREE.Vector3(aim.x,0,aim.z);
  if(flat.lengthSq()<1e-8)flat.set(0,0,-1);else flat.normalize();
  const aimedPitch=Math.asin(THREE.MathUtils.clamp(aim.y,-.999,.999));
  const pitchAdjust=Math.max(0,loftBias)*.28-Math.max(0,-loftBias)*.12;
  const launchPitch=THREE.MathUtils.clamp(aimedPitch+pitchAdjust,-.42,.72);
  const dir=flat.multiplyScalar(Math.cos(launchPitch));
  dir.y=Math.sin(launchPitch);
  const speed=(18.5+power*26.5)*(1+Math.max(0,-loftBias)*.20-Math.max(0,loftBias)*.07);
  return dir.normalize().multiplyScalar(speed);
}
function drawTrajectory(origin,v){
  let landing=null,count=0;
  for(let t=0;t<=4.6&&count<64;t+=.075){
    const x=origin.x+v.x*t,y=origin.y+v.y*t-4.905*t*t,z=origin.z+v.z*t;
    arcPositions[count*3]=x;arcPositions[count*3+1]=y;arcPositions[count*3+2]=z;count++;
    if(y<=.08&&t>.08){landing=new THREE.Vector3(x,.02,z);break;}if(z<-78||Math.abs(x)>38)break;
  }
  arcGeo.setDrawRange(0,count);arcGeo.attributes.position.needsUpdate=true;arcLine.frustumCulled=false;arcLine.visible=count>1;
  if(landing){landRing.position.copy(landing);landRing.visible=true;}else landRing.visible=false;return landing;
}

function updateArcPreview(){
  if(!charging||playState!=='live'||ballLive){if(!ballLive){arcLine.visible=false;landRing.visible=false}return}const stats=throwStats(currentHeld()),v=idealLaunchVelocity(stats.power),origin=camera.position.clone().add(centerThrowDirection().multiplyScalar(1.05));drawTrajectory(origin,v);
}
function throwBall(){
  playLog=playLog||F.newPlay(plays[selectedPlay].name);playLog.attempt=true;
  if(!replayRecording){replayFrames=[];replayPool=[];replayInterval=1/30;replaySample=0;}replayRecording=true;throwDown=down;replayEligible=replayEligible||down===4;captureReplay(true);
  const held=currentHeld(),stats=throwStats(held);
  chargePower=stats.power;spiralQuality=stats.quality;duckPhase=Math.random()*Math.PI*2;
  const ideal=centerThrowDirection(),baseVel=idealLaunchVelocity(chargePower);
  // The real throw now uses the same center-line velocity as the green preview.
  // Release quality can affect vertical wobble in flight, but never adds hidden left/right aim error.
  const origin=camera.position.clone().add(ideal.clone().multiplyScalar(1.05));
  pendingCatch=null;ballTumble=0;ballSpin=0;receiverDrop=false;ball.userData.spin.rotation.set(0,0,0);for(const a of [...receivers,...defenders]){a.contactLock=false;a.contactRig=null;}
  attemptPending=true;throwBobbles=0;
  releaseTiming=receivers.map(r=>{let dist=0;for(let i=1;i<r.path.length-1;i++){dist+=r.path[i].distanceTo(r.path[i-1]);if(Math.abs(dist-r.distance)<r.maxSpeed*.45)return true;}return false;});
  ball.position.copy(origin);ballPrev.copy(origin);ballVel.copy(baseVel);ball.visible=true;ballLive=true;throwTime=0;playState='thrown';
  const landing=drawTrajectory(origin,ballVel);if(landing){predictedLanding.copy(landing)}
  const shape=loftBias>.28?'LOFT':loftBias<-.28?'BULLET':'BALANCED';
  showMessage(spiralQuality>.86?'BALL OUT — TIGHT SPIRAL':'BALL OUT — WOBBLE',`${shape} · Power ${Math.round(chargePower*100)}% · Release ${Math.round(spiralQuality*100)}%`,600);
}
function pointSegmentDistance(point,a,b){const ab=b.clone().sub(a),den=ab.lengthSq();if(den<1e-8)return point.distanceTo(a);const t=THREE.MathUtils.clamp(point.clone().sub(a).dot(ab)/den,0,1);return point.distanceTo(a.clone().addScaledVector(ab,t))}
function nearestOpponentInfo(player,isDefense){let best=null,bestDist=99;const pool=isDefense?receivers:defenders;for(const o of pool){const dist=Math.hypot(o.mesh.position.x-player.mesh.position.x,o.mesh.position.z-player.mesh.position.z);if(dist<bestDist){best=o;bestDist=dist}}return{player:best,dist:bestDist}}
function nearestOpponentDistance(player,isDefense){return nearestOpponentInfo(player,isDefense).dist}
function receiverNormalCatchDistance(r){
  // Kept for diagnostics: this measures the visible hands, never chest/head proximity.
  r.mesh.updateMatrixWorld(true);
  return Math.min(...r.mesh.userData.hands.map(h=>pointSegmentDistance(h.getWorldPosition(new THREE.Vector3()),ballPrev,ball.position)));
}
function resolveDivingCatch(r){
  const catchZ=ball.position.z;recordAttempt(r);logCatch(r,catchZ);markCatch(r);pendingCatch=null;
  ballLive=false;arcLine.visible=false;landRing.visible=false;predictedFlightTime=0;charging=false;playState='divecatch';catches++;r.hasBall=true;
  // Preserve the pose and position at the real contact, including the extended hands.
  flashResult('DIVING CATCH!',true,900);showMessage('DIVING CATCH!',`${r.label} secured the ball in his hands while laying out.`,1050);updateScore();
  transition={at:gameTime+700,fn:()=>finishPlayAtSpot(catchZ,'DIVING CATCH')};
}
function catchPlacement(r){
  const contact=nearestOpponentInfo(r,false),d=contact.player;
  return V.placement({dx:ball.position.x-r.mesh.position.x,dz:ball.position.z-r.mesh.position.z,
    height:(ball.position.y-r.mesh.position.y)/r.mesh.scale.y,headingX:r.heading.x,headingZ:r.heading.z,
    speed:r.velocity.length(),contest:contact.dist,defenderX:d?d.mesh.position.x-r.mesh.position.x:0,
    defenderZ:d?d.mesh.position.z-r.mesh.position.z:0,jump:r.jumpY,
    sideline:Math.abs(r.mesh.position.x)>24,timing:releaseTiming[receivers.indexOf(r)],bobbled:throwBobbles>0});
}
// Contact shapes come directly from the rendered rig, including rotation and size.
function footballAxis(){
  ball.updateMatrixWorld(true);
  return new THREE.Vector3(0,0,1).applyQuaternion(ball.userData.spin.getWorldQuaternion(new THREE.Quaternion()));
}
function sampleContactRig(a,remember=false){
  const parts=[...a.mesh.userData.hands,...a.mesh.userData.arms,...a.mesh.userData.forearms];
  if(!a.contactRig)a.contactRig=parts.map((mesh,i)=>({mesh,hand:i<2,a:new THREE.Vector3(),b:new THREE.Vector3(),prevA:new THREE.Vector3(),prevB:new THREE.Vector3(),radius:0}));
  a.mesh.updateMatrixWorld(true);
  for(const c of a.contactRig){
    const half=Math.max(0,c.mesh.userData.boneLength/2-c.mesh.userData.contactRadius);
    c.a.set(0,-half,0).applyMatrix4(c.mesh.matrixWorld);c.b.set(0,half,0).applyMatrix4(c.mesh.matrixWorld);
    const scale=c.mesh.getWorldScale(new THREE.Vector3());
    c.radius=c.mesh.userData.contactRadius*Math.max(scale.x,scale.y,scale.z);
    if(remember||!c.ready){c.prevA.copy(c.a);c.prevB.copy(c.b);c.ready=true;}
  }
  return a.contactRig;
}
// Conservative advancement sweeps a moving football lobe against a moving capsule.
// The relative-speed bound prevents bullets or fast reaching hands tunneling between frames.
function sweepLimb(from,to,limb,radius){
  const travel=to.clone().sub(from),da=limb.a.clone().sub(limb.prevA),db=limb.b.clone().sub(limb.prevB);
  const bound=travel.length()+Math.max(da.length(),db.length()),combined=radius+limb.radius;
  const p=new THREE.Vector3(),a=new THREE.Vector3(),b=new THREE.Vector3(),ab=new THREE.Vector3(),nearest=new THREE.Vector3();
  let t=0;
  for(let i=0;i<48&&t<=1;i++){
    p.copy(from).addScaledVector(travel,t);a.copy(limb.prevA).addScaledVector(da,t);b.copy(limb.prevB).addScaledVector(db,t);
    ab.copy(b).sub(a);const fraction=THREE.MathUtils.clamp(p.clone().sub(a).dot(ab)/Math.max(1e-9,ab.lengthSq()),0,1);
    nearest.copy(a).addScaledVector(ab,fraction);const distance=p.distanceTo(nearest),gap=distance-combined;
    if(gap<=.0001){
      const normal=p.clone().sub(nearest);
      if(normal.lengthSq()<1e-8)normal.copy(travel).negate();if(normal.lengthSq()<1e-8)normal.set(0,1,0);
      return {time:t,normal:normal.normalize(),point:nearest.clone(),velocity:da.clone().lerp(db,fraction).divideScalar(Math.max(.001,contactStep))};
    }
    if(bound<1e-8)return null;
    t+=Math.max(.00001,gap/bound*.95);
  }
  return null;
}
function playerBallContacts(a){
  // Broad phase only rejects; it can never award a catch or a tip.
  if(pointSegmentDistance(a.mesh.position.clone().add(new THREE.Vector3(0,1.3,0)),ballPrev,ball.position)>3.8){a.contactLock=false;return [];}
  const rig=sampleContactRig(a),axis=footballAxis(),hits=[];
  // Three small overlapping lobes approximate the visible .19 x .3325 football.
  for(const limb of rig){
    let earliest=null;
    for(const [offset,radius] of [[0,.19],[-.17,.15],[.17,.15]]){
      const from=ballPrev.clone().addScaledVector(previousBallAxis,offset),to=ball.position.clone().addScaledVector(axis,offset);
      const hit=sweepLimb(from,to,limb,radius);
      if(hit&&(!earliest||hit.time<earliest.time))earliest={...hit,actor:a,limb};
    }
    if(earliest)hits.push(earliest);
  }
  // A touched actor cannot repeatedly kick an overlapping ball; re-arm on separation.
  if(a.contactLock){if(!hits.length)a.contactLock=false;return [];}
  return hits;
}
function deflectBall(hit,contested=false){
  const relative=ballVel.clone().sub(hit.velocity),normalSpeed=relative.dot(hit.normal);
  const restitution=hit.limb.hand?.30:.46;
  if(normalSpeed<0)relative.addScaledVector(hit.normal,-(1+restitution)*normalSpeed);
  relative.multiplyScalar(hit.limb.hand?.64:.82);
  ballVel.copy(relative).add(hit.velocity.clone().clampLength(0,12));
  ballVel.clampLength(.6,48);ball.position.addScaledVector(hit.normal,.006);
  ballTumble=Math.min(22,4+Math.abs(normalSpeed)*.32);spiralQuality=Math.min(spiralQuality,.35);
  hit.actor.contactLock=true;pendingCatch=null;throwBobbles++;receiverDrop=!!hit.actor.profile;
  if(playLog&&hit.actor.profile&&hit.limb.hand&&!contested)playLog.dropped=hit.actor.profile.id;
  ballPrev.copy(ball.position);flashResult(contested?'CONTESTED TIP':hit.actor.profile?'BOBBLE!':'PASS TIPPED',false,450);
}
function checkBallContact(){
  if(!ballLive)return false;
  const contacts=[];
  for(const actor of [...receivers,...defenders]){
    if(actor===pendingCatch?.actor||Math.abs(actor.mesh.position.x)>25.6)continue;
    contacts.push(...playerBallContacts(actor));
  }
  contacts.sort((a,b)=>a.time-b.time);
  const hit=contacts[0];if(!hit)return false;
  const actor=hit.actor,isDefense=!actor.profile;
  ball.position.lerpVectors(ballPrev,ball.position,hit.time);
  // Competing limbs must actually touch within this small time window.
  const opponent=contacts.find(c=>!!c.actor.profile!==!!actor.profile&&(c.time-hit.time)*contactStep<.006);
  const aware=isDefense?actor.ballSeen:actor.trackingBall;
  const rating=isDefense?.52+currentSkill()*.23+(actor.hands||0):Math.min(1,P.effective(actor.profile.catching)/100);
  const athletic=isDefense?currentSkill():Math.min(1,P.effective(actor.profile.athleticism)/100);
  const speed=ballVel.clone().sub(hit.velocity).length(),twoHands=contacts.some(c=>c.actor===actor&&c.limb.hand&&c.limb!==hit.limb&&(c.time-hit.time)*contactStep<.006);
  const signature=actor.signature==='Sure Hands'?.025:0;
  const control=THREE.MathUtils.clamp(.64-V.weather(matchWeather().name).hands+signature+rating*.30+athletic*.07+(twoHands?.09:0)+spiralQuality*.04-Math.max(0,speed-24)*.006-(actor.stagger>0?.14:0)-(actor.comebackActive?(actor.underthrowDifficulty||0)*.3:0),.18,.99);
  if(hit.limb.hand&&aware&&!opponent&&!pendingCatch&&Math.random()<control){
    if(!isDefense)actor.placement=catchPlacement(actor);
    actor.mesh.updateMatrixWorld(true);
    pendingCatch={actor,isDefense,limb:hit.limb,offset:hit.limb.mesh.worldToLocal(ball.position.clone()),remaining:twoHands?.045:.075};
    ballVel.copy(actor.velocity);actor.catchPose=1;
  }else{
    const holder=pendingCatch?.actor;
    deflectBall(hit,!!opponent||!!holder);
    if(holder){holder.contactLock=true;holder.placement=null;}
    if(opponent){opponent.actor.contactLock=true;opponent.actor.placement=null;}
  }
  return true;
}
function finishSecuringCatch(dt){
  if(!pendingCatch)return;
  pendingCatch.remaining-=dt;
  if(pendingCatch.remaining>0)return;
  const {actor,isDefense}=pendingCatch;pendingCatch=null;
  if(actor.catchDive&&!isDefense)resolveDivingCatch(actor);else resolveCatch(actor,isDefense);
}


function attachBallToCarrier(){if(!ballCarrier)return;ballCarrier.mesh.updateMatrixWorld(true);const hands=ballCarrier.mesh.userData.hands;ball.position.copy(hands[0].getWorldPosition(new THREE.Vector3())).lerp(hands[1].getWorldPosition(new THREE.Vector3()),.5+.5*(ballCarrier.finishPose?.brace||0));if(ballCarrier.stiffArmTime>0)ball.position.copy(hands[1].getWorldPosition(new THREE.Vector3()));ball.visible=true;ball.rotation.set(0,ballCarrier.mesh.rotation.y,Math.PI*.18);}
function clearGoalLane(r){
  const distance=r.mesh.position.z-GOAL_LINE_Z;
  if(distance<0||distance>5*WORLD_PER_YARD||Math.abs(r.mesh.position.x)>25.4)return false;
  const horizon=Math.min(.8,distance/Math.max(4,r.maxSpeed));
  return !defenders.some(d=>{
    if(d.diveRecovery>0)return false;
    const ahead=r.mesh.position.z-d.mesh.position.z;
    if(ahead<-.25||d.mesh.position.z<GOAL_LINE_Z-1)return false;
    const dx=d.mesh.position.x-r.mesh.position.x,next=dx+d.velocity.x*horizon;
    return Math.min(Math.abs(dx),Math.abs(next))<1.65||dx*next<0;
  });
}
function earlyScreenRun(r){return !!r?.screenTarget&&r.screenCatchAt!=null&&gameTime-r.screenCatchAt>=0&&gameTime-r.screenCatchAt<1800;}
function attemptCarrierMove(r,d,kind){
  if(playState!=='run'||ballLive||r!==ballCarrier||r.moveCooldown>0||r.jumpY>.08||r.stagger>.1||r.jukeAnim>0||r.stumbleTime>0)return false;
  const delta=d.mesh.position.clone().sub(r.mesh.position).setY(0),gap=delta.length();
  if(gap<.01||r.heading.dot(delta.clone().normalize())<-.1)return false;
  if(kind==='hurdle'&&(!(d.diveTime>0)||gap<1.8||gap>4.5||r.velocity.length()<4))return false;
  if(kind==='stiff'&&(gap>1.65||d.divingThisStep||d.diveTime>0))return false;
  // Evaluate each threat once per move, rather than rolling success every frame.
  const reads=kind==='hurdle'?(r.hurdleReads??=new Set()):(r.stiffReads??=new Set());
  if(reads.has(d))return false;reads.add(d);
  const odds=V.escapeOdds(r.profile,kind,d.strength,d.technique,earlyScreenRun(r));
  odds.timing=Math.min(1,odds.timing*(.97+F.awareness(r.profile)*.0003));
  if(Math.random()>odds.timing)return false;
  r.moveCooldown=1.35;r.jukeCooldown=Math.max(r.jukeCooldown||0,1.35);
  if(kind==='stiff'){r.stiffArmTime=.42;r.stiffTarget=d;}
  if(Math.random()>odds.success){if(kind==='stiff')flashResult('STIFF ARM — DEFENDER HELD',false,450);return false;}
  if(kind==='hurdle'){
    F.event(playLog,r.profile,'hurdles');r.hurdleTarget=d;r.hurdleTime=.9;r.jumpVel=4.6+Math.min(1.4,P.effective(r.profile.athleticism)/100)*1.1;r.jumpCooldown=1.4;r.takeoffPose=1;
    flashResult('HURDLE!',true,600);return true;
  }
  F.event(playLog,r.profile,'stiffArms');F.event(playLog,r.profile,'brokenTackles');
  d.contactDuration=.6+actorStrength(r)*.2;d.contactReaction=d.contactDuration;d.contactSide=Math.sign(delta.x)||1;d.contactPower=THREE.MathUtils.clamp(actorStrength(r)-actorStrength(d)+.55,0,1);
  const away=delta.normalize();d.mesh.position.addScaledVector(away,.85);d.velocity.addScaledVector(away,2.5);d.impactVel.addScaledVector(away,2);d.stagger=Math.max(d.stagger,.32);r.velocity.multiplyScalar(.94);
  flashResult('STIFF ARM!',true,600);return true;
}
function updateCarrierMoves(r,dt){
  r.moveCooldown=Math.max(0,(r.moveCooldown||0)-dt);r.stiffArmTime=Math.max(0,(r.stiffArmTime||0)-dt);r.hurdleTime=Math.max(0,(r.hurdleTime||0)-dt);
  const low=defenders.filter(d=>d.diveTime>0).sort((a,b)=>a.mesh.position.distanceToSquared(r.mesh.position)-b.mesh.position.distanceToSquared(r.mesh.position))[0];
  if(low)attemptCarrierMove(r,low,'hurdle');
}
function updateBallCarrier(r,dt){
  updateCarrierMoves(r,dt);
  r.stumbleTime=Math.max(0,(r.stumbleTime||0)-dt);
  updateTricks(r,dt,true);updateJump(r,dt,false);
  if(r.catchStyle==='TOE TAP'&&transition){r.velocity.set(0,0,0);animatePlayerContact(r,dt);return;}
  r.runIntensity=THREE.MathUtils.lerp(r.runIntensity||0,1,Math.min(1,dt*12));r.runPhase=(r.runPhase||0)+dt*Math.max(10,r.velocity.length()*2.65);r.catchPose=Math.max(0,(r.catchPose||0)-dt*3.5);r.plantPose=Math.max(0,(r.plantPose||0)-dt*5);
  const pos=r.mesh.position,openGoal=clearGoalLane(r),goal=new THREE.Vector3(openGoal?pos.x:0,0,GOAL_LINE_Z-3),desired=goal.clone().sub(pos);desired.y=0;if(desired.lengthSq()<.001)desired.set(0,0,-1);desired.normalize();
  r.bestRunZ=Math.min(r.bestRunZ??pos.z,pos.z);
  const screenBoost=earlyScreenRun(r);
  const lead=screenBoost?receivers.filter(a=>a!==r&&a.blockAim&&a.mesh.position.z<pos.z-.8&&a.mesh.position.distanceTo(pos)<10).sort((a,b)=>a.mesh.position.distanceToSquared(pos)-b.mesh.position.distanceToSquared(pos))[0]:null;
  const lane=V.lane({x:pos.x,z:pos.z,bestZ:r.bestRunZ,awareness:F.awareness(r.profile),style:r.style||V.playstyle(r.profile),goalZ:GOAL_LINE_Z,markerZ:worldZForYards(lineToGainYards),lead:lead?{x:lead.mesh.position.x,z:lead.mesh.position.z+1.5}:null,defenders:defenders.filter(d=>!(d.diveRecovery>0)).map(d=>({x:d.mesh.position.x,z:d.mesh.position.z,vx:d.velocity.x,vz:d.velocity.z,blocked:d.blockTime>0}))});
  if(!openGoal)desired.set(lane.x,0,lane.z);
  else {desired.set(0,0,-1);r.impactVel.x*=Math.pow(.04,dt);}
  // Limit voluntary backward travel, including leftover momentum after a cut.
  // Contact impulses remain separate so tackle/collision physics keep authority.
  const retreatLeft=Math.max(0,lane.limit-(pos.z-r.bestRunZ));
  if(retreatLeft<.3&&desired.z>0)desired.set(desired.x,0,-.4).normalize();
  const current=r.heading.clone().normalize(),angle=Math.acos(THREE.MathUtils.clamp(current.dot(desired),-1,1)),cross=current.x*desired.z-current.z*desired.x,sign=cross<0?-1:1,turnRating=P.effective((r.profile?.turning||50))/100,turn=Math.min(angle,THREE.MathUtils.lerp(7.0,10.2,turnRating)*V.weather(matchWeather().name).cut*dt)*sign,c=Math.cos(turn),s=Math.sin(turn);r.heading.set(current.x*c-current.z*s,0,current.x*s+current.z*c).normalize();
  const cutPenalty=THREE.MathUtils.clamp(angle/(Math.PI*.70),0,1),cutRating=P.effective((r.profile?.cutting||50))/100,targetSpeed=r.maxSpeed*(screenBoost?1.22:1)*(r.signature==='YAC Specialist'?1.02:.98)*(1-THREE.MathUtils.lerp(.31,.15,cutRating)*cutPenalty),desiredVel=r.heading.clone().multiplyScalar(targetSpeed),dv=desiredVel.sub(r.velocity),maxDv=THREE.MathUtils.lerp(20,29,cutRating)*(screenBoost?1.45:1)*dt;if(dv.length()>maxDv)dv.setLength(maxDv);r.velocity.add(dv);if(r.velocity.z>0)r.velocity.z=Math.min(r.velocity.z,retreatLeft/Math.max(dt,.001),2.4);pos.addScaledVector(r.velocity,dt);pos.addScaledVector(r.impactVel,dt);r.impactVel.multiplyScalar(Math.pow(.055,dt));r.mesh.rotation.y=Math.atan2(r.heading.x,r.heading.z);animatePlayerContact(r,dt);attachBallToCarrier();
  if(!r.history.length||gameTime-r.history[r.history.length-1].t>=30){const sample=r.history.length>=32?r.history.shift():{p:new THREE.Vector3(),v:new THREE.Vector3()};sample.t=gameTime;sample.p.copy(pos);sample.v.copy(r.velocity);r.history.push(sample);}
}
function finishSeries(offenseWon,headline,detail){
  endBall();if(offenseWon){flashResult('TOUCHDOWN!',true,1200);showMessage(headline||'TOUCHDOWN!',detail||'Drive complete.',1600)}else{flashResult('TURNOVER ON DOWNS',false,1200);showMessage(headline||'SERIES LOST',detail||'The defense stopped the drive.',1600)}
  resetDrive();if(!seriesResult(offenseWon))scheduleResult(()=>setupPlay(true),1450);
}
function finishPlayAtSpot(worldZ,reason='TACKLED',exactSpot=false){
  replayEligible=replayEligible||yardsForWorldZ(worldZ)-snapSpotYards>=10||worldZ<=GOAL_LINE_Z;
  captureReplay(true);replayRecording=false;playState='dead';
  const rawSpot=yardsForWorldZ(worldZ),newSpot=Math.min(50,Math.max(exactSpot?0:ballSpotYards+1,rawSpot)),gain=Math.max(0,newSpot-snapSpotYards);F.commitPlay(franchise,playLog,{spot:newSpot,snap:snapSpotYards,touchdown:newSpot>=50});ballSpotYards=newSpot;ballCarrier=null;tackler=null;tackleTimer=0;ball.visible=false;
  if(ballSpotYards>=50){score+=7;finishSeries(true,'TOUCHDOWN!',`${Math.round(gain)} yards on the play. Three touchdowns win this best-of-5 matchup.`);return}
  if(ballSpotYards>=lineToGainYards-.01){const oldTarget=lineToGainYards;lineToGainYards=Math.min(50,lineToGainYards+25);down=1;flashResult('FIRST DOWN!',true,950);showMessage('FIRST DOWN!',`${Math.round(gain)}-yard gain · reached ${oldTarget} yards. New set of downs.`,1250);updateScore();scheduleResult(()=>setupPlay(true),1100);return}
  if(down>=4){finishSeries(false,'STOPPED ON DOWNS',`${reason} after ${Math.round(gain)} yards. You needed ${Math.ceil(lineToGainYards-ballSpotYards)} more.`);return}
  down++;flashResult(gain>=15?'BIG PLAY!':reason==='TACKLED'?'TACKLED':'PLAY OVER',true,850);showMessage(reason,`${Math.round(gain)}-yard gain · ${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)} next.`,1100);updateScore();scheduleResult(()=>setupPlay(true),950);
}
function useDown(head,detail,countDrop=false){
  endBall();if(countDrop)drops++;flashResult(head==='PASS BROKEN UP'?'BREAKUP':'INCOMPLETE',false,900);if(down>=4){finishSeries(false,'STOPPED ON DOWNS',`${detail} Four downs expired before reaching ${lineToGainYards} yards.`);return}down++;showMessage(head,`${detail} ${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)} next.`,1150);updateScore();scheduleResult(()=>setupPlay(true),1050);
}
function logCatch(player,worldZ=player.mesh.position.z){if(!playLog)return;playLog.receiver=player.profile.id;playLog.target=player.profile.id;playLog.catchSpot=yardsForWorldZ(worldZ);playLog.contested=nearestOpponentDistance(player,false)<1.6;playLog.dropped=null;}
function beginRunAfterCatch(player){
  player.placement=player.placement||catchPlacement(player);recordAttempt(player);logCatch(player);markCatch(player);
  const placement=player.placement;
  ballLive=false;arcLine.visible=false;landRing.visible=false;predictedFlightTime=0;charging=false;
  player.securedHands=player.mesh.userData.hands.map(h=>h.position.clone());
  ballCarrier=player;player.hasBall=true;player.catchPose=.45;player.catchStyle=placement.kind;player.catchStyleTime=.6;
  player.velocity.multiplyScalar(placement.retention);playState='run';catches++;
  flashResult(placement.feedback,true,900);
  showMessage(placement.feedback,`${player.label} · ${placement.kind.toLowerCase()} · find space upfield.`,850);
  attachBallToCarrier();updateScore();
  if(Math.abs(player.mesh.position.x)>24.8){
    // Feet remain at the real catch location; toe taps end the play, never teleport a receiver inbounds.
    playState='sidelinecatch';player.velocity.set(0,0,0);const catchZ=player.mesh.position.z;transition={at:gameTime+380,fn:()=>finishPlayAtSpot(catchZ,'SIDELINE CATCH')};
  }
}

function assignPursuitRoles(){
  if(!ballCarrier)return;
  const ranked=defenders.filter(d=>!(d.diveRecovery>0)).sort((a,b)=>a.mesh.position.distanceToSquared(ballCarrier.mesh.position)-b.mesh.position.distanceToSquared(ballCarrier.mesh.position));
  ranked.forEach((d,i)=>{d.pursuitRole=i;});
}
function startDivingTackle(d,r){
  if(d.tackleCooldown>0||d.diveTime>0||d.diveRecovery>0||d.fakeUntil>gameTime||d.jumpY>.1||d.stagger>.15)return false;
  const delta=r.mesh.position.clone().sub(d.mesh.position).setY(0),gap=delta.length();
  const reach=2.15+d.technique*.65;
  if(gap<1.25||gap>reach||d.velocity.length()<3.2||d.heading.dot(delta.clone().normalize())<.72)return false;
  const aim=r.mesh.position.clone().addScaledVector(r.velocity,.16).sub(d.mesh.position).setY(0).normalize();
  const speed=d.maxSpeed+3.2+.8*d.technique,duration=.28;
  // Launch only if this committed path has a plausible physical contact window.
  const miss=V.sweptContact(-delta.x,-delta.z,aim.x*speed*duration-delta.x-r.velocity.x*duration,aim.z*speed*duration-delta.z-r.velocity.z*duration);
  if(miss.distance>1.35)return false;
  d.diveTime=duration;d.diveRecovery=0;d.tackleCooldown=2.1-.65*d.technique;
  d.heading.copy(aim);d.velocity.copy(aim).multiplyScalar(speed);d.fakeTarget=null;return true;
}
function tackleContact(d,r){
  if(d.diveRecovery>0&&!d.divingThisStep)return null;
  const start=d.tacklePrevious||d.mesh.position,runnerStart=r.tacklePrevious||r.mesh.position;
  const closeBody=.40*(d.mesh.scale.x+r.mesh.scale.x);
  const radius=d.fakeUntil>gameTime?closeBody:closeBody+.36+.14*d.technique-.06*Math.min(1,P.effective(r.profile.evasion)/100)+(d.divingThisStep?.28:0);
  const sweep=V.sweptContact(start.x-runnerStart.x,start.z-runnerStart.z,d.mesh.position.x-r.mesh.position.x,d.mesh.position.z-r.mesh.position.z,radius);
  // Being fooled affects steering, not whether actual shoulder/body contact can tackle.
  if(sweep.distance>radius)return null;
  // A hurdle clears only its low diving threat, and only at actual contact height.
  const clearance=THREE.MathUtils.lerp(r.tacklePreviousY??(r.jumpY||0),r.jumpY||0,sweep.entry);
  if(r.hurdleTime>0&&r.hurdleTarget===d&&(d.divingThisStep||d.diveTime>0)&&clearance>.48)return null;
  return {time:sweep.entry,distance:sweep.distance,radius};
}
// Finish-play decisions use existing attributes: turning/evasion provide body control.
function runnerFinishAbility(r){
  const rating=k=>THREE.MathUtils.clamp(P.effective(r.profile[k]||50)/100,0,1);
  return {athletic:rating('athleticism'),control:(rating('turning')+rating('evasion'))*.5,strength:actorStrength(r),speed:rating('speed')};
}
function effortDivePlan(r){
  if(playState!=='run'||r.effortConsidered||r.jumpY>.08||r.stagger>.1||r.stumbleTime>0||r.jukeAnim>0||r.velocity.z>-3.5||Math.abs(r.mesh.position.x)>23.8)return null;
  const ability=runnerFinishAbility(r),speed=r.velocity.length();
  if(ability.athletic<.65||speed<4||r.heading.z>-.72)return null;
  // Only reach for a marker still ahead, within the runner's real launch/arm range.
  const reach=Math.min(3.6,speed*(.22+ability.athletic*.10)+.65);
  const goal=lineToGainYards>=50||r.mesh.position.z-GOAL_LINE_Z<=reach,markerZ=goal?GOAL_LINE_Z:worldZForYards(lineToGainYards);
  const gap=r.mesh.position.z-markerZ;
  if(gap<.45||gap>reach)return null;
  let threat=null,closest=Infinity;
  for(const d of defenders){
    if(d.diveRecovery>0||d.fakeUntil>gameTime)continue;
    const delta=d.mesh.position.clone().sub(r.mesh.position).setY(0),distance=delta.length();
    if(distance<1.15)return null; // Already wrapped up: no late invulnerability dive.
    if(distance>3.8)continue;
    const future=delta.clone().addScaledVector(d.velocity,.28).addScaledVector(r.velocity,-.28);
    const contact=V.sweptContact(delta.x,delta.z,future.x,future.z,1.35+.14*d.technique);
    if(contact.distance<1.35+.14*d.technique&&distance<closest){threat=d;closest=distance;}
  }
  if(!threat)return null;
  return {goal,markerZ,ability,threat,chance:THREE.MathUtils.clamp(.12+(ability.athletic-.65)*.85+ability.control*.16+ability.strength*.06+(F.awareness(r.profile)-60)*.0005,.12,.64)};
}
function tryEffortDive(r){
  const plan=effortDivePlan(r);if(!plan)return false;
  r.effortConsidered=true; // One decision per possession, not a lottery every frame.
  if(Math.random()>=plan.chance)return false;
  const a=plan.ability,vertical=1.8+a.athletic*.65;
  r.jukeAnim=0;r.catchStyleTime=0;r.handTargets=null;r.securedHands=null;
  r.finishMotion={kind:'dive',elapsed:0,downAt:vertical*2/9.81,duration:vertical*2/9.81+.30,
    velocity:r.velocity.clone().multiplyScalar(1+.05*a.speed).clampLength(0,11),vertical,startY:r.mesh.position.y,
    side:1,ability:a,markerZ:plan.markerZ,goal:plan.goal,freeDive:true,extend:1,startPose:finishPoseSnapshot(r),
    bestZ:carriedBallFrontZ(),spotZ:null,down:false,result:null,contacts:0};
  playState='tackle';tackler=null;tackleTimer=r.finishMotion.duration;replayEligible=true;
  flashResult(plan.goal?'GOAL-LINE DIVE!':'REACH FOR THE FIRST!',true,650);
  return true;
}
function finishPoseSnapshot(a){
  const rig=a.mesh.userData.visualRig;
  return {pitch:rig.rotation.x,roll:rig.rotation.z,yaw:rig.rotation.y,drop:a.finishPose?.drop||0,extend:a.finishPose?.extend||0,fold:a.finishPose?.fold||0};
}
function triggerTackle(d,contactTime=1){
  if(!ballCarrier||!(playState==='run'||ballCarrier.finishMotion?.freeDive))return;
  const r=ballCarrier,prior=r.finishMotion,start=r.tacklePrevious||r.mesh.position;
  r.mesh.position.lerpVectors(start,r.mesh.position,contactTime);
  if(d.tacklePrevious)d.mesh.position.lerpVectors(d.tacklePrevious,d.mesh.position,contactTime);
  attachBallToCarrier();
  const a=runnerFinishAbility(r),rv=prior?prior.velocity:r.velocity,dv=d.velocity.clone();
  const offset=d.mesh.position.clone().sub(r.mesh.position).setY(0).normalize(),along=offset.dot(r.heading);
  const lateral=r.heading.z*offset.x-r.heading.x*offset.z,closing=rv.clone().sub(dv).length();
  const support=defenders.filter(other=>other!==d&&other.diveRecovery<=0&&other.mesh.position.distanceTo(r.mesh.position)<2).length;
  const kind=d.divingThisStep?'trip':along<-.45?'drag':Math.abs(along)<.55?'side':closing>8?'hit':'wrap';
  const rm=.8+a.strength*.7,dm=.8+actorStrength(d)*.7;
  const velocity=rv.clone().multiplyScalar(rm).addScaledVector(dv,dm*.65).divideScalar(rm+dm*.65);
  velocity.multiplyScalar(THREE.MathUtils.clamp(.42+a.strength*.22+a.control*.12-support*.14,.18,.8)).clampLength(0,7);
  const downAt=THREE.MathUtils.clamp(.22+a.control*.09+a.athletic*.05-(kind==='trip'?.05:0)-support*.04,.16,.36);
  const startPose=finishPoseSnapshot(r);
  r.finishMotion={kind,elapsed:0,downAt,duration:.72,velocity,vertical:0,startY:r.mesh.position.y,
    side:Math.sign(lateral)||1,ability:a,freeDive:false,extend:prior?.extend||0,startPose,
    bestZ:prior?.bestZ??carriedBallFrontZ(),spotZ:null,down:false,result:null,contacts:1+support};
  tackler=d;d.finishedDive=!!d.divingThisStep;d.diveTime=0;d.finishStart=finishPoseSnapshot(d);
  d.finishOffset=d.mesh.position.clone().sub(r.mesh.position);d.velocity.copy(velocity);r.velocity.copy(velocity);
  tackleTimer=.72;playState='tackle';
  flashResult(kind==='trip'?'TRIPPED UP':kind==='drag'?'DRAGGED DOWN':kind==='hit'?'BIG HIT':'WRAP TACKLE',false,600);
}
function carriedBallFrontZ(){return ball.position.z-(.19+.1425*Math.abs(footballAxis().z));}

// Blocks require actual front/side contact. Each defender gets a recovery window.
function updateBlocking(dt,screenReceiver=null){
  const runner=ballCarrier||screenReceiver;if(!runner)return;
  const carrier=runner.mesh.position,claimed=new Set();
  for(const d of defenders){d.blockTime=Math.max(0,(d.blockTime||0)-dt);d.blockCooldown=Math.max(0,(d.blockCooldown||0)-dt);}
  const blockers=receivers.filter(b=>b!==runner).sort((a,b)=>(b.style==='Blocking Receiver')-(a.style==='Blocking Receiver')||b.profile.strength-a.profile.strength);
  for(const b of blockers){
    b.blockCooldown=Math.max(0,(b.blockCooldown||0)-dt);b.blockPose=Math.max(0,(b.blockPose||0)-dt);b.blockAim=null;
    if(!ballCarrier&&b.route!=='Lead')continue;
    const pos=b.mesh.position;if(pos.distanceTo(carrier)>18)continue;
    const useful=d=>!claimed.has(d)&&d.mesh.position.distanceTo(carrier)<16&&d.mesh.position.z<carrier.z+2&&d.diveRecovery<=0;
    const score=d=>pos.distanceTo(d.mesh.position)*.65+carrier.distanceTo(d.mesh.position)*.55+Math.abs(d.mesh.position.x-carrier.x)*.3-(b.blockTarget===d?1.6:0);
    const d=defenders.filter(useful).sort((a,c)=>score(a)-score(c))[0];if(!d)continue;
    claimed.add(d);b.blockTarget=d;
    // Approach the carrier-facing shoulder, then mirror pursuit to keep leverage.
    const toRunner=carrier.clone().sub(d.mesh.position).setY(0).normalize();
    b.blockAim=d.mesh.position.clone().addScaledVector(d.velocity,.08+F.awareness(b.profile)*.0008).addScaledVector(toRunner,.85);
    b.blockAim.x=THREE.MathUtils.clamp(b.blockAim.x,-24.5,24.5);
    const toward=d.mesh.position.clone().sub(pos).setY(0),dist=toward.length();toward.normalize();
    if(dist>1.5||b.blockCooldown>0||d.blockCooldown>0||d.diveTime>0||b.heading.dot(toward)<.05)continue;
    const leverage=pos.clone().sub(d.mesh.position).setY(0).normalize().dot(toRunner);
    if(leverage<-.1)continue; // no useful seal from behind the defender
    const result=V.blockOutcome({strength:P.effective(b.profile.strength),size:P.effective(b.profile.size),defenseStrength:d.strength,defenseSize:(d.mesh.scale.x*d.mesh.scale.z-1)*100+55,alignment:leverage+(b.style==='Blocking Receiver'?.22:0),momentum:b.velocity.clone().sub(d.velocity).dot(toward)});
    if(result.slow<.8)F.event(playLog,b.profile,'blocks');
    d.blockTime=result.duration;d.blockSlow=result.slow;d.blockCooldown=result.duration+1.8;b.blockCooldown=result.duration+1.1;b.blockPose=result.duration;b.shoveAnim=.34;
    b.velocity.multiplyScalar(.55);d.velocity.multiplyScalar(result.slow);d.impactVel.addScaledVector(toward,.25+(1-result.slow));
  }
}
function updateRunAfterCatch(dt,now){
  if(!ballCarrier)return;
  const r=ballCarrier;
  if(tryEffortDive(r)){updateTackle(dt);return;}
  r.tacklePrevious=r.tacklePrevious||new THREE.Vector3();r.tacklePrevious.copy(r.mesh.position);r.tacklePreviousY=r.jumpY||0;
  for(const d of defenders){d.tacklePrevious=d.tacklePrevious||new THREE.Vector3();d.tacklePrevious.copy(d.mesh.position);d.divingThisStep=false;}
  if(r.screenCatchAt==null)r.screenCatchAt=gameTime;
  updateBlocking(dt);
  assignPursuitRoles();
  // Expose committed low tackles before the runner's timing decision this step.
  for(const d of defenders)if(!(d.blockTime>0))startDivingTackle(d,r);
  for(const receiver of receivers){if(receiver===r)updateBallCarrier(receiver,dt);else updateReceiver(receiver,dt,now);}
  for(const d of defenders){
    d.divingThisStep=d.diveTime>0;
    updateDefender(d,defenderTarget(d,now),dt);
  }
  attachBallToCarrier();
  let first=null;
  for(const d of defenders){const contact=tackleContact(d,r);if(contact&&(!first||contact.time<first.contact.time))first={d,contact};}
  const goalTime=r.mesh.position.z<=GOAL_LINE_Z?THREE.MathUtils.clamp((GOAL_LINE_Z-r.tacklePrevious.z)/(r.mesh.position.z-r.tacklePrevious.z||-1),0,1):Infinity;
  const sidelineTime=Math.abs(r.mesh.position.x)>25.6?THREE.MathUtils.clamp((Math.sign(r.mesh.position.x)*25.6-r.tacklePrevious.x)/(r.mesh.position.x-r.tacklePrevious.x||1),0,1):Infinity;
  // Award a score only when the goal-line crossing precedes contact or stepping out.
  if(goalTime<=sidelineTime&&goalTime<=(first?.contact.time??Infinity)&&goalTime!==Infinity){finishPlayAtSpot(GOAL_LINE_Z,'TOUCHDOWN');return;}
  if(sidelineTime<(first?.contact.time??Infinity)){
    const z=THREE.MathUtils.lerp(r.tacklePrevious.z,r.mesh.position.z,sidelineTime);finishPlayAtSpot(z,'OUT OF BOUNDS');return;
  }
  if(first){
    if(attemptCarrierMove(r,first.d,'stiff')){
      // A second tackler is still dangerous on the very same simulation step.
      const support=defenders.filter(d=>d!==first.d).map(d=>({d,contact:tackleContact(d,r)})).filter(x=>x.contact).sort((a,b)=>a.contact.time-b.contact.time)[0];
      if(support){triggerTackle(support.d,support.contact.time);return;}
      solvePlayerCollisions(dt);attachBallToCarrier();return;
    }
    const {d,contact}=first,dx=d.mesh.position.x-r.mesh.position.x,dz=d.mesh.position.z-r.mesh.position.z;
    const glancing=Math.abs(r.heading.x*dx+r.heading.z*dz)<contact.radius*.55;
    const support=defenders.some(other=>other!==d&&tackleContact(other,r));
    if(!support&&!d.divingThisStep&&glancing&&!r.brokenTackle&&actorStrength(r)>actorStrength(d)+.08&&r.velocity.length()>4){
      F.event(playLog,r.profile,'brokenTackles');r.brokenTackle=true;r.stumbleTime=.48;r.stumbleSide=Math.sign(dx)||1;r.velocity.multiplyScalar(.65);d.fakeUntil=gameTime+300;d.stagger=.3;
      d.diveRecovery=.22;d.impactVel.add(new THREE.Vector3(-r.heading.z,0,r.heading.x).multiplyScalar(2));
      flashResult('CONTACT BALANCE',true,650);
    }else {triggerTackle(d,contact.time);return;}
  }
  solvePlayerCollisions(dt);attachBallToCarrier();
}

// Small pose library on the existing joints; no ragdoll solver or new assets.
function poseFinishPlayer(a){
  const p=a.finishPose,rig=a.mesh.userData.visualRig,{hands,arms,forearms,legs,shins,feet}=a.mesh.userData;
  a.mesh.rotation.x=0;a.mesh.rotation.z=0;
  rig.rotation.set(p.pitch,p.yaw||0,p.roll);
  const hip=new THREE.Vector3(0,.9,0),rotated=hip.clone().applyQuaternion(rig.quaternion);
  rig.position.copy(hip).sub(rotated);rig.position.y-=p.drop||0;
  for(let i=0;i<2;i++){
    const side=i?1:-1,shoulder=new THREE.Vector3(side*.48,1.48,.02);
    if(a.hasBall){
      // The football stays between both gloves as the arms extend or protect it.
      hands[i].position.set(i?.40:.16,1.18,.32).lerp(new THREE.Vector3(side*.12,1.65,.80),p.extend||0);
      if(i===0&&p.brace)hands[i].position.lerp(new THREE.Vector3(-.55,1.05,.62),p.brace);
    }else hands[i].position.set(side*.35,1.24,.68);
    if(p.wrapTarget){
      a.mesh.updateMatrixWorld(true);const target=p.wrapTarget.mesh.position.clone();target.y+=p.wrapTarget.mesh.scale.y*(a.finishedDive?.62:1.08);
      rig.worldToLocal(target);target.x+=side*.22;hands[i].position.lerp(target,.9);
    }
    const elbow=bendJoint(shoulder,hands[i].position,.44,.46,new THREE.Vector3(side*.65,-.55,-.3));
    poseBone(arms[i],shoulder,elbow);poseBone(forearms[i],elbow,hands[i].position);
    const fold=p.fold||0,ankle=new THREE.Vector3(side*.24,.15+fold*(i?.22:.38),-fold*(i?.40:.25));
    const legHip=new THREE.Vector3(side*.21,.94,0),knee=bendJoint(legHip,ankle,.45,.45,new THREE.Vector3(side*.1,0,1));
    poseBone(legs[i],legHip,knee);poseBone(shins[i],knee,ankle);feet[i].position.copy(ankle);feet[i].rotation.set(Math.PI/2+fold*.5,0,0);
  }
  // Bounds are cached on shared geometry; only the one/two falling rigs are tested.
  a.mesh.updateMatrixWorld(true);let bottom=Infinity;const corner=new THREE.Vector3();
  rig.traverse(part=>{if(!part.isMesh)return;const g=part.geometry;if(!g.boundingBox)g.computeBoundingBox();const b=g.boundingBox;
    for(let n=0;n<8;n++){corner.set(n&1?b.max.x:b.min.x,n&2?b.max.y:b.min.y,n&4?b.max.z:b.min.z).applyMatrix4(part.matrixWorld);bottom=Math.min(bottom,corner.y);}
  });
  if(bottom<.025)rig.position.y+=(.025-bottom)/a.mesh.scale.y;
  a.mesh.updateMatrixWorld(true);
}
function poseFinishMotion(r,m){
  const t=THREE.MathUtils.clamp(m.elapsed/m.downAt,0,1),blend=t*t*(3-2*t),settle=THREE.MathUtils.clamp((m.elapsed-m.downAt)/.3,0,1);
  const dive=m.kind==='dive',side=m.kind==='side',back=m.kind==='hit'&&m.velocity.dot(r.heading)<1;
  const pitch=dive?1.32:back?-1.02:side?.60:m.kind==='trip'?1.42:1.18;
  const roll=(side?1.20:m.kind==='drag'?.45:.18)*m.side;
  const extension=m.extend?THREE.MathUtils.clamp(m.elapsed/(dive?.20:.10),0,1)*(m.down?1-settle*.25:1):0;
  r.finishPose={pitch:THREE.MathUtils.lerp(m.startPose.pitch,pitch,blend),roll:THREE.MathUtils.lerp(m.startPose.roll,roll,blend)+m.side*Math.sin(settle*Math.PI)*.18,
    yaw:THREE.MathUtils.lerp(m.startPose.yaw,0,blend),drop:THREE.MathUtils.lerp(m.startPose.drop||0,.54,blend),brace:blend*(1-extension),extend:Math.max(m.startPose.extend*(1-blend),extension),fold:.25+Math.sin(t*Math.PI)*.6};
  poseFinishPlayer(r);
  if(tackler){
    const d=tackler,initial=d.finishStart||{pitch:0,roll:0,yaw:0};
    d.finishPose={pitch:THREE.MathUtils.lerp(initial.pitch,d.finishedDive?1.4:.95,blend),roll:THREE.MathUtils.lerp(initial.roll,m.side*.6,blend),yaw:0,drop:blend*.45,fold:.4+blend*.3,wrapTarget:r};
    poseFinishPlayer(d);
  }
  attachBallToCarrier();
}
function updateTackle(dt){
  const r=ballCarrier;if(!r||!r.finishMotion)return;
  // Fixed small steps keep contact, marker crossing and the down spot independent of FPS.
  if(dt>1/120+.000001){const n=Math.ceil(dt/(1/120));for(let i=0;i<n&&playState==='tackle';i++)updateTackle(dt/n);return;}
  let m=r.finishMotion;
  // Stop exactly at the down event before any cosmetic settling can extend the ball.
  if(!m.down&&m.elapsed<m.downAt-1e-9&&m.elapsed+dt>m.downAt+1e-9){const live=m.downAt-m.elapsed;updateTackle(live);if(playState==='tackle')updateTackle(dt-live);return;}
  const before=r.mesh.position.clone(),beforeBall=carriedBallFrontZ();
  r.tacklePrevious=r.tacklePrevious||new THREE.Vector3();r.tacklePrevious.copy(before);
  const activeDt=m.down?0:Math.min(dt,Math.max(0,m.downAt-m.elapsed));
  if(activeDt>0){
    const drag=m.freeDive?.45:2.6,travel=(1-Math.exp(-drag*activeDt))/drag;
    r.mesh.position.addScaledVector(m.velocity,travel);m.velocity.multiplyScalar(Math.exp(-drag*activeDt));r.velocity.copy(m.velocity);
  }else r.mesh.position.addScaledVector(m.velocity,dt*.12); // cosmetic settling never changes the spot
  m.elapsed+=dt;
  r.mesh.position.y=m.freeDive?Math.max(0,m.startY+m.vertical*Math.min(m.elapsed,m.downAt)-4.905*Math.min(m.elapsed,m.downAt)**2):m.startY*Math.max(0,1-m.elapsed/m.downAt);
  if(tackler){tackler.mesh.position.copy(r.mesh.position).add(tackler.finishOffset);tackler.mesh.position.y=0;}
  poseFinishMotion(r,m);
  if(m.freeDive&&!m.down){
    let hit=null;
    for(const d of defenders){
      d.tacklePrevious=d.tacklePrevious||new THREE.Vector3();d.tacklePrevious.copy(d.mesh.position);
      startDivingTackle(d,r);d.divingThisStep=d.diveTime>0;
      updateDefender(d,r.mesh.position.clone().addScaledVector(r.velocity,.12),dt);
      const contact=tackleContact(d,r);if(contact&&(!hit||contact.time<hit.contact.time))hit={d,contact};
    }
    // A real tackle can shorten or stop a dive; it never grants invulnerability.
    if(hit){
      const ballNow=carriedBallFrontZ(),goalTime=ballNow<=GOAL_LINE_Z?(GOAL_LINE_Z-beforeBall)/(ballNow-beforeBall||-1):Infinity;
      const sideTime=Math.abs(r.mesh.position.x)>25.6?(Math.sign(r.mesh.position.x)*25.6-before.x)/(r.mesh.position.x-before.x||1):Infinity;
      if(hit.contact.time<Math.min(goalTime,sideTime)){triggerTackle(hit.d,hit.contact.time);return;}
    }
  }
  const front=carriedBallFrontZ();
  if(!m.down){
    const sideTime=Math.abs(r.mesh.position.x)>25.6?THREE.MathUtils.clamp((Math.sign(r.mesh.position.x)*25.6-before.x)/(r.mesh.position.x-before.x||1),0,1):Infinity;
    const goalTime=front<=GOAL_LINE_Z?THREE.MathUtils.clamp((GOAL_LINE_Z-beforeBall)/(front-beforeBall||-1),0,1):Infinity;
    if(goalTime<=sideTime&&goalTime!==Infinity){m.result='TOUCHDOWN';m.spotZ=GOAL_LINE_Z;m.down=true;replayEligible=true;flashResult('TOUCHDOWN REACH!',true,800);}
    else if(sideTime!==Infinity){m.result='OUT OF BOUNDS';m.spotZ=Math.min(m.bestZ,THREE.MathUtils.lerp(beforeBall,front,sideTime));m.down=true;}
    else {
      m.bestZ=Math.min(m.bestZ,front);
      if(m.elapsed+1e-9>=m.downAt){m.down=true;m.spotZ=m.bestZ;m.result=m.kind==='dive'?'DIVING REACH':'TACKLED';}
    }
    if(m.down){tackleSpotYards=yardsForWorldZ(m.spotZ);m.velocity.multiplyScalar(.35);r.velocity.set(0,0,0);}
  }
  if(m.down)m.velocity.multiplyScalar(Math.exp(-7*dt));
  tackleTimer=Math.max(0,m.duration-m.elapsed);
  if(m.elapsed+1e-9>=m.duration){
    // Capture the landing/roll before ending; replay skip resumes the next snap once.
    captureReplay(true);finishPlayAtSpot(m.spotZ??m.bestZ,m.result||'TACKLED',true);
  }
}

function seriesResult(offenseWon){
  if(offenseWon)seriesOffense++;else seriesDefense++;updateScore();
  if(seriesOffense>=3){showMessage('MATCHUP WON!',`You won Round ${activeRound()}. Cash payout and the receiver market are opening.`,1600);endMatchup(true);return true}
  if(seriesDefense>=3){score=Math.max(0,score-7);showMessage('MATCHUP LOST',`The defense won the best-of-5. Your roster and money remain saved.`,1600);endMatchup(false);return true}return false;
}
function endBall(){pendingCatch=null;recordAttempt();F.commitPlay(franchise,playLog,{spot:snapSpotYards,snap:snapSpotYards});captureReplay(true);replayRecording=false;ballLive=false;ball.visible=false;if(playState!=='run'&&playState!=='tackle')playState='dead';arcLine.visible=false;landRing.visible=false;predictedFlightTime=0}
function resolveCatch(player,isDefense){pendingCatch=null;if(isDefense){ints++;if(playLog)playLog.interception=true;endBall();flashResult('INTERCEPTION',false,1050);showMessage('INTERCEPTED','Turnover — the defense wins this series.',1300);resetDrive();if(!seriesResult(false))scheduleResult(()=>setupPlay(true),1250)}else beginRunAfterCatch(player);updateScore()}
function resolveDrop(head='DROP',detail='The receiver could not secure the catch.',countDrop=false){useDown(head,detail,countDrop)}
function resolveGround(){useDown('INCOMPLETE',throwBobbles?'The loose ball was not secured before it hit the ground or left the field.':'Placement missed the hands.',receiverDrop)}
function resolveNoThrow(){charging=false;arcLine.visible=false;landRing.visible=false;useDown('THROW CLOCK EXPIRED','Eight seconds elapsed before the ball came out.')}

function update(dt,now){
  // Bound physics work to five substeps at the existing .035-second frame cap.
  if(ballLive&&dt>1/120+.000001){const steps=Math.ceil(dt/(1/120)),step=dt/steps;for(let i=0;i<steps;i++)update(step,now-(steps-i-1)*step*1000);return;}
  if(ballLive){contactStep=dt;previousBallAxis.copy(footballAxis());for(const a of [...receivers,...defenders])sampleContactRig(a,true);}
  if(playState==='countdown'){
    const motion=formationMotion||plays[selectedPlay].motion;
    if(motion){const r=receivers[motion.slot],t=THREE.MathUtils.clamp(1-(snapTime-now)/2200,0,1);r.mesh.position.x=THREE.MathUtils.lerp(r.start.x,motion.to,t);r.mesh.rotation.y=Math.sign(motion.to-r.start.x)*Math.PI/2;r.runIntensity=.65;r.runPhase+=dt*10;animatePlayerContact(r,dt);
      if(t===1){r.start.copy(r.mesh.position);r.path=pathFor(r.route,r.start);r.heading.set(0,0,-1);r.velocity.set(0,0,-r.speed*.15);}}

    const left=Math.max(0,snapTime-now),count=Math.ceil(left/1000);if(count>0&&count!==nextCount){nextCount=count;showMessage(String(count),`${plays[selectedPlay].name} locked in.`,760)}
    if(now>=snapTime){replayFrames=[];replayPool=[];replayInterval=1/30;replayRecording=true;replaySample=0;replayEligible=down===4;captureReplay(true);playState='live';franchise.conceptMemory=[...(Array.isArray(franchise.conceptMemory)?franchise.conceptMemory:[]),plays[selectedPlay].name].slice(-24);throwClock=8;routeVisuals.visible=false;$('playCallPanel').style.display='none';updateThrowClock();showMessage('SNAP',`${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)}. Eight seconds to throw — a completed pass stays live until the runner is tackled or scores.`,950)}
  }
  if(playState==='sidelinecatch'&&ballCarrier){updateJump(ballCarrier,dt,false);animatePlayerContact(ballCarrier,dt);attachBallToCarrier();}
  if(playState==='live'||playState==='thrown'){const screen=receivers.find(r=>r.screenTarget);if(screen)updateBlocking(dt,screen);receivers.forEach(r=>updateReceiver(r,dt,now));defenders.forEach(d=>updateDefender(d,defenderTarget(d,now),dt));solvePlayerCollisions(dt)}else if(playState==='run'){updateRunAfterCatch(dt,now);if(ballCarrier){const target=ballCarrier.mesh.position.clone().add(new THREE.Vector3(0,3.6,8));camera.position.lerp(target,Math.min(1,dt*3));camera.lookAt(ballCarrier.mesh.position.clone().add(new THREE.Vector3(0,1.3,-3)))}}else if(playState==='tackle'){updateTackle(dt)}
  if(playState==='live'&&!ballLive){throwClock=Math.max(0,throwClock-dt);updateThrowClock();if(throwClock<=0){resolveNoThrow();return}}
  if(charging){if(keyLoft)loftBias=Math.min(1,loftBias+dt*1.5);if(keyBullet)loftBias=Math.max(-1,loftBias-dt*1.5);const st=throwStats(currentHeld());chargePower=st.power;updateCharge();updateArcPreview()}else if(!ballLive){chargePower*=Math.pow(.08,dt);loftBias*=Math.pow(.18,dt);updateCharge()}
  if(ballLive){
    throwTime+=dt;ballPrev.copy(ball.position);
    // Poor spirals can flutter vertically and lose a little speed, but no longer curve sideways away from the reticle.
    ballVel.x+=V.weather(matchWeather().name).wind*Math.min(1,Math.max(0,ball.position.y-3)/8)*dt*(activeRound()%2?1:-1);
    const wobble=(1-spiralQuality);if(wobble>.05){ballVel.y+=Math.cos(throwTime*11+duckPhase)*wobble*.8*dt;ballVel.multiplyScalar(1-wobble*.012*dt)}
    const securing=!!pendingCatch;
    if(pendingCatch){
      pendingCatch.actor.mesh.updateMatrixWorld(true);
      ball.position.copy(pendingCatch.limb.mesh.localToWorld(pendingCatch.offset.clone()));
    }else{ball.position.addScaledVector(ballVel,dt);ball.position.y-=4.905*dt*dt;ballVel.y-=9.81*dt;}
    const dir=ballVel.clone().normalize();if(!pendingCatch&&dir.lengthSq()>.001)ball.quaternion.setFromUnitVectors(new THREE.Vector3(0,0,1),dir);
    ballSpin+=dt*(18+spiralQuality*28);ball.userData.spin.rotation.z=ballSpin;
    ball.userData.spin.rotation.x=ballTumble?Math.sin(throwTime*ballTumble)*.9:Math.sin(throwTime*16+duckPhase)*(1-spiralQuality)*.18;
    // Ground/boundary contact must win over later limb contacts in the same step.
    const groundRadius=.19+Math.abs(footballAxis().y)*.1425;
    if(ballPrev.y<=.19){resolveGround();return;}
    let stop=1;
    if(ball.position.y<=groundRadius)stop=Math.min(stop,THREE.MathUtils.clamp((ballPrev.y-groundRadius)/Math.max(1e-9,ballPrev.y-ball.position.y),0,1));
    for(const [key,limit] of [['x',26],['z',78]])if(Math.abs(ball.position[key])>limit){const boundary=Math.sign(ball.position[key])*limit;stop=Math.min(stop,THREE.MathUtils.clamp((boundary-ballPrev[key])/(ball.position[key]-ballPrev[key]),0,1));}
    if(stop<1)ball.position.lerpVectors(ballPrev,ball.position,stop);
    const touched=checkBallContact();
    if(ballLive&&(stop<1&&!touched||throwTime>7)){pendingCatch=null;resolveGround();}
    else if(securing&&pendingCatch)finishSecuringCatch(dt);
    if(ballLive){const landing=drawTrajectory(ball.position,ballVel);if(landing){predictedLanding.copy(landing);const livePrediction=getBallLanding();predictedFlightTime=livePrediction?livePrediction.time:0;}}

  }
  if(resultFlashTimer&&now>resultFlashTimer){resultFlashTimer=0;$('resultFlash').className=''}
  if(messageTimer&&now>messageTimer&&playState==='live'){messageTimer=0;$('headline').textContent='THROW THE WINDOW';$('detail').textContent=`${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)} · Aim through the exact center reticle. W = loft, S = bullet.`}
}
function resize(){
  // IMPORTANT: keep the canvas CSS size exactly matched to the viewport.
  // With a >1 devicePixelRatio, setSize(..., false) can leave the canvas' visible
  // CSS/intrinsic size wider than the HUD, making the true 3D center appear right
  // of the 50% crosshair even though the camera-forward throw math is correct.
  renderer.setSize(innerWidth,innerHeight,true);
  canvas.style.width='100vw';
  canvas.style.height='100vh';
  camera.aspect=innerWidth/innerHeight;
  camera.updateProjectionMatrix();
}addEventListener('resize',resize);resize();
function loop(now){const frameMs=now-lastTime,dt=Math.min(.035,frameMs/1000);lastTime=now;
  if(replay){if(!menuOpen&&!document.hidden)updateReplay(dt)}
  else if(!inputBlocked()&&!document.hidden){gameTime+=dt*1000;if(transition&&gameTime>=transition.at){const fn=transition.fn;transition=null;fn()}update(dt,gameTime);captureReplay(false,dt)}
  updateHUD();updateEnvironment(dt,frameMs);renderer.render(scene,camera);requestAnimationFrame(loop)}requestAnimationFrame(loop);

// Desktop aiming and throws.
canvas.addEventListener('click',e=>{if(inputBlocked())return;if(playState==='call'){const idx=findReceiverAtScreen(e.clientX,e.clientY);if(idx>=0){openAudible(idx);return}}if(matchMedia('(pointer:fine)').matches&&document.pointerLockElement!==canvas)canvas.requestPointerLock?.()});
addEventListener('mousemove',e=>{if(!inputBlocked()&&document.pointerLockElement===canvas){yaw-=e.movementX*.0019;pitch-=e.movementY*.0017;pitch=THREE.MathUtils.clamp(pitch,-.68,.44);yaw=THREE.MathUtils.clamp(yaw,-Math.PI/2,Math.PI/2);applyCamera()}});
addEventListener('keydown',e=>{
  if(replay){if(['ArrowLeft','ArrowRight','ArrowUp','ArrowDown'].includes(e.code)){e.preventDefault();if(!e.repeat)cycleReplayCamera(['ArrowLeft','ArrowUp'].includes(e.code)?-1:1);return}if(e.code==='Space'||e.code==='Escape'){e.preventDefault();finishReplay()}return}
  if(inputBlocked())return;
  if(e.code==='Escape'&&audibleReceiverIndex==null){showMainMenu();return}
  if(e.code==='KeyF'&&playState==='live'){e.preventDefault();if(!e.repeat)pumpFake();return}
  if(e.code==='KeyJ'&&playState==='run'){e.preventDefault();if(!e.repeat)tryJuke(ballCarrier,true);return}
  if(playState==='call'){
    if(audibleReceiverIndex!=null&&/^Key[A-Z]$/.test(e.code)){const route=routeByKey[e.code.slice(3)];if(route){e.preventDefault();assignAudibleRoute(route);return}}
    if(/^Key[XHYZ]$/.test(e.code)){const idx=receiverByKey[e.code.slice(3)];if(idx!=null){e.preventDefault();openAudible(idx);return}}
    if(/^Digit[1-8]$/.test(e.code)){e.preventDefault();selectedPlay=Number(e.code.slice(-1))-1;playCategory=V.category(plays[selectedPlay]);setupPlay(false);return}
  }
  if(e.code==='Space'){e.preventDefault();if(playState==='call'){if(!e.repeat)beginCountdown();return}if(playState==='live'&&!e.repeat)startCharge()}
  if(e.code==='KeyW'){keyLoft=true;if(charging)e.preventDefault()}if(e.code==='KeyS'){keyBullet=true;if(charging)e.preventDefault()}
  if(e.code==='Escape'&&playState==='call'&&audibleReceiverIndex!=null){e.preventDefault();closeAudible(true)}
  if(e.code==='Enter'){e.preventDefault();beginCountdown()}if(e.code==='KeyR'&&playState==='call'){e.preventDefault();setupPlay(false)}
});addEventListener('keyup',e=>{if(inputBlocked())return;if(e.code==='Space'){e.preventDefault();if(playState==='live'||playState==='thrown')stopCharge()}if(e.code==='KeyW')keyLoft=false;if(e.code==='KeyS')keyBullet=false});

// Touch: swipe aims; stationary/slow hold charges. Release throws.
let touch=null;canvas.addEventListener('pointerdown',e=>{if(inputBlocked()||e.pointerType!=='touch')return;if(playState==='call'){const idx=findReceiverAtScreen(e.clientX,e.clientY);if(idx>=0)openAudible(idx);return}touch={id:e.pointerId,lastX:e.clientX,lastY:e.clientY,startY:e.clientY,t:gameTime,charging:false,moved:0};canvas.setPointerCapture?.(e.pointerId)});
canvas.addEventListener('pointermove',e=>{if(inputBlocked()||!touch||e.pointerId!==touch.id)return;const dx=e.clientX-touch.lastX,dy=e.clientY-touch.lastY;touch.lastX=e.clientX;touch.lastY=e.clientY;touch.moved+=Math.hypot(dx,dy);if(touch.charging){yaw-=dx*.0028;loftBias=THREE.MathUtils.clamp(loftBias-dy*.018,-1,1)}else{yaw-=dx*.0045;pitch-=dy*.0041;pitch=THREE.MathUtils.clamp(pitch,-.72,.46)}yaw=THREE.MathUtils.clamp(yaw,-Math.PI/2,Math.PI/2);applyCamera();if(!touch.charging&&gameTime-touch.t>220&&playState==='live'){touch.charging=true;startCharge()}});
function endTouch(e){if(inputBlocked()||!touch||e.pointerId!==touch.id)return;const held=gameTime-touch.t;if(!touch.charging&&held>220&&playState==='live'){touch.charging=true;startCharge()}if(touch.charging)stopCharge();touch=null}canvas.addEventListener('pointerup',endTouch);canvas.addEventListener('pointercancel',()=>{touch=null;charging=false});setInterval(()=>{if(!inputBlocked()&&touch&&!touch.charging&&gameTime-touch.t>220&&playState==='live'){touch.charging=true;startCharge()}},40);

$('audibleClose').addEventListener('click',e=>{e.stopPropagation();closeAudible(true)});
$('startBtn').addEventListener('click',resumeGame);$('startTeamBtn').addEventListener('click',()=>openManager('Train each attribute, sign prospects, and develop jukes, head fakes and stronger catches.',false));$('snapBtn').addEventListener('click',e=>{e.stopPropagation();beginCountdown()});$('newPlayBtn').addEventListener('click',e=>{e.stopPropagation();if(playState==='call')setupPlay(false)});$('cameraBtn').addEventListener('click',e=>{e.stopPropagation();resetAim()});
$('teamBtn').addEventListener('click',e=>{e.stopPropagation();if(playState==='call')openManager('Train your receivers between snaps. ATH lifts jumping and diving; SIZE adds reach and leverage; TRICK sells jukes and head fakes.',false);else showMessage('FINISH THE PLAY','Team management is available between snaps and automatically between matchups.',950)});
$('continueBtn').addEventListener('click',()=>closeManager());
$('cloudOfferSave').onclick=()=>{$('cloudOffer').hidden=true;openSaves();$('cloudName').focus()};
$('cloudOfferDismiss').onclick=()=>{$('cloudOffer').hidden=true};
$('refreshMarketBtn').addEventListener('click',()=>{if(franchise.cash<175){$('managerStatus').textContent=`You need $${175-franchise.cash} more to refresh the market.`;return}franchise.cash-=175;franchise.market=freshMarket();$('managerStatus').textContent='Market refreshed with five new prospects.';saveFranchise();renderManager()});
$('resetFranchiseBtn').addEventListener('click',()=>{$('managerLayer').style.display='none';showMainMenu();openSaves()});
// Menu, saved franchises, visual themes and deterministic replay playback.
function inputBlocked(){return !!studio?.isOpen||menuOpen||saveOpen||cloudBusy||!!replay||$('managerLayer').style.display==='flex'}
function checkpoint(){
  franchise.checkpoint={seriesOffense,seriesDefense,ballSpotYards,down,lineToGainYards,selectedPlay};
  saveFranchise();
}
function restoreCheckpoint(){
  const c=franchise.checkpoint;resetDrive();seriesOffense=0;seriesDefense=0;
  if(c){seriesOffense=c.seriesOffense;seriesDefense=c.seriesDefense;ballSpotYards=c.ballSpotYards;down=c.down;lineToGainYards=c.lineToGainYards;selectedPlay=THREE.MathUtils.clamp(c.selectedPlay||0,0,plays.length-1)}
}
function renderMenu(){
  studio?.hub();
  $('menuSummary').textContent=`Franchise ${activeSlot+1} · Round ${franchise.round} · ${franchise.wins} wins · $${franchise.cash}${localSaveAvailable?'':' · Local saving unavailable'}`;
  $('startBtn').textContent=receivers.length?'RESUME GAME':'PLAY GAME';
  for(const id of ['trainingBtn','recruitBtn','opponentsBtn'])$(id).disabled=receivers.length>0&&!['call','manager'].includes(playState);
  $('startTeamBtn').disabled=receivers.length>0&&playState!=='call';
}
function cancelInput(){charging=false;keyLoft=false;keyBullet=false;touch=null;arcLine.visible=false;landRing.visible=false;document.exitPointerLock?.()}
function showMainMenu(){cancelInput();menuOpen=true;$('startLayer').style.display='flex';renderMenu()}
function resumeGame(){
  if(cloudBusy)return;menuOpen=false;saveOpen=false;$('startLayer').style.display='none';$('saveLayer').hidden=true;
  if(!receivers.length){restoreCheckpoint();setupPlay(false)}
}
function openSaves(){saveOpen=true;cancelInput();$('saveLayer').hidden=false;renderSlots()}
function renderSlots(){
  const host=$('saveSlots');host.replaceChildren();
  for(let i=0;i<3;i++){
    const slot=slots[i],row=document.createElement('div');row.className='slotRow';
    const text=document.createElement('span');text.textContent=slot?`Slot ${i+1}${activeSlot===i?' · active':''} — Round ${slot.data.round} · $${slot.data.cash} · ${new Date(slot.updatedAt).toLocaleString()}`:`Slot ${i+1} — Empty`;
    row.appendChild(text);const load=document.createElement('button');load.className='ghost';load.textContent=slot?'LOAD':'NEW GAME';load.disabled=cloudBusy;
    load.onclick=()=>slot?loadSlot(i):newSlot(i);row.appendChild(load);
    if(slot){const fresh=document.createElement('button');fresh.textContent='NEW';fresh.className='ghost';fresh.disabled=cloudBusy;fresh.onclick=()=>newSlot(i);row.appendChild(fresh)}
    host.appendChild(row);
  }
}
function activateFranchise(f,slot){
  if(replay)finishReplay();clearLastReplay();transition=null;clearPlayers();replayFrames=[];replayRecording=false;replayEligible=false;
  snapDefense=null;attemptPending=false;franchise=F.normalize(P.normalizeCompetition(f));playLog=null;studio?.close();franchise.scouting=V.cleanHistory(franchise.scouting);franchise.team=franchise.team.map(normalizeReceiver);franchise.market=franchise.market.map(normalizeMarketReceiver);
  $('cloudOffer').hidden=true;activeSlot=slot;selectedRosterIndex=0;managerLocked=false;$('managerLayer').style.display='none';
  score=0;catches=0;drops=0;ints=0;playState='dead';ball.visible=false;ballLive=false;restoreCheckpoint();saveFranchise();
  saveOpen=false;$('saveLayer').hidden=true;showMainMenu();
}
function loadSlot(i){if(cloudBusy)return;try{const f=P.validateSave(slots[i].data);activateFranchise(f,i)}catch(err){$('cloudStatus').textContent=`Could not load: ${err.message}`}}
function newSlot(i){
  if(cloudBusy)return;if(slots[i]&&!confirm(`Replace local franchise ${i+1}? Its cloud save, if any, is kept.`))return;
  activateFranchise(defaultFranchise(),i);
}
$('menuBtn').onclick=showMainMenu;$('savesBtn').onclick=openSaves;
$('newGameBtn').onclick=()=>{const empty=[0,1,2].find(i=>!slots[i]);if(empty!==undefined)newSlot(empty);else openSaves()};
$('closeSaves').onclick=()=>{if(cloudBusy)return;saveOpen=false;$('saveLayer').hidden=true};
async function cloudAction(mode){
  if(cloudBusy)return;
  const name=$('cloudName').value,password=$('cloudPassword').value;
  if(mode==='load'&&!confirm(`Load the cloud franchise into local slot ${activeSlot+1}? This replaces that local slot.`))return;
  cloudBusy=true;$('cloudSave').disabled=true;$('cloudLoad').disabled=true;renderSlots();$('cloudStatus').textContent=mode==='save'?'Encrypting and saving…':'Loading cloud franchise…';
  try{
    if(mode==='save'){saveFranchise();await QBCloud.save(name,password,{...franchise,cloudSaveOffered:true});franchise.cloudSaveOffered=true;saveFranchise();$('cloudStatus').textContent='Cloud save complete. Use this name and password on any device.'}
    else{const f=await QBCloud.load(name,password);activateFranchise(f,activeSlot);$('cloudStatus').textContent='Cloud franchise loaded.'}
  }catch(err){$('cloudStatus').textContent=`Cloud ${mode} failed: ${err.message}. Your local progress is still available.`}
  finally{cloudBusy=false;$('cloudSave').disabled=false;$('cloudLoad').disabled=false;$('cloudPassword').value='';renderSlots()}
}
$('cloudSave').onclick=()=>cloudAction('save');$('cloudLoad').onclick=()=>cloudAction('load');
addEventListener('blur',()=>{if(!menuOpen)showMainMenu()});
document.addEventListener('visibilitychange',()=>{if(document.hidden){cancelInput();saveFranchise()}});

function tryJuke(r,manual=false){
  if(!r||r.moveCooldown>0||(r.jukeCooldown||0)>0||r.jumpY>.08||r.stagger>.1||ballLive)return false;
  const candidates=defenders.filter(d=>d.mesh.position.distanceTo(r.mesh.position)<4.8&&d.mesh.position.z<=r.mesh.position.z+1.5&&d.fakeUntil<=gameTime&&!(d.diveTime>0)&&!(d.diveRecovery>0));
  if(!candidates.length){if(manual)flashResult('GET CLOSER TO A DEFENDER',true,550);return false}
  const nearest=candidates.reduce((a,b)=>a.mesh.position.distanceTo(r.mesh.position)<b.mesh.position.distanceTo(r.mesh.position)?a:b);
  let side=nearest.mesh.position.x>=r.mesh.position.x?-1:1;
  if(r.mesh.position.x>22)side=-1;else if(r.mesh.position.x<-22)side=1;
  const traits=P.traits(r.profile),distance=nearest.mesh.position.distanceTo(r.mesh.position);
  r.jukeMove=distance<2.1?'SPIN':distance>3.5?'HESITATION':'HARD CUT';
  r.jukeSide=side;r.jukeDuration=(r.jukeMove==='SPIN'?.62:.56)*THREE.MathUtils.clamp(1.12-P.effective(r.profile.evasion)*.002,.84,1.08);
  r.jukeCooldown=traits.jukeCooldown;r.jukeAnim=r.jukeDuration;r.plantPose=1;
  r.velocity.multiplyScalar(r.jukeMove==='HESITATION'?.68:.86);
  r.impactVel.addScaledVector(new THREE.Vector3(side,0,0),2.6+P.effective(r.profile.tricks)*.015);
  let beaten=0;
  for(const d of [nearest]){
    const gap=d.mesh.position.distanceTo(r.mesh.position);
    const timing=gap>=2&&gap<=3.6?.12:0;
    const familiarity=!r.hasBall?Math.max(0,(franchise.conceptMemory||[]).filter(x=>x===plays[selectedPlay].name).length-1):0;
    const chance=THREE.MathUtils.clamp(traits.jukeChance-familiarity*.07-(V.identity(activeRound()).discipline??.5)*.06+timing+(r.screenTarget&&gameTime-(r.screenCatchAt??-Infinity)<1800?.28:0)-currentSkill()*.32-(d.technique||0)*.1-(gap>4?.18:0),.10,.92);
    if(Math.random()<chance){
      beaten++;d.fakeUntil=gameTime+THREE.MathUtils.clamp(260+P.effective(r.profile.tricks)*1.5-currentSkill()*160,180,470);
      d.fakeTarget=r.mesh.position.clone().add(new THREE.Vector3(-side*2.2,0,-1));
      d.plantPose=1;d.velocity.multiplyScalar(.68);
      d.fakeSide=-side;d.impactVel.addScaledVector(new THREE.Vector3(-side,0,0),1.6);
    }
  }
  if(beaten&&r.hasBall)F.event(playLog,r.profile,'jukes');
  if(manual||r===ballCarrier)flashResult(beaten?r.jukeMove+' — BEAT '+beaten+'!':r.jukeMove+' — DEFENDER HELD',!!beaten,700);
  return true;
}
function updateTricks(r,dt,carrier){
  r.jukeCooldown=Math.max(0,(r.jukeCooldown||0)-dt);r.jukeAnim=ballLive?0:Math.max(0,(r.jukeAnim||0)-dt);
  // Players choose a moment near a defender. Chance is time based, not frame based.
  if(!ballLive&&(!carrier||!clearGoalLane(r))&&(carrier||playState==='live')&&Math.random()<dt*(.5+P.effective(r.profile.tricks)*.009)*(carrier?(1+P.effective(r.profile.evasion)/100)*(earlyScreenRun(r)?2.8:1):1)*(.94+F.awareness(r.profile)*.0006)*(r.style==='YAC Specialist'?1.3:r.style==='Power Receiver'||r.style==='Possession Receiver'?.6:1))tryJuke(r);
}
$('jukeBtn').onclick=()=>{if(!inputBlocked()&&playState==='run')tryJuke(ballCarrier,true)};
function updateHUD(){
  document.body.dataset.phase=playState;document.body.dataset.active=String(['live','thrown','run','tackle'].includes(playState));document.body.dataset.charging=String(charging);document.body.dataset.replay=String(!!replay);
  for(const id of ['watchReplayBtn','watchReplayMenu','watchReplayTeam'])$(id).disabled=!canWatchReplay();
  $('pumpBtn').hidden=playState!=='live'||inputBlocked();$('pumpBtn').disabled=gameTime<pumpReadyAt;
  $('jukeBtn').hidden=playState!=='run'||inputBlocked();
  if(ballCarrier){$('jukeBtn').disabled=(ballCarrier.jukeCooldown||0)>0;$('jukeBtn').textContent=ballCarrier.jukeCooldown>0?`JUKE · ${ballCarrier.jukeCooldown.toFixed(1)}s`:'JUKE · J'}
  $('throwClock').hidden=menuOpen;
}

// A small reusable environment: mowing stripes, painted numbers, seating tiers,
// end-zone lettering and upright goals. No new geometry is allocated per matchup.
const fieldDecor=new THREE.Group();scene.add(fieldDecor);
const stripeMat=new THREE.MeshStandardMaterial({color:0xffffff,transparent:true,opacity:.045,roughness:1});
for(let z=35;z>=-55;z-=20){const m=new THREE.Mesh(new THREE.PlaneGeometry(52,10),stripeMat);m.rotation.x=-Math.PI/2;m.position.set(0,.009,z);fieldDecor.add(m)}
function fieldLabel(text,w,h,x,z,color='#f6f7e8'){
  const c=document.createElement('canvas');c.width=512;c.height=128;const cx=c.getContext('2d');cx.fillStyle=color;cx.font='bold 90px system-ui';cx.textAlign='center';cx.textBaseline='middle';cx.fillText(text,256,64);
  const tex=new THREE.CanvasTexture(c);tex.colorSpace=THREE.SRGBColorSpace;
  const m=new THREE.Mesh(new THREE.PlaneGeometry(w,h),new THREE.MeshBasicMaterial({map:tex,transparent:true,depthWrite:false}));m.rotation.x=-Math.PI/2;m.position.set(x,.022,z);fieldDecor.add(m);return m;
}
for(let yards=10;yards<=40;yards+=10){for(const x of [-20,20])fieldLabel(String(50-yards),3.1,1.5,x,worldZForYards(yards))}
const teamEndzoneLabel=fieldLabel('RECEIVER WINDOW',35,4,0,-65);
let endzoneLabelKey='';
function updateEndzoneIdentity(u){const key=u.name+u.logo+u.endzoneStyle;if(key===endzoneLabelKey)return;endzoneLabelKey=key;const c=teamEndzoneLabel.material.map.image,cx=c.getContext('2d');cx.clearRect(0,0,c.width,c.height);cx.fillStyle=u.secondary;cx.textAlign='center';cx.textBaseline='middle';cx.font='bold 48px "Segoe UI Emoji", system-ui';cx.fillText(u.endzoneStyle===1?`${u.logo}  ${u.name.toUpperCase()}  ${u.logo}`:`${u.logo} ${u.name.toUpperCase()}`,256,64,495);teamEndzoneLabel.material.map.needsUpdate=true;}
stadiumDetails=R?.stadium(scene)||null;
const seats=new THREE.MeshStandardMaterial({color:0x596c7c,roughness:.95});
for(const side of [-1,1])for(let tier=0;tier<4;tier++){
  const m=new THREE.Mesh(new THREE.BoxGeometry(2.5,.55,110),seats);m.position.set(side*(29+tier*2),2+tier*1.7,-10);fieldDecor.add(m);
}
const postMat=new THREE.MeshStandardMaterial({color:0xffc936,metalness:.35,roughness:.4});
for(const [x,y,z,sx,sy,sz] of [[0,2.6,-72,.2,5.2,.2],[0,5.2,-72,9,.18,.18],[-4.5,7.7,-72,.18,5,.18],[4.5,7.7,-72,.18,5,.18]]){const m=new THREE.Mesh(new THREE.BoxGeometry(sx,sy,sz),postMat);m.position.set(x,y,z);fieldDecor.add(m)}
const venues=[{name:'Riverside Field',turf:0x28733d,sky:0x8abbd5,end:0x184578,sun:0xfff6e2},{name:'Hillcrest Stadium',turf:0x336b40,sky:0xa3bfce,end:0x243954,sun:0xfff4d8},{name:'Harbor Park',turf:0x266c48,sky:0x93b6c5,end:0x174f5b,sun:0xe8f3ff},{name:'Sunset Bowl',turf:0x3e7044,sky:0xc4ac98,end:0x544047,sun:0xffd4a6}];
const weatherLooks=[
  {name:'Day',sky:0x8abbd5,sun:0xfff6e2,power:2.2,ambient:2.15,fog:155},
  {name:'Sunset',sky:0xc59287,sun:0xffba78,power:1.65,ambient:1.85,fog:150},
  {name:'Night',sky:0x121f39,sun:0xd9e8ff,power:1.7,ambient:1.55,fog:130},
  {name:'Overcast',sky:0x9baab3,sun:0xe3ecf0,power:.9,ambient:2.1,fog:130},
  {name:'Light Rain',sky:0x8396a3,sun:0xcddfe9,power:.75,ambient:2.0,fog:112},
  {name:'Windy',sky:0x9baab3,sun:0xe3ecf0,power:1.4,ambient:2.1,fog:140},
  {name:'Cold',sky:0xa5b9cf,sun:0xe3ecff,power:1.6,ambient:2.1,fog:145}
];
function matchWeather(){
  if(!Number.isInteger(franchise.matchWeather)||franchise.matchWeather<0||franchise.matchWeather>=weatherLooks.length)franchise.matchWeather=Math.floor(Math.random()*weatherLooks.length);
  return weatherLooks[franchise.matchWeather];
}
const skyline=new THREE.InstancedMesh(new THREE.BoxGeometry(1,1,1),new THREE.MeshStandardMaterial({color:0x334456,roughness:1}),12);
const decorMatrix=new THREE.Matrix4();
for(let i=0;i<12;i++){decorMatrix.compose(new THREE.Vector3((i-5.5)*9,4+(i%4)*2,-89),new THREE.Quaternion(),new THREE.Vector3(5,8+(i%4)*4,6));skyline.setMatrixAt(i,decorMatrix);}scene.add(skyline);
const banners=new THREE.InstancedMesh(new THREE.BoxGeometry(.3,1.3,5),new THREE.MeshStandardMaterial({color:0xa63742}),12);
for(let i=0;i<12;i++){decorMatrix.makeTranslation(i<6?-27.5:27.5,.8,30-(i%6)*16);banners.setMatrixAt(i,decorMatrix);}scene.add(banners);
const rainPositions=new Float32Array(96*3),rainGeo=new THREE.BufferGeometry();
for(let i=0;i<96;i++){rainPositions[i*3]=(i*17%53)-26;rainPositions[i*3+1]=(i*7%19)+2;rainPositions[i*3+2]=40-i*1.1;}
const rainBase=rainPositions.slice();rainGeo.setAttribute('position',new THREE.BufferAttribute(rainPositions,3).setUsage(THREE.DynamicDrawUsage));
const rain=new THREE.Points(rainGeo,new THREE.PointsMaterial({color:0xd7e9f4,size:.12,transparent:true,opacity:.5,depthWrite:false}));rain.frustumCulled=false;rain.visible=false;scene.add(rain);
let qualityMode='auto',qualityLevel=2,slowFrames=0,qualityFrames=0;
try{const saved=localStorage.getItem('receiverWindowQB_quality');if(['auto','low','high'].includes(saved))qualityMode=saved;}catch{}
function setQuality(mode=qualityMode){
  qualityMode=mode;if(mode==='low')qualityLevel=0;else if(mode==='high')qualityLevel=2;
  renderer.setPixelRatio(Math.min(devicePixelRatio||1,qualityLevel===0?1:qualityLevel===1?1.3:1.7));
  renderer.shadowMap.enabled=qualityLevel>0;rain.visible=matchWeather().name==='Light Rain'&&qualityLevel>0;
  $('qualityBtn').textContent=`GRAPHICS · ${mode.toUpperCase()}`;
  try{localStorage.setItem('receiverWindowQB_quality',mode);}catch{}
}
$('qualityBtn').onclick=()=>{const modes=['auto','low','high'];setQuality(modes[(modes.indexOf(qualityMode)+1)%3]);};
function updateEnvironment(dt,frameMs){
  stadiumDetails?.animate(inputBlocked()||replay?0:dt,qualityLevel===0);
  if(rain.visible){for(let i=0;i<96;i++){rainPositions[i*3+1]=1+(rainBase[i*3+1]+20000-gameTime*.008%20)%20;rainPositions[i*3+2]=camera.position.z-15-(i*13%65);}rainGeo.attributes.position.needsUpdate=true;}
  if(qualityMode==='auto'&&!document.hidden&&!inputBlocked()&&['live','thrown','run'].includes(playState)&&frameMs<250){
    qualityFrames++;if(frameMs>27)slowFrames++;
    if(qualityFrames>=180){if(slowFrames>80&&qualityLevel>0){qualityLevel--;setQuality();}qualityFrames=0;slowFrames=0;}
  }
}
function applyFieldTheme(){
  const uniform=F.opponent(activeRound()),v=venues[uniform.venue%venues.length],weather=matchWeather(),w=uniform.venue===2?{...weather,sky:0x121f39,sun:0xd9e8ff,power:1.7,ambient:1.55}:weather;
  stadiumDetails?.theme(activeRound());updateEndzoneIdentity({...uniform,name:opponentForRound(activeRound()).name});
  turf.material.color.set(v.turf);turf.material.roughness=w.name==='Light Rain'?.7:.95;endzone.material.color.set(uniform.primary);
  scene.background.set(w.sky);scene.fog.color.set(w.sky);scene.fog.far=w.fog;sun.color.set(w.sun);sun.intensity=w.power;ambient.intensity=w.ambient;
  sun.position.set(w.name==='Sunset'?-55:-35,w.name==='Sunset'?22:55,25);
  seats.color.set(uniform.primary);banners.material.color.set(uniform.secondary);skyline.scale.y=1+((activeRound()-1)%3)*.22;
  rain.visible=w.name==='Light Rain'&&qualityLevel>0;$('defensePanel').title=`${['Small Outdoor Field','Grand Bowl','Night Stadium','Urban Grounds','Classic Field','Modern Arena'][uniform.venue]} · ${uniform.venue===2&&['Day','Sunset','Night'].includes(w.name)?'Under the lights':w.name}`;
}
let replayObjects=null,lastReplay=null,replayAngle='qb';
const replayAngles=['qb','sideline','overhead'];
setQuality();

function scenePose(reuse=null){
  if(!replayObjects){replayObjects=[];for(const a of [...receivers,...defenders])a.mesh.traverse(o=>replayObjects.push(o));ball.traverse(o=>replayObjects.push(o));}
  const objects=replayObjects,pose=reuse&&reuse.objects===objects?reuse:{objects,transforms:new Float32Array(objects.length*8),ball:new THREE.Vector3()};
  for(let i=0;i<objects.length;i++){const o=objects[i],j=i*8,a=pose.transforms;
    a[j]=o.position.x;a[j+1]=o.position.y;a[j+2]=o.position.z;a[j+3]=o.quaternion.x;a[j+4]=o.quaternion.y;a[j+5]=o.quaternion.z;a[j+6]=o.quaternion.w;a[j+7]=o.visible?1:0;
  }
  pose.ball.copy(ball.position);pose.qbZ=worldZForYards(snapSpotYards)+14;pose.time=gameTime/1000;pose.phase=playState;
  if(!pose.focus)pose.focus=new THREE.Vector3();pose.focus.copy(ballCarrier?ballCarrier.mesh.position:ball.position);if(ballCarrier)pose.focus.y+=1.1;else if(playState==='live'||playState==='countdown'){pose.focus.copy(camera.position);pose.focus.z-=12;pose.focus.y=1.1;}return pose;
}

function captureReplay(force=false,dt=0){
  if(!replayRecording||replay)return;
  replaySample+=dt;if(!force&&replaySample<replayInterval)return;replaySample=0;
  const last=replayFrames[replayFrames.length-1];
  if(last&&last.time===gameTime/1000){scenePose(last);return;}
  if(replayFrames.length>=360){
    const kept=[];replayFrames.forEach((frame,i)=>{if(i%2===0||i===replayFrames.length-1)kept.push(frame);else replayPool.push(frame);});
    replayFrames=kept;replayInterval*=2;
  }
  replayFrames.push(scenePose(replayPool.pop()));
}

function markCatch(r){
  r.bestRunZ=r.mesh.position.z;r.screenCatchAt=gameTime;
  const gain=yardsForWorldZ(ball.position.z)-snapSpotYards;
  replayEligible=replayEligible||r.placement?.kind==='CONTACT CATCH'||r.placement?.kind==='HIGH POINT'||gain>=10||ball.position.z<=GOAL_LINE_Z;captureReplay(true);
}
function scheduleResult(fn,delay){
  captureReplay(true);replayRecording=false;archiveReplay(replayFrames);checkpoint();
  if(replayEligible&&replayFrames.length>=2){
    const frames=replayFrames;replayFrames=[];replayEligible=false;
    transition={at:gameTime+Math.min(delay,800),fn:()=>startReplay(frames,fn)};
  }else transition={at:gameTime+delay,fn};
}
// Keep one detached, independently owned replay so setupPlay can dispose old actors.
function clearLastReplay(){
  if(!lastReplay)return;
  scene.remove(lastReplay.group);for(const mesh of lastReplay.group.children)R?.release(mesh);
  for(const g of lastReplay.geometries)g.dispose();
  for(const m of lastReplay.materials)m.dispose();
  lastReplay=null;
}
function archiveReplay(frames){
  if(frames.length<2)return;
  clearLastReplay();
  const group=new THREE.Group(),mapping=new Map(),geometries=new Map(),materials=new Map();
  const cloneMaterial=m=>{if(!materials.has(m))materials.set(m,m.clone());return materials.get(m)};
  for(const root of [...receivers.map(a=>a.mesh),...defenders.map(a=>a.mesh),ball]){
    const copy=root.clone(true),source=[],clones=[];root.traverse(o=>source.push(o));copy.traverse(o=>clones.push(o));
    source.forEach((o,i)=>{const c=clones[i];mapping.set(o,c);if(o.geometry){if(!geometries.has(o.geometry))geometries.set(o.geometry,o.geometry.clone());c.geometry=geometries.get(o.geometry);}if(o.material)c.material=Array.isArray(o.material)?o.material.map(cloneMaterial):cloneMaterial(o.material);});
    R?.retain(copy);group.add(copy);
  }
  const objects=frames[0].objects.map(o=>mapping.get(o));
  const copies=frames.map(f=>({...f,objects,transforms:f.transforms.slice(),ball:f.ball.clone(),focus:f.focus.clone()}));
  group.visible=false;scene.add(group);lastReplay={group,frames:copies,geometries:[...geometries.values()],materials:[...materials.values()]};
}
function canWatchReplay(){return !!lastReplay&&!replay&&!transition&&!cloudBusy&&!saveOpen&&['call','dead','manager'].includes(playState);}
function watchLastReplay(){
  if(!canWatchReplay())return;
  const previous={menu:menuOpen,start:$('startLayer').style.display,manager:$('managerLayer').style.display,routes:routeVisuals.visible,los:losLine.visible,gain:gainLine.visible,focus:document.activeElement};
  menuOpen=false;$('startLayer').style.display='none';$('managerLayer').style.display='none';
  startReplay(lastReplay.frames,()=>{lastReplay.group.visible=false;menuOpen=previous.menu;$('startLayer').style.display=previous.start;$('managerLayer').style.display=previous.manager;routeVisuals.visible=previous.routes;losLine.visible=previous.los;gainLine.visible=previous.gain;previous.focus?.focus?.();});
  for(const a of [...receivers,...defenders])a.mesh.visible=false;
  ball.visible=false;routeVisuals.visible=false;losLine.visible=false;gainLine.visible=false;lastReplay.group.visible=true;
  updateReplay(0);
}
function replayCameraLabel(){return {qb:'QB VIEW',sideline:'SIDELINE',overhead:'ANGLED OVERHEAD'}[replay.angle];}
function cycleReplayCamera(direction){
  if(!replay)return;
  replayAngle=replayAngles[(replayAngles.indexOf(replay.angle)+direction+replayAngles.length)%replayAngles.length];replay.angle=replayAngle;
  $('replayBar').querySelector('span').textContent=`REPLAY · ${replayCameraLabel()} · Arrow keys change view`;updateReplay(0);
}
function startReplay(frames,after){
  if(replay||frames.length<2)return;
  cancelInput();replay={frames,after,time:0,saved:scenePose(),cameraP:camera.position.clone(),cameraQ:camera.quaternion.clone(),angle:replayAngle,index:0};$('replayBar').hidden=false;$('replayBar').querySelector('span').textContent=`REPLAY · ${replayCameraLabel()} · Arrow keys change view`;
}
const replayQuaternion=new THREE.Quaternion();
function putPose(a,b,t){
  for(let i=0;i<a.objects.length;i++){const o=a.objects[i],j=i*8,f=a.transforms,g=b.transforms;
    o.position.set(f[j]+(g[j]-f[j])*t,f[j+1]+(g[j+1]-f[j+1])*t,f[j+2]+(g[j+2]-f[j+2])*t);
    o.quaternion.set(f[j+3],f[j+4],f[j+5],f[j+6]);replayQuaternion.set(g[j+3],g[j+4],g[j+5],g[j+6]);o.quaternion.slerp(replayQuaternion,t);o.visible=!!(t<.5?f[j+7]:g[j+7]);
  }
}

function updateReplay(dt){
  const r=replay;r.time+=dt*.72;
  const time=r.frames[0].time+r.time,last=r.frames[r.frames.length-1];
  while(r.index<r.frames.length-2&&r.frames[r.index+1].time<=time)r.index++;
  const a=r.frames[r.index],b=r.frames[Math.min(r.index+1,r.frames.length-1)];
  const t=THREE.MathUtils.clamp((time-a.time)/Math.max(.0001,b.time-a.time),0,1);putPose(a,b,t);
  const target=a.focus.clone().lerp(b.focus,t);
  const desired=new THREE.Vector3(0,2.25,r.frames[0].qbZ??worldZForYards(snapSpotYards)+14);
  if(r.angle==='sideline')desired.set(32,8,target.z+6);
  if(r.angle==='overhead')desired.set(target.x*.35,32,target.z+18);
  camera.position.copy(desired);camera.lookAt(target);
  if(time>=last.time+.3)finishReplay();
}

function finishReplay(){
  if(!replay)return;const r=replay;putPose(r.saved,r.saved,0);camera.position.copy(r.cameraP);camera.quaternion.copy(r.cameraQ);replay=null;$('replayBar').hidden=true;r.after();
}
$('skipReplay').onclick=finishReplay;
$('replayPreviousCamera').onclick=()=>cycleReplayCamera(-1);
$('replayNextCamera').onclick=()=>cycleReplayCamera(1);
for(const id of ['watchReplayBtn','watchReplayMenu','watchReplayTeam'])$(id).onclick=watchLastReplay;
if(globalThis.QBFranchiseUI){
  studio=QBFranchiseUI.create({franchise:()=>franchise,round:activeRound,slot:()=>activeSlot,opponent:opponentForRound,defense:V.identity,overall:playerOverall,style:V.playstyle,save:saveFranchise,pauseInput:cancelInput,
    canCustomize:()=>!replay&&!transition&&(!receivers.length||['call','manager','dead'].includes(playState)),
    refreshUniform:()=>{if(receivers.length&&playState==='call')setupPlay(false);if($('managerLayer').style.display==='flex')renderManager();},
    plays:()=>plays,categories:()=>V.categories,category:V.category,paths:diagramPaths,situation:()=>({down,toGo:lineToGainYards-ballSpotYards,spot:ballSpotYards}),selected:()=>previewPlayIndex??selectedPlay,previewPlay:i=>previewPlayIndex=i,
    canChoosePlay:()=>!replay&&!transition&&(!receivers.length||playState==='call'),choosePlay:i=>{if(receivers.length&&playState!=='call')return;selectedPlay=i;playCategory=V.category(plays[i]);setupPlay(false);previewPlayIndex=null;}});
  for(const id of ['uniformBtn','teamUniformBtn'])$(id).onclick=()=>studio.uniform();
  for(const id of ['recordsBtn','teamRecordsBtn'])$(id).onclick=()=>studio.records();
  $('menuPlaybookBtn').onclick=()=>{previewPlayIndex=selectedPlay;studio.playbook();};
  for(const [id,target] of [['trainingBtn','rosterGrid'],['recruitBtn','marketGrid'],['opponentsBtn','matchupPreview']])$(id).onclick=()=>{if(receivers.length&&!['call','manager'].includes(playState))return;openManager();$(target).scrollIntoView({block:'start'});};
  updatePlayButtons();
}
addEventListener('beforeunload',saveFranchise);saveFranchise();
})();
