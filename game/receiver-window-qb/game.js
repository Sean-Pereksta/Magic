
(()=>{
'use strict';
let gameTime=performance.now();
const P=QBProgression;
let menuOpen=true,saveOpen=false,cloudBusy=false,activeSlot=0,transition=null,replay=null,replayFrames=[],replayEligible=false,replayRecording=false,replaySample=0,throwDown=1,replayTailEnds=0;
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
scene.add(new THREE.HemisphereLight(0xe8f6ff,0x31542a,2.15));const sun=new THREE.DirectionalLight(0xffffff,2.2);sun.position.set(-35,55,25);sun.castShadow=true;sun.shadow.mapSize.set(1024,1024);sun.shadow.camera.left=-65;sun.shadow.camera.right=65;sun.shadow.camera.top=90;sun.shadow.camera.bottom=-25;scene.add(sun);

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
function makePlayer(offense=true,appearance=null){
  const look=normalizeAppearance(appearance,offense),root=new THREE.Group();
  const jerseyMat=(offense?offenseMat:defenseMat).clone();jerseyMat.color.set(look.jerseyPrimary);
  const accentMat=(offense?offenseAccent:blackMat).clone();accentMat.color.set(look.jerseyAccent);
  const headMat=skinMat.clone();headMat.color.set(look.skinColor);
  const helmetMat=(offense?offenseAccent:whiteMat).clone();helmetMat.color.set(look.helmetColor);
  const cleatMat=blackMat.clone();cleatMat.color.set(look.cleatColor);
  const body=new THREE.Mesh(new THREE.CapsuleGeometry(.42,.78,4,8),jerseyMat);body.position.y=1.18;body.castShadow=true;root.add(body);
  const stripe=new THREE.Mesh(new THREE.BoxGeometry(.88,.16,.18),accentMat);stripe.position.set(0,1.31,.36);root.add(stripe);
  const head=new THREE.Mesh(new THREE.SphereGeometry(.26,12,10),headMat);head.position.y=1.88;head.castShadow=true;root.add(head);
  const helmet=new THREE.Mesh(new THREE.SphereGeometry(.29,12,8,0,Math.PI*2,0,Math.PI*.63),helmetMat);helmet.position.y=1.98;root.add(helmet);
  const armGeo=new THREE.CapsuleGeometry(.085,.50,3,6),arms=[],hands=[];for(const x of[-.53,.53]){const arm=new THREE.Mesh(armGeo,jerseyMat);arm.position.set(x,1.22,.02);arm.rotation.z=x<0?-.08:.08;arm.castShadow=true;root.add(arm);arms.push(arm);const hand=new THREE.Mesh(new THREE.SphereGeometry(.105,8,7),headMat);hand.position.set(x,.86,.02);hand.castShadow=true;root.add(hand);hands.push(hand)}
  const legGeo=new THREE.CapsuleGeometry(.105,.42,3,6),feet=[],legs=[];for(const x of[-.22,.22]){const leg=new THREE.Mesh(legGeo,jerseyMat);leg.position.set(x,.50,0);root.add(leg);legs.push(leg);const foot=new THREE.Mesh(new THREE.BoxGeometry(.22,.13,.48),cleatMat);foot.position.set(x,.16,-.11);foot.castShadow=true;root.add(foot);feet.push(foot)}
  const visualRig=new THREE.Group();
  for(const part of [...root.children])visualRig.add(part);
  root.add(visualRig);root.userData.visualRig=visualRig;
  root.scale.set(look.scaleX,look.scaleY,look.scaleZ);
  const shadow=new THREE.Mesh(new THREE.CircleGeometry(.5,14),shadowMat);shadow.rotation.x=-Math.PI/2;shadow.position.y=.016;shadow.scale.set(Math.max(.86,look.scaleX*.98),1,Math.max(.86,look.scaleZ*.98));root.add(shadow);let trackRing=null;if(offense){trackRing=new THREE.Mesh(new THREE.RingGeometry(.62,.82,24),new THREE.MeshBasicMaterial({color:0x62ff9c,transparent:true,opacity:.82,side:THREE.DoubleSide,depthWrite:false}));trackRing.rotation.x=-Math.PI/2;trackRing.position.y=.025;trackRing.visible=false;root.add(trackRing)}root.userData.arms=arms;root.userData.hands=hands;root.userData.feet=feet;root.userData.legs=legs;root.userData.body=body;root.userData.trackRing=trackRing;root.userData.appearance=look;return root;
}
function makeBall(){
  const root=new THREE.Group();const g=new THREE.SphereGeometry(.19,16,10);g.scale(1,1,1.75);const m=new THREE.Mesh(g,ballMat);m.castShadow=true;root.add(m);
  const lace=new THREE.Mesh(new THREE.BoxGeometry(.035,.025,.38),laceMat);lace.position.set(0,.17,0);root.add(lace);root.visible=false;scene.add(root);root.userData.mesh=m;return root;
}
const ball=makeBall();

// Pre-snap route art stays on the field until the snap.
const routeVisuals=new THREE.Group();scene.add(routeVisuals);const routeColors=[0x62d9ff,0xffd166,0x7cffae,0xff82c6];
function clearRouteVisuals(){while(routeVisuals.children.length){const o=routeVisuals.children[0];o.geometry?.dispose();o.material?.dispose();routeVisuals.remove(o)}}
function drawRouteVisuals(){clearRouteVisuals();receivers.forEach((r,i)=>{const pts=r.path.map(p=>new THREE.Vector3(p.x,.055,p.z));const line=new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts),new THREE.LineBasicMaterial({color:routeColors[i],transparent:true,opacity:.92}));routeVisuals.add(line);if(pts.length>1){const end=pts[pts.length-1],prev=pts[pts.length-2],dir=end.clone().sub(prev).normalize();const arrow=new THREE.Mesh(new THREE.ConeGeometry(.36,.85,10),new THREE.MeshBasicMaterial({color:routeColors[i],transparent:true,opacity:.95}));arrow.position.copy(end).add(new THREE.Vector3(0,.16,0));arrow.quaternion.setFromUnitVectors(new THREE.Vector3(0,1,0),dir.clone().setY(0).normalize());arrow.rotateX(Math.PI/2);routeVisuals.add(arrow)}});routeVisuals.visible=true}

// Green trajectory preview.
const arcMat=new THREE.LineBasicMaterial({color:0x62ff9c,transparent:true,opacity:.9});const arcGeo=new THREE.BufferGeometry();const arcLine=new THREE.Line(arcGeo,arcMat);arcLine.visible=false;scene.add(arcLine);
const landRing=new THREE.Mesh(new THREE.RingGeometry(.45,.66,28),new THREE.MeshBasicMaterial({color:0x62ff9c,transparent:true,opacity:.8,side:THREE.DoubleSide}));landRing.rotation.x=-Math.PI/2;landRing.visible=false;scene.add(landRing);

const routeOptions=[
  {name:'Go',key:'G'},{name:'Slant',key:'S'},{name:'Out',key:'O'},{name:'Post',key:'P'},{name:'Corner',key:'C'},
  {name:'Drag',key:'D'},{name:'Dig',key:'I'},{name:'Comeback',key:'B'},{name:'Flat',key:'F'},{name:'Wheel',key:'W'}
];
const routePool=routeOptions.map(r=>r.name);
const routeByKey=Object.fromEntries(routeOptions.map(r=>[r.key,r.name]));
const receiverByKey={X:0,H:1,Y:2,Z:3};
const plays=[
  {name:'Four Verticals',routes:['Go','Go','Go','Go']},
  {name:'Mesh',routes:['Drag','Post','Corner','Drag']},
  {name:'Levels',routes:['Dig','Drag','Dig','Go']},
  {name:'Smash',routes:['Corner','Out','Out','Corner']},
  {name:'Dagger',routes:['Go','Dig','Post','Go']},
  {name:'Drive',routes:['Dig','Drag','Post','Out']},
  {name:'Crossfire',routes:['Post','Slant','Slant','Corner']},
  {name:'Sideline',routes:['Out','Corner','Corner','Out']}
];
let selectedPlay=0;
const defenses=[
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
const endlessAdjectives=['Iron','Night','Storm','Prime','Viper','Titan','Phantom','Cobalt','Crimson','Onyx','Apex','Metro','Crown','Blitz','Steel','Summit'];
const endlessNouns=['Ballhawks','Shadows','Sentinels','Lockdown','Wardens','Jackals','Falcons','Guard','Hunters','Legion','Cyclones','Reapers','Stalkers','Titans','Rangers','Dragons'];
function opponentForRound(round){
  if(round<=baseOpponents.length)return baseOpponents[round-1];
  const i=round-1,name=`${endlessAdjectives[i%endlessAdjectives.length]} ${endlessNouns[(i*7+3)%endlessNouns.length]}`;
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
function opponentUniformForRound(round){return opponentUniformThemes[(round-1)%opponentUniformThemes.length]}
function buildDefenderAppearance(round){const skin=choice(skinTones),cleat=choice(cleatThemes),size=choice(sizeProfiles),jersey=opponentUniformForRound(round);return{skinColor:skin.color,skinName:skin.name,cleatColor:cleat.color,cleatName:cleat.name,sizeName:size.name,scaleX:size.sx,scaleY:size.sy,scaleZ:size.sz,jerseyPrimary:jersey.primary,jerseyAccent:jersey.accent,helmetColor:jersey.helmet,jerseyName:jersey.name}}
function normalizeAppearance(a,offense=true){if(!a||typeof a!=='object')return offense?buildReceiverAppearance():buildDefenderAppearance(1);const color=v=>typeof v==='string'&&/^#[0-9a-f]{6}$/i.test(v)?v:null; a={...a}; for(const k of ['skinColor','cleatColor','jerseyPrimary','jerseyAccent','helmetColor'])a[k]=color(a[k]);for(const k of ['skinName','cleatName','sizeName','jerseyName'])a[k]=escapeHTML(String(a[k]||'').slice(0,30));return{skinColor:a.skinColor||choice(skinTones).color,skinName:a.skinName||'Balanced',cleatColor:a.cleatColor||choice(cleatThemes).color,cleatName:a.cleatName||'Black',sizeName:a.sizeName||'Balanced',scaleX:Number.isFinite(Number(a.scaleX))?THREE.MathUtils.clamp(Number(a.scaleX),.85,1.16):1,scaleY:Number.isFinite(Number(a.scaleY))?THREE.MathUtils.clamp(Number(a.scaleY),.85,1.16):1,scaleZ:Number.isFinite(Number(a.scaleZ))?THREE.MathUtils.clamp(Number(a.scaleZ),.85,1.16):1,jerseyPrimary:a.jerseyPrimary||((offense?choice(offenseBlueThemes):choice(opponentUniformThemes)).primary),jerseyAccent:a.jerseyAccent||((offense?choice(offenseBlueThemes):choice(opponentUniformThemes)).accent),helmetColor:a.helmetColor||((offense?choice(offenseBlueThemes):choice(opponentUniformThemes)).helmet),jerseyName:a.jerseyName||(offense?'Blue Kit':'Road Kit')}}
const firstNames=['Jalen','Marcus','Darius','Tyrell','Devin','Malik','Tre','Andre','Jordan','Cameron','Nico','Xavier','Bryce','Kendrick','Zay','Keon','Jaylen','Trey','Calvin','Miles','Roman','Isaiah','Chris','Avery','Quentin','Darnell','Rashad','Elijah','Micah','Troy','Marvin','Donovan','Cedric','Terrance','Demarcus','Lance','Cole','Khalil','Tavian','Amari','Jaxon','Noah','Corey','Brandon','Desmond','Sterling','Jamal','Dante','Marlon','Reggie'];
const lastNames=['Banks','Mercer','Coleman','Vaughn','Price','Holloway','Sims','Maddox','Bennett','Cross','Hayes','Rowe','McCall','Jefferson','Foster','Reed','Brooks','Knight','Wells','Parker','Grant','Ellis','Rhodes','Pierce','Hampton','Carter','Bishop','Stone','Fleming','Moss','Rivers','Daniels','Turner','Cobb','Murray','Fields','Dawson','Wallace','Sharp','Harris','Monroe','Burke','Gaines','Lewis','Watkins','Dean','Riley','Owens','Ford','Marshall'];
const archetypes=[
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
  p.appearance=buildReceiverAppearance();P.migratePlayer(p);
  const buildStrength={Big:7,Tall:2,Balanced:0,Compact:-3,Lean:-4}[p.appearance.sizeName]||0;p.strength=clampRating(p.strength+buildStrength);
  if(market)return P.recruit(p);
  p.price=Math.max(140,Math.round((145+Math.max(0,playerOverall(p)-48)*12+randInt(-35,55))/10)*10);return p;
}
function freshMarket(){return Array.from({length:5},()=>newReceiver(true))}
function defaultFranchise(){return{cash:250,round:1,wins:0,team:Array.from({length:4},()=>newReceiver(false)),market:freshMarket()}}
function fallbackReceiverStrength(p){const arch=archetypes.find(a=>a.name===p?.archetype),sizeBonus={Big:7,Tall:2,Balanced:0,Compact:-3,Lean:-4}[p?.appearance?.sizeName]||0,catchInfluence=((Number(p?.catching)||50)-50)*.08;return clampRating((arch?.b?.strength||65)+sizeBonus+catchInfluence)}
function normalizeReceiver(p){if(!p||typeof p!=='object')return newReceiver(false);for(const k of ['speed','cutting','turning','evasion','catching'])p[k]=clampRating(Number(p[k])||50);p.strength=Number.isFinite(Number(p.strength))?clampRating(Number(p.strength)):fallbackReceiverStrength(p);p.trainings=Math.max(0,Number(p.trainings)||0);p.name=String(p.name||'').slice(0,80)||`${firstNames[randInt(0,firstNames.length-1)]} ${lastNames[randInt(0,lastNames.length-1)]}`;p.archetype=String(p.archetype||'Prospect').slice(0,40);p.id=p.id||`wr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,8)}`;p.price=Math.max(100,Number(p.price)||200);p.appearance=normalizeAppearance(p.appearance,true);P.migratePlayer(p);return p}
let localSaveAvailable=true;
function loadFranchise(){try{const raw=localStorage.getItem(SAVE_KEY);if(!raw)return defaultFranchise();const f=JSON.parse(raw);f.cash=Math.max(0,Number(f.cash)||0);f.round=Math.max(1,Number(f.round)||1);f.wins=Math.max(0,Number(f.wins)||0);f.team=(Array.isArray(f.team)?f.team:[]).slice(0,4).map(normalizeReceiver);while(f.team.length<4)f.team.push(newReceiver(false));f.market=(Array.isArray(f.market)?f.market:[]).slice(0,5).map(normalizeReceiver);while(f.market.length<5)f.market.push(newReceiver(true));return f}catch(err){localSaveAvailable=false;return defaultFranchise()}}
let franchise;
try{franchise=slots[activeSlot]?P.validateSave(slots[activeSlot].data):loadFranchise()}catch{franchise=loadFranchise()}
franchise.team.forEach(normalizeReceiver);franchise.market.forEach(normalizeReceiver);
function saveFranchise(){
  const stamp=Date.now();franchise.updatedAt=stamp;
  slots[activeSlot]={name:slots[activeSlot]?.name||`Franchise ${activeSlot+1}`,updatedAt:stamp,data:JSON.parse(JSON.stringify(franchise))};
  try{localStorage.setItem(SLOT_KEY,JSON.stringify(slots));localStorage.setItem('receiverWindowQB_activeSlot',String(activeSlot));localSaveAvailable=true}catch{localSaveAvailable=false}
  updateSavePill();renderMenu();
}
function updateSavePill(){const el=$('savePill');if(el){el.textContent=localSaveAvailable?'LOCAL SAVE ✓':'LOCAL SAVE FAILED';el.style.color=localSaveAvailable?'#bdf8d2':'#ff8c91'}}
let currentDefense=defenses[0],currentOpponentUniform=opponentUniformForRound(franchise.round),receivers=[],defenders=[];
let tournamentStage=franchise.round-1,seriesOffense=0,seriesDefense=0,playNumber=1,score=0,catches=0,drops=0,ints=0,playState='dead';
let selectedRosterIndex=0,managerLocked=false,audibleReceiverIndex=null;
let ballVel=new THREE.Vector3(),ballPrev=new THREE.Vector3(),ballLive=false,throwTime=0,spiralQuality=1,duckPhase=0,lastTime=performance.now(),snapTime=0,nextCount=0,charging=false,chargeStart=0,chargePower=0,messageTimer=0,resultFlashTimer=0;
let throwClock=8,loftBias=0,keyLoft=false,keyBullet=false;
const predictedLanding=new THREE.Vector3();let predictedFlightTime=0;
let ballSpotYards=0,down=1,lineToGainYards=25,snapSpotYards=0,ballCarrier=null,tackler=null,tackleTimer=0,tackleSpotYards=0;
function worldZForYards(yards){return FIELD_START_Z-THREE.MathUtils.clamp(yards,0,50)*WORLD_PER_YARD}
function yardsForWorldZ(z){return THREE.MathUtils.clamp((FIELD_START_Z-z)/WORLD_PER_YARD,0,50)}
function downLabel(){return down===1?'1st':down===2?'2nd':down===3?'3rd':'4th'}

function statLabel(k){return({speed:'SPD',cutting:'CUT',turning:'TRN',evasion:'EVA',catching:'CAT',strength:'STR',athleticism:'ATH',size:'SIZE',tricks:'TRICK'})[k]}
function previewFigureHTML(a){return `<div class="playerVisual"><div class="miniPlayer" style="transform:translateX(-50%) scale(${a.scaleX},${a.scaleY})"><div class="miniHelmet" style="background:${a.helmetColor}"></div><div class="miniHead" style="background:${a.skinColor}"></div><div class="miniBody" style="background:${a.jerseyPrimary}"><span class="miniStripe" style="background:${a.jerseyAccent}"></span></div><div class="miniArm left" style="background:${a.skinColor}"></div><div class="miniArm right" style="background:${a.skinColor}"></div><div class="miniLeg left" style="background:${a.jerseyPrimary}"></div><div class="miniLeg right" style="background:${a.jerseyPrimary}"></div><div class="miniCleat left" style="background:${a.cleatColor}"></div><div class="miniCleat right" style="background:${a.cleatColor}"></div></div></div>`}
function lookSummary(p){const a=p.appearance;return `Look: ${a.sizeName} build · ${a.skinName} skin · ${a.cleatName} cleats`}
function swatchLineHTML(a){return `<div class="swatchLine"><span class="swatchGroup"><span class="swatch" style="background:${a.jerseyPrimary}"></span>Jersey</span><span class="swatchGroup"><span class="swatch" style="background:${a.jerseyAccent}"></span>Accent</span><span class="swatchGroup"><span class="swatch" style="background:${a.helmetColor}"></span>Helmet</span><span class="swatchGroup"><span class="swatch" style="background:${a.cleatColor}"></span>Cleats</span></div>`}
function trainingCost(p){return 90+p.trainings*55}
function rosterCardHTML(p,i){const cost=trainingCost(p),max=p.trainings>=5,a=p.appearance;return `<div class="playerCard ${i===selectedRosterIndex?'selected':''}" data-roster="${i}"><div class="playerHeader">${previewFigureHTML(a)}<div class="playerIdentity"><div class="playerName">${['X','H','Y','Z'][i]} · ${escapeHTML(p.name)}</div><div class="playerSub">${escapeHTML(p.archetype)} · OVR ${playerOverall(p)} · ${Object.values(p.trainingByStat).reduce((a,b)=>a+b,0)} sessions</div><div class="lookLine">${lookSummary(p)}</div>${swatchLineHTML(a)}</div></div><div class="statRow"><span>Speed</span><b>${p.speed}</b><span>Cutting</span><b>${p.cutting}</b><span>Turning</span><b>${p.turning}</b><span>Evasion</span><b>${p.evasion}</b><span>Catch</span><b>${p.catching}</b><span>Strength</span><b>${p.strength}</b><span>Athleticism</span><b>${p.athleticism}</b><span>Size</span><b>${p.size}</b><span>Tricks</span><b>${p.tricks}</b></div><div class="trainGrid">${P.stats.map(k=>`<button data-train-slot="${i}" data-stat="${k}" ${p.trainingByStat[k]>=5?'disabled':''} title="$${90+p.trainingByStat[k]*55} · ${p.trainingByStat[k]}/5 sessions">+ ${statLabel(k)}</button>`).join('')}</div><div class="costLine">Training $90–310 per attribute · 5 sessions each · Prestige ${p.prestige||0}</div><button data-prestige="${i}" ${P.stats.some(k=>p.trainingByStat[k]>=5)?'':'disabled'}>PRESTIGE · ${P.prestigeCost(p).toLocaleString()}</button><div class="costLine">Keep ratings. Renew five sessions per attribute. Next prestige costs more.</div></div>`}
function marketCardHTML(p,i){const a=p.appearance;return `<div class="playerCard"><div class="playerHeader">${previewFigureHTML(a)}<div class="playerIdentity"><div class="playerName">${escapeHTML(p.name)}</div><div class="playerSub">${escapeHTML(p.archetype)} · OVR ${playerOverall(p)}</div><div class="lookLine">${lookSummary(p)}</div>${swatchLineHTML(a)}</div></div><div class="statRow"><span>Speed</span><b>${p.speed}</b><span>Cutting</span><b>${p.cutting}</b><span>Turning</span><b>${p.turning}</b><span>Evasion</span><b>${p.evasion}</b><span>Catch</span><b>${p.catching}</b><span>Strength</span><b>${p.strength}</b><span>Athleticism</span><b>${p.athleticism}</b><span>Size</span><b>${p.size}</b><span>Tricks</span><b>${p.tricks}</b></div><div class="costLine">${escapeHTML(p.rarity||'Prospect')} · Signing cost: ${p.price}</div><button class="signBtn" data-sign="${i}">SIGN · REPLACE ${['X','H','Y','Z'][selectedRosterIndex]}</button></div>`}
function renderManager(){$('cashPill').textContent=`$${franchise.cash}`;$('roundPill').textContent=`Round ${franchise.round} · ${franchise.wins} wins`;$('rosterGrid').innerHTML=franchise.team.map(rosterCardHTML).join('');$('marketGrid').innerHTML=franchise.market.map(marketCardHTML).join('');$('managerStatus').dataset.uniform=`Next opponent uniform: ${opponentUniformForRound(franchise.round).name}`;updateSavePill();document.querySelectorAll('[data-roster]').forEach(el=>el.addEventListener('click',()=>{selectedRosterIndex=Number(el.dataset.roster);renderManager()}));document.querySelectorAll('[data-train-slot]').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();trainReceiver(Number(btn.dataset.trainSlot),btn.dataset.stat)}));document.querySelectorAll('[data-prestige]').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();prestigeReceiver(Number(btn.dataset.prestige))}));document.querySelectorAll('[data-sign]').forEach(btn=>btn.addEventListener('click',()=>signReceiver(Number(btn.dataset.sign))))}
function prestigeReceiver(slot){const p=franchise.team[slot];if(!p)return;const result=P.prestige(p,franchise.cash);if(!result.ok){$('managerStatus').textContent=result.reason;return}franchise.cash=result.cash;$('managerStatus').textContent=`${p.name} prestiged! Five sessions per attribute renewed; ratings retained.`;saveFranchise();renderManager()}
function trainReceiver(slot,stat){const p=franchise.team[slot];if(!p)return;const result=P.train(p,stat,franchise.cash,randInt(4,8));if(!result.ok){$('managerStatus').textContent=result.reason;return}franchise.cash=result.cash;$('managerStatus').textContent=`${p.name}: ${statLabel(stat)} +${result.gain}.`;saveFranchise();renderManager()}
function signReceiver(index){const p=franchise.market[index];if(!p)return;if(franchise.cash<p.price){$('managerStatus').textContent=`You need $${p.price-franchise.cash} more to sign ${escapeHTML(p.name)}.`;return}const old=franchise.team[selectedRosterIndex];franchise.cash-=p.price;franchise.team[selectedRosterIndex]=p;franchise.market[index]=newReceiver(true);$('managerStatus').textContent=`Signed ${escapeHTML(p.name)} to ${['X','H','Y','Z'][selectedRosterIndex]}, replacing ${old.name}.`;saveFranchise();renderManager()}
function openManager(status='Manage your four starters before the next matchup.',locked=false){managerLocked=locked;playState=locked?'manager':playState;document.exitPointerLock?.();$('managerStatus').textContent=status;$('managerLayer').style.display='flex';$('continueBtn').textContent=locked?'CONTINUE TO NEXT MATCHUP':'RETURN TO FIELD';renderManager()}
function closeManager(){$('cloudOffer').hidden=true;if(managerLocked){managerLocked=false;playState='dead';$('managerLayer').style.display='none';resetDrive();setupPlay(true)}else {$('managerLayer').style.display='none';if(!menuOpen)setupPlay(false)}}
function endMatchup(won){
  const firstMatch=!franchise.wins&&!franchise.losses&&franchise.round===1;
  const offerCloud=firstMatch&&!franchise.cloudSaveOffered;
  const opp=opponentForRound(franchise.round),payout=P.payout(won,franchise.round,seriesOffense);
  franchise.cash+=payout;
  if(won){franchise.wins++;franchise.round++;}else franchise.losses=(franchise.losses||0)+1;
  tournamentStage=franchise.round-1;
  const status=`MATCHUP ${won?'WON':'LOST'} vs ${opp.name} · +$${payout}${won?' victory':' participation'} & scoring payout. ${won?'Next':'Retry'} Round ${franchise.round}.`;
  seriesOffense=0;seriesDefense=0;resetDrive();franchise.market=freshMarket();checkpoint();
  scheduleResult(()=>{
    openManager(status,true);
    if(offerCloud){franchise.cloudSaveOffered=true;saveFranchise();$('cloudOffer').hidden=false;}
  },1400);
}

function resetDrive(){ballSpotYards=0;down=1;lineToGainYards=25;snapSpotYards=0;ballCarrier=null;tackler=null;tackleTimer=0;tackleSpotYards=0}
function updateFieldMarkers(){losLine.position.z=worldZForYards(ballSpotYards);gainLine.position.z=worldZForYards(lineToGainYards);gainLine.visible=lineToGainYards<50;}


function pathFor(type,start){const x=start.x,z=start.z;switch(type){
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
function routePosition(points,distance){let left=distance;for(let i=0;i<points.length-1;i++){const a=points[i],b=points[i+1],len=a.distanceTo(b);if(left<=len)return a.clone().lerp(b,left/len);left-=len}const a=points[points.length-2],b=points[points.length-1],dir=b.clone().sub(a).normalize();return b.clone().addScaledVector(dir,left)}
function clearPlayers(){for(const a of [...receivers,...defenders]){scene.remove(a.mesh);const geos=new Set(),mats=new Set();a.mesh.traverse(o=>{if(o.geometry)geos.add(o.geometry);if(o.material&&o.material!==shadowMat)mats.add(o.material)});geos.forEach(g=>g.dispose());mats.forEach(m=>m.dispose())}receivers=[];defenders=[]}
function currentSkill(){return opponentForRound(franchise.round).skill}
function chooseDefense(){const opp=opponentForRound(franchise.round),allowed=opp.schemes,pool=defenses.filter(d=>allowed.includes(d.id));currentDefense=pool[Math.floor(Math.random()*pool.length)];currentOpponentUniform=opponentUniformForRound(franchise.round);$('defenseName').textContent=`R${franchise.round} · ${opp.name} · ${currentDefense.name}`;$('defenseDesc').textContent=`${currentDefense.desc} Uniform: ${currentOpponentUniform.name}.`}
function resetAim(){yaw=0;pitch=-.08;camera.position.set(0,2.25,worldZForYards(ballSpotYards)+14);applyCamera()}
function setupPlay(increment=false){
  if(increment)playNumber++;transition=null;replayFrames=[];replayRecording=false;replayEligible=false;applyFieldTheme();clearPlayers();clearRouteVisuals();ball.visible=false;ballLive=false;ballCarrier=null;tackler=null;tackleTimer=0;arcLine.visible=false;landRing.visible=false;charging=false;chargePower=0;loftBias=0;throwClock=8;predictedFlightTime=0;updateCharge();updateThrowClock();resetAim();chooseDefense();updateFieldMarkers();
  const xs=[-18,-6,6,18],play=plays[selectedPlay],skill=currentSkill(),losZ=worldZForYards(ballSpotYards);
  for(let i=0;i<4;i++){
    const start=new THREE.Vector3(xs[i],0,losZ-.9+(i%2)*.35),profile=franchise.team[i],mesh=makePlayer(true,profile.appearance);const sizeScale=P.traits(profile).sizeScale;mesh.scale.multiplyScalar(sizeScale);mesh.position.copy(start);scene.add(mesh);
    const speed=THREE.MathUtils.lerp(6.72,7.92,P.effective(profile.speed)/100)+[.06,-.03,.02,.08][i],catchReach=THREE.MathUtils.lerp(.94,1.08,P.effective(profile.catching)/100);for(const arm of(mesh.userData.arms||[]))arm.scale.y=catchReach;
    receivers.push({mesh,label:['X','H','Y','Z'][i],profile,route:play.routes[i],path:pathFor(play.routes[i],start),speed,maxSpeed:speed,catchReach,distance:0,start,velocity:new THREE.Vector3(0,0,-speed*.15),impactVel:new THREE.Vector3(),heading:new THREE.Vector3(0,0,-1),history:[],shoveCooldown:0,shoveSlow:0,stagger:0,shoveAnim:0,jumpY:0,jumpVel:0,jumpCooldown:0,trackingBall:false,burst:0,catchPose:0,plantPose:0,comebackActive:false,comebackPlant:0,underthrowDifficulty:0,runPhase:Math.random()*Math.PI*2,runIntensity:0,hasBall:false});
  }
  receivers.forEach((r,i)=>{
    const m=makePlayer(false,buildDefenderAppearance(franchise.round));scene.add(m);let start;if(currentDefense.corner==='press'||currentDefense.corner==='allout')start=new THREE.Vector3(r.start.x+(Math.random()-.5)*.8,0,r.start.z-1.6);else start=new THREE.Vector3(r.start.x+(Math.random()-.5)*1.6,0,r.start.z-5.5-Math.random()*1.7);m.position.copy(start);
    defenders.push(makeDefender(m,'corner',i,skill));
  });
  let sets;
  if(currentDefense.safety==='robber')sets=[[0,losZ-18,'robber'],[-14,losZ-29,'safety']];
  else if(currentDefense.safety==='single')sets=[[0,losZ-30,'safety']];
  else if(currentDefense.safety==='aggressive')sets=[[0,losZ-24,'safety']];
  else sets=[[-12,losZ-28,'safety'],[12,losZ-28,'safety']];
  for(const [x,z,kind] of sets){const m=makePlayer(false,buildDefenderAppearance(franchise.round));m.position.set(x,0,Math.max(ENDZONE_BACK_Z+2,z));scene.add(m);defenders.push(makeDefender(m,kind,null,skill))}
  snapSpotYards=ballSpotYards;playState='call';snapTime=0;nextCount=0;audibleReceiverIndex=null;$('audiblePanel').style.display='none';drawRouteVisuals();$('headline').textContent='CALL THE PLAY';$('detail').textContent=`${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)} from the ${Math.round(50-ballSpotYards)}. Selected: ${plays[selectedPlay].name}. Tap a receiver or press X / H / Y / Z to audible.`;$('playCallPanel').style.display='block';$('snapBtn').disabled=false;renderRoutes();updateScore();updatePlayButtons();checkpoint();
}
function beginCountdown(){
  if(inputBlocked()||playState!=='call')return;closeAudible(true);playState='countdown';snapTime=gameTime+2200;nextCount=3;$('snapBtn').disabled=true;showMessage('READY',`${plays[selectedPlay].name} locked in — routes stay drawn until the snap.`,650);
}
function makeDefender(mesh,kind,target,skill){
  const growth=P.defenseProgress(franchise.round);
  const reaction=THREE.MathUtils.lerp(.34,.085,skill)+(Math.random()*.06-.03);const turnRate=THREE.MathUtils.lerp(3.7,7.3,skill);const accel=THREE.MathUtils.lerp(13,24,skill);const maxSpeed=THREE.MathUtils.lerp(7.4,9.05,skill)+(kind==='safety'?.15:0);
  const sizeName=mesh.userData?.appearance?.sizeName||'Balanced',sizeStrength={Big:8,Tall:3,Balanced:0,Compact:-3,Lean:-4}[sizeName]||0,strength=clampRating(50+skill*37+randInt(-9,9)+sizeStrength+(kind==='safety'?2:0));
  return{mesh,kind,target,strength,fakeUntil:0,velocity:new THREE.Vector3(),impactVel:new THREE.Vector3(),heading:new THREE.Vector3(0,0,-1),reaction:Math.max(.06,reaction),turnRate:turnRate+growth.turn,accel:accel+growth.accel,maxSpeed:maxSpeed+growth.speed,jumpVelocity:growth.jump,depth: growth.depth,smartDeep:Math.random()<growth.smartChance,hands:growth.hands,commit:0,lastDesired:new THREE.Vector3(),shoveCooldown:0,shoveSlow:0,stagger:0,shoveAnim:0,jumpY:0,jumpVel:0,jumpCooldown:0,swatPose:0,ballSeen:false,plantPose:0,runPhase:Math.random()*Math.PI*2,runIntensity:0};
}
function renderRoutes(){
  $('routes').innerHTML=`<div id="playName">${plays[selectedPlay].name} · AUDIBLES ON</div>`+receivers.map((r,i)=>`<div class="routeRow audibleTap" data-audible-receiver="${i}"><span class="routeName">${r.label} · ${escapeHTML(r.profile.name.split(' ')[0])}</span><span class="routeType">${r.route}${r.audibled?' *':''} · OVR ${playerOverall(r.profile)}</span></div>`).join('');
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
  if(playState!=='call'||audibleReceiverIndex==null||!routePool.includes(route))return;const r=receivers[audibleReceiverIndex],slot=audibleReceiverIndex;r.route=route;r.path=pathFor(route,r.start);r.audibled=true;drawRouteVisuals();renderRoutes();
  showMessage('AUDIBLE SET',`${r.label} · ${r.profile.name.split(' ')[0]} → ${route}.`,900);closeAudible(true);
}
function findReceiverAtScreen(clientX,clientY){
  if(playState!=='call'||!receivers.length)return-1;let best=-1,bestDist=Infinity;for(let i=0;i<receivers.length;i++){const p=receivers[i].mesh.position.clone();p.y+=1.15;p.project(camera);if(p.z<-1||p.z>1)continue;const sx=(p.x*.5+.5)*innerWidth,sy=(-p.y*.5+.5)*innerHeight,dist=Math.hypot(clientX-sx,clientY-sy);if(dist<bestDist){bestDist=dist;best=i}}const radius=matchMedia('(pointer:coarse)').matches?76:50;return bestDist<=radius?best:-1;
}
function updateThrowClock(){const el=$('throwClock');el.textContent=`THROW CLOCK · ${throwClock.toFixed(1)}`;el.classList.toggle('urgent',throwClock<=2.25&&playState==='live')}
function updateScore(){const toGo=Math.max(0,Math.ceil(lineToGainYards-ballSpotYards)),opp=opponentForRound(franchise.round);$('score').textContent=`${seriesOffense}–${seriesDefense}`;$('tournamentLine').textContent=`Round ${franchise.round} · Best of 5 · ${opp.name} · $${franchise.cash}`;$('subscore').textContent=`${downLabel()} & ${toGo} · Ball: ${Math.round(50-ballSpotYards)} yd line · Drive ${Math.round(ballSpotYards)}/50 · ${catches} catches · ${ints} INT`}
function showMessage(head,detail,ms=1300){$('headline').textContent=head;$('detail').textContent=detail||'';messageTimer=gameTime+ms}
function flashResult(text,good=true,ms=900){const el=$('resultFlash');el.textContent=text;el.className=`${good?'good':'bad'} show`;resultFlashTimer=gameTime+ms}
function updatePlayButtons(){[...document.querySelectorAll('.playBtn')].forEach((b,i)=>b.classList.toggle('selected',i===selectedPlay))}
function buildPlayButtons(){plays.forEach((p,i)=>{const b=document.createElement('button');b.className='playBtn';b.textContent=`${i+1}. ${escapeHTML(p.name)}`;b.title=`Desktop shortcut: ${i+1}`;b.addEventListener('click',e=>{e.stopPropagation();if(playState!=='call')return;selectedPlay=i;setupPlay(false)});$('playGrid').appendChild(b)});updatePlayButtons()}
buildPlayButtons();

// Receivers run with real velocity/acceleration, so contact changes their path and momentum instead of being erased next frame.
function getBallLanding(pos=ball.position,vel=ballVel){
  if(!ballLive)return null;const y=Math.max(.02,pos.y);const disc=vel.y*vel.y+2*9.81*y;const t=(vel.y+Math.sqrt(Math.max(0,disc)))/9.81;if(!isFinite(t)||t<=0)return null;return{point:new THREE.Vector3(pos.x+vel.x*t,.02,pos.z+vel.z*t),time:t};
}
function updateJump(a,dt,shouldJump=false){
  a.jumpCooldown=Math.max(0,(a.jumpCooldown||0)-dt);if(shouldJump&&a.jumpY<.03&&a.jumpCooldown<=0){a.jumpVel=a.profile?P.traits(a.profile).jumpVelocity:(a.jumpVelocity||3.25);a.jumpCooldown=.75}
  if(a.jumpY>0||a.jumpVel>0){a.jumpVel-=9.81*dt;a.jumpY=Math.max(0,a.jumpY+a.jumpVel*dt);if(a.jumpY<=0){a.jumpY=0;a.jumpVel=0}}
  a.mesh.position.y=a.jumpY;
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
  let best=null,inStride=null;
  for(let t=.01;t<=maxT;t+=.04){
    const p=ball.position.clone().addScaledVector(ballVel,t);p.y-=.5*9.81*t*t;
    if(p.y<.24||p.y>P.traits(r.profile).highReach)continue;
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
    const inReceiverNeighborhood=routeGap<11.8||futureGap<14.8||(flatDist<7.0&&routeGap<16.5);
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
    if(point.y>=.1&&point.y<=P.traits(r.profile).highReach){
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
  if(!a.profile||!ballLive||!a.trackingBall||a.hasBall)return null;
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
    const offset=wanted.clone().sub(shoulder),maxReach=.95;
    if(offset.length()>maxReach)offset.setLength(maxReach);
    wanted.copy(shoulder).add(offset);
    const delta=wanted.clone().sub(a.handTargets[i]),step=Math.max(0,dt)*8;
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
    body.position.y=1.18-move*.18;}
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
    rig.rotation.z=side*move*.16+(a.fakeSide||1)*stumble*.25;
    rig.position.y=-move*.12-stumble*.10;
    if(fooled){for(let i=0;i<arms.length;i++)arms[i].rotation.z+=(i===0?-1:1)*stumble*.7;}
  }
  // Distinct one-arm swat, two-hand high point, and secured-ball tuck.
  for(let i=0;i<hands.length;i++){
    if(swat){hands[i].position.y+=i===0?.22:-.14;hands[i].position.x+=i===0?-.14:.18;}
    if(a.hasBall){hands[i].position.set(i===0?.12:.42,1.18,.32);arms[i].rotation.x=-.75;arms[i].position.y=1.18;}
  }
  trackReceiverHands(a,dt);
}

function actorStrength(a){return THREE.MathUtils.clamp(a?.profile?(P.effective(a.profile.strength)||50)/100+P.traits(a.profile).bodyBonus:(a?.strength||50)/100,.01,1)}
function nearestCornerContact(r,maxDist=1.24){let best=null,bestDist=maxDist;for(const d of defenders){if(d.kind!=='corner')continue;const dist=Math.hypot(d.mesh.position.x-r.mesh.position.x,d.mesh.position.z-r.mesh.position.z);if(dist<bestDist){best=d;bestDist=dist}}return best?{player:best,dist:bestDist}:null}
function receiverContactFactors(r){const hit=nearestCornerContact(r);if(!hit)return{speed:1,accel:1,edge:0};const edge=actorStrength(r)-actorStrength(hit.player);return{speed:THREE.MathUtils.clamp(.92+edge*.42,.66,1.03),accel:THREE.MathUtils.clamp(.82+edge*.42,.54,1.04),edge}}
function contestedStrengthBonus(receiver,defender,distance){if(!receiver||!defender||distance>=1.45)return 0;const rs=actorStrength(receiver),ds=actorStrength(defender),closeness=distance<.85?1:.62;return THREE.MathUtils.clamp(((rs-ds)*.24+(rs-.5)*.06+(receiver.profile?Math.max(0,P.effective(receiver.profile.size)-50)*.001:0))*closeness,-.11,.10)}

function updateReceiver(r,dt,now){
  updateTricks(r,dt,false);
  r.shoveCooldown=Math.max(0,r.shoveCooldown-dt);r.shoveSlow=Math.max(0,r.shoveSlow-dt);r.stagger=Math.max(0,r.stagger-dt);r.burst=Math.max(0,(r.burst||0)-dt);r.runIntensity=THREE.MathUtils.lerp(r.runIntensity||0,.78,Math.min(1,dt*10));r.runPhase=(r.runPhase||0)+dt*Math.max(6,r.velocity.length()*2.25);r.catchPose=Math.max(0,(r.catchPose||0)-dt*4.8);r.comebackPlant=Math.max(0,(r.comebackPlant||0)-dt);r.plantPose=r.comebackPlant>0?Math.min(1,r.comebackPlant/.18):Math.max(0,(r.plantPose||0)-dt*5.5);
  if(!ballLive){r.ballPursuit=null;r.comebackActive=false;r.comebackPlant=0;r.underthrowDifficulty=0}
  r.distance+=r.speed*dt;let target=routePosition(r.path,r.distance+1.45);r.trackingBall=false;let stridePlan=null;
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
  const desired=target.clone().sub(r.mesh.position);desired.y=0;if(desired.lengthSq()>.0001)desired.normalize();else desired.copy(r.heading);
  const current=r.heading.clone().normalize(),angle=Math.acos(THREE.MathUtils.clamp(current.dot(desired),-1,1)),cross=current.x*desired.z-current.z*desired.x,sign=cross<0?-1:1,turnRating=P.effective((r.profile?.turning||50))/100,turnRateBase=r.comebackPlant>0?2.25:r.comebackActive?8.8:(r.trackingBall?12.6:7.7),turnRate=turnRateBase*THREE.MathUtils.lerp(.76,1.22,turnRating),turn=Math.min(angle,turnRate*dt)*sign,c=Math.cos(turn),ss=Math.sin(turn);r.heading.set(current.x*c-current.z*ss,0,current.x*ss+current.z*c).normalize();
  const contact=receiverContactFactors(r),cutPenalty=THREE.MathUtils.clamp(angle/(Math.PI*.7),0,1),cutRating=P.effective((r.profile?.cutting||50))/100,slow=(r.shoveSlow>0?.68:1)*(r.stagger>0?.78:1),burst=r.trackingBall?P.traits(r.profile).pursuitBurst:1,plantSlow=r.comebackPlant>0?.27:1,speedCutLoss=THREE.MathUtils.lerp(.34,.16,cutRating),accelCutLoss=THREE.MathUtils.lerp(.40,.16,cutRating),desiredSpeed=(stridePlan?Math.min(stridePlan.strideSpeed,r.maxSpeed*burst):r.maxSpeed*burst)*(1-speedCutLoss*cutPenalty)*slow*plantSlow*contact.speed,desiredVel=r.heading.clone().multiplyScalar(desiredSpeed),accel=(r.trackingBall?34:20)*(1-accelCutLoss*cutPenalty)*(r.stagger>0?.62:1)*contact.accel,maxDv=accel*dt,dv=desiredVel.sub(r.velocity);if(dv.length()>maxDv)dv.setLength(maxDv);r.velocity.add(dv);if(r.comebackPlant>0)r.velocity.multiplyScalar(Math.pow(.16,dt));r.mesh.position.addScaledVector(r.velocity,dt);r.mesh.position.addScaledVector(r.impactVel,dt);r.impactVel.multiplyScalar(Math.pow(.055,dt));
  const face=Math.atan2(r.heading.x,r.heading.z),delta=Math.atan2(Math.sin(face-r.mesh.rotation.y),Math.cos(face-r.mesh.rotation.y));r.mesh.rotation.y+=THREE.MathUtils.clamp(delta,-11.5*dt,11.5*dt);
  let shouldJump=false;if(ballLive){const catchPoint=r.mesh.position.clone().add(new THREE.Vector3(0,1.92*r.mesh.scale.y,0)),approach=ballApproach(catchPoint,.9);if(r.trackingBall&&approach&&approach.time<.58&&approach.dist<1.52)r.catchPose=1;const h=Math.hypot(ball.position.x-r.mesh.position.x,ball.position.z-r.mesh.position.z);shouldJump=r.trackingBall&&h<1.72&&ball.position.y>1.72&&ball.position.y<P.traits(r.profile).highReach&&ballVel.y<3.9}updateJump(r,dt,shouldJump);
  animatePlayerContact(r,dt);const ring=r.mesh.userData.trackRing;if(ring){ring.visible=r.trackingBall;ring.rotation.z+=dt*2.8}
  r.history.push({t:now,p:r.mesh.position.clone(),v:r.velocity.clone()});while(r.history.length&&now-r.history[0].t>900)r.history.shift();
}

function observedReceiver(r,delayMs,now){let s=r.history[0]||{p:r.mesh.position,v:r.velocity};for(let i=r.history.length-1;i>=0;i--){if(r.history[i].t<=now-delayMs){s=r.history[i];break}}return s}
function getDeepThreat(side,delay,now){let best=null;for(const r of receivers){const o=observedReceiver(r,delay,now);if(side===0||Math.sign(o.p.x||side)===side||Math.abs(o.p.x)<5){if(!best||o.p.z<best.p.z)best=o}}return best}

// DB logic only uses delayed observed position/velocity and scheme leverage. It never reads route names or route paths.
function defenderTarget(d,now){
  if(d.fakeUntil>gameTime&&d.fakeTarget)return d.fakeTarget.clone();
  const delay=d.reaction*1000,skill=currentSkill();if(playState==='run'&&ballCarrier){const lead=ballCarrier.velocity.clone().multiplyScalar(.18+skill*.16);return ballCarrier.mesh.position.clone().add(lead)}
  if(d.kind==='corner'){
    const r=receivers[d.target],o=observedReceiver(r,delay,now);let target=o.p.clone(),v=o.v.clone();v.y=0;
    const speed=v.length();if(speed>.2)v.multiplyScalar(1/speed);
    if(currentDefense.corner==='press'){target.addScaledVector(v,.3+skill*.35);target.z-=.8}
    if(currentDefense.corner==='under'){target.z+=1.5;target.x*=.95}
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
    if(ballLive){const land=getBallLanding(),ballTarget=land?land.point:ball.position,toBall=ballTarget.clone().sub(d.mesh.position),range=THREE.MathUtils.lerp(8.5,18.0,skill);if(toBall.length()<range||throwTime>THREE.MathUtils.lerp(.65,.20,skill))target.lerp(ballTarget,THREE.MathUtils.lerp(.28,.78,skill))}
    return target;
  }
  if(d.kind==='safety'){
    const side=d.mesh.position.x<0?-1:1,th=getDeepThreat(side,delay,now);if(!th)return d.mesh.position.clone();let target=th.p.clone();
    if(currentDefense.safety==='patient'&&!ballLive){target.x=THREE.MathUtils.clamp(target.x,-20,20);target.z=Math.min(-8,target.z-7.2)}
    else if(currentDefense.safety==='aggressive'&&!ballLive){target.x*=.45;target.z=Math.min(-7,target.z-4.5)}
    else if(ballLive){const land=getBallLanding();target.lerp(land?land.point:ball.position,THREE.MathUtils.lerp(.45,.84,skill))}else{target.x*=.72;target.z-=6}
    return target;
  }
  if(d.kind==='robber'){
    if(ballLive){const land=getBallLanding();return land?land.point.clone():ball.position.clone();}
    // The robber reads QB gaze with delay, plus actual inside receiver movement—not the called play.
    const look=new THREE.Vector3();camera.getWorldDirection(look);const gazeX=THREE.MathUtils.clamp(camera.position.x+look.x*28,-9,9);let inside=0,count=0;
    for(const r of receivers){const o=observedReceiver(r,delay,now);if(Math.abs(o.p.x)<12){inside+=o.p.x;count++}}
    return new THREE.Vector3(THREE.MathUtils.lerp(gazeX,count?inside/count:0,.35),0,-6);
  }
  return d.mesh.position.clone();
}
function updateDefender(d,target,dt){
  d.shoveCooldown=Math.max(0,d.shoveCooldown-dt);d.shoveSlow=Math.max(0,d.shoveSlow-dt);d.stagger=Math.max(0,d.stagger-dt);d.swatPose=Math.max(0,(d.swatPose||0)-dt*5.2);d.ballSeen=defenderSeesBall(d);const pos=d.mesh.position,desired=target.clone().sub(pos);desired.y=0;if(desired.lengthSq()<.002){d.velocity.multiplyScalar(Math.pow(.1,dt));updateJump(d,dt,false);animatePlayerContact(d,dt);return}desired.normalize();
  const current=d.heading.clone().normalize();let angle=Math.acos(THREE.MathUtils.clamp(current.dot(desired),-1,1));
  // Planting: a DB pointed the wrong way cannot instantly rotate 90–180 degrees. Big changes reduce usable speed and acceleration.
  const cross=current.x*desired.z-current.z*desired.x,sign=cross<0?-1:1,maxTurn=d.turnRate*dt*(d.fakeUntil>gameTime?.45:1),turn=Math.min(angle,maxTurn)*sign;const c=Math.cos(turn),ss=Math.sin(turn);d.heading.set(current.x*c-current.z*ss,0,current.x*ss+current.z*c).normalize();
  const plantPenalty=THREE.MathUtils.clamp(angle/(Math.PI*.75),0,1);d.plantPose=plantPenalty>.48?THREE.MathUtils.clamp((plantPenalty-.48)*1.6,0,.85):Math.max(0,(d.plantPose||0)-dt*4.5);const slow=(d.shoveSlow>0?.67:1)*(d.stagger>0?.76:1),pursuitBoost=playState==='run'?1.22:1,targetSpeed=d.maxSpeed*pursuitBoost*(1-.50*plantPenalty)*slow,desiredVel=d.heading.clone().multiplyScalar(targetSpeed),maxDv=d.accel*(playState==='run'?1.18:1)*(1-.45*plantPenalty)*(d.stagger>0?.62:1)*dt,delta=desiredVel.sub(d.velocity);if(delta.length()>maxDv)delta.setLength(maxDv);d.velocity.add(delta);pos.addScaledVector(d.velocity,dt);pos.addScaledVector(d.impactVel,dt);d.impactVel.multiplyScalar(Math.pow(.055,dt));d.mesh.rotation.y=Math.atan2(d.heading.x,d.heading.z);
  d.runIntensity=THREE.MathUtils.lerp(d.runIntensity||0,playState==='run'?1:.78,Math.min(1,dt*11));d.runPhase=(d.runPhase||0)+dt*Math.max(playState==='run'?10:6,d.velocity.length()*(playState==='run'?3.0:2.2));let shouldJump=false;if(ballLive){const handTarget=pos.clone().add(new THREE.Vector3(0,2.00*d.mesh.scale.y,0)),approach=ballApproach(handTarget,.72);if(d.ballSeen&&approach&&approach.time<.42&&approach.dist<THREE.MathUtils.lerp(1.02,1.38,currentSkill()))d.swatPose=1;const h=Math.hypot(ball.position.x-pos.x,ball.position.z-pos.z);shouldJump=d.ballSeen&&h<1.50&&ball.position.y>1.80&&ball.position.y<2.2+(d.jumpVelocity*d.jumpVelocity)/(2*9.81)&&ballVel.y<3.5}updateJump(d,dt,shouldJump);animatePlayerContact(d,dt);
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
    const A=all[a],B=all[b],dx=B.mesh.position.x-A.mesh.position.x,dz=B.mesh.position.z-A.mesh.position.z,dist=Math.hypot(dx,dz),min=.40*(A.mesh.scale.x+B.mesh.scale.x);if(dist>0&&dist<min){const overlap=min-dist,nx=dx/dist,nz=dz/dist,crossTeam=!!A.profile!==!!B.profile,sA=crossTeam?actorStrength(A):.5,sB=crossTeam?actorStrength(B):.5,total=Math.max(.01,sA+sB),moveA=overlap*(sB/total),moveB=overlap*(sA/total);A.mesh.position.x-=nx*moveA;A.mesh.position.z-=nz*moveA;B.mesh.position.x+=nx*moveB;B.mesh.position.z+=nz*moveB;if(A.velocity)A.velocity.multiplyScalar(THREE.MathUtils.lerp(.98,.89,moveA/Math.max(.001,overlap)));if(B.velocity)B.velocity.multiplyScalar(THREE.MathUtils.lerp(.98,.89,moveB/Math.max(.001,overlap)))}
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
  const pts=[];let landing=null;for(let t=0;t<=4.6;t+=.075){const p=origin.clone().addScaledVector(v,t);p.y-=.5*9.81*t*t;pts.push(p);if(p.y<=.08&&t>.08){landing=p.clone();landing.y=.02;break}if(p.z<-78||Math.abs(p.x)>38)break}if(pts.length>1){arcGeo.setFromPoints(pts);arcLine.visible=true}if(landing){landRing.position.copy(landing);landRing.visible=true}else landRing.visible=false;return landing;
}
function updateArcPreview(){
  if(!charging||playState!=='live'||ballLive){if(!ballLive){arcLine.visible=false;landRing.visible=false}return}const stats=throwStats(currentHeld()),v=idealLaunchVelocity(stats.power),origin=camera.position.clone().add(centerThrowDirection().multiplyScalar(1.05));drawTrajectory(origin,v);
}
function throwBall(){
  replayFrames=[];replayRecording=true;replayTailEnds=0;replaySample=0;throwDown=down;replayEligible=down===4;
  const held=currentHeld(),stats=throwStats(held);
  chargePower=stats.power;spiralQuality=stats.quality;duckPhase=Math.random()*Math.PI*2;
  const ideal=centerThrowDirection(),baseVel=idealLaunchVelocity(chargePower);
  // The real throw now uses the same center-line velocity as the green preview.
  // Release quality can affect vertical wobble in flight, but never adds hidden left/right aim error.
  const origin=camera.position.clone().add(ideal.clone().multiplyScalar(1.05));
  ball.position.copy(origin);ballPrev.copy(origin);ballVel.copy(baseVel);ball.visible=true;ballLive=true;throwTime=0;playState='thrown';
  const landing=drawTrajectory(origin,ballVel);if(landing){predictedLanding.copy(landing)}
  const shape=loftBias>.28?'LOFT':loftBias<-.28?'BULLET':'BALANCED';
  showMessage(spiralQuality>.86?'BALL OUT — TIGHT SPIRAL':'BALL OUT — WOBBLE',`${shape} · Power ${Math.round(chargePower*100)}% · Release ${Math.round(spiralQuality*100)}%`,600);
}
function pointSegmentDistance(point,a,b){const ab=b.clone().sub(a),den=ab.lengthSq();if(den<1e-8)return point.distanceTo(a);const t=THREE.MathUtils.clamp(point.clone().sub(a).dot(ab)/den,0,1);return point.distanceTo(a.clone().addScaledVector(ab,t))}
function nearestOpponentInfo(player,isDefense){let best=null,bestDist=99;const pool=isDefense?receivers:defenders;for(const o of pool){const dist=Math.hypot(o.mesh.position.x-player.mesh.position.x,o.mesh.position.z-player.mesh.position.z);if(dist<bestDist){best=o;bestDist=dist}}return{player:best,dist:bestDist}}
function nearestOpponentDistance(player,isDefense){return nearestOpponentInfo(player,isDefense).dist}
function raisedHandsDistance(player){
  const hands=player.mesh.userData.hands||[],arms=player.mesh.userData.arms||[];if(!hands.length&&!arms.length)return Infinity;player.mesh.updateMatrixWorld(true);let best=Infinity;for(const h of hands){const p=h.getWorldPosition(new THREE.Vector3());best=Math.min(best,pointSegmentDistance(p,ballPrev,ball.position))}for(const a of arms){const p=a.getWorldPosition(new THREE.Vector3());best=Math.min(best,pointSegmentDistance(p,ballPrev,ball.position)+.05)}return best;
}
function checkRaisedDefenderSwat(){
  const skill=currentSkill();for(const d of defenders){if(!d.ballSeen||(d.swatPose||0)<.22)continue;const dist=raisedHandsDistance(d);if(dist<.34){const batChance=THREE.MathUtils.clamp(.62+skill*.28+(dist<.20?.08:0),.58,.96);if(Math.random()<batChance){resolveDrop('PASS BROKEN UP','The defender saw the throw, got both arms into the passing lane, and batted it down.');return true}}}return false;
}

// Emergency diving catches are deliberately a SECONDARY catch system.
// The normal catch logic below always gets first priority. A receiver may only dive when:
// 1) the pass is low, 2) his old standing/jumping catch window would miss it, and
// 3) the ball passes through a small extra rescue band that is close enough to lunge for.
function pointSegmentDistanceXZ(point,a,b){
  const px=point.x,pz=point.z,ax=a.x,az=a.z,bx=b.x,bz=b.z,abx=bx-ax,abz=bz-az,den=abx*abx+abz*abz;
  if(den<1e-8)return Math.hypot(px-ax,pz-az);
  const t=THREE.MathUtils.clamp(((px-ax)*abx+(pz-az)*abz)/den,0,1),cx=ax+abx*t,cz=az+abz*t;
  return Math.hypot(px-cx,pz-cz);
}
function receiverNormalCatchDistance(r){
  const chest=r.mesh.position.clone().add(new THREE.Vector3(0,1.34*r.mesh.scale.y,0)),head=r.mesh.position.clone().add(new THREE.Vector3(0,1.82*r.mesh.scale.y,0));
  let dist=Math.min(pointSegmentDistance(chest,ballPrev,ball.position),pointSegmentDistance(head,ballPrev,ball.position)+.06);
  if((r.catchPose||0)>.18)dist=Math.min(dist,raisedHandsDistance(r));
  return dist;
}
function poseReceiverDive(r){
  const target=ball.position.clone();target.y=0;const dir=target.sub(r.mesh.position).setY(0);
  if(dir.lengthSq()>.001){dir.normalize();r.heading.copy(dir);r.mesh.rotation.y=Math.atan2(dir.x,dir.z);r.mesh.position.addScaledVector(dir,.42)}
  r.velocity.set(0,0,0);r.impactVel.set(0,0,0);r.catchPose=1;r.runIntensity=0;r.mesh.position.y=.30;r.mesh.rotation.x=-1.14;
  animatePlayerContact(r,0);
}
function resolveDivingCatch(r){
  const catchZ=ball.position.z;markCatch(r);
  ballLive=false;arcLine.visible=false;landRing.visible=false;predictedFlightTime=0;charging=false;playState='divecatch';catches++;r.hasBall=true;poseReceiverDive(r);ball.visible=true;const secured=new THREE.Vector3(0,1.95,.45);r.mesh.localToWorld(secured);ball.position.copy(secured);
  flashResult('DIVING CATCH!',true,900);showMessage('DIVING CATCH!',`${r.label} laid out for a low ball that was outside his normal catch window. He is down at the catch spot.`,1050);updateScore();
  transition={at:gameTime+700,fn:()=>finishPlayAtSpot(catchZ,'DIVING CATCH')};
}
function checkEmergencyDiveCatch(){
  // Keep dives rare and rescue-only: low ball + old catch window missed + only a modest extra reach band.
  if(!ballLive||ball.position.y<.14||ball.position.y>1.08)return false;
  let best=null,bestReach=Infinity,bestNormal=Infinity;
  for(const r of receivers){
    if(!r.trackingBall||r.jumpY>.08||r.stagger>0.22)continue;
    const catchRating=P.effective((r.profile?.catching||50))/100,normalRadius=THREE.MathUtils.lerp(.84,.96,catchRating),normalDist=receiverNormalCatchDistance(r);
    // If the old system could catch this, NEVER dive. Preserve the normal animation and normal catch odds.
    if(normalDist<=normalRadius+.035)continue;
    const groundPoint=r.mesh.position.clone();groundPoint.y=0;
    const rescueDist=pointSegmentDistanceXZ(groundPoint,ballPrev,ball.position);
    const rescueRadius=P.traits(r.profile).diveReach;if(rescueDist>rescueRadius)continue;
    const toBall=ball.position.clone().sub(r.mesh.position).setY(0),facing=toBall.lengthSq()>.001?r.heading.dot(toBall.clone().normalize()):1;
    // Allow slight comeback/sideways layouts, but no absurd full-speed dive directly backward.
    if(facing<-.58)continue;
    if(rescueDist<bestReach){best=r;bestReach=rescueDist;bestNormal=normalDist;best.rescueRadius=rescueRadius}
  }
  if(!best)return false;
  const contestInfo=nearestOpponentInfo(best,false),contest=contestInfo.dist,strengthFight=contestedStrengthBonus(best,contestInfo.player,contest)*.68,stretch=THREE.MathUtils.clamp(bestReach/(best.rescueRadius||1.62),0,1),catchBonus=(P.effective((best.profile?.catching||50))-50)/50*.055,heightDifficulty=THREE.MathUtils.clamp((ball.position.y-.18)/.90,0,1),contestPenalty=contest<.85?.20:contest<1.45?.09:0,comebackPenalty=best.comebackActive?(best.underthrowDifficulty||.08)*.45:0;
  // Intentionally much lower than the normal catch formula. STR helps only if a defender is actually contesting the dive.
  const catchChance=THREE.MathUtils.clamp(.66+(P.effective(best.profile.athleticism)-50)*.0012-stretch*.24-heightDifficulty*.05+spiralQuality*.08+catchBonus+strengthFight-contestPenalty-comebackPenalty-(best.stagger>0?.07:0),.22,.76);
  if(Math.random()<catchChance)resolveDivingCatch(best);else{poseReceiverDive(best);resolveDrop('DIVING DROP',`${best.label} laid out for a low ball outside the normal catch window but could not secure it${contest<1.45?' through contact':''}.`,true);}
  return true;
}
function checkBallContact(){
  if(checkRaisedDefenderSwat())return true;
  let best=null,bestDist=Infinity,isDef=false;
  for(const r of receivers){const chest=r.mesh.position.clone().add(new THREE.Vector3(0,1.34*r.mesh.scale.y,0)),head=r.mesh.position.clone().add(new THREE.Vector3(0,1.82*r.mesh.scale.y,0));let d=Math.min(pointSegmentDistance(chest,ballPrev,ball.position),pointSegmentDistance(head,ballPrev,ball.position)+.06);if((r.catchPose||0)>.18)d=Math.min(d,raisedHandsDistance(r));if(d<bestDist){bestDist=d;best=r;isDef=false}}
  for(const d of defenders){const chest=d.mesh.position.clone().add(new THREE.Vector3(0,1.34*d.mesh.scale.y,0)),head=d.mesh.position.clone().add(new THREE.Vector3(0,1.82*d.mesh.scale.y,0));let dist=Math.min(pointSegmentDistance(chest,ballPrev,ball.position),pointSegmentDistance(head,ballPrev,ball.position)+.06);if((d.swatPose||0)>.18)dist=Math.min(dist,raisedHandsDistance(d));if(dist<bestDist){bestDist=dist;best=d;isDef=true}}
  const skill=currentSkill(),catchRating=!isDef&&best?.profile?(P.effective(best.profile.catching)/100):.5,receiverRadius=best&&best.trackingBall?THREE.MathUtils.lerp(.91,1.06,catchRating):THREE.MathUtils.lerp(.74,.84,catchRating),radius=isDef?THREE.MathUtils.lerp(.54,.72,skill):receiverRadius;if(best&&bestDist<radius&&ball.position.y>.30&&ball.position.y<(isDef?3.38+(best.jumpY||0):P.traits(best.profile).highReach)){const contestInfo=nearestOpponentInfo(best,isDef),contest=contestInfo.dist;if(isDef){if(!best.ballSeen&&bestDist>.28)return false;const cleanHit=bestDist<.27,receiverPressure=contest<1.20&&contestInfo.player?Math.max(0,contestedStrengthBonus(contestInfo.player,best,contest))*.82:0;if(cleanHit&&contest>=.85)resolveCatch(best,true);else{const secureChance=THREE.MathUtils.clamp(.20+skill*.35+(best.hands||0)+(bestDist<.40?.10:0)+(cleanHit?.17:0)-(contest<1?.05:0)-receiverPressure,.12,.78);if(Math.random()<secureChance)resolveCatch(best,true);else resolveDrop('PASS BROKEN UP',receiverPressure>.025?'The receiver used his strength to fight through the defender at the catch point and prevent the takeaway.':'The defender reached into the catch window and knocked the ball away.');}}else{const placement=THREE.MathUtils.clamp(1-bestDist/radius,0,1),contestedPenalty=contest<.85?.24:contest<1.45?.10:0,strengthFight=contestedStrengthBonus(best,contestInfo.player,contest),jumpBonus=best.jumpY>.12?.03:0,comebackPenalty=best.comebackActive?(best.underthrowDifficulty||.10):0,handsBonus=(best.catchPose||0)>.55?.055:0,cleanOnBody=best.trackingBall&&bestDist<.30&&contest>=1.10;const catchSkillBonus=(P.effective((best.profile?.catching||50))-50)/50*.07,catchChance=cleanOnBody?1:THREE.MathUtils.clamp(.74+placement*.22+spiralQuality*.08+catchSkillBonus+strengthFight+jumpBonus+handsBonus-contestedPenalty-comebackPenalty-(best.stagger>0?.08:0),.18,.995);if(Math.random()<catchChance)resolveCatch(best,false);else resolveDrop(best.comebackActive?'TOUGH COMEBACK DROP':'DROP',`${best.label} ${best.comebackActive?'planted and came back to the underthrow but ':''}could not finish the catch${contest<1.45?' through contact':''}.`,true);}return true}
  // Nothing was catchable by the original standing/jumping system. Only now may a low-ball rescue dive be considered.
  return checkEmergencyDiveCatch();
}


function attachBallToCarrier(){if(!ballCarrier)return;const p=new THREE.Vector3(.34,1.18,.18);(ballCarrier.mesh.userData.visualRig||ballCarrier.mesh).localToWorld(p);ball.position.copy(p);ball.visible=true;ball.rotation.set(0,ballCarrier.mesh.rotation.y,Math.PI*.18);}
function updateBallCarrier(r,dt){
  updateTricks(r,dt,true);
  r.runIntensity=THREE.MathUtils.lerp(r.runIntensity||0,1,Math.min(1,dt*12));r.runPhase=(r.runPhase||0)+dt*Math.max(10,r.velocity.length()*2.65);r.catchPose=Math.max(0,(r.catchPose||0)-dt*3.5);r.plantPose=Math.max(0,(r.plantPose||0)-dt*5);
  const pos=r.mesh.position,goal=new THREE.Vector3(0,0,GOAL_LINE_Z-3),desired=goal.clone().sub(pos);desired.y=0;if(desired.lengthSq()<.001)desired.set(0,0,-1);desired.normalize();
  const evade=new THREE.Vector3();for(const d of defenders){const away=pos.clone().sub(d.mesh.position);away.y=0;const dist=away.length();if(dist>0&&dist<9){away.normalize();const ahead=d.mesh.position.z<pos.z?1.15:.72;evade.addScaledVector(away,(9-dist)/9*ahead)}}if(Math.abs(pos.x)>21)evade.x+=-Math.sign(pos.x)*1.6;const evadeRating=P.effective((r.profile?.evasion||50))/100;desired.addScaledVector(evade,THREE.MathUtils.lerp(.70,1.18,evadeRating)).normalize();
  const current=r.heading.clone().normalize(),angle=Math.acos(THREE.MathUtils.clamp(current.dot(desired),-1,1)),cross=current.x*desired.z-current.z*desired.x,sign=cross<0?-1:1,turnRating=P.effective((r.profile?.turning||50))/100,turn=Math.min(angle,THREE.MathUtils.lerp(7.0,10.2,turnRating)*dt)*sign,c=Math.cos(turn),s=Math.sin(turn);r.heading.set(current.x*c-current.z*s,0,current.x*s+current.z*c).normalize();
  const cutPenalty=THREE.MathUtils.clamp(angle/(Math.PI*.70),0,1),cutRating=P.effective((r.profile?.cutting||50))/100,targetSpeed=r.maxSpeed*.90*(1-THREE.MathUtils.lerp(.31,.15,cutRating)*cutPenalty),desiredVel=r.heading.clone().multiplyScalar(targetSpeed),dv=desiredVel.sub(r.velocity),maxDv=THREE.MathUtils.lerp(20,29,cutRating)*dt;if(dv.length()>maxDv)dv.setLength(maxDv);r.velocity.add(dv);pos.addScaledVector(r.velocity,dt);pos.addScaledVector(r.impactVel,dt);r.impactVel.multiplyScalar(Math.pow(.055,dt));pos.x=THREE.MathUtils.clamp(pos.x,-25.2,25.2);r.mesh.rotation.y=Math.atan2(r.heading.x,r.heading.z);animatePlayerContact(r,dt);attachBallToCarrier();
}
function finishSeries(offenseWon,headline,detail){
  endBall();if(offenseWon){flashResult('TOUCHDOWN!',true,1200);showMessage(headline||'TOUCHDOWN!',detail||'Drive complete.',1600)}else{flashResult('TURNOVER ON DOWNS',false,1200);showMessage(headline||'SERIES LOST',detail||'The defense stopped the drive.',1600)}
  resetDrive();if(!seriesResult(offenseWon))scheduleResult(()=>setupPlay(true),1450);
}
function finishPlayAtSpot(worldZ,reason='TACKLED'){
  captureReplay(true);replayRecording=false;playState='dead';
  const rawSpot=yardsForWorldZ(worldZ),newSpot=Math.min(50,Math.max(ballSpotYards+1,rawSpot)),gain=Math.max(0,newSpot-snapSpotYards);ballSpotYards=newSpot;ballCarrier=null;tackler=null;tackleTimer=0;ball.visible=false;
  if(ballSpotYards>=50){score+=7;finishSeries(true,'TOUCHDOWN!',`${Math.round(gain)} yards on the play. Three touchdowns win this best-of-5 matchup.`);return}
  if(ballSpotYards>=lineToGainYards-.01){const oldTarget=lineToGainYards;lineToGainYards=Math.min(50,lineToGainYards+25);down=1;flashResult('FIRST DOWN!',true,950);showMessage('FIRST DOWN!',`${Math.round(gain)}-yard gain · reached ${oldTarget} yards. New set of downs.`,1250);updateScore();scheduleResult(()=>setupPlay(true),1100);return}
  if(down>=4){finishSeries(false,'STOPPED ON DOWNS',`${reason} after ${Math.round(gain)} yards. You needed ${Math.ceil(lineToGainYards-ballSpotYards)} more.`);return}
  down++;flashResult(reason==='TACKLED'?'TACKLED':'PLAY OVER',true,850);showMessage(reason,`${Math.round(gain)}-yard gain · ${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)} next.`,1100);updateScore();scheduleResult(()=>setupPlay(true),950);
}
function useDown(head,detail,countDrop=false){
  endBall();if(countDrop)drops++;flashResult(head==='PASS BROKEN UP'?'BREAKUP':'INCOMPLETE',false,900);if(down>=4){finishSeries(false,'STOPPED ON DOWNS',`${detail} Four downs expired before reaching ${lineToGainYards} yards.`);return}down++;showMessage(head,`${detail} ${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)} next.`,1150);updateScore();scheduleResult(()=>setupPlay(true),1050);
}
function beginRunAfterCatch(player){
  markCatch(player);
  ballLive=false;arcLine.visible=false;landRing.visible=false;predictedFlightTime=0;charging=false;ballCarrier=player;player.hasBall=true;player.catchPose=.45;player.velocity.multiplyScalar(.72);playState='run';catches++;flashResult(player.jumpY>.18?'SKY-HIGH CATCH!':'CATCH — GO!',true,950);showMessage('RUN AFTER CATCH',`${player.label} secured it. He slows slightly, turns upfield and looks for space while the defense accelerates in pursuit.`,1050);attachBallToCarrier();updateScore();
}
function triggerTackle(d){if(playState!=='run'||!ballCarrier)return;tackler=d;tackleSpotYards=Math.min(50,Math.max(ballSpotYards+1,yardsForWorldZ(ballCarrier.mesh.position.z)));tackleTimer=.72;playState='tackle';ball.visible=false;ballCarrier.velocity.set(0,0,0);d.velocity.set(0,0,0);flashResult('TACKLED',false,700);showMessage('TACKLED',`Ball spotted at the ${Math.round(50-tackleSpotYards)} yard line.`,900)}
function updateRunAfterCatch(dt,now){
  if(!ballCarrier)return;for(const r of receivers){if(r===ballCarrier)updateBallCarrier(r,dt);else updateReceiver(r,dt,now)}for(const d of defenders)updateDefender(d,defenderTarget(d,now),dt);attachBallToCarrier();
  if(ballCarrier.mesh.position.z<=GOAL_LINE_Z){finishPlayAtSpot(GOAL_LINE_Z,'TOUCHDOWN');return}
  for(const d of defenders){const dx=d.mesh.position.x-ballCarrier.mesh.position.x,dz=d.mesh.position.z-ballCarrier.mesh.position.z;const evadeRating=P.effective((ballCarrier.profile?.evasion||50))/100,tackleRadius=THREE.MathUtils.lerp(1.04,.86,evadeRating);if(d.fakeUntil<=gameTime&&Math.hypot(dx,dz)<tackleRadius){triggerTackle(d);return}}solvePlayerCollisions(dt);attachBallToCarrier();
}
function updateTackle(dt){if(!ballCarrier)return;tackleTimer-=dt;const t=THREE.MathUtils.clamp(1-tackleTimer/.72,0,1);ballCarrier.mesh.rotation.x=-Math.sin(Math.min(1,t)*Math.PI*.5)*1.28;ballCarrier.mesh.position.y=Math.max(0,.12-Math.sin(t*Math.PI)*.08);if(tackler){tackler.mesh.rotation.x=-Math.sin(Math.min(1,t)*Math.PI*.5)*.42}if(tackleTimer<=0){const z=worldZForYards(tackleSpotYards);ballCarrier.mesh.position.z=z;finishPlayAtSpot(z,'TACKLED')}}

function seriesResult(offenseWon){
  if(offenseWon)seriesOffense++;else seriesDefense++;updateScore();
  if(seriesOffense>=3){showMessage('MATCHUP WON!',`You won Round ${franchise.round}. Cash payout and the receiver market are opening.`,1600);endMatchup(true);return true}
  if(seriesDefense>=3){score=Math.max(0,score-7);showMessage('MATCHUP LOST',`The defense won the best-of-5. Your roster and money remain saved.`,1600);endMatchup(false);return true}return false;
}
function endBall(){captureReplay(true);replayRecording=false;ballLive=false;ball.visible=false;if(playState!=='run'&&playState!=='tackle')playState='dead';arcLine.visible=false;landRing.visible=false;predictedFlightTime=0}
function resolveCatch(player,isDefense){if(isDefense){ints++;endBall();flashResult('INTERCEPTION',false,1050);showMessage('INTERCEPTED','Turnover — the defense wins this series.',1300);resetDrive();if(!seriesResult(false))scheduleResult(()=>setupPlay(true),1250)}else beginRunAfterCatch(player);updateScore()}
function resolveDrop(head='DROP',detail='The receiver could not secure the catch.',countDrop=false){useDown(head,detail,countDrop)}
function resolveGround(){useDown('INCOMPLETE','Placement missed the catch window.')}
function resolveNoThrow(){charging=false;arcLine.visible=false;landRing.visible=false;useDown('THROW CLOCK EXPIRED','Eight seconds elapsed before the ball came out.')}

function update(dt,now){
  if(playState==='countdown'){
    const left=Math.max(0,snapTime-now),count=Math.ceil(left/1000);if(count>0&&count!==nextCount){nextCount=count;showMessage(String(count),`${plays[selectedPlay].name} locked in.`,760)}
    if(now>=snapTime){playState='live';throwClock=8;routeVisuals.visible=false;$('playCallPanel').style.display='none';updateThrowClock();showMessage('SNAP',`${downLabel()} & ${Math.ceil(lineToGainYards-ballSpotYards)}. Eight seconds to throw — a completed pass stays live until the runner is tackled or scores.`,950)}
  }
  if(playState==='live'||playState==='thrown'){receivers.forEach(r=>updateReceiver(r,dt,now));defenders.forEach(d=>updateDefender(d,defenderTarget(d,now),dt));solvePlayerCollisions(dt)}else if(playState==='run'){updateRunAfterCatch(dt,now);if(ballCarrier){const target=ballCarrier.mesh.position.clone().add(new THREE.Vector3(0,3.6,8));camera.position.lerp(target,Math.min(1,dt*3));camera.lookAt(ballCarrier.mesh.position.clone().add(new THREE.Vector3(0,1.3,-3)))}}else if(playState==='tackle'){updateTackle(dt)}
  if(playState==='live'&&!ballLive){throwClock=Math.max(0,throwClock-dt);updateThrowClock();if(throwClock<=0){resolveNoThrow();return}}
  if(charging){if(keyLoft)loftBias=Math.min(1,loftBias+dt*1.5);if(keyBullet)loftBias=Math.max(-1,loftBias-dt*1.5);const st=throwStats(currentHeld());chargePower=st.power;updateCharge();updateArcPreview()}else if(!ballLive){chargePower*=Math.pow(.08,dt);loftBias*=Math.pow(.18,dt);updateCharge()}
  if(ballLive){
    throwTime+=dt;ballPrev.copy(ball.position);
    // Poor spirals can flutter vertically and lose a little speed, but no longer curve sideways away from the reticle.
    const wobble=(1-spiralQuality);if(wobble>.05){ballVel.y+=Math.cos(throwTime*11+duckPhase)*wobble*.8*dt;ballVel.multiplyScalar(1-wobble*.012*dt)}
    ballVel.y-=9.81*dt;ball.position.addScaledVector(ballVel,dt);
    const landing=drawTrajectory(ball.position,ballVel);if(landing){predictedLanding.copy(landing);const livePrediction=getBallLanding();predictedFlightTime=livePrediction?livePrediction.time:0}
    const dir=ballVel.clone().normalize();ball.quaternion.setFromUnitVectors(new THREE.Vector3(0,0,1),dir);ball.userData.mesh.rotation.z+=dt*(18+spiralQuality*28);ball.userData.mesh.rotation.x=Math.sin(throwTime*16+duckPhase)*(1-spiralQuality)*.18;
    if(!checkBallContact()&&(ball.position.y<.10||ball.position.z<-78||Math.abs(ball.position.x)>38||throwTime>5.2))resolveGround();
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
function loop(now){const dt=Math.min(.035,(now-lastTime)/1000);lastTime=now;
  if(replay){if(!menuOpen&&!document.hidden)updateReplay(dt)}
  else if(!inputBlocked()&&!document.hidden){gameTime+=dt*1000;if(transition&&gameTime>=transition.at){const fn=transition.fn;transition=null;fn()}update(dt,gameTime);captureReplay(false,dt)}
  updateHUD();renderer.render(scene,camera);requestAnimationFrame(loop)}requestAnimationFrame(loop);

// Desktop aiming and throws.
canvas.addEventListener('click',e=>{if(inputBlocked())return;if(playState==='call'){const idx=findReceiverAtScreen(e.clientX,e.clientY);if(idx>=0){openAudible(idx);return}}if(matchMedia('(pointer:fine)').matches&&document.pointerLockElement!==canvas)canvas.requestPointerLock?.()});
addEventListener('mousemove',e=>{if(!inputBlocked()&&document.pointerLockElement===canvas){yaw-=e.movementX*.0019;pitch-=e.movementY*.0017;pitch=THREE.MathUtils.clamp(pitch,-.68,.44);yaw=THREE.MathUtils.clamp(yaw,-1.08,1.08);applyCamera()}});
addEventListener('keydown',e=>{
  if(replay){if(e.code==='Space'||e.code==='Escape'){e.preventDefault();finishReplay()}return}
  if(inputBlocked())return;
  if(e.code==='Escape'&&audibleReceiverIndex==null){showMainMenu();return}
  if(e.code==='KeyJ'&&playState==='run'){e.preventDefault();if(!e.repeat)tryJuke(ballCarrier,true);return}
  if(playState==='call'){
    if(audibleReceiverIndex!=null&&/^Key[A-Z]$/.test(e.code)){const route=routeByKey[e.code.slice(3)];if(route){e.preventDefault();assignAudibleRoute(route);return}}
    if(/^Key[XHYZ]$/.test(e.code)){const idx=receiverByKey[e.code.slice(3)];if(idx!=null){e.preventDefault();openAudible(idx);return}}
    if(/^Digit[1-8]$/.test(e.code)){e.preventDefault();selectedPlay=Number(e.code.slice(-1))-1;setupPlay(false);return}
  }
  if(e.code==='Space'){e.preventDefault();if(playState==='call'){if(!e.repeat)beginCountdown();return}if(playState==='live'&&!e.repeat)startCharge()}
  if(e.code==='KeyW'){keyLoft=true;if(charging)e.preventDefault()}if(e.code==='KeyS'){keyBullet=true;if(charging)e.preventDefault()}
  if(e.code==='Escape'&&playState==='call'&&audibleReceiverIndex!=null){e.preventDefault();closeAudible(true)}
  if(e.code==='Enter'){e.preventDefault();beginCountdown()}if(e.code==='KeyR'&&playState==='call'){e.preventDefault();setupPlay(false)}
});addEventListener('keyup',e=>{if(inputBlocked())return;if(e.code==='Space'){e.preventDefault();if(playState==='live'||playState==='thrown')stopCharge()}if(e.code==='KeyW')keyLoft=false;if(e.code==='KeyS')keyBullet=false});

// Touch: swipe aims; stationary/slow hold charges. Release throws.
let touch=null;canvas.addEventListener('pointerdown',e=>{if(inputBlocked()||e.pointerType!=='touch')return;if(playState==='call'){const idx=findReceiverAtScreen(e.clientX,e.clientY);if(idx>=0)openAudible(idx);return}touch={id:e.pointerId,lastX:e.clientX,lastY:e.clientY,startY:e.clientY,t:gameTime,charging:false,moved:0};canvas.setPointerCapture?.(e.pointerId)});
canvas.addEventListener('pointermove',e=>{if(inputBlocked()||!touch||e.pointerId!==touch.id)return;const dx=e.clientX-touch.lastX,dy=e.clientY-touch.lastY;touch.lastX=e.clientX;touch.lastY=e.clientY;touch.moved+=Math.hypot(dx,dy);if(touch.charging){yaw-=dx*.0028;loftBias=THREE.MathUtils.clamp(loftBias-dy*.018,-1,1)}else{yaw-=dx*.0045;pitch-=dy*.0041;pitch=THREE.MathUtils.clamp(pitch,-.72,.46)}yaw=THREE.MathUtils.clamp(yaw,-1.14,1.14);applyCamera();if(!touch.charging&&gameTime-touch.t>220&&playState==='live'){touch.charging=true;startCharge()}});
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
function inputBlocked(){return menuOpen||saveOpen||cloudBusy||!!replay||$('managerLayer').style.display==='flex'}
function checkpoint(){
  franchise.checkpoint={seriesOffense,seriesDefense,ballSpotYards,down,lineToGainYards,selectedPlay};
  saveFranchise();
}
function restoreCheckpoint(){
  const c=franchise.checkpoint;resetDrive();seriesOffense=0;seriesDefense=0;
  if(c){seriesOffense=c.seriesOffense;seriesDefense=c.seriesDefense;ballSpotYards=c.ballSpotYards;down=c.down;lineToGainYards=c.lineToGainYards;selectedPlay=THREE.MathUtils.clamp(c.selectedPlay||0,0,plays.length-1)}
}
function renderMenu(){
  $('menuSummary').textContent=`Franchise ${activeSlot+1} · Round ${franchise.round} · ${franchise.wins} wins · $${franchise.cash}${localSaveAvailable?'':' · Local saving unavailable'}`;
  $('startBtn').textContent=receivers.length?'RESUME GAME':'CONTINUE';
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
  if(replay)finishReplay();transition=null;clearPlayers();replayFrames=[];replayRecording=false;replayEligible=false;
  franchise=f;franchise.team=franchise.team.map(normalizeReceiver);franchise.market=franchise.market.map(normalizeReceiver);
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
  if(!r||(r.jukeCooldown||0)>0||r.jumpY>.08||r.stagger>.1||ballLive)return false;
  const candidates=defenders.filter(d=>d.mesh.position.distanceTo(r.mesh.position)<4.8&&d.mesh.position.z<=r.mesh.position.z+1.5&&d.fakeUntil<=gameTime);
  if(!candidates.length){if(manual)flashResult('GET CLOSER TO A DEFENDER',true,550);return false}
  const nearest=candidates.reduce((a,b)=>a.mesh.position.distanceTo(r.mesh.position)<b.mesh.position.distanceTo(r.mesh.position)?a:b);
  let side=nearest.mesh.position.x>=r.mesh.position.x?-1:1;
  if(r.mesh.position.x>22)side=-1;else if(r.mesh.position.x<-22)side=1;
  const traits=P.traits(r.profile),distance=nearest.mesh.position.distanceTo(r.mesh.position);
  r.jukeMove=distance<2.1?'SPIN':distance>3.5?'HESITATION':'HARD CUT';
  r.jukeSide=side;r.jukeDuration=r.jukeMove==='SPIN'?.62:.56;
  r.jukeCooldown=traits.jukeCooldown;r.jukeAnim=r.jukeDuration;r.plantPose=1;
  r.velocity.multiplyScalar(r.jukeMove==='HESITATION'?.68:.86);
  r.impactVel.addScaledVector(new THREE.Vector3(side,0,0),2.6+P.effective(r.profile.tricks)*.015);
  let beaten=0;
  for(const d of candidates){
    const gap=d.mesh.position.distanceTo(r.mesh.position);
    const timing=gap>=2&&gap<=3.6?.12:0;
    const chance=THREE.MathUtils.clamp(traits.jukeChance+.10+timing-currentSkill()*.22-(gap>4?.12:0),.15,.9);
    if(Math.random()<chance){
      beaten++;d.fakeUntil=gameTime+380+P.effective(r.profile.tricks)*3;
      d.fakeTarget=r.mesh.position.clone().add(new THREE.Vector3(-side*4,0,-1));
      d.plantPose=1;d.velocity.multiplyScalar(.48);
      d.fakeSide=-side;d.impactVel.addScaledVector(new THREE.Vector3(-side,0,0),1.6);
    }
  }
  if(manual||r===ballCarrier)flashResult(beaten?r.jukeMove+' — BEAT '+beaten+'!':r.jukeMove+' — DEFENDER HELD',!!beaten,700);
  return true;
}
function updateTricks(r,dt,carrier){
  r.jukeCooldown=Math.max(0,(r.jukeCooldown||0)-dt);r.jukeAnim=ballLive?0:Math.max(0,(r.jukeAnim||0)-dt);
  // Players choose a moment near a defender. Chance is time based, not frame based.
  if(!ballLive&&(carrier||playState==='live')&&Math.random()<dt*(.5+P.effective(r.profile.tricks)*.009))tryJuke(r);
}
$('jukeBtn').onclick=()=>{if(!inputBlocked()&&playState==='run')tryJuke(ballCarrier,true)};
function updateHUD(){
  document.body.dataset.phase=playState;document.body.dataset.active=String(['live','thrown','run','tackle'].includes(playState));document.body.dataset.charging=String(charging);document.body.dataset.replay=String(!!replay);
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
fieldLabel('RECEIVER WINDOW',35,4,0,-65);
const seats=new THREE.MeshStandardMaterial({color:0x596c7c,roughness:.95});
for(const side of [-1,1])for(let tier=0;tier<4;tier++){
  const m=new THREE.Mesh(new THREE.BoxGeometry(2.5,.55,110),seats);m.position.set(side*(29+tier*2),2+tier*1.7,-10);fieldDecor.add(m);
}
const postMat=new THREE.MeshStandardMaterial({color:0xffc936,metalness:.35,roughness:.4});
for(const [x,y,z,sx,sy,sz] of [[0,2.6,-72,.2,5.2,.2],[0,5.2,-72,9,.18,.18],[-4.5,7.7,-72,.18,5,.18],[4.5,7.7,-72,.18,5,.18]]){const m=new THREE.Mesh(new THREE.BoxGeometry(sx,sy,sz),postMat);m.position.set(x,y,z);fieldDecor.add(m)}
const venues=[{name:'Riverside Field',turf:0x28733d,sky:0x8abbd5,end:0x184578,sun:0xfff6e2},{name:'Hillcrest Stadium',turf:0x336b40,sky:0xa3bfce,end:0x243954,sun:0xfff4d8},{name:'Harbor Park',turf:0x266c48,sky:0x93b6c5,end:0x174f5b,sun:0xe8f3ff},{name:'Sunset Bowl',turf:0x3e7044,sky:0xc4ac98,end:0x544047,sun:0xffd4a6}];
function applyFieldTheme(){const v=venues[(franchise.round-1)%venues.length];turf.material.color.set(v.turf);endzone.material.color.set(v.end);scene.background.set(v.sky);scene.fog.color.set(v.sky);sun.color.set(v.sun);$('defensePanel').title=v.name}

function scenePose(){
  const objects=[];for(const a of [...receivers,...defenders])a.mesh.traverse(o=>objects.push(o));ball.traverse(o=>objects.push(o));
  return {objects,transforms:objects.map(o=>({p:o.position.clone(),q:o.quaternion.clone(),visible:o.visible})),ball:ball.position.clone()};
}
function captureReplay(force=false,dt=0){
  if(!replayRecording||replay)return;if(replayTailEnds&&gameTime>replayTailEnds){replayRecording=false;return}replaySample+=dt;if(!force&&replaySample<1/30)return;replaySample=0;
  replayFrames.push(scenePose());if(replayFrames.length>360)replayFrames.shift();
}
function markCatch(r){
  replayTailEnds=gameTime+500;
  const gain=yardsForWorldZ(ball.position.z)-snapSpotYards;
  replayEligible=replayEligible||gain>=10||ball.position.z<=GOAL_LINE_Z;captureReplay(true);
}
function scheduleResult(fn,delay){
  captureReplay(true);replayRecording=false;checkpoint();
  if(replayEligible&&replayFrames.length>=2){
    const frames=replayFrames;replayFrames=[];replayEligible=false;
    transition={at:gameTime+Math.min(delay,800),fn:()=>startReplay(frames,fn)};
  }else transition={at:gameTime+delay,fn};
}
function startReplay(frames,after){
  cancelInput();replay={frames,after,time:0,saved:scenePose(),cameraP:camera.position.clone(),cameraQ:camera.quaternion.clone()};$('replayBar').hidden=false;
}
function putPose(a,b,t){a.objects.forEach((o,i)=>{const from=a.transforms[i],to=b.transforms[i]||from;o.position.copy(from.p).lerp(to.p,t);o.quaternion.copy(from.q).slerp(to.q,t);o.visible=t<.5?from.visible:to.visible})}
function updateReplay(dt){
  const r=replay;r.time+=dt*.72;const index=r.time*30,lo=Math.floor(index);
  if(lo>=r.frames.length-1){finishReplay();return}
  const a=r.frames[lo],b=r.frames[lo+1],t=index-lo;putPose(a,b,t);
  const target=a.ball.clone().lerp(b.ball,t),dir=b.ball.clone().sub(a.ball);
  if(dir.lengthSq()<.0001){const prior=r.frames[Math.max(0,lo-1)];dir.copy(a.ball).sub(prior.ball)}
  if(dir.lengthSq()<.0001)dir.set(0,0,-1);dir.normalize();
  const desired=target.clone().addScaledVector(dir,-2.7).add(new THREE.Vector3(0,.65,0));desired.y=Math.max(.55,desired.y);
  camera.position.copy(desired);camera.lookAt(target.clone().addScaledVector(dir,1.3));
}
function finishReplay(){
  if(!replay)return;const r=replay;putPose(r.saved,r.saved,0);camera.position.copy(r.cameraP);camera.quaternion.copy(r.cameraQ);replay=null;$('replayBar').hidden=true;r.after();
}
$('skipReplay').onclick=finishReplay;
addEventListener('beforeunload',saveFranchise);saveFranchise();
})();

