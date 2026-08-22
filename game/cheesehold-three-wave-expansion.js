// Cheesehold Three-Wave Fortress Expansion
const CHEESEHOLD_LEVELS=[
 {name:'Fortress Lanes',sub:'long firing lanes and staggered barricades',layout:'lanes'},
 {name:'Split Foundry',sub:'four industrial islands with open kill corridors',layout:'foundry'},
 {name:'Spiral Pantry',sub:'nested shelves with rotating breach points',layout:'spiral'},
 {name:'Broken Freezer',sub:'diagonal ice fields and wide flanking routes',layout:'freezer'},
 {name:'Twin Vaults',sub:'paired strongholds divided by a broad center road',layout:'vaults'},
 {name:'Grand Mousetrap',sub:'concentric defenses with dangerous inner approaches',layout:'rings'}
];
const WAVE_REPRIEVE=15;
const ROUND_BASE_CHEESE=18;
const ROUND_CHEESE_GROWTH=4;
const ROUND_CHEESE_ACCEL=0.55;
const baseFreshMods=freshMods;
freshMods=function(){
 const m=baseFreshMods();
 m.generatorRate*=.92;
 m.generatorYield=Math.max(1,m.generatorYield*.82);
 return m;
};
BUILDINGS.generator.desc='Produces cheese steadily. Fully enclosed generators gain a modest production bonus.';
BUILDINGS.generator.hp=Math.round(BUILDINGS.generator.hp*1.12);

function roundAllowance(round=game.round){
 const r=Math.max(1,round)-1;
 return Math.round(ROUND_BASE_CHEESE+r*ROUND_CHEESE_GROWTH+Math.pow(r,1.22)*ROUND_CHEESE_ACCEL);
}
function lateRoundPressure(round=game.round){
 const r=Math.max(0,round-1);
 return Math.pow(r,1.34);
}
function waveQuota(wave=game.wave?.number||1){
 const r=Math.max(1,game.round),pressure=lateRoundPressure(r);
 const base=wave===1?7:wave===2?10:6;
 const linear=wave===3?r*1.7:r*2.5;
 const accel=pressure*(wave===1?.62:wave===2?.82:.58);
 return Math.round(base+linear+accel);
}
function waveLabel(){
 const w=game.wave?.number||1;
 return w===3?'WAVE 3 · CAT':'WAVE '+w;
}
function setWave(number){
 game.wave={number,spawned:0,quota:waveQuota(number),clearAt:null,nextAt:null,announced:false};
 game.nextMinion=game.time+(number===1?1.8:1.0);
 if(number===3&&!cat)spawnCat();
 const bossText=number===3?'CAT BOSS + ESCORTS':'Enemy force '+number+' of 3';
 showMsg(number===3?'WAVE 3 — CAT':'WAVE '+number,bossText);
 updateWaveHud();
}
function updateWaveHud(){
 if(!game.running||!game.wave)return;
 const w=game.wave;
 let status='Round '+game.round+' · '+waveLabel()+' · '+Math.max(0,w.quota-w.spawned)+' reinforcements';
 if(w.clearAt!=null&&w.number<3){
   const left=Math.max(0,Math.ceil(WAVE_REPRIEVE-(game.time-w.clearAt)));
   status='Round '+game.round+' · Wave '+w.number+' cleared · '+left+'s reprieve';
 }
 if(w.number===3&&cat&&!cat.dead)status='Round '+game.round+' · WAVE 3 · '+game.boss.name+' · '+Math.ceil(cat.hp)+' HP';
 ui.themeSub.textContent=status;
 if(!cat){
   ui.catHp.textContent='W3';
   ui.catBar.style.width='0%';
   ui.catAbility.textContent='Wave 3';
   ui.catAbilityTime.textContent='BOSS';
 }
}
function waveController(){
 if(game.running&&!game.paused&&game.wave){
   const w=game.wave;
   const live=enemies.filter(e=>!e.dead).length;
   if(w.spawned>=w.quota&&live===0&&w.number<3){
     if(w.clearAt==null){
       w.clearAt=game.time;
       showMsg('WAVE '+w.number+' CLEAR','15 SECOND REPRIEVE');
       toast('Fortify and rebuild — next wave in 15 seconds');
     }else if(game.time-w.clearAt>=WAVE_REPRIEVE){
       setWave(w.number+1);
     }
   }
   updateWaveHud();
 }
 requestAnimationFrame(waveController);
}

function safeEdgeSpot(offset=0){
 const candidates=[];
 const stride=3;
 for(let x=2;x<COLS-2;x+=stride){candidates.push([x,1],[x,ROWS-2]);}
 for(let y=2;y<ROWS-2;y+=stride){candidates.push([1,y],[COLS-2,y]);}
 for(let i=0;i<candidates.length;i++){
   const p=candidates[(i+offset+game.round*3+(game.wave?.number||1)*5)%candidates.length];
   if(!terrain.has(K(p[0],p[1]))&&!buildAt(p[0],p[1])&&!occupiedEnemy(p[0],p[1]))return p;
 }
 for(let y=1;y<ROWS-1;y++)for(let x=1;x<COLS-1;x++){
   if((x===1||y===1||x===COLS-2||y===ROWS-2)&&!terrain.has(K(x,y))&&!buildAt(x,y)&&!occupiedEnemy(x,y))return[x,y];
 }
 return[1,1];
}
function spawnPlayer(){
 const mh=100+game.mods.maxHpBonus,cx=Math.floor(COLS/2),cy=Math.floor(ROWS/2);
 player={gx:cx,gy:cy,lastDir:0,hp:mh,maxHp:mh,mesh:mouseMesh(),moving:null,inv:0,slowUntil:0,shield:game.mods.roundShield};
 player.mesh.position.copy(worldPos(player.gx,player.gy));entityGroup.add(player.mesh);
}
function spawnCat(){
 if(game.wave&&game.wave.number<3)return false;
 const s=safeEdgeSpot(7),boss=BOSS_VARIANTS[(game.round-1)%BOSS_VARIANTS.length];
 const r=Math.max(0,game.round-1),terror=1+r*.28+Math.pow(r,1.42)*.095;
 const hp=(380+game.round*160)*terror*(game.mutator?.catHp||1)*boss.hp;
 const dmg=(18+game.round*2.25)*(1+r*.13)*boss.damage;
 const move=Math.max(.105,(.49-game.round*.013)*boss.move);
 const mesh=catMesh();styleBossMesh(mesh,boss);
 mesh.scale.multiplyScalar(1+Math.min(.55,r*.04+Math.pow(r,1.16)*.006));
 game.boss=boss;
 cat={type:'cat',boss,gx:s[0],gy:s[1],mesh,hp,maxHp:hp,damage:dmg,baseMove:move,attackEvery:Math.max(.42,.7-r*.018),nextMove:game.time+.5,nextAttack:0,moving:null,dead:false,bounty:14+game.round*3,slow:0,poisonUntil:0,poisonTick:0,eaten:new Set(),wall:1.3*boss.wall,attackAnim:null,baseScale:mesh.scale.clone(),special:null,nextAbility:game.time+(game.round>=2?5.8:8.5),abilityCursor:0};
 cat.bar=healthBar(1.55);cat.bar.position.y=1.72;cat.mesh.add(cat.bar);cat.mesh.position.copy(worldPos(cat.gx,cat.gy));entityGroup.add(cat.mesh);
 flashDanger('rgba(255,25,42,.28)');
 toast(game.boss.name+' enters — '+Math.round(cat.maxHp)+' HP');
 return true;
}
function spawnEnemy(){
 const w=game.wave;
 if(w){
   if(w.clearAt!=null||w.spawned>=w.quota)return false;
 }
 if(enemies.filter(e=>!e.dead).length>=enemyCap())return false;
 const unlocked=Object.entries(ENEMIES).filter(([,d])=>game.round>=d.unlock);
 if(!unlocked.length)return false;
 const [type,d]=unlocked[Math.floor(Math.random()*unlocked.length)];
 const spot=safeEdgeSpot((w?.spawned||0)*2);
 if(!spot)return false;
 const r=Math.max(0,game.round-1),waveScale=1+((w?.number||1)-1)*.16;
 const healthScale=(1+r*.16+Math.pow(r,1.38)*.025)*waveScale;
 const elite=game.round>=3&&Math.random()<Math.min(.58,.06+game.round*.024+(w?.number===3?.1:0)+(game.mutator?.eliteBonus||0));
 const mesh=minionMesh(type,d.color),eliteHp=elite?1.9:1,eliteDmg=elite?1.3:1;
 const bodyScale=1+Math.min(.48,r*.018+Math.pow(r,1.24)*.006)+(elite?.16:0);
 mesh.scale.multiplyScalar(bodyScale);
 const e={type,gx:spot[0],gy:spot[1],mesh,hp:d.hp*healthScale*eliteHp,maxHp:d.hp*healthScale*eliteHp,damage:d.damage*(1+r*.09)*waveScale*eliteDmg,baseMove:Math.max(.13,d.move*(1-r*.012)*(elite?.86:1)*(game.mutator?.enemySpeed||1)),attackEvery:d.attack,bounty:d.bounty+(elite?2:0)+(game.mutator?.bountyBonus||0),xp:d.xp*(elite?1.75:1),wall:d.wall*(elite?1.15:1),elite,nextMove:game.time+.2,nextAttack:0,moving:null,dead:false,slow:0,poisonUntil:0,poisonTick:0,eaten:new Set(),attackAnim:null,baseScale:mesh.scale.clone()};
 if(elite){
   mesh.traverse(n=>{if(n.material?.emissive){n.material.emissive.setHex(0xffb52e);n.material.emissiveIntensity=.55}});
   const crown=new THREE.Mesh(new THREE.TorusGeometry(.32,.035,6,18),new THREE.MeshBasicMaterial({color:0xffcf55}));
   crown.rotation.x=Math.PI/2;crown.position.y=1.02;mesh.add(crown);
 }
 e.bar=healthBar((type==='ox'||type==='brute'||type==='badger')?1.08:.82);
 e.bar.position.y=(type==='ox'||type==='brute'||type==='badger')?1.2:.88;e.mesh.add(e.bar);
 e.mesh.position.copy(worldPos(e.gx,e.gy));entityGroup.add(e.mesh);enemies.push(e);
 if(w)w.spawned++;
 return true;
}

function beginRound(first=false){
 for(const b of buildings)disposeObject(b.mesh);for(const c of cheeses)disposeObject(c.mesh);for(const e of enemies)disposeObject(e.mesh);for(const b of bullets)disposeObject(b.mesh);
 if(cat)disposeObject(cat.mesh);if(player)disposeObject(player.mesh);
 buildings=[];cheeses=[];enemies=[];bullets=[];cat=null;player=null;
 game.mutator=ROUND_MUTATORS[(game.round-1)%ROUND_MUTATORS.length];
 game.cheese=roundAllowance(game.round)+Math.max(0,game.mods.roundCheese||0);
 buildArena(game.round);spawnPlayer();
 game.boss=BOSS_VARIANTS[(game.round-1)%BOSS_VARIANTS.length];
 for(let i=0;i<10;i++)spawnCheese();
 game.nextCheese=game.time+2.2;game.pendingRound=false;
 setWave(1);refreshEnclosures(true);updateSelected();
 showMsg(first?'LEVEL 1':'LEVEL '+game.round,CHEESEHOLD_LEVELS[(game.round-1)%CHEESEHOLD_LEVELS.length].name+' · 3 WAVES · '+game.cheese+' CHEESE');
}
function resetRun(){
 game={running:true,paused:false,time:0,round:1,kills:0,cheese:ROUND_BASE_CHEESE,xp:0,xpLevel:1,xpNext:10,nextCheese:.7,nextMinion:2.0,catFrenzyUntil:0,mods:freshMods(),ranks:{},victoryRanks:{},artifactRanks:{},pendingRound:false,mutator:ROUND_MUTATORS[0],boss:null,wave:null};
 ui.upgrade.classList.remove('open');clearEntities();beginRound(true);
}

function addLevelTerrain(x,y){
 const cx=Math.floor(COLS/2),cy=Math.floor(ROWS/2);
 if(!inside(x,y)||x<=0||y<=0||x>=COLS-1||y>=ROWS-1)return;
 if(Math.abs(x-cx)<=2&&Math.abs(y-cy)<=2)return;
 terrain.add(K(x,y));
}
function carveLevel(x,y){terrain.delete(K(x,y));}
function lineTerrain(x1,y1,x2,y2,gaps=[]){
 if(x1===x2){for(let y=Math.min(y1,y2);y<=Math.max(y1,y2);y++)if(!gaps.includes(y))addLevelTerrain(x1,y);}
 else if(y1===y2){for(let x=Math.min(x1,x2);x<=Math.max(x1,x2);x++)if(!gaps.includes(x))addLevelTerrain(x,y1);}
}
function buildArena(round){
 clearGroup(tileGroup);clearGroup(terrainGroup);clearGroup(decorGroup);terrain.clear();
 const t=THEMES[(round-1)%THEMES.length],level=CHEESEHOLD_LEVELS[(round-1)%CHEESEHOLD_LEVELS.length],rng=seeded(round*37+11);
 scene.background=new THREE.Color(t.sky);scene.fog=new THREE.FogExp2(t.fog,.029);
 ui.themeName.textContent='L'+round+' · '+level.name;ui.themeSub.textContent=level.sub;pointLight.color.setHex(t.accent);
 for(let y=0;y<ROWS;y++)for(let x=0;x<COLS;x++){const tile=box(CELL*.99,.14,CELL*.99,((x+y)&1)?t.tileA:t.tileB,.94,.01);tile.position.copy(worldPos(x,y,-.11));tileGroup.add(tile);}
 const bw=box(COLS*CELL+.5,1.9,.34,t.terrain,.62,.1);bw.position.set(0,.84,-ROWS*CELL/2-.14);terrainGroup.add(bw);
 const bs=bw.clone();bs.position.z=ROWS*CELL/2+.14;terrainGroup.add(bs);
 const bl=box(.34,1.9,ROWS*CELL+.5,t.terrain,.62,.1);bl.position.set(-COLS*CELL/2-.14,.84,0);terrainGroup.add(bl);
 const br=bl.clone();br.position.x=COLS*CELL/2+.14;terrainGroup.add(br);
 const cx=Math.floor(COLS/2),cy=Math.floor(ROWS/2);
 if(level.layout==='lanes'){
   for(const x of [5,9,17,21])lineTerrain(x,2,x,ROWS-3,[5,cy,15]);
   lineTerrain(3,5,COLS-4,5,[7,cx,19]);lineTerrain(3,15,COLS-4,15,[7,cx,19]);
 }else if(level.layout==='foundry'){
   for(const [x,y,w,h] of [[3,3,5,4],[COLS-8,3,5,4],[3,ROWS-7,5,4],[COLS-8,ROWS-7,5,4]])for(let yy=y;yy<y+h;yy++)for(let xx=x;xx<x+w;xx++)addLevelTerrain(xx,yy);
   lineTerrain(cx-4,4,cx+4,4,[cx]);lineTerrain(cx-4,ROWS-5,cx+4,ROWS-5,[cx]);
 }else if(level.layout==='spiral'){
   lineTerrain(4,4,COLS-5,4,[COLS-7]);lineTerrain(4,4,4,ROWS-5,[6]);lineTerrain(4,ROWS-5,COLS-5,ROWS-5,[6]);lineTerrain(COLS-5,7,COLS-5,ROWS-5,[ROWS-7]);
   lineTerrain(8,8,COLS-9,8,[10]);lineTerrain(8,8,8,ROWS-9,[ROWS-11]);lineTerrain(8,ROWS-9,COLS-9,ROWS-9,[COLS-11]);
 }else if(level.layout==='freezer'){
   for(let i=2;i<ROWS-2;i++){const x=3+((i*2)%8);addLevelTerrain(x,i);if(i%2)addLevelTerrain(x+1,i);}
   for(let i=2;i<ROWS-2;i++){const x=COLS-4-((i*3)%8);addLevelTerrain(x,i);if(i%3===0)addLevelTerrain(x-1,i);}
   lineTerrain(4,cy,COLS-5,cy,[7,cx,COLS-8]);
 }else if(level.layout==='vaults'){
   for(const x0 of [4,COLS-10]){lineTerrain(x0,4,x0+5,4,[x0+2]);lineTerrain(x0,4,x0,ROWS-5,[cy]);lineTerrain(x0,ROWS-5,x0+5,ROWS-5,[x0+3]);lineTerrain(x0+5,4,x0+5,ROWS-5,[cy]);}
   lineTerrain(cx,2,cx,ROWS-3,[5,cy,15]);
 }else{
   lineTerrain(3,3,COLS-4,3,[cx]);lineTerrain(3,ROWS-4,COLS-4,ROWS-4,[cx]);lineTerrain(3,3,3,ROWS-4,[cy]);lineTerrain(COLS-4,3,COLS-4,ROWS-4,[cy]);
   lineTerrain(7,7,COLS-8,7,[9,cx,COLS-10]);lineTerrain(7,ROWS-8,COLS-8,ROWS-8,[9,cx,COLS-10]);lineTerrain(7,7,7,ROWS-8,[cy]);lineTerrain(COLS-8,7,COLS-8,ROWS-8,[cy]);
 }
 for(let x=1;x<COLS-1;x++){carveLevel(x,cy);if(x%5===0)carveLevel(x,cy-1);}
 for(let y=1;y<ROWS-1;y++){carveLevel(cx,y);if(y%5===0)carveLevel(cx+1,y);}
 for(let y=cy-2;y<=cy+2;y++)for(let x=cx-2;x<=cx+2;x++)carveLevel(x,y);
 renderTerrain(t,rng);
 for(let i=0;i<30;i++){const x=1+Math.floor(rng()*(COLS-2)),y=1+Math.floor(rng()*(ROWS-2));if(terrain.has(K(x,y))||Math.abs(x-cx)+Math.abs(y-cy)<5)continue;const p=worldPos(x,y,0);if(i%3===0){const post=box(.10,1.55,.10,t.trim,.45,.55);post.position.set(p.x,.78,p.z);decorGroup.add(post);const lamp=sphere(.10,t.accent,9);lamp.material.emissive.setHex(t.accent);lamp.material.emissiveIntensity=1.8;lamp.position.set(p.x,1.62,p.z);decorGroup.add(lamp);}}
}

function enclosureBlocked(x,y){
 if(!inside(x,y)||terrain.has(K(x,y)))return true;
 const b=buildAt(x,y);
 return !!b&&(b.type==='wall'||b.type==='superwall');
}
function enclosedCellSet(){
 const outside=new Set(),q=[];
 const add=(x,y)=>{const k=K(x,y);if(outside.has(k)||enclosureBlocked(x,y))return;outside.add(k);q.push([x,y]);};
 for(let x=0;x<COLS;x++){add(x,0);add(x,ROWS-1);}
 for(let y=0;y<ROWS;y++){add(0,y);add(COLS-1,y);}
 for(let i=0;i<q.length;i++){const [x,y]=q[i];for(const d of DIR4)add(x+d.x,y+d.y);}
 const enclosed=new Set();
 for(let y=0;y<ROWS;y++)for(let x=0;x<COLS;x++)if(!enclosureBlocked(x,y)&&!outside.has(K(x,y)))enclosed.add(K(x,y));
 return enclosed;
}
function removeFortifiedAura(b){
 if(!b?.fortifiedAura)return;
 b.mesh?.remove(b.fortifiedAura);disposeObject(b.fortifiedAura);b.fortifiedAura=null;
}
function addFortifiedAura(b){
 if(!b||b.fortifiedAura)return;
 const group=new THREE.Group();
 const ring=new THREE.Mesh(new THREE.TorusGeometry(.52,.035,7,30),new THREE.MeshBasicMaterial({color:b.type==='generator'?0xffd45a:0x72e4ff,transparent:true,opacity:.82,depthWrite:false}));
 ring.rotation.x=Math.PI/2;ring.position.y=.12;group.add(ring);
 const halo=new THREE.Mesh(new THREE.RingGeometry(.58,.76,30),new THREE.MeshBasicMaterial({color:b.type==='generator'?0xffd45a:0x72e4ff,transparent:true,opacity:.12,side:THREE.DoubleSide,depthWrite:false}));
 halo.rotation.x=-Math.PI/2;halo.position.y=.025;group.add(halo);
 b.mesh.add(group);b.fortifiedAura=group;
}
let enclosureSignature='',lastEnclosureScan=-99;
function refreshEnclosures(force=false){
 if(!game?.running)return;
 const sig=buildings.filter(b=>!b.dead&&(b.type==='wall'||b.type==='superwall')).map(b=>b.gx+','+b.gy+':'+b.type).sort().join('|');
 if(!force&&sig===enclosureSignature&&game.time-lastEnclosureScan<.5)return;
 enclosureSignature=sig;lastEnclosureScan=game.time;
 const enclosed=enclosedCellSet();
 for(const b of buildings){
   if(b.dead||b.type==='wall'||b.type==='superwall'){b.fortified=false;removeFortifiedAura(b);continue;}
   const yes=enclosed.has(K(b.gx,b.gy));
   if(yes&&!b.fortified)addFortifiedAura(b);
   if(!yes&&b.fortified)removeFortifiedAura(b);
   b.fortified=yes;
 }
}
const baseRemoveBuilding=removeBuilding;
removeBuilding=function(b){baseRemoveBuilding(b);enclosureSignature='';refreshEnclosures(true);};
const baseRefreshBuildingMesh=refreshBuildingMesh;
refreshBuildingMesh=function(b){
 removeFortifiedAura(b);baseRefreshBuildingMesh(b);
 if(b?.fortified)addFortifiedAura(b);
};
const baseDamageBuilding=damageBuilding;
damageBuilding=function(b,amount){
 baseDamageBuilding(b,b?.fortified?amount*.82:amount);
};
const baseBuildDirectionForFortress=buildDirection;
buildDirection=function(dirIndex=player?.lastDir){
 const before=buildings.length,out=baseBuildDirectionForFortress(dirIndex);
 if(out&&buildings.length!==before){enclosureSignature='';refreshEnclosures(true);}
 return out;
};
let enclosureLastTime=0;
function enclosureTick(){
 if(game.running&&!game.paused){
   const dt=Math.max(0,Math.min(.08,game.time-enclosureLastTime));enclosureLastTime=game.time;
   refreshEnclosures();
   for(const b of buildings){
     if(b.dead||!b.fortified)continue;
     if(Number.isFinite(b.next)&&b.next>game.time)b.next-=dt*(b.type==='generator'?.20:.16);
     if(b.hp<b.maxHp)b.hp=Math.min(b.maxHp,b.hp+dt*(b.type==='generator'?1.05:.8));
     if(b.fortifiedAura){
       b.fortifiedAura.rotation.y+=dt*.65;
       const s=1+Math.sin(game.time*3+b.id)*.05;b.fortifiedAura.scale.setScalar(s);
     }
   }
 }else enclosureLastTime=game.time||0;
 requestAnimationFrame(enclosureTick);
}
requestAnimationFrame(waveController);
requestAnimationFrame(enclosureTick);
