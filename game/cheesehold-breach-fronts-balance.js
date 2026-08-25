// Cheesehold breach-front, enemy-job, sector-defense, turret-diminishing-return, and summon-mobility balance pass.
// Loaded last inside the shared Cheesehold progression eval so it can safely wrap existing systems.

const CHEESEHOLD_BREACH_COLOR=0xff4455;
const CHEESEHOLD_BREACH_BUILD_RADIUS=2;
const CHEESEHOLD_ROLE_COLORS={sapper:0xff9a52,raider:0xff5fd1,flanker:0x66e0ff,bombardier:0xc79cff,siege:0xff4a4a};
const CHEESEHOLD_SUMMON_RANGE_CAPS={
 sparkBot:1.15,arcWalker:2.55,heavyMech:1.8,shieldGuard:1.15,spearGuard:1.55,
 repairBot:0,phoenixling:2.25,greaterPhoenix:3.0,rifleMouse:2.75,marksman:3.8,
 frostWisp:2.2,frostGuardian:1.8,venomCrawler:1.1,broodMother:1.9
};
const CHEESEHOLD_SUMMON_AWARENESS={
 sparkBot:6.5,arcWalker:8,heavyMech:7,shieldGuard:7,spearGuard:7,repairBot:6,
 phoenixling:8,greaterPhoenix:9,rifleMouse:8.5,marksman:10,frostWisp:8,frostGuardian:7.5,
 venomCrawler:6.5,broodMother:7.5
};

let cheeseholdBreaches=[];
let cheeseholdBreachGroup=null;
let cheeseholdThreatenedSectors=new Set();
let cheeseholdDefenseWaveKey='';
let cheeseholdDefenseTickAt=0;

function cheeseholdBreachCount(round=game?.round||1){
 if(round>=20)return 6;
 if(round>=15)return 5;
 if(round>=10)return 4;
 if(round>=5)return 3;
 return 2;
}
function cheeseholdActiveFrontCount(round=game?.round||1){
 if(round>=20)return 5;
 if(round>=15)return 4;
 if(round>=10)return 3;
 if(round>=5)return 2;
 return 1;
}
function cheeseholdEdgeDepthCells(edge,pos,depth=3,width=1){
 const out=[];
 for(let lateral=-width;lateral<=width;lateral++)for(let d=0;d<depth;d++){
  let x=pos+lateral,y=1+d;
  if(edge==='bottom')y=ROWS-2-d;
  else if(edge==='left'){x=1+d;y=pos+lateral;}
  else if(edge==='right'){x=COLS-2-d;y=pos+lateral;}
  if(inside(x,y))out.push({x,y});
 }
 return out;
}
function cheeseholdGateValid(edge,pos){
 const cells=cheeseholdEdgeDepthCells(edge,pos,4,1);
 if(cells.length<8)return false;
 return cells.every(c=>!terrain.has(K(c.x,c.y)));
}
function cheeseholdFindGatePosition(edge,wanted,used=[]){
 const max=edge==='top'||edge==='bottom'?COLS-4:ROWS-4;
 const min=3,center=Math.max(min,Math.min(max,Math.round(wanted)));
 const attempts=[];
 for(let delta=0;delta<=Math.max(COLS,ROWS);delta++){
  if(delta===0)attempts.push(center);else attempts.push(center+delta,center-delta);
 }
 for(const pos of attempts){
  if(pos<min||pos>max||used.some(u=>u.edge===edge&&Math.abs(u.pos-pos)<5))continue;
  if(cheeseholdGateValid(edge,pos))return pos;
 }
 return null;
}
function cheeseholdConfigureBreaches(round=game?.round||1){
 const count=cheeseholdBreachCount(round),edges=['top','right','bottom','left'],used=[],gates=[];
 for(let i=0;i<count;i++){
  const edge=edges[(i+round)%edges.length];
  const horizontal=edge==='top'||edge==='bottom';
  const span=horizontal?COLS:ROWS;
  const frac=(i%2===0?.28:.72);
  let pos=cheeseholdFindGatePosition(edge,span*frac,used);
  if(pos==null){
   for(const fallback of edges){pos=cheeseholdFindGatePosition(fallback,span*.5,used);if(pos!=null){used.push({edge:fallback,pos});break}}
   if(pos==null)continue;
   const actual=used[used.length-1];
   gates.push(cheeseholdBuildGate(actual.edge,actual.pos,gates.length));
   continue;
  }
  used.push({edge,pos});
  gates.push(cheeseholdBuildGate(edge,pos,gates.length));
 }
 cheeseholdBreaches=gates;
 return gates;
}
function cheeseholdBuildGate(edge,pos,index){
 const lane=cheeseholdEdgeDepthCells(edge,pos,3,1),enemyOnly=cheeseholdEdgeDepthCells(edge,pos,2,1),spawn=cheeseholdEdgeDepthCells(edge,pos,1,1);
 const noBuild=new Set();
 for(const c of lane){
  for(let dx=-CHEESEHOLD_BREACH_BUILD_RADIUS;dx<=CHEESEHOLD_BREACH_BUILD_RADIUS;dx++)for(let dy=-CHEESEHOLD_BREACH_BUILD_RADIUS;dy<=CHEESEHOLD_BREACH_BUILD_RADIUS;dy++){
   if(Math.abs(dx)+Math.abs(dy)>CHEESEHOLD_BREACH_BUILD_RADIUS)continue;
   const x=c.x+dx,y=c.y+dy;if(inside(x,y))noBuild.add(K(x,y));
  }
 }
 return{edge,pos,index,lane,enemyOnly:new Set(enemyOnly.map(c=>K(c.x,c.y))),spawn,noBuild};
}
function cheeseholdIsEnemyOnlyCell(x,y){return cheeseholdBreaches.some(g=>g.enemyOnly.has(K(x,y)));}
function cheeseholdIsNoBuildCell(x,y){return cheeseholdBreaches.some(g=>g.noBuild.has(K(x,y)));}
function cheeseholdActiveBreaches(){
 if(!cheeseholdBreaches.length)return[];
 const count=Math.min(cheeseholdBreaches.length,cheeseholdActiveFrontCount()),wave=game?.wave?.number||1,round=game?.round||1,start=(round*3+wave*2)%cheeseholdBreaches.length,out=[];
 for(let i=0;i<count;i++){
  const idx=(start+Math.floor(i*cheeseholdBreaches.length/count))%cheeseholdBreaches.length;
  if(!out.includes(cheeseholdBreaches[idx]))out.push(cheeseholdBreaches[idx]);
 }
 return out;
}
function cheeseholdDrawBreaches(){
 if(!decorGroup)return;
 if(cheeseholdBreachGroup){decorGroup.remove(cheeseholdBreachGroup);disposeObject(cheeseholdBreachGroup)}
 const g=new THREE.Group();g.userData.cheeseholdBreaches=true;
 const active=new Set(cheeseholdActiveBreaches().map(x=>x.index));
 for(const gate of cheeseholdBreaches){
  const hot=active.has(gate.index),opacity=hot?.26:.11;
  for(const c of gate.lane){
   const p=worldPos(c.x,c.y,.018),plane=new THREE.Mesh(new THREE.PlaneGeometry(CELL*.94,CELL*.94),new THREE.MeshBasicMaterial({color:CHEESEHOLD_BREACH_COLOR,transparent:true,opacity,side:THREE.DoubleSide,depthWrite:false}));
   plane.rotation.x=-Math.PI/2;plane.position.copy(p);g.add(plane);
  }
  const width=CELL*3.05,gateMesh=(gate.edge==='top'||gate.edge==='bottom')?box(width,1.36,.08,0x14070a,.8,.02):box(.08,1.36,width,0x14070a,.8,.02);
  const cx=gate.edge==='left'?(-COLS*CELL/2-.13):gate.edge==='right'?(COLS*CELL/2+.13):((gate.pos-(COLS-1)/2)*CELL);
  const cz=gate.edge==='top'?(-ROWS*CELL/2-.13):gate.edge==='bottom'?(ROWS*CELL/2+.13):((gate.pos-(ROWS-1)/2)*CELL);
  gateMesh.position.set(cx,.68,cz);gateMesh.material.emissive?.setHex?.(hot?0x7a111c:0x33090e);gateMesh.material.emissiveIntensity=hot?.8:.3;g.add(gateMesh);
  const label=typeof sectorLabelSprite==='function'?sectorLabelSprite(hot?'ACTIVE BREACH':'ENEMY BREACH',CHEESEHOLD_BREACH_COLOR):null;
  if(label){label.scale.multiplyScalar(.78);const anchor=gate.spawn[1]||gate.spawn[0];label.position.copy(worldPos(anchor.x,anchor.y,.5));g.add(label)}
 }
 cheeseholdBreachGroup=g;decorGroup.add(g);
}
function cheeseholdRefreshBreachVisuals(){if(cheeseholdBreaches.length)cheeseholdDrawBreaches();}

const cheeseholdBalanceBaseBuildArena=buildArena;
buildArena=function(round){const out=cheeseholdBalanceBaseBuildArena(round);cheeseholdConfigureBreaches(round);cheeseholdDrawBreaches();return out;};

const cheeseholdBalanceBasePlayerCanMove=playerCanMove;
playerCanMove=function(x,y){if(cheeseholdIsEnemyOnlyCell(x,y))return false;return cheeseholdBalanceBasePlayerCanMove(x,y);};

const cheeseholdBalanceBaseBuildDirection=buildDirection;
buildDirection=function(dirIndex=player?.lastDir){
 if(player&&dirIndex!=null){const d=DIRS[dirIndex],x=player.gx+d.x,y=player.gy+d.y,existing=buildAt(x,y);if(!existing&&cheeseholdIsNoBuildCell(x,y)){buildFail('Enemy breach lane — build farther back');return false}}
 return cheeseholdBalanceBaseBuildDirection(dirIndex);
};

if(typeof sectorBuildEmergencyWall==='function'){
 const cheeseholdBalanceBaseEmergencyWall=sectorBuildEmergencyWall;
 sectorBuildEmergencyWall=function(owner,x,y){if(cheeseholdIsNoBuildCell(x,y))return false;return cheeseholdBalanceBaseEmergencyWall(owner,x,y);};
}

if(typeof safeOppositeEdgeSpot==='function'){
 const cheeseholdBalanceBaseOppositeSpot=safeOppositeEdgeSpot;
 safeOppositeEdgeSpot=function(offset=0){
  const gates=cheeseholdActiveBreaches();
  if(gates.length){
   const gate=gates[Math.abs(offset)%gates.length],cells=gate.spawn.concat(gate.lane);
   for(let i=0;i<cells.length;i++){
    const c=cells[(i+Math.abs(offset))%cells.length];
    if(inside(c.x,c.y)&&!terrain.has(K(c.x,c.y))&&!buildAt(c.x,c.y)&&!occupiedEnemy(c.x,c.y))return[c.x,c.y];
   }
  }
  return cheeseholdBalanceBaseOppositeSpot(offset);
 };
}
if(typeof safeEdgeSpot==='function'){
 const cheeseholdBalanceBaseEdgeSpot=safeEdgeSpot;
 safeEdgeSpot=function(offset=0){
  const gates=cheeseholdActiveBreaches();
  if(gates.length){const gate=gates[Math.abs(offset)%gates.length];for(const c of gate.spawn)if(!occupiedEnemy(c.x,c.y)&&!buildAt(c.x,c.y))return[c.x,c.y];}
  return cheeseholdBalanceBaseEdgeSpot(offset);
 };
}

function cheeseholdEnemyRole(e){
 const type=e?.type||'',variant=e?.variant||'';
 if(type==='mole')return'sapper';
 if(type==='badger')return(game.round>=8||/black/i.test(variant))?'bombardier':'sapper';
 if(type==='weasel'||type==='shrew')return'flanker';
 if(type==='ferret')return'raider';
 if(type==='ox'||type==='brute')return'siege';
 return null;
}
function cheeseholdDecorateEnemyRole(e){
 if(!e?.mesh||!e.role)return;const color=CHEESEHOLD_ROLE_COLORS[e.role]||0xffffff,ring=new THREE.Mesh(new THREE.TorusGeometry(.24,.035,6,18),new THREE.MeshBasicMaterial({color,transparent:true,opacity:.9}));ring.rotation.x=Math.PI/2;ring.position.y=(e.type==='ox'||e.type==='brute'||e.type==='badger')?1.18:.84;ring.userData.enemyJob=true;e.mesh.add(ring);
}
function cheeseholdAssignEnemyRole(e){
 if(!e||e.dead||e.role)return e;const role=cheeseholdEnemyRole(e);if(!role)return e;e.role=role;
 if(role==='sapper'){e.wall*=1.45;e.damage*=1.05;}
 else if(role==='raider'){e.baseMove=Math.max(.095,e.baseMove*.82);e.damage*=.92;}
 else if(role==='flanker'){e.baseMove=Math.max(.09,e.baseMove*.78);e.wall*=.82;}
 else if(role==='bombardier'){e.baseMove=Math.max(.12,e.baseMove*1.08);e.attackEvery*=1.28;e.bombardRange=4.1;}
 else if(role==='siege'){e.wall*=1.32;e.damage*=1.08;e.baseMove=Math.max(.11,e.baseMove*.94);}
 cheeseholdDecorateEnemyRole(e);if(typeof sectorIdAt==='function')cheeseholdThreatenedSectors.add(sectorIdAt(e.gx,e.gy));return e;
}

const cheeseholdBalanceBaseSpawnEnemy=spawnEnemy;
spawnEnemy=function(){const before=new Set(enemies),out=cheeseholdBalanceBaseSpawnEnemy();if(out){const e=enemies.find(x=>!before.has(x));if(e)cheeseholdAssignEnemyRole(e)}return out;};

function cheeseholdPathAdjacentToBuilding(e,target){
 if(!e||!target||target.dead)return null;const q=[{x:e.gx,y:e.gy,path:[]}],seen=new Set([K(e.gx,e.gy)]);
 for(let qi=0;qi<q.length;qi++){
  const c=q[qi];if(Math.abs(c.x-target.gx)+Math.abs(c.y-target.gy)===1)return c.path;
  for(const n of neighbors(c.x,c.y)){
   const kk=K(n.x,n.y);if(!inside(n.x,n.y)||seen.has(kk)||enemyBlocked(n.x,n.y))continue;
   seen.add(kk);q.push({x:n.x,y:n.y,path:c.path.concat({x:n.x,y:n.y})});
  }
 }
 return null;
}
function cheeseholdPreferredRoleBuilding(e,role){
 const live=buildings.filter(b=>!b.dead);if(!live.length)return null;
 let pool=live;
 if(role==='sapper'||role==='siege')pool=live.filter(b=>b.type==='wall'||b.type==='superwall');
 else if(role==='raider')pool=live.filter(b=>b.type==='generator'||b.type==='phoenix'||(typeof SECTOR_SUMMON_MAJOR_IDS!=='undefined'&&SECTOR_SUMMON_MAJOR_IDS.has(b.majorEvolution))||b.type==='tesla'||b.type==='turret');
 if(!pool.length)return null;
 return pool.slice().sort((a,b)=>{const da=Math.abs(e.gx-a.gx)+Math.abs(e.gy-a.gy),db=Math.abs(e.gx-b.gx)+Math.abs(e.gy-b.gy);return da-db})[0];
}
function cheeseholdMoveRoleToward(e,target){
 const path=cheeseholdPathAdjacentToBuilding(e,target);if(!path)return false;
 if(Math.abs(e.gx-target.gx)+Math.abs(e.gy-target.gy)===1){attackBuilding(e,target);schedule(e);return true;}
 if(path.length){const n=path[0];if(!occupiedEnemy(n.x,n.y,e)){const slow=e.slow>game.time?1.7:1;startMove(e,n.x,n.y,Math.max(.085,e.baseMove*.58*slow));schedule(e);return true;}}
 return false;
}
function cheeseholdBombardTarget(e){
 const range=e.bombardRange||4.1,candidates=buildings.filter(b=>!b.dead&&dist(e,b)<=range);if(!candidates.length)return null;
 return candidates.map(b=>({b,cluster:buildings.filter(x=>!x.dead&&dist(b,x)<=1.9).length,priority:(b.type==='generator'||b.type==='turret'||b.type==='tesla')?1:0})).sort((a,b)=>(b.cluster*4+b.priority)-(a.cluster*4+a.priority)||dist(e,a.b)-dist(e,b.b))[0]?.b||null;
}
function cheeseholdBombard(e,target){
 if(!target||target.dead)return false;if(game.time<e.nextAttack){e.nextMove=game.time+.16;return true;}
 e.nextAttack=game.time+e.attackEvery;e.nextMove=game.time+.22;orient(e,target.gx-e.gx,target.gy-e.gy);pulse(target.gx,target.gy,CHEESEHOLD_ROLE_COLORS.bombardier,1.1);damageBuilding(target,e.damage*.72*(game.mutator?.wallDamage||1));return true;
}

const cheeseholdBalanceBaseEnemyUpdate=enemyUpdate;
enemyUpdate=function(e,dt,isCat=false){
 if(!e||e.dead||isCat)return cheeseholdBalanceBaseEnemyUpdate(e,dt,isCat);
 cheeseholdAssignEnemyRole(e);
 if(e.moving||e.attackAnim||game.time<e.nextMove||e.poisonUntil>game.time&&game.time>=e.poisonTick||adjacentPoison(e))return cheeseholdBalanceBaseEnemyUpdate(e,dt,false);
 if(e.role==='bombardier'){const t=cheeseholdBombardTarget(e);if(t&&cheeseholdBombard(e,t))return;}
 if(e.role==='flanker'&&player){const path=pathfind({x:e.gx,y:e.gy},{x:player.gx,y:player.gy});if(path?.length){const n=path[0];if(!occupiedEnemy(n.x,n.y,e)){startMove(e,n.x,n.y,Math.max(.08,e.baseMove*.55));schedule(e);return}}}
 if(e.role==='raider'||e.role==='sapper'||e.role==='siege'){const target=cheeseholdPreferredRoleBuilding(e,e.role);if(target&&cheeseholdMoveRoleToward(e,target))return;}
 return cheeseholdBalanceBaseEnemyUpdate(e,dt,false);
};

// Turrets keep strong early upgrades, but extreme stacked damage/range/fire-rate builds now receive gentle diminishing returns.
function cheeseholdSoftUpper(value,knee,slope=.58){value=Number(value)||1;return value<=knee?value:knee+(value-knee)*slope;}
function cheeseholdSoftFire(value,knee=.72,slope=.46){value=Math.max(.18,Number(value)||1);return value>=knee?value:knee-(knee-value)*slope;}
const cheeseholdBalanceBaseBuildingEvoStats=buildingEvoStats;
buildingEvoStats=function(b){
 const out=cheeseholdBalanceBaseBuildingEvoStats(b);if(b?.type!=='turret')return out;
 const levels=Math.max(0,(b.level||1)-1),rawDmg=Math.max(.001,game.mods.turretDamage||1),rawRange=Math.max(.001,game.mods.turretRange||1),rawFire=Math.max(.001,game.mods.turretFire||1);
 const effDmg=cheeseholdSoftUpper(rawDmg,1.62,.58),effRange=cheeseholdSoftUpper(rawRange,1.34,.5),effFire=cheeseholdSoftFire(rawFire,.72,.46);
 out.damage*=effDmg/rawDmg;out.range*=effRange/rawRange;out.fire*=effFire/rawFire;
 // Level scaling is slightly flatter: damage 28% -> 22%, range 8% -> 6%, fire progression 8% -> 6% per level.
 out.damage*=(1+levels*.22)/(1+levels*.28);out.range*=(1+levels*.06)/(1+levels*.08);out.fire*=Math.pow(1.08/1.06,levels);
 return out;
};

if(typeof sectorAllyDefinition==='function'){
 const cheeseholdBalanceBaseAllyDefinition=sectorAllyDefinition;
 sectorAllyDefinition=function(type){const d=cheeseholdBalanceBaseAllyDefinition(type),cap=CHEESEHOLD_SUMMON_RANGE_CAPS[type];if(cap!=null)d.range=Math.min(d.range,cap);d.awareness=CHEESEHOLD_SUMMON_AWARENESS[type]||Math.max(6,d.range*2.4);return d;};
}
function cheeseholdAllyPatrolTarget(a,d){
 const owner=sectorAllyOwner(a);if(!owner)return null;
 const radius=d.flying?5:4;
 if(a.patrolTarget&&game.time<(a.patrolUntil||0)&&Math.abs(a.gx-a.patrolTarget.gx)+Math.abs(a.gy-a.patrolTarget.gy)>0)return a.patrolTarget;
 for(let i=0;i<18;i++){
  const dx=Math.floor(Math.random()*(radius*2+1))-radius,dy=Math.floor(Math.random()*(radius*2+1))-radius;if(Math.abs(dx)+Math.abs(dy)<2||Math.abs(dx)+Math.abs(dy)>radius+1)continue;
  const x=owner.gx+dx,y=owner.gy+dy;if(!inside(x,y)||!sectorAllowedForAlly(a,x,y)||(!d.flying&&terrain.has(K(x,y)))||buildAt(x,y)||sectorEnemyOccupiesCell(x,y))continue;
  a.patrolTarget={gx:x,gy:y};a.patrolUntil=game.time+2.2+Math.random()*2.2;return a.patrolTarget;
 }
 a.patrolTarget={gx:owner.gx,gy:owner.gy};a.patrolUntil=game.time+1.5;return a.patrolTarget;
}
function cheeseholdAllyTargets(a,d){
 const all=sectorEnemyTargetsFor(a),awareness=d.awareness||7;
 return all.filter(t=>Math.abs(a.gx-t.gx)+Math.abs(a.gy-t.gy)<=awareness);
}
function cheeseholdAllyStepAway(a,target,d){
 const dx=a.gx-target.gx,dy=a.gy-target.gy,fake={gx:a.gx+(dx||((a.id%2)?1:-1))*4,gy:a.gy+(dy||((a.id%3)?1:-1))*4};sectorStepAlly(a,fake,d);
}
if(typeof sectorTickAllies==='function'){
 sectorTickAllies=function(){
  for(const a of [...sectorAllies]){
   if(a.dead)continue;const d=sectorAllyDefinition(a.type),owner=sectorAllyOwner(a),profile=sectorSummonerProfile(owner);sectorEnsureAllyBar?.(a);
   const aura=typeof sectorAllyAuraStats==='function'?sectorAllyAuraStats(a):{ttl:0};if(game.time-a.born>d.ttl+(aura.ttl||0)){sectorKillAlly(a);continue}
   if(d.repair){const ox=a.gx,oy=a.gy,oldAttack=a.nextAttack;sectorRepairBotTick(a,d,profile||{repair:true});if(a.gx===ox&&a.gy===oy&&a.nextAttack===oldAttack){const patrol=cheeseholdAllyPatrolTarget(a,d);if(patrol)sectorStepAlly(a,patrol,d)}continue}
   const targets=cheeseholdAllyTargets(a,d),target=sectorNearestByGrid(a,targets);
   if(!target){const patrol=cheeseholdAllyPatrolTarget(a,d);if(patrol)sectorStepAlly(a,patrol,d);continue}
   a.patrolTarget=null;const md=Math.abs(a.gx-target.gx)+Math.abs(a.gy-target.gy),preferred=(d.range>=2?Math.max(1.4,d.range*.58):0);
   if(preferred&&md<preferred&&!d.guard){cheeseholdAllyStepAway(a,target,d);continue}
   if(md<=d.range)sectorAttackWithAlly(a,target,d);else sectorStepAlly(a,target,d);
  }
 };
}

function cheeseholdDefenseBuilding(b){
 if(!b||b.dead||b.type==='generator')return false;
 if(['turret','tesla','slow','poison','phoenix','superwall'].includes(b.type))return true;
 return typeof SECTOR_SUMMON_MAJOR_IDS!=='undefined'&&SECTOR_SUMMON_MAJOR_IDS.has(b.majorEvolution);
}
function cheeseholdRewardDefendedSectors(w){
 if(!w||w.cheeseholdDefenseRewarded)return;w.cheeseholdDefenseRewarded=true;
 if(typeof sectorIdAt!=='function'||!cheeseholdThreatenedSectors.size)return;
 const defended=[];
 for(const sid of cheeseholdThreatenedSectors){
  const live=buildings.filter(b=>!b.dead&&sectorIdAt(b.gx,b.gy)===sid),defenses=live.filter(cheeseholdDefenseBuilding);
  if(live.length>=2&&defenses.length>=1)defended.push(sid);
 }
 if(!defended.length)return;
 const count=Math.min(4,defended.length),cheeseEach=1+Math.floor((game.round||1)/5),xpEach=1+Math.floor((game.round||1)/6),money=count*cheeseEach,xp=count*xpEach;
 game.cheese+=money;if(xp>0)addXP(xp);toast('Defended '+count+' sector'+(count===1?'':'s')+' · +'+money+' cheese · +'+xp+' XP');
}
function cheeseholdDefenseMonitor(){
 if(game?.running&&!game.paused&&typeof sectorIdAt==='function'&&game.time>=cheeseholdDefenseTickAt){
  cheeseholdDefenseTickAt=game.time+.28;const w=game.wave,key=w?(game.round+':'+w.number):'';
  if(key!==cheeseholdDefenseWaveKey){cheeseholdDefenseWaveKey=key;cheeseholdThreatenedSectors.clear();cheeseholdRefreshBreachVisuals();}
  for(const e of enemies)if(!e.dead)cheeseholdThreatenedSectors.add(sectorIdAt(e.gx,e.gy));
  if(cat&&!cat.dead)cheeseholdThreatenedSectors.add(sectorIdAt(cat.gx,cat.gy));
  if(w?.clearAt!=null)cheeseholdRewardDefendedSectors(w);
 }
 requestAnimationFrame(cheeseholdDefenseMonitor);
}

if(typeof setWave==='function'){
 const cheeseholdBalanceBaseSetWave=setWave;
 setWave=function(number){const out=cheeseholdBalanceBaseSetWave(number);cheeseholdDefenseWaveKey=game.round+':'+number;cheeseholdThreatenedSectors.clear();cheeseholdRefreshBreachVisuals();return out;};
}

// The progression modules may load after the first arena has already been created.
if(game?.running){cheeseholdConfigureBreaches(game.round);cheeseholdDrawBreaches();for(const e of enemies)cheeseholdAssignEnemyRole(e);}
cheeseholdDefenseMonitor();
console.info('[Cheesehold] breach fronts + enemy jobs + mobile summons + sector defense rewards loaded');
