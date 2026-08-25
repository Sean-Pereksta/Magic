// Cheesehold summon combat, aura awakening, and Level 25 Mythic progression.
// Loaded after the sector/summoner runtime inside the shared progression eval.

const SECTOR_AURA_LEVEL=18;
const SECTOR_MYTHIC_LEVEL=25;
const SECTOR_AURA_RANGE=4.6;
const SECTOR_AURA_CHOICES={
 generator:[
  {id:'rallyGrid',icon:'⚑',name:'Rally Grid',desc:'Nearby summons hit harder while nearby attack structures cycle faster.',ally:{damage:1.2},building:{fire:.9,damage:1.08}},
  {id:'renewalGrid',icon:'✚',name:'Renewal Grid',desc:'Nearby summons regenerate health and nearby structures steadily repair.',ally:{regen:2.4},repair:2.6},
  {id:'logisticsField',icon:'➤',name:'Logistics Field',desc:'Nearby summons reposition faster and nearby summoner buildings replace units sooner.',ally:{move:.82},summoner:{rate:.82}}
 ],
 superwall:[
  {id:'bulwarkCommand',icon:'🛡',name:'Bulwark Command',desc:'Nearby summons take less damage and nearby structures gain damage resistance.',ally:{damageTaken:.78},buildingTaken:.82},
  {id:'garrisonStandard',icon:'♜',name:'Garrison Standard',desc:'Nearby summons attack faster and recover a small amount of health.',ally:{attack:.84,regen:1.1}},
  {id:'fortressLink',icon:'▦',name:'Fortress Link',desc:'Nearby walls and support buildings repair faster while attack structures gain range.',repair:1.8,building:{range:1.1}}
 ],
 phoenix:[
  {id:'emberChorus',icon:'🔥',name:'Ember Chorus',desc:'Nearby summons gain a large damage boost and nearby attack structures gain damage.',ally:{damage:1.25},building:{damage:1.12}},
  {id:'secondWind',icon:'♻',name:'Second Wind',desc:'Nearby summons regenerate and remain deployed longer before expiring.',ally:{regen:1.8,ttl:18}},
  {id:'sunwardBeacon',icon:'☀',name:'Sunward Beacon',desc:'Nearby structures fire faster and nearby summons take modestly less damage.',ally:{damageTaken:.9},building:{fire:.88}}
 ],
 slow:[
  {id:'frostCommand',icon:'❄',name:'Frost Command',desc:'Nearby summons attack faster and nearby slow structures project wider control fields.',ally:{attack:.82},building:{slowRadius:1.18,slowDuration:1.12}},
  {id:'timeWard',icon:'◴',name:'Time Ward',desc:'Nearby attack structures cycle faster while summons reposition more quickly.',ally:{move:.84},building:{fire:.86}},
  {id:'winterShelter',icon:'◇',name:'Winter Shelter',desc:'Nearby summons take less damage and nearby structures receive steady repairs.',ally:{damageTaken:.86},repair:1.7}
 ],
 tesla:[
  {id:'conductiveNetwork',icon:'ϟ',name:'Conductive Network',desc:'Nearby summons attack faster and nearby Tesla structures gain chain capacity.',ally:{attack:.84},building:{chains:1,fire:.92}},
  {id:'overclockAura',icon:'⚡',name:'Overclock Aura',desc:'Nearby attack structures cycle much faster at the cost of no defensive benefit.',building:{fire:.78}},
  {id:'staticWard',icon:'◎',name:'Static Ward',desc:'Nearby summons take less damage while nearby attack structures gain modest range.',ally:{damageTaken:.84},building:{range:1.08}}
 ]
};

const CHEESEHOLD_MYTHICS={
 tesla:[
  {id:'machineAscendancy',icon:'🦾',name:'Machine Ascendancy',desc:'Robot Foundries command a six-unit army, deploy faster, and roam across two sectors.',requiresMajor:'robotFoundry',summoner:true,mul:{hp:1.28}},
  {id:'stormCrown',icon:'⚡',name:'Storm Crown',desc:'Non-summoner Tesla towers become mythic storm engines with extreme chaining and fire rate.',mul:{damage:1.8,fire:.72,range:1.2},add:{chains:8}}
 ],
 turret:[
  {id:'mouseEmpire',icon:'♛',name:'Mouse Empire',desc:'Mouse Armories field a six-unit veteran force with elite damage and wider reinforcement reach.',requiresMajor:'mouseArmory',summoner:true},
  {id:'starbreaker',icon:'☄',name:'Starbreaker Battery',desc:'Non-summoner turrets gain mythic range, damage, and boss execution power.',mul:{damage:2.05,range:1.5,bossDamage:1.45,fire:.88}}
 ],
 generator:[
  {id:'eternalWorks',icon:'🏗',name:'Eternal Works',desc:'Engineer Workshops maintain five Repair Bots with stronger repairs and more emergency construction.',requiresMajor:'engineerWorkshop',summoner:true,mul:{hp:1.35}},
  {id:'infiniteMint',icon:'♦',name:'Infinite Mint',desc:'Non-summoner generators reach mythic output while remaining subject to sector saturation.',mul:{genRate:.58},add:{yield:8}}
 ],
 superwall:[
  {id:'livingFortress',icon:'🏰',name:'Living Fortress',desc:'Gatehouses field a six-unit garrison with elite guards and rapid replacements.',requiresMajor:'gatehouse',summoner:true,mul:{hp:1.55}},
  {id:'immortalRampart',icon:'█',name:'Immortal Rampart',desc:'Non-summoner Super Walls gain colossal durability and retaliation.',mul:{hp:3.1},add:{retaliation:38}}
 ],
 phoenix:[
  {id:'solarFlock',icon:'☀',name:'Solar Flock',desc:'Ember Nests maintain six phoenix units with frequent Greater Phoenix deployments.',requiresMajor:'emberNest',summoner:true,mul:{hp:1.3}},
  {id:'everdawn',icon:'✹',name:'Everdawn Shrine',desc:'Non-summoner Phoenix structures become mythic resurrection bastions.',mul:{hp:2.7},add:{reviveHp:120,phoenixSave:.2}}
 ],
 slow:[
  {id:'whiteHost',icon:'❄',name:'White Host',desc:'Frost Shrines command six mobile frost summons and reinforce neighboring sectors.',requiresMajor:'frostShrine',summoner:true},
  {id:'timeWinter',icon:'◇',name:'Time Winter',desc:'Non-summoner frost structures gain enormous radius, duration, and control damage.',mul:{slowRadius:1.8,slowDuration:1.7},add:{slowDamage:16}}
 ],
 poison:[
  {id:'apexBrood',icon:'♛',name:'Apex Brood',desc:'Brood Vats maintain seven summons with faster replacement and multiple Brood Mothers.',requiresMajor:'broodVat',summoner:true},
  {id:'blackGarden',icon:'☣',name:'Black Garden',desc:'Non-summoner poison structures gain mythic damage, range, duration, and spread.',mul:{poisonDamage:2.0,range:1.35,poisonDuration:1.35},add:{poisonSpread:3}}
 ]
};

function sectorAuraChoice(b){return (SECTOR_AURA_CHOICES[b?.type]||[]).find(x=>x.id===b?.auraEvolution)||null;}
function sectorMythicChoice(b){return (CHEESEHOLD_MYTHICS[b?.type]||[]).find(x=>x.id===b?.mythicEvolution)||null;}
function sectorAuraSourcesNear(target,range=SECTOR_AURA_RANGE){return buildings.filter(s=>s!==target&&!s.dead&&s.auraEvolution&&dist(s,target)<=range).slice(0,4);}
function sectorAuraSourcesNearAlly(a,range=SECTOR_AURA_RANGE){return buildings.filter(s=>!s.dead&&s.auraEvolution&&Math.hypot(s.gx-a.gx,s.gy-a.gy)<=range).slice(0,4);}
function sectorAllyAuraStats(a){const out={damage:1,attack:1,move:1,damageTaken:1,regen:0,ttl:0};for(const s of sectorAuraSourcesNearAlly(a)){const aura=sectorAuraChoice(s)?.ally;if(!aura)continue;if(aura.damage)out.damage*=aura.damage;if(aura.attack)out.attack*=aura.attack;if(aura.move)out.move*=aura.move;if(aura.damageTaken)out.damageTaken*=aura.damageTaken;if(aura.regen)out.regen+=aura.regen;if(aura.ttl)out.ttl+=aura.ttl;}out.damage=Math.min(1.65,out.damage);out.attack=Math.max(.62,out.attack);out.move=Math.max(.68,out.move);out.damageTaken=Math.max(.58,out.damageTaken);out.regen=Math.min(7,out.regen);out.ttl=Math.min(36,out.ttl);return out;}
function sectorSummonerAuraRate(b){let rate=1;for(const s of sectorAuraSourcesNear(b)){const v=sectorAuraChoice(s)?.summoner?.rate;if(v)rate*=v;}return Math.max(.65,rate);}
function sectorBuildingAuraTaken(b){let mult=1;for(const s of sectorAuraSourcesNear(b)){const v=sectorAuraChoice(s)?.buildingTaken;if(v)mult*=v;}return Math.max(.6,mult);}

const sectorAuraBaseBuildingEvoStats=buildingEvoStats;
buildingEvoStats=function(b){const out=sectorAuraBaseBuildingEvoStats(b),mythic=sectorMythicChoice(b);if(mythic){for(const [k,v] of Object.entries(mythic.mul||{}))out[k]=(out[k]??1)*v;for(const [k,v] of Object.entries(mythic.add||{}))out[k]=(out[k]??0)+v}for(const s of sectorAuraSourcesNear(b)){const bonus=sectorAuraChoice(s)?.building;if(!bonus)continue;for(const [k,v] of Object.entries(bonus)){if(['chains','yield','retaliation','slowDamage','repairAura','poisonSpread'].includes(k))out[k]=(out[k]??0)+v;else out[k]=(out[k]??1)*v}}return out;};

const sectorAuraBaseDamageBuilding=damageBuilding;
damageBuilding=function(b,amount){return sectorAuraBaseDamageBuilding(b,amount*sectorBuildingAuraTaken(b));};

const sectorAuraBaseSummonerProfile=sectorSummonerProfile;
sectorSummonerProfile=function(b){const p=sectorAuraBaseSummonerProfile(b);if(!p)return p;p.interval*=sectorSummonerAuraRate(b);switch(b.mythicEvolution){
 case 'machineAscendancy':Object.assign(p,{cap:6,interval:Math.min(p.interval,5.2),units:['sparkBot','arcWalker','heavyMech'],roam:2,eliteDamage:1.28});break;
 case 'mouseEmpire':Object.assign(p,{cap:6,interval:Math.min(p.interval,5.8),units:['rifleMouse','marksman'],roam:2,eliteDamage:1.65});break;
 case 'eternalWorks':Object.assign(p,{cap:5,interval:Math.min(p.interval,5.9),units:['repairBot'],repair:true,rebuildWalls:2,newWalls:3,repairMult:2});break;
 case 'livingFortress':Object.assign(p,{cap:6,interval:Math.min(p.interval,6.1),units:['shieldGuard','spearGuard'],roam:1,guardElite:true,eliteDamage:1.25});break;
 case 'solarFlock':Object.assign(p,{cap:6,interval:Math.min(p.interval,5.1),units:['phoenixling','greaterPhoenix'],roam:2,eliteDamage:1.3});break;
 case 'whiteHost':Object.assign(p,{cap:6,interval:Math.min(p.interval,5.7),units:['frostWisp','frostGuardian'],roam:2,eliteDamage:1.2});break;
 case 'apexBrood':Object.assign(p,{cap:7,interval:Math.min(p.interval,5.1),units:['venomCrawler','broodMother','venomCrawler'],roam:2,eliteDamage:1.3});break;
 }return p;};

function sectorEnsureAllyBar(a){if(!a||a.bar||!a.mesh)return;const d=sectorAllyDefinition(a.type),large=['heavyMech','greaterPhoenix','broodMother','shieldGuard','frostGuardian'].includes(a.type);a.bar=healthBar(large?.72:.54);a.bar.position.y=large?1.02:.78;a.mesh.add(a.bar);}
function sectorAllyCellMates(a,x=a.gx,y=a.gy){return sectorAllies.filter(o=>o!==a&&!o.dead&&o.gx===x&&o.gy===y);}
function sectorPlaceAlly(a){if(!a?.mesh)return;const mates=[a,...sectorAllyCellMates(a)].sort((x,y)=>x.id-y.id),idx=mates.indexOf(a),count=mates.length,p=worldPos(a.gx,a.gy,.12);if(count>1){const angle=idx/count*Math.PI*2,r=.17;p.x+=Math.cos(angle)*r;p.z+=Math.sin(angle)*r;}a.mesh.position.copy(p);}
function sectorNormalizeAllyCell(x,y){for(const a of sectorAllies)if(!a.dead&&a.gx===x&&a.gy===y)sectorPlaceAlly(a);}
function sectorEnemyOccupiesCell(x,y){if(cat&&!cat.dead&&cat.gx===x&&cat.gy===y)return true;return enemies.some(e=>!e.dead&&e.gx===x&&e.gy===y);}
function sectorCellFreeForAlly(a,x,y,d,allowSqueeze=false){if(!inside(x,y)||(!d.flying&&terrain.has(K(x,y)))||buildAt(x,y)||sectorEnemyOccupiesCell(x,y))return false;if(!sectorAllowedForAlly(a,x,y))return false;const mates=sectorAllies.filter(o=>o!==a&&!o.dead&&o.gx===x&&o.gy===y);if(!mates.length)return true;if(d.flying)return mates.length<2;return allowSqueeze&&!d.guard&&mates.length<2;}
function sectorStepAlly(a,target,d){if(game.time<a.nextMove)return;const aura=sectorAllyAuraStats(a);a.nextMove=game.time+d.move*aura.move;let candidates=DIR4.map(dir=>[a.gx+dir.x,a.gy+dir.y]).filter(([x,y])=>sectorCellFreeForAlly(a,x,y,d,false));if(!candidates.length){a.stuckSince=a.stuckSince||game.time;if(game.time-a.stuckSince>.8)candidates=DIR4.map(dir=>[a.gx+dir.x,a.gy+dir.y]).filter(([x,y])=>sectorCellFreeForAlly(a,x,y,d,true));}else a.stuckSince=0;if(!candidates.length)return;candidates.sort((p,q)=>{const pd=Math.abs(p[0]-target.gx)+Math.abs(p[1]-target.gy),qd=Math.abs(q[0]-target.gx)+Math.abs(q[1]-target.gy),pc=sectorAllies.filter(o=>!o.dead&&o.gx===p[0]&&o.gy===p[1]).length,qc=sectorAllies.filter(o=>!o.dead&&o.gx===q[0]&&o.gy===q[1]).length;return (pd+pc*2.5)-(qd+qc*2.5)});const ox=a.gx,oy=a.gy,p=candidates[0];a.gx=p[0];a.gy=p[1];sectorNormalizeAllyCell(ox,oy);sectorNormalizeAllyCell(a.gx,a.gy);}
function sectorAttackWithAlly(a,target,d){if(game.time<a.nextAttack)return;const aura=sectorAllyAuraStats(a);a.nextAttack=game.time+d.attack*aura.attack;let dmg=d.damage*(1+(game.round-1)*.075)*(game.mods.summonDamage||1)*(a.eliteDamage||1)*aura.damage;damageEnemy(target,dmg,target===cat);if(d.fire)applyBurnStatus(target,a.type==='greaterPhoenix'?1.6:1,3.5,a);if(d.frost)applyChillStatus(target,d.frost,2.8);if(d.poison)applyPoisonStatus(target,1,Math.max(3,game.mods.poisonDuration*.55),a);if(d.shock)target.conductiveUntil=Math.max(target.conductiveUntil||0,game.time+3);if(d.splash)splashDamage(target,1.45,dmg*.32,SECTOR_SUMMON_COLORS[a.type],target);if(d.guard)target.slow=Math.max(target.slow||0,game.time+.7);pulse(target.gx,target.gy,SECTOR_SUMMON_COLORS[a.type]||0xffffff,.35);}
function sectorDamageAlly(a,amount,attacker=null){if(!a||a.dead)return false;const d=sectorAllyDefinition(a.type),aura=sectorAllyAuraStats(a),nativeArmor=d.guard?.76:a.type==='heavyMech'?.82:a.type==='frostGuardian'?.86:a.type==='repairBot'?.92:1;a.hp-=Math.max(1,amount*nativeArmor*aura.damageTaken);pulse(a.gx,a.gy,0xff7a72,.42);if(a.hp<=0){sectorKillAlly(a);return true}sectorEnsureAllyBar(a);return false;}

const sectorCombatBaseSpawnAlly=sectorSpawnAlly;
sectorSpawnAlly=function(owner,type,profile){const ok=sectorCombatBaseSpawnAlly(owner,type,profile);if(!ok)return ok;const a=sectorAllies[sectorAllies.length-1];if(a){sectorEnsureAllyBar(a);sectorPlaceAlly(a)}return ok;};

function sectorGroundAllyAt(x,y){return sectorAllies.find(a=>!a.dead&&!sectorAllyDefinition(a.type).flying&&a.gx===x&&a.gy===y)||null;}
function sectorPreferredEnemyAlly(e,isCat=false){if(!sectorAllies.length)return null;const aggro=isCat?3.2:6.2;let best=null,bestScore=Infinity;for(const a of sectorAllies){if(a.dead)continue;const d=sectorAllyDefinition(a.type),md=Math.abs(e.gx-a.gx)+Math.abs(e.gy-a.gy);if(md>aggro)continue;let score=md;if(d.guard)score-=2.2;if(a.type==='repairBot')score+=1.5;if(isCat&&!d.guard&&md>1)score+=2;if(score<bestScore){best=a;bestScore=score}}return best;}
function sectorPathToAlly(e,a){const goals=DIR4.map(d=>({x:a.gx+d.x,y:a.gy+d.y})).filter(g=>inside(g.x,g.y)&&!enemyBlocked(g.x,g.y));let best=null;for(const g of goals){const p=pathfind({x:e.gx,y:e.gy},g);if(p&&(!best||p.length<best.length))best=p;}return best;}
function sectorEnemyAttackAlly(e,a){if(!e||!a||a.dead||game.time<e.nextAttack)return false;e.nextAttack=game.time+e.attackEvery;startAttackAnimation(e,a,e.type==='ox'||e.type==='brute'?'ram':e.type==='mole'||e.type==='badger'?'dig':'strike');sectorDamageAlly(a,e.damage,e);return true;}
const sectorCombatBaseStartMove=startMove;
startMove=function(e,nx,ny,duration){if(e&&e!==player&&(e===cat||enemies.includes(e))){const ally=sectorGroundAllyAt(nx,ny);if(ally){sectorEnemyAttackAlly(e,ally);schedule(e);return false}}return sectorCombatBaseStartMove(e,nx,ny,duration);};
const sectorCombatBaseEnemyUpdate=enemyUpdate;
enemyUpdate=function(e,dt,isCat=false){if(!e||e.dead)return;const ally=sectorPreferredEnemyAlly(e,isCat);if(ally&&!e.moving&&game.time>=e.nextMove){const md=Math.abs(e.gx-ally.gx)+Math.abs(e.gy-ally.gy);if(md<=1){sectorEnemyAttackAlly(e,ally);schedule(e);return sectorCombatBaseEnemyUpdate(e,dt,isCat)}const path=sectorPathToAlly(e,ally);if(path?.length){const n=path[0];if(!sectorGroundAllyAt(n.x,n.y)&&!occupiedEnemy(n.x,n.y,e)){const slow=e.slow>game.time?1.7:1;sectorCombatBaseStartMove(e,n.x,n.y,Math.max(.09,e.baseMove*.62*slow));schedule(e);return sectorCombatBaseEnemyUpdate(e,dt,isCat)}}}return sectorCombatBaseEnemyUpdate(e,dt,isCat);};

function sectorAuraRepairTick(){for(const source of buildings){if(source.dead||!source.auraEvolution)continue;const aura=sectorAuraChoice(source);if(!aura?.repair)continue;for(const b of buildings)if(!b.dead&&dist(source,b)<=SECTOR_AURA_RANGE)b.hp=Math.min(b.maxHp,b.hp+aura.repair*.25);}}
function sectorTickAllies(){for(const a of [...sectorAllies]){if(a.dead)continue;const d=sectorAllyDefinition(a.type),owner=sectorAllyOwner(a),profile=sectorSummonerProfile(owner),aura=sectorAllyAuraStats(a);sectorEnsureAllyBar(a);if(aura.regen)a.hp=Math.min(a.maxHp,a.hp+aura.regen*.18);if(a.bar)updateHealthBar(a.bar,a.hp,a.maxHp,a.mesh);if(game.time-a.born>d.ttl+aura.ttl){sectorKillAlly(a);continue}if(d.repair){sectorRepairBotTick(a,d,profile||{repair:true});continue}const targets=sectorEnemyTargetsFor(a),target=sectorNearestByGrid(a,targets);if(!target)continue;const md=Math.abs(a.gx-target.gx)+Math.abs(a.gy-target.gy);if(md<=d.range)sectorAttackWithAlly(a,target,d);else sectorStepAlly(a,target,d);}}

function sectorOpenChoiceModal(b,kind,choices,onPick){game.paused=true;ui.upgradeKind.textContent=kind;ui.upgradeTitle.textContent=BUILDINGS[b.type].name+' — '+kind.split(' · ')[0];ui.upgradeSub.textContent=kind.startsWith('AURA')?'Choose a persistent local aura for nearby summons and structures.':'Choose a Level 25 Mythic transformation for this individual structure.';ui.upgradeChoices.innerHTML='';for(const choice of choices){const btn=document.createElement('button');btn.className='upgradeChoice';btn.innerHTML='<span class="upIcon">'+choice.icon+'</span><span class="upName">'+choice.name+'</span><span class="upDesc">'+choice.desc+'</span><span class="upRank">'+(choice.summoner?'SUMMONER · ':'')+kind+'</span>';btn.addEventListener('click',()=>{onPick(choice);ui.upgrade.classList.remove('open');game.paused=false;refreshBuildingMesh(b);toast(choice.name+' acquired');pulse(b.gx,b.gy,kind.startsWith('MYTHIC')?0xff70f5:0x72e4ff,2.2);updateUI()});ui.upgradeChoices.appendChild(btn)}ui.upgrade.classList.add('open');navigator.vibrate?.(kind.startsWith('MYTHIC')?[70,35,110]:[35,25,55]);}
function openSectorAuraEvolution(b){const choices=SECTOR_AURA_CHOICES[b.type]||[];if(!choices.length)return;sectorOpenChoiceModal(b,'AURA AWAKENING · LEVEL '+SECTOR_AURA_LEVEL,choices,c=>{b.auraEvolution=c.id});}
function sectorMythicChoices(b){const all=CHEESEHOLD_MYTHICS[b.type]||[],matching=all.filter(x=>x.requiresMajor===b.majorEvolution);return matching.length?matching:all.filter(x=>!x.requiresMajor);}
function openSectorMythicEvolution(b){const choices=sectorMythicChoices(b);if(!choices.length)return;sectorOpenChoiceModal(b,'MYTHIC EVOLUTION · LEVEL '+SECTOR_MYTHIC_LEVEL,choices,c=>{const oldMax=b.maxHp;b.mythicEvolution=c.id;b.maxHp=buildingMaxHp(b);b.hp=Math.min(b.maxHp,b.hp+Math.max(0,b.maxHp-oldMax))});}
const sectorAuraBaseUpgradeBuilding=upgradeBuilding;
upgradeBuilding=function(b){const ok=sectorAuraBaseUpgradeBuilding(b);if(!ok)return ok;if(b.level===SECTOR_AURA_LEVEL&&!b.auraEvolution&&!ui.upgrade.classList.contains('open')&&SECTOR_AURA_CHOICES[b.type])openSectorAuraEvolution(b);else if(b.level===SECTOR_MYTHIC_LEVEL&&!b.mythicEvolution&&!ui.upgrade.classList.contains('open')&&CHEESEHOLD_MYTHICS[b.type])openSectorMythicEvolution(b);return ok;};

const sectorAuraBaseRefreshBuildingMesh=refreshBuildingMesh;
refreshBuildingMesh=function(b){sectorAuraBaseRefreshBuildingMesh(b);if(!b?.mesh)return;if(b.auraEvolution){const ring=new THREE.Mesh(new THREE.TorusGeometry(.78,.035,8,32),new THREE.MeshBasicMaterial({color:0x72e4ff,transparent:true,opacity:.62}));ring.rotation.x=Math.PI/2;ring.position.y=.05;ring.userData.auraDecoration=true;b.mesh.add(ring)}if(b.mythicEvolution){const crown=new THREE.Mesh(new THREE.TorusGeometry(.3,.055,6,20),new THREE.MeshBasicMaterial({color:0xff70f5,transparent:true,opacity:.9}));crown.rotation.x=Math.PI/2;crown.position.y=1.7;crown.userData.mythicDecoration=true;b.mesh.add(crown)}};

let sectorAuraLastTick=0;
function sectorAuraRuntimeLoop(){if(game?.running&&!game.paused&&game.time-sectorAuraLastTick>=.25){sectorAuraLastTick=game.time;sectorAuraRepairTick()}requestAnimationFrame(sectorAuraRuntimeLoop);}
sectorAuraRuntimeLoop();

console.info('[Cheesehold] summon combat + aura awakening + mythic progression loaded');
