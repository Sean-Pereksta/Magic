// Cheesehold elemental progression expansion.
// Injected inside the core IIFE by Cheesehold_3D_Progression_Upgrade.html.

const ELEMENTAL_ATTACK_TYPES=new Set(['turret','poison','slow','tesla']);
const ELEMENTAL_MILESTONES={
 turret:{major:[
  {id:'incendiary',icon:'🔥',name:'Incendiary Battery',desc:'Rounds ignite targets and trigger Combustion on poisoned enemies.',mul:{damage:1.12},element:'fire'},
  {id:'cryoAmmo',icon:'❄',name:'Cryo Munitions',desc:'Shots stack chill and punish frozen targets.',mul:{damage:1.08},element:'frost'},
  {id:'arcAmmo',icon:'⚡',name:'Arc Ammunition',desc:'Electric rounds arc into nearby enemies and exploit conductive statuses.',mul:{fire:.94},element:'shock'}
 ],ultra:[
  {id:'sunCannon',icon:'☀',name:'Sun Cannon',desc:'Explosive incendiary shells cause huge burn and Combustion bursts.',mul:{damage:1.55,fire:1.18},element:'fire'},
  {id:'absoluteRail',icon:'━',name:'Absolute Rail',desc:'Piercing cryo rail shots shatter frozen enemies.',mul:{damage:1.75,range:1.3,fire:1.28},element:'frost'},
  {id:'tempestGatling',icon:'ϟ',name:'Tempest Gatling',desc:'Rapid electric fire repeatedly chains through conductive enemies.',mul:{damage:1.3,fire:.68},element:'shock'}
 ]},
 poison:{major:[
  {id:'inferno',icon:'🔥',name:'Inferno Distillery',desc:'Converts the tower to ranged flame with a toxic residue.',mul:{poisonDamage:.72,range:1.12},element:'fire'},
  {id:'plagueArtillery',icon:'☣',name:'Plague Artillery',desc:'Long-range toxic shells splash poison across clustered enemies.',mul:{poisonDamage:1.28,range:1.5,fire:1.18},add:{poisonSpread:1},element:'poison'},
  {id:'acidCrucible',icon:'◈',name:'Acid Crucible',desc:'Corrosive poison makes afflicted enemies take more damage.',mul:{poisonDamage:1.18},element:'acid'}
 ],ultra:[
  {id:'worldfire',icon:'☀',name:'Worldfire Tower',desc:'Fireballs create enormous burn/Combustion payoff.',mul:{damage:1.5,range:1.2},element:'fire'},
  {id:'pandemicMortar',icon:'☣',name:'Pandemic Mortar',desc:'Extreme-range toxic artillery poisons groups at once.',mul:{poisonDamage:1.6,range:1.45,fire:1.12},add:{poisonSpread:2},element:'poison'},
  {id:'dragonbreath',icon:'🐉',name:'Dragonbreath Crucible',desc:'Alternates poison and flame volleys to set up its own combos.',mul:{poisonDamage:1.35,fire:.78},element:'dual'}
 ]},
 slow:{major:[
  {id:'absoluteZero',icon:'❄',name:'Absolute Zero',desc:'Repeated pulses can fully freeze enemies.',mul:{slowDuration:1.35,slowRadius:1.1},element:'frost'},
  {id:'shatterCore',icon:'✷',name:'Shatter Core',desc:'Frozen targets become dramatically more vulnerable to projectile damage.',add:{slowDamage:8},element:'frost'},
  {id:'permafrostRelay',icon:'◎',name:'Permafrost Relay',desc:'Chill propagates and greatly improves Tesla conduction.',mul:{slowRadius:1.22},element:'frost'}
 ],ultra:[
  {id:'glacialPrison',icon:'◇',name:'Glacial Prison',desc:'Large pulses periodically hard-freeze entire groups.',mul:{slowRadius:1.45,slowDuration:1.45},element:'frost'},
  {id:'cometCryo',icon:'☄',name:'Comet Cryo',desc:'Adds ranged ice shards to the local freeze field.',mul:{range:1.5},add:{slowDamage:16},element:'frost'},
  {id:'zeroPoint',icon:'✧',name:'Zero-Point Engine',desc:'Rapid chill stacking supercharges nearby Tesla chains.',mul:{fire:.6,slowRadius:1.18},element:'frost'}
 ]},
 tesla:{major:[
  {id:'chainReactor',icon:'↯',name:'Chain Reactor',desc:'A larger chain network gains major conductive-status bonuses.',mul:{range:1.15},add:{chains:3},element:'shock'},
  {id:'thunderLance',icon:'☄',name:'Thunder Lance',desc:'Fewer targets, extreme single-target and boss lightning.',mul:{damage:1.9,fire:1.35,bossDamage:1.35},add:{chains:-2},element:'shock'},
  {id:'magneticDynamo',icon:'◉',name:'Magnetic Dynamo',desc:'Unlocks returning electric-disc attack patterns.',mul:{damage:1.15,fire:.9},element:'magnetic'}
 ],ultra:[
  {id:'stormGod',icon:'ϟ',name:'Storm God',desc:'Massive branching lightning network gains bonus chains from statuses.',mul:{damage:1.45,range:1.25},add:{chains:7},element:'shock'},
  {id:'orbitalArc',icon:'◉',name:'Orbital Arc',desc:'Slow electric orbs repeatedly zap nearby enemies.',mul:{damage:1.35,fire:.75},add:{chains:3},element:'orb'},
  {id:'thunderBoomerang',icon:'↶',name:'Thunder Boomerang',desc:'Throws a returning electric disc that hits outbound and inbound and chains on impact.',mul:{damage:1.4,range:1.2},element:'boomerang'}
 ]}
};

function automaticBuildingCapBonus(){return Math.max(0,Math.floor((game?.round||1)/3));}
function effectiveBuildingLevelCap(){return Math.max(1,(game?.mods?.buildingLevelCap||5)+automaticBuildingCapBonus());}
function elementalMod(name,fallback){const v=game?.mods?.[name];return Number.isFinite(v)?v:fallback;}

const baseBuildingEvoStats=buildingEvoStats;
buildingEvoStats=function(b){
 const out=baseBuildingEvoStats(b),apply=d=>{if(!d)return;for(const [k,v] of Object.entries(d.mul||{}))out[k]=(out[k]??1)*v;for(const [k,v] of Object.entries(d.add||{}))out[k]=(out[k]??0)+v};
 const tree=ELEMENTAL_MILESTONES[b.type];apply(tree?.major?.find(x=>x.id===b.majorEvolution));apply(tree?.ultra?.find(x=>x.id===b.ultraEvolution));return out;
};

function milestoneChoice(type,tier,id){return ELEMENTAL_MILESTONES[type]?.[tier]?.find(x=>x.id===id)||null;}
function openElementalEvolution(b,tier){
 const choices=ELEMENTAL_MILESTONES[b.type]?.[tier];if(!choices?.length)return;
 game.paused=true;const ultra=tier==='ultra';ui.upgradeKind.textContent=ultra?'ULTRA EVOLUTION · LEVEL 15':'ELEMENTAL EVOLUTION · LEVEL 10';
 ui.upgradeTitle.textContent=BUILDINGS[b.type].name+' — '+(ultra?'Choose an Ultra':'Choose an Element');
 ui.upgradeSub.textContent=ultra?'This structure permanently changes its late-game attack behavior.':'Choose a major attack specialization for this individual structure.';
 ui.upgradeChoices.innerHTML='';for(const choice of choices){const btn=document.createElement('button');btn.className='upgradeChoice';btn.innerHTML='<span class="upIcon">'+choice.icon+'</span><span class="upName">'+choice.name+'</span><span class="upDesc">'+choice.desc+'</span><span class="upRank">'+(ultra?'ULTRA':'LEVEL 10')+'</span>';btn.addEventListener('click',()=>chooseElementalEvolution(b,choice,tier));ui.upgradeChoices.appendChild(btn)}ui.upgrade.classList.add('open');navigator.vibrate?.(ultra?[35,35,70]:[25,25,45]);
}
function chooseElementalEvolution(b,choice,tier){
 const before=buildingEvoStats(b),oldMax=b.maxHp;if(tier==='major')b.majorEvolution=choice.id;else b.ultraEvolution=choice.id;const after=buildingEvoStats(b);if(b.type==='poison')b.charges=Math.max(b.charges||0,9999);b.maxHp=buildingMaxHp(b);b.hp=Math.min(b.maxHp,b.hp+Math.max(0,b.maxHp-oldMax));refreshBuildingMesh(b);ui.upgrade.classList.remove('open');game.paused=false;toast(choice.name+' evolved');pulse(b.gx,b.gy,tier==='ultra'?0xffd45a:0x78e4ff,tier==='ultra'?2:1.5);updateUI();
}

const baseBuildingMesh=buildingMesh;
buildingMesh=function(type,level=1,evolution=null,capstone=null){
 const g=baseBuildingMesh(type,level,evolution,capstone),b=buildings?.find?.(x=>x.type===type&&x.level===level&&x.evolution===evolution&&x.capstone===capstone&&x.mesh===g);return g;
};
function decorateMilestoneMesh(b){
 if(!b?.mesh||!ELEMENTAL_ATTACK_TYPES.has(b.type))return;const major=milestoneChoice(b.type,'major',b.majorEvolution),ultra=milestoneChoice(b.type,'ultra',b.ultraEvolution);if(!major&&!ultra)return;
 const element=(ultra||major).element,colors={fire:0xff6a2f,frost:0x93e9ff,shock:0x9eefff,poison:0x72df7b,acid:0xc7ff58,dual:0xffb13b,magnetic:0xc58cff,boomerang:0x8feaff,orb:0xb8f7ff},c=colors[element]||0xffffff;
 const ring=new THREE.Mesh(new THREE.TorusGeometry(ultra?.56:.5,ultra?.055:.035,8,28),new THREE.MeshBasicMaterial({color:c,transparent:true,opacity:.9}));ring.rotation.x=Math.PI/2;ring.position.y=ultra?.23:.17;ring.userData.elementalDecoration=true;b.mesh.add(ring);
 if(ultra){const crown=new THREE.Mesh(new THREE.OctahedronGeometry(.13),material(c,.25,.25,c));crown.material.emissiveIntensity=2.5;crown.position.set(0,1.35,0);crown.userData.elementalDecoration=true;b.mesh.add(crown)}
}
const baseRefreshBuildingMesh=refreshBuildingMesh;
refreshBuildingMesh=function(b){baseRefreshBuildingMesh(b);decorateMilestoneMesh(b);};

const baseUpgradeBuilding=upgradeBuilding;
upgradeBuilding=function(b){
 if(!b||b.dead)return false;const cap=effectiveBuildingLevelCap();if(b.level>=cap){buildFail('Level cap '+cap+' · rises every 3 rounds');return false}const cost=buildingUpgradeCost(b);if(game.cheese<cost){buildFail('Need '+cost+' cheese to upgrade');return false}game.cheese-=cost;b.level++;const oldMax=b.maxHp;b.maxHp=buildingMaxHp(b);b.hp=Math.min(b.maxHp,b.hp+(b.maxHp-oldMax)+Math.round(b.maxHp*.18));refreshBuildingMesh(b);pulse(b.gx,b.gy,0x78e4ff,1.2);toast(BUILDINGS[b.type].name+' upgraded to Lv '+b.level+' / '+cap);updateUI();
 if(b.level===3&&!b.evolution)openBuildingEvolution(b,'branch');else if(b.level===5&&b.evolution&&!b.capstone)openBuildingEvolution(b,'capstone');else if(b.level===10&&ELEMENTAL_ATTACK_TYPES.has(b.type)&&!b.majorEvolution)openElementalEvolution(b,'major');else if(b.level===15&&ELEMENTAL_ATTACK_TYPES.has(b.type)&&!b.ultraEvolution)openElementalEvolution(b,'ultra');return true;
};

function comboReady(e,key){e.elementComboCooldowns??={};return (e.elementComboCooldowns[key]||0)<=game.time;}
function comboLock(e,key,seconds=.8){e.elementComboCooldowns??={};e.elementComboCooldowns[key]=game.time+seconds;}
function elementalTargets(){return enemies.filter(e=>!e.dead).concat(cat&&!cat.dead?[cat]:[]);}
function splashDamage(source,radius,damage,color,exclude=null){for(const e of elementalTargets())if(e!==exclude&&dist(source,e)<=radius)damageEnemy(e,damage,e===cat);pulse(source.gx,source.gy,color,1.15);}
function applyPoisonStatus(e,power=1,duration=null,source=null){
 if(!e||e.dead)return;e.poisonUntil=Math.max(e.poisonUntil||0,game.time+(duration||game.mods.poisonDuration||8));e.poisonTick=Math.min(e.poisonTick||Infinity,game.time+.2);e.poisonPower=Math.max(e.poisonPower||1,power);e.conductiveUntil=Math.max(e.conductiveUntil||0,game.time+4);
 if(e.slow>game.time&&comboReady(e,'brittleVenom')){comboLock(e,'brittleVenom',1.1);damageEnemy(e,5*elementalMod('elementalComboDamage',1),e===cat);e.poisonTick=game.time+.05;pulse(e.gx,e.gy,0x8fe59b,.7)}
}
function applyBurnStatus(e,power=1,duration=4,source=null){
 if(!e||e.dead)return;e.burnUntil=Math.max(e.burnUntil||0,game.time+duration*elementalMod('fireDuration',1));e.burnPower=Math.max(e.burnPower||1,power*elementalMod('fireDamage',1));e.burnTick=Math.min(e.burnTick||Infinity,game.time+.15);
 if(e.poisonUntil>game.time&&comboReady(e,'combustion')){comboLock(e,'combustion',1.25);const boom=(14+game.round*.6)*elementalMod('elementalComboDamage',1);damageEnemy(e,boom,e===cat);splashDamage(e,1.8,boom*.5,0xff8b32,e);e.poisonUntil=Math.max(game.time,e.poisonUntil-1.5);}
 if((e.frozenUntil||0)>game.time&&comboReady(e,'thermalShock')){comboLock(e,'thermalShock',1.1);damageEnemy(e,18*elementalMod('elementalComboDamage',1),e===cat);e.frozenUntil=game.time;e.armorBreakUntil=game.time+3;pulse(e.gx,e.gy,0xffffff,1.0)}
}
function applyChillStatus(e,stacks=1,duration=3){
 if(!e||e.dead)return;e.chillStacks=(e.chillStacks||0)+stacks*elementalMod('freezeBuild',1);e.slow=Math.max(e.slow||0,game.time+duration);if(e.poisonUntil>game.time&&comboReady(e,'brittleVenom')){comboLock(e,'brittleVenom',1);e.poisonTick=game.time+.05;damageEnemy(e,4*elementalMod('elementalComboDamage',1),e===cat)}if(e.chillStacks>=3){e.frozenUntil=Math.max(e.frozenUntil||0,game.time+1.65);e.slow=Math.max(e.slow,game.time+2.4);e.chillStacks=0;pulse(e.gx,e.gy,0xb9f4ff,1.0)}
}
function elementalDamageMultiplier(e,kind){let m=1;if((e.armorBreakUntil||0)>game.time)m*=1.2;if(kind==='physical'&&(e.frozenUntil||0)>game.time)m*=1.3;if(kind==='shock'){if((e.frozenUntil||0)>game.time)m*=1.35;if(e.poisonUntil>game.time)m*=1.25;if((e.conductiveUntil||0)>game.time)m*=1.15}return m;}

const baseDamageEnemy=damageEnemy;
damageEnemy=function(e,dmg,isCat=false){if(e&&!e.dead&&(e.armorBreakUntil||0)>game.time)dmg*=1.2;return baseDamageEnemy(e,dmg,isCat);};

const baseEatPoison=eatPoison;
eatPoison=function(e,b){if(!e||!b||b.dead)return;const es=buildingEvoStats(b);e.eaten.add(b.id);applyPoisonStatus(e,es.poisonDamage,game.mods.poisonDuration*es.poisonDuration,b);pulse(b.gx,b.gy,0x72df7b,.8);};

function firePoisonTower(b,targets){
 const es=buildingEvoStats(b),range=(4.4+(b.level-1)*.12)*(es.range||1)*elementalMod('poisonRange',1),t=nearestTarget(b,targets,range);if(!t)return;const major=milestoneChoice('poison','major',b.majorEvolution),ultra=milestoneChoice('poison','ultra',b.ultraEvolution),mode=(ultra||major)?.element||'poison';aimTurret(b,t,.2,1);
 const power=es.poisonDamage*(1+(b.level-1)*.1),fireMode=mode==='fire'||mode==='dual';bullet(b,t,3*power,fireMode?0xff7638:0x72df7b);
 if(fireMode){applyBurnStatus(t,.8+(b.level*.04),4.5,b);if(mode==='dual'||b.majorEvolution==='inferno')applyPoisonStatus(t,power*.55,game.mods.poisonDuration*.65,b)}else{applyPoisonStatus(t,power,game.mods.poisonDuration*es.poisonDuration,b);if(b.majorEvolution==='acidCrucible')t.armorBreakUntil=Math.max(t.armorBreakUntil||0,game.time+3.5);const spread=Math.max(0,Math.floor(game.mods.poisonSpread+es.poisonSpread));for(const x of targets.filter(x=>x!==t&&dist(x,t)<=2.2).slice(0,spread))applyPoisonStatus(x,power*.75,game.mods.poisonDuration*.7,b)}
 if(b.ultraEvolution==='worldfire')splashDamage(t,2,8*power,0xff6a2f,t);if(b.ultraEvolution==='pandemicMortar')for(const x of targets.filter(x=>x!==t&&dist(x,t)<=2.5).slice(0,4))applyPoisonStatus(x,power*.8,null,b);
 b.elementNext=game.time+Math.max(.28,1.05*(es.fire||1)/Math.pow(1.05,b.level-1));
}

function augmentTurretElement(b,targets){
 const major=milestoneChoice('turret','major',b.majorEvolution),ultra=milestoneChoice('turret','ultra',b.ultraEvolution);if(!major&&!ultra)return;const es=buildingEvoStats(b),range=5.4*game.mods.turretRange*es.range*(1+(b.level-1)*.08),t=nearestTarget(b,targets,range);if(!t)return;const mode=(ultra||major).element,dmg=7*game.mods.turretDamage*es.damage*(1+(b.level-1)*.12);
 if(mode==='fire'){bullet(b,t,dmg,0xff6a2f);applyBurnStatus(t,1+(b.level*.03),4,b)}else if(mode==='frost'){bullet(b,t,dmg,0xaeeeff);applyChillStatus(t,1,3);if((t.frozenUntil||0)>game.time)damageEnemy(t,dmg*.8,t===cat)}else if(mode==='shock'){bullet(b,t,dmg,0x9eefff);damageEnemy(t,dmg*elementalDamageMultiplier(t,'shock'),t===cat);const extra=targets.filter(x=>x!==t&&dist(x,t)<=2.8).slice(0,ultra?3:1);if(extra.length)lightning(t,extra)}
 b.elementNext=game.time+(ultra?.id==='tempestGatling'?.34:.7);
}

function augmentFreeze(b,targets){
 if(!b.majorEvolution&&!b.ultraEvolution)return;const es=buildingEvoStats(b),radius=(3.0+(b.level-1)*.22)*game.mods.slowRadius*es.slowRadius;for(const e of targets)if(dist(b,e)<=radius){applyChillStatus(e,b.ultraEvolution==='glacialPrison'?2:1,3.2);if(b.majorEvolution==='shatterCore'&&(e.frozenUntil||0)>game.time)e.armorBreakUntil=Math.max(e.armorBreakUntil||0,game.time+2.5)}
 if(b.ultraEvolution==='cometCryo'){const t=nearestTarget(b,targets,5.8*es.range);if(t){bullet(b,t,12+es.slowDamage,0xb9f4ff);applyChillStatus(t,2,4)}}
 b.elementNext=game.time+Math.max(.45,1.45*(es.fire||1));
}

const elementalBoomerangs=[];
function launchThunderBoomerang(b,t,damage){
 const mesh=new THREE.Mesh(new THREE.TorusGeometry(.18,.055,6,14),new THREE.MeshBasicMaterial({color:0x8feaff,transparent:true,opacity:.95}));mesh.rotation.x=Math.PI/2;mesh.position.copy(b.mesh.position).add(new THREE.Vector3(0,.75,0));fxGroup.add(mesh);elementalBoomerangs.push({mesh,b,target:t,damage,phase:0,t:0,start:mesh.position.clone(),end:t.mesh.position.clone().add(new THREE.Vector3(0,.45,0)),hitOut:false,hitBack:false});
}
function updateBoomerangs(dt){for(let i=elementalBoomerangs.length-1;i>=0;i--){const p=elementalBoomerangs[i];if(!p.mesh||!p.b||p.b.dead){p.mesh&&disposeObject(p.mesh);elementalBoomerangs.splice(i,1);continue}p.t+=dt*2.15;p.mesh.rotation.z+=dt*10;const q=Math.min(1,p.t),from=p.phase===0?p.start:p.end,to=p.phase===0?p.end:p.start;p.mesh.position.lerpVectors(from,to,q);if(q>=1){if(p.phase===0){if(!p.target.dead){damageEnemy(p.target,p.damage*elementalDamageMultiplier(p.target,'shock'),p.target===cat);p.target.conductiveUntil=game.time+4;const near=elementalTargets().filter(x=>x!==p.target&&dist(x,p.target)<=2.7).slice(0,2);if(near.length)lightning(p.target,near);for(const x of near)damageEnemy(x,p.damage*.45*elementalDamageMultiplier(x,'shock'),x===cat)}p.phase=1;p.t=0}else{const near=elementalTargets().filter(x=>dist(x,p.b)<=2.1).slice(0,2);for(const x of near)damageEnemy(x,p.damage*.55*elementalDamageMultiplier(x,'shock'),x===cat);disposeObject(p.mesh);elementalBoomerangs.splice(i,1)}}}}

function augmentTesla(b,targets){
 if(!b.majorEvolution&&!b.ultraEvolution)return;const es=buildingEvoStats(b),range=4.8*game.mods.teslaRange*es.range*(1+(b.level-1)*.07),t=nearestTarget(b,targets,range);if(!t)return;const dmg=(8+game.round*.2)*game.mods.teslaDamage*es.damage*(1+(b.level-1)*.12);
 if(b.ultraEvolution==='thunderBoomerang'){launchThunderBoomerang(b,t,dmg);b.elementNext=game.time+1.15*(es.fire||1);return}
 const bonusChains=(t.poisonUntil>game.time?1:0)+((t.frozenUntil||0)>game.time?2:0)+(t.burnUntil>game.time?1:0),chain=targets.filter(e=>dist(b,e)<=range).sort((a,c)=>dist(b,a)-dist(b,c)).slice(0,Math.max(1,2+bonusChains+(es.chains||0)));if(chain.length){lightning(b,chain);for(const e of chain){damageEnemy(e,dmg*elementalDamageMultiplier(e,'shock'),e===cat);e.conductiveUntil=Math.max(e.conductiveUntil||0,game.time+3.5);if(e.burnUntil>game.time&&comboReady(e,'plasmaArc')){comboLock(e,'plasmaArc',1);splashDamage(e,1.4,dmg*.35,0xb4f7ff,e)}}}b.elementNext=game.time+.95*(es.fire||1);
}

const baseEnemyUpdate=enemyUpdate;
enemyUpdate=function(e,dt,isCat=false){
 if(e&&!e.dead){if((e.frozenUntil||0)>game.time)e.slow=Math.max(e.slow||0,game.time+.3);if((e.burnUntil||0)>game.time&&game.time>=(e.burnTick||0)){e.burnTick=game.time+.55;damageEnemy(e,(6+game.round*.35)*(e.burnPower||1),isCat);pulse(e.gx,e.gy,0xff6a2f,.38)}}if(!e?.dead)return baseEnemyUpdate(e,dt,isCat);
};

const baseUpdateBuildings=updateBuildings;
updateBuildings=function(dt){baseUpdateBuildings(dt);updateBoomerangs(dt);const targets=elementalTargets();for(const b of buildings){if(b.dead||!ELEMENTAL_ATTACK_TYPES.has(b.type))continue;if(game.time<(b.elementNext||0))continue;if(b.type==='poison')firePoisonTower(b,targets);else if(b.type==='turret'&&(b.majorEvolution||b.ultraEvolution))augmentTurretElement(b,targets);else if(b.type==='slow'&&(b.majorEvolution||b.ultraEvolution))augmentFreeze(b,targets);else if(b.type==='tesla'&&(b.majorEvolution||b.ultraEvolution))augmentTesla(b,targets)}};

// Add a wider upgrade pool without replacing existing upgrades.
XP_UPGRADES.push(
 {id:'elementalCatalyst',icon:'✦',name:'Elemental Catalyst',max:4,desc:'Elemental combo damage +18%.',apply:m=>m.elementalComboDamage=(m.elementalComboDamage||1)*1.18},
 {id:'pyroCulture',icon:'🔥',name:'Pyro Culture',max:4,desc:'Burn damage +18% and duration +10%.',apply:m=>{m.fireDamage=(m.fireDamage||1)*1.18;m.fireDuration=(m.fireDuration||1)*1.1}},
 {id:'sporeBarrel',icon:'☣',name:'Spore Barrel',max:4,desc:'Ranged poison towers gain 12% range.',apply:m=>m.poisonRange=(m.poisonRange||1)*1.12},
 {id:'crystalNucleator',icon:'❄',name:'Crystal Nucleator',max:3,desc:'Freeze buildup +22%.',apply:m=>m.freezeBuild=(m.freezeBuild||1)*1.22},
 {id:'conductiveMesh',icon:'⚡',name:'Conductive Mesh',max:3,desc:'Tesla damage +12% and range +8%.',apply:m=>{m.teslaDamage*=1.12;m.teslaRange*=1.08}},
 {id:'mixedBattery',icon:'◈',name:'Mixed Battery',max:3,desc:'Turret, Tesla, poison and freeze damage systems all gain a small boost.',apply:m=>{m.turretDamage*=1.08;m.teslaDamage*=1.08;m.poisonDamage*=1.1;m.slowDamage+=2}},
 {id:'statusHunter',icon:'⌖',name:'Status Hunter',max:3,desc:'Improves physical damage into frozen or armor-broken targets.',apply:m=>{m.turretDamage*=1.1;m.turretCrit=Math.min(.55,m.turretCrit+.025)}},
 {id:'elementalReach',icon:'◎',name:'Elemental Reach',max:3,desc:'Poison, Tesla and freeze influence reach farther.',apply:m=>{m.poisonRange=(m.poisonRange||1)*1.08;m.teslaRange*=1.07;m.slowRadius*=1.07}}
);

const elementalBeginRound=beginRound;
beginRound=function(first=false){const before=effectiveBuildingLevelCap();const result=elementalBeginRound(first);const cap=effectiveBuildingLevelCap();if(!first&&game.round%3===0)setTimeout(()=>toast('Building level cap increased to '+cap+' · upgrade bonuses still stack'),120);return result;};

console.info('[Cheesehold] elemental progression loaded: cap '+effectiveBuildingLevelCap()+', ranged poison, Lv10/Lv15 evolutions');
