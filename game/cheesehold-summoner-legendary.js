// Restrict Level 15 choices to the chosen Level 10 branch when branch-specific options exist.
const sectorOriginalOpenElementalEvolution=openElementalEvolution;
openElementalEvolution=function(b,tier){
 let choices=ELEMENTAL_MILESTONES[b.type]?.[tier];if(!choices?.length)return;
 if(tier==='ultra'){
   const matching=choices.filter(x=>{const req=x.requiresMajor;return Array.isArray(req)?req.includes(b.majorEvolution):req===b.majorEvolution;});
   choices=matching.length?matching:choices.filter(x=>!x.requiresMajor);
 }
 game.paused=true;const ultra=tier==='ultra';ui.upgradeKind.textContent=ultra?'ULTRA EVOLUTION · LEVEL 15':'MAJOR EVOLUTION · LEVEL 10';
 ui.upgradeTitle.textContent=BUILDINGS[b.type].name+' — '+(ultra?'Choose an Ultra':'Choose a Major Path');
 ui.upgradeSub.textContent=ultra?'Deepen the Level 10 specialization for this individual structure.':'Choose direct power, support, economy, or a summoner conversion.';
 ui.upgradeChoices.innerHTML='';for(const choice of choices){const btn=document.createElement('button');btn.className='upgradeChoice';btn.innerHTML='<span class="upIcon">'+choice.icon+'</span><span class="upName">'+choice.name+'</span><span class="upDesc">'+choice.desc+'</span><span class="upRank">'+(choice.summoner?'SUMMONER · ':'')+(ultra?'LEVEL 15':'LEVEL 10')+'</span>';btn.addEventListener('click',()=>chooseElementalEvolution(b,choice,tier));ui.upgradeChoices.appendChild(btn)}ui.upgrade.classList.add('open');navigator.vibrate?.(ultra?[35,35,70]:[25,25,45]);
};

// Selected Level 20 legendary paths. Summoner branches stay summoner; direct branches get a direct legendary choice.
const CHEESEHOLD_LEGENDARIES={
 tesla:[
  {id:'machineDominion',icon:'🦾',name:'Machine Dominion',desc:'Robot Foundries maintain a larger mixed army and replace units faster.',mul:{hp:1.2},requiresMajor:'robotFoundry',summoner:true},
  {id:'colossusFoundry',icon:'⚙',name:'Colossus Foundry',desc:'Focuses the robot path on fewer, heavier mechs.',mul:{hp:1.35},requiresMajor:'robotFoundry',summoner:true},
  {id:'heavenwire',icon:'ϟ',name:'Heavenwire Array',desc:'Non-summoner Tesla towers gain huge damage, range, and chains.',mul:{damage:1.65,range:1.3},add:{chains:6}}
 ],
 turret:[
  {id:'grandMuster',icon:'⚑',name:'Grand Muster',desc:'Mouse Armories field larger squads and reinforce neighboring sectors.',requiresMajor:'mouseArmory',summoner:true},
  {id:'warCouncil',icon:'♛',name:'War Council',desc:'Fewer elite marksmen with much stronger attacks.',requiresMajor:'mouseArmory',summoner:true},
  {id:'atlasCannon',icon:'☄',name:'Atlas Cannon',desc:'Non-summoner turrets become extreme-range fortress artillery.',mul:{damage:1.8,range:1.55,fire:1.18}}
 ],
 generator:[
  {id:'architectColony',icon:'🏗',name:'Architect Colony',desc:'Engineer Workshops may cautiously construct new emergency wall segments as well as rebuild breaches.',requiresMajor:'engineerWorkshop',summoner:true},
  {id:'droneUnion',icon:'⚒',name:'Drone Union',desc:'Engineer Workshops maintain more, faster Repair Bots but cannot create new walls.',requiresMajor:'engineerWorkshop',summoner:true},
  {id:'sovereignMint',icon:'♦',name:'Sovereign Mint',desc:'Non-summoner generators gain a major yield boost, still subject to sector saturation.',mul:{genRate:.72},add:{yield:5}}
 ],
 superwall:[
  {id:'grandCitadel',icon:'🏰',name:'Grand Citadel',desc:'Gatehouses maintain a full defensive garrison and a Guardian.',mul:{hp:1.35},requiresMajor:'gatehouse',summoner:true},
  {id:'masonKingdom',icon:'⚒',name:'Mason Kingdom',desc:'Gatehouses add a Repair Bot to their defensive garrison.',mul:{hp:1.2},requiresMajor:'gatehouse',summoner:true},
  {id:'worldwall',icon:'█',name:'Worldwall',desc:'Non-summoner Super Walls gain monumental health.',mul:{hp:2.4}}
 ],
 phoenix:[
  {id:'sunfireEyrie',icon:'☀',name:'Sunfire Eyrie',desc:'Ember Nests maintain a larger flock and more Greater Phoenixes.',requiresMajor:'emberNest',summoner:true},
  {id:'phoenixCovenant',icon:'♻',name:'Phoenix Covenant',desc:'Phoenixling replacement is dramatically faster.',requiresMajor:'emberNest',summoner:true},
  {id:'thirdDawn',icon:'✹',name:'Third Dawn',desc:'Non-summoner Phoenix structures become massive fortified resurrection shrines.',mul:{hp:2.1}}
 ],
 slow:[
  {id:'winterLegion',icon:'❄',name:'Winter Legion',desc:'Frost Shrines maintain a larger mobile freeze screen.',requiresMajor:'frostShrine',summoner:true},
  {id:'absoluteWinter',icon:'◇',name:'Absolute Winter',desc:'Non-summoner slow structures gain enormous radius and duration.',mul:{slowRadius:1.55,slowDuration:1.5}}
 ],
 poison:[
  {id:'broodQueen',icon:'♛',name:'Brood Queen',desc:'Brood Vats maintain a larger capped pack including Brood Mothers.',requiresMajor:'broodVat',summoner:true},
  {id:'worldPlague',icon:'☣',name:'World Plague',desc:'Non-summoner poison structures gain huge damage and spread.',mul:{poisonDamage:1.7},add:{poisonSpread:2}}
 ]
};
function sectorLegendaryChoices(b){const all=CHEESEHOLD_LEGENDARIES[b.type]||[],matching=all.filter(x=>x.requiresMajor===b.majorEvolution);return matching.length?matching:all.filter(x=>!x.requiresMajor);}
function openSectorLegendaryEvolution(b){const choices=sectorLegendaryChoices(b);if(!choices.length)return;game.paused=true;ui.upgradeKind.textContent='LEGENDARY EVOLUTION · LEVEL 20';ui.upgradeTitle.textContent=BUILDINGS[b.type].name+' — Legendary Path';ui.upgradeSub.textContent='A final late-game transformation for this individual structure.';ui.upgradeChoices.innerHTML='';for(const choice of choices){const btn=document.createElement('button');btn.className='upgradeChoice';btn.innerHTML='<span class="upIcon">'+choice.icon+'</span><span class="upName">'+choice.name+'</span><span class="upDesc">'+choice.desc+'</span><span class="upRank">'+(choice.summoner?'SUMMONER · ':'')+'LEVEL 20</span>';btn.addEventListener('click',()=>{const oldMax=b.maxHp;b.legendaryEvolution=choice.id;b.maxHp=buildingMaxHp(b);b.hp=Math.min(b.maxHp,b.hp+Math.max(0,b.maxHp-oldMax));refreshBuildingMesh(b);ui.upgrade.classList.remove('open');game.paused=false;toast(choice.name+' — LEGENDARY');pulse(b.gx,b.gy,0xffd45a,2.2);updateUI()});ui.upgradeChoices.appendChild(btn)}ui.upgrade.classList.add('open');navigator.vibrate?.([55,35,90]);}
const sectorBaseBuildingEvoStats=buildingEvoStats;
buildingEvoStats=function(b){const out=sectorBaseBuildingEvoStats(b),choice=(CHEESEHOLD_LEGENDARIES[b?.type]||[]).find(x=>x.id===b?.legendaryEvolution);if(choice){for(const [k,v] of Object.entries(choice.mul||{}))out[k]=(out[k]??1)*v;for(const [k,v] of Object.entries(choice.add||{}))out[k]=(out[k]??0)+v}return out;};
const sectorBaseUpgradeBuilding=upgradeBuilding;
upgradeBuilding=function(b){if(b?.temporarySummonedWall){buildFail('Emergency walls cannot be upgraded');return false}const ok=sectorBaseUpgradeBuilding(b);if(ok&&b.level===20&&!b.legendaryEvolution&&!ui.upgrade.classList.contains('open')&&CHEESEHOLD_LEGENDARIES[b.type])openSectorLegendaryEvolution(b);return ok;};

