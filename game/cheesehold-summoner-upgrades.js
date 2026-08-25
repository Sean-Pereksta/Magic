function sectorEnsureSummonMods(m){if(!m)return m;if(!Number.isFinite(m.summonDamage))m.summonDamage=1;if(!Number.isFinite(m.summonHp))m.summonHp=1;if(!Number.isFinite(m.summonRate))m.summonRate=1;if(!Number.isFinite(m.summonRepair))m.summonRepair=1;if(!Number.isFinite(m.summonCapBonus))m.summonCapBonus=0;if(!Number.isFinite(m.summonGlobalCap))m.summonGlobalCap=0;if(!Number.isFinite(m.wallDroneDiscount))m.wallDroneDiscount=0;return m;}
const sectorBaseFreshMods=freshMods;
freshMods=function(){return sectorEnsureSummonMods(sectorBaseFreshMods());};
sectorEnsureSummonMods(game?.mods);
XP_UPGRADES.push(
 {id:'rapidAssembly',icon:'⚙',name:'Rapid Assembly',max:4,desc:'Summoner structures replace allied units 9% faster.',apply:m=>m.summonRate*=.91},
 {id:'reinforcedMinions',icon:'🛡',name:'Reinforced Minions',max:4,desc:'All summoned allies gain 18% health.',apply:m=>m.summonHp*=1.18},
 {id:'fieldWeapons',icon:'⚔',name:'Field Weapons',max:4,desc:'All summoned combat allies deal 14% more damage.',apply:m=>m.summonDamage*=1.14},
 {id:'masonProtocols',icon:'⚒',name:'Mason Protocols',max:4,desc:'Repair Bots repair 18% more per action.',apply:m=>m.summonRepair*=1.18},
 {id:'expandedQuarters',icon:'▦',name:'Expanded Quarters',max:2,desc:'Summoner buildings may maintain +1 allied unit; global summon cap also rises.',apply:m=>{m.summonCapBonus++;m.summonGlobalCap+=3}}
);
ARTIFACTS.push(
 {id:'foundryStandard',icon:'⚑',name:'Foundry Standard',max:1,desc:'All summons deal +20% damage and deploy 12% faster.',apply:m=>{m.summonDamage*=1.2;m.summonRate*=.88}},
 {id:'masonDroneCore',icon:'⚒',name:'Mason Drone Core',max:1,desc:'Repair Bots repair +35% and wall reconstruction costs 25% less.',apply:m=>{m.summonRepair*=1.35;m.wallDroneDiscount=.25}},
 {id:'broodCharter',icon:'✦',name:'Brood Charter',max:1,desc:'Global summon cap +5 without removing per-building caps.',apply:m=>m.summonGlobalCap+=5}
);

