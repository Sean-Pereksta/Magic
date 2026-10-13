/* TurnCraft: The Fractured Signal. Pure, deterministic campaign configuration. */
(function(root){
  'use strict';
  const REGIONS=Object.freeze([
    {id:'frontier',mapId:'classic',chapter:1,name:"Frontier's Last Stand",subtitle:'The Signal Awakens',theme:'frontier',waves:15,boss:'battlecruiser',bossName:'The Iron Harbinger',bossTitle:'Corrupted siege flagship',leader:'Captain Mara Voss',story:"The outer settlements have gone silent. Mara Voss gathers the last defenders of the Frontier to hold the line.",brief:"Establish a base and endure the Signal's scouting columns. Expect fast infantry, armored raids, and a siege flagship.",pool:['rifleman','reaper','marauder','hellion','goliath'],bossPool:['siegeTank','battlecruiser'],reward:'Frontier Sentinel',accent:'#69d9ff'},
    {id:'ember',mapId:'ember-rift',chapter:2,name:'The Burning Divide',subtitle:'Bridges of Ash',theme:'volcanic',waves:15,boss:'siegeTank',bossName:'The Furnace Titan',bossTitle:'Molten siege engine',leader:'Captain Mara Voss',story:'From the ruins of Ember Rift, the Signal has forged a mobile army of burning steel.',brief:'Protect multiple bridge approaches. Watch for artillery and flame vehicles before the Furnace Titan arrives.',pool:['hellion','marauder','siegeTank','reaper','goliath'],bossPool:['siegeTank','hellion'],reward:'Ember Vanguard',accent:'#ff956a'},
    {id:'frost',mapId:'frostline-basin',chapter:3,name:'Frozen Echoes',subtitle:'Whispers Under Ice',theme:'ice',waves:15,boss:'carrier',bossName:'The Ghost Admiral',bossTitle:'Phantom command carrier',leader:'Oracle Kaelis',story:'Under the ice, Kaelis hears the names of commanders the Signal consumed long ago.',brief:'Defend the ring approaches against shielded attackers, flyers, and sudden flank raids.',pool:['zealot','dragoon','sentinel','phoenix','immortal'],bossPool:['templar','carrier'],reward:'Frostward Beacon',accent:'#91e1ff'},
    {id:'verdant',mapId:'verdant-divide',chapter:4,name:'The Living Wilds',subtitle:'The Forest Remembers',theme:'jungle',waves:15,boss:'ultralisk',bossName:'The Devouring Root',bossTitle:'Ancient corrupted leviathan',leader:'Matriarch Nyra',story:'The living jungle is turning against its own brood. Nyra refuses to abandon her lost kin.',brief:'Prepare for burrowers, flanking packs, and rapid biological waves arriving from unexpected paths.',pool:['zergling','roach','hydralisk','ravager','lurker','mutalisk'],bossPool:['defiler','ultralisk'],reward:'Verdant Symbiote',accent:'#8bdd91'},
    {id:'dunes',mapId:'shattered-dunes',chapter:5,name:'Sands of Ruin',subtitle:'The Broken Convoy',theme:'desert',waves:15,boss:'battlecruiser',bossName:'The Dread Convoy',bossTitle:'Armored desert fleet',leader:'Captain Mara Voss',story:'An entire transport convoy is moving without living crew. Its route leads to the heart of the Signal.',brief:'Defend dispersed routes from raiders and heavy armor. Use the long sightlines to detect approaching assaults.',pool:['reaper','hellion','marauder','goliath','siegeTank','wraith'],bossPool:['goliath','battlecruiser'],reward:'Sunscour Banner',accent:'#ffd38c'},
    {id:'foundry',mapId:'orbital-foundry',chapter:6,name:'Heart of the Signal',subtitle:'The Null Regent',theme:'industrial',waves:20,boss:'carrier',bossName:'The Null Regent',bossTitle:'Master of the Fractured Signal',leader:'Oracle Kaelis',story:'The three commanders reach the ancient foundry where the Null Regent is building an army without end.',brief:'Expect mixed factions, coordinated lane pressure, air assaults, and a final adaptive defense-breaker.',pool:['marauder','siegeTank','dragoon','immortal','hydralisk','mutalisk','goliath','templar'],bossPool:['battlecruiser','carrier','ultralisk'],reward:'Signalbreaker Crest',accent:'#c7a7ff'}
  ]);
  const CHARACTERS=Object.freeze([
    {id:'mara',name:'Captain Mara Voss',faction:'frontier',role:'The strategist',quote:'No one gets left behind.',bio:'A Frontier commander who rallied scattered settlements after the first Signal incursion.'},
    {id:'kaelis',name:'Oracle Kaelis',faction:'ascendant',role:'The interpreter',quote:'It is learning from us.',bio:'An Ascendant seer who recognizes patterns hidden in the enemy transmission.'},
    {id:'nyra',name:'Matriarch Nyra',faction:'brood',role:'The survivor',quote:'The brood will endure.',bio:'A Brood matriarch determined to free her kin from the corruption.'},
    {id:'regent',name:'The Null Regent',faction:'signal',role:'The antagonist',quote:'The final command is silence.',bio:'A fallen planetary intelligence commanding armies of every faction.'}
  ]);
  const TIERS=Object.freeze([
    {name:'Bronze',minimum:0},{name:'Silver',minimum:850},{name:'Gold',minimum:1050},
    {name:'Platinum',minimum:1250},{name:'Diamond',minimum:1500},{name:'Master',minimum:1800},
    {name:'Grandmaster',minimum:2200}
  ]);
  function tierFor(mmr){return [...TIERS].reverse().find(t=>Math.max(0,Number(mmr)||0)>=t.minimum)||TIERS[0]}
  function clamp(n,a,b){return Math.min(b,Math.max(a,n))}
  function hash(seed){let s=(seed>>>0)||1;s^=s<<13;s^=s>>>17;s^=s<<5;return s>>>0}
  function getRegion(index){return REGIONS[clamp(Math.floor(Number(index)||0),0,REGIONS.length-1)]}
  function wavePlan(regionIndex,wave,endless=false){
    wave=clamp(Math.floor(Number(wave)||1),1,999999);
    const idx=endless?Math.floor((wave-1)/12)%REGIONS.length:clamp(Number(regionIndex)||0,0,REGIONS.length-1);
    const region=getRegion(idx);
    const boss=!endless&&wave===region.waves||endless&&wave%10===0;
    const elite=wave%5===0;
    const pressure=Math.min(65,Math.round(5+wave*1.45+idx*2.6+(elite?3:0)));
    const count=boss?Math.min(70,pressure+6):pressure;
    const seed=hash((idx+1)*2654435761+Math.imul(wave,2246822519)+(endless?17:0));
    const randomAt=n=>hash(seed+Math.imul(n+1,3266489917))/4294967296;
    const units=Array.from({length:count},(_,n)=>{
      const unlock=Math.min(region.pool.length,2+Math.floor(wave/3));
      const rank=Math.floor(randomAt(n)*unlock);
      return region.pool[rank];
    });
    if(boss)units.push(region.boss);
    return {
      regionId:region.id,regionIndex:idx,wave,boss,elite,bossName:boss?region.bossName:'',
      units,spawnIntervalMs:clamp(1100-wave*18,310,1100),
      prepSeconds:wave===1?35:clamp(22-Math.floor(wave/4),8,22),
      hpMultiplier:1+Math.max(0,wave-1)*.055+idx*.09,
      payout:{minerals:Math.round(65+wave*14),gas:Math.round(20+wave*6)},
      lanes:clamp(1+Math.floor((wave-1)/4),1,4),
      commanderLine:boss?'Boss wave incoming — prepare your defenses!':elite?'Elite enemy formation incoming.':'Hostiles approaching the perimeter.'
    };
  }
  const api=Object.freeze({REGIONS,CHARACTERS,TIERS,tierFor,getRegion,wavePlan});
  if(typeof module==='object'&&module.exports)module.exports=api;
  root.TurncraftExpeditionData=api;
})(typeof window!=='undefined'?window:globalThis);
