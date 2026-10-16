import assert from 'node:assert/strict';
import {createPlayer,updatePlayer} from '../js/physics.js';
import {grantBlessing,updateBlessings,collectPickups,useAbility} from '../js/powers/blessings.js';
import {cleanBoosts} from '../js/powers/boosts.js';
import {createLevel,updateWorldStreaming,persistWorldState,restoreWorldState} from '../js/world/generator.js';
import {AssetManager} from '../js/assets/asset-manager.js';
import {GameStorage} from '../js/save/storage.js';
const dt=1/120,world=()=>({width:8000,platforms:[{id:'floor',x:0,y:468,w:8000,h:500}],movers:[],enemies:[],effects:[],pickups:[]});
const player=()=>Object.assign(createPlayer(100,406),{onGround:true,groundId:'floor'});
const input=(jump=false,axis=0,dash=false)=>({horizontal:()=>axis,jumpPressed:()=>jump,jumpHeld:()=>true,dashPressed:()=>dash});
function airStep(p,w,jump=false){updatePlayer(p,w,input(jump),dt);}
for(const wings of [false,true]){
 const p=player(),w=world();p.powers.wings=wings;grantBlessing(p,'water');airStep(p,w,true);
 assert.equal(p.extraJumps,wings?2:1);
 for(let j=0;j<(wings?2:1);j++){for(let i=0;i<12;i++)airStep(p,w);airStep(p,w,true);assert.equal(p.airAction,'doubleJump');assert.ok(p.vy< -600);}
 assert.equal(p.extraJumps,0);for(let i=0;i<12;i++)airStep(p,w);const before=p.vy;airStep(p,w,true);assert.ok(p.vy>before,'no unlimited jumps');
 grantBlessing(p,'water');assert.equal(p.extraJumps,0,'refreshing water cannot refill jumps while airborne');
 assert.equal(p.boosts.water,45);
}
{
 const p=player(),w=world();grantBlessing(p,'water');airStep(p,w,true);updateBlessings(p,w,46);
 assert.equal(p.boosts.water,0);for(let i=0;i<12;i++)airStep(p,w);airStep(p,w,true);assert.equal(p.airAction,'doubleJump','expiry keeps the committed air jump until landing');
 for(let i=0;i<400&&!p.onGround;i++)airStep(p,w);assert.equal(p.onGround,true);assert.equal(p.extraJumps,0,'expired water cannot refill at landing');
 const air=createPlayer(100,200);grantBlessing(air,'water');assert.equal(air.extraJumps,1,'first airborne pickup grants a rescue jump');
}
{
 const w=world(),base=player(),wind=player();base.powers.sandals=true;wind.powers.sandals=true;grantBlessing(wind,'wind');
 for(let i=0;i<180;i++){updatePlayer(base,w,input(false,1),dt);updatePlayer(wind,w,input(false,1),dt);}
 assert.ok(wind.vx>base.vx*1.19,'wind still helps after permanent sandals are unlocked');
 updatePlayer(base,w,input(false,1,true),dt);updatePlayer(wind,w,input(false,1,true),dt);
 assert.ok(wind.vx>base.vx*1.19);assert.ok(wind.dashCooldown<base.dashCooldown*.66);
 updateBlessings(wind,w,4);grantBlessing(wind,'wind');assert.equal(wind.boosts.wind,30,'refresh without stacking duration');updateBlessings(wind,w,31);assert.equal(wind.boosts.wind,0);
}
{
 const p=player(),w=world();grantBlessing(p,'fireRain');assert.equal(useAbility(p,w,null),false);assert.equal(p.powers.fireRain,3,'empty casts preserve fire');
 for(let i=0;i<4;i++){
  p.abilityCooldown=0;w.enemies=[{id:'target'+i,type:'hound',x:230,y:416,w:40,h:52,hp:2}];
  assert.equal(useAbility(p,w,null),i<3?'fireRain':'holyLight','single-target fire uses the existing action, then returns to light');
  assert.equal(w.enemies[0].lastDamageSource,i<3?'fire-rain':'holy-light');
 }
 assert.equal(p.powers.fireRain,0);grantBlessing(p,'fireRain');assert.equal(p.powers.fireRain,3);
}
const manager=new AssetManager();
for(const biome of ['countryside','desert','city','mountains','ruins','storm']){
 const options={biome,seed:619,chunkCount:12},w=createLevel(options),firstWater=w.pickups.find(p=>p.id.endsWith('supply:water'));
 assert.ok(firstWater.x<300,'water available before the first jump');const p=player();p.x=firstWater.x;p.y=firstWater.y;
 assert.ok(collectPickups(w,p).includes('water'));assert.equal(p.boosts.water,45);assert.deepEqual(collectPickups(w,p),[]);
 const snapshot=persistWorldState(w),restored=createLevel(options);restoreWorldState(restored,snapshot,100);assert.equal(restored.pickups.find(item=>item.id===firstWater.id).collected,true);
 const seen=new Set();
 for(let chunk=0;chunk<w.chunkCount;chunk++){
  updateWorldStreaming(w,chunk*3200+100);
  const supplies=w.pickups.filter(p=>p.chunkIndex===chunk&&p.id.includes(':supply:'));
  assert.equal(supplies.length,2,`${biome}/${chunk}: movement and fire along the ordinary route`);
  for(const item of supplies){seen.add(item.type);assert.equal(item.secret,undefined);assert.ok(w.platforms.some(s=>!s.branch&&!s.temporary&&item.x>=s.x&&item.x+item.w<=s.x+s.w&&item.y+48===s.y),`${item.id} must be reachable on a stable main-route surface`);}
  const planned=manager.planLevel(w);for(const item of supplies)for(const asset of manager.powerIDs(item.type))assert.ok(planned.includes(asset),'resident supplies preload their effects');
 }
 assert.deepEqual([...seen].sort(),['fireRain','water','wind']);
 updateWorldStreaming(w,100);assert.equal(w.pickups.find(item=>item.id===firstWater.id).collected,true,'eviction does not regenerate collected supplies');
}
{
 const values=new Map();globalThis.localStorage={getItem:k=>values.get(k)||null,setItem:(k,v)=>values.set(k,v)};
 const storage=new GameStorage(),save=storage.loadJourney();save.checkpoint={nodeId:'countryside',x:100,y:406,seed:619,player:{powers:{fireRain:2},boosts:{water:23.75,wind:9},hearts:3}};
 storage.saveJourney(save);const restored=storage.loadJourney().checkpoint.player;
 assert.deepEqual(restored.boosts,{water:23.75,wind:9});assert.equal(restored.powers.fireRain,2);
 assert.deepEqual(cleanBoosts({water:999,wind:-7}),{water:45,wind:0});assert.deepEqual(cleanBoosts({water:Infinity,wind:'30'}),{water:0,wind:0});
}
console.log('Water and Wings air jumps, expiry/refresh, wind movement, fire-to-light casts, all-biome supplies, asset preloads, and checkpoint restore passed.');
