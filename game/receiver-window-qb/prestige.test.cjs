
const {test}=require('node:test');const assert=require('node:assert/strict');const P=require('./progression.js');
const player=()=>P.migratePlayer({name:'Test',...Object.fromEntries(P.stats.map(k=>[k,100])),trainings:0});
test('recruit tiers match 85/10/4/1 and correct OVR bands',()=>{
 const counts=[0,0,0,0],bands=[[60,79],[80,89],[90,96],[97,105]];
 for(let i=0;i<10000;i++){const p=P.recruit(player(),i/10000,()=>.5),ovr=Math.round(P.stats.reduce((n,k)=>n+p[k],0)/9),j=i<8500?0:i<9500?1:i<9900?2:3;assert.ok(ovr>=bands[j][0]&&ovr<=bands[j][1]);counts[j]++;}
 assert.deepEqual(counts,[8500,1000,400,100]);
});
test('prestige retains stats, charges escalating thousands, and renews exactly five sessions',()=>{
 const p=player();assert.equal(P.prestige(p,100000).ok,false);
 for(let i=0;i<5;i++)assert.equal(P.train(p,'speed',100000,8).ok,true);
 assert.equal(p.speed,140);assert.equal(P.train(p,'speed',100000,8).ok,false);
 const before=JSON.stringify(p);assert.equal(P.prestige(p,7499).ok,false);assert.equal(JSON.stringify(p),before);
 assert.equal(P.prestige(p,7500).cash,0);assert.equal(p.speed,140);assert.equal(P.prestigeCost(p),18750);
 for(let i=0;i<5;i++)assert.equal(P.train(p,'speed',100000,8).ok,true);
 assert.equal(P.train(p,'speed',100000,8).ok,false);assert.equal(p.speed,180);
 const f=P.validateSave({team:Array.from({length:4},()=>p),market:Array.from({length:5},player),cash:1,round:10,wins:2});
 assert.equal(f.team[0].speed,180);assert.equal(f.team[0].prestige,1);
});
test('late defenses continue improving and extreme player traits remain usable',()=>{
 for(const r of [1,10,50,100,1000]){const a=P.defenseProgress(r),b=P.defenseProgress(r+10);assert.ok(b.speed>a.speed);assert.ok(b.accel>a.accel);assert.ok(b.jump>=a.jump);assert.ok(b.smartChance>=a.smartChance);}
 const p=player();for(const k of P.stats)p[k]=100000;assert.ok(P.traits(p).jukeCooldown>0);assert.ok(P.effective(p.speed)<141);
});
