const {test}=require('node:test');const assert=require('node:assert/strict');const P=require('./progression.js');
const player=()=>P.migratePlayer({name:'Test Receiver',speed:50,cutting:50,turning:50,evasion:50,catching:50,strength:50,trainings:0});
const save=()=>({cash:250,round:1,wins:0,team:Array.from({length:4},player),market:Array.from({length:5},player),checkpoint:{seriesOffense:1,seriesDefense:2,ballSpotYards:12,down:3,lineToGainYards:25}});
test('old six-stat franchises migrate and retain their roster and checkpoint',()=>{const s=save();delete s.team[0].athleticism;const loaded=P.validateSave(JSON.stringify(s));assert.equal(loaded.team[0].athleticism,50);assert.deepEqual(loaded.checkpoint,s.checkpoint);assert.equal(loaded.team[0].name,'Test Receiver')});
test('loss payouts are positive, scoring helps, and victories pay more',()=>{for(const round of [1,8,100]){assert.ok(P.payout(false,round,0)>0);assert.ok(P.payout(false,round,2)>P.payout(false,round,0));assert.ok(P.payout(true,round,3)>P.payout(false,round,2))}});
test('training is independently capped per attribute and never charges for capped/invalid training',()=>{const p=player();for(let i=0;i<5;i++)assert.equal(P.train(p,'athleticism',10000,8).ok,true);assert.equal(P.train(p,'athleticism',10000,8).ok,false);assert.equal(P.train(p,'size',10000,8).ok,true);assert.equal(P.train(p,'invalid',10000,8).ok,false);const before=JSON.stringify(p);assert.equal(P.train(p,'size',0,8).ok,false);assert.equal(JSON.stringify(p),before)});
test('athleticism, catch, size and trick ratings produce bounded gameplay gains',()=>{const low=player(),high=player();for(const k of P.stats){low[k]=1;high[k]=100}const a=P.traits(low),b=P.traits(high);for(const k of ['jumpVelocity','highReach','pursuitBurst','diveReach','sizeScale','bodyBonus','jukeChance'])assert.ok(b[k]>a[k],k);assert.ok(b.jukeCooldown<a.jukeCooldown);assert.ok(b.highReach<4.1);assert.ok(b.pursuitBurst<1.3);assert.ok(b.jukeChance<1)});
test('damaged saves are rejected before replacing local progress',()=>{let s=save();s.team=[];assert.throws(()=>P.validateSave(s));s=save();s.checkpoint.down=5;assert.throws(()=>P.validateSave(s));s=save();s.cash=-1;assert.throws(()=>P.validateSave(s));s=save();s.team[0].speed=null;assert.throws(()=>P.validateSave(s))});

test('wins triple base and capped round rewards while losses retain their payouts',()=>{
  assert.equal(P.payout(true,1,3),825);assert.equal(P.payout(true,2,3),900);
  assert.equal(P.payout(true,21,3),2325);assert.equal(P.payout(true,100,3),2325);
  assert.equal(P.payout(false,1,2),150);assert.equal(P.payout(false,100,2),300);
});
test('recruit prices stay ordered and elite players remain premium at reduced prices',()=>{
  for(let ovr=61;ovr<=105;ovr++)assert.ok(P.signingPrice(ovr)>P.signingPrice(ovr-1));
  for(const [ovr,price] of [[60,300],[79,680],[90,3500],[100,8950],[105,12200]])assert.equal(P.signingPrice(ovr),price);
  const p=P.recruit(player(),.995,()=>3/9);assert.equal(p.price,8950);
});
