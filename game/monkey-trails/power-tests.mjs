import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave,stats,equipped} from './core.mjs';
import {snapshotTeam} from './social-combat.mjs';
import {monkeyPower,teamPower,rankMonkeys,rankCrews,POWER_EXPLANATION} from './power.mjs';

const monkey=(id='one',overrides={})=>({...structuredClone(newSave('Power tests','capuchin','gps',345).run.monkey),id,...overrides});

test('the estimate uses catalogue maximum stats and explains its battle limits',()=>{
 const p=monkey(),st=stats(p);
 assert.equal(monkeyPower(p),st.hp+4*st.atk+4*st.def+2*st.speed+8*st.maxEnergy);
 assert.match(POWER_EXPLANATION,/starting three/);assert.match(POWER_EXPLANATION,/matchups/);
});

test('levels, evolution, equipped gear and permanent mastery each increase power',()=>{
 const p=monkey(),base=monkeyPower(p);
 assert.ok(monkeyPower({...p,level:2})>base);
 assert.ok(monkeyPower({...p,stage:1,types:['fire']})>base);
 assert.ok(monkeyPower(p,{fang:2})>base);
 const evolved={...p,stage:3,types:['fire']};
 assert.ok(monkeyPower({...evolved,mastery:'fire'})>monkeyPower(evolved));
 assert.equal(monkeyPower({...evolved,mastery:'leaf'}),monkeyPower(evolved),'Unchosen elemental mastery does not count');
});

test('current injury, knockout, energy, temporary effects and battle-only modifiers do not lower or inflate power',()=>{
 const p=monkey(),before=structuredClone(p),value=monkeyPower(p);
 assert.equal(monkeyPower({...p,hp:0,energy:0,status:{poison:8},shield:200,guard:400,counter:200,prepare:20}),value);
 assert.equal(monkeyPower({...p,elite:true,bossOnly:true,tutorialHpScale:.2,combatStats:{hp:999999,atk:999999},power:999999}),value);
 assert.deepEqual(p,before);
});

test('expedition equipment and healthy social snapshots receive the same estimate',()=>{
 const s=newSave('Power tests','capuchin','gps',123);
 s.meta.gear.fang=2;s.run.monkey.equipment.weapon='fang';
 s.run.monkey.hp=3;s.run.monkey.status={poison:2};
 const direct=monkeyPower(s.run.monkey,equipped(s,s.run.monkey));
 assert.equal(monkeyPower(snapshotTeam(s)[0]),direct);
 assert.equal(teamPower([s.run.monkey],p=>equipped(s,p)),direct);
});

test('partial teams sum their real companions and never count more than three or duplicate IDs',()=>{
 const a=monkey('a'),b=monkey('b',{level:4}),c=monkey('c',{level:7}),d=monkey('d',{level:60});
 assert.equal(teamPower([]),0);assert.equal(teamPower([a]),monkeyPower(a));
 assert.equal(teamPower([a,b]),monkeyPower(a)+monkeyPower(b));
 assert.equal(teamPower([a,b,c,d]),monkeyPower(a)+monkeyPower(b)+monkeyPower(c));
 assert.equal(teamPower([a,{...a,level:60},b]),monkeyPower(a)+monkeyPower(b));
 const noId=monkey();delete noId.id;assert.equal(teamPower([noId,noId]),monkeyPower(noId));
});

test('roster rankings sort stronger monkeys first and use shared competition ranks for ties',()=>{
 const a=monkey('a',{level:6}),b=monkey('b',{level:6}),c=monkey('c'),input=[c,b,a],before=structuredClone(input);
 const rows=rankMonkeys(input);
 assert.deepEqual(rows.map(row=>[row.monkey.id,row.rank]),[['a',1],['b',1],['c',3]]);
 assert.equal(rows[0].power,monkeyPower(a));assert.deepEqual(input,before);
 assert.deepEqual(rankMonkeys([b,a,c]).map(row=>row.monkey.id),['a','b','c']);
});

test('roster ranks include equipped tiers supplied by a state-aware accessor',()=>{
 const a=monkey('a'),b=monkey('b');
 const rows=rankMonkeys([a,b],p=>p.id==='b'?{fang:3}:{});
 assert.equal(rows[0].monkey,b);assert.ok(rows[0].power>rows[1].power);
});

test('crew ranks compare starting trios with shared ties and stable player identities',()=>{
 const a={uid:'a',team:[monkey('a1',{level:4})]},b={id:'b',team:[monkey('b1',{level:4})]},c={uid:'c',team:[monkey('c1')]};
 const input=[c,b,a],before=structuredClone(input),rows=rankCrews(input);
 assert.deepEqual(rows.map(row=>[row.profile.uid||row.profile.id,row.rank]),[['a',1],['b',1],['c',3]]);
 assert.equal(rows[0].power,teamPower(a.team));assert.deepEqual(input,before);
 assert.equal(rankCrews([a,{...a,team:[monkey('fake',{level:60})]},b]).length,2);
});

test('old or malformed profiles and unknown species remain safe and cannot supply saved power',()=>{
 for(const value of [null,undefined,[],{},'capuchin',{species:'unknown'},{species:'constructor'}])assert.equal(monkeyPower(value),0);
 const p=monkey();assert.equal(monkeyPower({...p,level:NaN,stage:Infinity,types:null},null),monkeyPower({...p,level:1,stage:0}));
 assert.equal(monkeyPower(p,{fang:Infinity,armor:NaN,unknown:100,twig:-1}),monkeyPower(p));
 assert.equal(teamPower(null),0);assert.deepEqual(rankMonkeys(null),[]);assert.deepEqual(rankCrews(null),[]);
 assert.equal(rankCrews([{uid:'old',power:900000}])[0].power,0);
 assert.equal(teamPower([null,[],{species:'missing'}]),0);
});
