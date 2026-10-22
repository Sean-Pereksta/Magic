import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave,setParty,team} from './core.mjs';
import {localSocial,availableInPack,depositFromPack,retryPackDeposits,applySocialReceipt} from './social-local.mjs';

function fixture(){
  const s=newSave('Social fixture','capuchin','gps',34343);
  for(const [i,species]of ['langur','macaque','mandrill'].entries()){
    const p=structuredClone(newSave('Other',species,'gps',i+200).run.monkey);p.id=`member-${i}`;s.collection[p.id]=p;
  }
  setParty(s,[s.run.activeId,'member-0','member-1']);
  s.run.fruit=20;s.run.items.berry=4;localSocial(s);return s;
}
function receipt(extra={}){
  return {id:'receipt-1',uid:'uid-a',type:'withdraw',title:'Supplies received',walletCredited:false,
    payload:{fruit:5,xp:9,items:{berry:2}},sourceId:'source-1',createdAt:10000,seen:false,...extra};
}
function memoryStore(){
  const operations=new Map(),counts=new Map(),calls=[];let ambiguous=false;
  return {operations,counts,calls,set ambiguous(v){ambiguous=v;},
    async deposit(asset,amount,id){
      calls.push({asset,amount,id});
      if(!operations.has(id)){operations.set(id,{asset,amount});counts.set(asset,(counts.get(asset)||0)+amount);}
      else assert.deepEqual(operations.get(id),{asset,amount},'an operation cannot change its payload');
      if(ambiguous){ambiguous=false;throw Error('Response lost after commit');}
      return {id};
    }};
}
const transfer=(state,store,persist,extra={})=>({state,store,persist,uid:'uid-a',saveId:'save-a',asset:'fruit',amount:6,id:()=> 'deposit-1',...extra});

test('pack deposits persist their debit and operation before making any remote credit',async()=>{
  const s=fixture(),saved=[],order=[];
  const store={async deposit(asset,amount,id){order.push('remote');assert.equal(s.run.fruit,14);assert.deepEqual(saved[0].social.deposits[id],{asset,amount,uid:'uid-a',saveId:'save-a',runId:s.run.id});}};
  const persist=async()=>{order.push('persist');saved.push(structuredClone(s));};
  assert.equal(await depositFromPack(transfer(s,store,persist)),'deposit-1');
  assert.deepEqual(order,['persist','remote','persist']);assert.equal(s.run.fruit,14);
  assert.deepEqual(s.social.deposits,{});assert.deepEqual(saved.at(-1).social.deposits,{});
});
test('failed local persistence restores the pack and never credits the cloud wallet',async()=>{
  const s=fixture(),store=memoryStore();
  await assert.rejects(depositFromPack(transfer(s,store,async()=>{throw Error('Storage full');})),/Storage full/);
  assert.equal(s.run.fruit,20);assert.deepEqual(s.social.deposits,{});assert.equal(store.calls.length,0);
});
test('an ambiguous remote failure retains the saved debit and replays idempotently after reconnect',async()=>{
  const s=fixture(),store=memoryStore();store.ambiguous=true;let saved;
  const persist=async()=>{saved=structuredClone(s);};
  await assert.rejects(depositFromPack(transfer(s,store,persist)),/Response lost/);
  assert.equal(s.run.fruit,14);assert.ok(s.social.deposits['deposit-1']);assert.equal(store.counts.get('fruit'),6);
  const reopened=structuredClone(saved);let final;
  await retryPackDeposits({...transfer(reopened,store,async()=>{final=structuredClone(reopened);})});
  assert.equal(store.calls.length,2);assert.equal(store.counts.get('fruit'),6);assert.equal(reopened.run.fruit,14);
  assert.deepEqual(reopened.social.deposits,{});assert.deepEqual(final.social.deposits,{});
});
test('a failed post-credit checkpoint can retry the prior saved intent without a second remote credit',async()=>{
  const s=fixture(),store=memoryStore();let saved,calls=0;
  const persist=async()=>{if(++calls===2)throw Error('Lost final checkpoint');saved=structuredClone(s);};
  await assert.rejects(depositFromPack(transfer(s,store,persist)),/Lost final checkpoint/);
  assert.equal(store.counts.get('fruit'),6);
  const reopened=structuredClone(saved);
  await retryPackDeposits(transfer(reopened,store,async()=>{}));
  assert.equal(store.counts.get('fruit'),6);assert.equal(reopened.run.fruit,14);assert.deepEqual(reopened.social.deposits,{});
});
test('pending deposits from another identity or save are not credited by reconnect',async()=>{
  const s=fixture(),store=memoryStore();s.social.deposits={otherUid:{asset:'fruit',amount:4,uid:'uid-b',saveId:'save-a'},otherSave:{asset:'berry',amount:1,uid:'uid-a',saveId:'save-b'}};
  const before=structuredClone(s.social.deposits);
  await retryPackDeposits(transfer(s,store,async()=>{}));assert.equal(store.calls.length,0);assert.deepEqual(s.social.deposits,before);
});
test('only available whole supplies may leave the pack between expedition encounters',async()=>{
  const cases=[{amount:0},{amount:-1},{amount:1.5},{amount:Infinity},{amount:21},{asset:'not-a-supply'},...['battle','replacement','befriend','dead'].map(phase=>({phase}))];
  for(const options of cases){const s=fixture(),store=memoryStore();if(options.phase)s.run.phase=options.phase;const before=structuredClone(s);
    await assert.rejects(depositFromPack(transfer(s,store,async()=>{},options)));assert.deepEqual(s,before);assert.equal(store.calls.length,0);
  }
  const s=fixture(),store=memoryStore();assert.equal(availableInPack(s,'berry'),4);
  await depositFromPack(transfer(s,store,async()=>{},{asset:'berry',amount:2}));assert.equal(s.run.items.berry,2);assert.equal(store.counts.get('berry'),2);
});
test('durable receipts credit local withdrawals exactly once across a saved reload',()=>{
  const s=fixture(),r=receipt(),original=structuredClone(s);assert.equal(applySocialReceipt(s,r),true);
  assert.equal(s.run.fruit,25);assert.equal(s.run.items.berry,6);assert.equal(team(s).reduce((n,p)=>n+p.training,0),18);
  const reopened=structuredClone(s);assert.equal(applySocialReceipt(reopened,r),false);assert.deepEqual(reopened,s);
  assert.equal(original.run.monkey.training,0);
});
test('wallet-credited gifts and raid rewards never duplicate fruit/items into the expedition',()=>{
  const s=fixture(),members=team(s);members[1].level=4;members[2].level=6;
  const r=receipt({walletCredited:true});applySocialReceipt(s,r);
  assert.equal(s.run.fruit,20);assert.equal(s.run.items.berry,4);
  assert.equal(members[0].training,6);assert.equal(members[1].training,4);assert.equal(members[1].xpRemainder,.5);assert.equal(members[2].training,3);
});
test('XP receipts target the original unique companions even after a team swap',()=>{
  const s=fixture(),originalIds=team(s).map(p=>p.id);setParty(s,[s.run.activeId,'member-0','member-2']);
  applySocialReceipt(s,receipt({payload:{fruit:0,xp:9,items:{},teamIds:originalIds}}));
  assert.deepEqual(originalIds.map(id=>s.collection[id].training),[6,6,6]);assert.equal(s.collection['member-2'].training,0);
});
test('social rewards and deposits preserve expedition health, energy, statuses, stage and run identity',async()=>{
  const s=fixture();for(const p of team(s)){p.hp=9.5;p.energy=3;p.status={poison:2};p.statusPower={poison:.25};}
  const health=team(s).map(p=>({id:p.id,hp:p.hp,energy:p.energy,status:structuredClone(p.status),statusPower:structuredClone(p.statusPower),stage:p.stage})),run=s.run.id;
  applySocialReceipt(s,receipt());await depositFromPack(transfer(s,memoryStore(),async()=>{}));
  assert.deepEqual(team(s).map(p=>({id:p.id,hp:p.hp,energy:p.energy,status:p.status,statusPower:p.statusPower,stage:p.stage})),health);assert.equal(s.run.id,run);assert.equal(s.run.phase,'trail');
});
test('invalid numeric or item rewards cannot partially credit a pack or mark receipt applied',()=>{
  for(const payload of [{fruit:-1,xp:2,items:{}},{fruit:5,xp:-1,items:{}},{fruit:'5',xp:2,items:{}},{fruit:5,xp:2,items:{unknown:1}},{fruit:5,xp:2,items:{berry:1.5}}]){
    const s=fixture(),before=structuredClone(s);assert.throws(()=>applySocialReceipt(s,receipt({payload})),/Invalid social reward/);assert.deepEqual(s,before);
  }
});
test('malformed recipient team IDs are rejected before any currency or XP mutation',()=>{
  for(const teamIds of [{},true,123]){const s=fixture(),before=structuredClone(s);
    assert.throws(()=>applySocialReceipt(s,receipt({payload:{fruit:5,xp:9,items:{berry:2},teamIds}})));assert.deepEqual(s,before);
  }
});

test('duplicate or removed companion IDs never multiply a shared XP reward',()=>{
  const s=fixture(),[a,b,c]=team(s);
  applySocialReceipt(s,receipt({payload:{fruit:0,xp:9,items:{},teamIds:[a.id,a.id,b.id]}}));
  assert.equal(a.training+b.training+c.training,18);assert.equal(c.training,0);
});
