import test from 'node:test';
import assert from 'node:assert/strict';
import {newSoloProfile} from '../firebase-progress.js';
import {resolveForgeUsername,selectSoloAccount} from '../solo-account.js';

const troops={rifleman:{},medic:{},signalhawk:{unlockable:true}};
const rootPath='factionForgeSoloProfiles/owner';
function firebase(initial=[]){
  const docs=new Map(initial),versions=new Map();
  const api={doc:(_db,...parts)=>parts.join('/'),runTransaction:async(_db,callback)=>{
    for(let attempt=0;attempt<10;attempt++){
      const reads=new Map(),writes=new Map();
      const value=await callback({get:async ref=>{
        reads.set(ref,versions.get(ref)||0);
        const value=structuredClone(docs.get(ref));
        return {exists:()=>value!==undefined,data:()=>value};
      },set:(ref,value)=>writes.set(ref,structuredClone(value))});
      if([...reads].some(([ref,version])=>(versions.get(ref)||0)!==version))continue;
      for(const [ref,value] of writes){docs.set(ref,value);versions.set(ref,(versions.get(ref)||0)+1);}
      return value;
    }
    throw Error('Transaction did not converge');
  }};
  return {api,docs};
}
const select=(store,username,auth={currentUser:{uid:'owner'}})=>selectSoloAccount({auth,db:{},api:store.api,username,troops});

test('solo entry uses the current stored hub name while multiplayer uses its lobby URL',()=>{
  const params=new URLSearchParams('username=Commander'),storage={getItem:()=> ' Sean '};
  assert.equal(resolveForgeUsername({params,storage}),'Sean');
  assert.equal(resolveForgeUsername({params,storage,solo:false}),'Commander');
  assert.equal(resolveForgeUsername({params:new URLSearchParams('username=%20Alice%20'),storage:{getItem:()=> ' '}}),'Alice');
  assert.equal(resolveForgeUsername({params:new URLSearchParams(),storage:{getItem(){throw Error('Blocked');}}}),'Guest');
  assert.equal(resolveForgeUsername({storage:{getItem:()=> 'x'.repeat(90)}}).length,80);
});

test('solo entry reads browser storage by default and tolerates an inaccessible storage getter',()=>{
  const original=Object.getOwnPropertyDescriptor(globalThis,'localStorage');
  try{
    Object.defineProperty(globalThis,'localStorage',{configurable:true,value:{getItem:()=> 'Sean'}});
    assert.equal(resolveForgeUsername({params:new URLSearchParams('username=Commander')}),'Sean');
    Object.defineProperty(globalThis,'localStorage',{configurable:true,get(){throw Error('Blocked');}});
    assert.equal(resolveForgeUsername({params:new URLSearchParams('username=Alice')}),'Alice');
  }finally{
    if(original)Object.defineProperty(globalThis,'localStorage',original);
    else delete globalThis.localStorage;
  }
});

test('a new save stays on the root path and retains its pending queue namespace',async()=>{
  const store=firebase(),selection=await select(store,'Sean');
  assert.deepEqual(selection,{profilePath:['factionForgeSoloProfiles','owner'],queueNamespace:'',username:'Sean'});
  assert.equal(store.docs.get(rootPath).username,'Sean');
  assert.deepEqual(await select(store,'Sean'),selection);
  assert.equal(store.docs.size,1);
});

test('legacy placeholder claim retains balances, campaign, cards, and original receipts',async()=>{
  for(const legacyName of ['Commander','Guest','']){
    const profile=newSoloProfile('owner',legacyName||'Commander',troops);
    profile.username=legacyName;profile.forgeDust=940;profile.troops.rifleman.cards=75;
    profile.campaign.missions['forge-01']={completed:true,stars:3,clears:4};
    const result={id:'already-won',forgeDust:20},upgrade={requestId:'already-upgraded',cardsSpent:10};
    const store=firebase([[rootPath,profile],[rootPath+'/results/already-won',result],[rootPath+'/upgrades/already-upgraded',upgrade]]);
    const selected=await select(store,'Sean');
    assert.equal(selected.queueNamespace,'');
    assert.deepEqual(store.docs.get(rootPath),{...profile,username:'Sean'});
    assert.deepEqual(store.docs.get(rootPath+'/results/already-won'),result);
    assert.deepEqual(store.docs.get(rootPath+'/upgrades/already-upgraded'),upgrade);
    assert.equal(store.docs.size,3);
  }
});

test('different names and guests get independent saves with case-sensitive encoded paths and queues',async()=>{
  const original=newSoloProfile('owner','Sean',troops);original.forgeDust=400;
  const store=firebase([[rootPath,original]]);
  for(const name of ['sean','Alice / Bob','Guest','Commander','.','..','__name__']){
    const selected=await select(store,name),key='name-'+encodeURIComponent(name);
    assert.deepEqual(selected.profilePath,['factionForgeSoloProfiles','owner','accounts',key]);
    assert.equal(selected.queueNamespace,'.'+key);
    assert.equal(store.docs.get(selected.profilePath.join('/')).forgeDust,0);
    assert.deepEqual(await select(store,name),selected);
  }
  assert.deepEqual(store.docs.get(rootPath),original);
});

test('concurrent names can claim a legacy save only once',async()=>{
  const legacy=newSoloProfile('owner','Commander',troops);legacy.forgeDust=1200;
  const store=firebase([[rootPath,legacy]]);
  const selections=await Promise.all([select(store,'Sean'),select(store,'Alice')]);
  assert.equal(selections.filter(value=>value.queueNamespace==='').length,1);
  const owner=store.docs.get(rootPath).username,other=owner==='Sean'?'Alice':'Sean';
  assert.equal(store.docs.get(rootPath).forgeDust,1200);
  assert.equal(store.docs.get(rootPath+'/accounts/name-'+other).forgeDust,0);
});

test('concurrent first saves assign the root once and scope the other name',async()=>{
  const store=firebase();
  const selections=await Promise.all([select(store,'Sean'),select(store,'Alice')]);
  assert.equal(selections.filter(value=>value.queueNamespace==='').length,1);
  assert.equal(store.docs.size,2);
});

test('a changed UID while reading cannot write the previous owner save',async()=>{
  const auth={currentUser:{uid:'owner'}},store=firebase();
  const original=store.api.runTransaction;
  store.api.runTransaction=(db,callback)=>original(db,tx=>callback({...tx,get:async ref=>{
    const snap=await tx.get(ref);auth.currentUser={uid:'other'};return snap;
  }}));
  await assert.rejects(select(store,'Sean',auth),{code:'ACCOUNT_CHANGED'});
  assert.equal(store.docs.size,0);
});

test('a changed UID after transaction completion rejects its selection',async()=>{
  const auth={currentUser:{uid:'owner'}},store=firebase([[rootPath,newSoloProfile('owner','Sean',troops)]]);
  const original=store.api.runTransaction;
  store.api.runTransaction=async(db,callback)=>{const value=await original(db,callback);auth.currentUser={uid:'other'};return value;};
  await assert.rejects(select(store,'Sean',auth),{code:'ACCOUNT_CHANGED'});
  assert.equal(store.docs.size,1);
});
