import test from 'node:test';
import assert from 'node:assert/strict';
import {createSocialContextRouter,openNamedSocialContext,socialAppName} from './social-identity.mjs';
import {Vault} from './storage.mjs';
import {SocialController} from './social-controller.mjs';
import {createMemorySocialAdapter} from './social-store.mjs';
import {newSave} from './core.mjs';

const original='a'.repeat(64),second='b'.repeat(64),third='c'.repeat(64);
function fakeFirebase(persisted=new Map()){
 const registry=[],auths=new Map();let signedIn=0;
 const apps={getApps:()=>registry,initializeApp(options,name){const app={name,options};registry.push(app);return app;}};
 const authSDK={getAuth(app){if(!auths.has(app.name))auths.set(app.name,{app,currentUser:null,async authStateReady(){this.currentUser=persisted.get(app.name)||null;}});return auths.get(app.name);},async signInAnonymously(auth){signedIn++;auth.currentUser={uid:`anonymous-${auth.app.name}`};persisted.set(auth.app.name,auth.currentUser);}};
 const app=apps.initializeApp({projectId:'fixture',apiKey:'fake'},'[DEFAULT]'),context={auth:{app,currentUser:{uid:'original-uid'}},db:{app},f:{getFirestore:app=>({app})}};
 const connectNamed=(name,base)=>openNamedSocialContext(name,base,{apps,authSDK});
 const restoreNamed=(name,base)=>openNamedSocialContext(name,base,{apps,authSDK,create:false});
 return{apps,context,connectNamed,restoreNamed,persisted,signedIn:()=>signedIn};
}

test('the original trail keeps its existing UID and an unbound browser uses the default session',async()=>{
 const f=fakeFirebase();let binding={saveId:original,tribeId:'OLD-CREW'},named=0;
 const route=createSocialContextRouter({connectDefault:async()=>f.context,readPlayer:async(context,uid)=>{assert.equal(context,f.context);assert.equal(uid,'original-uid');return binding;},connectNamed:async()=>{named++;throw Error('Unexpected named session');}});
 assert.equal(await route(original),f.context);binding=null;assert.equal(await route(second),f.context);assert.equal(named,0);
});

test('different trails use distinct persistent sessions without changing the original account',async()=>{
 const f=fakeFirebase(),readPlayer=async()=>({saveId:original}),route=createSocialContextRouter({connectDefault:async()=>f.context,readPlayer,connectNamed:f.connectNamed});
 const [b,again]=await Promise.all([route(second),route(second)]),c=await route(third);
 assert.equal(b,again);assert.notEqual(b.auth.currentUser.uid,c.auth.currentUser.uid);assert.equal(f.signedIn(),2);
 assert.equal(b.auth.app.name,socialAppName(second));assert.equal(f.context.auth.currentUser.uid,'original-uid');assert.equal(await route(original),f.context);
 // Recreate the app registry and router as a fresh page would. The stored
 // named Firebase Auth session restores before anonymous sign-in is considered.
 const reopened=fakeFirebase(f.persisted),restore=createSocialContextRouter({connectDefault:async()=>reopened.context,readPlayer,connectNamed:reopened.connectNamed}),sameTrail=await restore(second);
 assert.equal(sameTrail.auth.currentUser.uid,b.auth.currentUser.uid);assert.equal(reopened.signedIn(),0);
});

test('a named trail keeps its identity when the shared default UID changes or has no profile',async()=>{
 const f=fakeFirebase(),binding=new Map([['original-uid',{saveId:original}]]),readPlayer=async context=>binding.get(context.auth.currentUser.uid)||null;
 const route=createSocialContextRouter({connectDefault:async()=>f.context,readPlayer,connectNamed:f.connectNamed,restoreNamed:f.restoreNamed});
 const named=await route(second);assert.equal(f.signedIn(),1);
 f.context.auth.currentUser={uid:'fresh-default'};assert.equal(await route(second),named,'The cached named session must survive lobby authentication changes.');
 // A page reload restores existing named auth without creating any account.
 const reload=fakeFirebase(f.persisted);reload.context.auth.currentUser={uid:'fresh-default'};
 const reopened=createSocialContextRouter({connectDefault:async()=>reload.context,readPlayer,connectNamed:reload.connectNamed,restoreNamed:reload.restoreNamed}),restored=await reopened(second);
 assert.equal(restored.auth.currentUser.uid,named.auth.currentUser.uid);assert.equal(reload.signedIn(),0);
 assert.equal(await reopened(third),reload.context,'An unbound default remains available when the selected trail has no separate session.');assert.equal(reload.signedIn(),0);
 reload.context.auth.currentUser={uid:'original-uid'};assert.equal(await reopened(original),reload.context,'The original save keeps its matching default identity.');
});

test('restore-only checks never create anonymous accounts and propagate restoration errors',async()=>{
 const f=fakeFirebase();assert.equal(await f.restoreNamed(socialAppName(second),f.context),null);assert.equal(f.signedIn(),0);
 let reads=0,named=0;const failure=Error('Auth restoration unavailable'),route=createSocialContextRouter({connectDefault:async()=>f.context,readPlayer:async()=>{reads++;return null;},restoreNamed:async()=>{throw failure;},connectNamed:async()=>{named++;}});
 await assert.rejects(route(second),error=>error===failure);assert.equal(reads,0);assert.equal(named,0);
});

test('permission and connection failures propagate instead of allocating a replacement identity',async()=>{
 const f=fakeFirebase();let named=0;
 for(const code of ['permission-denied','unavailable']){
  const failure=Object.assign(Error(code),{code}),route=createSocialContextRouter({connectDefault:async()=>f.context,readPlayer:async()=>{throw failure;},connectNamed:async()=>{named++;}});
  await assert.rejects(route(second),error=>error===failure);
 }
 assert.equal(named,0);
 let connected=0;const route=createSocialContextRouter({connectDefault:async()=>{connected++;return f.context;},readPlayer:async()=>null,connectNamed:f.connectNamed});
 for(const id of [undefined,'../other','short','A'.repeat(64)])await assert.rejects(route(id),/valid saved trail/);
 assert.equal(connected,0);
});

test('a failed named-session connection can be retried without replacing the default account',async()=>{
 const f=fakeFirebase();let tries=0;const route=createSocialContextRouter({connectDefault:async()=>f.context,readPlayer:async()=>({saveId:original}),connectNamed:async(...args)=>{if(!tries++)throw Error('Temporary auth outage');return f.connectNamed(...args);}});
 await assert.rejects(route(second),/Temporary auth outage/);assert.equal((await route(second)).auth.app.name,socialAppName(second));assert.equal(tries,2);assert.equal(f.context.auth.currentUser.uid,'original-uid');
});

test('opening another social account leaves encrypted cloud checkpoints on the default connection',async()=>{
 const f=fakeFirebase(),rows=new Map(),storage={getItem:key=>rows.get(key)||null,setItem:(key,value)=>rows.set(key,value)},commits=[];
 const cloud={context:f.context,read:async()=>null,async commit(_id,base){commits.push(this.context.auth.currentUser.uid);return base+1;}},vault=new Vault(()=>{},storage,async()=>cloud);
 const state=await vault.open('Second Trail','a-test-password',{create:true});await vault.sync();
 const route=createSocialContextRouter({connectDefault:async()=>cloud.context,readPlayer:async()=>({saveId:original}),connectNamed:f.connectNamed}),social=await route(vault.id);
 assert.notEqual(social.auth.currentUser.uid,'original-uid');state.run.fruit+=1;await vault.checkpoint(state,true);await vault.sync();
 assert.ok(commits.length>=2);assert.ok(commits.every(uid=>uid==='original-uid'));assert.equal(vault.adapter,cloud);assert.equal(cloud.context,f.context);
});

test('the social controller passes the opened save identity to its connection',async t=>{
 globalThis.document={hidden:false};const state=newSave('Original Trail','capuchin','gps',123),adapter=createMemorySocialAdapter();let received;
 const controller=new SocialController({getState:()=>state,getVault:()=>({id:original}),persist:async()=>{},connect:async saveId=>{received=saveId;return{adapter,uid:'original-uid'};}});t.after(()=>controller.stop());
 await controller.start();assert.equal(received,original);assert.equal(controller.model.status,'ready');
});
