'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {mountFactionForge,DEFAULT_ORIGINS,PROJECT_ID}=require('../../../server/faction-forge/mount.cjs');

test('existing host mounts an isolated API using the existing Admin clients and exact site origins',()=>{
  const mounted=[],db={},auth={};let factoryArgs;
  const app={use:(prefix,router)=>mounted.push({prefix,router})};
  const result=mountFactionForge(app,{db,auth,projectId:PROJECT_ID,routerFactory:options=>{factoryArgs=options;return 'isolated-router';}});
  assert.equal(result,true);assert.deepEqual(mounted,[{prefix:'/api/faction-forge',router:'isolated-router'}]);assert.equal(factoryArgs.db,db);assert.equal(factoryArgs.auth,auth);assert.deepEqual([...factoryArgs.allowedOrigins],[...DEFAULT_ORIGINS]);assert(!factoryArgs.allowedOrigins.includes('*'));
});
test('optional mount failures and a wrong Firebase project do not disable the original host',()=>{
  let mounts=0,errors=0;const app={use(){mounts++;}},logger={error(){errors++;}};
  assert.equal(mountFactionForge(app,{db:{},auth:{},projectId:PROJECT_ID,routerFactory(){throw new Error('Unavailable optional module');},logger}),false);
  assert.equal(mountFactionForge(app,{db:{},auth:{},projectId:'unrelated-project',routerFactory(){throw new Error('Must not be called');},logger}),false);assert.equal(mounts,0);assert.equal(errors,2);
});
test('root server initializes Firebase only once and still starts Grid Overlord with Faction Forge attached',()=>{
  const source=fs.readFileSync(path.resolve(__dirname,'../../../server.js'),'utf8');let initialized=0,started=0,captured;
  const app={get(){},use(){}},db={},auth={};const certificate={project_id:PROJECT_ID};
  const admin={credential:{cert:account=>{assert.equal(account.project_id,PROJECT_ID);return {}; }},initializeApp(){initialized++;},firestore:()=>db,auth:()=>auth};
  class SocketServer {on(){}}
  const context={console:{log(){},error(){}},process:{env:{FIREBASE_SERVICE_ACCOUNT:JSON.stringify(certificate)},exit(){throw new Error('Unexpected host exit');}},require:name=>{
    if(name==='express')return ()=>app;if(name==='http')return {createServer:received=>{assert.equal(received,app);return {listen(_port,_host,callback){started++;callback();}};}};
    if(name==='socket.io')return {Server:SocketServer};if(name==='firebase-admin')return admin;if(name==='path')return path;if(name==='fs')return {existsSync:()=>false};
    if(name==='./server/faction-forge/mount.cjs')return {mountFactionForge:(received,options)=>{assert.equal(received,app);captured=options;return true;}};
    throw new Error('Unexpected import: '+name);
  }};
  vm.runInNewContext(source,context,{timeout:1000});assert.equal(initialized,1);assert.equal(started,1);assert.equal(captured.db,db);assert.equal(captured.auth,auth);assert.equal(captured.projectId,PROJECT_ID);
});
