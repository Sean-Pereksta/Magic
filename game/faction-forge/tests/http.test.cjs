'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const express=require('express');
const {createRouter}=require('../../../server/faction-forge/http.cjs');const {ForgeError}=require('../../../server/faction-forge/domain.cjs');
test('HTTP boundary rejects missing credentials/unconfigured origins, keeps cache private, and authenticates mutations as token owner',async()=>{
  let action=null;
  const service={identity:async token=>{if(token!=='trusted-token')throw new ForgeError('UNAUTHENTICATED','Sign in.',401);return {uid:'verified-owner',username:'VerifiedName'};},profile:async who=>({uid:who.uid,forgeDust:10}),upgradeTroop:async(who,body)=>{action={who,body};return {profile:{uid:who.uid,forgeDust:0},transaction:{toTier:2}};},buyPack:async(who,body)=>{action={who,body};return {profile:{uid:who.uid,forgeDust:0},transaction:{packId:body.packId}};}};
  const app=express();app.use('/api/faction-forge',createRouter({service,allowedOrigins:['https://catnmice.com']}));
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});const base='http://127.0.0.1:'+server.address().port+'/api/faction-forge';
  try{
    const noAuth=await fetch(base+'/profile');assert.equal(noAuth.status,401);assert.equal((await noAuth.json()).error.code,'UNAUTHENTICATED');
    const wrongOrigin=await fetch(base+'/profile',{headers:{Origin:'https://attacker.invalid',Authorization:'Bearer trusted-token'}});assert.equal(wrongOrigin.status,403);
    const owner=await fetch(base+'/profile',{headers:{Origin:'https://catnmice.com',Authorization:'Bearer trusted-token'}});assert.equal(owner.status,200);assert.equal(owner.headers.get('access-control-allow-origin'),'https://catnmice.com');assert.equal(owner.headers.get('cache-control'),'no-store');assert.equal((await owner.json()).uid,'verified-owner');
    const upgrade=await fetch(base+'/upgrades',{method:'POST',headers:{Origin:'https://catnmice.com',Authorization:'Bearer trusted-token','Content-Type':'application/json'},body:JSON.stringify({troopKey:'rifleman',requestId:'one'})});assert.equal(upgrade.status,200);assert.equal(action.who.uid,'verified-owner');assert.deepEqual(action.body,{troopKey:'rifleman',requestId:'one'});
    const pack=await fetch(base+'/packs',{method:'POST',headers:{Origin:'https://catnmice.com',Authorization:'Bearer trusted-token','Content-Type':'application/json'},body:JSON.stringify({packId:'vanir',requestId:'pack-one'})});assert.equal(pack.status,200);assert.equal(action.who.uid,'verified-owner');assert.deepEqual(action.body,{packId:'vanir',requestId:'pack-one'});
    const preflight=await fetch(base+'/sessions/solo',{method:'OPTIONS',headers:{Origin:'https://catnmice.com'}});assert.equal(preflight.status,204);assert.equal(preflight.headers.get('access-control-allow-methods'),'GET, POST, OPTIONS');
  }finally{await new Promise(resolve=>server.close(resolve));}
});
