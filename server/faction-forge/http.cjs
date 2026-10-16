'use strict';
const { createForgeService }=require('./service.cjs');
const { ForgeError }=require('./domain.cjs');
function createRouter({db,auth,allowedOrigins=[],service}) {
  const express=require('express');const router=express.Router();const forge=service||createForgeService({db,auth});
  const windows=new Map();
  router.use((req,res,next)=>{
    const origin=req.get('Origin');
    if(origin){if(!allowedOrigins.includes(origin))return res.status(403).json({error:{code:'ORIGIN_DENIED',message:'This game origin is not configured.'}});res.set('Access-Control-Allow-Origin',origin);res.set('Vary','Origin');}
    res.set('Cache-Control','no-store');res.set('X-Content-Type-Options','nosniff');
    if(req.method==='OPTIONS'){res.set('Access-Control-Allow-Headers','Authorization, Content-Type');res.set('Access-Control-Allow-Methods','GET, POST, OPTIONS');return res.sendStatus(204);}next();
  });
  router.use(express.json({limit:'16kb'}));
  router.get('/health',(_req,res)=>res.json({service:'faction-forge',version:1}));
  router.use(async(req,res,next)=>{
    try {
      const match=/^Bearer (\S+)$/.exec(req.get('Authorization')||'');
      req.forgeUser=await forge.identity(match?.[1]);
      const stamp=Date.now(),uid=req.forgeUser.uid;let window=windows.get(uid);
      if(!window||window.until<stamp){window={until:stamp+60000,count:0};windows.set(uid,window);}
      if(++window.count>180)return res.status(429).json({error:{code:'RATE_LIMITED',message:'Wait a moment before retrying.'}});
      if(windows.size>5000)for(const [key,value]of windows)if(value.until<stamp)windows.delete(key);
      next();
    }catch(error){next(error);}
  });
  const route=fn=>async(req,res,next)=>{try{res.json(await fn(req));}catch(error){next(error);}};
  router.get('/profile',route(req=>forge.profile(req.forgeUser)));
  router.post('/sessions/solo',route(req=>forge.startSolo(req.forgeUser,req.body)));
  router.post('/sessions/pvp',route(req=>forge.joinPvp(req.forgeUser,req.body)));
  router.get('/sessions/:id',route(req=>forge.getSession(req.forgeUser,req.params.id,{knownReplayId:typeof req.query.knownReplayId==='string'?req.query.knownReplayId:null})));
  router.post('/sessions/:id/pick',route(req=>forge.pick(req.forgeUser,req.params.id,req.body)));
  router.post('/sessions/:id/abandon',route(req=>forge.abandon(req.forgeUser,req.params.id)));
  router.post('/upgrades',route(req=>forge.upgradeTroop(req.forgeUser,req.body)));
  router.post('/packs',route(req=>forge.buyPack(req.forgeUser,req.body)));
  router.post('/reward-target',route(req=>forge.target(req.forgeUser,req.body.troopKey)));
  router.post('/decks',route(req=>forge.saveDeck(req.forgeUser,req.body)));
  router.post('/selected-deck',route(req=>forge.selectDeck(req.forgeUser,req.body)));
  router.use((error,_req,res,_next)=>{
    if(error instanceof ForgeError)return res.status(error.status).json({error:{code:error.code,message:error.message}});
    if(error.code?.startsWith('auth/'))return res.status(401).json({error:{code:'UNAUTHENTICATED',message:'Your Firebase sign-in has expired. Sign in again.'}});
    if(error.type==='entity.too.large')return res.status(413).json({error:{code:'INVALID_ARGUMENT',message:'Request is too large.'}});
    if(error instanceof SyntaxError)return res.status(400).json({error:{code:'INVALID_ARGUMENT',message:'Request JSON is invalid.'}});
    console.error('Faction Forge request failed:',error.code||error.name);res.status(503).json({error:{code:'SERVICE_UNAVAILABLE',message:'Progress could not be validated. No rewards were granted. Retry safely.'}});
  });return router;
}
module.exports={createRouter};
