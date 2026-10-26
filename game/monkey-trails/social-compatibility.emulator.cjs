// Test an existing rules file without modifying it. Override MONKEY_TRAILS_RULES_FILE
// to verify an older deployed rules export; all traffic stays in the local emulator.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{pathToFileURL}=require('node:url');
const {initializeTestEnvironment,assertFails}=require('@firebase/rules-unit-testing'),f=require('firebase/firestore');
(async()=>{
 const root=__dirname;
 const [host,port]=(process.env.FIRESTORE_EMULATOR_HOST||'127.0.0.1:8097').split(':');
 const rulesPath=process.env.MONKEY_TRAILS_RULES_FILE||path.resolve(__dirname,'../../firestore.rules');
 const env=await initializeTestEnvironment({projectId:'demo-monkey-trails-compat',firestore:{host,port:Number(port),rules:fs.readFileSync(rulesPath,'utf8')}});
 const {SocialStore,createFirestoreSocialAdapter}=await import(pathToFileURL(path.join(root,'social-store.mjs'))),C=await import(pathToFileURL(path.join(root,'core.mjs'))),A=await import(pathToFileURL(path.join(root,'social-combat.mjs')));
 let n=0,checks=0;const dbs={};const yes=async(p,label)=>{try{const r=await p;console.log('PASS '+label);checks++;return r;}catch(e){throw Error(label+': '+e.message,{cause:e});}};
 const make=async uid=>{const db=dbs[uid]=env.authenticatedContext(uid).firestore(),s=new SocialStore(createFirestoreSocialAdapter({db,f}),{uid,id:()=>`${uid}-${++n}`});await yes(s.initializeCompatibility(),uid+' capabilities');await yes(s.register({saveId:'save-'+uid,name:'Player '+uid,team:A.snapshotTeam(C.newSave('Player '+uid,'capuchin','gps',22))}),uid+' register');return s;};
 try {
 await env.clearFirestore();const a=await make('alice'),b=await make('bob');
 await yes(a.deposit('fruit',40,'deposit-a'),'deposit alice');await yes(b.deposit('fruit',40,'deposit-b'),'deposit bob');
 const t=await yes(a.createTribe({name:'Canopy Guardians'}),'create tribe');await yes(b.requestTribe(t.id),'join tribe');
 await yes(a.listDirectory(),'tribe directory');await yes(a.getPublicTribe(t.id),'public tribe');await yes(a.updateTribe({name:'New Canopy'}),'edit banner');
 await yes(a.gift('bob','fruit',3),'gift');await yes(a.withdraw('fruit',1,'withdraw-a'),'withdraw');
 await yes(a.deposit('berry',3,'berries-a'),'deposit berries');const o=await yes(a.offerTrade('bob',{asset:'berry',amount:1},{asset:'fruit',amount:2}),'offer trade');await yes(b.acceptTrade(o.id),'accept trade');
 const o2=await yes(a.offerTrade('bob',{asset:'berry',amount:1},{asset:'fruit',amount:2}),'second trade');await yes(b.cancelTrade(o2.id),'cancel trade');
 await yes(a.heartbeat({discoverable:true,location:{lat:40,lon:-75,accuracy:700,at:Date.now()}}),'coarse location heartbeat');await yes(b.heartbeat(),'bob heartbeat');
 const challenge=await yes(a.challenge('bob'),'challenge');await yes(b.acceptChallenge(challenge.id),'accept challenge');await yes(a.act(challenge.id,{type:'move',id:'strike'},0),'duel turn');await yes(b.surrender(challenge.id,1),'surrender and settle');await yes(a.returnToTrail(),'alice return');await yes(b.returnToTrail(),'bob return');
 const zero={meters:0,discoveries:0,challenges:0,raids:0,trades:0};await yes(a.joinAdventure(zero),'join adventure');await yes(a.contributeAdventure({...zero,meters:30000}),'contribute adventure');await yes(a.claimAdventure(),'claim weekly reward');
 await yes(a.heartbeat(),'raid heartbeat');const raid=await yes(a.startRaid(),'start raid'),attempt=A.newRaidAttempt(raid.team,raid.bossLevel,raid.seed,raid.version,raid.patternVersion);const actions=[];while(attempt.status!=='finished'){const action={type:'move',id:'brace'};A.raidAction(attempt,action);actions.push(action);}await yes(a.submitRaid(raid.id,actions),'submit raid');
 await yes(b.leaveTribe(),'leave tribe');await yes(b.joinTribe(t.id),'rejoin tribe');await yes(b.leaveTribe(),'leave again');await yes(a.leaveTribe(),'last member leave');await yes(b.joinTribe(t.id),'reclaim empty tribe');
 await yes(assertFails(f.updateDoc(f.doc(dbs.bob,'monkeyTrailsSocial/v1/players/alice'),{'wallet.fruit':999999})),'forged wallet blocked');
 await yes(assertFails(f.getDoc(f.doc(env.unauthenticatedContext().firestore(),'monkeyTrailsSocial/v1/players/alice'))),'signed-out read blocked');
 console.log('Firebase compatibility: '+checks+' checks passed');
 }finally{await env.cleanup();}
})().catch(e=>{console.error(e);process.exitCode=1;});
