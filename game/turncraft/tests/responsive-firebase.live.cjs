'use strict';
// Two-browser integration against Auth + Firestore emulators by default. The
// live project requires explicit opt-in and creates one private, unranked QA room.
// No player profiles or ranks are written; artwork is replaced by image fixtures.
const emulated=!!(process.env.FIRESTORE_EMULATOR_HOST&&process.env.FIREBASE_AUTH_EMULATOR_HOST);
if(!emulated&&process.env.TURNCRAFT_LIVE_FIREBASE!=='1')throw new Error('Run with Firestore + Auth emulators, or set TURNCRAFT_LIVE_FIREBASE=1 for the real playtest.');
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),{chromium}=require('playwright');
const repo=path.resolve(__dirname,'../../..'),stamp=Date.now(),roomId=`tc-orders-qa-${stamp}`;
const origin=emulated?'http://127.0.0.1:8896':'http://turncraft.test';
const expose=`window.tcLive={run:source=>eval(source)};window.qaIssued=[];const qaOriginalSend=sendCommand;sendCommand=function(c){const started=performance.now(),result=qaOriginalSend(c);qaIssued.push({type:c.type,feedbackMs:performance.now()-started,pending:unackedCommands.size,status:orderFeedbackEntries.get(orderFeedbackLatest)?.status});return result};resize();syncMyMatchResult=async()=>true;window.TurncraftStartup?.complete();`;
let source=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8').replace('\nboot();','\n'+expose).replace('<script type="module" src="turncraft/shattered-front/menu.mjs"></script>','');
if(emulated){
 const [fireHost,firePort]=process.env.FIRESTORE_EMULATOR_HOST.split(':');
 source=source.replace('<script type="module">',`<script type="module">\nimport {connectFirestoreEmulator} from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js';\nimport {connectAuthEmulator} from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js';`)
  .replace('projectId:"bible-game-246c0"','projectId:"demo-turncraft"')
  .replace('const params=new URLSearchParams(location.search);',`connectAuthEmulator(auth,${JSON.stringify('http://'+process.env.FIREBASE_AUTH_EMULATOR_HOST)},{disableWarnings:true});connectFirestoreEmulator(db,${JSON.stringify(fireHost)},${Number(firePort)});\nconst params=new URLSearchParams(location.search);`);
}
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/6kQAAAAASUVORK5CYII=','base64');
const report={roomId,startedAt:new Date().toISOString(),project:emulated?'demo-turncraft (local Firestore + Auth emulators)':'configured TurnCraft Firebase',checks:[],errors:[],cleanup:[]};
report.transportErrors=[];report.consoleErrors=[];report.httpErrors=[];
let browser,host,peer,contexts=[];
async function run(page,code){let timer;try{return await Promise.race([page.evaluate(code=>tcLive.run(code),code),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('Firebase playtest step timed out: '+code.slice(0,100))),30000)})])}finally{clearTimeout(timer)}}
async function check(page,code,timeout=20000){await page.waitForFunction(code=>window.tcLive&&tcLive.run(code),code,{timeout});}
async function setup(touch){const context=await browser.newContext({viewport:touch?{width:430,height:932}:{width:1360,height:900},hasTouch:touch});contexts.push(context);if(emulated)await context.grantPermissions(['local-network-access'],{origin});const page=await context.newPage();page.on('pageerror',e=>report.errors.push(e.message));page.on('console',message=>{if(message.type()==='error')report.consoleErrors.push({message:message.text(),url:message.location().url})});page.on('response',response=>{if(response.status()>=400)report.httpErrors.push({status:response.status(),url:response.url().slice(0,160)})});page.on('requestfailed',r=>report.transportErrors.push({url:r.url().slice(0,160),error:r.failure()?.errorText}));await page.route('**/*',async route=>{const req=route.request(),url=new URL(req.url());if(req.resourceType()==='image')return route.fulfill({body:png,contentType:'image/png'});if(url.origin!==origin)return route.continue();if(url.pathname==='/game/turncraft.html')return route.fulfill({body:source,contentType:'text/html'});const file=path.resolve(repo,'.'+decodeURIComponent(url.pathname));if(file.startsWith(repo+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file,contentType:file.endsWith('.mjs')?'text/javascript':undefined});return route.fulfill({status:404,body:''});});await page.goto(origin+'/game/turncraft.html');await check(page,'true');return page}
async function identify(page,name){return run(page,`(async()=>{username=${JSON.stringify(name)};await restoreFirebaseUser(auth,{onAuthStateChanged,signInAnonymously});const api=await import('https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js'),{createLobbyStore}=await import('./turncraft/shattered-front/lobby-store.mjs');window.qaApi=api;window.qaStore=createLobbyStore({db,auth,api,getIdentity:()=>({uid:auth.currentUser.uid,username})});return {uid:auth.currentUser.uid,name:username}})()`)}
(async()=>{
 browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
 try{
  host=await setup(false);peer=await setup(true);
  report.identities=await Promise.all([identify(host,`TCQA${stamp}Host`),identify(peer,`TCQA${stamp}Peer`)]);
  const room=await run(host,`qaStore.create({requestId:${JSON.stringify(roomId)},name:'TurnCraft responsive orders QA',visibility:'private',settings:{mapId:'classic',maxPlayers:2,fillAi:false}})`);
  await run(peer,`qaStore.join({code:${JSON.stringify(room.inviteCode)}})`);
  await run(host,`qaStore.player('${roomId}',{ready:true})`);await run(peer,`qaStore.player('${roomId}',{factionId:'ascendant',ready:true})`);
  await run(host,`qaStore.start('${roomId}')`);
  await run(host,`lobbyId='${roomId}';bootNetwork()`);await run(peer,`lobbyId='${roomId}';bootNetwork()`);
  await check(host,'gameStarted&&isController',30000);await check(peer,'gameStarted&&!isController',30000);
  report.checks.push('private unranked room started in two independent Firebase Auth browser contexts');
  const unit=await run(host,`(()=>{terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));entities=entities.filter(e=>e.kind==='structure'&&isBase(e));resourceNodes=[];temporaryObstacles=[];pendingImpacts=[];window.qaTroop=addEntity('unit','dragoon',1,16,15);window.qaHostTroop=addEntity('unit','rifleman',0,25,7);stateDirty=true;writeState(true);return qaTroop.id})()`);
  await check(peer,`!!entityById(${unit})`);
  await Promise.all([run(host,'lastFrame=performance.now();requestAnimationFrame(frame)'),run(peer,'lastFrame=performance.now();requestAnimationFrame(frame)')]);
  const desktop=await run(host,`selectEntity(qaHostTroop);cam.zoom=.7;centerOn(25,7);worldToScreen(26.5,7.5)`);await host.mouse.click(desktop.x,desktop.y);await check(host,'qaIssued.length===1&&unackedCommands.size===0');
  assert.equal(await run(host,'qaIssued[0].status'),'pending','host presentation awaits its committed state too');
  const touchPoint=await run(peer,`selectEntity(entityById(${unit}));cam.zoom=.7;centerOn(20,15);worldToScreen(22.5,15.5)`),touchStarted=Date.now();await peer.touchscreen.tap(touchPoint.x,touchPoint.y);
  await check(peer,'qaIssued.length===1');const immediate=await run(peer,'qaIssued[0]');assert.ok(immediate.pending>=1,'local pending exists in the issuing event before state ack');assert.equal(immediate.status,'pending');report.firstTouchFeedbackMs=immediate.feedbackMs;
  await check(host,`entityById(${unit}).moveDestination?.x===22`);await check(peer,'unackedCommands.size===0');report.firstTouchOrderAckMs=Date.now()-touchStarted;
  await run(peer,`sendSelectionMovement({x:8,y:15});sendSelectionMovement({x:26,y:15});sendSelectedTroopOrder('stop')`);
  await check(host,`entityById(${unit}).order==='stop'`);await check(peer,'unackedCommands.size===0');
  const stopped=await run(host,`({x:entityById(${unit}).x,y:entityById(${unit}).y})`);
  await peer.waitForTimeout(1100);assert.deepEqual(await run(host,`({x:entityById(${unit}).x,y:entityById(${unit}).y})`),stopped);
  report.checks.push(`desktop mouse and touch tap pending feedback, rapid 180-degree redirects and final stop acknowledged through ${emulated?'Firestore emulator':'live Firestore'}`);
  await run(peer,`qaApi.disableNetwork(db)`);
  await run(peer,`sendSelectionMovement({x:9,y:15});sendSelectedTroopOrder('hold')`);
  assert.ok(await run(peer,'unackedCommands.size')>=2);
  await run(peer,'qaApi.enableNetwork(db)');await check(host,`entityById(${unit}).order==='hold'`,30000);await check(peer,'unackedCommands.size===0',30000);
  report.checks.push('offline outbox reconnect preserves move then hold');report.feedbackBeforeReload=await run(peer,'qaIssued');
  // Reload the follower in the same browser context, retaining Auth and stream.
  await peer.reload();await check(peer,'true');await identify(peer,report.identities[1].name);await run(peer,`lobbyId='${roomId}';bootNetwork()`);await check(peer,`gameStarted&&!!entityById(${unit})`,30000);
  await run(peer,`selectEntity(entityById(${unit}));sendSelectionMovement({x:25,y:15});sendSelectedTroopOrder('stop')`);await check(host,`entityById(${unit}).order==='stop'`);await check(peer,'unackedCommands.size===0');
  report.checks.push('same-context reload restores authenticated seat and command stream');
  await run(host,`for(const u of [lobbyUnsub,presenceUnsub,commandUnsub,stateUnsub])u?.();clearInterval(presenceTimer);clearInterval(controllerTimer);isController=false;paused=true;qaApi.disableNetwork(db)`);
  await peer.waitForTimeout(17000);await run(peer,'refreshPresenceAndLease()');await check(peer,'isController',30000);
  await run(peer,`selectEntity(entityById(${unit}));sendSelectionMovement({x:9,y:15});sendSelectedTroopOrder('stop')`);await check(peer,'unackedCommands.size===0',30000);
  report.checks.push('surviving browser acquires expired controller lease and acknowledges new commands');
  report.final=await run(peer,`({epoch:controllerLease.epoch,order:entityById(${unit}).order,pending:unackedCommands.size,snapshotBytes:new TextEncoder().encode(JSON.stringify(serializeState())).length})`);
  report.feedback=await Promise.all([run(host,'qaIssued'),run(peer,'qaIssued')]);report.maximumImmediateFeedbackMs=Math.max(...[...report.feedback.flat(),...report.feedbackBeforeReload].map(row=>row.feedbackMs));
  assert.equal(report.final.order,'stop');assert.equal(report.final.pending,0);assert.deepEqual(report.errors,[]);
  report.passed=true;
 }catch(error){report.passed=false;report.failure={message:error.message,stack:error.stack};process.exitCode=1;}
 finally{
  // Finish only this test room. Suppress result/profile writes in the fixture.
  for(const page of [peer,host].filter(Boolean)){try{const cleanup=await run(page,`(async()=>{if(lobbyId==='${roomId}'&&isController&&gameStarted){if(!gameOver)finishMatch(null,'Responsive-order QA complete');await writeState(true);return {action:'finish',complete:customCompletionAcknowledged||lobbyData?.status==='finished'}}if(window.qaStore&&!gameStarted){try{await qaStore.leave('${roomId}');return {action:'leave',complete:true}}catch(error){return {action:'leave',complete:false,error:error.message}}}return {action:'none'}})()`);report.cleanup.push(cleanup);if(cleanup.action==='finish'&&!cleanup.complete){report.passed=false;process.exitCode=1}}catch(error){report.cleanup.push({action:'cleanup',complete:false,error:error.message});report.passed=false;process.exitCode=1}}
  report.finishedAt=new Date().toISOString();if(process.env.TURNCRAFT_QA_OUTPUT)fs.writeFileSync(process.env.TURNCRAFT_QA_OUTPUT,JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report,null,2));await browser?.close();
 }
})().catch(error=>{console.error(error);process.exitCode=1});
