// Real Chromium/Firestore smoke test. Connects only to a local emulator.
// Run with FIRESTORE_EMULATOR_HOST=127.0.0.1:8097 and the Bible Game rules loaded.
const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const assert=require('node:assert/strict');
let playwright;
try{playwright=require('playwright')}catch{playwright=require(path.join(process.env.CODEX_NODE_MODULES||'C:/Users/seanp/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules','playwright'))}
const project=process.env.GCLOUD_PROJECT||'demo-biblegame';
const emulator=process.env.FIRESTORE_EMULATOR_HOST||'127.0.0.1:8097';
if(!/^(127\.0\.0\.1|localhost):\d+$/.test(emulator))throw new Error('This test requires a localhost Firestore emulator.');
const [emulatorHost,emulatorPort]=emulator.split(':'),stamp=Date.now(),gameId=`browser-${stamp}`;
const names=[`BrowserAlice${stamp}`,`BrowserBob${stamp}`],authIds=[`browser-alice-${stamp}`,`browser-bob-${stamp}`];
const appRoot=path.join(__dirname,'..','..');
const bible='VerseId,BookName,Chapter,Verse,Text\n1,Genesis,1,1,In the beginning God created the heaven and the earth.\n2,Genesis,1,2,The Spirit of God moved upon the face of the waters.\n3,Genesis,1,3,And God said Let there be light.\n4,Genesis,1,4,God saw the light that it was good.\n5,John,3,16,For God so loved the world.\n6,John,3,17,God sent not his Son into the world to condemn the world.\n7,John,3,18,He that believeth on him is not condemned.\n8,John,3,19,Light is come into the world.\n';
const encodeValue=value=>value===null?{nullValue:null}:value instanceof Date?{timestampValue:value.toISOString()}:Array.isArray(value)?{arrayValue:{values:value.map(encodeValue)}}:typeof value==='object'?{mapValue:{fields:Object.fromEntries(Object.entries(value).map(([key,item])=>[key,encodeValue(item)]))}}:typeof value==='boolean'?{booleanValue:value}:typeof value==='number'?{integerValue:String(value)}:{stringValue:String(value)};
const decodeValue=value=>'timestampValue'in value?new Date(value.timestampValue):'arrayValue'in value?(value.arrayValue.values||[]).map(decodeValue):'mapValue'in value?decodeFields(value.mapValue.fields||{}):'nullValue'in value?null:'booleanValue'in value?value.booleanValue:'integerValue'in value?Number(value.integerValue):'doubleValue'in value?value.doubleValue:value.stringValue;
const decodeFields=fields=>Object.fromEntries(Object.entries(fields).map(([key,value])=>[key,decodeValue(value)]));
async function restDoc(documentPath,patch){
  const base=`http://${emulator}/v1/projects/${project}/databases/(default)/documents/${documentPath}`;
  const fields=patch&&Object.fromEntries(Object.entries(patch).map(([key,value])=>[key,encodeValue(value)]));
  const mask=patch?'?'+Object.keys(patch).map(key=>'updateMask.fieldPaths='+encodeURIComponent(key)).join('&'):'';
  const response=await fetch(base+mask,{method:patch?'PATCH':'GET',headers:{Authorization:'Bearer owner','Content-Type':'application/json'},...(patch?{body:JSON.stringify({fields})}:{})});
  if(!response.ok)throw new Error(`${response.status} ${documentPath}: ${await response.text()}`);
  return decodeFields((await response.json()).fields||{});
}
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function eventually(check,label,timeout=15000){const until=Date.now()+timeout;let error;while(Date.now()<until){try{const result=await check();if(result)return result}catch(caught){error=caught}await pause(150)}throw new Error(`Timed out: ${label}${error?' — '+error.message:''}`)}
const state=page=>page.evaluate(()=>window.__bibleTest?.state());
const browserErrors=[],deniedRequests=[],badResponses=[];
let server,browser,debugPages=[],debugSessionPath='';
(async()=>{
  await restDoc(`lobbies/${gameId}`,{host:names[0],players:names,gameType:'biblegame'});
  for(let i=0;i<names.length;i++)await restDoc(`users/${names[i]}`,{username:names[i],chatAuthUid:authIds[i],bibleGame:{elo:0,wins:0,losses:0,draws:0,gamesPlayed:0}});
  server=http.createServer((request,response)=>{
    const url=new URL(request.url,'http://localhost');
    if(url.pathname==='/favicon.ico'){response.statusCode=204;response.end();return}
    if(url.pathname==='/test-auth.js'){
      response.setHeader('Content-Type','text/javascript');
      response.end(`const names=${JSON.stringify(names)},ids=${JSON.stringify(authIds)};const uid=ids[names.indexOf(new URL(location.href).searchParams.get('username'))];export const getAuth=()=>({});export const signInAnonymously=async()=>({user:{uid}});export const onAuthStateChanged=(_auth,callback)=>setTimeout(()=>callback({uid}),0);`);return;
    }
    if(url.pathname==='/bible.csv'){response.setHeader('Content-Type','text/csv');response.end(bible);return}
    const filename=path.resolve(appRoot,'.'+decodeURIComponent(url.pathname));
    if(!filename.startsWith(appRoot+path.sep)){response.statusCode=403;response.end();return}
    try{
      let source=fs.readFileSync(filename,'utf8');
      if(url.pathname.endsWith('/biblegame-multiplayer.html')){
        source=source.replace('getFirestore,doc','connectFirestoreEmulator,getFirestore,doc')
          .replace(/"https:\/\/www\.gstatic\.com\/firebasejs\/[^\"]+\/firebase-auth\.js"/,'"/test-auth.js"')
          .replace('projectId:"bible-game-246c0"',`projectId:${JSON.stringify(project)}`)
          .replace('const MODE_DURATION=',`connectFirestoreEmulator(db,${JSON.stringify(emulatorHost)},${Number(emulatorPort)},{mockUserToken:{sub:currentMockUid()}});function currentMockUid(){return ${JSON.stringify(authIds)}[${JSON.stringify(names)}.indexOf(username)]}\nconst MODE_DURATION=`)
          .replace('https://raw.githubusercontent.com/Sean-Pereksta/Bible-Data/refs/heads/main/web.csv','/bible.csv')
          .replace('</script>',`window.__bibleTest={state:()=>({session:SESSION,players:Array.from(PLAYERS.values()),uid:playerUid,connectionId,synced:canUseSyncedSession(),clockOffset:serverClockOffsetMs})};\n</script>`);
      }
      response.setHeader('Content-Type',filename.endsWith('.html')?'text/html':'text/javascript');response.end(source);
    }catch(error){response.statusCode=404;response.end(error.message)}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const origin=`http://127.0.0.1:${server.address().port}`;
  const chrome=process.env.CHROME_PATH||'C:/Program Files/Google/Chrome/Application/chrome.exe';
  browser=await playwright.chromium.launch({headless:true,...(fs.existsSync(chrome)?{executablePath:chrome}:{})});
  const contexts=await Promise.all(names.map(()=>browser.newContext())),pages=await Promise.all(contexts.map(context=>context.newPage()));debugPages=pages;
  for(const page of pages){page.on('pageerror',error=>browserErrors.push(error.message));page.on('console',message=>{if(message.type()==='error')browserErrors.push(message.text())});page.on('response',async response=>{if(response.status()>=400&&response.url().includes(emulator)){if(response.status()===403)deniedRequests.push(response.request().postData());try{badResponses.push({http:response.status(),body:await response.json()})}catch{}}})}
  await Promise.all(pages.map((page,index)=>page.goto(`${origin}/game/biblegame-multiplayer.html?gameId=${gameId}&username=${names[index]}`)));
  await eventually(async()=>{const states=await Promise.all(pages.map(state));return states.every(item=>item?.synced&&item.players.length===2&&item.session?.status==='voting')},'two browsers joined',30000);
  const initial=(await state(pages[0])).session,sessionPath=`lobbies/${gameId}/bibleSessions/${initial.sessionId}`;debugSessionPath=sessionPath;
  console.log('PASS two browsers create/join the same live Firestore session');
  await Promise.all(pages.map(page=>page.locator('[data-mode="teams"]').click()));
  await eventually(async()=>{const s=await restDoc(sessionPath+'/votes/'+(await state(pages[0])).uid);return s.mode==='teams'},'host vote saved');
  await eventually(async()=>{const s=await restDoc(sessionPath+'/votes/'+(await state(pages[1])).uid);return s.mode==='teams'},'guest vote saved');
  await restDoc(sessionPath,{phaseStartedAt:new Date(Date.now()-31000)});
  await eventually(async()=>{const states=await Promise.all(pages.map(state));return states.every(item=>item.session?.mode==='teams'&&item.session.status==='playing'&&item.session.round===1)},'shared teams round');
  console.log('PASS votes and shared team rosters transition atomically');
  const firstQuestion=(await restDoc(sessionPath)).currentQuestion;
  await restDoc(sessionPath,{phaseStartedAt:new Date(Date.now()-45000)});
  await eventually(async()=>Promise.all(pages.map(page=>page.locator('#submitBtn').isEnabled())).then(enabled=>enabled.every(Boolean)),'team final window');
  for(const page of pages){await page.locator('#bookInput').selectOption(firstQuestion.book);await page.locator('#chapInput').fill(String(firstQuestion.chapter));await page.locator('#verseInput').fill(String(firstQuestion.verse))}
  await Promise.all(pages.map(page=>page.locator('#submitBtn').click()));
  await eventually(async()=>{const s=await restDoc(sessionPath);return s.teamLocks.A&&s.teamLocks.B&&s.teamTotals.A===5&&s.teamTotals.B===5},'both team answers committed');
  await eventually(async()=>{const states=await Promise.all(pages.map(state));return states.every(item=>item.session.round===2&&item.session.roundKey===2)},'shared early next round');
  console.log('PASS concurrent team locks score once and reveal before next shared round');
  const beforeOffset=(await state(pages[1])).clockOffset;
  const hostUid=(await state(pages[0])).uid;
  await contexts[0].setOffline(true);
  await restDoc(sessionPath+'/players/'+hostUid,{online:false});
  await restDoc(sessionPath,{phaseStartedAt:new Date(Date.now()-61000)});
  await eventually(async()=>{const item=await state(pages[1]);return item.session.round===3},'guest driver failover');
  assert.ok(Math.abs((await state(pages[1])).clockOffset-beforeOffset)<1000,'unrelated player writes must not move clock offset');
  await contexts[0].setOffline(false);
  await eventually(async()=>{const item=await state(pages[0]);return item.synced&&item.session.round===3},'host reconnect catches up');
  console.log('PASS failover advances without host and reconnect restores the same round');
  await restDoc(sessionPath,{status:'ended',durationMs:0,endReason:'completed',teamTotals:{A:10,B:5}});
  await eventually(async()=>{const s=await restDoc(sessionPath);return s.ratingsApplied},'match results finalized');
  for(const name of names)await eventually(async()=>{const receipt=await restDoc(`users/${name}/bibleGameResults/${initial.sessionId}`);return receipt.sessionId===initial.sessionId},'rating receipt '+name);
  for(const name of names)assert.equal((await restDoc(`users/${name}`)).bibleGame.gamesPlayed,1,'account receives result once');
  await Promise.all(pages.map(page=>page.reload()));
  await eventually(async()=>{const states=await Promise.all(pages.map(state));return states.every(item=>item?.synced&&item.session?.status==='ended'&&item.players.find(player=>player.uid===item.uid)?.connectionId===item.connectionId&&item.players.find(player=>player.uid===item.uid)?.online)},'reload ended game');
  for(const name of names)assert.equal((await restDoc(`users/${name}`)).bibleGame.gamesPlayed,1,'reload does not apply rating twice');
  console.log('PASS results save to both accounts once and survive reload');
  await eventually(()=>pages[0].locator('#playAgainBtn').isEnabled(),'host can restart after receipt');
  await pages[0].locator('#playAgainBtn').click();
  await eventually(async()=>{const states=await Promise.all(pages.map(state));return states.every(item=>item?.synced&&item.session?.status==='voting'&&item.session.sessionId!==initial.sessionId&&item.players.length===2)},'both join rematch');
  const rematch=(await state(pages[0])).session,rematchPath=`lobbies/${gameId}/bibleSessions/${rematch.sessionId}`;
  await Promise.all(pages.map(page=>page.locator('[data-mode="traditional"]').click()));
  for(const page of pages)await eventually(async()=>{const vote=await restDoc(rematchPath+'/votes/'+(await state(page)).uid);return vote.mode==='traditional'},'rematch vote');
  await restDoc(rematchPath,{phaseStartedAt:new Date(Date.now()-31000)});
  await eventually(async()=>{const states=await Promise.all(pages.map(state));return states.every(item=>item.session?.status==='playing'&&item.session.mode==='traditional')},'rematch roster frozen');
  await pages[1].close();
  await restDoc(rematchPath,{status:'ended',durationMs:0,endReason:'completed'});
  await eventually(async()=>{const s=await restDoc(rematchPath);return s.ratingsApplied},'closed-player match finalized');
  await eventually(async()=>{const inbox=await restDoc(`bibleGameRatingInbox/${authIds[1]}/matches/${rematch.sessionId}`);return inbox.sessionId===rematch.sessionId},'offline result indexed');
  assert.equal((await restDoc(`users/${names[1]}`)).bibleGame.gamesPlayed,1,'closed player has not yet received result');
  await eventually(()=>pages[0].locator('#playAgainBtn').isEnabled(),'host can start another session');
  await pages[0].locator('#playAgainBtn').click();
  await eventually(async()=>{const item=await state(pages[0]);return item.session?.status==='voting'&&item.session.sessionId!==rematch.sessionId},'active session switches before return');
  pages[1]=await contexts[1].newPage();debugPages=pages;
  pages[1].on('pageerror',error=>browserErrors.push(error.message));
  await pages[1].goto(`${origin}/game/biblegame-multiplayer.html?gameId=${gameId}&username=${names[1]}`);
  await eventually(async()=>{const item=await state(pages[1]);return item?.synced&&item.session?.status==='voting'&&item.session.sessionId!==rematch.sessionId},'closed player returns to current session');
  assert.equal((await restDoc(`users/${names[1]}`)).bibleGame.gamesPlayed,2,'returning player recovers missed old-session result');
  assert.equal((await restDoc(`users/${names[1]}/bibleGameResults/${rematch.sessionId}`)).sessionId,rematch.sessionId);
  console.log('PASS a closed player recovers their rating after the lobby starts another match');
  // Firestore returns HTTP400 for retryable transaction conflicts. Chrome logs
  // those normal retries as resource errors even when the SDK succeeds.
  const isConflict=response=>response.body?.error?.status==='ABORTED'||response.body?.error?.status==='FAILED_PRECONDITION'&&/stored version .* does not match the required base version/.test(response.body.error.message);
  const conflicted=badResponses.some(isConflict);
  assert.deepEqual(badResponses.filter(response=>!isConflict(response)),[],'emulator must not deny or reject application requests');
  const meaningful=browserErrors.filter(error=>!error.includes('net::ERR_INTERNET_DISCONNECTED')&&!error.includes('client is offline')&&!error.includes('The operation was aborted')&&!(conflicted&&error==='Failed to load resource: the server responded with a status of 400 (Bad Request)'));
  assert.deepEqual(meaningful,[],'browser must have no application or Firestore permission errors');
})().catch(async error=>{console.error(error.stack);if(browserErrors.length)console.error('Browser errors:',browserErrors.slice(-12));if(deniedRequests.length)console.error('Denied emulator request:',deniedRequests.at(-1));if(debugSessionPath)console.error('Stored session:',JSON.stringify(await restDoc(debugSessionPath)));for(const page of debugPages)try{console.error('Browser state:',JSON.stringify(await state(page)))}catch{}process.exitCode=1}).finally(async()=>{if(browser)await browser.close();if(server)await new Promise(resolve=>server.close(resolve))});
