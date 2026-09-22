import {createRequire} from 'node:module';
import {readFile} from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES ? process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES+'/playwright/package.json' : import.meta.url);
const {chromium}=require('playwright');
const root=fileURLToPath(new URL('../../',import.meta.url));
const exposure=`
window.__cw={get:()=>({started:gameStarted,ready:graphicsReady,version:localStateVersion,selected:selectedPiece?.id,cam:{...cam},pieces:pieces.map(p=>({id:p.id,x:p.x,y:p.y,faction:p.faction,alive:p.alive})),factions:factions.length,record:matchRecord}),
selectKing:()=>{selectBattlePiece(myKing());return selectedPiece.id;},
point:()=>{const p=myKing(),g=isoProjectWorld((p.x+.5)*BASE_CELL,(p.y+.5)*BASE_CELL);return {x:g.x*cam.zoom-cam.x,y:g.y*cam.zoom-cam.y,id:p.id};},
pause:()=>{paused=true;},
sync:()=>({writes:syncDiagnostics.writes,bytes:syncDiagnostics.bytes}),
run:(code)=>eval(code)};
`;
const html=(await readFile(root+'/game/chess_warlord.html','utf8')).replace('\n})();\n</script>',exposure+'\n})();\n</script>');
const server=http.createServer(async(req,res)=>{
 try{if(req.url.startsWith('/game/chess_warlord.html')){res.setHeader('content-type','text/html');res.end(html);return;}
 const filename=path.join(root,decodeURIComponent(req.url.split('?')[0]));const data=await readFile(filename);res.setHeader('content-type',filename.endsWith('.mjs')?'text/javascript':'text/plain');res.end(data);
 }catch{res.statusCode=404;res.end('Missing test asset');}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const firestore=`export const getFirestore=()=>({});export const doc=(...a)=>({path:a.slice(1).join('/')});export const collection=doc;export const serverTimestamp=()=>0;export const getDoc=async()=>({exists:()=>false,data:()=>({})});export const getDocs=async()=>({forEach:()=>{}});export const onSnapshot=()=>()=>{};export const setDoc=async()=>{};export const updateDoc=setDoc;export const deleteDoc=setDoc;export const query=(...x)=>x;export const where=query;export const orderBy=query;export const limit=query;export const documentId=()=>'';export const getCountFromServer=async()=>({data:()=>({count:0})});export const runTransaction=async(db,fn)=>fn({get:getDoc,set:()=>{}});`;
let browser;
try{
 for(const viewport of [{width:1440,height:900},{width:390,height:844}]){
 browser=await chromium.launch({headless:true,executablePath:process.env.CHESS_TEST_CHROMIUM || undefined,args:['--no-sandbox',...(process.env.CHESS_TEST_CHROMIUM?['--single-process','--no-zygote','--disable-dev-shm-usage','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']:[])]});
 const page=await browser.newPage({viewport,hasTouch:viewport.width<500});const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.route('https://www.gstatic.com/**',route=>route.fulfill({headers:{'access-control-allow-origin':'*'},contentType:'text/javascript',body:route.request().url().includes('firebase-firestore')?firestore:route.request().url().includes('firebase-auth')?`export const getAuth=()=>({currentUser:null});export const signInAnonymously=async()=>{};export const onAuthStateChanged=()=>()=>{};`:`export const initializeApp=()=>({});`}));
 await page.goto(`http://127.0.0.1:${server.address().port}/game/chess_warlord.html?username=SmokeTester`);
 await page.waitForFunction(()=>window.__cw?.get().ready && document.querySelector('#startBtn').textContent==='Start Solo War',{timeout:30000});
 assert.equal(await page.locator('.factionCard').count(),6);
 await page.locator('#leaderboardBtn').click();assert.equal(await page.locator('#leaderboardDialog').evaluate(e=>e.open),true);await page.locator('#leaderboardClose').click();
 await page.locator('.factionCard').nth(2).focus();await page.keyboard.press('Enter');assert.equal(await page.locator('.factionCard').nth(2).getAttribute('aria-pressed'),'true');
 await page.locator('#startBtn').click();await page.waitForFunction(()=>window.__cw.get().started);await page.evaluate(()=>window.__cw.pause());
 assert.equal(await page.locator('#overlay').evaluate(e=>getComputedStyle(e).display),'none');
 const point=await page.evaluate(()=>window.__cw.point());
 await page.mouse.click(point.x,point.y);
 const after=await page.evaluate(()=>window.__cw.get());assert.equal(after.selected,point.id,'board cell hit selects the king');
 assert.match(await page.locator('#piecePanel').textContent(),/HP/);
 await page.locator('.piecePanelToggle').click();assert.equal(await page.locator('.piecePanelToggle').getAttribute('aria-expanded'),'true');
 await page.locator('.piecePanelToggle').click();
 const beforeCam=after.cam;await page.locator('#minimap').click({position:{x:10,y:10}});await page.waitForTimeout(400);
 const afterCam=await page.evaluate(()=>window.__cw.get().cam);assert.notDeepEqual(afterCam,beforeCam);
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,'no horizontal overflow');
 const recovery=await page.evaluate(()=>window.__cw.run(`(()=>{
  const base=JSON.parse(JSON.stringify(buildSelfContainedState('browser-test')));base.version=10;base.protocol=2;base.controllerEpoch=0;
  const original=base.pieces.find(p=>p.t!=='king');
  isController=false;gameStarted=false;localStateVersion=0;lastAuthoritativeData=null;recoverySnapshot=null;
  applyRemoteState(base);
  const intermediate=JSON.parse(JSON.stringify(base));intermediate.version=11;intermediate.pieces.find(p=>p.i===original.i).x=(original.x+1)%BOARD_W;
  applyRemoteState(snapshotDelta(base,intermediate));
  const final=JSON.parse(JSON.stringify(base));final.version=14;final.pieces.find(p=>p.i===original.i).hp=1;
  applyRemoteState(snapshotDelta(base,final));
  const recovered=getPiece(original.i);
  const skipped=recovered.x===original.x && recovered.hp===1 && localStateVersion===14;
  recovered.x=(original.x+2)%BOARD_W;rollbackToAuthoritative('test rollback');
  const rollback=getPiece(original.i).x===original.x && lastAuthoritativeData!==null;
  paused=true;
  return {skipped,rollback};
})()`));
 assert.deepEqual(recovery,{skipped:true,rollback:true});
 assert.deepEqual(errors,[]);console.log(JSON.stringify({viewport,passed:true,factions:after.factions,selected:after.selected}));await browser.close();
 }
}finally{await browser?.close().catch(()=>{});server.close();}
