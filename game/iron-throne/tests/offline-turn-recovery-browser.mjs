import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {createGame} from './fixtures/legacy-game.mjs';
import {declareWar,orderArmy,parseSave} from '../core.mjs';
import {endTurn} from '../diplomacy.mjs';
import {emptyUnits} from '../economy.mjs';
import {configureOfflineBattles,resolveEncounterAutomatically,returnFromEncounter} from '../offline-encounters.mjs';
const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright'),root=fileURLToPath(new URL('../../../',import.meta.url)),key='catnmice.iron-throne.v1';
function cursorFixture(){
  const s=parseSave(JSON.stringify(createGame(321))),a=s.armies[0],d=s.armies[1];declareWar(s,a.owner,d.owner);
  for(let q=7;q<=15;q++)for(let r=7;r<=13;r++)Object.assign(s.tiles[`${q},${r}`],{terrain:'plains',owner:null,building:null,levels:{},walls:0,fortIntegrity:0,road:false,river:false});
  Object.assign(a,{tile:'10,10',units:{...emptyUnits(),levy:140,archer:12},path:[],target:null,order:'hold',morale:1});
  Object.assign(d,{tile:'11,10',units:{...emptyUnits(),levy:40},path:[],target:null,order:'hold',morale:1});s.tiles[d.tile].owner=d.owner;
  configureOfflineBattles(s);orderArmy(s,a.owner,a.id,d.tile,'attack');endTurn(s);assert.ok(s.offlineBattle);resolveEncounterAutomatically(s);returnFromEncounter(s);assert.ok(s.offlineTurn);return s;
}
const cursor=cursorFixture(),expected=structuredClone(cursor);endTurn(expected);assert.equal(expected.turn,cursor.turn+1);
const server=http.createServer(async(req,res)=>{try{const url=new URL(req.url,'http://localhost'),file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root))throw Error();res.writeHead(200,{'Content-Type':{'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.json':'application/json','.css':'text/css','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});res.end(await readFile(file));}catch{res.writeHead(404);res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));let browser;
try{
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--no-sandbox']});
  const context=await browser.newContext(),page=await context.newPage(),errors=[];page.setDefaultTimeout(30000);page.on('pageerror',e=>errors.push(e.message));page.on('dialog',dialog=>dialog.accept());
  await context.addInitScript(({s,key})=>{if(!localStorage.getItem(key))localStorage.setItem(key,JSON.stringify(s));},{s:parseSave(JSON.stringify(createGame(321))),key});
  await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
  let workers=0;await page.route('**/turn-worker.mjs',route=>{workers++;return route.fulfill({contentType:'text/javascript',body:"self.onmessage=()=>self.postMessage({type:'error',message:'Injected worker failure'});"});});
  await page.goto(`http://127.0.0.1:${server.address().port}/game/iron-throne/index.html`);await page.locator('#resume').click();await page.locator('[data-tab=realm]').click();await page.locator('#menu-button').click();
  await page.locator('#import-save').setInputFiles({name:'cursor.json',mimeType:'application/json',buffer:Buffer.from(JSON.stringify(cursor))});
  await page.waitForFunction(()=>document.querySelector('#end-turn').textContent==='Resume turn'&&!document.querySelector('#end-turn').disabled);assert.equal(workers,1,'cursor import resumes automatically');
  const before=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key);
  await page.locator('#tax').evaluate(select=>{select.value='high';select.dispatchEvent(new Event('change',{bubbles:true}));});
  assert.deepEqual(await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key),before,'planning is locked after continuation failure');
  await page.unroute('**/turn-worker.mjs');
  await page.evaluate(()=>{const original=Storage.prototype.setItem;window.rejectNextSave=true;Storage.prototype.setItem=function(key,value){if(window.rejectNextSave){window.rejectNextSave=false;throw new Error('Injected save failure');}return original.call(this,key,value);};});
  await page.locator('#end-turn').click();await page.waitForFunction(()=>document.querySelector('#toast').textContent.includes('Saving the new campaign position failed'));
  assert.equal(await page.locator('#end-turn').textContent(),'Resume turn');assert.deepEqual(await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key),before,'failed resumed-turn save keeps prior cursor snapshot');
  await page.reload();await page.locator('#resume').click();
  await page.waitForFunction(turn=>document.querySelector('#turn').textContent===`Turn ${turn}`&&!document.querySelector('#end-turn').disabled,expected.turn);
  const after=await page.evaluate(key=>JSON.parse(localStorage.getItem(key)),key);assert.equal(after.offlineTurn,undefined);assert.equal(after.turn,expected.turn);assert.deepEqual(after.kingdoms.map(k=>k.resources),expected.kingdoms.map(k=>k.resources));assert.deepEqual(after.armies,expected.armies);assert.deepEqual(errors,[]);
  console.log('PASS: cursor-only import auto-resumes; worker/save failures lock planning; Resume turn stays callable; reload resumes exactly once with matching armies/resources.');
}finally{await browser?.close();await new Promise(r=>server.close(r));}
