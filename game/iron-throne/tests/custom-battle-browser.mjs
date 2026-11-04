import assert from 'node:assert/strict';
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import {createGame} from './fixtures/legacy-game.mjs';
import {declareWar,orderArmy,parseSave} from '../core.mjs';
import {endTurn} from '../diplomacy.mjs';
import {emptyUnits} from '../economy.mjs';
import {beginEncounter,configureOfflineBattles,saveEncounterCheckpoint} from '../offline-encounters.mjs';
import {beginBattle,createBattle} from '../battle-sim.mjs';

const require=createRequire(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright/package.json`:import.meta.url);
const {chromium}=require('playwright');
const root=fileURLToPath(new URL('../../../',import.meta.url)),saveKey='catnmice.iron-throne.v1';
const types={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.json':'application/json','.css':'text/css','.svg':'image/svg+xml'};
const server=http.createServer(async(req,res)=>{
  try{
    const file=path.resolve(root,'.'+new URL(req.url,'http://localhost').pathname);
    if(!file.startsWith(root))throw Error();
    const body=await readFile(file);res.writeHead(200,{'Content-Type':types[path.extname(file)]||'application/octet-stream'});res.end(body);
  }catch{res.writeHead(404);res.end();}
});
function nearlyWonCampaign(){
  const s=parseSave(JSON.stringify(createGame(321))),[a,d]=s.armies;
  declareWar(s,a.owner,d.owner);
  for(let q=7;q<=15;q++)for(let r=7;r<=13;r++)Object.assign(s.tiles[`${q},${r}`],{terrain:'plains',owner:null,building:null,levels:{},walls:0,fortIntegrity:0,road:false,river:false});
  Object.assign(a,{tile:'10,10',units:{...emptyUnits(),menAtArms:12,ram:1},path:[],target:null,order:'hold',morale:1});
  Object.assign(d,{tile:'11,10',units:{...emptyUnits(),spearman:8},path:[],target:null,order:'hold',morale:1});
  Object.assign(s.tiles[d.tile],{owner:d.owner,building:'fort',levels:{fort:1,wall:1},walls:60,fortIntegrity:45,wallBreaches:['wall-east-front']});
  configureOfflineBattles(s);orderArmy(s,a.owner,a.id,d.tile,'attack');endTurn(s);
  assert.ok(s.offlineTurn);beginEncounter(s);
  const b=createBattle(s.offlineBattle);beginBattle(b);
  for(const u of b.units){
    if(u.side===1){u.hp=0;u.status='dead';}
    else {u.order={kind:'hold',x:u.x,z:u.z};u.explicitOrder=true;}
  }
  saveEncounterCheckpoint(s,b);return s;
}
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const url=`http://127.0.0.1:${server.address().port}/game/iron-throne/index.html`;
async function customFinishIsolation(browser){
  const context=await browser.newContext({viewport:{width:1280,height:860}});
  // Whitespace is intentional. Re-serializing the same campaign would change these bytes.
  const original='\n'+JSON.stringify(createGame(971))+' ';
  try{
    await context.addInitScript(({key,original})=>{
      localStorage.setItem(key,original);window.campaignWrites=[];
      for(const method of ['setItem','removeItem','clear']){
        const run=Storage.prototype[method];
        Storage.prototype[method]=function(...args){
          if(this===localStorage&&(method==='clear'||String(args[0])===key))window.campaignWrites.push(method);
          return run.apply(this,args);
        };
      }
    },{key:saveKey,original});
    let workers=0;
    await context.route('**/battle-finish-worker.mjs',route=>{
      if(++workers>1)return route.continue();
      // Exercise the browser's worker error path once. Retry loads the real resolver.
      return route.fulfill({contentType:'text/javascript',headers:{'Cache-Control':'no-store'},
        body:'self.onmessage=()=>{throw new Error("Test worker startup failure");};'});
    });
    const page=await context.newPage();
    page.setDefaultTimeout(30000);
    await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
    await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
    await page.goto(url);await page.locator('#custom-battle').click();
    await page.locator('[name=mode]').selectOption('field');
    await page.locator('#custom-total-0').fill('4');await page.locator('#custom-total-1').fill('1');
    await page.locator('[data-side="0"][data-unit="ram"]').fill('0');
    const field=await page.locator('#custom-field-note').textContent();
    await page.evaluate(()=>{
      const pause=document.querySelector('#battle-pause'),notes=[];
      window.customPauseNotes=notes;
      new MutationObserver(()=>{
        if(!document.querySelector('#battlefield').open||pause.hidden)return;
        const copy=[pause.querySelector('.eyebrow').textContent,pause.querySelector('#battle-pause-note').textContent,
          pause.querySelector('[data-battle-action=exit]').textContent].join(' · ');
        if(copy!==notes.at(-1))notes.push(copy);
      }).observe(pause,{attributes:true,childList:true,subtree:true});
    });
    await page.getByRole('button',{name:'Fight Now'}).click();
    await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent==='LEAD THE BATTLE');
    await page.locator('[data-battle-action=pause]').click();
    const finish=page.locator('[data-battle-action=finish]');
    await finish.click();
    await page.waitForFunction(()=>/could not|failed|retry|error/i.test(document.querySelector('#battle-pause-note').textContent));
    assert.equal(workers,1);assert.equal(await finish.isEnabled(),true);
    assert.equal(await page.locator('#battle-pause').isVisible(),true,'custom finish failure leaves the live battle available to retry');
    await finish.click();
    await page.locator('#custom-rematch').waitFor({timeout:120000});
    assert.equal(workers,2);assert.equal(await page.locator('#battlefield').isVisible(),false);
    assert.equal(await page.locator('#custom-setup').isVisible(),true);
    assert.match(await page.locator('#custom-result').textContent(),/^(Victory|Defeat|Draw) at /);
    assert.equal(await page.locator('#custom-field-note').textContent(),field);
    assert.equal(await page.locator('#custom-total-0').inputValue(),'4');
    assert.equal(await page.locator('#custom-total-1').inputValue(),'1');
    const audit=await page.evaluate(key=>({bytes:localStorage.getItem(key),writes:window.campaignWrites,notes:window.customPauseNotes}),saveKey);
    assert.deepEqual(audit.writes,[],'custom pause, finish and retry must never attempt campaign storage writes');
    assert.equal(audit.bytes,original);
    assert.ok(audit.notes.length>=3,'pause, resolution and failure status were inspected');
    for(const note of audit.notes)assert.doesNotMatch(note,/\bsav(?:e|ed|ing)\b|\bcheckpoint\b|campaign paused|your campaign waits/i);
    console.log('Custom Finish Automatically recovered from a worker failure, returned to setup after retry and never wrote the existing campaign.');
  }finally{await context.close();}
}
let browser;
try{
  browser=await chromium.launch({headless:true,executablePath:process.env.IRON_THRONE_CHROMIUM||undefined,args:['--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
  await customFinishIsolation(browser);
  if(!process.argv.includes('--finish-only')){
  const page=await browser.newPage({viewport:{width:1280,height:860}}), errors=[];
  page.on('pageerror',error=>errors.push(error.message));
  await page.route('https://pub-*.r2.dev/**',route=>route.fulfill({status:404,body:''}));
  await page.route('**/game/iron-throne/config.json',route=>route.fulfill({json:{}}));
  await page.goto(url);await page.locator('#custom-battle').click();
  const campaignSave=()=>page.evaluate(key=>localStorage.getItem(key),saveKey);
  assert.equal(await campaignSave(),null);
  assert.match(await page.locator('[data-estimate="0"]').textContent(),/72 soldiers · 1 engine. 73 start/);
  await page.locator('#custom-total-0').fill('300');await page.locator('#custom-total-1').fill('200');
  assert.match(await page.locator('[data-estimate="0"]').textContent(),/90 start on field .*211 in reserve/);
  assert.match(await page.locator('[data-estimate="1"]').textContent(),/60 start on field .*140 in reserve/);
  await page.locator('[name=mode]').selectOption('field');
  await page.locator('#custom-total-0').fill('4');await page.locator('#custom-total-1').fill('1');
  await page.getByRole('button',{name:'Fight Now'}).click();await page.locator('#battle-canvas').waitFor();
  await page.waitForFunction(()=>document.querySelector('#battle-mode').textContent==='LEAD THE BATTLE');
  await page.locator('[data-battle-action=pause]').click();
  await page.locator('[data-battle-action=finish]').click();
  await page.locator('#custom-rematch').waitFor({timeout:120000});
  assert.equal(await page.locator('#custom-total-0').inputValue(),'4');assert.equal(await campaignSave(),null);
  const field=await page.locator('#custom-field-note').textContent();
  await page.locator('#custom-rematch').click();
  await page.locator('#battle-canvas').waitFor();await page.locator('[data-battle-action=pause]').click();
  await page.locator('[data-battle-action=exit]').click();
  assert.equal(await page.locator('#custom-field-note').textContent(),field);
  await page.locator('#custom-new').click();assert.notEqual(await page.locator('#custom-field-note').textContent(),field);
  await page.locator('[name=mode]').selectOption('defend');
  await page.getByRole('button',{name:'Deploy First'}).click();await page.locator('#battle-deployment').waitFor();
  await page.locator('[data-battle-action=pause]').click();await page.locator('[data-battle-action=exit]').click();
  assert.equal(await campaignSave(),null);console.log('Custom setup, Fight Now, Deploy First, rematch, New Field and campaign isolation passed.');

  await page.locator('[data-scenario=royal]').click();
  assert.equal(await page.locator('[name=defenses]').inputValue(),'citadel');
  assert.equal(await page.locator('[name=limit]').inputValue(),'400');
  await page.getByRole('button',{name:'Fight Now'}).click();
  await page.waitForFunction(()=>document.querySelector('#battle-army-counts')?.textContent.includes('400/400'));
  await page.keyboard.press('Escape');
  await page.locator('#battle-pause-sensitivity').selectOption('3');
  assert.equal(await page.locator('#offline-battle-sensitivity').inputValue(),'3');
  await page.locator('[data-battle-action=exit]').click();
  await page.locator('[name=limit]').selectOption('100');
  await page.locator('[name=defenses]').selectOption('town');
  await page.getByRole('button',{name:'Fight Now'}).click();
  await page.waitForFunction(()=>document.querySelector('#battle-army-counts')?.textContent.includes('100/100'));
  await page.keyboard.press('Escape');await page.locator('[data-battle-action=exit]').click();
  assert.equal(await campaignSave(),null);
  console.log('Royal City Siege starts with 400 troops, town defenses start at the chosen 100 cap, and mouse sensitivity changes without writing the campaign.');

  const campaign=nearlyWonCampaign(),id=campaign.offlineBattle.id, tile=campaign.offlineBattle.tile.id;
  await page.evaluate(({key,s})=>localStorage.setItem(key,JSON.stringify(s)),{key:saveKey,s:campaign});
  await page.reload();await page.locator('#resume').click();await page.locator('[data-battle-action=enter]').click();
  // No keyboard, pointer or dismissal after entering the final simulation step.
  await page.waitForFunction(({key,turn})=>{
    const s=JSON.parse(localStorage.getItem(key));return s.turn===turn+1&&!s.offlineBattle&&!s.offlineTurn&&!document.querySelector('#end-turn').disabled;
  },{key:saveKey,turn:campaign.turn},{timeout:120000});
  const saved=JSON.parse(await campaignSave());
  assert.equal(saved.militaryEvents.filter(e=>e.battleId===id).length,1);assert.equal(saved.tiles[tile].owner,'ashen');
  assert.equal(saved.lastOfflineBattleReport.id,id);assert.equal(await page.locator('#battlefield').isVisible(),false);
  await page.reload();await page.locator('#resume').click();
  const reloaded=JSON.parse(await campaignSave());assert.deepEqual(reloaded.armies,saved.armies);assert.deepEqual(reloaded.militaryEvents,saved.militaryEvents);
  console.log('Won siege returned to the map, captured the fort, resumed the interrupted turn, and survived reload exactly once.');
  await page.locator('#menu-button').click();
  assert.equal(await page.locator('#offline-battle-sensitivity').inputValue(),'3','mouse sensitivity survives a reload');
  await page.locator('#offline-battle-size').selectOption('100');
  await page.locator('#new-campaign').click();await page.locator('#custom-battle').click();
  await page.locator('[data-scenario=royal]').click();
  assert.equal(await page.locator('[name=limit]').inputValue(),'100','the larger scenario honors the lower menu setting');
  assert.deepEqual(errors,[]);
  }
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
