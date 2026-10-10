// Run with Playwright installed: node game/nfl-radio/browser-smoke.mjs
// Optional: CHROMIUM_PATH=/path/to/chromium. Network/audio are fixture-controlled.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve,extname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
let playwright;
try{playwright=require('playwright');}catch{if(!process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES)throw new Error('Install Playwright to run browser smoke tests.');playwright=require(`${process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES}/playwright`);}
const root=fileURLToPath(new URL('../../',import.meta.url));
const server=createServer(async(req,res)=>{try{const path=resolve(root,`.${new URL(req.url,'http://local').pathname}`);if(!path.startsWith(root)){res.writeHead(403).end();return;}const content=await readFile(path);res.setHeader('Content-Type',({'.html':'text/html','.mjs':'text/javascript','.css':'text/css','.json':'application/json'})[extname(path)]||'application/octet-stream');res.end(content);}catch{res.writeHead(404).end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin=`http://127.0.0.1:${server.address().port}`;
let browser;
try{
 browser=await playwright.chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{}),args:['--no-sandbox','--disable-dev-shm-usage']});
 const page=await browser.newPage({viewport:{width:390,height:844}}),errors=[];page.on('pageerror',e=>errors.push(String(e)));
 const athlete=(id,displayName,shortName,position)=>({id,displayName,shortName,position:{abbreviation:position}});
 const det=[athlete('goff','Jared Goff','J. Goff','QB'),athlete('gibbs','Jahmyr Gibbs','J. Gibbs','RB'),athlete('brown','Amon-Ra St. Brown','A. St. Brown','WR'),athlete('bates','Jake Bates','J. Bates','K')];
 const gb=[athlete('mckinney','Xavier McKinney','X. McKinney','S')];
 const teams={8:{id:'8',abbreviation:'DET',location:'Detroit',name:'Lions',displayName:'Detroit Lions'},9:{id:'9',abbreviation:'GB',location:'Green Bay',name:'Packers',displayName:'Green Bay Packers'},2:{id:'2',abbreviation:'BUF',name:'Bills'},12:{id:'12',abbreviation:'KC',name:'Chiefs'}};
 for(const t of Object.values(teams))t.logo=`https://a.espncdn.com/test/${t.abbreviation}.png`;
 const makePlay=(id,text)=>({id:String(id),sequenceNumber:String(id),text,type:{text:'Rush'},period:{number:3},clock:{displayValue:'12:05'},start:{down:1,distance:10,yardsToEndzone:60,shortDownDistanceText:'1st & 10',team:{id:'8'}},end:{down:2,yardsToEndzone:52,team:{id:'8'}}});
 let plays=[makePlay(1,'J.Gibbs left tackle to DET 42 for 8 yards.')],failBoard=false,failSummary=false;
 const game=(id,away,home,last)=>({id,date:'2026-10-04T17:00:00Z',status:{type:{state:'in',shortDetail:'12:05 - 3rd'},period:3,displayClock:'12:05'},competitions:[{competitors:[{id:away.id,homeAway:'away',team:away,score:'10'},{id:home.id,homeAway:'home',team:home,score:'17'}],situation:{possession:home.id,downDistanceText:'2nd & 2 at DET 42',lastPlay:last}}]});
 await page.route('**/*',async route=>{
  const url=new URL(route.request().url());
  const json=body=>route.fulfill({json:body});
  if(url.pathname.endsWith('/scoreboard'))return failBoard?route.fulfill({status:503,body:'unavailable'}):json({events:[game('g1',teams[9],teams[8],plays.at(-1)),game('g2',teams[12],teams[2],makePlay(101,'Runner up the middle for 4 yards.'))]});
  if(url.pathname.endsWith('/summary'))return failSummary?route.fulfill({status:503,body:'unavailable'}):json({drives:{current:{team:teams[8],plays:url.searchParams.get('event')==='g1'?plays:[makePlay(101,'Runner up the middle for 4 yards.')]}},boxscore:{players:[]}});
  if(url.pathname.endsWith('/roster')){const id=url.pathname.split('/').at(-2);return json({team:teams[id],athletes:[{items:id==='8'?det:id==='9'?gb:[athlete(`p${id}`,'Some Runner','S. Runner','RB')]}]});}
  if(url.hostname.endsWith('radio-browser.info'))return json([{stationuuid:'fixture-stream',name:'Fixture radio',url_resolved:'https://stream.example/fixture.mp3',lastcheckok:1,ssl_error:0,codec:'MP3'}]);
  if(url.origin===origin)return route.continue();
  return route.fulfill({status:204});
 });
 await page.addInitScript(()=>{
  if(!localStorage.getItem('nfl-dial:rotation'))localStorage.setItem('nfl-dial:rotation','["g1","g2"]');
  const interval=window.setInterval.bind(window);window.setInterval=(fn,ms,...args)=>interval(fn,ms===5000?90:ms,...args);
  window.__spoken=[];window.__audios=[];
  window.Audio=function(){const audio=document.createElement('audio');window.__audios.push(audio);return audio;};
  HTMLMediaElement.prototype.play=function(){queueMicrotask(()=>this.dispatchEvent(new Event('playing')));return Promise.resolve();};
  HTMLMediaElement.prototype.pause=function(){this.dispatchEvent(new Event('pause'));};HTMLMediaElement.prototype.load=function(){};
  class Utterance{constructor(text){this.text=text;}}
  const synth=new EventTarget();Object.assign(synth,{speaking:false,pending:false,getVoices:()=>[{name:'English Basic',voiceURI:'basic',lang:'en-US',default:true},{name:'English Natural',voiceURI:'natural',lang:'en-US'}],resume(){},cancel(){this.speaking=false;},speak(u){if(!u.text.trim())return;this.speaking=true;window.__spoken.push(u);u.onstart?.();window.__finishSpeech=()=>{this.speaking=false;u.onend?.();};}});
  Object.defineProperty(window,'speechSynthesis',{value:synth,configurable:true});Object.defineProperty(window,'SpeechSynthesisUtterance',{value:Utterance,configurable:true});
 });
 await page.goto(`${origin}/game/nfl-radio.html`);
 await page.waitForFunction(()=>document.querySelector('#transcriptCount').textContent==='1');
 assert.match(await page.locator('#latestPlay').textContent(),/Jahmyr Gibbs runs left for 8 yards/);
 for(const width of [360,390,768,1280]){await page.setViewportSize({width,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,`no overflow at ${width}px`);}
 await page.setViewportSize({width:390,height:844});
 await page.locator('#start').click();await page.waitForFunction(()=>document.querySelector('#liveDot').classList.contains('playing'));
 await page.locator('[data-play-mode=every]').click();
 plays.push(makePlay(2,'J.Gibbs right guard for 5 yards.'));
 await page.waitForFunction(()=>window.__spoken.some(u=>u.text.includes('runs right for 5 yards')));
 assert.equal(await page.evaluate(()=>window.__audios.at(-1).volume),.18,'radio is ducked while speech is active');
 assert.equal(await page.locator('#latestPlay').textContent(),await page.evaluate(()=>window.__spoken.at(-1).text));
 await page.evaluate(()=>window.__finishSpeech());assert.equal(await page.evaluate(()=>window.__audios.at(-1).volume),1,'radio volume restored');
 await page.locator('[data-play-mode=touchdowns]').click();
 const before=await page.evaluate(()=>window.__spoken.length);
 plays.push(makePlay(3,'J.Gibbs right tackle for 3 yards.'));
 await page.waitForFunction(()=>document.querySelector('#transcriptCount').textContent==='3');assert.equal(await page.evaluate(()=>window.__spoken.length),before);
 plays.push({...makePlay(4,'J.Goff pass short middle to A.St. Brown for 22 yards, TOUCHDOWN.'),scoreValue:6,team:{id:'8'}});
 await page.waitForFunction(()=>window.__spoken.at(-1)?.text.includes('touchdown Detroit'));
 await page.evaluate(()=>window.__finishSpeech());
 const afterTouchdown=await page.evaluate(()=>window.__spoken.length);
 plays[3]={...plays[3],text:'J.Goff pass short middle to A.St. Brown for 23 yards, TOUCHDOWN.'};
 // Correction is reflected on the next summary refresh (force a scoreboard update to a new last-play key).
 plays.push(makePlay(5,'J.Gibbs up the middle for 2 yards.'));
 await page.waitForFunction(()=>document.querySelector('#transcriptCount').textContent==='5');assert.equal(await page.evaluate(()=>window.__spoken.length),afterTouchdown);
 await page.locator('[data-play-mode=players]').click();
 await page.locator('#playerSearch').fill('St. Brown');await page.locator('.playerOption').filter({hasText:'Amon-Ra'}).click();
 await page.locator('#playerSearch').fill('Gibbs');await page.locator('.playerOption').filter({hasText:'Jahmyr'}).click();
 assert.equal(await page.locator('.playerChip').count(),2);
 await page.locator('#playerSearch').fill('');assert.ok(await page.locator('.rosterGroup').filter({hasText:'Green Bay Packers'}).count());
 await page.locator('#voiceSelect').selectOption('basic');await page.locator('#voiceStyle').selectOption('excited');
 await page.locator('#closePlayByPlay').click();
 plays.push(makePlay(6,'J.Bates 43 yard field goal is GOOD.'));
 await page.waitForFunction(()=>document.querySelector('#transcriptCount').textContent==='6');assert.equal(await page.evaluate(()=>window.__spoken.length),afterTouchdown);
 plays.push(makePlay(7,'J.Goff pass short middle to A.St. Brown for 14 yards.'));
 await page.waitForFunction(()=>window.__spoken.at(-1)?.text.includes('for 14 yards'));
 assert.equal(await page.evaluate(()=>window.__spoken.at(-1).voice.voiceURI),'basic');await page.evaluate(()=>window.__finishSpeech());
 await page.locator('#next').click();assert.equal(await page.locator('#transcriptGame').inputValue(),'g2');
 await page.locator('#prev').click();assert.equal(await page.locator('#transcriptGame').inputValue(),'g1');
 await page.locator('#pause').click();await page.waitForFunction(()=>!document.querySelector('#liveDot').classList.contains('playing'));
 await page.locator('#pause').click();await page.waitForFunction(()=>document.querySelector('#liveDot').classList.contains('playing'));
 await page.locator('#stations').click();assert.ok(await page.locator('#stationList .sourceOption').count());await page.locator('#closeStations').click();
 await page.reload();await page.waitForFunction(()=>document.querySelector('#transcriptCount').textContent==='7');
 assert.equal(await page.locator('[data-play-mode=players]').getAttribute('aria-pressed'),'true');
 await page.locator('#playByPlayButton').click();assert.equal(await page.locator('.playerChip').count(),2);assert.equal(await page.locator('#voiceSelect').inputValue(),'basic');assert.equal(await page.locator('#voiceStyle').inputValue(),'excited');
 await page.locator('#closePlayByPlay').click();
 failSummary=true;plays.push(makePlay(8,'J.Gibbs right end for 7 yards.'));await page.waitForFunction(()=>document.querySelector('#transcriptMeta').textContent.includes('Latest play only'));
 failBoard=true;await page.waitForFunction(()=>document.querySelector('#gameLiveState').textContent==='RECONNECTING');
 failBoard=false;failSummary=false;await page.waitForFunction(()=>document.querySelector('#gameLiveState').textContent==='LIVE');
 await page.evaluate(()=>window.__finishSpeech?.());
 await page.locator('[data-play-mode=every]').click();
 await page.locator('#visualTab').click();
 assert.equal(await page.locator('#visualPanel').isVisible(),true);
 assert.equal(await page.locator('#playPanel').isVisible(),false);
 assert.equal(await page.locator('.fieldCard').count(),2);
 assert.equal(await page.locator('.fieldCard svg').count(),2);
 for(const width of [360,390,768,1280]){await page.setViewportSize({width,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,`visual layout at ${width}px`);}
 plays.push(makePlay(9,'J.Gibbs right tackle for 9 yards.'));
 await page.waitForFunction(()=>window.__spoken.at(-1)?.text.includes('for 9 yards'));
 assert.match(await page.evaluate(()=>window.__spoken.at(-1).text),/^First and 10/);
 const speakingCount=await page.evaluate(()=>window.__spoken.length);
 plays.push(makePlay(10,'J.Gibbs right tackle for 10 yards.'));
 await page.waitForFunction(()=>document.querySelector('#transcriptCount').textContent==='10');
 plays[9]={...plays[9],text:'J.Gibbs right tackle for 11 yards.'};
 plays.push({...makePlay(11,'J.Goff pass short middle to A.St. Brown for 14 yards.'),type:{text:'Pass Reception'}});
 await page.waitForFunction(()=>document.querySelector('#transcriptCount').textContent==='11');
 assert.equal(await page.evaluate(()=>window.__spoken.length),speakingCount,'new plays wait behind current speech');
 assert.match(await page.locator('#speechIndicator').textContent(),/2 queued/);
 assert.match(await page.locator('.fieldCard[data-game-id="g1"] .lastPlayRoute').getAttribute('d'),/Q/);
 await page.locator('#playTab').click();await page.locator('#visualTab').click();
 assert.equal(await page.evaluate(()=>window.__spoken.length),speakingCount,'view changes never interrupt speech');
 await page.evaluate(()=>window.__finishSpeech());await page.waitForFunction(()=>window.__spoken.at(-1)?.text.includes('for 11 yards'));
 await page.evaluate(()=>window.__finishSpeech());await page.waitForFunction(()=>window.__spoken.at(-1)?.text.includes('for 14 yards'));
 await page.evaluate(()=>window.__finishSpeech());
 await page.reload();await page.waitForSelector('.fieldCard');assert.equal(await page.locator('#visualPanel').isVisible(),true);
 assert.deepEqual(errors,[]);
 console.log('Browser smoke passed: mobile/desktop layout, radio playback/rotation/stations, transcript, filters, player search, voice persistence, duck/restore, corrections and data recovery.');
}finally{await browser?.close();await new Promise(r=>server.close(r));}
