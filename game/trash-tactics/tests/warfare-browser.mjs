// Run with `node tests/warfare-browser.mjs` after installing Playwright.
// PLAYWRIGHT_MODULE and BROWSER_PATH may point at an existing local installation.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const output=path.resolve(process.env.ARTIFACT_DIR||'browser-artifacts');await mkdir(output,{recursive:true});
const executablePath=process.env.BROWSER_PATH||(process.platform==='win32'&&existsSync('C:/Program Files/Google/Chrome/Application/chrome.exe')?'C:/Program Files/Google/Chrome/Application/chrome.exe':undefined);
let server;
const url=process.env.GAME_URL||'http://127.0.0.1:57304/';
if(!process.env.GAME_URL){
 server=spawn(process.execPath,[fileURLToPath(new URL('../server.mjs',import.meta.url))],{env:{...process.env,PORT:'57304'},windowsHide:true,stdio:['ignore','pipe','pipe']});
 await new Promise((resolve,reject)=>{server.stdout.once('data',resolve);server.once('error',reject);server.once('exit',code=>reject(new Error('Local server exited: '+code)));});
}
const browser=await chromium.launch({headless:true,executablePath});
const errors=[];
try{
 const page=await browser.newPage({viewport:{width:1440,height:900},deviceScaleFactor:1});
 page.on('pageerror',e=>errors.push(e.message));
 await page.goto(url);
 await page.waitForFunction(()=>!!window.trashTactics);
 assert.equal(await page.locator('[data-map]').count(),4);
 await page.screenshot({path:path.join(output,'expansion-lobby.png')});
 for(const map of ['canyon','conveyor']){
  if(map==='conveyor'){await page.keyboard.press('p');await page.locator('#newMatchButton').click();}
  await page.locator(`[data-map="${map}"]`).click();await page.locator('#startButton').click();
  assert.equal(await page.evaluate(()=>window.trashTactics.game.map.id),map);
  await page.evaluate(()=>{const {game:g,renderer:r}=window.trashTactics;g._think=Infinity;g.canSee=()=>true;g.isVisible=()=>true;r.drawFog=()=>{};r.camera.zoom=.36;r.centerOn(2400,1600);});
  await page.waitForTimeout(180);
  await page.screenshot({path:path.join(output,`${map}-battlefield.png`)});
 }
 // Exercise the real production UI against a funded, supply-capable fixture.
 const center=await page.evaluate(()=>{
  const {game:g,renderer:r}=window.trashTactics;g.entities=g.entities.filter(e=>e.type==='desk');g._collisionBuildings=null;
  g.teams[0].trash=10000;g.map.obstacles=[];g.map.plateaus=[];g.map.ramps=[];
  const w=g._spawn('workshop',0,1600,1000);for(let i=0;i<5;i++)g._spawn('cabinet',0,1200+i*100,600);
  r.camera.zoom=.85;r.centerOn(w.x,w.y);const p=r.worldToScreen(w.x,w.y);return p;
 });
 const rect=await page.locator('#world').boundingBox();await page.mouse.click(rect.x+center.x,rect.y+center.y);
 await page.locator('[data-action="train"][data-type="sniper"]').click();
 assert.equal(await page.evaluate(()=>window.trashTactics.game.entities.find(e=>e.type==='workshop').queue[0].type),'sniper');
 await page.screenshot({path:path.join(output,'specialist-production.png')});
 // Keep gallery artwork at the same sizes and paths as the real battlefield.
 await page.evaluate(async()=>{
  const {drawIcon}=await import('./render.js');
  const card=document.createElement('canvas');card.id='art-gallery';card.width=1440;card.height=870;
  Object.assign(card.style,{position:'fixed',inset:'0',zIndex:'99999',width:'100vw',height:'100vh',objectFit:'contain',background:'#eee8d9'});document.body.append(card);
  const c=card.getContext('2d');c.fillStyle='#f6f1e7';c.fillRect(0,0,1440,870);c.fillStyle='#373a37';c.font='bold 33px system-ui';c.fillText('TRASH TACTICS / THE WARFARE EXPANSION',55,65);c.font='17px system-ui';c.fillStyle='#647569';c.fillText('Six specialists. Two production buildings. Three families of trouble.',55,100);
  const labels=[['sniper','Scissors Sniper','TIER 2 / SCISSORS'],['airplane','Paper Airplane','TIER 2 / PAPER'],['mage','Rock Mage','TIER 2 / ROCK'],['divider','Paper Divider','TIER 3 / PAPER'],['rollingrock','Rolling Rock','TIER 3 / ROCK'],['shears','Shadow Shears','TIER 3 / SCISSORS'],['workshop','Prototype Workshop','TIER 2 PRODUCTION'],['laboratory','Shredder Laboratory','TIER 3 PRODUCTION']];
  labels.forEach(([type,label,tier],i)=>{const x=55+(i%4)*345,y=140+Math.floor(i/4)*350;c.fillStyle='#e7e0cf';c.beginPath();c.roundRect(x,y,320,325,20);c.fill();drawIcon(c,type,x+160,y+142,155,0,{phase:1.2,walk:1,active:true,sprite:false});c.textAlign='center';c.fillStyle='#6a867a';c.font='bold 12px system-ui';c.fillText(tier,x+160,y+266);c.fillStyle='#303a35';c.font='bold 20px system-ui';c.fillText(label,x+160,y+298);});
 });
 await page.locator('#art-gallery').screenshot({path:path.join(output,'warfare-artwork.png')});
 await page.locator('#art-gallery').evaluate(e=>e.remove());
 const mobile=await browser.newPage({viewport:{width:390,height:844},isMobile:true,hasTouch:true,deviceScaleFactor:1});
 mobile.on('pageerror',e=>errors.push(e.message));await mobile.goto(url);await mobile.waitForFunction(()=>!!window.trashTactics);
 assert.equal(await mobile.locator('[data-map]').count(),4);await mobile.locator('[data-map="canyon"]').tap();await mobile.locator('#startButton').tap();
 await mobile.screenshot({path:path.join(output,'mobile-canyon.png')});
 assert(await mobile.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'mobile layout fits viewport');
 assert.deepEqual(errors,[],'desktop and touch screens have no runtime errors');
 console.log(JSON.stringify({ok:true,checks:['four map choices','canyon and conveyor render','live specialist production','all eight new artwork icons','mobile map selection and layout'],output}));
}finally{await browser.close();server?.kill();}
