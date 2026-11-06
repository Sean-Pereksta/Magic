import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const require=createRequire(import.meta.url),{chromium}=require(process.env.PLAYWRIGHT_MODULE||'playwright');
const output=path.resolve(process.env.ARTIFACT_DIR||'browser-artifacts');await mkdir(output,{recursive:true});
let server;const url=process.env.GAME_URL||'http://127.0.0.1:57314/';
if(!process.env.GAME_URL){server=spawn(process.execPath,[fileURLToPath(new URL('../server.mjs',import.meta.url))],{env:{...process.env,PORT:'57314'},windowsHide:true,stdio:['ignore','pipe','pipe']});await new Promise((resolve,reject)=>{server.stdout.once('data',resolve);server.once('error',reject);server.once('exit',code=>reject(new Error('Local server exited: '+code)));});}
const executablePath=process.env.BROWSER_PATH||(process.platform==='win32'&&existsSync('C:/Program Files/Google/Chrome/Application/chrome.exe')?'C:/Program Files/Google/Chrome/Application/chrome.exe':undefined);
const browser=await chromium.launch({headless:true,executablePath});const errors=[];
async function worldPoint(page,x,y){return page.evaluate(({x,y})=>{const r=window.trashTactics.renderer,p=r.worldToScreen(x,y),rect=document.getElementById('world').getBoundingClientRect();return{x:p.x+rect.left,y:p.y+rect.top};},{x,y});}
async function clickWorld(page,x,y,touch=false){const p=await worldPoint(page,x,y);if(touch)await page.touchscreen.tap(p.x,p.y);else await page.mouse.click(p.x,p.y);}
async function begin(page){page.on('pageerror',e=>errors.push(e.message));await page.goto(url);await page.waitForFunction(()=>!!window.trashTactics);await page.locator('#startButton').click();await page.evaluate(()=>{const{game:g,renderer:r}=window.trashTactics;g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');g._collisionBuildings=null;g.teams[0].trash=1000;const w=g._spawn('twig',0,1008,900);g._spawn('airplane',0,2400,1800);r.camera.zoom=innerWidth<761?.72:1;r.centerOn(1104,1008);g._updateVision();window.fixtureWorker=w.id;});}
try{
 const page=await browser.newPage({viewport:{width:1440,height:900}});await begin(page);
 const vision=await page.evaluate(()=>{const g=window.trashTactics.game;return{near:g.canSee(0,3200,1800),far:g.canSee(0,3400,1800)};});
 assert.deepEqual(vision,{near:true,far:false},'airplane reveals a large but bounded scouting area');
 await clickWorld(page,1008,900);await page.locator('[data-action="build"][data-type="wall"]').click();
 await clickWorld(page,1001,1004);await clickWorld(page,1052,1003);await clickWorld(page,1102,1001);
 const placement=await page.evaluate(()=>{const g=window.trashTactics.game,w=g.getEntity(window.fixtureWorker);return{walls:g.entities.filter(e=>e.type==='wall').map(e=>[e.x,e.y]),trash:g.teams[0].trash,pending:w.buildQueue.length};});
 assert.deepEqual(placement.walls,[[1008,1008],[1056,1008],[1104,1008]]);assert.equal(placement.trash,940);assert.equal(placement.pending,2);
 await page.locator('[data-action="cancelTarget"]').click();
 await page.evaluate(()=>{const g=window.trashTactics.game;for(let i=0;i<240;i++)g.update(.05);});
 assert(await page.evaluate(()=>window.trashTactics.game.entities.filter(e=>e.type==='wall').every(e=>e.constructing===0&&e.hp===1200)));
 await clickWorld(page,1056,1008);await page.locator('[data-action="dismantleWall"]').click();
 assert.equal(await page.evaluate(()=>window.trashTactics.game.entities.filter(e=>e.type==='wall'&&e.hp>0).length),2);
 await page.evaluate(()=>{
  const {game:g,renderer:r}=window.trashTactics;
  for(let x=960;x<=1392;x+=48)for(let y=1008;y<=1296;y+=48){if(x!==960&&x!==1392&&y!==1008&&y!==1296)continue;if(y===1008&&x===1056||x===1392&&(y===1104||y===1152))continue;if(!g.entities.some(e=>e.type==='wall'&&e.x===x&&e.y===y))g._spawn('wall',0,x,y);}
  const worker=g.getEntity(window.fixtureWorker);worker.x=1100;worker.y=1190;worker.order={type:'idle'};
  g._spawn('wall',0,1440,1248,{hp:720});g._spawn('wall',0,1488,1248,{hp:240,constructing:3,buildTotal:3});
  const mouse=g._spawn('mouse',0,1200,1152);window.fixtureMouse=mouse.id;
  g._spawn('rollingrock',1,1510,1152,{cooldown:100});g._spawn('scissors',1,1540,1080,{cooldown:100});
  r.camera.zoom=1.25;r.centerOn(1190,1140);g._updateVision();
 });
 await clickWorld(page,1200,1152);await page.evaluate(()=>{const g=window.trashTactics.game;for(let i=0;i<40;i++)g.update(.05);});await page.waitForTimeout(100);
 assert.match(await page.locator('#actionContent').innerText(),/850 HP.*360 range/);
 const combat=await page.evaluate(()=>{const g=window.trashTactics.game,e=g.entities.find(e=>e.type==='rollingrock'&&e.team===1),m=g.getEntity(window.fixtureMouse);return{hp:e?.hp,mouse:m?.hp,cooldown:m?.cooldown,visible:e&&g.isVisible(e,0),shots:g.projectiles.length,time:g.time};});assert(combat.hp<360,'reinforced mouse damages an attacker through its own short walls: '+JSON.stringify(combat));
 await page.screenshot({path:path.join(output,'wall-fortifications-gameplay.png')});
 const mobile=await browser.newPage({viewport:{width:390,height:844},isMobile:true,hasTouch:true});await begin(mobile);
 await clickWorld(mobile,1008,900,true);await mobile.locator('[data-action="build"][data-type="wall"]').tap();
 await clickWorld(mobile,1000,1000,true);await clickWorld(mobile,1050,1000,true);
 assert.equal(await mobile.evaluate(()=>window.trashTactics.game.entities.filter(e=>e.type==='wall').length),2);
 assert(await mobile.locator('[data-action="cancelTarget"]').isVisible());
 await mobile.screenshot({path:path.join(output,'wall-placement-mobile.png')});
 assert(await mobile.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
 await mobile.locator('[data-action="cancelTarget"]').tap();assert.deepEqual(errors,[]);
 console.log(JSON.stringify({ok:true,checks:['900px actual airplane vision','desktop repeated snapped wall construction','paid Twig construction queue','owned block dismantling','reinforced mouse combat and stats','mobile repeated wall placement and layout'],output}));
}finally{await browser.close();server?.kill();}
