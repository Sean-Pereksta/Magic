const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const repo=path.resolve(__dirname,'../../..');
const expose=`window.feedbackClock=100000;nowMs=()=>feedbackClock;
window.tcFeedback={run:source=>eval(source),prepare(){
 resize();terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
 entities=[];resourceNodes=[];nextEntityId=1;players=[makePlayer(0,'Test','frontier',false),makePlayer(1,'Opponent','ascendant',false)];
 gameStarted=true;gameOver=false;paused=false;modeState=null;lobbyId='feedback-offline-fixture';PLAYER=0;isController=false;areaEffects=[];pendingImpacts=[];temporaryObstacles=[];tileVisibleToFaction=()=>true;overlay.style.display='none';
 window.feedbackUnits=[addEntity('unit','rifleman',0,8,8),addEntity('unit','rifleman',0,9,8)];window.feedbackEnemy=addEntity('unit','zealot',1,13,8);window.feedbackCommands=[];
 sendCommand=c=>{const command={...c,slot:PLAYER,id:'browser-'+feedbackCommands.length};feedbackCommands.push(command);orderFeedbackIssued(command)};
 selectEntities(feedbackUnits);lastCanvasTap={at:0,entityId:null};cam.zoom=.65;centerOn(11,9);draw();refreshUI(true);
},resolve(){const command=feedbackCommands.at(-1),result=applyCommand(command);orderFeedbackResolved(command,result);draw();refreshUI(true);return result}};`;
const source=fs.readFileSync(path.join(repo,'game/turncraft.html'),'utf8').replace(/^import .*;$/gm,'').replace('\nboot();','\n'+expose+'\nwindow.TurncraftStartup?.complete();').replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
const placeholder=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/6kQAAAAASUVORK5CYII=','base64');
async function setup(viewport,hasTouch){
 const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']}),page=await browser.newPage({viewport,hasTouch}),errors=[];
 page.on('pageerror',error=>errors.push(error.message));
 await page.route('**/*',route=>{const request=route.request(),url=new URL(request.url());if(request.resourceType()==='image')return route.fulfill({contentType:'image/png',body:placeholder});if(url.pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});const file=path.resolve(repo,'.'+decodeURIComponent(url.pathname));if(file.startsWith(repo+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file});return route.fulfill({status:404,body:''})});
 await page.goto('http://turncraft.test/game/turncraft.html');await page.evaluate(()=>tcFeedback.prepare());return {browser,page,errors};
}
async function point(page,x,y){return page.evaluate(([x,y])=>tcFeedback.run('worldToScreen('+x+','+y+')'),[x,y])}
async function tap(page,p,touch){if(touch)await page.touchscreen.tap(p.x,p.y);else await page.mouse.click(p.x,p.y)}

test('desktop and touch orders immediately show pending markers, replace redirects, and resolve stop/hold/attack feedback',async()=>{
 for(const [viewport,touch]of [[{width:1440,height:900},false],[{width:390,height:844},true]]){
  const {browser,page,errors}=await setup(viewport,touch);
  try{
   const destination=await point(page,11.5,9.5);await tap(page,destination,touch);
   assert.match(await page.locator('#tcOrderStatus').innerText(),/Move.*Pending controller/);assert.equal(await page.locator('#tcOrderStatus').getAttribute('data-state'),'pending');
   assert.equal(await page.evaluate(()=>feedbackUnits.every(e=>!e.moveDestination)),true,'the pending presentation never predicts authoritative positions');
   const redirect=await point(page,10.5,10.5);await tap(page,redirect,touch);
   assert.equal(await page.evaluate(()=>tcFeedback.run('orderFeedbackEntries.size')),1);assert.equal(await page.evaluate(()=>feedbackCommands.length),2);
   await page.evaluate(()=>tcFeedback.resolve());assert.equal(await page.locator('#tcOrderStatus').getAttribute('data-state'),'accepted');
   await page.locator('#commandButtons [data-command-label="Stop All"]').click();assert.match(await page.locator('#tcOrderStatus').innerText(),/Stop.*Pending controller/);await page.evaluate(()=>tcFeedback.resolve());
   await page.locator('#commandButtons [data-command-label="Hold All"]').click();assert.match(await page.locator('#tcOrderStatus').innerText(),/Hold.*Pending controller/);await page.evaluate(()=>tcFeedback.resolve());
   const enemy=await point(page,13.5,8.5);await tap(page,enemy,touch);assert.match(await page.locator('#tcOrderStatus').innerText(),/Attack.*Pending controller/);await page.evaluate(()=>tcFeedback.resolve());
   assert.equal(await page.locator('#tcOrderStatus').getAttribute('data-state'),'accepted');assert.equal(await page.evaluate(()=>feedbackUnits.every(e=>e.targetId===feedbackEnemy.id)),true);
   const layout=await page.locator('#tcOrderStatus').evaluate(e=>{const r=e.getBoundingClientRect();return {visible:r.width>0&&r.height>0,within:r.left>=0&&r.right<=innerWidth,pointer:getComputedStyle(e).pointerEvents}});assert.deepEqual(layout,{visible:true,within:true,pointer:'none'});assert.deepEqual(errors,[]);
  }finally{await browser.close()}
 }
});

test('real touch panning and a two-finger pinch never issue an order or marker',async()=>{
 const {browser,page,errors}=await setup({width:390,height:844},true);
 try{
  const cdp=await page.context().newCDPSession(page),p=await point(page,10.5,9.5),a={x:p.x-35,y:p.y},b={x:p.x+35,y:p.y};
  await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[p]});await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:p.x+25,y:p.y+15}]});await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
  await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[a]});await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[a,b]});await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:a.x-20,y:a.y},{x:b.x+20,y:b.y}]});await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[a]});await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
  const result=await page.evaluate(()=>tcFeedback.run('({commands:feedbackCommands.length,markers:orderFeedbackEntries.size,zoom:cam.zoom})'));assert.equal(result.commands,0);assert.equal(result.markers,0);assert.ok(result.zoom>.65);assert.deepEqual(errors,[]);
 }finally{await browser.close()}
});
