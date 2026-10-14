const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const game=path.resolve(__dirname,'../..');
const html=fs.readFileSync(path.join(game,'turncraft.html'),'utf8');
const expose=`window.tcResults={
  start(f='frontier',mode='annihilation'){PLAYER=0;chosenFaction=f;chosenGameMode=mode;resize();createGame([{name:'Commander',factionId:f,ai:false},{name:'Opponent',factionId:f==='ascendant'?'frontier':'ascendant',ai:false}])},
  finish(winner=0){finishMatch(players[winner],'The command bases have fallen.')},
  get snapshot(){return serializeState()},remote(data,slot=0){PLAYER=slot;applyRemoteState(data)},
  get result(){return matchResult},get shown(){return presentedMatchResult},get over(){return gameOver}
};resize();renderStartMenu();overlay.style.display='flex';`;
const source=html.replace(/^import .*;$/gm,'').replace('\nboot();','\n'+expose+'\nwindow.TurncraftStartup?.complete();').replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
async function setup(width=1280,height=800){
  const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
  const page=await browser.newPage({viewport:{width,height}}),errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.route('**/*',route=>{const url=new URL(route.request().url());if(url.pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});const file=path.join(game,url.pathname.replace('/game/',''));if(file.startsWith(game+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file});return route.fulfill({status:404,body:''})});
  await page.goto('http://turncraft.test/game/turncraft.html');assert.deepEqual(errors,[]);return{browser,page,errors};
}

test('each faction gets a focused victory dialog and result actions remain usable on desktop and phones',async()=>{
  for(const [width,height] of [[1280,800],[390,844],[844,390]]){
    const {browser,page,errors}=await setup(width,height);try{
      for(const faction of ['frontier','ascendant','brood']){
        await page.evaluate(f=>{tcResults.start(f);tcResults.finish()},faction);
        const end=page.locator('#endOverlay');assert.equal(await end.isVisible(),true);assert.equal(await end.getAttribute('role'),'dialog');assert.equal(await end.getAttribute('aria-modal'),'true');assert.equal(await end.getAttribute('aria-hidden'),'false');assert.equal(await end.getAttribute('data-faction'),faction);assert.equal(await page.locator('#endTitle').textContent(),'Victory');assert.equal(await page.evaluate(()=>document.activeElement.id),'endCard');
        const rect=await page.locator('#endCard').boundingBox();assert.ok(rect.x>=0&&rect.y>=0&&rect.x+rect.width<=width+1&&rect.y+rect.height<=height+1,JSON.stringify(rect));
        await page.keyboard.press('Tab');assert.equal(await page.evaluate(()=>document.activeElement.id),'restartBtn');await page.keyboard.press('Shift+Tab');assert.equal(await page.evaluate(()=>document.activeElement.id),'endExitBtn');await page.keyboard.press('Tab');assert.equal(await page.evaluate(()=>document.activeElement.id),'restartBtn');
        await page.locator('#spectateBtn').scrollIntoViewIfNeeded();await page.locator('#spectateBtn').click();assert.equal(await end.isVisible(),false);assert.equal(await page.evaluate(()=>document.activeElement.id),'game');
      }
      assert.deepEqual(errors,[]);
    }finally{await browser.close()}
  }
});

test('a remote victory is shown from the receiving slot and repeated snapshots preserve focus/report and spectate dismissal',async()=>{
  const {browser,page,errors}=await setup();try{
    const snapshot=await page.evaluate(()=>{tcResults.start('frontier','conquest');tcResults.finish(1);return tcResults.snapshot});
    await page.reload();await page.evaluate(data=>tcResults.remote(data,1),snapshot);
    assert.equal(await page.locator('#endTitle').textContent(),'Victory');assert.equal(await page.locator('#endOverlay').getAttribute('data-faction'),'ascendant');
    const report=await page.locator('#postMatchReport').innerHTML();await page.locator('#endExitBtn').focus();
    await page.evaluate(data=>tcResults.remote(data,1),snapshot);assert.equal(await page.evaluate(()=>document.activeElement.id),'endExitBtn');assert.equal(await page.locator('#postMatchReport').innerHTML(),report);
    await page.locator('#spectateBtn').click();await page.evaluate(data=>tcResults.remote(data,1),{...snapshot,simulationTick:snapshot.simulationTick+1});assert.equal(await page.locator('#endOverlay').isVisible(),false);
    await page.evaluate(()=>document.getElementById('restartBtn').click());assert.equal(await page.locator('#overlay').isVisible(),true);assert.equal(await page.locator('#endOverlay').isVisible(),false);
    await page.evaluate(()=>{tcResults.start('brood');tcResults.finish()});assert.equal(await page.locator('#endTitle').textContent(),'Victory');assert.equal(await page.locator('#endOverlay').getAttribute('data-faction'),'brood');assert.deepEqual(errors,[]);
  }finally{await browser.close()}
});
