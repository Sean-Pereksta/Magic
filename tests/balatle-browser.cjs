/* Real DOM checks; test-only hooks never ship in the game pages. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const { chromium } = require('playwright');
const root = path.resolve(__dirname, '..');
const mime = { '.html':'text/html', '.js':'text/javascript', '.mjs':'text/javascript', '.css':'text/css' };
const server = http.createServer((req,res) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if(pathname==='/hub/play-signal.mjs'){res.writeHead(200,{'content-type':'text/javascript'});res.end('');return;}
  const file=path.join(root,decodeURIComponent(pathname));
  if(!file.startsWith(root+path.sep) || !fs.existsSync(file)){res.writeHead(404);res.end();return;}
  let data=fs.readFileSync(file);
  if(pathname==='/game/balatlesolo.html'){
    data=data.toString().replace('\n  boot();', '\n  window.__balatleTest={game,options,handUp,startNewRun,applyRoundConfig,calculateScore,renderAll,useConsumable,FULL_CONSUME_POOL,FULL_JOKER_POOL,mkJoker};\n  boot();');
  }
  res.writeHead(200,{'content-type':mime[path.extname(file)]||'application/octet-stream'});res.end(data);
});
function check(condition, message){assert.ok(condition,message);console.log('PASS',message);}
async function fit(page,label){
  const bounds=await page.evaluate(()=>{
    const hand=document.getElementById('hand').getBoundingClientRect();
    const actions=document.querySelector('.table-actions').getBoundingClientRect();
    return {width:innerWidth,height:innerHeight,scrollWidth:document.documentElement.scrollWidth,hand:{x:hand.x,right:hand.right,bottom:hand.bottom},actionsBottom:actions.bottom,cards:[...document.querySelectorAll('#hand .card')].map(el=>{const b=el.getBoundingClientRect();return {left:b.left,right:b.right,width:b.width};})};
  });
  check(bounds.scrollWidth<=bounds.width+1,`${label}: no horizontal page overflow`);
  check(bounds.cards.every(c=>c.left>=-1&&c.right<=bounds.width+1),`${label}: every card stays in the viewport`);
  if(bounds.actionsBottom>bounds.height+1){console.log('LAYOUT',label,JSON.stringify(bounds));if(process.env.BALATLE_SCREENSHOTS) await page.screenshot({path:path.join(process.env.BALATLE_SCREENSHOTS,`failure-${bounds.width}.png`),fullPage:true});}
  check(bounds.actionsBottom<=bounds.height+1,`${label}: primary actions visible without scrolling`);
  return bounds;
}
(async()=>{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const base=`http://127.0.0.1:${server.address().port}`;
  const browser=await chromium.launch({headless:true,executablePath:process.env.CHROME_BIN||undefined,args:['--no-sandbox']});
  const errors=[];
  try{
    const context=await browser.newContext({viewport:{width:1366,height:768},reducedMotion:'reduce'});
    await context.route('**/*',route=>new URL(route.request().url()).hostname==='127.0.0.1' ? route.continue() : route.abort());
    const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
    await page.goto(base+'/game/balatlesolo.html');
    await page.waitForSelector('#hand .card');
    check(await page.locator('#hand .card').count()===8,'Solo deals eight cards');
    check(await page.locator('#hand .card svg').count()>=8,'Playing cards have local SVG artwork');
    await fit(page,'Desktop 1366×768');
    await page.evaluate(()=>{
      const {game,renderAll}=window.__balatleTest;
      const c=(rank,suit,id)=>({id,rank:String(rank),rv:({A:14,K:13,Q:12,J:11})[rank]||Number(rank),suit,suitSym:{S:'♠',H:'♥',D:'♦',C:'♣'}[suit],red:suit==='H'||suit==='D',baseChip:rank==='A'?11:['K','Q','J'].includes(rank)?10:Number(rank),bonusChip:0});
      game.hand=[c(7,'H','pair1'),c(7,'S','pair2'),c('A','D','kicker'),c(2,'C','x1'),c(4,'C','x2'),c(6,'S','x3'),c(9,'D','x4'),c('Q','H','x5')];
      game.sel.clear();renderAll();
    });
    await page.locator('[data-card-id="pair1"]').click();await page.locator('[data-card-id="pair2"]').click();await page.locator('[data-card-id="kicker"]').click();
    check(await page.locator('#selectionName').textContent()==='Pair','Selection previews Pair');
    check(await page.locator('#scoreChips').textContent()==='24','Unrelated Ace contributes no card chips');
    check(await page.locator('#hand .scoring').count()===2,'Only matching pair is highlighted for scoring');
    await page.locator('#btnPlay').click();
    await page.waitForFunction(()=>window.__balatleTest.game.handsLeft===3);
    check(await page.locator('#blindScore').textContent()==='48','Played score matches the preview');
    await page.evaluate(()=>{
      const t=window.__balatleTest;t.startNewRun();
      t.game.hand=['A','K','Q','J','9','4','3','2'].map((rank,i)=>({id:'win'+i,rank,rv:({A:14,K:13,Q:12,J:11})[rank]||Number(rank),suit:'H',suitSym:'♥',red:true,baseChip:rank==='A'?11:['K','Q','J'].includes(rank)?10:Number(rank),bonusChip:0}));
      t.game.sel=new Set(t.game.hand.slice(0,5).map(c=>c.id));t.renderAll();
    });
    await page.locator('#btnPlay').click();await page.waitForSelector('#shopBack.show');
    check(await page.locator('.shop-card').count()===6,'Winning a blind opens six illustrated shop offers');
    check((await page.locator('#cashoutLine').textContent()).includes('unused hands $3'),'Cashout shows unused-hand reward');
    await page.locator('#btnCloseShop').click();await page.locator('#btnOpenShop').click();
    check(await page.locator('#shopBack').evaluate(el=>el.classList.contains('show')),'Shop can be reopened after viewing the table');
    await page.locator('#shopList [data-buy]:not(:disabled)').first().click();
    await page.locator('#btnNextRound').click();
    check(await page.locator('#blindName').textContent()==='Big','Shop advances to the Big blind');
    await page.evaluate(()=>{
      const t=window.__balatleTest;t.game.cons=[{...t.FULL_CONSUME_POOL.find(c=>c.name==='Tarot: Chip Paint')},{...t.FULL_CONSUME_POOL.find(c=>c.name==='Tarot: Cash Burst')}];t.renderAll();
    });
    await page.locator('#consRack button').first().click();await page.locator('#hand .card').first().click();
    check(await page.locator('#consRack button').count()===1,'Targeted consumable disappears on completion');
    check(await page.locator('#consRack button').isEnabled(),'Remaining consumables re-enable immediately');
    await page.locator('#btnHow').click();check(await page.locator('#handGuide .hand-guide-row').count()===12,'All poker hand combinations are available in help');await page.keyboard.press('Escape');
    await page.evaluate(()=>{const t=window.__balatleTest;t.game.cons=[{...t.FULL_CONSUME_POOL.find(c=>c.name==='Planet: Train Hand')}];t.renderAll();});
    await page.locator('#consRack button').click();await page.locator('#btnCancelPick').click();
    check(await page.locator('#consRack button').count()===1,'Cancelling a Planet picker retains the consumable');
    await page.locator('#btnDeck').click();check(await page.locator('.deck-rank').count()===52,'Deck inspector shows every rank and suit');await page.keyboard.press('Escape');
    if(process.env.BALATLE_SCREENSHOTS){fs.mkdirSync(process.env.BALATLE_SCREENSHOTS,{recursive:true});await page.screenshot({path:path.join(process.env.BALATLE_SCREENSHOTS,'solo-desktop.png')});}
    for(const viewport of [{width:390,height:844},{width:360,height:740},{width:844,height:390},{width:1920,height:1080}]){
      await page.setViewportSize(viewport);await page.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));await page.evaluate(()=>window.__balatleTest.startNewRun());
      await fit(page,`${viewport.width}×${viewport.height}`);
      await page.keyboard.press('1');
      await page.keyboard.press('Enter');
      await page.waitForFunction(()=>window.__balatleTest.game.handsLeft===3);
      await fit(page,`${viewport.width}×${viewport.height} after scoring`);
      if(process.env.BALATLE_SCREENSHOTS) await page.screenshot({path:path.join(process.env.BALATLE_SCREENSHOTS,`solo-${viewport.width}.png`)});
    }
    check(errors.length===0,`No browser exceptions (${errors.join('; ')})`);
    await context.close();
  }finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
})().catch(error=>{console.error(error);process.exitCode=1;server.close();});
