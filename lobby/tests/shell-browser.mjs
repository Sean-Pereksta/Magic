// Shared root shell with the real lobby and deterministic service fixtures.
// No production service is contacted. Optional screenshots use SHELL_QA_OUTPUT.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import {fileURLToPath,pathToFileURL} from 'node:url';
const moduleName=process.env.PLAYWRIGHT_MODULE||'playwright';
const {chromium}=await import(path.isAbsolute(moduleName)?pathToFileURL(moduleName).href:moduleName);
const root=path.resolve(fileURLToPath(new URL('../..',import.meta.url)));
const expectedGameCount=Object.keys(JSON.parse(fs.readFileSync(path.join(root,'lobby/assets/game-artwork.json'),'utf8')).games).length;
const fixtureSource=fs.readFileSync(path.join(root,'lobby/tests/arcade-nexus-browser.mjs'),'utf8');
const firebase=fixtureSource.match(/const firebase=String.raw`([\s\S]*?)`;/)[1];
const output=process.env.SHELL_QA_OUTPUT&&path.resolve(process.env.SHELL_QA_OUTPUT);
if(output)fs.mkdirSync(output,{recursive:true});
const mime={'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp','.png':'image/png'};
const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://fixture');
  const filename=path.resolve(root,'.'+(url.pathname==='/'?'/index.html':decodeURIComponent(url.pathname)));
  if(!filename.startsWith(root+path.sep)||!fs.existsSync(filename)||!fs.statSync(filename).isFile()){res.writeHead(404);return res.end('Not found');}
  res.setHeader('Content-Type',mime[path.extname(filename)]||'application/octet-stream');res.end(fs.readFileSync(filename));
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_PATH?{executablePath:process.env.CHROMIUM_PATH}:{})});
const checks=[],contexts=[],errors=[],layouts=[];
const check=(name,condition=true)=>{assert.ok(condition,name);checks.push(name);};
async function open(width,{blockRootSdk=false,user=''}={}){
  const context=await browser.newContext({viewport:{width,height:width<600?900:1000}});contexts.push(context);
  context.setDefaultTimeout(12000);
  await context.addInitScript(({user})=>{
    globalThis.__fixture={data:{'users/Tester':{username:'Tester',password:'fixture-password',displayName:'Tester',chatAuthUid:'fixture-auth-user'},'chat/message':{username:'Friend',text:'See you in the arcade!',timestamp:Date.now(),type:'message'}},listeners:[],sequence:0,calls:[]};
    localStorage.setItem('gh.lastTab','discover');localStorage.setItem('gh.profilePromptSeen','1');
    if(user)localStorage.setItem('gh.username',user);
  },{user});
  await context.route('**/*',route=>{
    const request=route.request(),url=new URL(request.url());
    if(url.hostname==='www.gstatic.com'&&url.pathname.includes('/firebasejs/')){
      if(blockRootSdk&&request.frame()===request.frame().page().mainFrame())return route.abort('failed');
      return route.fulfill({contentType:'text/javascript',body:firebase});
    }
    if(url.origin!==origin)return route.abort('blockedbyclient');
    if(url.pathname.startsWith('/game/')&&url.pathname.endsWith('.html'))return route.fulfill({contentType:'text/html',body:'<!doctype html><title>Game session fixture</title><main id="launched">Original game route reached</main>'});
    return route.continue();
  });
  const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  await page.goto(origin+'/index.html');
  await page.frameLocator('#contentFrame').locator('#shellChatRelay').waitFor();
  await page.waitForFunction(()=>document.body.dataset.shellMode==='hub');
  await page.frameLocator('#contentFrame').locator('#cnmFlagshipGrid .game-card').nth(3).waitFor({state:'attached'});
  await page.waitForTimeout(400);
  return page;
}
async function inspectViewport(page,width){
  const hub=page.frameLocator('#contentFrame');
  check(`${width}px root has no horizontal overflow`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  check(`${width}px hub has no horizontal overflow`,await hub.locator('html').evaluate(node=>node.scrollWidth<=innerWidth));
  const header=await hub.locator('.topbar .header-actions').evaluate(node=>{const box=node.getBoundingClientRect();return{left:box.left,right:box.right,width:innerWidth};});
  check(`${width}px integrated Chat fits in the header`,header.left>=0&&header.right<=header.width);
  check(`${width}px root removes duplicate header`,!await page.locator('.appbar').isVisible());
  const broken=await hub.locator('#view-discover img').evaluateAll(nodes=>nodes.filter(node=>{const box=node.getBoundingClientRect();return box.width>0&&box.height>0&&box.top<innerHeight&&box.bottom>0&&(!node.complete||!node.naturalWidth);}).map(node=>node.src));
  check(`${width}px visible Discover artwork loads`,broken.length===0);
  if(width<600){
    const nav=await hub.locator('.mobile-nav').evaluate(node=>({height:node.getBoundingClientRect().height,labels:[...node.querySelectorAll('button span')].map(label=>parseFloat(getComputedStyle(label).fontSize))}));
    check(`${width}px bottom navigation retains compact readable labels`,nav.height<=90&&nav.labels.every(size=>size<=13));
  }
}
async function capture(page,name){if(output){await page.waitForTimeout(250);await page.screenshot({path:path.join(output,name+'.png')});}}
try{
  for(const width of [1440,360]){
    const page=await open(width),hub=page.frameLocator('#contentFrame');
    await inspectViewport(page,width);await capture(page,`cat-n-mice-discover-${width}`);
    const relay=hub.locator('#shellChatRelay');
    await relay.click();await page.locator('#chatContainer.open').waitFor();
    await page.waitForTimeout(150);check(`${width}px guest chat opens with a keyboard destination`,await page.locator('#closeChat').evaluate(node=>document.activeElement===node));
    check(`${width}px guest composer remains account gated`,await page.locator('#chatMessage').isDisabled());
    await page.keyboard.press('Escape');await page.locator('#chatContainer.open').waitFor({state:'hidden'});
    check(`${width}px Escape restores focus to hub Chat`,await relay.evaluate(node=>node.ownerDocument.activeElement===node));
    const nav=await hub.locator('.main-nav').isVisible()?'.main-nav':'.mobile-nav';
    await hub.locator(`${nav} [data-view="singleplayer"]`).click();
    await hub.locator('#singleGameGrid .game-card').nth(expectedGameCount-1).waitFor({state:'attached'});
    await page.waitForTimeout(300);
    check(`${width}px complete library remains available`,await hub.locator('#singleGameGrid .game-card').count()===expectedGameCount);
    const toolbar=await hub.locator('.library-toolbar').evaluate(node=>{const style=getComputedStyle(node);return{columns:style.gridTemplateColumns,rows:style.gridTemplateRows,children:[...node.children].map(child=>{const css=getComputedStyle(child),box=child.getBoundingClientRect();return{id:child.id,column:css.gridColumn,row:css.gridRow,width:box.width,left:box.left};})};});
    layouts.push({width,toolbar});
    if(width===360){
      const search=toolbar.children.find(item=>item.id==='gameSearch'),status=toolbar.children.find(item=>item.id==='gameStatus'),sort=toolbar.children.find(item=>item.id==='gameSort');
      check('360px library search and status span the full filter width',search.width>sort.width*1.8&&Math.abs(status.width-search.width)<1);
    }
    await capture(page,`cat-n-mice-library-${width}`);
    await hub.locator('#gameSearch').fill('Apes Together Strong');
    await hub.locator('#singleGameGrid [data-game-key="apestogetherstrong"] [data-game-action="single"]').click();
    await hub.locator('#launched').waitFor();await page.waitForFunction(()=>document.body.dataset.shellMode==='game');
    check(`${width}px game restores labeled toolbar`,await page.locator('#returnToLobby').isVisible()&&await page.locator('#fullscreenLobby').isVisible()&&await page.locator('#reloadLobby').isVisible());
    check(`${width}px game toolbar has no horizontal overflow`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
    await page.locator('#reloadLobby').click();await hub.locator('#launched').waitFor();
    check(`${width}px Reload preserves actual game URL`,await page.locator('#contentFrame').evaluate(node=>node.contentWindow.location.pathname==='/game/apes-together-strong.html'));
    if(await page.evaluate(()=>document.fullscreenEnabled)){
      await page.locator('#fullscreenLobby').click();await page.waitForFunction(()=>document.fullscreenElement?.id==='contentFrame');
      check(`${width}px fullscreen targets the actual game frame`);await page.evaluate(()=>document.exitFullscreen());
    }
    await page.locator('#returnToLobby').click();await hub.locator('#shellChatRelay').waitFor();await page.waitForFunction(()=>document.body.dataset.shellMode==='hub');
    check(`${width}px returning reinstalls one Chat relay`,await hub.locator('#shellChatRelay').count()===1);
    await page.context().close();
  }
  const offline=await open(360,{blockRootSdk:true,user:'Tester'}),hub=offline.frameLocator('#contentFrame');
  await hub.locator('#shellChatRelay').click();await offline.locator('#chatContainer.open').waitFor();
  await offline.waitForTimeout(150);
  check('blocked SDK still opens chat and explains service unavailability',/temporarily unavailable/i.test(await offline.locator('#chatMessages').innerText()));
  await offline.locator('#chatMessage').fill('Never sent to production');await offline.locator('#sendBtn').click();
  await offline.locator('#toast.show').waitFor();
  check('blocked SDK rejects a send visibly',/temporarily unavailable/i.test(await offline.locator('#toast').innerText()));
  await offline.locator('#closeChat').click();
  await hub.locator('#cnmFlagshipGrid [data-game-key="apestogetherstrong"] [data-game-action="single"]').click();await hub.locator('#launched').waitFor();
  await offline.locator('#reloadLobby').click();await hub.locator('#launched').waitFor();
  await offline.locator('#returnToLobby').click();await hub.locator('#shellChatRelay').waitFor();
  check('blocked SDK preserves launch, reload, and return controls');await offline.context().close();
  check('no uncaught browser errors',errors.length===0);
  console.log(JSON.stringify({passed:checks.length,checks},null,2));
}catch(error){
  console.error(error);const page=contexts.at(-1)?.pages()[0];
  if(page&&output)await capture(page,'cat-n-mice-shell-failure').catch(()=>{});
  process.exitCode=1;
}finally{
  if(output)fs.writeFileSync(path.join(output,'cat-n-mice-shell-qa.json'),JSON.stringify({checks,pageErrors:errors,layouts},null,2));
  await Promise.allSettled(contexts.map(context=>context.close()));await browser.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));
}
