const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.BRAMBLEBOUND_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../../..');
const shots=process.env.BRAMBLEBOUND_SCREENSHOTS;
const mime={'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp','.json':'application/json'};
const server=http.createServer((req,res)=>{let file;try{file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));if(!file.startsWith(root+path.sep)||file.includes(path.sep+'.git'))throw Error();if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',mime[path.extname(file)]||'application/octet-stream');fs.createReadStream(file).on('error',()=>res.end()).pipe(res);}catch{res.writeHead(404).end();}});
async function main(){
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+server.address().port;
  const options={headless:true};
  const edge='C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';
  if(process.platform==='win32'&&fs.existsSync(edge))options.executablePath=edge;
  const browser=await chromium.launch(options);
  const results=[];
  try{
    for(const viewport of [{width:1440,height:1050},{width:390,height:844}]){
      const context=await browser.newContext({viewport,acceptDownloads:true}),page=await context.newPage(),errors=[],missing=[];
      page.on('pageerror',e=>errors.push(e.message));page.on('response',r=>{if(r.status()>=400)missing.push(r.status()+' '+r.url());});
      await page.goto(origin+'/game/bramblebound/');await page.locator('#title-play').waitFor();
      await page.locator('.title-poster img').evaluate(img=>img.decode());
      assert.equal(await page.locator('#main').evaluate(el=>el.inert),true);
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
      if(shots){fs.mkdirSync(shots,{recursive:true});await page.screenshot({path:path.join(shots,'title-'+viewport.width+'.png'),fullPage:true});}
      await page.locator('#title-help').click();await page.locator('#modal[open]').waitFor();assert.match(await page.locator('#dialog-body').innerText(),/Walking with Pip/);await page.locator('#close-btn').click();
      await page.locator('#title-kit').click();await page.locator('#title-screen').waitFor({state:'hidden'});assert.match(await page.locator('#run-btn').innerText(),/Begin expedition/);
      await page.locator('#run-btn').click();await page.waitForTimeout(4200);await page.locator('#run-btn').click();
      const saved=await page.evaluate(()=>JSON.parse(localStorage.getItem('bramblebound.profile.v1')));assert.ok(saved.profile.run.time>0);
      await page.locator('.brand[data-title]').click();assert.match(await page.locator('#title-play').innerText(),/Continue expedition/);
      const before=await page.evaluate(()=>JSON.parse(localStorage.getItem('bramblebound.profile.v1')).profile.run.time);await page.waitForTimeout(400);
      assert.equal(await page.evaluate(()=>JSON.parse(localStorage.getItem('bramblebound.profile.v1')).profile.run.time),before);
      await page.reload();await page.locator('#title-play').waitFor();assert.match(await page.locator('#title-record').innerText(),/Round/);
      await page.locator('#title-play').click();await page.locator('nav [data-nav="guide"]').click();await page.locator('#guide-page:not([hidden])').waitFor();
      await page.locator('nav [data-nav="legacy"]').click();await page.locator('#legacy-page:not([hidden])').waitFor();
      await page.locator('nav [data-nav="expedition"]').click();await page.locator('#profile-btn').click();
      const download=page.waitForEvent('download');await page.locator('[data-do="export"]').click();assert.match((await download).suggestedFilename(),/bramblebound.*\.json/i);
      const raw=await page.evaluate(()=>localStorage.getItem('bramblebound.profile.v1'));
      await page.locator('#import-input').setInputFiles({name:'profile.json',mimeType:'application/json',buffer:Buffer.from(raw)});await page.locator('[data-do="confirm-import"]').click();
      await page.locator('#modal').waitFor({state:'hidden'});await page.locator('#run-btn').click();
      if(shots)await page.screenshot({path:path.join(shots,'expedition-'+viewport.width+'.png'),fullPage:true});
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
      await page.addInitScript(()=>localStorage.setItem('bramblebound.profile.v1','broken original profile'));
      await page.reload();await page.locator('#modal[open]').waitFor();assert.match(await page.locator('#dialog-body').innerText(),/could not load/);
      assert.equal(await page.evaluate(()=>localStorage.getItem('bramblebound.profile.v1')),'broken original profile');
      assert.deepEqual(errors,[]);assert.deepEqual(missing,[]);results.push(viewport.width+'px: title, handbook, kit, combat, pause, continue, guide, legacy, export/import, recovery, no page errors or missing assets');await context.close();
    }
    // Exercise the real hub loader and cards. Only Firebase uses the hub's
    // existing isolated fixture; game navigation and local artwork are real.
    const fixtureSource=fs.readFileSync(path.join(root,'lobby/tests/arcade-nexus-browser.mjs'),'utf8');
    const firebase=fixtureSource.match(/const firebase=String.raw`([\s\S]*?)`;/)[1];
    for(const width of [1440,390]){
      const context=await browser.newContext({viewport:{width,height:1000}}),page=await context.newPage(),errors=[];
      await context.addInitScript(()=>{globalThis.__fixture={data:{'users/Tester':{username:'Tester',displayName:'Tester',password:'fixture-password',favoriteGames:[],recentlyPlayed:[],gamePlayCounts:{},avatar:'🐭'}},calls:[],listeners:[],sequence:0};localStorage.setItem('gh.username','Tester');localStorage.setItem('gh.lastTab','discover');localStorage.setItem('gh.profilePromptSeen','1');});
      await context.route('**/*',route=>{const url=new URL(route.request().url());if(url.hostname==='www.gstatic.com'&&url.pathname.includes('/firebasejs/'))return route.fulfill({status:200,contentType:'text/javascript',body:firebase});if(!url.href.startsWith(origin))return route.abort();return route.continue();});
      page.on('pageerror',e=>errors.push(e.message));await page.goto(origin+'/lobby/lobby.html');
      const pick=page.locator('#cnmBuiltOutGrid [data-game-key="bramblebound"]');await pick.waitFor();await pick.scrollIntoViewIfNeeded();await pick.locator('img').first().evaluate(img=>img.decode());
      await page.locator('[data-view="singleplayer"]:visible').first().click();await page.locator('#gameSearch').fill('Bramblebound');
      const card=page.locator('#singleGameGrid [data-game-key="bramblebound"]');await card.waitFor();await card.locator('.catalog-status-badge[data-status="showcase"]').waitFor();
      assert.equal(await page.locator('#singleGameGrid .game-card').count(),1);assert.match(await card.innerText(),/Built Out/);
      await page.waitForFunction(()=>document.getElementById('accountButtonText')?.textContent==='Tester');
      const favorite=card.locator('.favorite-btn');await favorite.click();await card.locator('.favorite-btn[aria-pressed="true"]').waitFor();
      if(shots)await page.screenshot({path:path.join(shots,'library-'+width+'.png'),fullPage:true});
      await card.locator('[data-game-action="single"]').click();await page.waitForURL('**/game/bramblebound/');await page.locator('#title-play').waitFor();
      assert.deepEqual(errors,[]);results.push(width+'px library: Built Out shelf, title artwork, search, favorite, real Play navigation');await context.close();
    }
    console.log(results.join('\n'));
  }finally{await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>server.close());
