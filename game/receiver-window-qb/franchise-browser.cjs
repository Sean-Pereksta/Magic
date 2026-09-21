/* Browser integration: real Three.js, saved uniforms, menus, stats and replay ownership. */
const {chromium}=require(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES+'/playwright':'playwright');
const fs=require('node:fs'),http=require('node:http'),path=require('node:path'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../..');
const hook=`globalThis.__franchiseTest={get f(){return franchise},get receivers(){return receivers},get defenders(){return defenders},get log(){return playLog},get selected(){return plays[selectedPlay]},get stadium(){return stadiumDetails},get menu(){return menuOpen},get phase(){return playState},setupPlay,beginCountdown,throwBall,resolveCatch,finishPlayAtSpot,showMainMenu,resumeGame,saveFranchise,endMatchup,animatePlayerContact,scenePose,archiveReplay,clearLastReplay,get lastReplay(){return lastReplay},setState(v){if(v.state)playState=v.state;if(v.series!==undefined)seriesOffense=v.series;if(v.clearTransition)transition=null;},get pose(){return scenePose()},closeStudio(){studio.close()}};`;
const server=http.createServer((req,res)=>{try{const p=path.join(root,new URL(req.url,'http://local').pathname);let data=fs.readFileSync(p);if(p.endsWith('/game.js'))data=data.toString().replace(/\}\)\(\);\s*$/,hook+'})();');res.setHeader('Content-Type',p.endsWith('.js')?'text/javascript':p.endsWith('.css')?'text/css':'text/html');res.end(data);}catch{res.writeHead(404);res.end('missing');}});
(async()=>{
 await new Promise(r=>server.listen(0,'127.0.0.1',r));const browser=await chromium.launch({headless:true,executablePath:process.env.QB_CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--enable-unsafe-swiftshader','--use-gl=angle','--use-angle=swiftshader']});
 try{
 const page=await browser.newPage({viewport:{width:1280,height:900}}),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto(`http://127.0.0.1:${server.address().port}/game/receiver-window-qb.html`);await page.waitForFunction(()=>!!window.__franchiseTest);
 await page.click('#uniformBtn');await page.fill('#uniformName','Test Wolves');await page.click('[data-logo="🐉"]').then(()=>{});
 await page.locator('[data-color="jersey"]').evaluate(el=>{el.value='#a11234';el.dispatchEvent(new Event('input',{bubbles:true}));});
 await page.click('[data-kit="away"]');await page.locator('[data-color="jersey"]').evaluate(el=>{el.value='#e1f2a3';el.dispatchEvent(new Event('input',{bubbles:true}));});await page.selectOption('#activeUniform','away');await page.click('#saveUniform');assert.match(await page.locator('#uniformStatus').innerText(),/saved/);await page.keyboard.press('Escape');
 assert.equal(await page.locator('#franchiseLayer').isHidden(),true);assert.equal(await page.evaluate(()=>document.activeElement.id),'uniformBtn');
 await page.reload();await page.waitForFunction(()=>!!window.__franchiseTest);
 assert.deepEqual(await page.evaluate(()=>{const i=__franchiseTest.f.identity;return [i.name,i.logo,i.active,i.home.jersey,i.away.jersey]}),['Test Wolves','🐉','away','#a11234','#e1f2a3']);
 await page.click('#menuPlaybookBtn');assert.equal(await page.locator('[data-studio-category]').count(),8);assert.equal(await page.locator('[data-studio-play]').count(),6);await page.click('[data-studio-category="Screens"]');await page.locator('[data-studio-play]').first().click();await page.click('#callSelectedPlay');await page.click('#startBtn');
 assert.equal(await page.evaluate(()=>__franchiseTest.selected.name),'WR Bubble');
 assert.equal(await page.evaluate(()=>__franchiseTest.receivers.every(r=>r.mesh.userData.appearance.jerseyPrimary==='#e1f2a3'&&r.mesh.userData.appearance.logo==='🐉')),true);
 assert.equal(await page.evaluate(()=>__franchiseTest.receivers.every(r=>r.mesh.userData.headRig&&r.mesh.userData.decals.length===5)),true);
 // Saved career totals count the completed play once, including a recovered pass and YAC.
 await page.evaluate(()=>{const q=__franchiseTest;q.beginCountdown();q.setState({state:'live'});q.throwBall();for(const d of q.defenders)d.mesh.position.set(100,0,100);const r=q.receivers[0];r.mesh.position.z=20;q.resolveCatch(r,false);q.finishPlayAtSpot(-20);q.showMainMenu();});
 assert.deepEqual(await page.evaluate(()=>{const q=__franchiseTest,r=q.f.currentGame.receivers[q.receivers[0].profile.id];return [q.f.currentGame.qb.attempts,q.f.currentGame.qb.completions,q.f.currentGame.qb.yards,r.yac];}),[1,1,30,20]);
 await page.evaluate(()=>{const q=__franchiseTest;const before=JSON.stringify(q.f.career);if(QBFranchise.commitPlay(q.f,q.log,{snap:0,spot:50,touchdown:true}))throw Error('committed twice');if(JSON.stringify(q.f.career)!==before)throw Error('duplicate changed career');});
 await page.reload();assert.deepEqual(await page.evaluate(()=>[__franchiseTest.f.currentGame.qb.attempts,__franchiseTest.f.career.qb.yards]),[1,30]);
 await page.click('#recordsBtn');assert.match(await page.locator('#franchiseContent').innerText(),/Longest completion/);assert.match(await page.locator('#franchiseContent').innerText(),/Receiver careers/);await page.click('#closeFranchise');
 await page.click('#startBtn');
 // Decals remain valid in detached replay models; actor resets keep texture use bounded.
 await page.evaluate(()=>{const q=__franchiseTest,a=q.pose;q.archiveReplay([a,{...a,time:a.time+.1}]);const count=QBPresentation.textureCount;q.setupPlay();if(!q.lastReplay)throw Error('missing archive');for(const r of q.lastReplay.group.children)r.traverse(o=>{if(o.material?.map&&!o.material.map.image)throw Error('lost replay decal')});for(let i=0;i<15;i++)q.setupPlay();if(QBPresentation.textureCount>count+20)throw Error('unbounded decal cache');q.clearLastReplay();});
 // All six deterministic venue templates apply without changing weather mechanics.
 assert.equal(await page.evaluate(()=>{const q=__franchiseTest,seen=new Set();for(let round=1;round<=50;round++){const v=QBFranchise.opponent(round);if(seen.has(v.venue))continue;q.stadium.theme(round);seen.add(v.venue);}return seen.size}),6);
 await page.click('#menuBtn');
 for(const [width,height] of [[390,844],[844,390]]){
  await page.setViewportSize({width,height});await page.click('#uniformBtn');assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.equal(await page.locator('#closeFranchise').isVisible(),true);await page.locator('[data-kit="home"]').focus();await page.keyboard.press('Enter');assert.equal(await page.locator('[data-kit="home"]').getAttribute('aria-pressed'),'true');await page.click('#saveUniform');await page.click('#closeFranchise');
  await page.click('#recordsBtn');assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);await page.click('#closeFranchise');
 }
 assert.deepEqual(errors,[]);console.log('PASS: home/away designer, emoji field decals, save reload, Suggested playbook, keyboard modal focus, completed-play statistics, replay textures, six venues, phone/landscape layouts.');
 }finally{await browser.close();server.close();}
})().catch(e=>{console.error(e);server.close();process.exit(1);});
