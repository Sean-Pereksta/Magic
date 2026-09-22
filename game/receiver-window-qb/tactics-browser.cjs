'use strict';
// Local-only browser harness: production code does not expose these debug hooks.
const {chromium}=require('playwright');
const fs=require('node:fs'),http=require('node:http'),path=require('node:path'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../..');
const hook=`globalThis.__qbTactics={setupPlay,callPlay,beginCountdown,updateBlocking,advanceBlockEngagements,releaseBlock,tackleContact,startDivingTackle,get plays(){return plays},get state(){return {receivers,defenders,playState,currentDefense,queuedSnap,gameTime}},run(code){return eval(code)}};`;
const server=http.createServer((req,res)=>{
  try{
    const filename=path.join(root,new URL(req.url,'http://localhost').pathname);
    if(!filename.startsWith(root+path.sep))throw Error('outside test root');
    let data=fs.readFileSync(filename);
    if(filename===path.join(__dirname,'game.js'))data=data.toString().replace(/\}\)\(\);\s*$/,hook+'})();');
    res.setHeader('Content-Type',filename.endsWith('.js')?'text/javascript':filename.endsWith('.css')?'text/css':filename.endsWith('.png')?'image/png':'text/html');res.end(data);
  }catch{res.statusCode=404;res.end('not found');}
});
(async()=>{
  await new Promise(resolve=>server.listen(8766,'127.0.0.1',resolve));
  const browser=await chromium.launch({headless:true,executablePath:process.env.QB_CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage','--enable-unsafe-swiftshader','--use-gl=angle','--use-angle=swiftshader']});
  try{
    for(const mobile of [false,true]){
      const context=await browser.newContext({viewport:mobile?{width:390,height:844}:{width:1280,height:800},hasTouch:mobile,isMobile:mobile});
      const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
      await page.goto('http://127.0.0.1:8766/game/receiver-window-qb.html');await page.waitForFunction(()=>!!window.__qbTactics);
      await page.locator('#startBtn').click();
      const audible=await page.evaluate(()=>{
        const q=__qbTactics;q.setupPlay();const before=q.state;
        window.tacticsBefore={receivers:[...before.receivers],defenders:[...before.defenders],coverage:before.currentDefense};
        const positions=before.defenders.map(d=>d.mesh.position.clone());
        const index=q.plays.findIndex(p=>p.name==='Bunch Read Screen');
        if(!q.callPlay(index))throw Error('team audible rejected');
        return {same:before.defenders.every((d,i)=>d===q.state.defenders[i]&&d.mesh.position.distanceTo(positions[i])<1e-8),moving:q.state.receivers.some(r=>r.setTarget),screen:q.state.receivers.filter(r=>r.screenTarget).length};
      });
      assert.deepEqual(audible,{same:true,moving:true,screen:1});
      await page.locator('#snapBtn').click();
      assert.equal(await page.locator('#playCallPanel').isVisible(),false);
      await page.waitForFunction(()=>__qbTactics.state.playState==='live',{},{timeout:60000});
      assert.equal(await page.evaluate(()=>{
        const s=__qbTactics.state,b=tacticsBefore;
        return !s.queuedSnap&&s.currentDefense===b.coverage&&s.receivers.every((r,i)=>r===b.receivers[i]&&!r.setTarget)&&s.defenders.every((d,i)=>d===b.defenders[i]);
      }),true);
      const battle=await page.evaluate(()=>{
        const q=__qbTactics;q.run("setupPlay();playState='run';ballCarrier=receivers[0];receivers[0].hasBall=true;receivers[0].mesh.position.set(0,0,3);for(const a of [...receivers.slice(1),...defenders])a.mesh.position.set(50,0,30)");
        const b=q.state.receivers[1],d=q.state.defenders[0];b.mesh.position.set(0,0,0);d.mesh.position.set(0,0,-1.2);b.mesh.rotation.y=Math.PI;b.heading.set(0,0,-1);d.heading.set(0,0,1);b.velocity.set(0,0,0);d.velocity.set(0,0,0);b.profile.strength=100;d.strength=25;
        q.updateBlocking(1/120);const linked=b.blockEngagement&&b.blockEngagement===d.blockEngagement;
        q.advanceBlockEngagements(1/120);
        return {linked:!!linked,drive:b.mesh.position.z<0,facing:Math.cos(b.mesh.rotation.y)<-.8,finite:Number.isFinite(d.mesh.position.length())};
      });
      assert.deepEqual(battle,{linked:true,drive:true,facing:true,finite:true});
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
      if(process.env.QB_SCREENSHOT_DIR){fs.mkdirSync(process.env.QB_SCREENSHOT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.QB_SCREENSHOT_DIR,mobile?'tactics-mobile.png':'tactics-desktop.png')});}
      console.log(`PASS ${mobile?'mobile touch':'desktop'}: physical audible, unchanged defense/actors, queued snap, hidden play panel, sustained strength block, no page errors or horizontal overflow.`);
      await context.close();
    }
  }finally{await browser.close();server.close();}
})().catch(e=>{console.error(e);server.close();process.exitCode=1;});
