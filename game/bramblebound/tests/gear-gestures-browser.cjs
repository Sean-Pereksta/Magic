const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.BRAMBLEBOUND_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../../..'),KEY='bramblebound.profile.v1';
const server=http.createServer((req,res)=>{
  try{
    let file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));
    if(!file.startsWith(root+path.sep)||file.includes(path.sep+'.git'))throw Error('Outside fixture');
    if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
    res.setHeader('Content-Type',({'.html':'text/html','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp'})[path.extname(file)]||'application/octet-stream');
    fs.createReadStream(file).on('error',()=>res.end()).pipe(res);
  }catch{res.writeHead(404).end();}
});
async function main(){
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+server.address().port,options={headless:true};
  if(process.env.BRAMBLEBOUND_BROWSER)options.executablePath=process.env.BRAMBLEBOUND_BROWSER;
  const edge='C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';
  if(!options.executablePath&&process.platform==='win32'&&fs.existsSync(edge))options.executablePath=edge;
  const browser=await chromium.launch(options),results=[];
  try{
    for(const width of [1440,390]){
      const context=await browser.newContext({viewport:{width,height:width===390?844:1050},hasTouch:width===390,reducedMotion:'reduce'}),page=await context.newPage(),errors=[];
      context.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
      await context.addInitScript(({KEY})=>{
        const pending=sessionStorage.getItem('bramblebound.test.pending');
        if(pending){localStorage.setItem(KEY,pending);sessionStorage.removeItem('bramblebound.test.pending');}
      },{KEY});
      await page.goto(origin+'/game/bramblebound/');await page.locator('#title-kit').waitFor();
      const fixture=await page.evaluate(()=>{
        const E=GameEngine,C=Catalog,p=E.freshProfile();E.start(p);
        const r=p.run,defs=Object.values(C.items),base=element=>defs.find(d=>d.element===element&&!d.recipe).id;
        r.active=['fire','frost','leaf','storm'].map((element,i)=>({uid:i+1,id:base(element),level:1,cd:0,slot:i}));
        r.pack=[{uid:100,id:base('fire'),level:1,cd:0,slot:0},{uid:101,id:base('fire'),level:1,cd:0,slot:1},
          {uid:102,id:base('stone'),level:1,cd:0,slot:3},{uid:103,id:base('frost'),level:1,cd:0,slot:8}];
        r.nextUid=104;p.seenItems=[...new Set([...r.active,...r.pack].map(it=>it.id))];
        return {game:'bramblebound',format:1,profile:E.validate(JSON.parse(JSON.stringify(p))),settings:{audio:false,motion:false}};
      });
      const reset=async(save=fixture)=>{
        await page.evaluate(save=>sessionStorage.setItem('bramblebound.test.pending',JSON.stringify(save)),save);
        await page.reload();await page.locator('#title-kit').click();
        if(save.profile.run.phase==='dead'){
          await page.locator('#modal[open]').waitFor();await page.locator('#close-btn').click();
        }else assert.match(await page.locator('#run-btn').innerText(),/Resume/);
        await page.locator('.kit-content').evaluate(el=>el.scrollTop=0);
      };
      const stored=()=>page.evaluate(KEY=>JSON.parse(localStorage.getItem(KEY)).profile.run,KEY);
      const gear=async()=>{const r=await stored();return JSON.stringify({active:r.active,pack:r.pack});};
      const slot=uid=>page.locator('.gear-slot[data-uid="'+uid+'"]');
      const center=async node=>{await node.scrollIntoViewIfNeeded();const b=await node.boundingBox();return {x:b.x+b.width/2,y:b.y+b.height/2};};
      const client=width===390?await context.newCDPSession(page):null;
      const touch=async(type,p)=>client.send('Input.dispatchTouchEvent',{type,touchPoints:['touchEnd','touchCancel'].includes(type)?[]:[{x:p.x,y:p.y,radiusX:2,radiusY:2,force:1}]});
      const press=async node=>{
        const p=await center(node);
        if(client)await touch('touchStart',p);else {await page.mouse.move(p.x,p.y);await page.mouse.down();}
        return p;
      };
      const move=async p=>{if(client)await touch('touchMove',p);else await page.mouse.move(p.x,p.y);};
      const release=async p=>{if(client)await touch('touchEnd',p);else await page.mouse.up();await page.waitForTimeout(70);};
      const select=async uid=>{if(client)await slot(uid).tap();else await slot(uid).click();assert.equal(await slot(uid).getAttribute('aria-pressed'),'true');};
      const assertCombined=async(selected,held)=>{
        const r=await stored(),items=[...r.active,...r.pack],result=items.find(it=>it.uid>=104);
        assert.equal(items.length,7,'one hold consumes exactly two inputs and creates one result');
        assert.equal(items.some(it=>it.uid===selected||it.uid===held),false);
        assert.ok(result);assert.equal(result.level,2);
        assert.equal(await slot(result.uid).getAttribute('aria-pressed'),'true','trailing release must retain result selection');
        assert.equal(await page.locator('#modal').evaluate(el=>el.open),false,'hold combines directly without a preview dialog');
        assert.equal(await page.locator('.drag-chip').count(),0);
        assert.equal(await page.evaluate(KEY=>{const save=JSON.parse(localStorage.getItem(KEY));return GameEngine.validate(save.profile).run.nextUid;},KEY),105);
        return result;
      };

      // Short taps select glowing pack partners without consuming either relic.
      await reset();const initial=await gear();await select(100);
      assert.match(await slot(101).getAttribute('class'),/fusion-partner/);
      await select(101);assert.equal(await gear(),initial);assert.equal(await page.locator('#modal').evaluate(el=>el.open),false);

      // Selected relics drag immediately, including a horizontal move that would
      // otherwise be recognized as a page swipe. The destination is an empty slot.
      await reset();await select(100);
      let start=await press(slot(100)),target=await center(page.locator('#pack .gear-slot[data-index="2"]'));
      for(let i=1;i<=5;i++)await move({x:start.x+(target.x-start.x)*i/5,y:start.y+(target.y-start.y)*i/5});
      assert.equal(await page.locator('body').evaluate(el=>el.classList.contains('dragging-gear')),true,'selected item must drag without a long press');
      await release(target);assert.equal((await stored()).pack.find(it=>it.uid===100).slot,2);
      assert.equal(await page.locator('#pack-page').inputValue(),'0');

      // A glowing pack partner and a glowing equipped partner both combine on
      // hold. Test both native touch and mouse, retaining the selected result.
      for(const [selected,held] of [[1,100],[100,1],[100,101]]){
        await reset();await select(selected);assert.match(await slot(held).getAttribute('class'),/fusion-partner/);
        start=await press(slot(held));await page.waitForTimeout(380);
        const beforeRelease=await stored();assert.equal(beforeRelease.nextUid,105,'combination happens while the partner is held');
        await page.waitForTimeout(350);assert.equal((await stored()).nextUid,105,'continued holding must not combine a second time');
        await release(start);const result=await assertCombined(selected,held);
        await page.reload();await page.locator('#title-kit').click();
        assert.equal([...((await stored()).active),...((await stored()).pack)].find(it=>it.uid===result.uid).level,2,'combination persists after reload');
      }

      // Different relics with a recipe fuse through the same hold gesture.
      const fusion=await page.evaluate(save=>{
        const output=Object.values(Catalog.items).find(d=>d.recipe&&d.recipe.every(id=>!Catalog.items[id].recipe));
        const copy=JSON.parse(JSON.stringify(save));copy.profile.run.active[0].id=output.recipe[0];copy.profile.run.pack[0].id=output.recipe[1];
        copy.profile.seenItems=[...new Set([...copy.profile.seenItems,...output.recipe])];
        return {save:copy,id:output.id};
      },fixture);
      await reset(fusion.save);await select(100);assert.match(await slot(1).getAttribute('class'),/fusion-partner/);
      start=await press(slot(1));await page.waitForTimeout(380);await release(start);
      assert.equal((await assertCombined(100,1)).id,fusion.id,'holding recipe partners creates the expected fusion');

      // Pointer cancellation ends the hold, with no delayed mutation.
      await reset();await select(1);const beforeCancel=await gear();start=await press(slot(100));
      if(client)await touch('touchCancel',start);else {
        await slot(100).dispatchEvent('pointercancel',{pointerId:1,isPrimary:true,pointerType:'mouse'});await page.mouse.up();
      }
      await page.waitForTimeout(420);assert.equal(await gear(),beforeCancel);

      // Movement before the hold expires cancels quick combine. For mouse, it
      // becomes an ordinary drag; for touch, it retains the page swipe gesture.
      await reset();await select(1);const beforeMove=await gear();start=await press(slot(100));
      const shifted={x:start.x+25,y:start.y};await move(shifted);await page.waitForTimeout(380);await release(shifted);
      assert.equal(await gear(),beforeMove,'moving a glowing item cannot trigger hold-to-combine');
      assert.equal((await stored()).nextUid,104);

      if(client){
        const swipe=async(dx)=>{
          const box=await page.locator('#pack').boundingBox(),p={x:dx<0?box.x+box.width-30:box.x+30,y:box.y+30};
          await touch('touchStart',p);
          for(let i=1;i<=6;i++){await touch('touchMove',{x:p.x+dx*i/6,y:p.y});await page.waitForTimeout(15);}
          await touch('touchEnd',p);await page.waitForTimeout(70);
        };
        await reset();const beforeSwipe=await gear();await swipe(-180);
        assert.equal(await page.locator('#pack-page').inputValue(),'1','unselected filled slots retain page swiping');assert.equal(await gear(),beforeSwipe);
        await page.locator('#pack-page').selectOption('5');assert.equal(await page.locator('#pack .gear-slot[data-uid]').count(),0);
        await swipe(-180);assert.equal(await page.locator('#pack-page').inputValue(),'6','empty slots retain page swiping');assert.equal(await gear(),beforeSwipe);
      }

      // Finished journeys cannot consume relics through hold gestures.
      const dead=await page.evaluate(save=>{
        const p=GameEngine.validate(save.profile),r=p.run;
        r.walkLeft=0;GameEngine.tick(p,.075);r.hp=.01;r.venom=1000;r.venomClock=.99;
        GameEngine.tick(p,.075);if(r.phase!=='dead')throw Error('Death fixture did not finish');
        return {...save,profile:GameEngine.validate(JSON.parse(JSON.stringify(p)))};
      },fixture);
      await reset(dead);const beforeDead=await gear();start=await press(slot(100));await page.waitForTimeout(380);await release(start);assert.equal(await gear(),beforeDead);
      assert.deepEqual(errors,[]);if(client)await client.detach();await context.close();
      results.push(width+'px: immediate selected-item drag; pack/equipped hold upgrades and recipe fusion once, selection/save/reload; taps, motion and cancellation; finished-run protection'+(width===390?'; native filled/empty swipes':''));
    }
    console.log(results.join('\n'));
  }finally{await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>server.close());
