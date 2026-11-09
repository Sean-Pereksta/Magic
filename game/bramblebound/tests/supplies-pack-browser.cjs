const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.BRAMBLEBOUND_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../../..'),KEY='bramblebound.profile.v1',shots=process.env.BRAMBLEBOUND_SCREENSHOTS;
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
  const capture=async(page,name)=>{if(shots){fs.mkdirSync(shots,{recursive:true});await page.screenshot({path:path.join(shots,name+'.png'),fullPage:true});}};
  try{
    for(const width of [1440,390]){
      const context=await browser.newContext({viewport:{width,height:width===390?844:1050},hasTouch:width===390,reducedMotion:'reduce'}),page=await context.newPage(),errors=[],missing=[];
      context.setDefaultTimeout(12000);
      page.on('pageerror',e=>errors.push(e.message));page.on('response',r=>{if(r.status()>=400)missing.push(r.status()+' '+r.url());});
      await context.addInitScript(({KEY})=>{const pending=sessionStorage.getItem('bramblebound.test.pending');if(pending){localStorage.setItem(KEY,pending);sessionStorage.removeItem('bramblebound.test.pending');}},{KEY});
      await page.goto(origin+'/game/bramblebound/');await page.locator('#title-kit').waitFor();
      const fixture=await page.evaluate(()=>{
        const E=GameEngine,C=Catalog,p=E.freshProfile();E.start(p);const r=p.run,defs=Object.values(C.items);
        r.hp=40;
        r.active=['fire','frost','leaf','storm'].map((el,i)=>({uid:i+1,id:defs.find(d=>d.element===el&&!d.recipe).id,level:1,cd:0,slot:i}));
        const candidates=defs.filter(d=>!d.recipe&&!r.active.some(it=>it.id===d.id));
        r.pack=[{uid:100,id:r.active[0].id,level:1,cd:0,slot:0},...candidates.slice(0,23).map((d,i)=>({uid:i+101,id:d.id,level:1+(i%7)*2,cd:0,slot:i+1}))];
        r.pack.at(-1).id=defs.find(d=>d.rarity==='ancient').id; // No copy or next-stage partner: must disappear under Combinable.
        r.nextUid=124;p.seenItems=[...new Set([...r.active,...r.pack].map(it=>it.id))];
        const score=it=>{const s=E.itemComparison(p,it);return (s.damage*(1+s.lifesteal)+s.heal+s.shield+8*(s.stun+s.freeze))/Math.max(.15,s.cooldown);};
        const items=[...r.active,...r.pack],possible=new Set();
        for(let i=0;i<items.length;i++)for(let j=i+1;j<items.length;j++)if(E.previewPair(items[i],items[j]).ok){possible.add(items[i].uid);possible.add(items[j].uid);}
        return {save:{game:'bramblebound',format:1,profile:E.validate(JSON.parse(JSON.stringify(p))),settings:{audio:false,motion:false}},
          sorted:r.pack.slice().sort((a,b)=>score(b)-score(a)||a.slot-b.slot).map(it=>it.uid),
          combinable:r.pack.filter(it=>possible.has(it.uid)).map(it=>it.uid),gear:JSON.stringify({active:r.active,pack:r.pack}),hp:r.hp,tonicHeal:r.maxHp*.28};
      });
      await page.evaluate(save=>sessionStorage.setItem('bramblebound.test.pending',JSON.stringify(save)),fixture.save);await page.reload();await page.locator('#title-kit').click();
      await page.locator('#provisions-btn').waitFor();assert.match(await page.locator('#run-btn').innerText(),/Resume/);
      const stored=()=>page.evaluate(KEY=>JSON.parse(localStorage.getItem(KEY)).profile.run,KEY);
      const gear=async()=>{const r=await stored();return JSON.stringify({active:r.active,pack:r.pack});};
      const visibleUids=()=>page.locator('#pack .gear-slot[data-uid]').evaluateAll(nodes=>nodes.map(n=>Number(n.dataset.uid)));
      const allVisible=async()=>{
        const values=await page.locator('#pack-page option').evaluateAll(nodes=>nodes.map(n=>n.value)),uids=[];
        for(const value of values){await page.locator('#pack-page').selectOption(value);uids.push(...await visibleUids());}
        return uids;
      };
      assert.equal(await gear(),fixture.gear);

      await page.locator('#provisions-btn').click();await page.locator('#modal[open]').waitFor();
      assert.equal(await page.locator('#modal .provision-card').count(),5);
      assert.equal(await page.locator('#modal .provision-art svg').count(),5);
      assert.equal(new Set(await page.locator('#modal .provision-art svg').evaluateAll(nodes=>nodes.map(n=>n.innerHTML))).size,5);
      assert.match(await page.locator('#dialog-body').innerText(),/2 \/ 6 packed/);
      const pausedTime=(await stored()).time;await page.waitForTimeout(150);assert.equal((await stored()).time,pausedTime);
      await capture(page,'bramblebound-trail-pouch-'+width);
      await page.locator('[data-use-consumable="moss_tonic"]').click();await page.locator('#modal').waitFor({state:'hidden'});
      let r=await stored();assert.ok(Math.abs(r.hp-(fixture.hp+fixture.tonicHeal))<1e-8);assert.equal(r.provisions.counts.moss_tonic,0);assert.equal(r.provisions.counts.bark_ward,1);assert.equal(r.provisions.used,1);assert.equal(r.provisions.cooldown,1.25);assert.equal(r.time,pausedTime);
      await page.locator('#provisions-btn').click();assert.equal(await page.locator('[data-use-consumable="bark_ward"]').isDisabled(),true);assert.match(await page.locator('.pouch-note').innerText(),/recovering/i);await page.locator('#close-btn').click();
      await page.reload();await page.locator('#title-kit').click();r=await stored();assert.equal(r.provisions.used,1);assert.ok(Math.abs(r.hp-(fixture.hp+fixture.tonicHeal))<1e-8);assert.equal(await gear(),fixture.gear);

      assert.match(await page.locator('#synergies-count').innerText(),/Emberglass II/);assert.match(await page.locator('#synergies-count').innerText(),/Bramble Circuit II/);
      await page.locator('#synergies-btn').click();assert.equal(await page.locator('#modal .set-card').count(),3);assert.equal(await page.locator('#modal .set-card.active').count(),2);
      assert.deepEqual(await page.locator('#modal .set-card h3').allTextContents(),['Emberglass','Bramble Circuit','Soulstone']);
      assert.match(await page.locator('#dialog-body').innerText(),/3 pieces/);await capture(page,'bramblebound-elemental-sets-'+width);await page.locator('#close-btn').click();

      await page.locator('#pack-sort').selectOption('strongest');assert.deepEqual(await allVisible(),fixture.sorted,'strongest sort must use current level and build output across pages');assert.equal(await gear(),fixture.gear,'sort must not rearrange saved slots or gear');
      await page.locator('#pack-sort').selectOption('slots');await page.locator('#pack-combinable').click();assert.equal(await page.locator('#pack-combinable').getAttribute('aria-pressed'),'true');
      assert.deepEqual(await allVisible(),fixture.combinable);assert.ok(fixture.combinable.includes(100),'equipped copy counts as a valid partner');assert.ok(fixture.combinable.length<24,'filter removes at least one unmatched relic');
      assert.equal(await gear(),fixture.gear,'filter must not move or consume gear');await page.locator('#pack-combinable').click();await page.locator('#pack-page').selectOption('0');
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);await capture(page,'bramblebound-pack-tools-'+width);

      if(width===390){
        const client=await context.newCDPSession(page);
        const touch=async(type,x,y)=>client.send('Input.dispatchTouchEvent',{type,touchPoints:type==='touchEnd'||type==='touchCancel'?[]:[{x,y,radiusX:2,radiusY:2,force:1}]});
        const swipe=async(dx,dy=0)=>{
          const box=await page.locator('#pack').boundingBox(),x=dx<0?box.x+box.width-30:box.x+30,y=box.y+Math.min(30,box.height/3);
          await touch('touchStart',x,y);
          for(let i=1;i<=6;i++){await touch('touchMove',x+dx*i/6,y+dy*i/6);await page.waitForTimeout(15);}
          await touch('touchEnd',x+dx,y+dy);await page.waitForTimeout(60);
        };
        await page.locator('#pack').scrollIntoViewIfNeeded();
        const beforeSwipe=await gear();await swipe(-180);assert.equal(await page.locator('#pack-page').inputValue(),'1');assert.equal(await gear(),beforeSwipe,'a filled-cell swipe must not move, equip or fuse');
        await swipe(180);assert.equal(await page.locator('#pack-page').inputValue(),'0');assert.equal(await gear(),beforeSwipe);
        await page.locator('#pack-page').selectOption('5');assert.equal(await page.locator('#pack .gear-slot[data-uid]').count(),0);await swipe(-180);assert.equal(await page.locator('#pack-page').inputValue(),'6');await swipe(180);assert.equal(await page.locator('#pack-page').inputValue(),'5');assert.equal(await gear(),beforeSwipe,'empty pages swipe without touching gear');
        // A mainly vertical gesture stays a scroll gesture, never a page turn.
        const scrollable=await page.locator('.kit-content').evaluate(el=>{const max=el.scrollHeight-el.clientHeight;el.scrollTop=Math.min(max/2,20);return {max,top:el.scrollTop};});
        const packBox=await page.locator('#pack').boundingBox(),verticalY=Math.min(760,packBox.y+packBox.height-8),verticalX=packBox.x+packBox.width/2;
        await touch('touchStart',verticalX,verticalY);for(let i=1;i<=6;i++){await touch('touchMove',verticalX+2,verticalY-i*9);await page.waitForTimeout(15);}await touch('touchEnd',verticalX+2,verticalY-54);await page.waitForTimeout(120);
        assert.equal(await page.locator('#pack-page').inputValue(),'5');assert.equal(await gear(),beforeSwipe);
        if(scrollable.max>12)assert.ok(await page.locator('.kit-content').evaluate((el,before)=>el.scrollTop!==before,scrollable.top),'vertical pack gestures must retain normal scrolling');
        await page.locator('#pack-page').selectOption('0');await page.locator('#pack .gear-slot[data-uid="100"]').scrollIntoViewIfNeeded();
        const held=await page.locator('#pack .gear-slot[data-uid="100"]').boundingBox(),hx=held.x+held.width/2,hy=held.y+held.height/2;
        await touch('touchStart',hx,hy);await page.waitForTimeout(310);await touch('touchMove',hx+12,hy);assert.equal(await page.locator('body').evaluate(el=>el.classList.contains('dragging-gear')),true);assert.equal(await page.locator('.drag-chip').count(),1);await touch('touchEnd',hx+12,hy);await page.waitForTimeout(100);
        assert.equal(await gear(),beforeSwipe,'holding and releasing the same relic retains drag without unintended fusion');assert.equal(await page.locator('#pack-page').inputValue(),'0');
        assert.equal(await page.locator('#modal').evaluate(el=>el.open),false);
        // A deliberate held vertical drag still reaches Equipped. Matching copies
        // should upgrade exactly once, and the immediate checkpoint must validate.
        await page.locator('.kit-content').evaluate(el=>el.scrollTop=0);
        const source=await page.locator('#pack .gear-slot[data-uid="100"]').boundingBox(),target=await page.locator('#equipped .gear-slot[data-uid="1"]').boundingBox();
        const sx=source.x+source.width/2,sy=source.y+source.height/2,tx=target.x+target.width/2,ty=target.y+target.height/2;
        await touch('touchStart',sx,sy);await page.waitForTimeout(310);
        for(let i=1;i<=5;i++){await touch('touchMove',sx+(tx-sx)*i/5,sy+(ty-sy)*i/5);await page.waitForTimeout(15);}
        assert.equal(await page.locator('body').evaluate(el=>el.classList.contains('dragging-gear')),true);
        await touch('touchEnd',tx,ty);await page.waitForTimeout(100);
        const upgraded=await stored();assert.equal(upgraded.pack.length,23);assert.equal(upgraded.active.length,4);assert.equal(upgraded.active.find(it=>it.slot===0).level,2);assert.equal(upgraded.pack.some(it=>it.uid===100),false);
        assert.equal(await page.evaluate(KEY=>{const save=JSON.parse(localStorage.getItem(KEY));return GameEngine.validate(save.profile).run.active.find(it=>it.slot===0).level;},KEY),2);
        await client.detach();
      }
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);assert.deepEqual(errors,[]);assert.deepEqual(missing,[]);
      results.push(width+'px: five distinct supply icons; pause/use/cooldown/save/reload; three readable sets with two active; complete strongest order and equipped-partner combinable filter without gear mutation'+(width===390?'; native filled/empty swipes, vertical scrolling and long-press drag':'' )+'; no overflow, missing assets or page errors');
      await context.close();
    }
    console.log(results.join('\n'));
  }finally{await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>server.close());
