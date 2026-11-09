const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.BRAMBLEBOUND_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../../..');
const KEY='bramblebound.profile.v1';
const shots=process.env.BRAMBLEBOUND_SCREENSHOTS;
const server=http.createServer((req,res)=>{
  try{
    let file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));
    if(!file.startsWith(root+path.sep)||file.includes(path.sep+'.git'))throw Error();
    if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
    res.setHeader('Content-Type',({'.html':'text/html','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp'})[path.extname(file)]||'application/octet-stream');
    fs.createReadStream(file).on('error',()=>res.end()).pipe(res);
  }catch{res.writeHead(404).end();}
});
const painted=canvas=>{
  const data=canvas.getContext('2d').getImageData(0,0,canvas.width,canvas.height).data,colors=new Set();let opaque=0;
  for(let i=0;i<data.length;i+=4){if(data[i+3])opaque++;colors.add(data[i]+','+data[i+1]+','+data[i+2]);}
  return {colors:colors.size,coverage:opaque/(canvas.width*canvas.height)};
};
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
      const context=await browser.newContext({viewport:{width,height:width===390?844:1050},reducedMotion:'reduce'}),page=await context.newPage(),errors=[],missing=[];
      context.setDefaultTimeout(12000);
      page.on('pageerror',e=>errors.push(e.message));page.on('response',r=>{if(r.status()>=400)missing.push(r.status()+' '+r.url());});
      // Apply a fixture in the new document, after the outgoing page's final
      // autosave, and consume it so later reloads verify the real saved result.
      await context.addInitScript(({KEY})=>{const pending=sessionStorage.getItem('bramblebound.test.pending');if(pending){localStorage.setItem(KEY,pending);sessionStorage.removeItem('bramblebound.test.pending');}},{KEY});
      const install=async save=>{await page.evaluate(save=>sessionStorage.setItem('bramblebound.test.pending',JSON.stringify(save)),save);await page.reload();};
      await page.goto(origin+'/game/bramblebound/');await page.locator('#title-kit').waitFor();
      const fixture=await page.evaluate(()=>{
        const E=GameEngine,C=Catalog,p=E.freshProfile();E.start(p);const r=p.run;
        const ancient=C.items.ancient_genesis_tide||Object.values(C.items).find(d=>d.rarity==='ancient');
        if(!ancient||ancient.recipe.length!==2||!ancient.recipe.every(id=>C.items[id].rarity==='mythic'))throw Error('Two Mythics required');
        r.active=[{uid:1,id:'stick',level:1,cd:0,slot:0}];
        r.pack=ancient.recipe.map((id,i)=>({uid:i+2,id,level:i?7.25:3.5,cd:0,slot:i}));r.nextUid=4;
        p.seenItems=[...new Set([...r.active,...r.pack].map(it=>it.id))];
        return {save:{game:'bramblebound',format:1,profile:E.validate(JSON.parse(JSON.stringify(p))),settings:{audio:false,motion:false}},id:ancient.id,name:ancient.name,ingredients:ancient.recipe.map(id=>C.items[id].name)};
      });
      await install(fixture.save);await page.locator('#title-kit').click();
      await page.locator('#pack .gear-slot[data-uid="2"]').click();await page.locator('[data-selection="details"]').click();
      await page.locator('#dialog-body [data-gear="combine"]').click();await page.locator('[data-pair-with="3"]').click();
      assert.equal(await page.locator('#dialog-body .dialog-title').innerText(),fixture.name);
      assert.equal(await page.locator('#dialog-body .level-tag').innerText(),'LEVEL 8.25');
      assert.match(await page.locator('#dialog-body .tier-badge[data-rarity="ancient"]').innerText(),/^ancient$/i);
      const preview=await page.locator('#dialog-body').innerText();for(const ingredient of fixture.ingredients)assert.ok(preview.includes(ingredient));
      await capture(page,'ancient-forge-'+width);
      await page.locator('[data-do="forge"]').click();await page.locator('#modal').waitFor({state:'hidden'});
      const forged=await page.evaluate(KEY=>JSON.parse(localStorage.getItem(KEY)).profile.run.pack,KEY);
      assert.equal(forged.length,1);assert.equal(forged[0].id,fixture.id);assert.equal(forged[0].level,8.25);assert.equal(forged[0].uid,4);
      await page.locator('#pack-tier').selectOption('ancient');assert.equal(await page.locator('#pack .gear-slot[data-rarity="ancient"]').count(),1);
      // Forging selects the result; clear it before opening its details.
      await page.locator('[data-selection="clear"]').click();await page.locator('#pack .gear-slot[data-uid="4"]').click();await page.locator('[data-selection="details"]').click();
      const details=await page.locator('#dialog-body').innerText();assert.match(details,/Ancient power/i);assert.match(details,/Character aura/i);assert.match(details,/Ancient fusion/i);
      await page.locator('#dialog-body [data-gear="equip"]').click();await page.locator('#modal').waitFor({state:'hidden'});
      assert.equal(await page.locator('#equipped .gear-slot[data-uid="4"][data-rarity="ancient"]').count(),1);
      await page.reload();await page.locator('#title-kit').click();assert.equal(await page.locator('#equipped .gear-slot[data-uid="4"][data-rarity="ancient"]').count(),1);
      await page.locator('nav [data-nav="guide"]').click();await page.locator('[data-guide-tier="ancient"]').click();
      const ancientCards=page.locator('#guide-page .guide-card[data-catalog]');await ancientCards.first().waitFor();
      assert.equal(await ancientCards.evaluateAll(nodes=>nodes.every(n=>n.dataset.rarity==='ancient')),true);
      await page.locator('[data-guide-tab="recipes"]').click();await page.locator('#guide-search').fill(fixture.name);
      assert.ok((await page.locator('#guide-page').innerText()).includes(fixture.name));assert.match(await page.locator('#guide-page').innerText(),/Ancient/i);
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);

      // Produce a real fresh round-ten encounter, then checkpoint a live warning.
      const bossFixture=await page.evaluate(()=>{
        const E=GameEngine,C=Catalog,p=E.freshProfile();p.perks.heart=100;p.best=9;E.start(p);const r=p.run;
        r.round=10;r.region=E.routeInfo(r).region;r.kills=9;r.foes=9;r.walkLeft=0;r.draft=[];
        r.active=[{uid:1,id:'stick',level:1,cd:C.items.stick.cooldown,slot:0}];r.pack=[];r.nextUid=2;
        E.tick(p,.001);let boss=r.enemies.find(e=>e.titanId);if(!boss)throw Error('Round ten must spawn a Titan');
        for(let step=0;step<500&&!boss.titan.warning;step++)E.tick(p,.05);
        if(!boss.titan.warning)throw Error('Titan must telegraph its power');
        boss.titan.warning.left=.25;
        const ancient=C.items.ancient_stormsovereign||Object.values(C.items).find(d=>d.rarity==='ancient');
        r.active.push({uid:r.nextUid++,id:ancient.id,level:1,cd:ancient.cooldown,slot:1});p.seenItems=[...new Set([...p.seenItems,ancient.id])];
        return {save:{game:'bramblebound',format:1,profile:E.validate(JSON.parse(JSON.stringify(p))),settings:{audio:false,motion:false}},id:boss.titanId,name:boss.name,warning:boss.titan.warning.move};
      });
      await install(bossFixture.save);await page.locator('#title-kit').click();await page.locator('#titan-banner').waitFor();
      assert.ok(await page.locator('#titan-banner').evaluate(el=>parseFloat(getComputedStyle(el).paddingTop)>0),'Titan banner styling must load');
      assert.ok((await page.locator('#titan-banner').innerText()).includes(bossFixture.name));
      assert.ok((await page.locator('#titan-telegraph').innerText()).trim().length>0,'warning must be visible while paused');
      const scene=await page.locator('#world').evaluate(painted);assert.ok(scene.coverage>.8);assert.ok(scene.colors>50);
      await capture(page,'titan-warning-'+width);
      const checkpoint=await page.evaluate(KEY=>JSON.parse(localStorage.getItem(KEY)).profile.run.enemies.find(e=>e.titanId),KEY);
      assert.equal(checkpoint.titanId,bossFixture.id);assert.equal(checkpoint.titan.warning.move,bossFixture.warning);
      await page.reload();await page.locator('#title-kit').click();assert.ok((await page.locator('#titan-banner').innerText()).includes(bossFixture.name));
      const beforeCast=await page.locator('#world').evaluate(canvas=>canvas.toDataURL());
      await page.locator('#run-btn').click();await page.waitForTimeout(430);await page.locator('#run-btn').click();
      const castState=await page.evaluate(KEY=>JSON.parse(localStorage.getItem(KEY)).profile.run.enemies.find(e=>e.titanId),KEY);
      assert.equal(castState.titan.casts,checkpoint.titan.casts+1);assert.equal(castState.titan.warning,null);
      assert.notEqual(await page.locator('#world').evaluate(canvas=>canvas.toDataURL()),beforeCast,'live Titan power must alter the rendered scene');
      assert.equal(await page.locator('body').evaluate(el=>el.classList.contains('reduced-art')),true);
      await capture(page,'titan-cast-'+width);
      const enrage=await page.evaluate(KEY=>{
        const save=JSON.parse(localStorage.getItem(KEY)),p=GameEngine.validate(save.profile),boss=p.run.enemies.find(e=>e.titanId);boss.hp=boss.maxHp*.49;
        GameEngine.tick(p,.001);save.profile=GameEngine.validate(p);return {save,phase:boss.titan.phase};
      },KEY);
      assert.equal(enrage.phase,2);await install(enrage.save);await page.locator('#title-kit').click();assert.match(await page.locator('#titan-banner').innerText(),/2|enraged|awakened/i);
      await capture(page,'titan-enraged-'+width);
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);assert.deepEqual(errors,[]);assert.deepEqual(missing,[]);
      results.push(width+'px: two-Mythic Ancient forge, fractional level, ingredient consumption, tier filters, recipe/power/aura details, equipped reload, fresh round-ten Titan, visible saved warning, live ability cast with reduced motion, phase-two render; no overflow, missing assets or page errors');
      await context.close();
    }
    // A single overview makes every distinct boss silhouette and every equipped
    // Ancient transformation reviewable without manipulating a player's save.
    const gallery=await browser.newContext({viewport:{width:1320,height:1100}}),page=await gallery.newPage();
    await page.goto(origin+'/game/bramblebound/');await page.locator('#title-kit').waitFor();
    const art=await page.evaluate(()=>{
      const panel=document.createElement('section');panel.id='expansion-gallery';panel.style.cssText='position:absolute;z-index:10000;inset:0 auto auto 0;width:1320px;padding:28px;background:#111e21;color:#fff0cf;font-family:Georgia,serif';document.body.append(panel);
      const heading=document.createElement('h1');heading.textContent='BRAMBLEBOUND · THE TITANS';heading.style.cssText='font-size:28px;margin:0 0 18px';panel.append(heading);
      const bosses=document.createElement('div');bosses.id='titan-gallery';bosses.style.cssText='display:grid;grid-template-columns:repeat(3,1fr);gap:14px';panel.append(bosses);
      const hashes=[];
      for(const d of Titans.definitions){
        const card=document.createElement('article');card.style.cssText='border:1px solid #526c68;background:#1f3033;padding:12px';
        const title=document.createElement('h2');title.textContent=d.name;title.style.cssText='font-size:17px;margin:0 0 5px';card.append(title);
        const subtitle=document.createElement('div');subtitle.textContent=d.moves.map(m=>m.name).join(' · ');subtitle.style.cssText='font:10px/1.4 system-ui;color:#bbd9ce;height:29px';card.append(subtitle);
        const canvas=document.createElement('canvas');canvas.style.cssText='display:block;width:100%;height:230px;image-rendering:pixelated';card.append(canvas);bosses.append(card);
        WorldArt.draw(canvas,{time:0,round:10,region:0,phase:'fight',reducedMotion:true,heroX:.19,enemyX:.69,waveSize:1,enemies:[{...d,uid:1,lane:0,titanId:d.id,titan:{phase:2,warning:null},hp:100,maxHp:100,attackProgress:0}],equipped:[],encounterAge:1});hashes.push(canvas.toDataURL());
      }
      const castHeading=document.createElement('h1');castHeading.textContent='TWELVE TITAN POWERS';castHeading.style.cssText='font-size:28px;margin:30px 0 18px';panel.append(castHeading);
      const casts=document.createElement('div');casts.id='titan-cast-gallery';casts.style.cssText='display:grid;grid-template-columns:repeat(3,1fr);gap:14px';panel.append(casts);
      const castHashes=[];
      for(const d of Titans.definitions)for(const [move,m] of d.moves.entries()){
        const card=document.createElement('article');card.style.cssText='border:1px solid #766975;background:#252c38;padding:12px';
        const title=document.createElement('h2');title.textContent=m.name;title.style.cssText='font-size:16px;min-height:22px;margin:0';card.append(title);
        const canvas=document.createElement('canvas');canvas.style.cssText='display:block;width:100%;height:210px;image-rendering:pixelated';card.append(canvas);casts.append(card);
        WorldArt.draw(canvas,{time:0,round:10,region:0,phase:'fight',reducedMotion:true,heroX:.23,enemyX:.73,waveSize:1,enemies:[{...d,uid:1,lane:0,titanId:d.id,titan:{phase:1,warning:null},hp:100,maxHp:100,attackProgress:0}],equipped:[],encounterAge:1,effects:[{kind:'boss',element:d.element,pattern:'titan-cast',abilityPattern:m.pattern,move,targetUid:1,sourceUid:1,lane:0,age:.35,strength:3}]});castHashes.push(canvas.toDataURL());
      }
      const subheading=document.createElement('h1');subheading.textContent='ANCIENT ARTIFACTS · EQUIPPED AURAS';subheading.style.cssText='font-size:28px;margin:30px 0 18px';panel.append(subheading);
      const ancients=document.createElement('div');ancients.id='ancient-gallery';ancients.style.cssText='display:grid;grid-template-columns:repeat(4,1fr);gap:12px';panel.append(ancients);
      const auraHashes=[];
      for(const d of Object.values(Catalog.items).filter(d=>d.ancient)){
        const card=document.createElement('article');card.style.cssText='border:1px solid #608b80;background:#1b3030;padding:10px';
        const title=document.createElement('h2');title.textContent=d.name;title.style.cssText='font-size:15px;min-height:36px;margin:0';card.append(title);
        const canvas=document.createElement('canvas');canvas.style.cssText='display:block;width:100%;height:210px;image-rendering:pixelated';card.append(canvas);ancients.append(card);
        WorldArt.draw(canvas,{time:0,round:1,region:0,phase:'walk',reducedMotion:true,heroX:.5,equipped:[{id:d.id,level:1}],heldId:d.id,heldLevel:1});auraHashes.push(canvas.toDataURL());
      }
      return {bosses:hashes.length,bossUnique:new Set(hashes).size,casts:castHashes.length,castUnique:new Set(castHashes).size,ancients:auraHashes.length,auraUnique:new Set(auraHashes).size};
    });
    assert.equal(art.bosses,6);assert.equal(art.bossUnique,6);assert.equal(art.casts,12);assert.equal(art.castUnique,12);assert.equal(art.ancients,11);assert.equal(art.auraUnique,11);
    if(shots){await page.locator('#titan-gallery').screenshot({path:path.join(shots,'bramblebound-six-titans.png')});await page.locator('#titan-cast-gallery').screenshot({path:path.join(shots,'bramblebound-twelve-titan-powers.png')});await page.locator('#ancient-gallery').screenshot({path:path.join(shots,'bramblebound-ancient-auras.png')});}
    await gallery.close();results.push('All six Titan silhouettes, twelve boss powers and eleven Ancient character transformations render distinct artwork.');
    console.log(results.join('\n'));
  }finally{await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>server.close());
