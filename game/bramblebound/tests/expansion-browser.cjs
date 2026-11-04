const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.BRAMBLEBOUND_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../../..');
const shots=process.env.BRAMBLEBOUND_SCREENSHOTS;
const mime={'.html':'text/html','.js':'text/javascript','.mjs':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp','.json':'application/json'};
const server=http.createServer((req,res)=>{
  try{
    let file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));
    if(!file.startsWith(root+path.sep)||file.includes(path.sep+'.git'))throw Error('Outside fixture');
    if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
    res.setHeader('Content-Type',mime[path.extname(file)]||'application/octet-stream');
    fs.createReadStream(file).on('error',()=>res.end()).pipe(res);
  }catch{res.writeHead(404).end();}
});
const canvasPainted=canvas=>{
  const bytes=canvas.getContext('2d').getImageData(0,0,canvas.width,canvas.height).data;
  let opaque=0;const colors=new Set();
  for(let i=0;i<bytes.length;i+=4){if(bytes[i+3])opaque++;colors.add(bytes[i]+','+bytes[i+1]+','+bytes[i+2]);}
  return {opaque,colors:colors.size,width:canvas.width,height:canvas.height};
};
async function main(){
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+server.address().port,options={headless:true};
  const edge='C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';
  if(process.platform==='win32'&&fs.existsSync(edge))options.executablePath=edge;
  const browser=await chromium.launch(options),results=[];
  try{
    for(const spec of [{width:1440,height:1050,round:81,motion:true},{width:390,height:844,round:129,motion:false}]){
      const context=await browser.newContext({viewport:{width:spec.width,height:spec.height},reducedMotion:spec.motion?'no-preference':'reduce'});
      context.setDefaultTimeout(12000);context.setDefaultNavigationTimeout(20000);
      const page=await context.newPage(),errors=[],missing=[];
      page.on('pageerror',e=>errors.push(e.message));page.on('response',r=>{if(r.status()>=400)missing.push(r.status()+' '+r.url());});
      await page.goto(origin+'/game/bramblebound/');await page.locator('#title-play').waitFor();
      const fixture=await page.evaluate(({round,motion})=>{
        const E=GameEngine,C=Catalog,p=E.freshProfile();p.perks.heart=100;p.best=round-1;E.start(p);
        const r=p.run;r.round=round;r.region=E.routeInfo(r,round).region;r.kills=round-1;r.foes=r.kills;r.bestAtStart=p.best;
        const weapons=Object.values(C.items).filter(d=>d.dropRegion>=20&&d.special&&d.counter);
        const chosen=[...new Set([weapons[0],...weapons.filter(d=>d.element!==weapons[0].element)])].slice(0,4);
        if(chosen.length!==4)throw Error('Four distinct expansion weapons required');
        r.active=chosen.map((d,i)=>({uid:i+1,id:d.id,level:20,cd:i*.07,slot:i}));
        r.pack=Object.keys(C.rarities).map((tier,i)=>({uid:i+5,id:Object.values(C.items).find(d=>d.rarity===tier&&!d.recipe).id,level:20,cd:0,slot:i}));r.nextUid=10;
        p.seenItems=[...new Set([...chosen.map(d=>d.id),...r.pack.map(it=>it.id)])];r.walkLeft=.001;E.tick(p,.002);
        const normalized=E.validate(JSON.parse(JSON.stringify(p)));
        return {save:{game:'bramblebound',format:1,profile:normalized,settings:{audio:false,motion}},itemId:chosen[0].id,itemName:chosen[0].name,region:C.regions[r.region].name};
      },spec);
      await context.addInitScript(save=>localStorage.setItem('bramblebound.profile.v1',JSON.stringify(save)),fixture.save);
      await page.reload();await page.locator('#title-play').waitFor();
      assert.match(await page.locator('#title-play').innerText(),/Continue expedition/);assert.ok((await page.locator('#title-record').innerText()).includes(fixture.region));
      await page.locator('#title-play').click();await page.waitForTimeout(220);await page.locator('#run-btn').click();
      assert.match(await page.locator('#run-btn').innerText(),/Resume/);assert.equal(await page.locator('#region-name').innerText(),fixture.region);
      assert.equal(await page.locator('#equipped .gear-slot[data-uid]').count(),4);
      const art=await page.locator('#world').evaluate(canvasPainted);assert.ok(art.opaque>art.width*art.height*.8);assert.ok(art.colors>50,'expanded scene and held weapons must paint actual artwork');
      if(!spec.motion)assert.equal(await page.locator('body').evaluate(el=>el.classList.contains('reduced-art')),true);
      if(shots){fs.mkdirSync(shots,{recursive:true});await page.screenshot({path:path.join(shots,'expansion-combat-'+spec.width+'.png'),fullPage:true});}
      const tiers=['common','uncommon','rare','epic','mythic'];
      assert.deepEqual(await page.locator('#pack-tier option').evaluateAll(options=>options.map(o=>o.value)),['all',...tiers]);
      for(const tier of tiers){await page.locator('#pack-tier').selectOption(tier);assert.equal(await page.locator('#pack .gear-slot[data-uid]').count(),1);assert.equal(await page.locator('#pack .gear-slot[data-uid]').getAttribute('data-rarity'),tier);}
      await page.locator('#pack-tier').selectOption('all');assert.equal(await page.locator('#pack .gear-slot[data-uid]').count(),5);
      await page.locator('#equipped .gear-slot[data-uid="1"]').click();await page.locator('[data-selection="details"]').click();
      const details=await page.locator('#dialog-body').innerText();assert.ok(details.includes(fixture.itemName));assert.match(details,/Element ·/i);assert.match(details,/Signature ·/i);assert.match(details,/Counter ·/i);assert.match(details,/Rarity power ×/i);
      await page.locator('#close-btn').click();
      await page.locator('[data-dialog="route"]').click();await page.locator('#modal[open] .atlas-grid').waitFor();assert.equal(await page.locator('#modal .atlas-stop').count(),35);
      assert.equal(await page.locator('#modal .rarity-odds tbody tr').count(),5);assert.deepEqual(await page.locator('#modal .rarity-odds tbody tr th').allTextContents(),['Common','Uncommon','Rare','Epic','Mythic']);
      await page.locator('#modal .atlas-stop[data-region="34"]').click();assert.equal(await page.locator('#modal .dialog-title').innerText(),'Everdawn Observatory');
      const regionArt=await page.locator('#region-art').evaluate(canvasPainted);assert.ok(regionArt.colors>30);assert.ok(regionArt.opaque>regionArt.width*regionArt.height*.8);
      await page.locator('#modal [data-species]').first().click();assert.match(await page.locator('#dialog-body').innerText(),/Enemy effect ·/i);assert.match(await page.locator('#dialog-body').innerText(),/Counter/i);
      await page.locator('#close-btn').click();
      await page.locator('nav [data-nav="guide"]').click();
      assert.equal(await page.locator('#guide-page [data-guide-tier]').count(),6);
      for(const tier of tiers){
        await page.locator('#guide-page [data-guide-tier="'+tier+'"]').click();const tierCards=page.locator('#guide-page .guide-card[data-catalog]');await tierCards.first().waitFor();
        assert.equal(await tierCards.evaluateAll((nodes,tier)=>nodes.every(n=>n.dataset.rarity===tier),tier),true,'field guide '+tier+' filter');
      }
      await page.locator('#guide-page [data-guide-tier="all"]').click();await page.locator('#guide-search').fill('wardbreaker');
      const matches=page.locator('#guide-page .guide-card[data-catalog]');await matches.first().waitFor();
      const ids=await matches.evaluateAll(nodes=>nodes.map(n=>n.dataset.catalog));assert.ok(ids.length>=2);
      assert.equal(await page.evaluate(ids=>ids.every(id=>Catalog.items[id].counter==='wardbreaker'),ids),true,'counter search must return appropriate equipment');
      await matches.first().click();assert.match(await page.locator('#dialog-body').innerText(),/Counter · wardbreaker/i);await page.locator('#close-btn').click();
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
      assert.deepEqual(errors,[]);assert.deepEqual(missing,[]);
      results.push(spec.width+'px: saved late-region encounter, four held expansion relics, rendered combat'+(!spec.motion?' with reduced motion':'')+', item effects, 35-stop atlas, five-tier rarity table and working backpack/guide filters, final-region art, enemy counters, searchable counter gear; no errors or missing assets');
      await context.close();
    }
    for(const width of [1440,390]){
      const context=await browser.newContext({viewport:{width,height:width===390?844:1050}}),page=await context.newPage(),errors=[];
      context.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
      await page.goto(origin+'/game/bramblebound/');await page.locator('#title-play').waitFor();
      const fixture=await page.evaluate(()=>{
        const E=GameEngine,C=Catalog,p=E.freshProfile();E.start(p);const r=p.run;
        const utility=['heal','shield','stun','freeze'].map(style=>Object.values(C.items).find(d=>d.style===style&&!d.recipe));
        const final=Object.values(C.items).find(d=>d.style==='heal'&&d.fusionDepth===2);
        const intermediate=C.items[final.recipe.find(id=>C.items[id].recipe)],partner=final.recipe.find(id=>id!==intermediate.id);
        r.active=utility.map((d,i)=>({uid:i+1,id:d.id,level:10,cd:0,slot:i}));
        r.pack=[{uid:5,id:intermediate.recipe[0],level:2.5,cd:0,slot:0},{uid:6,id:intermediate.recipe[1],level:7.25,cd:0,slot:1},{uid:7,id:partner,level:20.5,cd:0,slot:2}];r.nextUid=8;
        p.seenItems=[...new Set([...r.active,...r.pack].map(it=>it.id))];
        return {save:{game:'bramblebound',format:1,profile:E.validate(JSON.parse(JSON.stringify(p))),settings:{audio:false,motion:false}},intermediate:intermediate.id,final:final.id};
      });
      await context.addInitScript(save=>localStorage.setItem('bramblebound.profile.v1',JSON.stringify(save)),fixture.save);
      await page.reload();await page.locator('#title-play').waitFor();await page.locator('#title-play').click();await page.locator('#run-btn').click();
      for(const [uid,stat] of [[1,/healing power/i],[2,/shield power/i],[3,/stun duration/i],[4,/freeze duration/i]]){
        await page.locator('#equipped .gear-slot[data-uid="'+uid+'"]').click();await page.locator('[data-selection="details"]').click();
        const details=await page.locator('#dialog-body').innerText();assert.match(details,stat);assert.doesNotMatch(details,/base damage/i);assert.match(details,/deals no damage/i);
        if(shots&&uid===4){fs.mkdirSync(shots,{recursive:true});await page.screenshot({path:path.join(shots,'utility-freeze-details-'+width+'.png'),fullPage:true});}
        await page.locator('#close-btn').click();
      }
      const forge=async(uid,partner,level)=>{
        // Forging selects its result; clear that selection before starting the
        // next details flow so tapping it does not toggle it off.
        const clear=page.locator('[data-selection="clear"]');if(await clear.isVisible())await clear.click();
        await page.locator('#pack .gear-slot[data-uid="'+uid+'"]').click();await page.locator('[data-selection="details"]').click();
        await page.locator('#dialog-body [data-gear="combine"]').click();await page.locator('[data-pair-with="'+partner+'"]').click();
        assert.equal(await page.locator('#dialog-body .level-tag').innerText(),'LEVEL '+level);assert.doesNotMatch(await page.locator('#dialog-body').innerText(),/base damage/i);
        if(shots&&level===21.5){fs.mkdirSync(shots,{recursive:true});await page.screenshot({path:path.join(shots,'utility-second-stage-preview-'+width+'.png'),fullPage:true});}
        await page.locator('[data-do="forge"]').click();await page.locator('#modal').waitFor({state:'hidden'});
      };
      await forge(5,6,8.25);await page.locator('#pack .gear-slot[data-uid="8"]').waitFor();await forge(8,7,21.5);
      const crafted=await page.evaluate(()=>JSON.parse(localStorage.getItem('bramblebound.profile.v1')).profile.run.pack);
      assert.equal(crafted.length,1);assert.equal(crafted[0].id,fixture.final);assert.equal(crafted[0].level,21.5);
      await page.locator('nav [data-nav="guide"]').click();
      for(const [role,style] of [['Healing','heal'],['Shield','shield'],['Stun','stun'],['Freeze','freeze']]){
        await page.locator('#guide-page [data-gear-role="'+role+'"]').click();
        const cards=page.locator('#guide-page .guide-card[data-catalog]');await cards.first().waitFor();
        const ids=await cards.evaluateAll(nodes=>nodes.map(n=>n.dataset.catalog));
        assert.equal(await page.evaluate(({ids,style})=>ids.some(id=>Catalog.items[id].style===style),{ids,style}),true,role+' role must expose pure utility gear');
      }
      await page.locator('[data-guide-tab="recipes"]').click();
      assert.match(await page.locator('#guide-page').innerText(),/one level above its highest-level ingredient/i);
      assert.match(await page.locator('#guide-page').innerText(),/second-stage fusion/i);
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);assert.deepEqual(errors,[]);
      results.push(width+'px: pure utility labels, four role filters, fractional first/second-stage forge previews and crafted level 21.5 save; no page errors');await context.close();
    }
    console.log(results.join('\n'));
  }finally{await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>server.close());
