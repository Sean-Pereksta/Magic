const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const vm=require('node:vm');
const assert=require('node:assert/strict');
const {chromium}=require(process.env.BRAMBLEBOUND_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../../..'),dir=path.resolve(__dirname,'..');
const KEY='bramblebound.profile.v1';
const server=http.createServer((req,res)=>{
  try{let file=path.resolve(root,'.'+decodeURIComponent(new URL(req.url,'http://localhost').pathname));
    if(!file.startsWith(root+path.sep)||file.includes(path.sep+'.git'))throw Error();
    if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
    res.setHeader('Content-Type',({'.html':'text/html','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.webp':'image/webp'})[path.extname(file)]||'application/octet-stream');
    fs.createReadStream(file).on('error',()=>res.end()).pipe(res);
  }catch{res.writeHead(404).end();}
});
function fixture(){
  const c=vm.createContext({console});c.window=c;
  for(const file of ['catalog','legacy','progression','living-combat','engine'])vm.runInContext(fs.readFileSync(path.join(dir,file+'.js'),'utf8'),c);
  const p=c.GameEngine.freshProfile(),L=c.Legacy;p.best=140;p.deaths=5;p.points=50000;
  p.seenItems=Object.keys(c.Catalog.items);p.seenEnemies=c.Catalog.enemies.map(e=>e.id);
  const l=L.ensure(p);l.stats.guardians=35;l.stats.expeditions=5;l.stats.highestRegion=35;L.ensure(p);
  function buy(id,n=1){for(let i=0;i<n;i++)assert.equal(L.purchase(p,id).ok,true,id);}
  buy('treasure_nose',10);buy('guardian_hoard',5);buy('legendary_expedition');
  buy('heirloom_temper',4);buy('second_heirloom',3);buy('starter_satchel',2);buy('mastercraft',3);buy('heirloom_vault');buy('relic_calling');
  buy('guardian_wisdom',10);buy('fate_rerolls',2);buy('destiny_board');buy('expanded_destiny',2);
  for(const el of L.elements)buy('mastery_'+el,3);
  buy('oath_worldroot');buy('oath_phoenix');buy('convergence');
  c.GameEngine.validate(p);
  return JSON.stringify({game:'bramblebound',format:1,profile:p,settings:{audio:false,motion:false}});
}
async function main(){
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const options={headless:true};if(process.env.BRAMBLEBOUND_BROWSER)options.executablePath=process.env.BRAMBLEBOUND_BROWSER;
  const edge='C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe';if(!options.executablePath&&process.platform==='win32'&&fs.existsSync(edge))options.executablePath=edge;
  const browser=await chromium.launch(options),raw=fixture();
  try{
    for(const width of [1440,390]){
      const context=await browser.newContext({viewport:{width,height:1000},reducedMotion:'reduce'}),page=await context.newPage(),errors=[];
      page.on('pageerror',e=>errors.push(e.message));
      await context.addInitScript(({KEY,raw})=>{if(!localStorage.getItem(KEY))localStorage.setItem(KEY,raw);},{KEY,raw});
      await page.goto('http://127.0.0.1:'+server.address().port+'/game/bramblebound/');
      await page.locator('#title-kit').click();await page.locator('nav [data-nav="legacy"]').click();
      await page.getByRole('heading',{name:'Camp of Echoes',exact:true}).waitFor();
      assert.equal(await page.locator('.echo-district').count(),6);
      if(process.env.BRAMBLEBOUND_SCREENSHOTS){fs.mkdirSync(process.env.BRAMBLEBOUND_SCREENSHOTS,{recursive:true});await page.screenshot({path:path.join(process.env.BRAMBLEBOUND_SCREENSHOTS,'camp-top-'+width+'.png')});}
      await page.locator('[data-camp-buy="heart"]').click();
      await page.waitForFunction(KEY=>JSON.parse(localStorage.getItem(KEY)).profile.perks.heart===1,KEY);
      for(const area of ['heartwood','wayfinder','forge','fate','grove','mythic']){
        await page.locator('[data-camp-area="'+area+'"]').click();
        assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,area+' overflows '+width);
        assert.equal(await page.locator('[data-upgrade]').evaluateAll(nodes=>new Set(nodes.map(n=>n.dataset.upgrade)).size===nodes.length),true,'duplicate upgrade cards');
      }
      await page.locator('[data-camp-oath="phoenix"]').click();
      await page.waitForFunction(KEY=>JSON.parse(localStorage.getItem(KEY)).profile.legacy.activeOaths.length===2,KEY);
      await page.locator('[data-camp-area="grove"]').click();
      assert.equal(await page.locator('[data-camp-path="mastery_leaf"]').evaluate(el=>el.open),true);await page.locator('[data-camp-path="mastery_fire"] > summary').click();assert.equal(await page.locator('[data-camp-path="mastery_fire"]').evaluate(el=>el.open),true);
      await page.locator('[data-camp-grove-view="constellations"]').click();
      assert.equal(await page.locator('[data-upgrade]').filter({has:page.locator('[data-camp-buy="ashroot"]')}).count(),1);
      await page.locator('[data-camp-buy="ashroot"]').click();
      await page.locator('#camp-config-form select[name="favoredElement"]').selectOption('fire');
      await page.locator('#camp-config-form select[name="style"]').selectOption('rapid');
      await page.locator('#camp-config-form select[name="build"]').selectOption({label:'Fire and Explosions'});
      await page.locator('#camp-config-form button[type="submit"]').click();
      const kit=page.locator('[data-camp-kit-form="0"]');
      await kit.locator('[name="kitName"]').fill('Firestorm test');await kit.locator('[name="item0"]').selectOption('stick');await kit.locator('[name="item1"]').selectOption('torch');
      await kit.locator('button[type="submit"]').click();await page.locator('[data-camp-select-kit="0"]').click();
      if(process.env.BRAMBLEBOUND_SCREENSHOTS){fs.mkdirSync(process.env.BRAMBLEBOUND_SCREENSHOTS,{recursive:true});await page.screenshot({path:path.join(process.env.BRAMBLEBOUND_SCREENSHOTS,'camp-'+width+'.png'),fullPage:true});}
      await page.reload();await page.locator('#title-kit').click();await page.locator('nav [data-nav="legacy"]').click();
      const saved=await page.evaluate(KEY=>JSON.parse(localStorage.getItem(KEY)).profile,KEY);
      assert.equal(saved.legacy.selectedKit,0);assert.equal(saved.legacy.config.favoredElement,'fire');assert.equal(saved.legacy.constellations.includes('ashroot'),true);
      await page.locator('.echo-camp-footer [data-do="start"]').click();
      const started=await page.evaluate(KEY=>JSON.parse(localStorage.getItem(KEY)).profile,KEY);
      assert.equal(started.run.maxHp,102);assert.equal(started.run.active[0].id,'stick');assert.equal(started.run.active[0].level,5);assert.equal(started.run.active[1].id,'torch');assert.equal(started.run.active[1].level,4);
      assert.equal(started.run.legacy.oaths.length,2);assert.ok(started.run.living.companions.length>0);
      await page.locator('nav [data-nav="legacy"]').click();
      assert.equal(await page.locator('[data-camp-buy="heart"]').isDisabled(),true);
      assert.equal(await page.locator('[data-camp-respec]').isDisabled(),true);
      const stress=await page.evaluate(()=>{
        const E=GameEngine,P=Progression,L=Legacy,p=E.freshProfile();p.best=140;p.deaths=5;p.perks.heart=100;
        L.ensure(p);E.start(p);
        for(const id of Object.keys(P.upgrades))P.apply(p.run,id);
        p.run.level=1+Object.values(p.run.skills).reduce((a,b)=>a+b,0);
        const begin=performance.now();let steps=0,checkpoints=0;
        for(;steps<1200&&p.run.phase!=='dead';steps++){
          if(p.run.draft.length)E.act(p,'chooseUpgrade',p.run.draft[0]);
          E.tick(p,.05);
          if(steps%30===0){E.validate(JSON.stringify(p));checkpoints++;}
          if((p.run.living?.companions.length||0)>24)throw Error('Companion budget exceeded');
        }
        const r=p.run,canvas=document.createElement('canvas');canvas.style.width='390px';canvas.style.height='200px';
        const ally=r.living.companions[0]||{id:'phantom',element:'spirit',timer:1};
        for(let i=0;i<60;i++)WorldArt.draw(canvas,{time:i/30,distance:i,region:r.region,round:r.round,phase:'fight',heroShield:r.shield,enemies:r.enemies,waveSize:r.waveSize,effects:[],companions:Array.from({length:24},()=>({...ally})),equipped:r.active,heroAilments:r.ailments});
        return {steps,checkpoints,elapsed:performance.now()-begin};
      });
      assert.ok(stress.checkpoints>=2,'stress run should exercise saved combat states');
      assert.ok(stress.elapsed<10000,'busy build must remain responsive: '+JSON.stringify(stress));
      assert.deepEqual(errors,[]);console.log(width+'px: six camp areas, purchases, constellations, Oaths, preferences, kits, reload, engine benefits, active-run guards');
      console.log(width+'px stress: '+stress.steps+' simulation steps, '+stress.checkpoints+' valid checkpoints, 60 crowded renders in '+Math.round(stress.elapsed)+'ms');
      await context.close();
    }
  }finally{await browser.close();}
}
main().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>server.close());
