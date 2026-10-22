const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),origin='http://127.0.0.1:7777',cdn='https://monkey-art.example.test/monkey-trails/wild-companions-v3/';
const mime={'.html':'text/html','.mjs':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};
const scenarios=[
 {name:'CDN success',outcome:'remote',expected:'remote'},
 {name:'CDN 404 uses committed local asset',outcome:'local',expected:'local'},
 {name:'CDN and local 404 preserve inline art',outcome:'inline',expected:'inline'},
 {name:'reduced-motion option preserves inline art',outcome:'remote',reduced:true,expected:'skip'},
 {name:'reduced-motion media preference preserves inline art',outcome:'remote',media:true,expected:'skip'},
 {name:'runtime reduced-motion class preserves inline art',outcome:'remote',runtimeClass:'reduce-motion',expected:'skip'},
 {name:'portrait reduced-motion flag preserves inline art',outcome:'remote',portraitReduced:true,expected:'skip'},
 {name:'native evolution loads its packaged variant',outcome:'remote',species:'rillhopper',types:['water'],stage:1,expected:'remote'},
 {name:'different starter evolution keeps its correct inline art',outcome:'remote',species:'capuchin',types:['fire'],stage:1,expected:'skip'},
 {name:'new hybrid evolution keeps its correct inline art',outcome:'remote',species:'rillhopper',types:['water','light'],stage:2,expected:'skip'}
];

(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
 try{for(const scenario of scenarios){
  const context=await browser.newContext({viewport:{width:390,height:844},reducedMotion:scenario.media?'reduce':'no-preference'}),requests=[],errors=[];
  await context.route('**/*',async route=>{
   const url=new URL(route.request().url());
   if(url.hostname==='monkey-art.example.test'){
    requests.push({kind:'remote',url:url.href});
    if(scenario.outcome!=='remote'){await route.fulfill({status:404,body:'Missing CDN asset'});return;}
    const file=path.resolve(__dirname,'assets',decodeURIComponent(url.pathname.slice('/monkey-trails/wild-companions-v3/'.length)));
    assert.ok(file.startsWith(path.resolve(__dirname,'assets')+path.sep));assert.ok(fs.existsSync(file),`missing committed CDN fixture: ${file}`);
    await route.fulfill({contentType:'image/svg+xml',body:fs.readFileSync(file)});return;
   }
   const file=path.resolve(root,'.'+decodeURIComponent(url.pathname));
   if(url.pathname.includes('/assets/monkeys/')){
    requests.push({kind:'local',url:url.href});
    if(scenario.outcome==='inline'){await route.fulfill({status:404,body:'Missing local asset'});return;}
   }
   if(url.origin!==origin||!file.startsWith(root+path.sep)||!fs.existsSync(file)){await route.abort();return;}
   await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});
  });
  const page=await context.newPage();page.on('pageerror',error=>errors.push(error.message));
  await page.goto(`${origin}/game/monkey-trails.html`);await page.locator('#practice-button').waitFor();
  const before=await page.evaluate(async({cdn,scenario})=>{
   const A=await import('/game/monkey-trails/art.mjs');
   const host=document.createElement('section');host.id='hosted-art-fixture';host.className=scenario.runtimeClass||'';document.body.append(host);
   const p={species:scenario.species||'acornbuckler',types:scenario.types||['leaf'],stage:scenario.stage||0};
   host.innerHTML=A.portrait(p,{state:'idle',reducedMotion:!!scenario.portraitReduced});
   // These remain real native browser images. Event capture only makes both
   // failed requests observable without timers or a fake image implementation.
   const NativeImage=window.Image;window.__hostedImageEvents=[];
   window.Image=function(...args){const img=new NativeImage(...args);for(const type of ['load','error'])img.addEventListener(type,()=>window.__hostedImageEvents.push({type,src:img.src}));return img;};
   window.Image.prototype=NativeImage.prototype;
   globalThis.MONKEY_TRAILS_ASSET_BASE=cdn;const before=host.innerHTML;
   A.hydrateHostedPortraits(host,{reducedMotion:!!scenario.reduced});return before;
  },{cdn,scenario});
  const fixture=page.locator('#hosted-art-fixture');
  if(['remote','local'].includes(scenario.expected)){
   await fixture.locator('img.monkey-art').waitFor();
   await page.waitForFunction(()=>document.querySelector('#hosted-art-fixture img')?.complete&&document.querySelector('#hosted-art-fixture img')?.naturalWidth>0);
   const result=await fixture.locator('img').evaluate(img=>({src:img.src,width:img.width,naturalWidth:img.naturalWidth,types:img.dataset.types,stage:img.dataset.stage}));
   const relative=scenario.stage?'monkeys/rillhopper/variants/attuned.svg':'monkeys/acornbuckler/idle.svg';
   assert.equal(result.src,scenario.expected==='remote'?cdn+relative:`${origin}/game/monkey-trails/assets/${relative}`,scenario.name);
   assert.equal(result.width,240);assert.ok(result.naturalWidth>0);assert.equal(result.types,JSON.stringify(scenario.types||['leaf']));assert.equal(result.stage,String(scenario.stage||0));
   assert.deepEqual(requests.map(r=>r.kind),scenario.expected==='remote'?['remote']:['remote','local']);
  }else if(scenario.expected==='inline'){
   await page.waitForFunction(()=>window.__hostedImageEvents.filter(event=>event.type==='error').length===2);
   assert.deepEqual(requests.map(r=>r.kind),['remote','local']);
   assert.equal(requests[1].url,`${origin}/game/monkey-trails/assets/monkeys/acornbuckler/idle.svg`);
   assert.equal(await fixture.locator('svg.monkey-art').count(),1);assert.equal(await fixture.locator('img').count(),0);
   assert.equal(await fixture.locator('svg').getAttribute('data-hosted-requested'),'true');
  }else{
   assert.deepEqual(requests,[],scenario.name);assert.equal(await fixture.locator('img').count(),0);assert.equal(await fixture.innerHTML(),before,scenario.name);
  }
  assert.deepEqual(errors,[],scenario.name);report.push({scenario:scenario.name,requests:requests.map(r=>r.kind),result:scenario.expected});await context.close();
 }}finally{await browser.close();}
 console.log(JSON.stringify(report,null,2));
})().catch(error=>{console.error(error);process.exitCode=1;});
