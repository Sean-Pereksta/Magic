const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const game=path.resolve(__dirname,'../..');
const exposure=`window.tcMaps={menu(){resize();renderStartMenu();overlay.style.display='flex'},render:renderStartMenu,start(){resize();createGame(buildSoloSlots())},get config(){return{map:chosenMapId,definition:mapDefinition(),layout:mapLayout(),saved:localStorage.getItem('csf.map')}},get state(){return{map:chosenMapId,starts:startPositions(),bases:entities.filter(isBase).map(e=>({x:e.x,y:e.y})),conquest:modeState?.conquest,relic:modeState?.relic,terrain:encodeTerrain(),snapshot:serializeState()}}};window.tcMaps.menu();`;
const source=fs.readFileSync(process.env.TURNCRAFT_SOURCE||path.join(game,'turncraft.html'),'utf8').replace(/^import .*;$/gm,'').replace('\nboot();','\n'+exposure).replace('<script type="module">','<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
async function setup(width=1280,height=900,savedMap){
  const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_EXECUTABLE_PATH||undefined,args:['--no-sandbox']});
  const page=await browser.newPage({viewport:{width,height}}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  if(savedMap!==undefined)await page.addInitScript(map=>localStorage.setItem('csf.map',map),savedMap);
  await page.route('**/*',route=>{const url=new URL(route.request().url());if(url.pathname==='/game/turncraft.html')return route.fulfill({contentType:'text/html',body:source});const file=path.join(game,url.pathname.replace('/game/',''));if(file.startsWith(game+path.sep)&&fs.existsSync(file)&&fs.statSync(file).isFile())return route.fulfill({path:file});return route.fulfill({status:404,body:''})});
  await page.goto('http://turncraft.test/game/turncraft.html');assert.deepEqual(errors,[]);return{browser,page,errors};
}

test('six illustrated map choices show actual terrain previews and persist a selected map',async()=>{
  const {browser,page,errors}=await setup();try{
    const choices=page.locator('#mapChoices [data-map]');assert.equal(await choices.count(),6);assert.equal(await page.locator('#mapPopout').isVisible(),false);await page.locator('#mapSelectBtn').click();
    const previews=await choices.locator('canvas').evaluateAll(canvases=>canvases.map(canvas=>{const ctx=canvas.getContext('2d'),data=ctx.getImageData(0,0,canvas.width,canvas.height).data;let colors=new Set();for(let i=0;i<data.length;i+=4)if(data[i+3])colors.add(data[i]+','+data[i+1]+','+data[i+2]);return{width:canvas.width,height:canvas.height,colors:colors.size}}));
    assert.equal(previews.length,6);assert.ok(previews.every(p=>p.width>=42&&p.height>=30&&p.colors>=5),JSON.stringify(previews));
    for(const id of ['classic','ember-rift','frostline-basin','verdant-divide','shattered-dunes','orbital-foundry']){
      if(!await page.locator('#mapPopout').isVisible())await page.locator('#mapSelectBtn').click();await page.locator(`[data-map="${id}"]`).click();await page.locator('#mapPopout').waitFor({state:'hidden'});const config=await page.evaluate(()=>tcMaps.config);assert.equal(config.map,id);assert.equal(config.saved,id);assert.equal(config.layout.id,id);assert.equal(await page.locator(`[data-map="${id}"]`).getAttribute('aria-pressed'),'true');assert.ok((await page.locator('#mapSummary').textContent()).trim().length>20);
    }
    await page.reload();assert.equal(await page.evaluate(()=>tcMaps.config.map),'orbital-foundry');assert.equal(await page.locator('[data-map="orbital-foundry"]').getAttribute('aria-pressed'),'true');assert.deepEqual(errors,[]);
  }finally{await browser.close()}
});

test('saved unknown maps fall back to Classic while valid saved choices remain selected',async()=>{
  for(const [saved,expected]of [['missing-map','classic'],['frostline-basin','frostline-basin']]){
    const {browser,page,errors}=await setup(1280,900,saved);try{assert.equal(await page.evaluate(()=>tcMaps.config.map),expected);assert.equal(await page.locator(`[data-map="${expected}"]`).getAttribute('aria-pressed'),'true');assert.deepEqual(errors,[])}finally{await browser.close()}
  }
});

test('deploying each map uses the selected map in the live match and shared snapshot',async()=>{
  const {browser,page,errors}=await setup();try{
    for(const id of ['classic','ember-rift','frostline-basin','verdant-divide','shattered-dunes','orbital-foundry']){
      await page.reload();await page.locator('#mapSelectBtn').click();await page.locator(`[data-map="${id}"]`).click();await page.locator('#mapPopout').waitFor({state:'hidden'});const result=await page.evaluate(()=>{tcMaps.start();return tcMaps.state});assert.equal(result.map,id);assert.equal(result.snapshot.chosenMapId,id);assert.deepEqual(result.bases,result.starts.slice(0,result.bases.length));assert.equal(result.terrain.length,42*30);assert.ok(result.snapshot.resourceNodes.length>=50);
    }
    assert.deepEqual(errors,[]);
  }finally{await browser.close()}
});

test('map choices remain visible, accessible and tappable in portrait and landscape menus',async()=>{
  for(const [width,height]of [[390,844],[844,390],[1280,900]]){
    const {browser,page,errors}=await setup(width,height);try{
      for(const id of ['ember-rift','orbital-foundry']){
        await page.locator('#mapSelectBtn').click();
        const choice=page.locator(`#mapPopout .mapChoice[data-map="${id}"]`);
        await choice.waitFor({state:'visible'});await choice.scrollIntoViewIfNeeded();
        const result=await choice.evaluate(el=>{const r=el.getBoundingClientRect();return{w:r.width,h:r.height,label:el.getAttribute('aria-label')||el.textContent}});
        assert.ok(result.w>=44&&result.h>=44,`${width}×${height} ${id}: ${JSON.stringify(result)}`);
        assert.ok(result.label.trim().length>5);
        await choice.click();await page.locator('#mapPopout').waitFor({state:'hidden'});
        assert.equal(await choice.getAttribute('aria-pressed'),'true');
      }
      const overflow=await page.locator('#startCard').evaluate(el=>el.scrollWidth-el.clientWidth);assert.ok(overflow<=2,`${width} menu overflow: ${overflow}`);assert.deepEqual(errors,[]);
    }finally{await browser.close()}
  }
});
