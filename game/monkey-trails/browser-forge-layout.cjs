// The forge is a workspace: only the inventory contents scroll while the
// furnace, odds summary and inventory frame stay reachable in the viewport.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),url='http://127.0.0.1:7777/game/monkey-trails.html';
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json'};

async function geometry(page){return page.evaluate(()=>{
  const rect=selector=>{const box=document.querySelector(selector)?.getBoundingClientRect();return box?{x:box.x,y:box.y,width:box.width,height:box.height,bottom:box.bottom,right:box.right}:null;};
  const grid=document.querySelector('.forge-inventory-scroll'),nav=rect('nav.tabs');
  return {width:innerWidth,height:innerHeight,pageY:scrollY,pageWidth:document.documentElement.scrollWidth,nav,furnace:rect('.forge-furnace-column'),drop:rect('[data-forge-drop]'),inventory:rect('.forge-inventory-scroll'),odds:rect('[data-forge-preview]'),scrollTop:grid?.scrollTop,scrollHeight:grid?.scrollHeight,clientHeight:grid?.clientHeight};
});}
function assertVisible(box,state,label){
  assert.ok(box,label+' exists');const bottom=Math.min(state.height,state.nav?.y??state.height);
  assert.ok(box.x>=-1&&box.right<=state.width+1,label+' fits horizontally');
  assert.ok(box.y>=-1&&box.bottom<=bottom+1,label+' remains wholly visible above navigation: '+JSON.stringify({box,bottom}));
}
function assertStationary(before,after,label){
  for(const key of ['x','y','width','height'])assert.ok(Math.abs(before[key]-after[key])<=1,label+' '+key+' stays fixed while inventory scrolls');
}
async function seed(page){await page.evaluate(async()=>{
  const C=await import('/game/monkey-trails/core.mjs'),{Vault}=await import('/game/monkey-trails/storage.mjs'),vault=new Vault(()=>{}),s=await vault.open('Trail Practice','practice-only-not-ranked',{create:true,species:'capuchin',mode:'practice'});
  s.run.fruit=200;for(const id of Object.keys(C.ELEMENTS))s.run.essence[id]=8;for(const id of Object.keys(C.GEAR))s.meta.gear[id]=1;for(const id of Object.keys(C.ITEMS))s.run.items[id]=8;s.run.items.charmMaterial=8;
  await vault.checkpoint(s,true);
});}
async function scrollInventory(page,width){
  const frame=page.locator('.forge-inventory-scroll'),box=await frame.boundingBox();
  await frame.hover({position:{x:Math.min(40,box.width/2),y:Math.min(40,box.height/2)}});
  if(width<700){
    const cdp=await page.context().newCDPSession(page);
    const x=Math.round(box.x+box.width/2),y=Math.round(box.y+box.height*.85),distance=Math.min(230,Math.round(box.height*.65));
    await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x,y,id:1}]});
    for(let step=1;step<=8;step++){await page.waitForTimeout(20);await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x,y:Math.round(y-distance*step/8),id:1}]});}
    await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});
    await cdp.detach();
  }else await page.mouse.wheel(0,540);
  try{await page.waitForFunction(()=>document.querySelector('.forge-inventory-scroll')?.scrollTop>60,{},{timeout:3000});}catch(error){throw Error('Native inventory scroll failed: '+JSON.stringify(await geometry(page)));}
}
async function check(page,width,height){
  await page.goto(url);await page.locator('#practice-button').waitFor();await seed(page);await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
  await page.locator('[data-action="tab"][data-id="pack"]').click();await page.locator('[data-action="pack-section"][data-id="forge"]').click();await page.locator('[data-forge-panel]').waitFor();
  const before=await geometry(page),captureDir=path.resolve(root,'../qa'),captureName='forge-workspace-'+width+(height===844?'':'x'+height)+'.png';fs.mkdirSync(captureDir,{recursive:true});await page.screenshot({path:path.join(captureDir,captureName),fullPage:false});assertVisible(before.furnace,before,'Furnace and Smelt');assertVisible(before.inventory,before,'Inventory viewport');assertVisible(before.odds,before,'Condensed odds');
  const minimum=Math.min(width<700?240:300,Math.max(120,height-500));assert.ok(before.inventory.height>=minimum,'Inventory receives usable screen space (at least '+minimum+'px): '+JSON.stringify(before));assert.ok(before.scrollHeight>before.clientHeight+100,'Inventory has enough contents to test scrolling');
  assert.ok(before.odds.height<=(width<700?240:250),'Outcomes use a condensed summary: '+before.odds.height+'px; '+JSON.stringify(before));assert.ok(before.pageWidth<=before.width,'Workspace has no horizontal overflow');
  await scrollInventory(page,width);const after=await geometry(page);assertStationary(before.furnace,after.furnace,'Furnace');assertStationary(before.inventory,after.inventory,'Inventory viewport');assertStationary(before.odds,after.odds,'Odds summary');assert.equal(after.pageY,before.pageY,'Inner inventory scroll does not move the page');
  // Search and a canceled quantity picker leave the same workspace in place.
  await page.locator('[data-forge-search]').fill('Fire essence');const fire=page.locator('[data-forge-item="essence:fire"]'),value=fire.locator('.forge-tile-value');assert.equal(await value.innerText(),'2 power each','Crafting value appears directly on the ingredient tile');
  const textBounds=el=>{const range=document.createRange();range.selectNodeContents(el);const r=range.getBoundingClientRect();return{x:r.x,y:r.y,width:r.width,height:r.height};},valueBox=await value.evaluate(textBounds),nameBox=await fire.locator('.forge-tile-name').boundingBox(),itemBox=await fire.boundingBox(),quantityBox=await fire.locator('.forge-tile-quantity').evaluate(textBounds);assert.ok(valueBox.y>=nameBox.y+nameBox.height-1,'Ingredient value sits below its name');assert.ok(valueBox.y+valueBox.height<=itemBox.y+itemBox.height,'Ingredient value fits inside the tile');assert.ok(valueBox.y+valueBox.height<=quantityBox.y+1||valueBox.x+valueBox.width<=quantityBox.x+1||valueBox.x>=quantityBox.x+quantityBox.width-1,'Value and quantity text do not overlap: '+JSON.stringify({valueBox,quantityBox}));
  await fire.click();await page.locator('[data-forge-quantity-dialog]').waitFor();assert.equal(await page.locator('[data-forge-picker-range]').getAttribute('max'),'8');await page.locator('[data-forge-picker-cancel]').click();
  await page.locator('[data-forge-search]').fill('');const canceled=await geometry(page);assertStationary(before.furnace,canceled.furnace,'Furnace after Cancel');assertStationary(before.inventory,canceled.inventory,'Inventory after Cancel');assert.equal(await page.locator('[data-forge-edit]').count(),0,'Cancel does not reserve or consume ingredients');
  await page.screenshot({path:path.join(captureDir,captureName),fullPage:false});
  return{width,height,furnaceHeight:Math.round(before.furnace.height),oddsHeight:Math.round(before.odds.height),inventoryHeight:Math.round(before.inventory.height),innerScroll:Math.round(after.scrollTop),pageScroll:after.pageY,overflow:0};
}
(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
  try{for(const[width,height]of [[320,844],[390,844],[1280,844],[320,568],[390,667],[844,390]]){const context=await browser.newContext({viewport:{width,height},isMobile:width<700,hasTouch:width<700});try{
    await context.route('**/*',async route=>{const request=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(request.pathname));if(request.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file))return route.abort();await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});});
    const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message));try{report.push(await check(page,width,height));assert.deepEqual(errors,[],width+'px has no runtime errors');}catch(error){report.push({width,height,failure:error.message});}
  }finally{await context.close();}}}finally{await browser.close();}console.log(JSON.stringify(report,null,2));assert.equal(report.filter(row=>row.failure).length,0,'Every viewport keeps a usable fixed workspace');
})().catch(error=>{console.error(error);process.exitCode=1;});
