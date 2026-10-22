const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require(process.env.MONKEY_TRAILS_PLAYWRIGHT||'playwright');
const root=path.resolve(__dirname,'../..'),mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.svg':'image/svg+xml','.json':'application/json','.webp':'image/webp'};

// Exercise the actual UI and browser activation path while keeping phone GPS and
// permission responses deterministic. Each case starts with a fresh device save.
async function mockLocation(page,options={}){
  await page.addInitScript(options=>{
    const requests=[],watches=[],cleared=[],queries=[];
    const permission={state:options.permission||'prompt',onchange:null,listeners:[],addEventListener(type,fn){if(type==='change')this.listeners.push(fn);},removeEventListener(type,fn){this.listeners=this.listeners.filter(x=>x!==fn);}};
    const fix=()=>({coords:{latitude:40,longitude:-74,accuracy:8,altitude:null,altitudeAccuracy:null,heading:null,speed:null},timestamp:Date.now()});
    const geo={
      getCurrentPosition(success,failure,config){requests.push({success,failure,config,activation:navigator.userActivation?.isActive});},
      watchPosition(success,failure,config){watches.push({success,failure,config,activation:navigator.userActivation?.isActive});return watches.length;},
      clearWatch(id){cleared.push(id);}
    };
    Object.defineProperty(navigator,'geolocation',{configurable:true,value:options.unsupported?undefined:geo});
    Object.defineProperty(navigator,'permissions',{configurable:true,value:options.noPermissions?undefined:{query(descriptor){queries.push(descriptor.name);return options.queryRejects?Promise.reject(new Error('Unavailable in this browser')):Promise.resolve(permission);}}});
    if(options.insecure)Object.defineProperty(window,'isSecureContext',{configurable:true,value:false});
    if(options.policy){for(const key of ['permissionsPolicy','featurePolicy'])Object.defineProperty(document,key,{configurable:true,value:{allowsFeature:()=>false}});}
    window.__geo={requests,watches,cleared,queries,fix,
      counts:()=>({requests:requests.length,watches:watches.length,queries:queries.length,cleared:cleared.length}),
      succeed(index=requests.length-1){requests[index].success(fix());},
      fail(code,index=requests.length-1){requests[index].failure({code,message:'Mock location response'});},
      watchFix(index=watches.length-1){watches[index].success(fix());},
      watchFail(code,index=watches.length-1){watches[index].failure({code,message:'Mock watch response'});},
      setPermission(state){permission.state=state;permission.onchange?.();for(const fn of permission.listeners)fn();}
    };
  },options);
}

async function setup(browser,options={}){
  const context=await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true});
  await context.route('**/*',async route=>{
    const url=new URL(route.request().url()),file=path.resolve(root,'.'+decodeURIComponent(url.pathname));
    if(url.hostname!=='127.0.0.1'||!file.startsWith(root+path.sep)||!fs.existsSync(file)){await route.abort();return;}
    await route.fulfill({contentType:mime[path.extname(file)]||'application/octet-stream',body:fs.readFileSync(file)});
  });
  const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await mockLocation(page,options);await page.goto('http://127.0.0.1:7777/game/monkey-trails.html');
  await page.locator('#welcome-location [data-action="enable-location"]').waitFor();
  return {page,context,errors};
}
const counts=page=>page.evaluate(()=>window.__geo.counts());
const welcomeText=page=>page.locator('#welcome-location').innerText();
async function waitText(page,pattern,selector='#welcome-location'){
  await page.waitForFunction(({source,flags,selector})=>new RegExp(source,flags).test(document.querySelector(selector)?.innerText||''),{source:pattern.source,flags:pattern.flags,selector});
}
async function openGPS(page){
  await page.locator('#creature-name').fill('Mossfoot');await page.locator('#creature-password').fill('a-test-password-only');
  await page.locator('#save-form button[value="create"]').click();await page.locator('#game').waitFor({state:'visible'});
}
async function checkClean(page,errors){
  const size=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth}));
  assert.ok(size.scroll<=size.width,`Mobile overflow: ${size.scroll-size.width}px`);assert.deepEqual(errors,[]);
}

(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE?{executablePath:process.env.MONKEY_TRAILS_BROWSER_EXECUTABLE}:{})}),report=[];
  const run=async(name,options,test)=>{const {page,context,errors}=await setup(browser,options);try{await test(page);await checkClean(page,errors);report.push({scenario:name,errors:0});}finally{await context.close();}};
  try{
    await run('menus retain walking, discoveries fade, and encounters still pause',{},async page=>{
      await page.clock.install();await openGPS(page);await page.locator('#tracking-button').click();
      let step=0;
      const walk=async(count=1)=>{let found=false;for(let i=0;i<count;i++){
        await page.clock.fastForward(8000);
        await page.evaluate(step=>__geo.watches.at(-1).success({coords:{latitude:40+step*10/111195,longitude:-74,accuracy:8,speed:1.25},timestamp:Date.now()}),step++);
        found ||= await page.locator('#discovery-popup').evaluate(el=>el.matches(':popover-open'));
      }return found;};
      await walk(2); // Tracker warmup; subsequent samples earn ten metres each.
      await page.locator('[data-action="tab"][data-id="sanctuary"]').click();
      await page.locator('[data-action="collection-filter"][data-id="all"]').click();
      const search=page.locator('#collection-search');
      await search.fill('cap');await search.focus();
      await walk(8);
      assert.equal((await counts(page)).cleared,0,'A menu must not stop the GPS watch');
      assert.equal(await search.inputValue(),'cap');assert.equal(await search.evaluate(el=>el===document.activeElement),true);
      const popup=page.locator('#discovery-popup');await popup.waitFor({state:'visible'});
      assert.match(await popup.innerText(),/YOU FOUND/);assert.match(await popup.innerText(),/×[12]/);
      assert.equal(await popup.evaluate(el=>getComputedStyle(el).pointerEvents),'none');
      await page.clock.fastForward(3601);await popup.waitFor({state:'hidden'});
      for(const tab of ['team','pack','walk']){
        await page.locator(`[data-action="tab"][data-id="${tab}"]`).click();await walk();
        assert.equal((await counts(page)).cleared,0,`${tab} must keep the watch`);
      }
      assert.ok(parseInt((await page.locator('.distance-block strong').innerText()).replace(/[^0-9]/g,''))>300);
      // Each modal keeps the same watch and permits a discovery above its top layer.
      for(const action of ['settings','records','lands','compass','inspect']){
        if(action==='records')await page.locator('[data-action="settings"]').click();
        await page.locator(`[data-action="${action}"]`).first().click();const found=await walk(8);
        assert.equal(await page.locator('#inspect-dialog').evaluate(el=>el.open),true);
        assert.equal((await counts(page)).cleared,0,`${action} must keep the watch`);
        assert.equal(found,true,'A discovery is visible above the open menu');
        await page.locator('.dialog-close').click();
      }
      await page.locator('[data-action="tab"][data-id="team"]').click();
      await page.locator('[data-action="ability-open"]').first().click();await walk();
      assert.equal((await counts(page)).cleared,0,'Ability editing must keep walking');
      await page.locator('.dialog-close').click();
      await page.locator('[data-action="tab"][data-id="walk"]').click();
      await page.locator('#tracking-button').click();assert.equal((await counts(page)).cleared,1,'Manual pause still works');
      await page.locator('#tracking-button').click();await walk(2);
      await page.locator('[data-action="settings"]').click();await walk(20);
      assert.ok((await counts(page)).cleared>=2,'The encounter milestone pauses tracking from a menu');
      assert.equal(await page.locator('#inspect-dialog').evaluate(el=>el.open),false);
      assert.equal(await page.locator('[data-action="ready"]').count(),1,'Encounter returns to the Walk screen');
    });
    await run('explicit setup verifies location without starting or adding distance',{},async page=>{
      assert.equal((await counts(page)).requests,0,'Opening the page must not request location');assert.equal((await counts(page)).watches,0);
      await page.locator('#welcome-location [data-action="enable-location"]').click();
      assert.equal((await counts(page)).requests,1);assert.equal((await counts(page)).watches,0);
      assert.equal(await page.evaluate(()=>__geo.requests[0].activation),true,'Use the actual browser user gesture');
      await page.evaluate(()=>__geo.succeed());await waitText(page,/ready|enabled|allowed/i);
      await openGPS(page);assert.equal((await counts(page)).watches,0,'Verifying access must not start an expedition watch');
      assert.match(await page.locator('.distance-block').innerText(),/\b0\s*ft\b/);
      await page.locator('#tracking-button').click();assert.equal((await counts(page)).watches,1);
      await page.evaluate(()=>__geo.watchFix());assert.match(await page.locator('.distance-block').innerText(),/\b0\s*ft\b/);
      await page.locator('#tracking-button').click();assert.equal((await counts(page)).cleared,1);
    });
    await run('request happens synchronously before a click handler returns',{},async page=>{
      const immediate=await page.evaluate(()=>{document.querySelector('#welcome-location [data-action="enable-location"]').click();return __geo.requests.length;});
      assert.equal(immediate,1,'Do not await a Permissions API query before invoking geolocation');
      await page.evaluate(()=>__geo.succeed());await waitText(page,/ready|enabled|allowed/i);
    });
    await run('denial explains both Chrome and phone settings',{},async page=>{
      await page.locator('#welcome-location [data-action="enable-location"]').click();await page.evaluate(()=>__geo.fail(1));await waitText(page,/blocked|denied/i);
      assert.match(await welcomeText(page),/Chrome/i);assert.match(await welcomeText(page),/phone|Android|iPhone/i);assert.match(await welcomeText(page),/settings/i);
      await openGPS(page);assert.equal((await counts(page)).watches,0);assert.equal(await page.locator('#view .location-access [data-action="enable-location"]').count(),1);
      if(process.env.MONKEY_TRAILS_LOCATION_SCREENSHOT){await page.locator('#view .location-access').evaluate(panel=>window.scrollTo(0,panel.getBoundingClientRect().top+scrollY-110));await page.screenshot({path:process.env.MONKEY_TRAILS_LOCATION_SCREENSHOT});}
    });
    await run('tracking denial pauses and exposes the same recovery help',{},async page=>{
      await openGPS(page);await page.locator('#tracking-button').click();assert.equal((await counts(page)).watches,1);
      await page.evaluate(()=>__geo.watchFail(1));await waitText(page,/blocked|denied/i,'#view .location-access');
      assert.equal(await page.locator('#tracking-button').innerText(),'Start walking');assert.equal((await counts(page)).cleared,1);
      assert.equal(await page.locator('#view [data-location-help]:visible').count(),1);assert.equal((await counts(page)).requests,0);
      await page.locator('#view [data-action="enable-location"]').click();assert.equal((await counts(page)).requests,1);
      await page.evaluate(()=>__geo.succeed());await waitText(page,/ready|enabled|allowed/i,'#view .location-access');
      assert.equal((await counts(page)).watches,1,'A recovery access check must not restart walking');
    });
    await run('an already allowed permission is explained without requesting GPS',{permission:'granted'},async page=>{
      await waitText(page,/already allows|access allowed/i);assert.equal((await counts(page)).requests,0);assert.equal((await counts(page)).watches,0);
    });
    await run('a phone that never responds eventually permits a retry',{},async page=>{
      await page.clock.install();await page.locator('#welcome-location [data-action="enable-location"]').click();
      assert.equal(await page.locator('#welcome-location [data-action="enable-location"]:disabled').count(),1);
      await page.clock.fastForward(20001);await waitText(page,/no location returned|not returned/i);
      await page.evaluate(()=>__geo.succeed());assert.match(await welcomeText(page),/no location returned|not returned/i,'Late success from an expired request must be ignored');
      assert.equal(await page.locator('#welcome-location [data-action="enable-location"]:disabled').count(),0);
    });
    for(const [name,code,pattern]of [['timeout',3,/timed out|timeout|taking longer|no location returned|not returned/i],['unavailable',2,/unavailable|couldn.t|could not|unable/i]]){
      await run(`${name} displays recoverable instructions`,{},async page=>{
        await page.locator('#welcome-location [data-action="enable-location"]').click();await page.evaluate(code=>__geo.fail(code),code);await waitText(page,pattern);
        assert.equal(await page.locator('#welcome-location [data-location-help]:visible').count(),1);
        assert.equal(await page.locator('#welcome-location [data-action="enable-location"]:disabled').count(),0,'Allow an explicit retry');
      });
    }
    for(const options of [{noPermissions:true},{queryRejects:true}]){
      await run(options.noPermissions?'works when Permissions API is missing':'works when permission status query rejects',options,async page=>{
        assert.equal((await counts(page)).requests,0);
        await page.locator('#welcome-location [data-action="enable-location"]').click();assert.equal((await counts(page)).requests,1);
        await page.evaluate(()=>__geo.succeed());await waitText(page,/ready|enabled|allowed/i);
      });
    }
    for(const [name,options,pattern]of [['insecure',{insecure:true},/HTTPS|secure/i],['unsupported',{unsupported:true},/support|available/i],['policy',{policy:true},/blocked|policy|embedded|full browser|direct/i]]){
      await run(`${name} gives an actionable diagnosis without a GPS call`,options,async page=>{
        await waitText(page,pattern);const button=page.locator('#welcome-location [data-action="enable-location"]');if(await button.isEnabled())await button.click();
        assert.equal((await counts(page)).requests,0);assert.equal((await counts(page)).watches,0);
      });
    }
    await run('practice stays separate and requests no location automatically',{},async page=>{
      await page.locator('#practice-button').click();await page.locator('#game').waitFor({state:'visible'});
      assert.equal((await counts(page)).requests,0);assert.equal((await counts(page)).watches,0);assert.equal(await page.locator('#tracking-button:visible').count(),0);
      await page.locator('.practice-tools summary').click();const setup=page.locator('[data-action="gps-setup"]');assert.equal(await setup.count(),1);await setup.click();await page.locator('#welcome').waitFor({state:'visible'});
      assert.equal((await counts(page)).requests,0);assert.equal((await counts(page)).watches,0);
    });
    await run('cancelled location checks ignore late callbacks',{},async page=>{
      const result=await page.evaluate(async()=>{
        const {LocationAccess}=await import('/game/monkey-trails/location.mjs'),changes=[],access=new LocationAccess(s=>changes.push(s));
        access.request();access.cancel();const before=JSON.stringify(access.status),count=changes.length;__geo.succeed();
        return {before,after:JSON.stringify(access.status),count,afterCount:changes.length};
      });
      assert.equal(result.after,result.before);assert.equal(result.afterCount,result.count);
    });
    const index=fs.readFileSync(path.join(root,'index.html'),'utf8'),iframe=index.match(/<iframe\b[\s\S]*?<\/iframe>/i)?.[0]||'',allow=iframe.match(/\ballow="([^"]+)"/)?.[1]||'',src=iframe.match(/\bsrc="([^"]+)"/)?.[1]||'';
    assert.ok(iframe,'The hosted app must retain its lobby iframe');
    const geolocation=allow.split(';').find(x=>/^\s*geolocation\s/.test(x))||'';
    assert.match(geolocation,/'self'/);assert.match(geolocation,/https:\/\/catnmice\.com(?:\s|$)/);assert.match(geolocation,/https:\/\/www\.catnmice\.com(?:\s|$)/);
    assert.equal(new URL(src,'https://www.catnmice.com/').origin,'https://www.catnmice.com','Root lobby navigation must preserve the current hostname');
    assert.match(index,/const\s+LOBBY_URL\s*=\s*new URL\(['"]\/lobby\/lobby\.html['"],\s*location\.origin\)/);
    for(const [parentOrigin,childOrigin]of [['https://catnmice.com','https://www.catnmice.com'],['https://www.catnmice.com','https://catnmice.com'],['https://catnmice.com','https://catnmice.com'],['https://www.catnmice.com','https://www.catnmice.com']]){
      for(const [name,policy,expected]of [['old','geolocation https://catnmice.com https://www.iheart.com',childOrigin==='https://catnmice.com'],['fixed',allow,true]]){
        const context=await browser.newContext({permissions:['geolocation'],geolocation:{latitude:40,longitude:-74}});
        try{
          await context.route('**/*',route=>route.fulfill({contentType:'text/html',body:new URL(route.request().url()).pathname==='/'?`<!doctype html><iframe id="game-frame" src="${childOrigin}/game/monkey-trails.html" allow="${policy}"></iframe>`:'<!doctype html><p>Game frame</p>'}));
          const page=await context.newPage();await page.goto(`${parentOrigin}/`);const frame=await (await page.locator('#game-frame').elementHandle()).contentFrame();await frame.waitForLoadState();
          const permitted=await frame.evaluate(()=>document.featurePolicy.allowsFeature('geolocation'));
          assert.equal(permitted,expected,`${name} iframe policy from ${parentOrigin} to ${childOrigin}`);
          const result=await frame.evaluate(()=>new Promise(resolve=>navigator.geolocation.getCurrentPosition(()=>resolve('allowed'),error=>resolve(`denied:${error.code}`),{timeout:2000})));
          assert.equal(result,expected?'allowed':'denied:1',`${name} policy must govern the actual geolocation API`);
          report.push({scenario:`${name} iframe policy ${new URL(parentOrigin).hostname} → ${new URL(childOrigin).hostname}`,permitted});
        }finally{await context.close();}
      }
    }
    console.log(JSON.stringify(report,null,2));
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
