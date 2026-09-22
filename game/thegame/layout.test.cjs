const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
test('actual match DOM fits desktop and mobile, effects stay bounded, offline disables moves',async()=>{
  const browser=await chromium.launch({headless:true});
  try {
    const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
    let html=fs.readFileSync(path.join(__dirname,'../thegame.html'),'utf8');
    const helpers=fs.readFileSync(path.join(__dirname,'sync.mjs'),'utf8').replaceAll('export ','');
    html=html.replace(/<script type="module">([\s\S]*?)<\/script>/,(_,js)=>{
      js=js.replace(/import\s+[\s\S]*?\s+from\s+"[^"]+";/g,'').split('if (!auth.currentUser)')[0];
      return `<script type="module">${helpers}
      const initializeApp=()=>({}),getFirestore=()=>({}),getAuth=()=>({}),doc=()=>({});
      ${js}
      G=buildGame2(['A','Player Two','Player Three','Player Four','Player Five'],'A');
      G.revision=1;G.matchId='test';G.hands.A=[5,15,25,'FLIP','BUNGEE'];serverReady=true;renderAll();
      window.matchTest={change(){const old=structuredClone(G);G.piles.U1.top=25;G.piles.D1.dir='up';G.revision++;renderAll();animateMatchChange(old,G)},offline(){serverReady=false;renderAll()}};
      </script>`;
    });
    await page.route('http://thegame.test/**',route=>{
      if(route.request().url().includes('match.css'))return route.fulfill({contentType:'text/css',body:fs.readFileSync(path.join(__dirname,'match.css'),'utf8')});
      return route.fulfill({contentType:'text/html',body:html});
    });
    for(const [width,height] of [[1440,900],[390,844],[320,568],[844,390]]) {
      await page.setViewportSize({width,height});await page.goto('http://thegame.test/game/thegame.html?gameId=test&username=A');
      await page.waitForFunction(()=>window.matchTest);
      const geometry=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth,
        end:document.querySelector('#endBtn').getBoundingClientRect().toJSON(),hand:document.querySelector('#hand').getBoundingClientRect().toJSON(),
        sync:document.querySelector('.sync-status').getBoundingClientRect().toJSON()}));
      assert.ok(geometry.scroll<=width,JSON.stringify(geometry));
      assert.ok(geometry.end.right<=width && geometry.end.bottom<=height,JSON.stringify(geometry));
      assert.ok(geometry.sync.right<=width && geometry.sync.x>=0,JSON.stringify(geometry));
      await page.evaluate(()=>window.matchTest.change());
      assert.ok(await page.locator('.match-spark').count()<=16);
      await page.waitForTimeout(750);assert.equal(await page.locator('.match-spark').count(),0);
      await page.evaluate(()=>window.matchTest.offline());
      assert.equal(await page.locator('#hand button:not(:disabled)').count(),0);
    }
    assert.deepEqual(errors,[]);
  } finally {await browser.close();}
});
