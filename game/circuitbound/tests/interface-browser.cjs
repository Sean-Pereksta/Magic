/* Optional browser regression: node game/circuitbound/tests/interface-browser.cjs.
   Uses bundled or locally installed Playwright, and an installed Chromium browser. */
'use strict';
const fs=require('node:fs');
const path=require('node:path');
const http=require('node:http');
const assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..');
const {playwright,launchOptions}=require('./browser-runtime.cjs');
const server=http.createServer((request,response)=>{
  const relative=decodeURIComponent(new URL(request.url,'http://localhost').pathname);
  const file=path.resolve(root,'.'+relative);
  if(!file.startsWith(root+path.sep)||!['.html','.js','.css'].includes(path.extname(file))){response.writeHead(404).end();return;}
  fs.readFile(file,(error,data)=>{if(error){response.writeHead(404).end();return;}response.setHeader('Content-Type',{'.html':'text/html','.js':'text/javascript','.css':'text/css'}[path.extname(file)]);response.end(data);});
});
(async()=>{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const browser=await playwright.chromium.launch(launchOptions);
  try{
    const context=await browser.newContext({viewport:{width:1440,height:900}}),page=await context.newPage(),errors=[];
    page.on('pageerror',error=>errors.push(error.message));
    // Intercept local reads as well, so this regression runs inside restricted desktop sandboxes.
    await page.route(`http://127.0.0.1:${server.address().port}/**`,async route=>{
      const relative=decodeURIComponent(new URL(route.request().url()).pathname),file=path.resolve(root,'.'+relative);
      if(!file.startsWith(root+path.sep)||!['.html','.js','.css'].includes(path.extname(file))||!fs.existsSync(file)){await route.fulfill({status:404,body:''});return;}
      await route.fulfill({status:200,contentType:{'.html':'text/html','.js':'text/javascript','.css':'text/css'}[path.extname(file)],body:fs.readFileSync(file)});
    });
    await page.goto(`http://127.0.0.1:${server.address().port}/game/circuitbound.html`);
    await page.waitForFunction(()=>document.querySelectorAll('#hotbar .slot').length>20);
    await page.waitForTimeout(300);
    let moveRight=true;
    async function assertMovement(label){
      await page.locator('#pauseBtn').click();await page.locator('#saveBtn').click();
      const before=await page.evaluate(()=>JSON.parse(localStorage.getItem('circuitbound-save-v1')).player.x);
      await page.locator('#resumeBtn').click();
      const key=moveRight?'d':'a';moveRight=!moveRight;
      await page.keyboard.down(key);await page.waitForTimeout(220);await page.keyboard.up(key);
      await page.locator('#pauseBtn').click();await page.locator('#saveBtn').click();
      const after=await page.evaluate(()=>JSON.parse(localStorage.getItem('circuitbound-save-v1')).player.x);
      assert.ok(Math.abs(after-before)>5,`Movement resumes after ${label}`);
      await page.locator('#resumeBtn').click();
    }
    if(await page.locator('#welcomePanel').isVisible())await page.locator('#startBtn').click();
    await page.locator('#journalBtn').click();
    await page.locator('#journalPanel.show').waitFor();
    assert.equal(await page.locator('#journalChapters .chapter').count(),5,'Journal exposes all five chapters');
    await page.keyboard.press('Escape');
    assert.equal(await page.locator('.modal.show').count(),0,'Escape closes the journal');
    await assertMovement('closing the journal');
    await page.locator('#blueprintBtn').click();
    assert.equal(await page.locator('#blueprintDetail .blueprint-cell.filled').count(),6,'Miner preview contains all real components');
    assert.ok(await page.locator('#blueprintDetail .blueprint-cost').count()>3,'Preview displays component costs');
    await page.locator('.blueprint-preview').click();
    assert.equal(await page.locator('#blueprintActive').isVisible(),true,'Blueprint preview is active in the world');
    await page.locator('#blueprintCancel').click();
    assert.equal(await page.locator('#blueprintActive').isVisible(),false,'Blueprint preview cancels without consuming parts');
    await assertMovement('closing blueprints');
    await page.locator('[data-mode="combat"]').click();
    assert.equal(await page.locator('[data-mode="combat"]').getAttribute('aria-pressed'),'true');
    await page.keyboard.press('e');
    await page.locator('#craftPanel.show').waitFor();
    await page.locator('#craftSearch').fill('ladder');
    assert.ok(await page.locator('#recipes .recipe').count()>=1,'Craft search finds movement parts');
    await page.locator('#craftPanel .close').click();
    assert.equal(await page.locator('.modal.show').count(),0);
    await assertMovement('closing the Fabricator');
    await page.locator('#pauseBtn').click();
    await page.locator('#difficultySelect').selectOption('casual');
    await page.locator('#smartPlacement').uncheck();
    await page.locator('#soundVolume').fill('34');
    await page.locator('#musicVolume').fill('12');
    await page.locator('#saveBtn').click();
    await page.locator('#pausePanel .close').click();
    await page.locator('#pauseBtn').click();
    assert.equal(await page.locator('#difficultySelect').inputValue(),'casual');
    assert.equal(await page.locator('#smartPlacement').isChecked(),false,'Smart placement setting survives leaving and reopening pause');
    assert.equal(await page.locator('#soundVolume').inputValue(),'34');
    assert.equal(await page.locator('#musicVolume').inputValue(),'12');
    await page.keyboard.press('Escape');
    const screenshotDirectory=path.resolve(process.env.CIRCUITBOUND_SCREENSHOTS||path.join(root,'../work/circuitbound-browser'));
    fs.mkdirSync(screenshotDirectory,{recursive:true});
    await page.screenshot({path:path.join(screenshotDirectory,'desktop.png')});
    for(const viewport of [{width:390,height:844},{width:844,height:390}]){
      await page.setViewportSize(viewport);
      await page.locator('[data-mode="build"]').click();
      assert.equal(await page.locator('[data-touch="down"]').isVisible(),true,'Climb-down is available on mobile');
      assert.equal(await page.locator('[data-touch="place"]').isVisible(),true,'Build mode has a contextual place button');
      assert.equal(await page.locator('[data-touch="mine"]').isVisible(),false,'Mine control collapses outside Mine mode');
      const overlap=await page.evaluate(()=>{const a=document.querySelector('.touch-pad.left').getBoundingClientRect(),b=document.querySelector('.touch-pad.right').getBoundingClientRect();return a.left<b.right&&a.right>b.left&&a.top<b.bottom&&a.bottom>b.top;});
      assert.equal(overlap,false,'Mobile movement and action pads do not overlap');
      await page.locator('#blueprintBtn').click();
      assert.equal(await page.locator('#touch').isVisible(),false,'World controls cannot cover or intercept a dialog');
      const overflowing=await page.locator('#blueprintPanel').evaluate(element=>element.scrollWidth>element.clientWidth);
      assert.equal(overflowing,false,'Blueprint dialog fits the viewport width');
      await page.screenshot({path:path.join(screenshotDirectory,`mobile-${viewport.width}x${viewport.height}.png`)});
      await page.locator('#blueprintPanel .close').click();
      assert.equal(await page.locator('#touch').isVisible(),true,'Closing the dialog restores movement controls');
      await assertMovement("closing a mobile dialog at " + viewport.width + "x" + viewport.height);
    }
    assert.deepEqual(errors,[],'Game and UI report no browser exceptions');
    console.log('Circuitbound browser checks passed: campaign journal, blueprint previews, menus, crafting, settings, and mobile portrait/landscape controls.');
    await context.close();
    const fileContext=await browser.newContext({viewport:{width:1280,height:800}}),filePage=await fileContext.newPage(),fileErrors=[];
    filePage.on('pageerror',error=>fileErrors.push(error.message));
    await filePage.goto(require('node:url').pathToFileURL(path.join(root,'game/circuitbound.html')).href);
    await filePage.locator('#startBtn').click();await filePage.waitForFunction(()=>document.querySelectorAll('#hotbar .slot').length>10);
    assert.deepEqual(fileErrors,[],'The static game also boots directly from local files without a server');
    await fileContext.close();
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;}).finally(()=>server.close());
