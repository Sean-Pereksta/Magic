/* Playable-bundle integration: decoded art, settings, input, mobile and failure fallback. */
'use strict';
const fs = require('node:fs'), path = require('node:path'), assert = require('node:assert/strict');
const { pathToFileURL } = require('node:url');
const { chromium } = require('playwright');
const url = pathToFileURL(path.resolve(__dirname, '../../apes-together-strong.html')).href;
const output = process.env.QA_ARTIFACT_DIR;
const screenshot = async (page, name) => { if (output) { fs.mkdirSync(output, { recursive: true }); await page.screenshot({ path: path.join(output, name + '.png'), fullPage: true }); } };
(async () => {
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}), args: ['--disable-dev-shm-usage'] });
  const errors = [], remote = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });
    page.on('pageerror', e => errors.push(e.message));
    page.on('request', request => { if (/^https?:/i.test(request.url())) remote.push(request.url()); });
    await page.goto(url);
    await page.waitForFunction(() => window.ATSVisualAssets?.status === 'ready');
    const assets = await page.evaluate(() => Object.values(ATSVisualAssets.manifest).flatMap(family => family.atlases.map(atlas => {
      const image = ATSVisualAssets.get(atlas.id);
      return { id: atlas.id, width: image?.naturalWidth, height: image?.naturalHeight, declaredWidth: atlas.width, declaredHeight: atlas.height };
    })));
    assert.ok(assets.length >= 3, 'character and environmental artwork is present');
    for (const atlas of assets) {
      assert.ok(atlas.width > 0 && atlas.height > 0, 'decoded ' + atlas.id);
      if (atlas.declaredWidth) assert.equal(atlas.width, atlas.declaredWidth);
      if (atlas.declaredHeight) assert.equal(atlas.height, atlas.declaredHeight);
    }
    await screenshot(page, 'visual-menu');
    for (const quality of ['low', 'medium', 'high', 'ultra']) {
      await page.locator('#menuSettings').click();
      await page.locator('#qualitySelect').selectOption(quality);
      assert.equal(await page.evaluate(() => ATS.renderer.quality), quality);
      assert.equal(await page.evaluate(() => JSON.parse(localStorage.getItem('ats-crown-settings-v1')).quality), quality);
      await page.locator('#backSettings').click();
    }
    await page.reload();
    await page.waitForFunction(() => ATSVisualAssets.status === 'ready');
    assert.equal(await page.evaluate(() => ATS.renderer.quality), 'ultra', 'quality persists across reload');
    await page.locator('#menuSettings').click();
    await page.locator('#qualitySelect').selectOption('high');
    await page.locator('#reducedMotion').check();
    assert.equal(await page.evaluate(() => document.body.classList.contains('reduced-motion')), true);
    await page.locator('#reducedMotion').uncheck();
    await page.locator('#backSettings').click();
    await page.locator('#seedInput').fill('FOREST-VISUAL-QA');
    await page.locator('#newRun').click();
    await page.waitForFunction(() => ATS.game?.time > .2);
    assert.equal(await page.locator('#loadingScreen').isVisible(), false);
    assert.equal(await page.evaluate(() => document.body.getAttribute('aria-busy')), 'false');
    assert.equal(await page.locator('#armyDock .army-commands button:visible').count(), 8, 'compact desktop dock exposes the eight core commands, including field recall');
    await page.locator('#armyDock .army-expand').click();
    assert.ok(await page.locator('#armyDock .army-commands button:visible').count() > 8, 'advanced commands remain accessible');
    assert.equal(await page.locator('#armyDock .army-expand').getAttribute('aria-expanded'), 'true');
    await page.locator('#armyDock .army-expand').click();
    const before = await page.evaluate(() => ({ x: ATS.game.king.x, y: ATS.game.king.y }));
    await page.keyboard.down('d'); await page.waitForTimeout(400); await page.keyboard.up('d');
    const after = await page.evaluate(() => ({ x: ATS.game.king.x, y: ATS.game.king.y }));
    assert.ok(Math.hypot(after.x - before.x, after.y - before.y) > 8, 'new sprites retain keyboard control');
    await screenshot(page, 'visual-desktop-play');
    await page.keyboard.press('Escape');
    const fidelity = await page.evaluate(() => {
      const g = ATS.game, r = ATS.renderer;
      for (const [i, species] of ['gorilla', 'chimpanzee', 'orangutan', 'gibbon', 'capuchin', 'mandrill'].entries()) {
        const a = g.makeApe(g.king.x + (i - 2) * 38, g.king.y + 70, 'follow');
        a.species = species; a.moving = true; a.dir = i * Math.PI / 3;
      }
      const fingerprint = () => JSON.stringify({
        time: g.time, king: [g.king.x, g.king.y, g.king.hp, g.king.dir],
        apes: g.apes.map(a => [a.id, a.x, a.y, a.hp, a.dir, a.state]),
        humans: g.humans.map(a => [a.id, a.x, a.y, a.hp, a.dir, a.state]),
        objects: [...g.world.objects.values()].map(o => [o.id, o.x, o.y, o.hp, o.dead, o.solid, o.collision])
      });
      const initial = fingerprint();
      for (const quality of ['low', 'medium', 'high', 'ultra']) { r.quality = quality; for (let n = 0; n < 6; n++) r.draw(g, 0); }
      r.quality = 'high'; r.draw(g, 0);
      return { unchanged: fingerprint() === initial, spriteArt: !!window.ATSCharacterArt, environmentArt: !!window.ATSEnvironmentArt };
    });
    assert.equal(fidelity.unchanged, true, 'quality changes and repeated rendering do not move, damage or mutate gameplay objects');
    assert.equal(fidelity.spriteArt, true);
    await page.locator('#resumeRun').click();
    await page.waitForTimeout(100);
    await screenshot(page, 'visual-species-play');
    await page.keyboard.press('v');
    assert.equal(await page.evaluate(() => ATS.renderer.findSettlements), true, 'V still enables the village finder');
    await page.keyboard.press('Escape');
    await page.evaluate(() => {
      const r = ATS.renderer, original = r.settlementIndicators;
      r._qaOriginalIndicators = original;
      r.settlementIndicators = function (game) {
        const {x,y}=game.king;
        return original.call(this,{...game,settlements:[
          {id:'qa-nearest',name:'Cedar Refuge',x:x+500,y:y+200,attack:false},
          {id:'qa-east',name:'Willow Sanctuary',x:x-1000,y:y+600,attack:false},
          {id:'qa-north',name:'Moonlit Canopy',x:x+1100,y:y-1300,attack:false}
        ]});
      };
      document.getElementById('pause').hidden=true; r.draw(ATS.game,0);
    });
    await page.waitForTimeout(50);
    const labels=await page.evaluate(()=>ATS.renderer.settlementFinderLabels);
    assert.equal(labels.length,3);
    assert.equal(labels.find(label=>label.nearest)?.name,'Cedar Refuge');
    assert.ok(labels.every(label=>label.x>=0&&label.x+label.width<=1440),'village names remain within viewport');
    await screenshot(page,'visual-village-finder');
    await page.evaluate(()=>{const r=ATS.renderer;r.settlementIndicators=r._qaOriginalIndicators;delete r._qaOriginalIndicators;r.findSettlements=false;document.getElementById('pause').hidden=false;});
    await page.evaluate(() => {
      const canvas = document.createElement('canvas'); canvas.id = 'contactReview';
      canvas.style.cssText = 'position:fixed;inset:0;z-index:9999;width:1440px;height:900px;background:#52635c'; document.body.append(canvas);
      const r = new ATSRenderer(canvas), c = r.ctx;
      c.fillStyle = '#52635c'; c.fillRect(0, 0, 1440, 900); c.fillStyle = '#fff'; c.font = '16px system-ui';
      const samples = [
        { label: 'King · idle · high', king: true, quality: 'high', moving: false, scale: 1 },
        { label: 'King · run · high', king: true, quality: 'high', moving: true, scale: 1 },
        { label: 'King · idle · low', king: true, quality: 'low', moving: false, scale: 1 },
        { label: 'King · run · low', king: true, quality: 'low', moving: true, scale: 1 },
        { label: 'Gorilla · run · 80%', species: 'gorilla', quality: 'high', moving: true, scale: .8 },
        { label: 'Chimpanzee · run · 65%', species: 'chimpanzee', quality: 'high', moving: true, scale: .65 }
      ];
      for (const [row, sample] of samples.entries()) {
        c.fillStyle = '#f1e8cd'; c.fillText(sample.label, 12, 27 + row * 145);
        for (let facing = 0; facing < 8; facing++) {
          let dir = 0; for (let n = 0; n < 720; n++) if (ATSCharacterArt.direction(n * Math.PI / 360).sector === facing) { dir = n * Math.PI / 360; break; }
          const x = 115 + facing * 170, y = 131 + row * 145;
          c.save(); c.translate(x, y); c.strokeStyle = '#c6be93'; c.lineWidth = .5; c.beginPath(); c.moveTo(-60, 0); c.lineTo(60, 0); c.moveTo(0, -8); c.lineTo(0, 8); c.stroke();
          r.quality = sample.quality; r.time = 10.31; r.detailLevel = sample.quality === 'low' ? 3 : 0;
          c.scale(sample.scale, sample.scale);
          r.drawApe(c, { id: sample.king ? 'king' : 'qa-' + facing, species: sample.species || 'gorilla', dir, phase: .2, age: 240, hp: 100, maxHp: 100, moving: sample.moving, state: 'follow', x: 0, y: 0 }, !!sample.king);
          c.restore();
        }
      }
    });
    await screenshot(page, 'visual-foot-contact-grid');
    await page.evaluate(() => document.getElementById('contactReview').remove());
    await page.evaluate(() => {
      const canvas = document.createElement('canvas'); canvas.id = 'cageReview';
      canvas.style.cssText = 'position:fixed;inset:0;z-index:9999;width:1440px;height:900px;background:#52635c'; document.body.append(canvas);
      const r = new ATSRenderer(canvas), c = r.ctx;
      r.time = 10; r.quality = 'high';
      c.fillStyle = '#52635c'; c.fillRect(0, 0, 1440, 900); c.font = '20px system-ui';
      for (let row = 0; row < 2; row++) for (let column = 0; column < 3; column++) {
        const [w, h, count] = [[45, 40, 2], [62, 50, 3], [95, 75, 4]][column];
        const o = { id: 'review-cage-' + row + '-' + column, type: 'cage', x: 0, y: 0, w, h, count, hp: 100, maxHp: 100, ...(row ? { prisonKind: 'research', prisonLock: 'electronic', prisonUnlocked: false, prisonReward: column === 2 ? 'champion' : 'family' } : {}) };
        c.fillStyle = '#f2e8c9'; c.fillText((row ? 'Locked prison cell' : 'Captive cage') + ' · ' + count + ' captives', 30 + column * 480, 50 + row * 440);
        c.save(); c.translate(240 + column * 480, 280 + row * 440); c.scale(3, 3); r.drawObject(c, o); c.restore();
      }
    });
    await screenshot(page, 'visual-cage-contact-grid');
    await page.evaluate(() => document.getElementById('cageReview').remove());

    const gallery = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    const cards = await page.evaluate(() => Object.values(ATSVisualAssets.manifest).flatMap(f => f.atlases.map(a => ({ id: a.id, source: ATSVisualAssets.get(a.id).src }))));
    await gallery.setContent('<style>body{margin:28px;background:#12262a;color:#eee;font:16px system-ui}h1{font-size:26px}.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:18px}article{padding:16px;background:#26383c;border:1px solid #61736c}img{width:100%;height:auto}h2{font-size:16px}</style><h1>Apes Together Strong — original sprite atlas review</h1><div class="grid"></div>');
    await gallery.evaluate(cards => { for (const card of cards) { const article = document.createElement('article'), label = document.createElement('h2'), image = document.createElement('img'); label.textContent = card.id; image.src = card.source; article.append(label, image); document.querySelector('.grid').append(article); } }, cards);
    await gallery.waitForFunction(() => [...document.images].every(image => image.complete && image.naturalWidth));
    await screenshot(gallery, 'visual-sprite-gallery');

    const mobile = await browser.newPage({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 3 });
    mobile.on('pageerror', e => errors.push(e.message));
    await mobile.goto(url); await mobile.waitForFunction(() => ATSVisualAssets.status === 'ready');
    await mobile.locator('#menuSettings').tap(); await mobile.locator('#qualitySelect').selectOption('low'); await mobile.locator('#backSettings').tap();
    await mobile.locator('#newRun').tap(); await mobile.waitForFunction(() => ATS.game?.time > .2);
    assert.equal(await mobile.locator('#joystick').isVisible(), true);
    assert.equal(await mobile.evaluate(() => ATS.renderer.dpr), 1, 'low quality caps dense mobile framebuffer');
    assert.equal(await mobile.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'no mobile horizontal overflow');
    const start = await mobile.evaluate(() => ({ x: ATS.game.king.x, y: ATS.game.king.y }));
    const joystick = await mobile.locator('#joystick').boundingBox(), touch = await mobile.context().newCDPSession(mobile);
    const point = { x: joystick.x + joystick.width / 2, y: joystick.y + joystick.height / 2 };
    await touch.send('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [point] });
    await touch.send('Input.dispatchTouchEvent', { type: 'touchMove', touchPoints: [{ x: point.x + 33, y: point.y }] });
    await mobile.waitForTimeout(350);
    await touch.send('Input.dispatchTouchEvent', { type: 'touchEnd', touchPoints: [] });
    const end = await mobile.evaluate(() => ({ x: ATS.game.king.x, y: ATS.game.king.y }));
    assert.ok(Math.hypot(end.x - start.x, end.y - start.y) > 5, 'real touch drag moves the king through the joystick');
    await mobile.locator('#attackButton').tap();
    await mobile.locator('#mapButton').tap(); assert.equal(await mobile.evaluate(() => ATS.screen), 'overview');
    await mobile.locator('#closeOverview').tap();
    await screenshot(mobile, 'visual-mobile-play');

    const loading = await browser.newPage({viewport:{width:1440,height:900}});
    loading.on('pageerror',e=>errors.push(e.message));
    await loading.addInitScript(()=>{const Original=window.Image;let index=0;window.Image=class extends Original{set src(value){setTimeout(()=>{super.src=value},600+(index++)*100)}get src(){return super.src}}});
    await loading.goto(url);
    await loading.waitForFunction(()=>window.ATSVisualAssets?.status==='loading');
    assert.equal(await loading.locator('#loadingScreen').isVisible(),true);
    assert.equal(await loading.locator('#newRun').isDisabled(),true);
    assert.equal(await loading.evaluate(()=>document.getElementById('menu').inert),true);
    await loading.waitForFunction(()=>ATSVisualAssets.progress>0&&ATSVisualAssets.progress<1);
    await screenshot(loading,'visual-loading-progress');
    const partial=await loading.locator('#artworkProgress').evaluate(element=>element.value);
    assert.ok(partial>0&&partial<1,'loading bar reflects partial image completion');
    await loading.waitForFunction(()=>ATSVisualAssets.status==='ready');
    assert.equal(await loading.locator('#loadingScreen').isVisible(),false);
    assert.equal(await loading.locator('#newRun').isEnabled(),true);
    assert.equal(await loading.locator('#artworkProgress').evaluate(element=>element.value),1);

    const degraded = await browser.newPage();
    degraded.on('pageerror', e => errors.push(e.message));
    await degraded.addInitScript(() => { const Original = window.Image; window.Image = class extends Original { set src(value) { super.src = String(value).startsWith('data:image/') ? 'data:image/png;base64,broken' : value; } get src() { return super.src; } }; });
    await degraded.goto(url); await degraded.waitForFunction(() => ATSVisualAssets.status === 'degraded');
    assert.equal(await degraded.locator('#newRun').isEnabled(), true, 'failed artwork does not lock the main menu');
    assert.match(await degraded.locator('.menu-feedback').textContent(), /artwork could not load/i);
    await degraded.locator('#newRun').click(); await degraded.waitForFunction(() => ATS.game?.time > .2);
    assert.equal(await degraded.evaluate(() => ATS.screen), 'play', 'legacy fallback remains playable after asset failure');
    assert.deepEqual(errors, []); assert.deepEqual(remote, [], 'all runtime artwork is local or embedded');
    console.log(`PASS: ${assets.length} decoded local atlases; four persistent graphics presets; keyboard, touch, mobile DPR, reduced motion, render purity, sprite gallery and playable missing-art fallback. No browser errors.`);
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
