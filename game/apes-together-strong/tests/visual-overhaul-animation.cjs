/* Native browser Canvas recording; no external capture or video dependencies. */
'use strict';
const fs = require('node:fs'), path = require('node:path'), assert = require('node:assert/strict');
const { pathToFileURL } = require('node:url'), { chromium } = require('playwright');
const output = process.env.QA_ARTIFACT_DIR;
if (!output) throw new Error('Set QA_ARTIFACT_DIR to save animation recordings.');
(async () => {
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}) });
  const errors = [];
  try {
    const page = await browser.newPage({ viewport: { width: 1280, height: 720 }, deviceScaleFactor: 1 });
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(pathToFileURL(path.resolve(__dirname, '../../apes-together-strong.html')).href);
    await page.waitForFunction(() => ATSVisualAssets.status === 'ready');
    await page.locator('#seedInput').fill('FOREST-VISUAL-QA'); await page.locator('#newRun').click();
    await page.waitForFunction(() => ATS.game?.time > .2); await page.keyboard.press('Escape');
    const gallery = await page.evaluate(async () => {
      const cv = document.createElement('canvas'); cv.style.cssText = 'position:fixed;inset:0;width:1280px;height:720px;z-index:9999'; document.body.append(cv);
      const r = new ATSRenderer(cv), c = r.ctx; r.quality = 'high'; r.detailLevel = 0;
      const actors = [
        ...['gorilla','chimpanzee','orangutan','gibbon','capuchin','mandrill'].map(species => ({ id: species, species, label: species })),
        { id: 'king', species: 'gorilla', label: 'King Ape' },
        ...['pistol','rifle','machine','sniper'].map(kind => ({ id: kind, type: 'human', kind, label: kind === 'pistol' ? 'patrol guard' : kind === 'machine' ? 'heavy soldier' : kind === 'sniper' ? 'scout / sniper' : 'rifle soldier' }))
      ].map((a, i) => ({ hp: 100, maxHp: 100, state: 'patrol', moving: true, phase: i * .37, age: 240, x: 0, y: 0, ...a }));
      const crops = Object.fromEntries(actors.map(a => [a.id, new Set()]));
      let active = null;
      const drawImage = c.drawImage.bind(c); c.drawImage = (...args) => { if (active && args.length === 9) crops[active.id].add(args.slice(1, 5).join(':')); return drawImage(...args); };
      const stream = cv.captureStream(30), chunks = [], recorder = new MediaRecorder(stream, { mimeType: 'video/webm;codecs=vp9', videoBitsPerSecond: 4500000 });
      recorder.ondataavailable = event => { if (event.data.size) chunks.push(event.data); };
      const done = new Promise(resolve => { recorder.onstop = resolve; }); recorder.start();
      const start = performance.now();
      await new Promise(resolve => {
        function frame(now) {
          const time = (now - start) / 1000; r.time = time;
          c.setTransform(1,0,0,1,0,0); c.fillStyle = '#4c6057'; c.fillRect(0,0,1280,720);
          c.fillStyle = '#f6e3b1'; c.font = '24px system-ui'; c.fillText('Authored limb animation · ' + (time < 4 ? 'front views' : 'rear views'), 30, 40);
          c.font = '15px system-ui'; c.fillStyle = '#d6ded5'; c.fillText('King, all six species and all four human classes — real game renderer', 30, 67);
          for (let i=0;i<actors.length;i++) {
            const a=actors[i], row=i<6?0:1, col=row?i-6:i, x=row?128+col*256:107+col*213, y=row?640:327;
            a.dir=time<4?Math.PI/4:Math.PI*1.25;
            c.save(); c.translate(x,y); c.strokeStyle='#96a596'; c.beginPath(); c.moveTo(-80,0); c.lineTo(80,0); c.stroke(); c.scale(2,2);
            active=a; if(a.type==='human')r.drawHuman(c,a);else r.drawApe(c,a,a.id==='king'); active=null; c.restore();
            c.fillStyle='#e8e6d2'; c.font='17px system-ui'; c.textAlign='center'; c.fillText(a.label,x,y+31); c.textAlign='left';
          }
          if(time<8)requestAnimationFrame(frame);else resolve();
        } requestAnimationFrame(frame);
      });
      recorder.stop(); await done; stream.getTracks().forEach(track=>track.stop()); cv.remove();
      const bytes=new Uint8Array(await new Blob(chunks,{type:'video/webm'}).arrayBuffer());
      let binary='';for(let i=0;i<bytes.length;i+=32768)binary+=String.fromCharCode(...bytes.subarray(i,i+32768));
      return {base64:btoa(binary),frames:Object.fromEntries(Object.entries(crops).map(([id,set])=>[id,set.size]))};
    });
    for (const [actor, frames] of Object.entries(gallery.frames)) assert.ok(frames >= 2, actor + ' uses distinct authored limb frames in the actual renderer');
    fs.mkdirSync(output,{recursive:true}); fs.writeFileSync(path.join(output,'character-limb-animation.webm'),Buffer.from(gallery.base64,'base64'));
    await page.locator('#resumeRun').click();
    await page.evaluate(() => {
      ATS.renderer.camera.zoom=1.3;
      const cv=document.getElementById('gameCanvas'),stream=cv.captureStream(30),chunks=[];
      const recorder=new MediaRecorder(stream,{mimeType:'video/webm;codecs=vp9',videoBitsPerSecond:4500000});
      recorder.ondataavailable=e=>{if(e.data.size)chunks.push(e.data)};
      window.finishAnimationClip=async()=>{const done=new Promise(resolve=>{recorder.onstop=resolve});recorder.stop();await done;stream.getTracks().forEach(t=>t.stop());const bytes=new Uint8Array(await new Blob(chunks).arrayBuffer());let binary='';for(let i=0;i<bytes.length;i+=32768)binary+=String.fromCharCode(...bytes.subarray(i,i+32768));return btoa(binary)};
      recorder.start();
    });
    for (const key of ['s','a','w','d']) { await page.keyboard.down(key); await page.waitForTimeout(1000); await page.keyboard.up(key); }
    await page.waitForTimeout(500);
    const clip=await page.evaluate(()=>finishAnimationClip());
    fs.writeFileSync(path.join(output,'king-live-movement.webm'),Buffer.from(clip,'base64'));
    fs.writeFileSync(path.join(output,'animation-frame-evidence.json'),JSON.stringify(gallery.frames,null,2));
    assert.deepEqual(errors,[]);
    console.log('PASS: real renderer changes limb source frames for King, six species and four human classes. Saved 8-second gallery and live movement WebM clips.');
  } finally { await browser.close(); }
})().catch(error=>{console.error(error);process.exitCode=1});
