const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const {assetPath, enablePixelInspection} = require('./helpers/cloudflare-assets.cjs');

const repo = path.resolve(__dirname, '../../..');
const expose = `window.colorClock=100000;nowMs=()=>window.colorClock;
window.tcColors={run:source=>eval(source),get runtime(){return spriteRuntime},prepare(){
  resize();entities=[];resourceNodes=[];nextEntityId=1;
  players=[makePlayer(0,'You','frontier',false),makePlayer(1,'Enemy','frontier',false),makePlayer(2,'Teammate','frontier',false)];
  players[0].teamId=0;players[1].teamId=1;players[2].teamId=0;
  terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
  gameStarted=true;gameOver=false;paused=false;PLAYER=0;selectedId=null;selectedIds=[];modeState=null;lobbyId='';
  pendingImpacts=[];areaEffects=[];temporaryObstacles=[];scanEffects=[];initialCameraSet=true;tileVisibleToFaction=()=>true;overlay.style.display='none';
  spriteRuntime.reset();cam.x=0;cam.y=0;cam.zoom=1;
}};`;

test('real Cloudflare troop artwork keeps transparent edges and detail beneath strong player tints at every quality', async () => {
  const browser = await chromium.launch({headless: true, executablePath: process.env.CHROMIUM_EXECUTABLE_PATH || undefined, args: ['--no-sandbox']});
  const page = await browser.newPage({viewport: {width: 1280, height: 900}}), errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.addInitScript(enablePixelInspection);
  const source = fs.readFileSync(path.join(repo, 'game/turncraft.html'), 'utf8')
    .replace(/^import .*;$/gm, '')
    .replace('\nboot();', '\n' + expose + '\nwindow.TurncraftStartup?.complete();')
    .replace('<script type="module">', '<script>const initializeApp=()=>({}),getAuth=()=>({}),getFirestore=()=>({});');
  await page.route('**/*', route => {
    const artwork = assetPath(route.request().url());
    if (artwork) return route.fulfill({path: artwork, contentType: 'image/webp', headers: {'Access-Control-Allow-Origin': '*'}});
    const pathname = decodeURIComponent(new URL(route.request().url()).pathname);
    if (pathname === '/game/turncraft.html') return route.fulfill({contentType: 'text/html', body: source});
    const file = path.resolve(repo, '.' + pathname);
    if (file.startsWith(repo + path.sep) && fs.existsSync(file) && fs.statSync(file).isFile()) return route.fulfill({path: file});
    return route.fulfill({status: 404, body: ''});
  });
  try {
    await page.goto('http://turncraft.test/game/turncraft.html');
    await page.evaluate(() => tcColors.prepare());
    assert.equal(await page.evaluate(() => tcColors.runtime.loadManifest('/game/turncraft/assets/animation-manifest.json')), true);
    const result = await page.evaluate(async () => {
      const runtime = tcColors.runtime, failures = [], rows = [];
      const canvas = () => {const c=document.createElement('canvas');c.width=384;c.height=384;return c};
      const inspect = c => {
        const data=c.getContext('2d').getImageData(0,0,c.width,c.height).data;let visible=0,red=0,cyan=0,transparent=0;
        for(let i=0;i<data.length;i+=4){if(data[i+3]===0)transparent++;if(data[i+3]<200)continue;visible++;if(data[i]>data[i+1]+40&&data[i]>data[i+2]+40)red++;if(data[i+1]>data[i]+40&&data[i+2]>data[i]+60)cyan++}
        return {visible,red,cyan,transparent,data};
      };
      for (const quality of ['low','medium','high']) {
        runtime.setQuality(quality);
        for (const type of ['engineer','probe','drone','rifleman','zealot','hydralisk']) {
          for (const state of ['idle','move','attack','hit','death']) {
            const definition=runtime.metadata(type,state)?.state;
            if(!definition?.sheet){failures.push(type+'/'+state+': missing animation');continue}
            runtime.cache.get(definition.sheet,{priority:true});
            for(let wait=0;wait<200&&!runtime.cache.get(definition.sheet);wait++)await new Promise(resolve=>setTimeout(resolve,10));
            const canvases=[canvas(),canvas(),canvas()], colors=[null,'#ff334f','#35ddff'];
            for(let index=0;index<3;index++)if(!runtime.drawState(canvases[index].getContext('2d'),type,state,{direction:'S',progress:.3,size:160,x:192,y:192,tint:colors[index]}))failures.push(quality+'/'+type+'/'+state+': draw failed');
            const [own,enemy,ally]=canvases.map(inspect), label=quality+'/'+type+'/'+state;
            if(enemy.visible<50||enemy.red/enemy.visible<.98)failures.push(label+': opponent body is not strongly red');
            if(ally.visible<50||ally.cyan/ally.visible<.98)failures.push(label+': allied body is not strongly cyan');
            let alphaDifferences=0,maxAlphaDifference=0,detailLevels=new Set();
            for(let pixel=0;pixel<own.data.length;pixel+=4){const delta=Math.abs(own.data[pixel+3]-enemy.data[pixel+3]);maxAlphaDifference=Math.max(maxAlphaDifference,delta);if(delta>2)alphaDifferences++;if(enemy.data[pixel+3]>200)detailLevels.add(enemy.data[pixel])}
            if(alphaDifferences>0)failures.push(label+': tint changed transparent edges '+alphaDifferences+' pixels/max '+maxAlphaDifference);
            if(detailLevels.size<10)failures.push(label+': sprite detail flattened');
            if(enemy.transparent<canvases[0].width*canvases[0].height*.5)failures.push(label+': tinted rectangular background');
            rows.push({quality,type,state,red:enemy.red/enemy.visible,cyan:ally.cyan/ally.visible,detailLevels:detailLevels.size});
          }
        }
        if(runtime.tintCache.stats().bytes>runtime.tintCache.stats().limit)failures.push(quality+': tint cache over memory budget');
      }
      // Exercise the full game renderer, including static and missing-art fallbacks.
      tcColors.run(`players.forEach(p=>p.factionId='frontier');entities=[];[0,1,2].forEach(slot=>addEntity('unit','rifleman',slot,5+slot*3,6));selectedId=entities[0].id;selectedIds=[selectedId];updateSpritePresentation();draw()`);
      const staticCanvas=canvas(),staticImage=document.createElement('canvas');staticImage.width=32;staticImage.height=32;
      const staticContext=staticImage.getContext('2d');staticContext.fillStyle='#74a580';staticContext.beginPath();staticContext.arc(16,16,10,0,Math.PI*2);staticContext.fill();
      const staticRed=tcColors.run('playerArtwork') (staticImage,1);
      staticCanvas.getContext('2d').drawImage(staticRed,0,0);
      const staticStats=inspect(staticCanvas);
      if(staticStats.red/staticStats.visible<.98)failures.push('static fallback is not tinted');
      return {failures,rows:rows.length};
    });
    assert.equal(result.rows, 90);
    assert.deepEqual(result.failures, []);
    assert.deepEqual(errors, []);

    if (process.env.TURNCRAFT_COLOR_SCREENSHOT) {
      await page.evaluate(async () => {
        const gallery=document.createElement('canvas');gallery.width=1000;gallery.height=600;gallery.style.cssText='position:fixed;left:0;top:0;width:1000px;height:600px;max-width:none;max-height:none;z-index:99999';const context=gallery.getContext('2d');
        context.fillStyle='#07111f';context.fillRect(0,0,1000,600);context.fillStyle='#eaf5ff';context.font='bold 24px sans-serif';
        ['Your troop','Enemy player','Teammate'].forEach((label,index)=>context.fillText(label,100+index*320,42));
        const types=['engineer','probe','drone'],runtime=tcColors.runtime;runtime.setQuality('high');
        for(let row=0;row<types.length;row++){
          const type=types[row],sheet=runtime.metadata(type,'idle').state.sheet;runtime.cache.get(sheet);
          for(let wait=0;wait<200&&!runtime.cache.get(sheet);wait++)await new Promise(resolve=>setTimeout(resolve,10));
          for(let column=0;column<3;column++)runtime.drawState(context,type,'idle',{direction:'S',progress:.3,size:100,x:180+column*320,y:180+row*180,tint:[null,'#ff334f','#35ddff'][column]});
        }
        document.body.replaceChildren(gallery);document.body.style.cssText='margin:0;background:#07111f';
      });
      await page.screenshot({path: process.env.TURNCRAFT_COLOR_SCREENSHOT, clip: {x: 0, y: 0, width: 1000, height: 600}});
    }
  } finally {await browser.close()}
});
