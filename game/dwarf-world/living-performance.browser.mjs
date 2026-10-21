import fs from 'node:fs/promises';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
const {chromium}=require('playwright');
const core=await fs.readFile(new URL('../DwarfWorld-core.html',import.meta.url),'utf8');
const helpers=await fs.readFile(new URL('./living-performance.js',import.meta.url),'utf8');
let html=core.replace('<script src="./dwarf-world/living-performance.js"></script>',`<script>${helpers}</script>`);
const closing=html.lastIndexOf('})();');
html=html.slice(0,closing)+`
window.__performanceTest={
  snapshot:()=>({positions:dwarves.map(d=>({x:d.x,y:d.y})),chunks:terrainChunks.size,nav:{...livingNav.stats},forge:!!UI._forgeInit}),
  checkTerrain:()=>{
    let target=null;
    for(let y=fortress.y-12;y<=fortress.y+12&&!target;y++)for(let x=fortress.x-12;x<=fortress.x+12;x++)
      if(tSeen[idx(x,y)]&&isDiggable(tType[idx(x,y)])){target={x,y};break;}
    if(!target)throw Error('No visible rock fixture');
    draw();const key=Math.floor(target.x/TERRAIN_CHUNK)+','+Math.floor(target.y/TERRAIN_CHUNK);
    const chunk=terrainChunks.get(key);if(!chunk)throw Error('Missing cached terrain');
    mineTile(target.x,target.y,1);const invalidated=chunk.dirty;draw();return invalidated&&!chunk.dirty;
  },
  checkForge:()=>{const node=UI.forgeCraftBtn;for(let i=0;i<20;i++)flushUI(performance.now()+i);return node===UI.forgeCraftBtn;},
  checkOre:()=>{
    const cx=fortress.x+100,cy=fortress.y+100;
    for(let y=cy-4;y<=cy+4;y++)for(let x=cx-4;x<=cx+4;x++)tType[idx(x,y)]=(Math.abs(x-cx)===4||Math.abs(y-cy)===4||x===cx+1)?TILE_ROCK:TILE_EMPTY;
    tType[idx(cx+3,cy)]=TILE_DIAMOND;tType[idx(cx-2,cy)]=TILE_GOLD;
    oreScansLeft=1;oreScanCache.clear();
    const result=findPathableOreTargetAndPath(cx,cy,4);
    return !!result&&result.ore.x===cx-2&&result.path.every(p=>canTraverse(p.x,p.y));
  },
  persist:async()=>{await saveGameAsync();await loadGameAsync();return dwarves.length===4;},
  pause:()=>{paused=true;},
  clock:simulationClock
};
`+html.slice(closing);
const browser=await chromium.launch({headless:true,...(process.env.PLAYWRIGHT_CHANNEL?{channel:process.env.PLAYWRIGHT_CHANNEL}:{})});
try{
  const page=await browser.newPage({viewport:{width:1280,height:800}});
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.route('http://dwarfworld.test/**',route=>route.fulfill({contentType:'text/html',body:html}));
  await page.goto('http://dwarfworld.test/performance');
  await page.waitForFunction(()=>window.__performanceTest?.snapshot().chunks>0);
  const before=await page.evaluate(()=>window.__performanceTest.snapshot());
  await page.waitForTimeout(2500);
  const after=await page.evaluate(()=>window.__performanceTest.snapshot());
  assert.equal(before.forge,true);
  assert.ok(after.chunks<=112);
  assert.ok(after.positions.some((p,i)=>Math.abs(p.x-before.positions[i].x)+Math.abs(p.y-before.positions[i].y)>0.1),'dwarves continue moving');
  assert.equal(await page.evaluate(()=>window.__performanceTest.checkTerrain()),true,'mining invalidates and rebuilds the correct chunk');
  assert.equal(await page.evaluate(()=>window.__performanceTest.checkForge()),true,'forge nodes survive repeated UI flushes');
  assert.equal(await page.evaluate(()=>window.__performanceTest.checkOre()),true,'ore scan returns reachable ore across a local queue with a blocked richer target');
  assert.equal(await page.evaluate(()=>window.__performanceTest.persist()),true,'save and reload preserves dwarves');
  assert.deepEqual(errors,[]);
  console.log(JSON.stringify({result:'passed',viewport:'1280×800',terrainChunks:after.chunks,checks:['boot without exceptions','dwarf movement','terrain invalidation','stable forge DOM','save/load']},null,2));
}finally{await browser.close();}
