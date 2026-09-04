import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname,join} from 'node:path';
import vm from 'node:vm';

const here=dirname(fileURLToPath(import.meta.url));
const source=readFileSync(join(here,'mobile-runtime.js'),'utf8');
const html=readFileSync(join(here,'..','arcane-wilds.html'),'utf8');
const interaction=readFileSync(join(here,'mobile-interaction.js'),'utf8');

function startupHarness(){
  let baseRenderCalls=0,baseBeginCalls=0,scheduled=null;
  const ctx={
    save(){},restore(){},setTransform(){},fillRect(){},
    globalCompositeOperation:'source-over',fillStyle:'#000'
  };
  const context={
    isTouch:true,
    window:{},
    document:{documentElement:{classList:{add(){}}}},
    running:false,
    ctx,
    canvas:{width:390,height:844},
    W:390,H:844,
    render(){baseRenderCalls++},
    beginWorld(){baseBeginCalls++},
    bindStick(){},
    requestAnimationFrame(fn){scheduled=fn;return 1;},
    $(){return null},
    Math
  };
  vm.createContext(context);
  vm.runInContext(source,context);
  return {
    context,
    counts:()=>({baseRenderCalls,baseBeginCalls}),
    runScheduled:()=>{const fn=scheduled;scheduled=null;fn?.()}
  };
}

test('mobile bootstrap does not execute the expensive renderer before the governor loads',()=>{
  const h=startupHarness();
  h.context.render();
  assert.equal(h.counts().baseRenderCalls,0);
  h.context.window.__arcaneWildsPerformanceLoaded=true;
  h.context.render();
  assert.equal(h.counts().baseRenderCalls,1);
});

test('mobile beginWorld waits for the governor before entering the first live frame',()=>{
  const h=startupHarness();
  h.context.beginWorld();
  assert.equal(h.counts().baseBeginCalls,0);
  h.runScheduled();
  assert.equal(h.counts().baseBeginCalls,0);
  h.context.window.__arcaneWildsPerformanceLoaded=true;
  h.runScheduled();
  assert.equal(h.counts().baseBeginCalls,1);
});

test('mobile runtime still loads before ui startup render and governor remains in the late loader chain',()=>{
  const mobile=html.indexOf('arcane-wilds/mobile-runtime.js');
  const ui=html.indexOf('arcane-wilds/ui.js');
  assert.ok(mobile>=0&&ui>mobile);
  assert.ok(interaction.includes("script.src='arcane-wilds/performance.js'"));
  assert.ok(interaction.includes("script.src='arcane-wilds/runtime-stability.js'"));
});
