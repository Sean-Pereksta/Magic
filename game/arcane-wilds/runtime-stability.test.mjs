import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname,join} from 'node:path';
import vm from 'node:vm';

const here=dirname(fileURLToPath(import.meta.url));
const source=readFileSync(join(here,'runtime-stability.js'),'utf8');
const mobileInteraction=readFileSync(join(here,'mobile-interaction.js'),'utf8');

function harness({frameThrows=false}={}){
  let scheduled=null;
  const context={
    window:{arcaneWildsPerformance:{low:false}},
    game:{effects:[],particles:[]},
    running:true,
    performance:{now:()=>1000},
    Date,
    Number,
    console:{error(){}},
    toastMsg(){},
    requestAnimationFrame(fn){scheduled=fn;return 1;},
    drawEffect(e){
      if(e.kind==='puppetString'){
        const x=Math.sin(4+e.branch);
        if(!Number.isFinite(x))throw new TypeError('non-finite canvas coordinate');
      }
    },
    loop(){if(frameThrows)throw new Error('render exploded');}
  };
  vm.createContext(context);
  vm.runInContext(source,context);
  return {context,getScheduled:()=>scheduled};
}

test('Rift Puppeteer strings are normalized before strict canvas rendering',()=>{
  const {context}=harness();
  const effect={kind:'puppetString',x:4,y:5,life:.8};
  assert.doesNotThrow(()=>context.drawEffect(effect,false));
  assert.equal(effect.branch,0);
});

test('broken cosmetic effects are quarantined instead of escaping render',()=>{
  const {context}=harness();
  const effect={kind:'other',x:Number.NaN,y:5,life:.8};
  assert.doesNotThrow(()=>context.drawEffect(effect,false));
  assert.equal(effect.life,0);
});

test('a frame exception re-arms the animation loop instead of hard freezing',()=>{
  const {context,getScheduled}=harness({frameThrows:true});
  assert.doesNotThrow(()=>context.loop(16));
  assert.equal(getScheduled(),context.loop);
  assert.equal(context.window.arcaneWildsPerformance.low,true);
});

test('runtime stability loads after the performance and navigation wrappers',()=>{
  const performance=mobileInteraction.indexOf('arcane-wilds/performance.js');
  const navigation=mobileInteraction.indexOf('arcane-wilds/navigation-clarity.js');
  const stability=mobileInteraction.indexOf('arcane-wilds/runtime-stability.js');
  assert.ok(performance>=0&&navigation>performance&&stability>navigation);
});
