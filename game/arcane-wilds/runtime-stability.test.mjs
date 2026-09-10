import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname,join} from 'node:path';
import vm from 'node:vm';

const here=dirname(fileURLToPath(import.meta.url));
const source=readFileSync(join(here,'runtime-stability.js'),'utf8');
const mobileInteraction=readFileSync(join(here,'mobile-interaction.js'),'utf8');

function harness({frameThrows=false,movementState=false,partialInput=false}={}){
  let scheduled=null;
  const context={
    window:{arcaneWildsPerformance:{low:false},...(partialInput?{AWInput:{}}:{})},
    game:{
      effects:[],
      particles:[],
      player:movementState?{x:4,y:5,dodgeTime:.2,facing:{x:0,y:1}}:null
    },
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
    }
  };
  context.loop=function(){
    if(frameThrows)throw new Error('render exploded');
    if(movementState){
      const p=context.game.player;
      let dir={x:0,y:0};
      if(p.dodgeTime>0)dir=p.dodgeDir;
      p.x+=dir.x;
      p.y+=dir.y;
    }
    if(partialInput){
      context.window.AWInput.move.x+=0;
      context.window.AWInput.move.y+=0;
    }
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

test('active dodge without dodgeDir is repaired before playerMovement dereferences dir.x',()=>{
  const {context}=harness({movementState:true});
  assert.doesNotThrow(()=>context.loop(16));
  assert.equal(context.game.player.dodgeDir.x,0);
  assert.equal(context.game.player.dodgeDir.y,1);
  assert.equal(context.game.player.x,4);
  assert.equal(context.game.player.y,6);
});

test('partial AWInput state gets a neutral move vector before simulation',()=>{
  const {context}=harness({partialInput:true});
  assert.doesNotThrow(()=>context.loop(16));
  assert.equal(context.window.AWInput.move.x,0);
  assert.equal(context.window.AWInput.move.y,0);
});

test('a frame exception re-arms the animation loop instead of hard freezing',()=>{
  const {context,getScheduled}=harness({frameThrows:true});
  assert.doesNotThrow(()=>context.loop(16));
  assert.equal(getScheduled(),context.loop);
  assert.equal(context.window.arcaneWildsPerformance.low,true);
});

test('runtime stability loads after the performance and navigation wrappers',()=>{
  const html=readFileSync(join(here,'..','arcane-wilds.html'),'utf8');
  const performance=html.indexOf('arcane-wilds/performance.js');
  const navigation=html.indexOf('arcane-wilds/navigation-clarity.js');
  const stability=html.indexOf('arcane-wilds/runtime-stability.js');
  assert.ok(performance>=0&&navigation>performance&&stability>navigation);
});
