import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import { createPatchWriter, stateFieldPatch, eligibleHostCandidate, reconcileKeyedNodes } from './sync.mjs';
import { createTacticalDirector, findOpenSpawn, stableHash } from './tactics.mjs';
import { createCombatPresentation } from './combat-presentation.mjs';

// Small deterministic DOM/clock fixture: executes the entire actual optimized
// module, including its real unit factory, buffered BFS and combat entrypoints.
// It validates wiring/logic, not CSS layout, frame rate or a live SDK session.
class Element {
  constructor(document){this.ownerDocument=document;this.children=[];this.dataset={};this.className='';this.textContent='';this.clientWidth=900;this.clientHeight=650;this.attributes={};this.listeners={};
    this.style={setProperty(key,value){this[key]=value;}};
    this.classList={contains:name=>this.className.split(/\s+/).includes(name),add:(...names)=>{this.className=[...new Set([...this.className.split(/\s+/),...names])].join(' ').trim();},remove:(...names)=>{this.className=this.className.split(/\s+/).filter(n=>!names.includes(n)).join(' ');},toggle:(name,on)=>on?this.classList.add(name):this.classList.remove(name)};}
  set id(value){this._id=value;this.ownerDocument.ids.set(value,this);}get id(){return this._id;}
  get firstChild(){return this.children[0] || null;}
  set innerHTML(value){this.replaceChildren();this._html=value;
    for(const match of value.matchAll(/<[a-z][a-z0-9-]*\b([^>]*)>/gi)){const child=this.ownerDocument.createElement('div'),attrs=match[1];
      child.className=attrs.match(/class="([^"]*)"/)?.[1] || '';const id=attrs.match(/id="([^"]*)"/)?.[1];if(id)child.id=id;this.appendChild(child);}}
  get innerHTML(){return this._html || '';}
  appendChild(node){node.remove();this.children.push(node);node.parentNode=this;return node;}
  append(...nodes){for(const node of nodes)this.appendChild(node);}prepend(node){node.remove();this.children.unshift(node);node.parentNode=this;}
  remove(){if(this.parentNode)this.parentNode.children=this.parentNode.children.filter(n=>n!==this);this.parentNode=null;}
  replaceChildren(...nodes){for(const child of this.children)child.parentNode=null;this.children=[];this.append(...nodes);}
  setAttribute(name,value){this.attributes[name]=String(value);}getAttribute(name){return this.attributes[name];}
  addEventListener(name,callback){this.listeners[name]=callback;}
  insertAdjacentHTML(_where,html){const tmp=this.ownerDocument.createElement('div');tmp.innerHTML=html;for(const node of [...tmp.children])this.appendChild(node);}
  matches(selector){return [...selector.matchAll(/\.([\w-]+)/g)].every(m=>this.classList.contains(m[1])) && [...selector.matchAll(/\[data-([\w-]+)="([^"]*)"\]/g)].every(m=>this.dataset[m[1].replace(/-([a-z])/g,(_,c)=>c.toUpperCase())]===m[2]);}
  querySelectorAll(selector){const result=[];for(const child of this.children){if(selector.split(',').some(s=>child.matches(s)))result.push(child);result.push(...child.querySelectorAll(selector));}return result;}
  querySelector(selector){return this.querySelectorAll(selector)[0] || null;}
}
async function runtime(reduced=false) {
  let time=1791097200000,nextId=0;const frames=new Map(),warnings=[];
  const document={ids:new Map(),hidden:false,createElement(){return new Element(this);},getElementById(id){if(!this.ids.has(id)){const node=this.createElement();node.id=id;}return this.ids.get(id);},addEventListener(){},querySelectorAll(){return [];},querySelector(){return null;}};
  const window={location:{search:'?gameId=fixture&username=tester'},addEventListener(){},matchMedia:()=>({matches:reduced,addEventListener(){}})};
  class ClockDate extends Date {static now(){return time;}}
  const unexpected=()=>{throw Error('unexpected transport write');};
  const context=vm.createContext({window,document,navigator:{onLine:true,userAgent:'fixture'},localStorage:{getItem:()=>null,setItem(){}},URLSearchParams,Date:ClockDate,
    Audio:class{play(){return Promise.resolve();}pause(){}},console:{warn:(...args)=>warnings.push(args),error:(...args)=>warnings.push(args),log(){}},
    setTimeout:()=>++nextId,clearTimeout(){},setInterval:()=>++nextId,clearInterval(){},requestAnimationFrame:fn=>{const id=++nextId;frames.set(id,fn);return id;},cancelAnimationFrame:id=>frames.delete(id),
    crypto:{randomUUID:()=>String(++nextId)},createPatchWriter,stateFieldPatch,eligibleHostCandidate,reconcileKeyedNodes,createTacticalDirector,findOpenSpawn,stableHash,createCombatPresentation,
    initializeApp:()=>({}),getAuth:()=>({currentUser:{uid:'test-player'}}),signInAnonymously:async()=>{},getFirestore:()=>({}),doc:(_db,path)=>({id:path.split('/').pop()}),collection:()=>({}),
    getDoc:async()=>({exists:()=>false}),getDocs:async()=>({docs:[]}),setDoc:unexpected,updateDoc:unexpected,deleteDoc:unexpected,writeBatch:unexpected,onSnapshot:()=>()=>{},runTransaction:unexpected,increment:n=>n
  });
  const loader=readFileSync(new URL('../catandmouse.html',import.meta.url),'utf8');let optimized;
  await vm.runInNewContext(loader.match(/<script>([\s\S]*)<\/script>/)[1],{document:{getElementById:()=>({}),open(){},write:html=>optimized=html,close(){}},fetch:async()=>({ok:true,text:async()=>readFileSync(new URL('../catandmouse-core.html',import.meta.url),'utf8')}),console:{warn:assert.fail,error:assert.fail}});
  let script=optimized.match(/<script type="module">([\s\S]*?)<\/script>/)[1].replace(/^  import[\s\S]*?;\n/gm,'');
  const browser=readFileSync(new URL('./browser-smoke.mjs',import.meta.url),'utf8');
  const scenario=browser.match(/const fixture=String\.raw`([\s\S]*?)`;/)[1];
  script=script.slice(0,script.indexOf('  bootGame().catch(err => {'))+scenario;
  await vm.runInContext(`(async()=>{${script}\n})();`,context);
  const fixture=window.__battleFixture;fixture.seed();
  return {fixture,document,warnings,frame(ms){time+=ms;const callbacks=[...frames.values()];frames.clear();for(const callback of callbacks)callback(time);},advance(ms){time+=ms;}};
}

test('whole optimized engine executes a mixed battlefield without transport writes or stale rat double steps',async()=>{
  const h=await runtime();
  assert.ok(h.document.getElementById('unitLayer').children.length>=10);
  const tick=await h.fixture.tick();assert.ok(tick.maxRatStep<=1);assert.equal(tick.invalidSpawn,false);assert.ok(tick.rabbits<=12);
  const before=h.fixture.authority();h.frame(80);h.frame(80);
  assert.equal(h.fixture.authority(),before);assert.deepEqual(h.warnings,[]);
  const stats=h.fixture.stats();assert.ok(stats.ai.peakDecisions<=5);assert.ok(stats.ai.peakPaths<=10);
});
test('real unit factory preserves remote wrappers and interpolates their received movement',async()=>{
  const h=await runtime(),layer=h.document.getElementById('unitLayer');
  const remote=layer.children.find(n=>n.dataset.unitKey==='mouse:remote');
  h.fixture.moveRemote();h.frame(55);const position=h.fixture.rendered('mouse:remote');
  assert.ok(position.x>10 && position.x<11);assert.ok(layer.children.includes(remote));
  h.frame(200);assert.equal(h.fixture.rendered('mouse:remote').x,11);
});
test('local input starts visual movement before the next coalesced board redraw',async()=>{
  const h=await runtime();h.fixture.moveLocal();h.frame(30);
  const position=h.fixture.rendered('mouse:test-player');
  assert.ok(position.x>9 && position.x<10);h.frame(100);assert.equal(h.fixture.rendered('mouse:test-player').x,10);
});
test('actual tower/structure wiring retains reduced-motion markers and bounded effect cleanup',async()=>{
  const h=await runtime(true);h.fixture.warn();h.fixture.fx();h.fixture.hitStructure();h.frame(40);
  const effects=h.document.getElementById('combatEffectLayer');
  assert.ok(effects.children.some(n=>n.classList.contains('cm-marker')));assert.ok(effects.children.some(n=>n.classList.contains('cm-projectile')));
  assert.ok(h.document.getElementById('structureLayer').children.some(n=>n.classList.contains('cm-damaged')));
  h.fixture.overload();h.frame(30);assert.ok(h.fixture.stats().presentation.effects<=128);assert.equal(h.fixture.stats().presentation.reducedMotion,true);
  h.fixture.finish();h.frame(7000);h.frame(1000);assert.equal(h.fixture.stats().presentation.effects,0);assert.deepEqual(h.warnings,[]);
});
test('mixed waves repeatedly run within caps, consume friendly spawns and keep metadata local',async()=>{
  const h=await runtime();
  for(let i=0;i<6;i++){
    h.advance(900);const tick=await h.fixture.tick();h.frame(200);
    assert.ok(tick.maxRatStep<=1);assert.equal(tick.invalidSpawn,false);assert.ok(tick.rabbits<=12);
  }
  assert.ok(h.fixture.stats().ai.peakDecisions<=5);assert.ok(h.fixture.stats().ai.peakPaths<=10);assert.deepEqual(h.warnings,[]);
});
