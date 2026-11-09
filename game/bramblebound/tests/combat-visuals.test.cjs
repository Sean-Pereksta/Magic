const test=require('node:test');
const assert=require('node:assert/strict');
const vm=require('node:vm');
const fs=require('node:fs');
const path=require('node:path');

// Exercise the real renderer through a minimal canvas recorder. This catches
// invalid geometry without depending on a particular browser's rasterizer.
function renderer(){
  function canvas(record=false){
    const out={width:560,height:420,clientWidth:560,clientHeight:420,commands:[]},state=[];
    const g={canvas:out,globalAlpha:1,fillStyle:'#000000'};
    for(const name of ['fillRect','clearRect','translate','rotate','scale','strokeText','fillText'])g[name]=(...args)=>{
      for(const n of args)if(typeof n==='number')assert.ok(Number.isFinite(n),name+' requires finite geometry');
      if(record)out.commands.push([name,g.fillStyle,...args]);
    };
    g.drawImage=(image,...args)=>{for(const n of args)assert.ok(Number.isFinite(n));if(record)out.commands.push(['image',image.width,image.height,...args]);};
    g.save=()=>state.push({globalAlpha:g.globalAlpha,fillStyle:g.fillStyle});
    g.restore=()=>Object.assign(g,state.pop());
    out.getContext=()=>g;return out;
  }
  const context=vm.createContext({document:{createElement:()=>canvas()}});context.window=context;
  vm.runInContext(fs.readFileSync(path.join(__dirname,'..','catalog.js'),'utf8'),context);
  vm.runInContext(fs.readFileSync(path.join(__dirname,'..','world.js'),'utf8'),context);
  const enemy={id:'moss-mite',uid:7,lane:0,species:'beetle',color:'#8caa77',name:'Target',hp:40,maxHp:40,attackProgress:.3};
  return (effects,reducedMotion=false)=>{const c=canvas(true);context.WorldArt.draw(c,{time:1,round:1,phase:'fight',region:0,enemies:[enemy],waveSize:1,encounterAge:1,effects,reducedMotion});return c.commands;};
}

test('all elemental sparks and expedition powers draw finite, deterministic frames',()=>{
  const effects=['leaf','fire','frost','storm','stone','spirit'].map((element,i)=>({kind:'attack',itemId:'stick',targetUid:7,element,age:.27,strength:i+1}));
  for(const [pattern,element,secondaryElement] of [['emberglass','fire','frost'],['bramble_circuit','leaf','storm'],['soulstone','stone','spirit']])effects.push({kind:'synergy',pattern,element,secondaryElement,age:.3,strength:2,...(pattern==='soulstone'?{}:{targetUid:7})});
  for(const [pattern,element] of [['moss_tonic','leaf'],['bark_ward','stone'],['ember_bomb','fire'],['frost_bell','frost'],['quicksilver','storm']])effects.push({kind:'consumable',pattern,element,age:.3,...(['ember_bomb','frost_bell'].includes(pattern)?{targetUid:7}:{})});
  for(const [pattern,element] of [['moss_tonic','leaf'],['bark_ward','stone'],['ember_bomb','fire'],['frost_bell','frost'],['quicksilver','storm']])effects.push({kind:'supply-find',pattern,element,age:.3,outcome:'packed'});
  assert.deepEqual(renderer()(effects),renderer()(effects),'effect geometry must depend only on simulation inputs');
});

test('a large event burst retains a bounded rendering workload',()=>{
  const event={kind:'hit',targetUid:7,element:'fire',age:.3,strength:10000};
  const bounded=renderer()(Array.from({length:62},()=>({...event}))),crowded=renderer()(Array.from({length:1000},()=>({...event})));
  assert.deepEqual(crowded,bounded,'old effects are culled and extra spark work stays bounded');
});

test('reduced motion keeps new power feedback stationary and removes flying sparks',()=>{
  const powers=[{kind:'synergy',pattern:'emberglass',element:'fire',secondaryElement:'frost',targetUid:7,age:.3},
    {kind:'consumable',pattern:'moss_tonic',element:'leaf',age:.3},
    {kind:'consumable',pattern:'quicksilver',element:'storm',age:.3},
    {kind:'supply-find',pattern:'ember_bomb',element:'fire',outcome:'packed',age:.3}];
  const earlier=renderer()(powers,true),later=renderer()(powers.map(event=>({...event,age:.54})),true);
  assert.deepEqual(earlier,later,'reduced motion power glyphs stay still while their opacity fades');
  assert.ok(earlier.length<renderer()(powers).length,'flying debris is disabled');
});

test('Soulstone and collected supply labels appear over Pip',()=>{
  for(const event of [{kind:'synergy',pattern:'soulstone',element:'stone',label:'Ward +2',age:.3},
    {kind:'supply-find',pattern:'moss_tonic',element:'leaf',label:'Moss Tonic found',outcome:'packed',age:.3}]){
    const commands=renderer()([event],true),label=commands.find(command=>command[0]==='fillText'&&command[2]===event.label);
    assert.ok(label,'power label is visible');assert.ok(label[3]<130,'power label anchors to Pip instead of the enemy');
  }
});
