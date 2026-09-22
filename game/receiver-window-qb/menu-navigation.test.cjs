const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
function fixture(){
  let listener;
  const doc={activeElement:null,body:{dataset:{phase:'call'}},getElementById:id=>panels[id],querySelector:selector=>selector.startsWith('script[src=')?{}:null,querySelectorAll:()=>[],addEventListener:(type,fn)=>{listener=fn}};
  const element=(x,y)=>({hidden:false,tagName:'BUTTON',getClientRects:()=>[1],getBoundingClientRect:()=>({x,y,width:80,height:40}),focus(){doc.activeElement=this},scrollIntoView(){},click(){this.clicked=(this.clicked||0)+1}});
  const a=element(0,0),b=element(100,0),c=element(0,60),input=element(0,120);input.tagName='INPUT';
  const panels={hud:{...element(0,0),querySelectorAll:()=>[a,b,c]},saveLayer:{...element(0,0),hidden:true,querySelectorAll:()=>[input]}};
  vm.runInNewContext(fs.readFileSync(__dirname+'/menu-navigation.js','utf8'),{document:doc,getComputedStyle:()=>({visibility:'visible'}),MutationObserver:class {observe(){}}});
  return {doc,panels,a,b,c,input,key(key,repeat=false){const e={key,repeat,preventDefault(){this.prevented=true},stopImmediatePropagation(){this.stopped=true}};listener(e);return e}};
}
test('arrows follow menu geometry, Enter activates once, Tab cycles',()=>{
 const f=fixture();f.key('ArrowDown');assert.equal(f.doc.activeElement,f.a);f.key('ArrowRight');assert.equal(f.doc.activeElement,f.b);f.key('ArrowDown');assert.equal(f.doc.activeElement,f.c);f.key('Enter');f.key('Enter',true);assert.equal(f.c.clicked,1);f.key('Tab');assert.equal(f.doc.activeElement,f.a);
});
test('top modal traps navigation, text fields retain editing, live input is untouched',()=>{
 const f=fixture();f.panels.saveLayer.hidden=false;f.key('ArrowDown');assert.equal(f.doc.activeElement,f.input);assert.equal(f.key('ArrowLeft').prevented,undefined);f.panels.saveLayer.hidden=true;f.doc.body.dataset.phase='live';assert.equal(f.key('Enter').prevented,undefined);
});
test('replay arrow keys pass through menu navigation to the camera handler',()=>{
 const f=fixture();f.panels.replayBar={id:'replayBar',hidden:false,getClientRects:()=>[1],querySelectorAll:()=>[f.a]};
 assert.equal(f.key('ArrowRight').stopped,undefined);f.doc.activeElement=f.a;assert.equal(f.key('Enter').stopped,true);assert.equal(f.a.clicked,1);
});

test('screen tuning loader gates play buttons and restores their prior disabled state',()=>{
  for(const event of ['onload','onerror']){
    let script;const buttons={startBtn:{disabled:false},continueBtn:{disabled:true},snapBtn:{disabled:false}};
    const document={body:{dataset:{}},activeElement:null,querySelector:()=>null,querySelectorAll:()=>[],getElementById:id=>buttons[id],createElement:()=>({}),head:{appendChild:s=>{script=s}},addEventListener(){}};
    vm.runInNewContext(fs.readFileSync(__dirname+'/menu-navigation.js','utf8'),{document,getComputedStyle:()=>({visibility:'visible'}),MutationObserver:class{observe(){}}});
    assert.ok(Object.values(buttons).every(b=>b.disabled));assert.equal(script.src,'receiver-window-qb/screen-playability.js');
    script[event]();assert.equal(buttons.startBtn.disabled,false);assert.equal(buttons.continueBtn.disabled,true);assert.equal(buttons.snapBtn.disabled,false);
  }
});
