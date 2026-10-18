'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
global.NeonConquest=require('../game/neon-arena/neon-conquest.js');
const H=require('../game/neon-arena/neon-frontline-hud.js'),Q=global.NeonConquest;
const state=()=>Q.create({sectors:[{id:'A',x:0,z:0,radius:6,links:['cat_hq','B']},{id:'B',x:16,z:0,links:['A','mouse_hq']}]});
const player={x:0,y:1.86,z:0,team:1,human:true,name:'YOU'};
test('capture status follows neutralization, contested freezes and physical access',()=>{
  const s=state();s.sectors[0].progress=.42;s.sectors[0].capturing=1;
  assert.equal(H.captureStatus(s,player).text,'CAPTURING A · 42%');
  s.sectors[0].owner=2;assert.equal(H.captureStatus(s,player).text,'NEUTRALIZING A · 42%');
  s.sectors[0].contested=true;assert.equal(H.captureStatus(s,player).text,'CONTESTED · A');
  assert.equal(H.captureStatus(s,player,()=>false),null);
  assert.equal(H.captureStatus(s,{...player,y:6}),null);
  assert.equal(H.captureStatus(s,{...player,dead:true}),null);
  assert.equal(H.captureStatus(s,{...player,spawnGrace:2}),null);
});
test('disconnected sectors explain the missing front link',()=>{
  const s=state();assert.match(H.captureStatus(s,{...player,x:16}).text,/CONNECT THE FRONT/);
  s.sectors[0].owner=1;assert.match(H.captureStatus(s,{...player,x:16}).text,/CAPTURING B/);
});
test('expanded tactical map never exposes hidden live enemies or expired reports',()=>{
  const enemy={x:20,z:20,team:2},ally={x:2,z:2,team:1};
  const found=H.mapContacts(player,[player,ally,enemy,{...ally,dead:true}],[{x:10,z:10,team:1,until:6},{x:15,z:15,team:1,until:4},{x:20,z:20,team:2,until:6}],5);
  assert.equal(found.length,3);assert(found.some(p=>p.self));assert(found.some(p=>p.reported&&p.x===10));assert(!found.some(p=>p.x===20||p.x===15));
});
test('elimination cards expire and belong only to the human killer',()=>{
  const victim={team:2,name:'Cat Scout'};
  assert.deepEqual(H.killNotice(victim,player,true,100),{name:'Cat Scout',head:true,until:3300});
  assert.equal(H.killNotice(victim,{...player,human:false},false,100),null);
  assert.equal(H.killNotice(player,player,false,100),null);
  assert.equal(H.killNotice({...victim,team:1},player,false,100),null);
});
test('compact and expanded maps fit portrait, landscape and both split rectangles',()=>{
  for(const [w,h]of [[390,844],[844,390],[640,800],[390,422],[320,568]]){const l=H.layout({w,h});assert(l.mapSize+198<w);assert(l.expandedSize+30<=w);assert(l.expandedSize+112<=h);assert(l.mapTop+l.mapSize+28<h);}
});
test('landscape tactical map clears the upper Jump and Build controls',()=>{
  for(const [w,h]of [[844,390],[667,375],[422,390],[568,320]]){const l=H.layout({w,h},{touch:true}),actionTop=h-16-110;assert(l.mapTop+l.mapSize+l.mapCaptionHeight<actionTop,'map must end above the action group');}
});

test('idle mobile territory HUD uses a narrow 30px strip and an 84px or smaller map in both orientations',()=>{
  for(const [w,h]of [[390,844],[844,390],[667,375],[320,568],[390,422],[422,390]]){
    const l=H.layout({w,h},{touch:true});
    assert.equal(l.compact,true);assert.equal(l.scoreHeight,30);assert(l.scoreWidth<=350&&l.scoreWidth<=w-20);
    assert(l.mapSize>=72&&l.mapSize<=86,'collapsed map remains a useful touch target without covering the view');
    assert(l.mapSize+l.mapCaptionHeight<=100);
    const previousMap=h<500?110:w<500?126:170,oldArea=(w-16)*74+previousMap*(previousMap+28),newArea=l.scoreWidth*l.scoreHeight+l.mapSize*(l.mapSize+l.mapCaptionHeight);
    assert(newArea<oldArea*.5,'score strip and map cover less than half their former area');
    assert.equal(l.mapTop,58,'main player HUD can align with the map at y58');
    assert(l.scoreTop+l.scoreHeight+4+17<=l.mapTop,'active capture badge clears the aligned health/map row');
  }
});

test('desktop scores also stay centered and compact while expanded maps retain room for detail',()=>{
  for(const [w,h]of [[1280,800],[1920,1080],[640,800]]){
    const l=H.layout({w,h});assert(l.scoreWidth<=360);assert.equal(l.scoreHeight,30);assert(l.mapSize<=112);
    assert.equal(l.expandedSize,440);assert(l.expandedSize>l.mapSize*3);
  }
});

test('secured sectors and empty space add no persistent capture message',()=>{
  const s=state();s.sectors[0].owner=player.team;
  assert.equal(H.captureStatus(s,player),null);
  assert.equal(H.captureStatus(s,{...player,x:50,z:50}),null);
  s.sectors[0].capturing=2;s.sectors[0].progress=.3;
  assert.equal(H.captureStatus(s,player).text,'DEFEND A · ENEMY CAPTURE');
  s.sectors[0].contested=true;
  assert.equal(H.captureStatus(s,player).text,'CONTESTED · A');
});

function renderHarness(){
  const previousDocument=global.document,previousAdd=global.addEventListener,events={},changes=[];
  const context=new Proxy({}, {get:(target,key)=>target[key]||(()=>{}),set:(target,key,value)=>(target[key]=value,true)});
  class Node{
    constructor(tag){this.tagName=tag;this.children=[];this.attributes={};this.hidden=false;this.style={setProperty(name,value){this[name]=value}};const classes=new Set();this.classList={toggle(name,active){active?classes.add(name):classes.delete(name)},contains:name=>classes.has(name)};}
    append(...nodes){this.children.push(...nodes)}appendChild(node){this.children.push(node)}replaceChildren(...nodes){this.children=nodes}setAttribute(name,value){this.attributes[name]=String(value)}getContext(){return context}
  }
  global.document={createElement:tag=>new Node(tag)};global.addEventListener=(name,callback)=>{events[name]=callback};
  const host=new Node('body'),hud=H.create({host,onMapChange:(open,reason)=>changes.push({open,reason})});
  const layer=host.children[0],s=state(),args={visible:true,touch:true,rects:[{x:0,y:0,w:390,h:844,ent:{...player,x:40,z:40}}],state:s,world:{bounds:65},actors:[],sightings:[],time:0,now:100};
  return {hud,args,layer,changes,events,restore(){global.document=previousDocument;global.addEventListener=previousAdd}};
}

test('render applies compact dimensions, keeps full accessible scores, and shows capture only when relevant',()=>{
  const h=renderHarness();try{
    h.hud.render(h.args);const frame=h.layer.children[0],bar=frame.children[0],status=bar.children[2],map=frame.children[2];
    assert.equal(frame.classList.contains('compactHud'),true);assert.equal(bar.style.height,'30px');assert.equal(bar.style.width,'326px');
    assert.match(bar.attributes['aria-label'],/Cats 250, Mice 250, tickets.*15 seconds/);
    assert.equal(status.hidden,true);assert.equal(status.textContent,'');assert.equal(map.style.width,'84px');assert.equal(map.style.top,'58px');
    Object.assign(h.args.rects[0].ent,{x:0,z:0});h.args.state.sectors[0].progress=.63;h.args.state.sectors[0].capturing=1;h.hud.render(h.args);
    assert.equal(status.hidden,false);assert.equal(status.textContent,'CAPTURING A · 63%');
    const flag=bar.children[1].children[0];assert.match(flag.attributes['aria-label'],/capture 63%/);
    h.args.state.sectors[0].owner=1;h.hud.render(h.args);assert.equal(status.hidden,true);
  }finally{h.restore()}
});

test('smaller map retains expand, collapse, keyboard and overlay-close behavior',()=>{
  const h=renderHarness();try{
    h.hud.render(h.args);const map=h.layer.children[0].children[2];map.onclick();
    assert.equal(h.hud.expanded,true);assert.equal(map.style.width,'360px');assert.equal(map.attributes['aria-expanded'],'true');
    assert.equal(map.children[1].textContent,'CLOSE MAP ×');
    h.events.keydown({code:'Escape',preventDefault(){}});assert.equal(h.hud.expanded,false);assert.equal(map.style.width,'84px');assert.equal(h.changes.at(-1).reason,'user');
    h.events.keydown({code:'KeyM',target:{tagName:'BODY'},preventDefault(){}});assert.equal(h.hud.expanded,true);
    h.hud.render({...h.args,visible:false});assert.equal(h.layer.hidden,true);assert.equal(h.hud.expanded,false);assert.equal(h.changes.at(-1).reason,'hidden');
  }finally{h.restore()}
});

test('compact elimination notices preserve full name and headshot text only for their own player',()=>{
  const h=renderHarness();try{
    const viewer=h.args.rects[0].ent;
    h.hud.recordKill({name:'Cat Heavy Enforcer',team:2},viewer,true,100);
    h.hud.render(h.args);const toast=h.layer.children[0].children[3],card=toast.children[0];
    assert.equal(card.children[0].textContent,'HEADSHOT ELIMINATION');assert.equal(card.children[1].textContent,'Cat Heavy Enforcer');
    assert.equal(toast.attributes['aria-live'],'polite');
    h.hud.render({...h.args,now:3400});assert.equal(toast.children.length,0);
  }finally{h.restore()}
});
