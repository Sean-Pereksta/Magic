import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('hosted-art-world.js',import.meta.url),'utf8');
const catalogContext={window:{}};vm.createContext(catalogContext);vm.runInContext(readFileSync(new URL('hosted-art-catalog.js',import.meta.url),'utf8'),catalogContext);
const catalog=catalogContext.window.AWArtCatalog;
function harness(){
  const calls={prop:[],object:[],floor:[],details:[],draw:[],shadow:[],labels:[],invalidations:0},entries=new Map(),ready=new Map(),stack=[];
  const ctx=new Proxy({globalAlpha:.8,save(){stack.push(this.globalAlpha);},restore(){this.globalAlpha=stack.pop();},fillText(...args){calls.labels.push(args);}},{get(target,key){return key in target?target[key]:()=>{};}});
  const context={Math,Number,Map,Promise,console,elapsed:1,isTouch:false,ctx,TILE_W:96,TILE_H:48,ROOM_W:18,ROOM_H:14,
    game:{player:{x:4,y:5},roomData:{campaignNode:'verdant-road',biome:'forest',x:2,y:3,seed:110}},
    AWCampaignData:{nodes:{'verdant-road':{continent:'verdant'},'meridian-road':{continent:'meridian'},'gloam-road':{continent:'gloam'},'shadow:root':{shadow:true,continent:'verdant'}}},
    AWArtRenderer:{armorId:()=> 'armor-verdant-oath'},AWPresentation:{camera:{zoom:.75},quality:'high'},AWModernUI:{},
    AWWorld:{caching:false,floor:(...args)=>calls.floor.push(args),invalidate(){calls.invalidations++;},propOpacity:()=>.3},
    AWArt:{entry:id=>entries.get(id),get:id=>ready.get(id),url:path=>'https://art.test/'+path.replace(/^assets\//,''),ensure:()=>Promise.resolve(null),draw(id,canvas,x,y,options){calls.draw.push({id,x,y,...options,alpha:canvas.globalAlpha});return ready.has(id);}},
    worldToScreen:(x,y)=>({x:x*10,y:y*10}),dist:(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),shadowAt:(...args)=>calls.shadow.push(args),hash2:(x,y,seed)=>(Math.abs(x*5+y*3+seed)%97)/97,
    diamondPath(){},drawProp:(...args)=>calls.prop.push(args),drawInteractable:(...args)=>calls.object.push(args),drawFloorDetails:(...args)=>calls.details.push(args),gearCard:item=>`<div class="gear-card"><div class="gear-preview">${item?.icon||''}</div></div>`};
  context.window=context;vm.createContext(context);vm.runInContext(source,context);
  function atlas(id,frames){const meta={width:2048,height:2048,frames};entries.set(id,{image:'assets/'+id+'.png',meta});ready.set(id,{meta,image:{},frames:new Map(frames.map(f=>[f.id,f]))});}
  return {context,calls,entries,ready,atlas,world:context.AWArtWorld,ui:context.AWArtUI};
}

test('continent selection follows room metadata, including shadow rooms independently of their origin',()=>{
  const h=harness();
  for(const [campaignNode,region,asset] of [['verdant-road','verdant','terrain-verdant-reach'],['meridian-road','meridian','terrain-shattered-meridian'],['gloam-road','gloam','terrain-astral-gloam'],['shadow:root','shadow','terrain-shadow-realms']]){
    assert.equal(h.world.regionFor({campaignNode}),region);
    assert.equal(h.world.groundSelection({campaignNode,biome:'forest'}).asset,asset);
  }
  h.context.AWArt.regionFor=()=> 'shadow';assert.equal(h.world.regionFor({}), 'shadow','loader region classification takes precedence');
});

test('unavailable and unmapped scenery keep the current renderer without starting image loads per frame',()=>{
  const h=harness(),p={type:'oak',x:4,y:5,scale:1};
  h.context.drawProp(p,'forest');h.context.drawProp({...p,type:'house'},'town');
  assert.equal(h.calls.prop.length,2);assert.equal(h.calls.draw.length,0);assert.equal(h.calls.shadow.length,0);
  h.context.drawInteractable({type:'shadowPortal',x:1,y:1});assert.equal(h.calls.object.length,1);
});

test('hosted props retain ground position, camera scale, occlusion opacity and context state',()=>{
  const h=harness();h.atlas('terrain-verdant-reach',[{id:'verdant-reach/oak-tree',frame:{x:0,y:0,w:512,h:512},contentBounds:{x:0,y:0,w:220,h:440}}]);
  const p={type:'oak',x:4,y:5,scale:1.2,phase:0,depth:7};h.context.drawProp(p,'forest');
  assert.equal(h.calls.prop.length,0);assert.equal(h.calls.draw.length,1);assert.equal(h.calls.draw[0].x,40);
  assert.equal(h.calls.draw[0].y,50+Math.sin(1.2)*1.2);
  assert.equal(h.calls.draw[0].scale,55/440*1.2*.75);assert.equal(h.calls.draw[0].alpha,.8*.3);
  assert.equal(h.context.ctx.globalAlpha,.8);assert.equal(p.depth,7);assert.equal(h.calls.shadow.length,1);
});

test('hosted ordinary interactions preserve prompt ownership and used/opened state',()=>{
  const h=harness(),o={type:'chest',x:4,y:5,label:'Arcane Cache'};
  h.atlas('terrain-verdant-reach',[{id:'verdant-reach/wooden-chest',rect:{x:0,y:0,w:512,h:512},contentBounds:{x:0,y:0,w:180,h:200}}]);
  h.context.drawInteractable(o);assert.equal(h.calls.draw.length,1);assert.equal(h.calls.object.length,0);assert.equal(h.calls.labels.length,0,'modern UI owns the prompt');
  for(const status of [{used:true},{opened:true},{ref:{opened:true}}])h.context.drawInteractable({...o,...status});
  assert.equal(h.calls.draw.length,1);assert.equal(h.calls.object.length,3,'existing renderer owns completed object state');
  delete h.context.AWModernUI;h.context.drawInteractable(o);assert.equal(h.calls.labels.at(-1)[0],'E • Arcane Cache');
  h.context.isTouch=true;h.context.drawInteractable(o);assert.equal(h.calls.labels.at(-1)[0],'USE • Arcane Cache');
  h.context.drawInteractable({...o,x:15});assert.equal(h.calls.labels.length,2,'far objects do not show the legacy prompt');
  assert.equal(h.context.ctx.globalAlpha,.8);
});

test('new terrain readiness invalidates the cached floor exactly once and changing continents invalidates it',()=>{
  const h=harness(),room=h.context.game.roomData;
  h.context.AWWorld.floor(room,{});h.context.AWWorld.floor(room,{});assert.equal(h.calls.invalidations,1);
  h.atlas('terrain-verdant-reach',[]);h.context.AWWorld.floor(room,{});h.context.AWWorld.floor(room,{});assert.equal(h.calls.invalidations,2);
  h.context.AWWorld.floor({campaignNode:'meridian-road'},{});assert.equal(h.calls.invalidations,3);assert.equal(h.calls.floor.length,5);
});

test('floor patches stay decorative and avoid town roads and homestead plans',()=>{
  const h=harness(),room=h.context.game.roomData;
  h.atlas('terrain-verdant-reach',[{id:'verdant-reach/mossy-grass',frame:{x:0,y:0,w:512,h:512},contentBounds:{x:20,y:40,w:300,h:220}}]);
  h.context.drawFloorDetails(room,{});assert.equal(h.calls.details.length,1);assert.equal(h.calls.draw.length,12);
  assert.equal(h.context.ctx.globalAlpha,.8);assert.ok(h.calls.draw.every(d=>d.frame==='verdant-reach/mossy-grass'&&d.x>=25&&d.y>=25));
  h.context.drawFloorDetails({...room,town:true},{});h.context.drawFloorDetails({...room,homestead:true},{});assert.equal(h.calls.draw.length,12);
  h.context.AWPresentation.quality='low';h.world.decorateGround(room);assert.equal(h.calls.draw.length,18);
  for(const biome of ['swamp','volcanic','crypt'])assert.doesNotMatch(h.world.groundSelection({...room,biome}).frame,/stream|pool|fissure/);
});

test('gear crops use actual frame metadata and preserve the armor content rectangle',()=>{
  const h=harness();
  h.atlas('weapon-catalog',[{id:'weapon-hand-cannon',rect:{x:512,y:256,w:256,h:256}}]);
  h.atlas('armor-verdant-oath',[{id:'SE-idle-a',rect:{x:192,y:384,w:192,h:192},contentRect:{x:45,y:25,w:90,h:140}}]);
  const weapon=h.ui.crop({slot:'weapon',type:'handcannon'},44);assert.equal(weapon.frame,'weapon-hand-cannon');assert.equal(weapon.x,80);assert.equal(weapon.y,40);assert.equal(weapon.width,40);
  const armor=h.ui.crop({slot:'armor',name:'Verdant Oath'},144);assert.equal(armor.asset,'armor-verdant-oath');assert.equal(armor.x,237);assert.equal(armor.y,409);assert.equal(armor.width,90);assert.equal(armor.height,140);
  assert.equal(h.ui.crop({slot:'trinket'}),null);assert.equal(h.ui.crop({slot:'weapon',type:'future'}),null);
});

test('all scenery, object and floor selections reference valid real atlas rectangles and ground pivots',()=>{
  const h=harness();
  function validate(selected,ground=false){
    const entry=catalog.entries[selected.asset];assert.ok(entry,selected.asset);
    const frame=entry.meta.frames.find(f=>f.id===selected.frame);assert.ok(frame,selected.frame);
    const r=frame.rect,p=frame.pivot;
    assert.ok(r.x>=0&&r.y>=0&&r.w>0&&r.h>0&&r.x+r.w<=entry.meta.width&&r.y+r.h<=entry.meta.height,selected.frame+' rectangle');
    assert.ok(p.x>=0&&p.y>=0&&p.x<=r.w&&p.y<=r.h,selected.frame+' pivot');
    assert.ok(frame.contentBounds.w>0&&frame.contentBounds.h>0);
    if(ground)assert.equal(p.y,r.h*.5,selected.frame+' ground center');
    else assert.ok(p.y>=r.h*.5,selected.frame+' ground contact');
  }
  for(const region of Object.keys(h.world.regions)){
    const room={continent:region};
    for(const type of Object.keys(h.world.props[region]))validate(h.world.selection(type,room));
    for(const type of Object.keys(h.world.objects[region]))validate(h.world.selection(type,room,true));
    for(const biome of ['meadow','forest','ruins','swamp','frost','desert','volcanic','crypt','crystal','celestial'])validate(h.world.groundSelection({...room,biome}),true);
  }
});

test('real normalized armor metadata is cropped to its own character instead of a hardcoded grid',()=>{
  const h=harness();for(const [id,entry] of Object.entries(catalog.entries))h.entries.set(id,entry);
  const item={slot:'armor',name:'Verdant Oath'},entry=catalog.entries['armor-verdant-oath'],frame=entry.meta.frames.find(f=>f.id==='SE-idle-a'),c=h.ui.crop(item,100);
  assert.ok(c.width>0&&c.height>0&&Math.max(c.width,c.height)===96);
  const scale=96/Math.max(frame.contentBounds.w,frame.contentBounds.h);
  assert.equal(c.x,(frame.rect.x+frame.contentBounds.x)*scale);assert.equal(c.y,(frame.rect.y+frame.contentBounds.y)*scale);
  for(const [type,expected] of [['blade','sword'],['greatblade','sword'],['glaive','spear'],['voidpike','spear'],['handcannon','hand-cannon'],['arcaneorb','arcane-orb'],['sundisc','sun-disc'],['unarmed','unarmed-magic-gauntlet']])assert.equal(h.ui.crop({slot:'weapon',type},44)?.frame,'weapon-'+expected);
});
