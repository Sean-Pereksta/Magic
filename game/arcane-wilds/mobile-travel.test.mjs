import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const read=name=>fs.readFileSync(new URL(name,import.meta.url),'utf8');
const tuning=read('travel-tuning.js');
function section(source,start,end){
  const a=source.indexOf(start),b=source.indexOf(end,a+start.length);
  assert.ok(a>=0&&b>a,`Production source section not found: ${start}`);
  return source.slice(a,b);
}
// Exercise the actual existing camera/catalog and campaign speed adapter rather
// than inventing a second implementation of their behavior for these tests.
const cameraSource=section(read('presentation-core.js'),'  const camera=','  class Pool');
const mountsSource=section(read('campaign-data.js'),'  const mounts =','  const items =');
const regional=read('regional-content.js');
const regionalMounts=section(regional,'  Object.assign(D.mounts,{','  const mountNodes=');
const movementSource=section(read('campaign.js'),'  playerMovement=function(dt){','  autoAttack=function(dt)');
function fixture(touch=false){
  const s={isTouch:touch,W:390,H:844,TILE_W:84,TILE_H:42,frameAt:0,shake:0,
    intro:null,settings:{shake:.5},lerp:(a,b,t)=>a+(b-a)*t,
    game:{player:{x:9,y:7,speed:3},enemies:[],campaign:{riding:false,activeMount:'horse'}}};
  s.window=s;vm.createContext(s);
  vm.runInContext(mountsSource+'\nglobalThis.AWCampaignData={mounts};',s);
  vm.runInContext('(function(){const D=AWCampaignData;'+regionalMounts+'})();',s);
  vm.runInContext(cameraSource+'\nglobalThis.AWPresentation={camera};',s);
  return s;
}
const apply=s=>vm.runInContext(tuning,s);
const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-10,`${a} != ${b}`);

test('all eight mounts gain roughly 80% speed without changing their identities or abilities',()=>{
  const s=fixture(),before=JSON.parse(JSON.stringify(s.AWCampaignData.mounts));apply(s);
  assert.ok(Object.keys(before).length>=8);
  for(const [id,m] of Object.entries(s.AWCampaignData.mounts)){
    assert.equal(m.speed,Math.round(before[id].speed*1.8*100)/100);
    assert.ok(m.speed>=2.5);
    const {speed,...metadata}=JSON.parse(JSON.stringify(m));
    const {speed:oldSpeed,...oldMetadata}=before[id];
    assert.deepEqual(metadata,oldMetadata);
  }
  assert.equal(s.game.player.speed,3);
});
test('desktop camera is completely unchanged while desktop mounts also benefit',()=>{
  const s=fixture(),tick=s.AWPresentation.camera.tick;apply(s);
  assert.equal(s.AWPresentation.camera.tick,tick);assert.equal(s.AWPresentation.camera.zoom,1);
  assert.equal(s.AWCampaignData.mounts.horse.speed,2.56);
});
test('touch camera starts at 88% scale, with no first-frame full-zoom flash',()=>{
  const s=fixture(true);apply(s);near(s.AWPresentation.camera.zoom,.88);
});
for(const [name,riding,enemies] of [
  ['normal travel',false,[]],['mounted travel',true,[]],
  ['busy combat',false,Array.from({length:15},()=>({}))],['boss fight',false,[{boss:true}]]
])test(`mobile zoom follows existing ${name} behavior without accumulating scale`,()=>{
  const base=fixture(true),mobile=fixture(true);apply(mobile);
  for(const s of [base,mobile]){s.game.campaign.riding=riding;s.game.enemies=enemies;}
  for(let i=0;i<900;i++){
    if(i===120)for(const s of [base,mobile])s.AWPresentation.camera.kick=.04;
    if(i===400)for(const s of [base,mobile]){s.AWPresentation.camera.ready=false;s.game.player.x=11;}
    base.AWPresentation.camera.tick(1/60);mobile.AWPresentation.camera.tick(1/60);
    near(mobile.AWPresentation.camera.zoom,base.AWPresentation.camera.zoom*.88);
    near(mobile.AWPresentation.camera.x,base.AWPresentation.camera.x);
  }
});
test('world projection uses the same scaled camera for both axes and height',()=>{
  const base=fixture(true),s=fixture(true);apply(s);
  base.AWPresentation.camera.tick(.1);s.AWPresentation.camera.tick(.1);
  const a=base.AWPresentation.camera.project(13,3,20),b=s.AWPresentation.camera.project(13,3,20);
  near(b.x-s.W*.5,(a.x-base.W*.5)*.88);near(b.y-s.H*.53,(a.y-base.H*.53)*.88);
});
test('camera early return and an exception never leave an unscaled mobile zoom',()=>{
  const s=fixture(true);s.game.player=null;apply(s);
  for(let i=0;i<20;i++)s.AWPresentation.camera.tick(.016);
  near(s.AWPresentation.camera.zoom,.88);
  const failing=fixture(true);failing.AWPresentation.camera.tick=()=>{throw Error('test');};apply(failing);
  assert.throws(()=>failing.AWPresentation.camera.tick(.016),/test/);near(failing.AWPresentation.camera.zoom,.88);
});
test('duplicate script evaluation never doubles mount speeds or zoom wrappers',()=>{
  const s=fixture(true);apply(s);const data=JSON.stringify(s.AWCampaignData.mounts),tick=s.AWPresentation.camera.tick;
  apply(s);assert.equal(JSON.stringify(s.AWCampaignData.mounts),data);assert.equal(s.AWPresentation.camera.tick,tick);
  near(s.AWPresentation.camera.zoom,.88);
});
function withMovement(touch=false){
  const s=fixture(touch);apply(s);s.inCampaign=()=>true;s.state=()=>s.game.campaign;
  s.current=()=>({biome:'forest'});s.D=s.AWCampaignData;s.samples=[];
  s.originals={playerMovement:dt=>{s.samples.push(s.game.player.speed);s.game.player.x+=s.game.player.speed*dt;}};
  vm.runInContext(movementSource,s);return s;
}
test('real campaign adapter uses faster mounts and restores the on-foot speed',()=>{
  const s=withMovement();s.game.campaign.riding=true;s.playerMovement(.1);
  near(s.samples[0],3*2.56);assert.equal(s.game.player.speed,3);
  s.game.campaign.riding=false;s.playerMovement(.1);assert.equal(s.samples[1],3);
});
test('terrain bonuses still compose with new speeds and cannot leak into player stats',()=>{
  const s=withMovement(true);s.game.campaign.riding=true;s.game.campaign.activeMount='verdantElk';
  s.playerMovement(.1);near(s.samples[0],3*2.79*1.08);assert.equal(s.game.player.speed,3);
  s.originals.playerMovement=()=>{throw Error('movement');};
  assert.throws(()=>s.playerMovement(.1),/movement/);assert.equal(s.game.player.speed,3);
});
test('legacy noncampaign play is not given the campaign mount bonus',()=>{
  const s=withMovement();s.inCampaign=()=>false;s.game.campaign.riding=true;s.playerMovement(.1);
  assert.equal(s.samples[0],3);assert.equal(s.game.player.speed,3);
});
test('entrypoint loads the final mobile CSS and tuning after all mount catalogs',()=>{
  const html=read('../arcane-wilds.html');
  for(const file of ['mobile-compact.css','travel-tuning.js'])assert.equal(html.split(`arcane-wilds/${file}`).length-1,1);
  assert.ok(html.indexOf('mobile-compact.css')>html.indexOf('campaign.css'));
  for(const file of ['presentation-core.js','campaign-data.js','regional-content.js','shadow-ui.js'])assert.ok(html.indexOf('travel-tuning.js')>html.indexOf(file));
  assert.ok(html.indexOf('travel-tuning.js')<html.indexOf('runtime-stability.js'));
});
test('mobile layout preserves spell sizes, safe edges, expanded rows and desktop scope',()=>{
  const css=read('mobile-compact.css');
  assert.match(css,/@media \(pointer:coarse\),\(max-width:780px\)/);
  assert.match(css,/width:54px!important;min-width:54px!important;height:60px!important;min-height:60px!important/);
  assert.match(css,/bottom:var\(--aw-edge-bottom\)!important;top:auto!important;transform:none!important/);
  assert.match(css,/body:has\(#spells\[data-spell-slots="3"\]\)/);
  for(const edge of ['top','left','right','bottom'])assert.ok(css.includes(`env(safe-area-inset-${edge})`));
  assert.match(css,/grid-auto-flow:column!important/);
  assert.match(css,/#mobileControls #aimZone[\s\S]*var\(--aw-dock-height\) \+ 8px/);
  assert.doesNotMatch(css,/transform:\s*scale\(/);
});
