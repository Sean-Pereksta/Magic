import test from 'node:test';
import assert from 'node:assert/strict';
import { HEROES } from '../game/moba-mania/data.mjs';
import { createEffectBudget, shieldTheme } from '../game/moba-mania/combat-visuals.mjs';
import { unitPose } from '../game/moba-mania/unit-animation.mjs';
import { drawExpansionHero, drawDevice, drawMagazine, drawExpansionIcon, drawExpansionWarning, drawExpansionAim, drawExpansionProjectile, drawExpansionEffect, drawExpansionZones } from '../game/moba-mania/expansion-visuals.mjs';

const ids=['rivet','brinehook','vela','solstice'];
function context(){
  const calls=[],stack=[],c={globalAlpha:1,fillStyle:'#000',strokeStyle:'#000',lineWidth:1};
  c.save=()=>stack.push({globalAlpha:c.globalAlpha,fillStyle:c.fillStyle,strokeStyle:c.strokeStyle,lineWidth:c.lineWidth});
  c.restore=()=>{assert(stack.length,'matched canvas save/restore');Object.assign(c,stack.pop());};
  for(const method of ['beginPath','closePath','moveTo','lineTo','arc','ellipse','translate','rotate','scale','setTransform','fillRect','strokeRect','fill','stroke','setLineDash'])c[method]=(...args)=>{
    for(const value of args.flat())if(typeof value==='number')assert(Number.isFinite(value),method+' has finite coordinates');
    assert(Number.isFinite(c.globalAlpha)&&c.globalAlpha>=0&&c.globalAlpha<=1,'valid opacity');
    calls.push({method,args,alpha:c.globalAlpha,color:method==='stroke'?c.strokeStyle:c.fillStyle});
  };
  return {c,calls,balanced:()=>stack.length===0&&c.globalAlpha===1};
}
const unit=extra=>({id:1,kind:'hero',heroId:'vela',alive:true,hp:100,maxHp:100,team:0,x:100,y:180,angle:0,r:17,status:{},attackTimer:1,attackAnim:0,...extra});

test('the four human silhouettes and all sixteen skill icons use distinct authored pixel geometry',()=>{
  const bodies=[],icons=[];
  for(const id of ids){
    const body=context();assert(drawExpansionHero(body.c,id));assert(body.balanced());bodies.push(JSON.stringify(body.calls));
    for(const a of HEROES[id].abilities){
      const icon=context();assert(drawExpansionIcon(icon.c,a.id));assert(icon.balanced());
      assert(icon.calls.some(v=>v.method==='fillRect'||v.method==='stroke'));icons.push(JSON.stringify(icon.calls));
    }
  }
  assert.equal(new Set(bodies).size,4);assert.equal(new Set(icons).size,16);
  assert.equal(drawExpansionIcon(context().c,'not-a-skill'),false);
  assert.equal(drawExpansionHero(context().c,'dragon'),false,'existing hero artwork keeps its own renderer');
});

test('Vela reload and fixed-angle channel poses derive from actual magazine and channel state',()=>{
  const u=unit({magazine:{rounds:0,reloadRemaining:1.4,reloadDuration:1.4}}),game={time:10,units:[u]},before=JSON.stringify(u);
  const first=unitPose(u,game);assert.equal(first.stage,5);assert.equal(first.frame,0);
  assert.equal(JSON.stringify(u),before,'pose selection cannot refill ammunition');
  u.magazine.reloadRemaining=.3;const last=unitPose(u,game);assert.equal(last.stage,5);assert.equal(last.frame,3);
  u.channel={kind:'silver-volley',fireAt:10.4,endsAt:11.6,angle:Math.PI/2};
  assert.equal(unitPose(u,game,10.2).stage,1);
  assert.equal(unitPose(u,game,10.6).stage,6);assert.equal(unitPose(u,game,10.6).direction,2);
  assert.equal(unitPose(u,game,10.6,true).frame,0);
  delete u.channel;u.magazine.reloadRemaining=0;u.magazine.rounds=6;
  assert.equal(unitPose(u,game,11.6).stage,0,'an interrupted channel does not keep a false firing pose');
});

test('magazine pips show six real chambers, sequential reload progress and Steady Aim brackets',()=>{
  const count=u=>{
    const result=context();drawMagazine(result.c,u,20,30,10);assert(result.balanced());
    return {full:result.calls.filter(v=>v.method==='fillRect'&&v.color==='#f4efdc').length,calls:result.calls};
  };
  assert.equal(count(unit({magazine:{rounds:6}})).full,6);
  assert.equal(count(unit({magazine:{rounds:2}})).full,2);
  const reload=unit({magazine:{rounds:0,reloadRemaining:.7,reloadDuration:1.4}}),before=JSON.stringify(reload);
  assert.equal(count(reload).full,3);assert.equal(JSON.stringify(reload),before);
  const plain=count(unit({magazine:{rounds:2}})),aim=count(unit({magazine:{rounds:2},status:{steadyAim:{remaining:2}}}));
  assert(aim.calls.length>plain.calls.length,'the sight brackets reflect an active buff');
});

test('Split Shot and Captain’s Wake previews match the exact projectile spread and full warning width',()=>{
  const u=unit(),game={units:[u]},split=HEROES.vela.abilities[0],wake=HEROES.brinehook.abilities[3];
  const shots=context();drawExpansionAim(shots.c,game,u,split,{angle:0,tx:300,ty:180,range:360,color:split.color});
  assert.deepEqual(shots.calls.filter(v=>v.method==='rotate').map(v=>v.args[0]),[-.055,.055]);
  assert(shots.calls.filter(v=>v.method==='strokeRect').every(v=>v.args[1]===-split.radius&&v.args[3]===split.radius*2));
  const wave=context();drawExpansionAim(wave.c,game,u,wake,{angle:0,tx:300,ty:180,range:360,color:wake.color});
  assert(wave.calls.some(v=>v.method==='strokeRect'&&v.args[1]===-50&&v.args[3]===100),'100 means full width, never a doubled 200-unit warning');
  assert(shots.balanced());assert(wave.balanced());
});

test('Time Ward preview selects the same eligible allied hero as the real cast, including self and tie breaking',()=>{
  const caster=unit({heroId:'solstice'}),ally=unit({id:3,x:210,y:180}),other=unit({id:2,x:210,y:180});
  const a=HEROES.solstice.abilities[1],game={units:[caster,ally,other,unit({id:4,team:1,x:205,y:180}),unit({id:5,kind:'summon',x:205,y:180})],getAbility:()=>a};
  const render=(x,y)=>{const out=context();drawExpansionAim(out.c,game,caster,a,{angle:0,tx:x,ty:y,range:a.range,color:a.color});return out;};
  let out=render(205,180);assert(out.calls.some(v=>v.method==='translate'&&v.args[0]===other.x&&v.args[1]===other.y-52));assert(out.balanced());
  ally.alive=other.alive=false;out=render(caster.x,caster.y);
  assert(out.calls.some(v=>v.method==='translate'&&v.args[0]===caster.x&&v.args[1]===caster.y-52));
  const far=unit({id:6,x:450,y:180});game.units=[caster,far];out=render(430,180);
  assert(!out.calls.some(v=>v.method==='translate'&&v.args[0]===far.x&&v.args[1]===far.y-52),'a target outside caster range is not promised by the preview');
});

test('device assembly, arming, firing and overclock remain deterministic, bounded and presentation-only',()=>{
  const signatures=[];
  for(const props of [{subtype:'sentry',assembleUntil:10.5},{subtype:'sentry'},{subtype:'sentry',attackAnim:.2,angle:1,overclockUntil:16},{subtype:'mine',armAt:10.5},{subtype:'mine',armAt:9}]){
    const u=unit({kind:'summon',...props}),before=JSON.stringify(u),render=()=>{
      const out=context(),budget=createEffectBudget(390);drawDevice(out.c,u,10,budget);
      assert(budget.remaining>=0&&budget.accentRemaining>=0);assert(out.balanced());return out.calls;
    };
    const calls=render();assert.deepEqual(calls,render());assert.equal(JSON.stringify(u),before);signatures.push(JSON.stringify(calls));
  }
  assert.equal(new Set(signatures).size,5);
});

test('new impact effects and warning symbols are finite, deterministic and obey cosmetic budgets',()=>{
  const variants=[
    ['shock-mine','copper'],['overclock','copper'],['repair','copper'],['anchor-cable','tide'],['undertow','tide'],['captains-wake','tide'],['sea-legs','tide'],['rough-current','tide'],
    ['reload','ivory'],['sidestep','ivory'],['steady-aim','ivory'],['silver-volley','ivory'],['time-ward','time'],['unwind','time'],['borrowed-seconds','time'],['glass-echo','time'],['perfect-timing','time'],['time-return','time'],['time-sand','time'],
  ];
  const run=reduced=>{
    const out=context(),budget=createEffectBudget(390,reduced);
    for(const [type,theme]of variants)for(const phase of [.1,.5,.9])for(const ground of [true,false]){
      const e={type,theme,x:100,y:180,x2:250,y2:190,r:85,range:360,width:50,angle:.2,createdAt:10,maxLife:1,life:1-phase},before=JSON.stringify(e);
      assert(drawExpansionEffect(out.c,e,10+phase,ground,budget));assert.equal(JSON.stringify(e),before);
      assert(budget.remaining>=0&&budget.accentRemaining>=0);
    }
    assert(out.balanced());return out.calls;
  };
  assert.deepEqual(run(false),run(false));assert(run(true).length<run(false).length);
  for(const kind of ['anchorline','undertow','captains-wake','silver-volley','borrowed-seconds','glass-echo']){
    const out=context();assert(drawExpansionWarning(out.c,{kind,team:1,x:100,y:180,r:110,range:360,width:50,angle:.3},.5,10,createEffectBudget()));assert(out.balanced());
  }
  const ended=context();assert(drawExpansionEffect(ended.c,{type:'undertow',x:100,y:180,createdAt:10,maxLife:1,life:0},12,false,createEffectBudget()));assert.equal(ended.calls.length,0);
});

test('hook ropes and channel lines cannot reveal an unseen owner or hidden field position',()=>{
  const owner=unit({heroId:'brinehook'}),game={player:{team:0},units:[owner],canSee:()=>false,pointVisible:()=>false,zones:[]};
  const p={type:'anchorline',abilityId:'anchorline',theme:'tide',sourceId:owner.id,ownerId:owner.id,x:300,y:180,fromX:100,fromY:180,angle:0};
  const hidden=context();assert(drawExpansionProjectile(hidden.c,p,10,createEffectBudget(),game));
  assert(!hidden.calls.some(v=>v.method==='moveTo'&&v.args[0]===100),'visible anchor head does not expose an unseen launch position');
  game.canSee=()=>true;const seen=context();drawExpansionProjectile(seen.c,p,10,createEffectBudget(),game);
  assert(seen.calls.some(v=>v.method==='moveTo'&&v.args[0]===100));assert(seen.balanced());assert(hidden.balanced());
  owner.channel={kind:'silver-volley',fireAt:9,endsAt:11,angle:0,x:100,y:180,range:480};game.canSee=()=>false;
  game.zones=[{type:'time-sand',x:100,y:180,r:85,createdAt:9,life:1,maxLife:2}];const concealed=context();
  drawExpansionZones(concealed.c,game,10,()=>true,createEffectBudget());assert.equal(concealed.calls.length,0);
});

test('new protection themes remain distinct even for old saves without shield metadata',()=>{
  assert.deepEqual(ids.map(heroId=>shieldTheme(unit({heroId,shield:100}))),['copper','tide','ivory','time']);
});
