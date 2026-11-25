import test from 'node:test';
import assert from 'node:assert/strict';
import { HEROES } from '../game/moba-mania/data.mjs';
import { Game } from '../game/moba-mania/engine.mjs';
import { createEffectBudget, drawShieldShell, shieldTheme } from '../game/moba-mania/combat-visuals.mjs';
import { unitPose } from '../game/moba-mania/unit-animation.mjs';
import { drawExpansionHero, drawExpansionIcon, drawExpansionWarning, drawExpansionAim, drawExpansionProjectile, drawExpansionEffect, drawExpansionUnitAccents, drawResonanceResource, resonanceResource } from '../game/moba-mania/expansion-visuals.mjs';

function canvas(){
  const calls=[],stack=[],c={globalAlpha:1,fillStyle:'#000',strokeStyle:'#000',lineWidth:1};
  c.save=()=>stack.push({globalAlpha:c.globalAlpha,fillStyle:c.fillStyle,strokeStyle:c.strokeStyle,lineWidth:c.lineWidth});
  c.restore=()=>{assert(stack.length,'a restore needs its matching save');Object.assign(c,stack.pop());};
  for(const method of ['beginPath','closePath','moveTo','lineTo','arc','ellipse','translate','rotate','scale','setTransform','fillRect','strokeRect','fill','stroke','setLineDash'])c[method]=(...args)=>{
    for(const value of args.flat())if(typeof value==='number')assert(Number.isFinite(value),method+' needs finite coordinates');
    assert(c.globalAlpha>=0&&c.globalAlpha<=1,'opacity stays valid');calls.push({method,args,color:method==='stroke'?c.strokeStyle:c.fillStyle,alpha:c.globalAlpha});
  };
  return {c,calls,balanced:()=>assert.equal(stack.length,0)};
}
const hero=(heroId,extra={})=>({id:1,kind:'hero',heroId,x:100,y:150,hp:500,maxHp:500,r:17,team:0,alive:true,status:{},resonance:{},...extra});

test('magnetic core and moth musician have distinct silhouettes, eight authored icons, and finite directional casts',()=>{
  const bodies=[],icons=[];
  for(const id of ['ferrite','cadence']){
    const idle=canvas();assert(drawExpansionHero(idle.c,id));idle.balanced();bodies.push(JSON.stringify(idle.calls));
    for(let direction=0;direction<8;direction++)for(const stage of [0,1,2,3,4]){
      const out=canvas();assert(drawExpansionHero(out.c,id,{direction,stage,frame:3,casting:stage>0,back:direction===6,variant:id==='ferrite'?'push':'chime',tint:'#87aacc'}));out.balanced();
    }
    for(const ability of HEROES[id].abilities){const out=canvas();assert(drawExpansionIcon(out.c,ability.icon));out.balanced();icons.push(JSON.stringify(out.calls));}
  }
  assert.equal(new Set(bodies).size,2);assert.equal(new Set(icons).size,8);
});

test('resource displays preserve repeated musical notes, the real note window, charge expiry and current polarity',()=>{
  const ferrite=hero('ferrite',{resonance:{scrap:2,polarity:'push'}}),f=JSON.stringify(ferrite),cadence=hero('cadence',{resonance:{notes:[{slot:0,at:0},{slot:0,at:1},{slot:1,at:2}],chordUntil:0,finaleNote:1}});
  assert.equal(resonanceResource(ferrite,9).short,'PUSH 2/3');assert.match(resonanceResource(ferrite,9).detail,/Pulse switches to pull/);
  assert.equal(resonanceResource(cadence,9).short,'Q·Q·E','runtime owns expiry, including Long Phrase');
  assert.match(resonanceResource(cadence,9).detail,/2 of 3 different/);
  assert.equal(resonanceResource(cadence,9).last,1);
  cadence.resonance.chordUntil=10;assert.equal(resonanceResource(cadence,9).short,'CHORD');assert.equal(resonanceResource(cadence,10).short,'Q·Q·E');
  cadence.resonance.notes=[];assert.equal(resonanceResource(cadence,10).last,0,'an empty phrase previews default Shatter instead of stale finaleNote');
  for(const unit of [ferrite,cadence]){const out=canvas(),before=JSON.stringify(unit);assert(drawResonanceResource(out.c,unit,5,6,9));drawExpansionUnitAccents(out.c,unit,9,createEffectBudget(390));out.balanced();assert.equal(JSON.stringify(unit),before);}
  assert.equal(JSON.stringify(ferrite),f);
});

test('Pulse preview advertises the polarity it will switch to while Storm keeps the current polarity',()=>{
  const u=hero('ferrite',{resonance:{scrap:3,polarity:'push'}}),pulse=HEROES.ferrite.abilities[2],storm=HEROES.ferrite.abilities[3];
  const render=ability=>{const out=canvas();assert(drawExpansionAim(out.c,{time:5},u,ability,{angle:0,tx:u.x,ty:u.y,range:ability.range,color:ability.color}));out.balanced();return out.calls;};
  const x=u.x+pulse.radius*.7;
  assert(render(pulse).some(v=>v.method==='moveTo'&&v.args[0]===Math.round(x+Math.cos(.65)*11)),'PUSH state previews an inward Pulse arrow');
  assert(render(storm).some(v=>v.method==='moveTo'&&v.args[0]===Math.round(x-Math.cos(.65)*11)),'Storm previews outward displacement');
  u.resonance.polarity='pull';assert(render(pulse).some(v=>v.method==='moveTo'&&v.args[0]===Math.round(x-Math.cos(.65)*11)));
});

test('new resource and ability art never changes saved combat state or consumes simulation randomness',()=>{
  for(const id of ['ferrite','cadence']){
    const game=new Game({hero:id,seed:9862}),u=game.player,before=JSON.stringify(game.serialize()),out=canvas(),budget=createEffectBudget(390);
    drawExpansionHero(out.c,id,{frame:2,stage:2,casting:true});drawExpansionUnitAccents(out.c,u,game.time,budget);
    drawResonanceResource(out.c,u,10,10,game.time);unitPose(u,game,game.time);
    for(const ability of HEROES[id].abilities){drawExpansionIcon(out.c,ability.icon);drawExpansionAim(out.c,game,u,ability,{angle:0,tx:u.x+90,ty:u.y,range:ability.range,color:ability.color});}
    assert.equal(JSON.stringify(game.serialize()),before);out.balanced();
  }
});

test('all ultimate modes, echoes and epic flares are deterministic, budgeted and visible in reduced motion',()=>{
  const types=['rail-spike','scrap-orbit','scrap-barrier','polarity-pulse','lodestone-storm','lodestone-echo','piercing-note','sheltering-chime','skipbeat','grand-finale','charged-chord','finale-echo','note-echo','dance-stage','inferno-chain','pandemic-burst','winter-field'];
  const render=reduced=>{
    const out=canvas(),budget=createEffectBudget(390,reduced);
    for(const type of types)for(const phase of [.05,.4,.85])for(const ground of [false,true]){
      const e={type,x:100,y:150,x2:240,y2:150,r:100,createdAt:5,maxLife:1,life:1-phase,polarity:'push',scrap:3,finaleNote:2},before=JSON.stringify(e);
      assert(drawExpansionEffect(out.c,e,5+phase,ground,budget),type);assert.equal(JSON.stringify(e),before);
      assert(budget.remaining>=0&&budget.accentRemaining>=0);
    }
    out.balanced();return out.calls;
  };
  assert.deepEqual(render(false),render(false));assert(render(true).length>0);assert(render(true).length<render(false).length);
  for(const kind of ['polarity-pulse','lodestone-storm','lodestone-echo','grand-finale','finale-echo','note-echo'])for(const finaleNote of [0,1,2]){
    const out=canvas();assert(drawExpansionWarning(out.c,{kind,x:100,y:150,r:100,team:1,finaleNote,polarity:'pull'},.5,5,createEffectBudget()));out.balanced();
  }
});

test('magnet and resonance shields and projectiles communicate different mechanics',()=>{
  const styles=[];
  for(const [id,theme] of [['ferrite','magnet'],['cadence','resonance']]){
    const u=hero(id,{shield:100,shieldTime:3});assert.equal(shieldTheme(u),theme);
    const out=canvas(),budget=createEffectBudget();assert(drawShieldShell(out.c,u,1,90,66,true,budget));
    assert(drawExpansionProjectile(out.c,{abilityId:HEROES[id].abilities[0].id,theme,x:100,y:150,angle:.4},1,budget));
    assert(out.calls.some(v=>v.method==='fill'&&v.alpha<.1),'shield stays translucent');styles.push(JSON.stringify(out.calls));out.balanced();
  }
  assert.notEqual(...styles);
});
