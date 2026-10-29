import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {renderPackNav,renderSupplies,renderRecipe,supplyAvailability} from './pack-ui.mjs';

function stocked(){
  const s=C.newSave('Pack UI','capuchin','practice',13);
  Object.assign(s.run.items,{berry:3,revive:3,bondCharm:3,tonic:3,salve:3,trailRibbon:3,treasureScent:3,scoutFeather:3,campfireSeed:3,charmMaterial:8});
  s.run.fruit=50;s.run.essence.water=8;s.run.essence.leaf=0;
  return s;
}

test('the Pack exposes four labeled sections with one selected keyboard stop',()=>{
  const html=renderPackNav('supplies');
  assert.equal((html.match(/role="tab"/g)||[]).length,4);
  assert.equal((html.match(/aria-selected="true"/g)||[]).length,1);
  assert.equal((html.match(/tabindex="0"/g)||[]).length,1);
  assert.ok(html.indexOf('>Supplies<')<html.indexOf('>Equipment<'));
  assert.ok(html.indexOf('>Equipment<')<html.indexOf('>Forge<'));
  assert.ok(html.indexOf('>Forge<')<html.indexOf('>Materials<'));
});

test('zero-owned consumables retain direct recipes and the charm explains its recruitment use',()=>{
  const s=stocked();for(const id of ['berry','revive','bondCharm'])s.run.items[id]=0;
  const before=structuredClone(s),html=renderSupplies(s,s.run.monkey,{recipeId:'berry',essence:'water'});
  for(const id of ['berry','revive','bondCharm']){
    assert.ok(html.includes(`data-supply-card="${id}"`));
    assert.ok(html.includes(`data-action="supply-recipe" data-id="${id}"`));
    assert.ok(html.includes(`data-supply-count="${id}" aria-label="0 owned"`));
  }
  assert.equal((html.match(/data-supply-recipe=/g)||[]).length,1);
  assert.match(html,/post-battle befriending/);
  assert.doesNotMatch(html,/data-action="item" data-id="bondCharm"/);
  assert.deepEqual(s,before,'rendering does not change the saved game');
});

test('recipes expose authoritative costs, owned amounts and missing ingredient reasons',()=>{
  const s=stocked(),leaf=renderRecipe(s,'berry','leaf'),water=renderRecipe(s,'berry','water');
  assert.match(leaf,/data-recipe-ingredient="fruit" data-owned="50" data-required="4"/);
  assert.match(leaf,/data-recipe-ingredient="essence" data-owned="0" data-required="2"/);
  assert.match(leaf,/Not enough Leaf essence/);
  assert.match(leaf,/data-action="craft-consumable"[^>]*disabled/);
  assert.doesNotMatch(water,/data-action="craft-consumable"[^>]*disabled/);
  assert.match(water,/value="water" selected/);
  assert.match(renderRecipe(s,'revive','water'),/data-recipe-ingredient="material" data-owned="8" data-required="2"/);
  assert.doesNotMatch(renderRecipe(s,'bondCharm','leaf'),/id="consumable-essence"/);
  s.run.phase='battle';assert.match(renderRecipe(s,'berry','water'),/Craft supplies outside active battles/);
});

test('displayed use restrictions agree with authoritative recovery rules',()=>{
  for(const id of ['berry','revive','tonic','salve'])for(const phase of ['trail','camp','battle','replacement','befriend','dead'])for(const condition of ['healthy','injured','knocked-out','status','no-items']){
    const s=stocked(),p=s.run.monkey,max=C.stats(p,C.equipped(s,p));
    p.hp=condition==='knocked-out'?0:condition==='injured'?1:max.hp;
    p.energy=condition==='injured'?0:max.maxEnergy;p.status=condition==='status'?{burn:1}:{};
    if(condition==='no-items')s.run.items[id]=0;
    s.run.phase=phase;if(phase==='battle')s.run.battle={ready:true,round:1,itemUsedRound:0};
    const before=structuredClone(s),availability=supplyAvailability(s,id,p.id);
    assert.deepEqual(s,before,'availability never spends an item');
    let allowed=true;try{C.useItem(structuredClone(s),id,p.id);}catch{allowed=false;}
    assert.equal(availability.ok,allowed,`${id} · ${phase} · ${condition}`);
    if(!allowed)assert.ok(availability.reason);
  }
  const s=stocked();s.run.phase='battle';s.run.battle={ready:true,round:2,itemUsedRound:2};
  assert.match(supplyAvailability(s,'berry',s.run.activeId).reason,/One supply per round/);
  s.run.battle.ready=false;assert.match(supplyAvailability(s,'berry',s.run.activeId).reason,/Stop safely/);
});
