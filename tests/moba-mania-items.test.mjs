import test from 'node:test';
import assert from 'node:assert/strict';
import {ITEMS,ITEM_BY_ID,itemBonus,purchaseState,describeBonuses,drawItemIcon} from '../game/moba-mania/items.mjs';

test('thirty permanent upgrades have ten items per tier and valid recipes without cycles',()=>{
  assert.equal(ITEMS.length,30);
  assert.equal(new Set(ITEMS.map(item=>item.id)).size,30);
  const implementedBonuses=new Set(['hp','damage','spellDamage','cooldown','mana','manaRegen','speed','attackSpeed','healing','shield','summonPower','summonHaste','area','lifesteal']);
  for(const tier of [1,2,3])assert.equal(ITEMS.filter(item=>item.tier===tier).length,10);
  for(const item of ITEMS){
    assert(Number.isInteger(item.cost)&&item.cost>0,item.id+' has a usable price');
    assert(item.name&&item.description&&item.color&&item.icon);
    assert(Object.isFrozen(item)&&Object.isFrozen(item.requires)&&Object.isFrozen(item.bonuses));
    assert.equal(item.requires.length,item.tier-1);
    assert.equal(new Set(item.requires).size,item.requires.length);
    const prerequisites=item.requires.map(id=>ITEM_BY_ID[id]);
    assert(prerequisites.every(component=>component&&component.tier<item.tier));
    if(item.tier===3)assert.deepEqual(prerequisites.map(component=>component.tier).sort(),[1,2]);
    for(const [key,value] of Object.entries(item.bonuses)){
      assert(implementedBonuses.has(key),key+' is an implemented combat bonus');
      assert(Number.isFinite(value)&&value>0);
    }
    assert(describeBonuses(item).length>0);
  }
  assert(Object.isFrozen(ITEMS)&&Object.isFrozen(ITEM_BY_ID));
});

test('each recipe checks both components, additional gold, and unique ownership without mutating the hero',()=>{
  for(const item of ITEMS){
    const hero={kind:'hero',gold:item.cost,items:[...item.requires]};
    const initial=JSON.stringify(hero);
    assert.equal(purchaseState(hero,item.id).ok,true);
    assert.equal(purchaseState(hero,item.id).goldNeeded,0);
    assert.equal(JSON.stringify(hero),initial,'displaying an item cannot buy or remove anything');
    hero.gold=item.cost-1;
    const poor=purchaseState(hero,item.id);
    assert.equal(poor.ok,false);assert.equal(poor.goldNeeded,1);assert.match(poor.reason,/1 more gold/);
    hero.gold=item.cost;
    for(const component of item.requires){
      hero.items=item.requires.filter(id=>id!==component);
      const locked=purchaseState(hero,item.id);
      assert.equal(locked.ok,false);assert.deepEqual(locked.missing,[component]);
      assert(locked.reason.includes(ITEM_BY_ID[component].name));
    }
    hero.items=[...item.requires,item.id];
    assert.equal(purchaseState(hero,item.id).ok,false);
    assert.equal(purchaseState(hero,item.id).reason,'Already active');
  }
});

test('invalid items, actors, and balances cannot purchase',()=>{
  const hero={kind:'hero',gold:10000,items:[]};
  for(const id of ['missing','toString','__proto__',null,undefined])assert.equal(purchaseState(hero,id).ok,false);
  for(const actor of [null,{}, {kind:'footman',gold:10000,items:[]}])assert.equal(purchaseState(actor,'ruby-heart').ok,false);
  for(const gold of [undefined,NaN,Infinity,-1,'10000'])assert.equal(purchaseState({...hero,gold},'ruby-heart').ok,false);
});

test('active ingredients stack with upgrades and duplicate inventory entries cannot multiply bonuses',()=>{
  const hero={kind:'hero',gold:0,items:['ruby-heart','guardian-plate','sun-charm','sovereign-aegis']};
  assert(Math.abs(itemBonus(hero,'hp')-.50)<1e-9);
  assert(Math.abs(itemBonus(hero,'shield')-.36)<1e-9);
  const expected=itemBonus(hero,'hp');
  hero.items.push('ruby-heart','missing','toString');
  assert.equal(itemBonus(hero,'hp'),expected);
  assert.equal(itemBonus(hero,'unknown'),0);
  assert.equal(itemBonus(hero,'toString'),0);
  assert.equal(itemBonus(null,'hp'),0);
  assert.equal(itemBonus({items:null},'hp'),0);
  assert.match(describeBonuses('blood-fang'),/2\.5% lifesteal/);
  assert.match(describeBonuses('blue-flask'),/0\.4 mana\/sec/);
  assert.equal(describeBonuses('missing'),'');
});

test('all thirty icons rasterize as distinct nonblank pixel art inside their requested bounds',()=>{
  const fingerprints=new Set(),silhouettes=new Set();
  for(const item of ITEMS){
    const pixels=Array.from({length:40},()=>Array(40).fill(''));
    let depth=0;
    const ctx={fillStyle:'',imageSmoothingEnabled:true,save(){depth++;},restore(){depth--;},fillRect(x,y,w,h){
      assert(Number.isInteger(x)&&Number.isInteger(y)&&Number.isInteger(w)&&Number.isInteger(h));
      assert(x>=0&&y>=0&&x+w<=40&&y+h<=40,item.id+' stays inside its canvas');
      for(let yy=y;yy<y+h;yy++)for(let xx=x;xx<x+w;xx++)pixels[yy][xx]=this.fillStyle;
    }};
    drawItemIcon(ctx,item,0,0,40);
    assert.equal(depth,0,'canvas state restored');
    fingerprints.add(pixels.flat().join(','));
    const interior=pixels.slice(6,32).map(row=>row.slice(6,34));
    const art=interior.flat().filter(color=>!['#132331','#243348','#1b2a3d','#35445b'].includes(color));
    assert(art.length>60,item.id+' has substantial visible equipment artwork');
    silhouettes.add(interior.map(row=>row.map(color=>['#132331','#243348','#1b2a3d','#35445b'].includes(color)?'.':'#').join('')).join('\n'));
  }
  assert.equal(fingerprints.size,30,'all thirty icons have distinct color artwork');
  assert.equal(silhouettes.size,30,'all thirty items also differ by silhouette');
});
