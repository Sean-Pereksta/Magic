import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as B from './fruit-baskets.mjs';
import {renderFruitBasket,refreshFruitBasket} from './fruit-baskets-ui.mjs';
const FOOT=.3048,save=()=>C.newSave('Fruit baskets','capuchin','practice',14);
const habitat=(id='grove')=>({id,position:{east:123,north:-456}});
function find(s,h=habitat()){for(let i=0;i<12;i++){const spawned=B.recordFruitBasketDiscovery(s,h);if(spawned)return spawned;}throw Error('Basket did not spawn.');}
test('occasional grove discoveries spawn a stored fruit reward at the actual fantasy discovery position without RNG',()=>{
 const s=save(),rng=s.rng;let found=null,discoveries=0;while(!found&&discoveries<12){found=B.recordFruitBasketDiscovery(s,habitat());discoveries++;}
 assert.ok(discoveries>=6&&discoveries<=12);assert.ok(found.fruit>=3&&found.fruit<=5);assert.equal(found.landId,s.world.activeLand);assert.deepEqual(found.position,{east:123,north:-456});assert.equal(found.meters,0);assert.equal(s.rng,rng);assert.equal(B.fruitBasketView(s).remainingFeet,500);
});
test('baskets use grove habitats in any region, and forest trails, excluding cave and rocky or wet habitats',()=>{
 for(const region of ['emerald-jungle','moonlit-forest','crystal-caverns'])for(const kind of ['grove','trail','cave','riverbank','ruins','landmark']){
  const s=save();s.world.activeLand=region;let found=null;for(let i=0;i<12;i++)found=B.recordFruitBasketDiscovery(s,habitat(kind))||found;
  assert.equal(!!found,kind==='grove'||kind==='trail'&&region!=='crystal-caverns',`${region} ${kind}`);
 }
});
test('only subsequent accepted walking in the same region counts, and progress caps at 500 feet',()=>{
 const s=save();B.advanceFruitBaskets(s,500*FOOT);find(s);assert.equal(B.fruitBasketView(s).remainingFeet,500);
 B.advanceFruitBaskets(s,125*FOOT);assert.equal(B.fruitBasketView(s).remainingFeet,375);
 for(const phase of ['camp','battle','replacement','dead']){s.run.phase=phase;assert.equal(B.advanceFruitBaskets(s,500*FOOT),0);}s.run.phase='trail';
 s.world.activeLand='sunstone-savannah';B.advanceFruitBaskets(s,500*FOOT);assert.equal(B.fruitBasketView(s,'emerald-jungle').remainingFeet,375);s.world.activeLand='emerald-jungle';
 for(const meters of [NaN,Infinity,0,-1])assert.equal(B.advanceFruitBaskets(s,meters),0);
 B.advanceFruitBaskets(s,1000*FOOT);const view=B.fruitBasketView(s);assert.equal(view.remainingFeet,0);assert.equal(view.meters,500*FOOT);assert.equal(view.ready,true);
});
test('an active basket blocks further discovery accumulation and resumes its reward after reload and restart',()=>{
 let s=save();const basket=find(s);B.advanceFruitBaskets(s,200*FOOT);for(let i=0;i<100;i++)assert.equal(B.recordFruitBasketDiscovery(s,habitat()),null);assert.equal(s.meta.fruitBaskets.regions[s.world.activeLand].discoveries,0);
 s=C.validate(JSON.parse(JSON.stringify(s)));assert.equal(B.fruitBasketView(s).remainingFeet,300);s.run.phase='dead';C.restart(s);assert.equal(B.fruitBasketView(s).remainingFeet,300);assert.equal(B.fruitBasketView(s).fruit,basket.fruit);
});
test('claiming is explicit, atomic and idempotent, with no second payout after reload',()=>{
 let s=save();const basket=find(s),before=s.run.fruit;assert.throws(()=>B.claimFruitBasket(s,basket.id),/Walk 500/);assert.equal(s.run.fruit,before);
 B.advanceFruitBaskets(s,500*FOOT);const receipt=B.claimFruitBasket(s,basket.id);assert.equal(s.run.fruit,before+basket.fruit);assert.equal(B.fruitBasketView(s),null);
 s=C.validate(JSON.parse(JSON.stringify(s)));const snapshot=JSON.stringify(s);assert.deepEqual(B.claimFruitBasket(s,basket.id),receipt);assert.equal(JSON.stringify(s),snapshot);
 assert.throws(()=>B.claimFruitBasket(s,'fruit-basket:emerald-jungle:99999'),/no longer/);
 for(let i=0;i<5;i++)assert.equal(B.recordFruitBasketDiscovery(s,habitat()),null);
});
test('full fruit balances and saves do not consume a ready basket',()=>{
 const s=save(),basket=find(s);B.advanceFruitBaskets(s,500*FOOT);s.run.fruit=1e9;let before=JSON.stringify(s);assert.throws(()=>B.claimFruitBasket(s,basket.id),/cannot hold/);assert.equal(JSON.stringify(s),before);
 s.run.fruit=10;s.meta.fruitBaskets.regions[s.world.activeLand].active.position={east:0,north:0};C.validate(s);s.meta.basketTestPadding='';s.meta.basketTestPadding='x'.repeat(650000-JSON.stringify(s).length);assert.doesNotThrow(()=>C.validate(JSON.parse(JSON.stringify(s))));before=JSON.stringify(s);assert.throws(()=>B.claimFruitBasket(s,basket.id),/too large/);assert.equal(JSON.stringify(s),before);
});
test('tracker shows feet left, can add a newly found basket to an empty host and updates existing controls',()=>{
 const s=save();assert.equal(renderFruitBasket(s),'');const basket=find(s);assert.match(renderFruitBasket(s),/500 ft left/);const host={innerHTML:'',querySelector:()=>null};refreshFruitBasket(s,{querySelectorAll:selector=>selector==='[data-fruit-basket-host]'?[host]:[]});assert.match(host.innerHTML,/fruit-basket-claim/);
 B.advanceFruitBaskets(s,500*FOOT);assert.match(renderFruitBasket(s),/Ready to collect/);assert.match(renderFruitBasket(s),new RegExp(`Collect ${basket.fruit} fruit`));
});

test('accepted core discovery chunks give the same basket progress and reward for long or small GPS samples',()=>{
 const a=save(),b=save();for(const s of[a,b])s.run.nextCamp=s.run.nextEncounter=1e9;
 C.advance(a,3200*FOOT,0,3200*FOOT);for(let i=0;i<32;i++)C.advance(b,100*FOOT,0,100*FOOT);
 const one=B.fruitBasketView(a),many=B.fruitBasketView(b);assert.ok(one&&many);assert.equal(one.id,many.id);assert.equal(one.fruit,many.fruit);assert.ok(Math.abs(one.meters-many.meters)<1e-7);assert.ok(one.meters<500*FOOT);assert.ok(Math.abs(one.position.north-many.position.north)<1e-7);assert.equal(one.remainingFeet,many.remainingFeet);assert.equal(a.rng,b.rng);
});
