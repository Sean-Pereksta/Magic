import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {fruitBasketView,claimFruitBasket} from './fruit-baskets.mjs';
import {renderFruitBasket} from './fruit-baskets-ui.mjs';
import {specialMapMarkersUI} from './living-world-ui.mjs';
const fixture=()=>{const s=C.newSave('Basket core','capuchin','practice',123);s.run.nextEncounter=s.run.nextCamp=1e9;return s;};
const walk=(s,feet)=>C.advance(s,feet*C.FEET);
function find(s){for(let i=0;i<12&&!fruitBasketView(s);i++)walk(s,250);return fruitBasketView(s);}
test('accepted discovery spawns a basket whose 500-foot requirement starts after spawning',()=>{
 const s=fixture(),basket=find(s);assert.ok(basket);assert.equal(basket.remainingFeet,500);assert.equal(basket.progressFeet,0);assert.equal(basket.ready,false);
 assert.match(specialMapMarkersUI(s),/data-action="fruit-basket-open"/);assert.match(renderFruitBasket(s),/500 ft left/);
 walk(s,499);assert.equal(fruitBasketView(s).ready,false);const loaded=C.validate(JSON.parse(JSON.stringify(s)));walk(loaded,1);assert.equal(fruitBasketView(loaded).ready,true);
 const before=loaded.run.fruit,receipt=claimFruitBasket(loaded,basket.id);assert.equal(loaded.run.fruit-before,receipt.fruit);assert.ok(receipt.fruit>=3&&receipt.fruit<=5);
 assert.deepEqual(claimFruitBasket(loaded,basket.id),receipt);assert.equal(loaded.run.fruit-before,receipt.fruit);
});
test('large accepted samples and small updates produce identical basket state',()=>{
 const a=fixture(),b=fixture();walk(a,3500);for(let i=0;i<35;i++)walk(b,100);assert.deepEqual(a.meta.fruitBaskets,b.meta.fruitBaskets);
});
test('distance cut off by a camp only advances the active basket by its accepted portion',()=>{
 const s=fixture();find(s);s.run.nextCamp=s.run.distance+100*C.FEET;const accepted=walk(s,1000);assert.ok(Math.abs(accepted-100*C.FEET)<1e-7);assert.equal(s.run.phase,'camp');assert.equal(fruitBasketView(s).remainingFeet,400);assert.equal(walk(s,400),0);assert.equal(fruitBasketView(s).remainingFeet,400);
});
