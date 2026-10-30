import test from 'node:test';
import assert from 'node:assert/strict';
import {opportunityArt} from './opportunity-art.mjs';
import {locationArt,locationSymbol} from './living-world-art.mjs';
import {renderFruitBasket} from './fruit-baskets-ui.mjs';
import {renderMerchant,merchantResourceArt} from './merchant-ui.mjs';
import * as C from './core.mjs';
import * as W from './living-world.mjs';
import {specialLocationUI,specialMapMarkersUI,worldLocationsUI} from './living-world-ui.mjs';
import {recordFruitBasketDiscovery,advanceFruitBaskets} from './fruit-baskets.mjs';
import {awardGearCopy} from './equipment-inventory.mjs';
import {renderForgeInventory} from './forge-ui.mjs';
const fixture=()=>{const s=C.newSave('Artwork checks','capuchin','practice',25);s.run.nextEncounter=s.run.nextCamp=1e9;C.advance(s,3500*C.FEET);return s;};
test('all opportunity illustrations are accessible inline SVG with no external dependencies or duplicate IDs',()=>{
 for(const compact of [false,true])for(const kind of ['temple','merchant','basket'])for(const direction of ['east','west','south','north']){
  const svg=opportunityArt(kind,{compact,direction,label:'A "><script>bad</script> scene'});
  assert.match(svg,/^<svg\b/);assert.match(svg,/role="img"/);assert.match(svg,/viewBox="[\d .-]+"/);assert.match(svg,new RegExp(`data-opportunity-art="${kind}"`));
  assert.doesNotMatch(svg,/<(?:script|image|foreignObject)\b|\son\w+=|\sid=|(?:href|src)=|undefined|NaN/);
  assert.match(svg,/&lt;script&gt;/);
 }
});
test('four temples use distinct geometry in scene and compact/map versions',()=>{
 for(const compact of [false,true]){
  const graphics=['east','west','south','north'].map(direction=>opportunityArt('temple',{compact,direction}));
  const shapes=graphics.map(svg=>[...svg.matchAll(/<(?:path|polygon|rect|circle|ellipse)\b[^>]*>/g)].map(m=>m[0].replace(/(?:fill|stroke|opacity)="[^"]*"/g,'')).join(''));
  assert.equal(new Set(shapes).size,4);
 }
});
test('new temple art is mounted in destination details, cards and map markers',()=>{
 const s=fixture(),row=W.specialLocations(s)[0];s.meta.livingWorld.locations[row.id].definitionId=`${s.world.activeLand}:temple-north`;
 assert.match(specialLocationUI(s,row.id),/data-opportunity-art="temple"/);assert.match(specialLocationUI(s,row.id),/data-direction="north"/);
 assert.match(worldLocationsUI(s),/data-opportunity-art="temple"/);assert.match(specialMapMarkersUI(s),/data-direction="north"/);
 assert.match(locationArt({icon:'merchant',landId:s.world.activeLand,name:'Trader'}),/data-opportunity-art="merchant"/);
 assert.match(locationSymbol('basket'),/data-opportunity-art="basket"/);
});
test('fruit basket graphics remain visible before and after the walking challenge',()=>{
 const s=fixture();for(let i=0;i<12;i++)recordFruitBasketDiscovery(s,{id:'grove',position:{east:0,north:0}});
 const pending=renderFruitBasket(s);assert.match(pending,/data-opportunity-art="basket"/);assert.doesNotMatch(pending,/🍎/);
 advanceFruitBaskets(s,500*C.FEET);assert.match(renderFruitBasket(s),/data-opportunity-art="basket"/);assert.match(renderFruitBasket(s),/Ready to collect/);
});
test('merchant scene and every stock item use matching graphics, and spare forge copies keep equipment art',()=>{
 const s=fixture(),row=W.specialLocations(s)[0],def=W.SPECIAL_LOCATIONS.find(d=>d.landId===s.world.activeLand&&d.icon==='merchant');s.meta.livingWorld.locations[row.id].definitionId=def.id;s.meta.livingWorld.locations[row.id].status='visited';
 const html=renderMerchant(s,row.id,{fruit:2});assert.match(html,/data-opportunity-art="merchant"/);assert.match(html,/merchant-payment-art/);
 const cards=[...html.matchAll(/<article class="item-card merchant-stock-item">([\s\S]*?)<\/article>/g)];assert.equal(cards.length,6);for(const card of cards)assert.match(card[1],/<svg\b/);
 assert.match(merchantResourceArt('essence:leaf'),/class="element-icon"/);assert.match(merchantResourceArt('item:starOre'),/aria-label="Star Ore"/);
 const gear=awardGearCopy(s,'fang');const forge=renderForgeInventory(s,{}, {filter:'equipment'});assert.match(forge,new RegExp(`data-forge-drag="equipment:${gear.copyId}"`));assert.match(forge,/<svg\b/);
});
