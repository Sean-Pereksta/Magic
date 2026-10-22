import test from 'node:test';
import assert from 'node:assert/strict';
import {SPECIES,MOVES,ELEMENTS,TYPE_CHART,STARTERS} from './data.mjs';

test('release roster includes exactly the requested sixty identities, six starters and eight Ultras',()=>{
  assert.equal(Object.keys(SPECIES).length,60);assert.equal(new Set(Object.values(SPECIES).map(s=>s.name)).size,60);
  assert.deepEqual(STARTERS,['capuchin','macaque','langur','mandrill','spider','howler']);
  assert.equal(Object.values(SPECIES).filter(s=>s.rarity==='ultra').length,8);
  assert.deepEqual(Object.values(SPECIES).map(s=>s.number).sort((a,b)=>a-b),Array.from({length:60},(_,i)=>i+1));
  for(const [id,s]of Object.entries(SPECIES)){assert.equal(s.id,id);assert.ok(s.learnset.includes('strike'));assert.ok(s.learnset.length>=6);assert.ok(s.types.length<=2);for(const type of s.types)assert.ok(ELEMENTS[type]);for(const move of s.learnset)assert.ok(MOVES[move],`${id} references ${move}`);}
});
test('all required technique families are usable by at least one curated species',()=>{
  const counts={};const reachable=new Set(Object.values(SPECIES).flatMap(s=>s.learnset));
  for(const [id,m]of Object.entries(MOVES)){counts[m.family]=(counts[m.family]||0)+1;assert.ok(reachable.has(id),`unreachable move ${id}`);assert.ok(Number.isFinite(m.cost)&&m.cost>=0);assert.ok(m.type==='neutral'||ELEMENTS[m.type]);if(m.types){assert.equal(m.types.length,2);assert.equal(m.splitDamage,true);}}
  assert.ok(counts.general>=24);assert.ok(counts.elemental>=32);assert.ok(counts.hybrid>=16);assert.ok(counts.ultra>=8);
  assert.equal(MOVES.strike.cost,0);assert.ok(MOVES.strike.power>0);
});
test('one complete authoritative eight-element chart covers every defensive pair',()=>{
  assert.equal(Object.keys(ELEMENTS).length,8);assert.deepEqual(Object.keys(TYPE_CHART).sort(),Object.keys(ELEMENTS).sort());
  for(const [attack,row]of Object.entries(TYPE_CHART)){assert.deepEqual(Object.keys(row).sort(),Object.keys(ELEMENTS).sort(),attack);for(const multiplier of Object.values(row))assert.ok(multiplier>0&&multiplier<=2);}
});
