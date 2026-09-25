import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { endTurn } from '../diplomacy.mjs';

test('progress reports real completed councils without changing round results', () => {
  const state=createGame(412), expected=structuredClone(state), events=[];
  endTurn(expected);
  endTurn(state, progress=>events.push(progress));
  assert.deepEqual(state,expected);
  assert.deepEqual(events.map(e=>e.phase),['preparing',...Array(6).fill('ai'),'resolving','complete']);
  assert.deepEqual(events.filter(e=>e.phase==='ai').map(e=>[e.completed,e.total]),
    Array.from({length:6},(_,i)=>[i,5]));
  assert.ok(events.every(e=>Object.keys(e).every(k=>['phase','completed','total'].includes(k))),
    'progress must not expose private plans or orders');
});

test('eliminated and human-controlled Houses are excluded from AI progress', () => {
  const state=createGame(412), events=[];
  state.controllers=Object.fromEntries(state.kingdoms.map((k,i)=>[k.id,{kind:i<2?'human':'ai',uid:i<2?`user-${i}`:null}]));
  const eliminated=state.kingdoms.at(-1).id;
  for(const tile of Object.values(state.tiles))if(tile.owner===eliminated)tile.owner=null;
  endTurn(state,p=>events.push(p));
  const ai=events.filter(p=>p.phase==='ai');
  assert.equal(ai.at(-1).completed,3);
  assert.equal(ai.at(-1).total,3);
});

test('founding and finished campaigns emit no turn progress', () => {
  for(const patch of [{phase:'founding'},{outcome:{winner:'ashen'}}]) {
    const state=Object.assign(createGame(),patch), before=structuredClone(state), events=[];
    endTurn(state,p=>events.push(p));
    assert.deepEqual(events,[]);assert.deepEqual(state,before);
  }
});
