import test from 'node:test';
import assert from 'node:assert/strict';
import { marriageDiscussionFor } from '../marriage-discussion-state.mjs';
const fixture = () => ({ royalBonds: { negotiations: { 'ashen:redharbor': {
  proposer: 'ashen', host: 'redharbor', actorMember: 'son', rulerMember: 'daughter',
  started: 8, lastDiscussed: 10, rounds: 3
} } } });
test('the receiving ruler sees the same discussed marriage from their own perspective', () => {
  const s = fixture(); const n = marriageDiscussionFor(s, 'redharbor', 'ashen');
  assert.equal(n.actorMember, 'daughter'); assert.equal(n.rulerMember, 'son');
  assert.equal(n.started, 8); assert.equal(n.rounds, 3); assert.equal(n.lastDiscussed, 10);
});
test('reading the reverse direction does not alter the stored match', () => {
  const s = fixture(), before = JSON.stringify(s); marriageDiscussionFor(s, 'redharbor', 'ashen');
  assert.equal(JSON.stringify(s), before);
});
test('a human reply stored in the reverse direction does not invalidate the original proposer', () => {
  const s = fixture(); const reply = marriageDiscussionFor(s, 'redharbor', 'ashen');
  s.royalBonds.negotiations['ashen:redharbor'] = { ...reply, lastDiscussed: 11, rounds: 4 };
  const original = marriageDiscussionFor(s, 'ashen', 'redharbor');
  assert.equal(original.actorMember, 'son'); assert.equal(original.rulerMember, 'daughter');
  assert.equal(original.started, 8); assert.equal(original.rounds, 4);
});
test('unrelated Houses cannot reuse a marriage discussion', () => {
  const s = fixture(); assert.equal(marriageDiscussionFor(s, 'sunspire', 'ashen'), null);
  assert.equal(marriageDiscussionFor(s, 'ashen', 'ashen'), null);
});
test('legacy saves without a marriage discussion remain safe', () => {
  assert.equal(marriageDiscussionFor({}, 'ashen', 'redharbor'), null);
});
