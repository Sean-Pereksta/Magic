import test from 'node:test';
import assert from 'node:assert/strict';
import {registerRadioAudio,setRadioDucked,getRadioDuckState} from './radio-audio-bridge.mjs';

test('ducks radio and restores its exact previous volume',()=>{
  const audio={volume:.73};
  registerRadioAudio(audio);
  assert.equal(setRadioDucked(true,{level:.18}),true);
  assert.equal(audio.volume,.18);
  assert.equal(getRadioDuckState().restoreVolume,.73);
  setRadioDucked(false);
  assert.equal(audio.volume,.73);
});

test('never raises a radio that was already quieter than the duck level',()=>{
  const audio={volume:.1};
  registerRadioAudio(audio);
  setRadioDucked(true,{level:.18});
  assert.equal(audio.volume,.1);
  setRadioDucked(false);
  assert.equal(audio.volume,.1);
});

test('moves ducking to a newly registered radio stream without losing either volume',()=>{
  const first={volume:.8},second={volume:.6};
  registerRadioAudio(first);
  setRadioDucked(true,{level:.2});
  assert.equal(first.volume,.2);
  registerRadioAudio(second);
  assert.equal(first.volume,.8);
  assert.equal(second.volume,.2);
  setRadioDucked(false);
  assert.equal(second.volume,.6);
});
