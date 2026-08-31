import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import test from 'node:test';

const cloudPath=new URL('./cloud-save.js',import.meta.url);
const source=readFileSync(cloudPath,'utf8');
const html=readFileSync(new URL('../arcane-wilds.html',import.meta.url),'utf8');

test('cloud save layer is valid JavaScript and loaded last',()=>{
  const checked=spawnSync(process.execPath,['--check',cloudPath.pathname],{encoding:'utf8'});
  assert.equal(checked.status,0,checked.stderr);
  const cloudAt=html.lastIndexOf('arcane-wilds/cloud-save.js');
  assert.ok(cloudAt>html.lastIndexOf('arcane-wilds/mobile-interaction.js'));
});

test('reuses the working Firebase project, anonymous auth and lobbies rules surface',()=>{
  assert.match(source,/projectId:"bible-game-246c0"/);
  assert.match(source,/AW_CLOUD_COLLECTION="lobbies"/);
  assert.match(source,/signInAnonymously/);
  assert.match(source,/AW_CLOUD_GAME_TYPE="arcane-wilds-cloud-save"/);
});

test('official save encrypts both base journey and expansion state without storing password',()=>{
  assert.match(source,/AES-GCM/);
  assert.match(source,/PBKDF2/);
  assert.match(source,/AW_CLOUD_KDF_ITERATIONS=210000/);
  assert.match(source,/localStorage\.getItem\(SAVE_KEY\)/);
  assert.match(source,/localStorage\.getItem\(EXPANSION_SAVE_KEY\)/);
  assert.match(source,/JSON\.stringify\(\{format:AW_CLOUD_FORMAT,base,expansion\}\)/);
  const recordStart=source.indexOf('firebase.setDoc(ref,{');
  const recordEnd=source.indexOf('});',recordStart);
  const firestoreRecord=source.slice(recordStart,recordEnd);
  assert.ok(recordStart>=0&&recordEnd>recordStart);
  assert.doesNotMatch(firestoreRecord,/\bpassword\b/i);
});

test('cloud reopen restores both save layers and exposes official save controls',()=>{
  assert.match(source,/localStorage\.setItem\(SAVE_KEY,bundle\.base\)/);
  assert.match(source,/localStorage\.setItem\(EXPANSION_SAVE_KEY,bundle\.expansion\)/);
  assert.match(source,/loadGame\(\)/);
  assert.match(source,/loadExpansion\(\)/);
  assert.match(source,/Reopen Cloud Journey/);
  assert.match(source,/Official Firebase Save/);
  assert.match(source,/awCloudHudSave/);
});
