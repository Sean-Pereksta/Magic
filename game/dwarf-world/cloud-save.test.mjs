import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";

const wrapper=readFileSync(new URL("../DwarfWorld.html",import.meta.url),"utf8");
const cloud=readFileSync(new URL("./cloud-save.js",import.meta.url),"utf8");

test("Dwarf World keeps the original game as the core and adds a start menu",()=>{
  assert.match(wrapper,/\.\/DwarfWorld-core\.html/);
  assert.match(wrapper,/id="newWorld"/);
  assert.match(wrapper,/id="showLoad"/);
  assert.match(wrapper,/id="localContinue"/);
  assert.match(wrapper,/id="loadCloud"/);
});

test("cloud saves reuse the repository Firebase project and lobby rules surface",()=>{
  assert.match(cloud,/projectId:"bible-game-246c0"/);
  assert.match(cloud,/const COLLECTION="lobbies"/);
  assert.match(cloud,/const GAME_TYPE="dwarf-world-cloud-save"/);
  assert.match(cloud,/signInAnonymously/);
});

test("cloud saves encrypt client-side with the same primitives as Arcane Wilds and Space Tyrants",()=>{
  assert.match(cloud,/AES-GCM/);
  assert.match(cloud,/PBKDF2/);
  assert.match(cloud,/KDF_ITERATIONS=210000/);
  assert.match(cloud,/crypto\.subtle\.encrypt/);
  assert.match(cloud,/crypto\.subtle\.decrypt/);
});

test("large Dwarf World saves are split below Firestore document limits",()=>{
  assert.match(cloud,/CHUNK_BYTES=640\*1024/);
  assert.match(cloud,/chunkCount/);
  assert.match(cloud,/dwarfWorldCloudSaveChunk/);
  assert.match(cloud,/readEncryptedPayload/);
});

test("the existing local DWGZ2 serializer remains authoritative",()=>{
  assert.match(wrapper,/dwarven_fortress_deepwild_remaster_v1/);
  assert.match(wrapper,/getElementById\("btnSave"\)/);
  assert.match(wrapper,/clickCoreControl\("btnLoad"/);
  assert.match(cloud,/raw\.startsWith\("DWGZ2:"\)/);
  assert.doesNotMatch(cloud,/tType|tHp|dwarves\s*:/);
});

test("the in-game Save button can sync the bound Firebase slot after the local save succeeds",()=>{
  assert.match(wrapper,/waitForCoreToast\(\/Saved locally\/i,15000\)/);
  assert.match(wrapper,/DwarfWorldCloud\.saveBound\(\{quiet:true,skipFlush:true\}\)/);
  assert.match(cloud,/function canAutoSave\(\)/);
});

test("combat and mining damage popups fade and are deleted after their lifetime",()=>{
  assert.match(cloud,/function installFxLifecycleGuard\(\)/);
  assert.match(cloud,/const remaining=Math\.max\(0,fx\.max-\(now-fx\.__dwFxBornAt\)\/1000\)/);
  assert.match(cloud,/fx\.life=Math\.min\(fx\.life,remaining\)/);
  assert.match(cloud,/if\(fx\.life<=0\)fxArray\.splice\(i,1\)/);
  assert.match(cloud,/gameFrame\.addEventListener\("load"/);
});
