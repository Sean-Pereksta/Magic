import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";

const source=readFileSync(new URL("./cloud-save.js",import.meta.url),"utf8");
const loader=readFileSync(new URL("../space-tyrants.html",import.meta.url),"utf8");

test("cloud saves reuse the repository Firebase project and existing lobby rules surface",()=>{
  assert.match(source,/projectId:\s*"bible-game-246c0"/);
  assert.match(source,/STX_CLOUD_COLLECTION="lobbies"/);
  assert.match(source,/signInAnonymously\(auth\)/);
  assert.match(source,/gameType:STX_CLOUD_GAME_TYPE/);
  assert.match(source,/status:"active"/);
});

test("cloud payloads are password encrypted instead of storing the password in Firebase",()=>{
  assert.match(source,/PBKDF2/);
  assert.match(source,/AES-GCM/);
  assert.match(source,/crypto\.subtle\.encrypt/);
  assert.match(source,/crypto\.subtle\.decrypt/);
  assert.doesNotMatch(source,/password\s*:\s*password/);
  assert.doesNotMatch(source,/savePassword/);
});

test("official save and reopen controls are installed in both the communications menu and start screen",()=>{
  assert.match(source,/Save \/ Reopen/);
  assert.match(source,/id="stxCloudStartReopen"/);
  assert.match(source,/official\.onclick=stxOfficialSave/);
  assert.match(source,/Reopen Cloud Save/);
  assert.match(source,/Local Backup/);
});

test("cloud patch loads last so it can wrap the final Space Tyrants UI and save button",()=>{
  const cloud=loader.indexOf("./space-tyrants/cloud-save.js");
  const scaling=loader.indexOf("./space-tyrants/command-option-scaling.js");
  assert.ok(cloud>scaling);
  assert.ok(cloud>=0);
});