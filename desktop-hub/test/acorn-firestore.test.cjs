'use strict';

const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');
const { prepareRules } = require('../build/prepare-acorn-firestore.cjs');

const repositoryRules = readFileSync(path.join(__dirname, '..', '..', 'firestore.rules'), 'utf8');
const wrap = content => "rules_version = '2';\nservice cloud.firestore {\n  match /databases/{database}/documents {\n" + content + '\n  }\n}\n';

test('adds only the Acorn Rally rules and preserves the active game rules', () => {
  const unrelated = "    match /existingLobbies/{id} {\n      allow read: if resource.data.visibility == 'public';\n    }";
  const active = wrap(unrelated).replaceAll('\n', '\r\n');
  const result = prepareRules(active, repositoryRules);
  assert.match(result, /match \/acornRallyLobbies\/\{code\}/);
  assert.match(result, /match \/acornRallyHostQueues\/\{uid\}/);
  assert.match(result, /match \/acornRallyService\/public/);
  assert.match(result, /request\.auth != null/);
  assert.doesNotMatch(result, /isAuthed\(\)|managedGameDirectories/);
  assert.ok(result.includes(unrelated));
  assert.equal(prepareRules(result, repositoryRules), result);
});

test('leaves an existing official listing rule in place', () => {
  const official = '    match /acornRallyService/public {\n      allow get: if false;\n    }';
  const result = prepareRules(wrap(official), repositoryRules);
  assert.ok(result.includes(official));
  assert.equal(result.match(/match \/acornRallyService\/public/g).length, 1);
});

test('fits the production line budget without moving conditions into line comments', () => {
  const permission = "      allow read: if request.auth != null // signed in\n        && request.auth.uid == 'test'\n        && resource.data.enabled == true;";
  const active = wrap(Array.from({ length: 470 }, (_, i) =>
    '    match /collection' + i + '/{id} {\n' + permission + '\n    }'
  ).join('\n'));
  const result = prepareRules(active, repositoryRules);
  assert.equal(result.trimEnd().split('\n').length, 2000);

  const activePermissions = active.slice(active.indexOf('    match /collection0/'));
  const resultPermissions = result.slice(result.indexOf('    match /collection0/'));
  assert.equal(resultPermissions.trim().replace(/\s+/g, ' '), activePermissions.trim().replace(/\s+/g, ' '));
  assert.equal(resultPermissions.match(/\/\/ signed in\n/g).length, 470);
});

test('fits the checked-in rules within the deployment limit', () => {
  const result = prepareRules(repositoryRules, repositoryRules);
  assert.equal(result.split('\n').length, 2000);
  assert.equal(result.replace(/\s+/g, ' '), repositoryRules.trimEnd().replace(/\s+/g, ' '));
});

test('reports an already partial lobby policy or an uncompressible ruleset', () => {
  assert.throws(() => prepareRules(wrap('    match /acornRallyLobbies/{code} {\n    }'), repositoryRules), /part of/);
  assert.throws(() => prepareRules(wrap('    // description\n'.repeat(2000)), repositoryRules), /exceed 2000 lines/);
});
