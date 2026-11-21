'use strict';

const { readFileSync } = require('node:fs');
const path = require('node:path');

const lineLimit = 2000;
const documentsScope = 'match /databases/{database}/documents {';
const official = 'match /acornRallyService/public {';
const lobbyValidator = 'function validAcornRallyLobby(code) {';
const lobbies = 'match /acornRallyLobbies/{code} {';
const queues = 'match /acornRallyHostQueues/{uid} {';
const headings = [official, lobbyValidator, lobbies, queues];

function readAcornBlock(lines, heading) {
  const start = lines.indexOf('    ' + heading);
  const end = lines.indexOf('    }', start + 1);
  if (start < 0 || end < 0) throw new Error('Missing Acorn Rally rule in firestore.rules: ' + heading);
  // Production may have a differently named shared authentication helper.
  return lines.slice(start, end + 1).join('\n').replaceAll('isAuthed()', '(request.auth != null)');
}

function fitLineLimit(lines) {
  if (lines.length <= lineLimit) return lines;
  const nonempty = lines.filter(line => line.trim());
  let excess = nonempty.length - lineLimit;
  const result = [];
  for (const line of nonempty) {
    const previous = result.at(-1);
    const continuation = line.trimStart();
    // Keep line comments intact, even when a URL makes this check conservative.
    if (excess > 0 && previous && !previous.includes('//') && /^(?:&&|\|\|)\s/.test(continuation) &&
        previous.length + continuation.length < 200) {
      result[result.length - 1] = previous.trimEnd() + ' ' + continuation;
      excess--;
    } else {
      result.push(line);
    }
  }
  if (result.length > lineLimit) throw new Error('The combined Firestore rules exceed ' + lineLimit + ' lines.');
  return result;
}

function prepareRules(activeRules, repositoryRules) {
  const active = activeRules.replace(/^\uFEFF/, '').trimEnd().split(/\r?\n/);
  const checkedIn = repositoryRules.split(/\r?\n/);
  const scope = active.findIndex(line => line.trim() === documentsScope);
  if (scope < 0) throw new Error('The active rules are missing the Firestore documents scope.');

  const present = new Set(active.map(line => line.trim()));
  if (present.has(lobbyValidator) !== present.has(lobbies)) {
    throw new Error('The active rules already have part of the Acorn Rally lobby policy. Review those rules before applying the update.');
  }

  const missing = headings.filter(heading => !present.has(heading));
  const addition = missing.map(heading => readAcornBlock(checkedIn, heading)).join('\n\n');
  if (addition) active.splice(scope + 1, 0, '', ...addition.split('\n'), '');
  // Firestore rule strings are single-line. Only join lines outside block comments.
  if (active.length > lineLimit && active.some(line => line.includes('/*'))) {
    throw new Error('Review block comments before compressing the Firestore rules.');
  }
  return fitLineLimit(active).join('\n');
}

if (require.main === module) {
  try {
    const [activePath, ...extra] = process.argv.slice(2);
    if (!activePath || extra.length) throw new Error('Usage: node desktop-hub/build/prepare-acorn-firestore.cjs active.rules');
    const active = readFileSync(activePath, 'utf8');
    const checkedIn = readFileSync(path.join(__dirname, '..', '..', 'firestore.rules'), 'utf8');
    process.stdout.write(prepareRules(active, checkedIn));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = { prepareRules };
