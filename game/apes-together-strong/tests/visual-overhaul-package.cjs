/* Independent ZIP reader: verifies directory records, CRCs and exact release bytes. */
'use strict';
const fs = require('node:fs'), path = require('node:path'), zlib = require('node:zlib'), assert = require('node:assert/strict');
const directory = path.resolve(process.argv[2] || path.join(__dirname, '..', 'dist'));
const root = path.resolve(__dirname, '..');
function crc32(bytes) {
  let value = -1;
  for (const byte of bytes) {
    value ^= byte;
    for (let bit = 0; bit < 8; bit++) value = value & 1 ? value >>> 1 ^ 0xedb88320 : value >>> 1;
  }
  return (value ^ -1) >>> 0;
}
function readZip(filename) {
  const zip = fs.readFileSync(path.join(directory, filename)), files = new Map();
  const end = zip.length - 22;
  assert.equal(zip.readUInt32LE(end), 0x06054b50, 'valid end-of-directory');
  const count = zip.readUInt16LE(end + 10), centralLength = zip.readUInt32LE(end + 12);
  let position = zip.readUInt32LE(end + 16), start = position;
  for (let index = 0; index < count; index++) {
    assert.equal(zip.readUInt32LE(position), 0x02014b50);
    const packedSize = zip.readUInt32LE(position + 20), size = zip.readUInt32LE(position + 24), crc = zip.readUInt32LE(position + 16);
    const nameLength = zip.readUInt16LE(position + 28), extraLength = zip.readUInt16LE(position + 30), commentLength = zip.readUInt16LE(position + 32);
    const filename = zip.subarray(position + 46, position + 46 + nameLength).toString('utf8');
    assert.ok(!filename.startsWith('/') && !filename.includes('..') && !filename.includes('\\'), 'safe relative ZIP path');
    assert.ok(!files.has(filename), 'no duplicate entry ' + filename);
    const local = zip.readUInt32LE(position + 42);
    assert.equal(zip.readUInt32LE(local), 0x04034b50);
    assert.equal(zip.readUInt32LE(local + 14), crc);
    const localNameLength = zip.readUInt16LE(local + 26), localExtraLength = zip.readUInt16LE(local + 28);
    assert.equal(zip.subarray(local + 30, local + 30 + localNameLength).toString('utf8'), filename);
    const dataStart = local + 30 + localNameLength + localExtraLength;
    const data = zlib.inflateRawSync(zip.subarray(dataStart, dataStart + packedSize));
    assert.equal(data.length, size, filename + ' size');
    assert.equal(crc32(data), crc, filename + ' CRC');
    files.set(filename, data);
    position += 46 + nameLength + extraLength + commentLength;
  }
  assert.equal(position - start, centralLength);
  assert.equal(position, end);
  return files;
}
const sprites = readZip('apes-together-strong-sprites.zip');
const update = readZip('apes-together-strong-visual-update.zip');
assert.deepEqual(update.get('apes-together-strong.html'), fs.readFileSync(path.join(root, '../apes-together-strong.html')));
assert.ok(update.get('START-HERE.txt').includes('Open apes-together-strong.html'));
assert.ok(sprites.has('assets/visual/characters/manifest.json'));
assert.ok(sprites.has('assets/visual/environment/manifest.json'));
for (const [name, bytes] of sprites) {
  assert.deepEqual(bytes, fs.readFileSync(path.join(root, name)), 'sprite ZIP matches source: ' + name);
  assert.deepEqual(update.get('source/game/apes-together-strong/' + name), bytes, 'update contains sprite: ' + name);
}
for (const [name, bytes] of update) {
  if (!name.startsWith('source/game/apes-together-strong/')) continue;
  assert.deepEqual(bytes, fs.readFileSync(path.join(root, name.slice('source/game/apes-together-strong/'.length))), 'update matches source: ' + name);
}
console.log(`PASS: ${sprites.size} sprite files and ${update.size} update files; ZIP directory records, every decompressed CRC and byte-for-byte source parity verified.`);
