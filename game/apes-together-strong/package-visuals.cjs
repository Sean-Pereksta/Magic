/* Reproducible, dependency-free distribution of the playable game and original art. */
'use strict';
const fs = require('node:fs'), path = require('node:path'), zlib = require('node:zlib');
const root = __dirname;
const output = path.resolve(process.argv[2] || path.join(root, 'dist'));
fs.mkdirSync(output, { recursive: true });
const crcTable = Array.from({ length: 256 }, (_, n) => {
  for (let k = 0; k < 8; k++) n = n & 1 ? 0xedb88320 ^ (n >>> 1) : n >>> 1;
  return n >>> 0;
});
function crc32(buffer) {
  let crc = 0xffffffff;
  for (const n of buffer) crc = crcTable[(crc ^ n) & 255] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}
function walk(directory, prefix = '') {
  return fs.readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name)).flatMap(entry => {
    const absolute = path.join(directory, entry.name), name = prefix + entry.name;
    if (entry.isSymbolicLink()) throw new Error('Symlink excluded from release: ' + name);
    return entry.isDirectory() ? walk(absolute, name + '/') : [{ name, absolute }];
  });
}
function zip(filename, entries) {
  const local = [], central = [];
  let offset = 0;
  for (const entry of entries) {
    const name = Buffer.from(entry.name.replaceAll('\\', '/'));
    const data = entry.data ? Buffer.from(entry.data) : fs.readFileSync(entry.absolute);
    const packed = zlib.deflateRawSync(data, { level: 9 }), crc = crc32(data);
    const header = Buffer.alloc(30);
    header.writeUInt32LE(0x04034b50, 0); header.writeUInt16LE(20, 4);
    header.writeUInt16LE(0x800, 6); header.writeUInt16LE(8, 8);
    header.writeUInt16LE(33, 12); // Fixed DOS date: 1980-01-01, deterministic archives.
    header.writeUInt32LE(crc, 14); header.writeUInt32LE(packed.length, 18);
    header.writeUInt32LE(data.length, 22); header.writeUInt16LE(name.length, 26);
    local.push(header, name, packed);
    const record = Buffer.alloc(46);
    record.writeUInt32LE(0x02014b50, 0); record.writeUInt16LE(20, 4); record.writeUInt16LE(20, 6);
    record.writeUInt16LE(0x800, 8); record.writeUInt16LE(8, 10); record.writeUInt16LE(33, 14);
    record.writeUInt32LE(crc, 16); record.writeUInt32LE(packed.length, 20); record.writeUInt32LE(data.length, 24);
    record.writeUInt16LE(name.length, 28); record.writeUInt32LE(offset, 42);
    central.push(record, name); offset += header.length + name.length + packed.length;
  }
  const directory = Buffer.concat(central), end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); end.writeUInt16LE(entries.length, 8); end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(directory.length, 12); end.writeUInt32LE(offset, 16);
  fs.writeFileSync(path.join(output, filename), Buffer.concat([...local, directory, end]));
  console.log(filename + ': ' + entries.length + ' files');
}
const start = 'APES TOGETHER STRONG — VISUAL UPDATE\n\nOpen apes-together-strong.html in Chrome, Edge, Firefox, or Safari to play.\nAll game artwork and campaign audio are embedded. No server or network is required.\nThe source folder contains the original game modules, reusable artwork, manifests, tests and asset documentation.\nExisting saves remain compatible. The browser keeps saves per origin; use Pause > Export save in the old game and Import save in this copy to transfer a run.\n\nRebuild: node source/game/apes-together-strong/build.cjs\nSprite documentation: source/game/apes-together-strong/assets/visual/README.md\nImplementation and validation: source/game/apes-together-strong/VISUAL-UPDATE.md\n';
const sprites = walk(path.join(root, 'assets', 'visual'), 'assets/visual/');
zip('apes-together-strong-sprites.zip', sprites);
const source = walk(root).filter(entry => !entry.name.startsWith('dist/') && !entry.name.endsWith('.zip'));
zip('apes-together-strong-visual-update.zip', [
  { name: 'START-HERE.txt', data: start },
  { name: 'apes-together-strong.html', absolute: path.join(root, '../apes-together-strong.html') },
  ...source.map(entry => ({ ...entry, name: 'source/game/apes-together-strong/' + entry.name }))
]);
fs.copyFileSync(path.join(root, '../apes-together-strong.html'), path.join(output, 'apes-together-strong.html'));
