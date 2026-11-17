// Run against the two trusted release checkouts. The catalog transformation is the
// website's own loader, including injected Truth games and game-specific overrides.
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const [, , magicArg, truthArg] = process.argv;
if (!magicArg || !truthArg) throw Error('Usage: node build-catalog.mjs <Magic checkout> <Truth checkout>');
const magic = path.resolve(magicArg), truth = path.resolve(truthArg);
const output = path.dirname(fileURLToPath(import.meta.url));
const text = name => fs.readFileSync(path.join(magic, 'lobby', name), 'utf8').replaceAll('\r\n','\n');
const revision = repo => execFileSync('git', ['-C',repo,'rev-parse','HEAD'],{encoding:'utf8'}).trim();
const core = text('lobby-core.html'), wrapper = text('lobby.html');
let finished;
const sandbox = vm.createContext({
  console, URL, URLSearchParams, fetch: async () => ({ok:true, text:async()=>core}),
  document:{open(){}, write(html){finished=html;}, close(){}, body:{}}
});
vm.runInContext(text('lobby-library.js'),sandbox);
const source = [...wrapper.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)].map(m=>m[1]).find(s=>s.includes('document.write('));
if (!source) throw Error('Review changed website loader');
await vm.runInContext(source,sandbox);
const registry = finished?.match(/const GAMES\s*=\s*(\[[\s\S]*?\n\s*\]);/);
if (!registry) throw Error('Review changed website registry');
const records = vm.runInContext('('+registry[1]+')',sandbox);
if (!records.length || new Set(records.map(g=>g.key)).size!==records.length) throw Error('Invalid game registry');
const art = JSON.parse(text('assets/game-artwork.json'));
const file = name => path.join(output,name);
fs.mkdirSync(file('art'),{recursive:true});
fs.copyFileSync(path.join(magic,'lobby/assets/catnmice-mark.svg'),file('art/catnmice-mark.svg'));
function copyArt(input, name){
  const dest = 'art/'+name+path.extname(input);
  fs.copyFileSync(input,file(dest));
  return dest;
}
const packManifest = fs.existsSync(file('pack-index.json')) ? JSON.parse(fs.readFileSync(file('pack-index.json'),'utf8')) : {};
const games = records.map(game => {
  if (!/^[a-z0-9_-]+$/i.test(game.key)) throw Error('Invalid key '+game.key);
  const id = game.key, artwork = art.games[id] || {};
  const assets = path.join(magic,'lobby','assets');
  const tile = [
    game.paintedTile && path.join(magic, game.paintedTile),
    game.imagePath?.startsWith('/lobby/assets/game-tiles/') && path.join(magic,game.imagePath),
    artwork.tile && path.join(assets,artwork.tile),
    path.join(assets,'game-tiles',id+'.webp'),
    game.imagePath && path.join(magic,game.imagePath),
    path.join(assets,art.fallback)
  ].find(p=>p && fs.existsSync(p));
  const cover = copyArt(tile,id);
  const srcHero = artwork.hero && path.join(assets,artwork.hero);
  const hero = srcHero && fs.existsSync(srcHero) && srcHero!==tile ? copyArt(srcHero,id+'-hero') : cover;
  const screenshot = 'art/'+id+'-gameplay.webp';
  const {key,desc,...web} = game;
  return {id, ...web, description:desc, tier:artwork.tier || (game.featured?'flagship':'library'),
    actions:sandbox.GameHubLibrary.getActions(game), art:cover, hero, screenshots:fs.existsSync(file(screenshot))?[screenshot]:[],
    offline:packManifest[id] || null};
});
const catalog = {version:1,site:'https://catnmice.com',source:{magic:revision(magic),truth:revision(truth)},games};
fs.writeFileSync(file('catalog.json'), JSON.stringify(catalog,null,2)+'\n');
console.log(games.length+' published catalog entries, '+games.filter(g=>g.offline).length+' offline packs');
