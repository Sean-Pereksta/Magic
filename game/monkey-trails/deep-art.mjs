// The same engraved symbols appear on the trail, in the journal and on species.
const glyphs = {
  bell: 'M-16 9Q-10 4-10-5Q-10-18 0-18Q10-18 10-5Q10 4 16 9ZM-4 12Q0 20 4 12',
  mail: 'M-20-12H20V13H-20ZM-20-12L0 3 20-12',
  ferry: 'M-26 5H28L17 17H-13ZM-6 4V-21L15-3H-6',
  nest: 'M-21 0Q0 10 21 0Q18 18 0 19Q-17 16-21 0ZM-13-2Q-14-26-3-13L0-2Q3-24 12-16Q18-9 12 0',
  lantern: 'M-11-10L-15 11H15L11-10ZM-11-13Q0-29 11-13ZM-4 8L0-4 5 8',
  forge: 'M-20 2H21L12 10H2V18H-10V10ZM-12-7L1-22 13-12 0 1Z',
  shell: 'M0 16Q-25 0-22-11Q-17-21-9-17Q0-29 9-17Q18-21 22-10Q23 1 0 16ZM0 10L-12-12M0 10V-15M0 10L12-12',
  drum: 'M-18-8Q0-20 18-8V12Q0 23-18 12ZM-18-8Q0 4 18-8M-12-2L-6 13 1 0 9 13 16-3',
  bridge: 'M-25-13V19M25-13V19M-25-9Q0 14 25-9M-25 2Q0 23 25 2M-12 0V10M0 3V14M12 0V10',
  knot: 'M-24 10Q-5-20 10-10Q25 0 8 12Q-7 21-13 6Q-19-9-4-14L23 10',
  water: 'M0-23Q-26 4-16 14Q0 29 16 14Q27 4 0-23Z',
  court: 'M-19 15V-7L0-19 19-7V15ZM-8 15V-4H8V15M-24 19H24',
  cable: 'M-23 14Q-8-24 2-10T23-12M-23 19Q-8-19 2-5T23-7',
  mirror: 'M-17 0L0-23 17 0 0 23ZM0-23V23M-17 0H17',
  crown: 'M-21-12L-9-3 0-19 9-3 21-12 16 13H-16Z',
  claw: 'M-21 16Q-12-12 8-20L-5 9ZM-5 18Q6-10 21-14L9 14Z',
  coil: 'M-22 8Q-21-14-2-16Q22-16 20 4Q18 22-3 18Q-19 17-14 1Q-7-12 7-7Q18 0 7 8Q-2 13-3 4',
  kelp: 'M-17 16Q-29-1-8-20Q-12-3-2 2Q0-14 15-22Q8-3 17 8L4 19Z',
  shade: 'M-21 12Q-10-14 1-20Q21-9 18 9L7 3 2 17-5 6Z',
};
export const motifs = [
  'bell',
  'mail',
  'ferry',
  'nest',
  'lantern',
  'forge',
  'shell',
  'drum',
  'bridge',
  'knot',
  'water',
  'court',
];
export const badges = [
  'shell',
  'kelp',
  'forge',
  'cable',
  'claw',
  'coil',
  'mirror',
  'shade',
  'bell',
  'shell',
  'shade',
  'mirror',
  'crown',
];
export function emblem(key, x = 0, y = 0, size = 1, color = '#ffc97d') {
  return `<path transform="translate(${x} ${y}) scale(${size})" d="${glyphs[key]}" stroke="#243f49" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round" fill="${color}"/>`;
}
const palettes = [
  ['#173d61', '#85d6cc', '#fae7aa'],
  ['#444365', '#a6d3cd', '#f2d5ad'],
  ['#823f54', '#e9b38e', '#ffefbc'],
];
export function huntIllustration(p, stage) {
  const index = ['nacreon', 'vesperloom', 'vitreus', 'orrakai'].indexOf(p.id);
  const keys = [
    ['shell', 'ferry', 'bell'],
    ['knot', 'shade', 'claw'],
    ['mirror', 'water', 'mirror'],
    ['court', 'crown', 'claw'],
  ][index];
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 600 160" style="width:100%;max-height:120px;border-radius:12px" role="img" aria-label="${p.title} investigation"><g transform="translate(0 -85)">${panorama([0, 1, 2, 2][index])}</g>${keys.map((k, i) => emblem(k, 170 + i * 115, 83, i === Math.min(2, stage) ? 1.7 : 0.85, i === Math.min(2, stage) ? '#ffdda0' : '#cee0d5')).join('')}</svg>`;
}
export function panorama(i) {
  const [sky, ground, sun] = palettes[i];
  return `<rect width="600" height="340" fill="${sky}"/><circle cx="443" cy="79" r="35" fill="${sun}"/><path d="M0 185Q95 78 177 153T348 120Q450 57 600 150V340H0" fill="${ground}" opacity=".52"/><path d="${['M0 205Q90 179 175 200T356 191T600 200V340H0', 'M0 134L169 155 222 311 349 310 423 158 600 109V340H0', 'M0 230L76 180 108 216 204 159 249 210 358 182 450 211 600 152V340H0'][i]}" fill="${ground}"/>${i === 0 ? '<path d="M150 206H345L302 234H187Z" fill="#684657"/><path d="M237 201V109L307 185H239" fill="#ffdbab"/>' : i === 1 ? '<path d="M140 139Q279 260 448 142M140 157Q280 277 448 160M197 178L197 199M240 195V215M287 199V221M335 191V211M382 171V193" fill="none" stroke="#e2ecdf" stroke-width="6"/>' : '<path d="M190 230L222 127 250 201 288 81 329 223 357 153 379 234Z" fill="#ffdfbc"/><path d="M222 127L225 227 250 201 288 81 298 223Z" fill="#b76c73"/>'}${[0, 1, 2, 3, 4, 5].map((j) => `<path d="M${j * 108 - 32} ${251 + (j % 3) * 17}q40 -10 82 1" fill="none" stroke="${sky}" opacity=".42" stroke-width="4"/>`).join('')}`;
}
const chapterSymbols = [
  ['bell', 'coil', 'bell'],
  ['mail', 'knot', 'mail', 'court'],
  ['ferry', 'cable', 'knot', 'bell', 'ferry'],
  ['nest', 'claw', 'nest', 'kelp', 'nest'],
  ['mirror', 'shade', 'lantern', 'bridge', 'lantern'],
  ['forge', 'bell', 'shade', 'forge', 'bell'],
  ['shell', 'bell', 'shade', 'ferry'],
  ['ferry', 'drum', 'cable', 'bell', 'drum'],
  ['cable', 'knot', 'bridge', 'mail', 'bridge'],
  ['mail', 'cable', 'knot', 'cable', 'bridge', 'bridge'],
  ['claw', 'mirror', 'shade', 'water', 'water'],
  ['shell', 'bridge', 'claw', 'crown', 'court'],
];
const originalScenes = [
  '<path fill="#b8b7e5" d="M0 158L102 67 170 139 245 81 301 153 409 72 480 159V220H0Z"/><path fill="#ffe2a0" d="M390 30L358 92H385L366 144 427 73H399Z"/>',
  '<path fill="#396e5c" d="M0 62Q65-18 132 45T260 47T403 44T600 27V0H0Z"/><path d="M225 22Q218 100 280 118M487 16Q460 82 492 122" stroke="#50785d" fill="none" stroke-width="8"/>',
  '<path d="M222 183Q222 112 201 88M222 137Q266 106 266 77M466 184Q458 121 484 102" fill="none" stroke="#645971" stroke-width="9"/>',
  '<path d="M303 201l40-82 92-67-53 84-28 63Z" fill="#775f57"/><path d="M347 125l-32-47 48 30 45-47" fill="none" stroke="#987d64" stroke-width="13"/>',
  '<path fill="#899aad" d="M0 190L175 44 299 174 417 34 600 176V215H0Z"/><path fill="#e2f0e0" d="M118 92L175 44 228 99 177 77 160 91 153 80ZM367 81L417 34 465 76 424 62 402 73Z"/>',
  '<path fill="#684858" d="M140 201L302 70 347 78 415 148 511 201Z"/><path fill="#ffad6d" d="M302 70L347 78 342 105 373 130 349 155 392 194 334 152 354 131 323 110Z"/>',
];
function vignette(index, stage) {
  const symbols = chapterSymbols[index],
    symbol = symbols[Math.min(stage, symbols.length - 1)],
    finish = stage >= symbols.length - 1;
  const silhouette =
    '<path d="M-12 1q-13-10-18 0 3 12 12 6m31-6q13-10 18 0-3 12-12 6M-13 4q-5-13 13-15 17 1 15 16l6 16-8 4-4-9-3 16h-12l-1-16-9 10-6-6Z" fill="#284952"/>';
  return `<g>${originalScenes[index] || ''}<path d="M192 172Q285 137 373 161T588 144" fill="none" stroke="#fff3bc" stroke-width="3" stroke-dasharray="6 8" opacity=".7"/><g transform="translate(${245 + Math.min(stage, 4) * 38} 139) scale(.78)">${silhouette}</g>${emblem(symbol, 493, 111, 1.5, finish ? '#fff0ab' : '#f9c998')}${finish ? `<g transform="translate(452 144) scale(.52)">${silhouette}</g>` : ''}</g>`;
}
export function questArt(index, stage = 0) {
  return `<svg xmlns="http://www.w3.org/2000/svg" role="img" aria-label="${motifs[index]} trail illustration" viewBox="0 0 600 210" style="display:block;width:100%;max-height:135px;border-radius:12px"><g transform="translate(0 -63)">${panorama(index < 8 ? 0 : index < 10 ? 1 : 2)}</g>${vignette(index, stage)}<circle cx="95" cy="108" r="65" fill="#183747"/><circle cx="95" cy="108" r="57" stroke="#e5be84" fill="none" stroke-width="2"/>${emblem(motifs[index], 95, 108, 1.7)}${Array.from({ length: stage }, (_, j) => `<circle cx="${181 + j * 14}" cy="178" r="4" fill="#ffe6ac"/>`).join('')}</svg>`;
}
