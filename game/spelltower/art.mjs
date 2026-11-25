// Original, resolution-independent storybook artwork. These SVGs deliberately use
// local shapes and colours only, so the offline game never needs an image service.
const INK = '#18313b';
const escapeClass = value => String(value).replace(/[^a-zA-Z0-9_ -]/g, '');
const svg = (body, className = '', viewBox = '0 0 64 64') =>
  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${viewBox}" class="spell-art ${escapeClass(className)}" aria-hidden="true" focusable="false"><g stroke="${INK}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">${body}</g></svg>`;
const shine = '<path d="M10 14v6m-3-3h6M52 43v6m-3-3h6" stroke="#fff2bd" stroke-width="1.8" opacity=".8"/>';
const shadow = '<ellipse cx="32" cy="56" rx="22" ry="4" fill="#152d39" opacity=".25" stroke="none"/>';

const ELEMENT_ART = {
  fire: `<path d="M33 5c5 13-1 16 8 23 2-5 3-9 2-13 14 13 17 26 11 35-7 12-32 13-41 0C4 37 17 26 21 16c2 8 5 12 7 12 5-8 0-15 5-23Z" fill="#ef7448"/>
    <path d="M32 20c2 9-3 12 4 19 2-3 4-5 4-9 9 11 10 16 5 22-5 6-16 7-22 1-7-7-1-15 3-20 0 7 3 9 5 10 4-8-1-14 1-23Z" fill="#ffc361" stroke="#9b4c35" stroke-width="1.5"/>
    <path d="M33 41c5 4 9 9 4 13-3 3-8 2-9-1-3-4 1-7 5-12Z" fill="#fff0ae" stroke="none"/>
    <path d="m12 13 2-5m34-1 3 5M7 31l-3-3" stroke="#ffe7a2"/>`,
  water: `<path d="M33 5c-5 10-21 25-22 35-2 12 7 20 20 20s24-8 22-20C51 28 39 18 33 5Z" fill="#4bbbd8"/>
    <path d="M39 20c4 9 9 18 7 25-3 10-14 12-24 8 4 6 16 7 24 1 12-10 4-24-7-34Z" fill="#3289b6" stroke="none"/>
    <path d="M26 26c-5 5-8 11-7 16" fill="none" stroke="#d3fbef" stroke-width="5"/>
    <path d="M16 48c8-4 17 5 27 0" fill="none" stroke="#9ae5e6" stroke-width="2"/>
    <circle cx="48" cy="15" r="4" fill="#a2eaf0"/><circle cx="9" cy="18" r="2" fill="#bbf5ef" stroke="none"/>`,
  nature: `<path d="M18 57c3-15 10-28 24-43" fill="none" stroke="#536b37" stroke-width="6"/>
    <path d="M29 41C10 43 8 27 9 15c16-1 25 9 20 26Z" fill="#71b65f"/>
    <path d="M29 32C25 12 43 6 57 7c-1 17-10 28-28 25Z" fill="#9ccc6f"/>
    <path d="M21 50c8-16 21-15 30-8-4 12-16 15-30 8Z" fill="#50a880"/>
    <path d="m15 23 14 18m1-10L49 15M23 50l20-6" fill="none" stroke="#345b43" stroke-width="1.8"/>
    <path d="m34 21 6 1 1-6M19 32l4-1-1-6" fill="none" stroke="#d3e9a0" stroke-width="1.4"/>
    <circle cx="12" cy="50" r="3" fill="#efad7c"/><path d="m56 29-2 4m4-2-4-1" stroke="#d9eaa6"/>`,
  lightning: `<path d="M34 3 12 35h16l-6 26 32-38H38l9-20Z" fill="#f6c85d"/>
    <path d="m35 10-15 20h16l-7 18 17-21H33l7-17Z" fill="#fff2ac" stroke="none"/>
    <path d="m9 17-4-3m6 32-7 3m47-7 7 3M53 11l5-4" stroke="#8fdad8" stroke-width="3"/>
    <path d="m25 4-4 4m26 46-4 5" stroke="#fff2ac"/>`,
  ice: `<path d="m31 5 17 11 8 26-24 18L9 43l6-27Z" fill="#79cbd7"/>
    <path d="m31 5 2 24 15-13ZM15 16l18 13-24 14ZM33 29l23 13-24 18Z" fill="#c5edf0" stroke="#3f8da5" stroke-width="1.4"/>
    <path d="M33 29 32 60m1-31 15-13" fill="none" stroke="#4292ae" stroke-width="1.4"/>
    <path d="m22 18 7-7M15 37l4-11" stroke="#f0ffff" stroke-width="3"/>
    <path d="M52 7v7m-4-3h8M7 52v5m-3-2h6" stroke="#d9f9fa" stroke-width="1.8"/>`,
  earth: `<path d="m11 25 15-13 19 4 12 23-9 17-31 2L6 45Z" fill="#a5a27e"/>
    <path d="m11 25 19 8-4-21m4 21 15-17M30 33l18 23 9-17Z" fill="#c4bd8f" stroke="#676957" stroke-width="1.5"/>
    <path d="m30 33-13 25-11-13 5-20Z" fill="#818c72" stroke="#676957" stroke-width="1.5"/>
    <path d="m15 25 10-8m15 7 8 12" stroke="#e7d9a8" stroke-width="2.5"/>
    <path d="M12 45c1-9 7-11 12-7-1 5-8 9-12 7Zm25-25c-2-9 6-13 14-13-1 9-6 14-14 13Z" fill="#75aa5d"/>
    <path d="m39 17 7-6" stroke="#c3d98f" stroke-width="1.6"/>`,
  wind: `<path d="M8 23h29c13 0 16-13 8-16-7-4-13 1-11 7" fill="none" stroke="#eff9dc" stroke-width="7"/>
    <path d="M8 23h29c13 0 16-13 8-16-7-4-13 1-11 7" fill="none" stroke="#578a91" stroke-width="2"/>
    <path d="M6 34h43c12 0 14 13 6 16-6 3-12-1-10-6" fill="none" stroke="#b4e0cf" stroke-width="7"/>
    <path d="M6 34h43c12 0 14 13 6 16-6 3-12-1-10-6" fill="none" stroke="#578a91" stroke-width="2"/>
    <path d="M13 45h17c10 0 10 12 2 12" fill="none" stroke="#d1efdd" stroke-width="6"/>
    <path d="M13 45h17c10 0 10 12 2 12" fill="none" stroke="#578a91" stroke-width="1.8"/>
    <path d="M9 9c9-3 13 1 12 8-7 2-11-1-12-8Z" fill="#92bd72"/><path d="m12 11 8 5" stroke="#517c62" stroke-width="1.4"/>`,
  arcane: `<path d="m32 4 9 15 17 5-12 15 1 18-15-7-16 7 1-18L6 24l17-5Z" fill="#bc9ad9"/>
    <path d="m32 15 6 13 12 1-10 8 1 13-9-9-10 9 2-13-10-8 12-1Z" fill="#f2d7ed" stroke="#926fab" stroke-width="1.4"/>`,
};

const ALIASES = {plant:'nature', vine:'nature', light:'lightning', electric:'lightning', frozen:'ice', wall:'earth', neutral:'arcane'};

export function icon(key, className = '') {
  const element = ALIASES[key] || key;
  return svg((ELEMENT_ART[element] || ELEMENT_ART.arcane) + (element === 'arcane' ? shine : ''), `element-art ${className}`);
}

const ENEMY_ART = {
  rat: `<path d="M45 43c16-3 18 8 10 11" fill="none" stroke="#c58c99" stroke-width="5"/>
    <path d="M9 45c-2-10 6-19 18-21 14-3 23 5 23 17 0 12-29 15-40 7Z" fill="#9d9aab"/>
    <path d="M9 38 3 47l13 4 6-14Z" fill="#bdb0ba"/><path d="m22 49-1 7h8m11-8v6h8" fill="none" stroke-width="3"/>
    <circle cx="21" cy="26" r="8" fill="#bdabb7"/><circle cx="21" cy="26" r="4" fill="#d98c9b" stroke="none"/>
    <circle cx="13" cy="40" r="2.5" fill="#1f2c3a" stroke="none"/><path d="m5 46 2 1M7 42l-5-2m7 6-7 2" fill="none" stroke-width="1.3"/>
    <path d="M29 29c9-1 15 3 17 9" fill="none" stroke="#c4bdc8" stroke-width="2.3"/>`,
  imp: `<path d="m12 32-8-9 2 20 13-1m34-10 8-9-2 20-13-1" fill="#a95366"/>
    <path d="m18 14-4-10 12 6m20 4 4-10-12 6" fill="#e5c391"/>
    <path d="M15 25c-3-20 38-20 35 0l-2 17c-1 8-6 13-16 13S17 48 16 42Z" fill="#df7967"/>
    <path d="m18 22 11 4m17-4-11 4" fill="none" stroke-width="3"/>
    <path d="m21 28 6 1m10 0 6-1" stroke="#fff0b3" stroke-width="3"/>
    <path d="m26 39 6 3 7-3m-18 9-6 9h11m17-9 6 9H38" fill="#b95f5d"/>
    <path d="M46 49c12-2 8-14 13-16l1 7" fill="none" stroke="#c2686b" stroke-width="3"/>`,
  orc: `<path d="m12 36-5 15 13 3m32-18 5 15-13 3" fill="#7d9a67"/>
    <path d="m19 37-2 20h12l3-9 3 9h12l-2-20Z" fill="#675c69"/>
    <path d="M18 14 11 9l2 15 6 5m26-15 8-5-2 15-6 5" fill="#91ae73"/>
    <path d="M19 13q13-7 26 0l2 22q-14 13-30 0Z" fill="#9fba7e"/>
    <path d="m21 24 7 1m8 0 7-1" stroke-width="3"/>
    <path d="m24 35 8 3 8-3m-18-3 1 8 5-5m14-3-1 8-5-5" fill="#e4dbbd"/>
    <path d="m15 39 17 7 17-7-3 13-14 5-14-5Z" fill="#976c56"/>
    <path d="m31 44-3 10m8-10 3 10" stroke="#d7b889" stroke-width="1.6"/>`,
  troll: `<path d="M14 24C3 29 2 44 5 52l13 2 5-18m28-12c10 5 11 20 8 28l-13 2-5-18" fill="#698b83"/>
    <path d="M18 20q14-11 28 0l6 22-8 15H20l-8-15Z" fill="#7fa69a"/>
    <path d="m18 16-7-3 4 11m31-8 7-3-4 11" fill="#aac4a5"/>
    <path d="M20 12q12-7 24 0l2 19q-13 9-28 0Z" fill="#9eba9f"/>
    <path d="m23 19 6 1m7 0 6-1" stroke-width="3"/><path d="m29 22-4 7h12l-4-7" fill="#789887"/>
    <path d="m24 33 3 7 4-6m9-1-3 7-4-6" fill="#f0e1b6"/>
    <path d="m16 47 16 4 16-4-4 11H20Z" fill="#786367"/>
    <path d="m19 10 2-6 7 2 5-4 6 4 6-1-2 8" fill="#72884b"/>`,
  warg: `<path d="m12 33-4-15 13 8 14-7 15 10 7 19-9 8-12-5-12 3-15-7Z" fill="#8294a3"/>
    <path d="m27 28 6-19 8 13 13-11-1 24-15 16-16-7Z" fill="#a3adb7"/>
    <path d="m30 29 5-10 2 11m7-2 6-9-1 12" fill="#c4a8aa" stroke-width="1.3"/>
    <path d="m29 35 7 2m7-2 6-3" stroke="#f4d789" stroke-width="3"/>
    <path d="m35 42 3 8 9-7-4-4Z" fill="#d6c9ba"/><path d="m41 40 5 1-3 4Z" fill="#24333e"/>
    <path d="m16 48-2 9h10m15-6v6h10M10 37 3 29l1 16 9 4" fill="#748594"/>`,
  mage: `<path d="m17 34-8 24h46l-8-24-9-6Z" fill="#786aa6"/>
    <path d="m31 33-6 25h15l-5-25" fill="#afa1ce"/>
    <path d="m17 28 3-10L35 5l10 18 5 6Z" fill="#514f80"/>
    <path d="m14 28 19-4 20 5-5 6H18Z" fill="#8f7daf"/>
    <path d="M24 34v9q8 7 16-1v-8Z" fill="#37475a" stroke-width="1.5"/>
    <path d="m27 37 3 1m5 0 3-1" stroke="#97edd8" stroke-width="2.7"/>
    <path d="m10 55 3-33" stroke="#b79f83" stroke-width="4"/>
    <path d="m13 9 7 8-7 11-7-11Z" fill="#a7e5c9"/><path d="m12 13-2 5" stroke="#e0fff1" stroke-width="2"/>`,
  dlord: `<path d="M20 18 7 5l2 18 12 7m23-12L57 5l-2 18-12 7" fill="#9c6263"/>
    <path d="m19 33-13 5 4 19 14-5m21-19 13 5-4 19-14-5" fill="#8c485c"/>
    <path d="m21 28-5 29h32l-5-29Z" fill="#624b65"/>
    <path d="M20 14q12-6 24 0l3 21-15 13-15-13Z" fill="#c77565"/>
    <path d="m21 23 9 3m4 0 9-3" stroke="#ffe49d" stroke-width="3"/>
    <path d="m24 34 8 5 8-5m-14 2 1 6 4-4m7-2-1 6-4-4" fill="#f1d7b4"/>
    <path d="m25 50 7-5 7 5-7 8Z" fill="#f2a458" stroke-width="1.5"/>`,
  golem: `<path d="m7 28 11-5 6 11-5 17-14-2Z" fill="#839189"/>
    <path d="m46 24 12 5 2 20-14 3-6-16Z" fill="#829187"/>
    <path d="m19 27 13-7 13 7 3 23-16 7-17-7Z" fill="#a4aa92"/>
    <path d="m21 9 21-2 6 12-8 12-19-3-5-9Z" fill="#bdc0a4"/>
    <path d="m21 19 7 1m7 0 7-2" stroke="#a4e0c1" stroke-width="3"/>
    <path d="m32 26-2 12 8 5-6 12m-15-20 13 3m8 5 9-7" fill="none" stroke="#697b73" stroke-width="1.7"/>
    <path d="m32 33 6 6-6 7-5-7Z" fill="#83d2b9" stroke="#446e6a" stroke-width="1.4"/>
    <path d="m17 50-3 10h16l1-8m5 0 1 8h15l-5-10" fill="#809084"/>
    <path d="M18 10c2-7 8-5 11-2m10 0c2-5 7-5 9-2M9 32l3-3" fill="none" stroke="#7fa464" stroke-width="4"/>`,
  slime: `<path d="M9 43c1-8 7-9 9-16 4-17 22-18 29-4 4 8 4 12 8 18 8 12-2 17-22 17C11 58 4 53 9 43Z" fill="#80c59b"/>
    <path d="M19 25c4-7 12-10 17-6" fill="none" stroke="#dcf1b7" stroke-width="5"/>
    <path d="M14 49c13 6 32 6 39-1" fill="none" stroke="#4d9c89" stroke-width="4"/>
    <ellipse cx="24" cy="38" rx="3" ry="4" fill="#314e50" stroke="none"/><ellipse cx="41" cy="38" rx="3" ry="4" fill="#314e50" stroke="none"/>
    <path d="m30 44 3 2 4-2" fill="none" stroke-width="1.6"/>
    <circle cx="45" cy="22" r="3" fill="#d2e8a9" stroke="none"/>`,
  specter: `<path d="M13 29C12 11 25 5 37 8s20 17 14 32l5 12-14-3-7 10-6-9-13 7 2-13-11-3Z" fill="#a2c9d0"/>
    <path d="M19 25c2-10 12-14 20-9" fill="none" stroke="#dcf0e4" stroke-width="3"/>
    <path d="m22 28 7 2-4 7-5-3Zm15 2 7-3 1 7-5 4Z" fill="#4b677e" stroke="#4b677e"/>
    <path d="M30 39q3-3 6 0l-1 9-5-3Z" fill="#678597" stroke="none"/>
    <path d="m11 28-7-3m52 12 5-4" stroke="#bfdedc"/>`,
  ogre: `<path d="m14 29-9 9 1 17 15-1 4-21m24-4 9 9-1 17-15-1-4-21" fill="#a5aa7c"/>
    <path d="M18 22q14-7 28 0l7 25-7 11H18l-7-11Z" fill="#b6b58a"/>
    <path d="M19 12q12-7 26 0l2 21-15 7-15-7Z" fill="#c7c49b"/>
    <path d="m20 9 2-7 7 6 6-7 4 7 8-3-2 9Z" fill="#b29c67"/>
    <path d="m21 22 8 1m7 0 7-1" stroke-width="3"/><path d="m23 32 2 8 5-6m11-2-2 8-5-6" fill="#f2e7bb"/>
    <path d="m17 42 15 6 15-6-1 15H18Z" fill="#826c62"/>
    <path d="m30 45-1 11 8-1-1-10Z" fill="#c9aa68"/>
    <path d="m6 54 5-30" stroke="#765e4d" stroke-width="5"/><path d="m5 15 13 2-1 17-13-2Z" fill="#858b81"/>`,
  dragon: `<path d="M24 29C13 9 4 12 4 7l-1 30 12-6 8 12m17-14C51 9 60 12 60 7l1 30-12-6-8 12" fill="#bd7e7b"/>
    <path d="m7 17 10 13 6 13M57 17 47 30l-6 13" fill="none" stroke="#895868" stroke-width="1.6"/>
    <path d="M23 28c-3 8-7 20-1 26 7 8 23 4 24-4 1-10-4-16-7-22Z" fill="#79a78b"/>
    <path d="m27 36-1 18q7 6 13-2l-4-16Z" fill="#c7c18a" stroke-width="1.5"/>
    <path d="M23 17 18 7l10 6m13 4 5-10-10 6" fill="#e0d3a6"/>
    <path d="m22 17 10-5 11 5 2 17-13 9-13-9Z" fill="#91b69a"/>
    <path d="m22 24 7 2m7 0 7-2" stroke="#eecf79" stroke-width="3"/>
    <path d="m25 33 7 2 7-2m-13 1 1 6 4-5m7-1-1 6-4-5" fill="#f4dfb3"/>
    <path d="M44 50c10 6 16 0 15-7" fill="none" stroke="#79a78b" stroke-width="4"/>
    <path d="m21 50-8 7h13m16-7 9 7H38" fill="#719e84"/>`,
};

export function drawEnemy(key) {
  const normalized = ({trol:'troll', slime_baby:'slime', goblin:'orc', wolf:'warg', boss:'dragon'})[key] || key;
  return svg(shadow + (ENEMY_ART[normalized] || ENEMY_ART.imp), `enemy-art enemy-art-${escapeClass(normalized)}`);
}

export function drawTower() {
  return svg(`<ellipse cx="64" cy="149" rx="55" ry="8" fill="#18313b" opacity=".25" stroke="none"/>
    <path d="m16 116 47-12 48 12 9 25-19 10H29L9 141Z" fill="#7d9c85"/>
    <path d="M37 48h54l-2 92q-24 10-49 0Z" fill="#d7c6a0"/>
    <path d="M70 49h21l-2 91-19 4Z" fill="#b7a887" stroke="none"/>
    <path d="M43 61h41m-43 18h43m-44 20h45m-44 22h45M58 62v17m14 1v19m-18 1v20m23 2v20" fill="none" stroke="#a3947e" stroke-width="1.4"/>
    <path d="M31 51 63 9l34 42-6 8H38Z" fill="#6d819d"/>
    <path d="m41 47 22-31 6 32Z" fill="#a0afbc" stroke="none"/>
    <path d="M62 9V1m1 3 22 5-21 7" fill="#b69acb"/>
    <path d="M56 76V65q8-9 16 0v11Z" fill="#4a697d"/>
    <path d="M60 73v-8m7 8v-9" stroke="#c0e8db" stroke-width="2"/>
    <path d="M51 143v-28q13-15 26 0v28" fill="#6b685b"/>
    <path d="M57 141v-23q7-9 14 0v23Z" fill="#9f9875" stroke-width="1.4"/>
    <path d="M64 116v26" stroke="#645e51" stroke-width="1.6"/><circle cx="68" cy="132" r="1.7" fill="#f4d98d" stroke="none"/>
    <path d="m18 82 22-5v65l-20-2Z" fill="#b8b69b"/>
    <path d="m14 80 17-28 17 28-3 8H17Z" fill="#788fa7"/>
    <path d="M27 111V98q5-5 10 0v13Z" fill="#4a697d"/>
    <path d="m88 91 22 4-2 45-20 2Z" fill="#b7b294"/>
    <path d="m83 92 16-26 17 30-28 2Z" fill="#687f99"/>
    <path d="M96 119v-12q5-5 9 1v12Z" fill="#4a697d"/>
    <path d="M17 140c3-8 10-9 15-4m59 6c6-11 17-10 23-1M37 103c-8-3-11 4-6 9 7 2 7 9 0 12" fill="none" stroke="#6d9d68" stroke-width="5"/>
    <path d="m39 101 7-5-2 9m-11 18 7-2-3 6" fill="#90b67c" stroke-width="1.4"/>
    <path d="m52 91 3-4 3 4-3 4Z" fill="#bcebd9" stroke-width="1.3"/>
    <path d="M53 13v6m-3-3h6M105 54v7m-3-4h6" fill="none" stroke="#f3deb0" stroke-width="1.4"/>`, 'tower-art', '0 0 128 160');
}
