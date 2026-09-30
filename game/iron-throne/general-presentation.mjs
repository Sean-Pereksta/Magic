import {ART,displayArtURL} from './asset-manifest.mjs';
import {generalTemperament,generalTraits} from './general-roster.mjs';
export const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function generalPortrait(g){
  const url=ART.generals[g.characterId],src=url&&displayArtURL(url),initials=g.name.split(' ').map(x=>x[0]).join('');
  return `<span class="general-portrait" role="img" aria-label="${esc(g.name)} portrait"><span aria-hidden="true">${esc(initials)}</span>${src?`<img data-iron-art src="${esc(src)}" alt="">`:''}</span>`;
}
export function generalSummary(g){return `${esc(generalTemperament(g))} · ${esc(generalTraits(g))}`;}
