import { hash } from './world-hex.mjs';
export const MAP_PROFILES = Object.freeze({
  heartlands: {name:'Heartlands',description:'Broad fertile plains with wooded borders and winding highlands.',forest:.34,hills:.18,ranges:2,axis:'vertical',ridgeWidth:1.1,coast:1.6,basin:0},
  'great-divide': {name:'Great Divide',description:'A long central mountain spine, fertile valleys and guarded passes.',forest:.3,hills:.25,ranges:1,axis:'vertical',ridgeWidth:1.75,coast:1.3,basin:0},
  'highland-crown': {name:'Highland Crown',description:'Northern ranges and mineral-rich foothills above lowland kingdoms.',forest:.38,hills:.28,ranges:2,axis:'horizontal',ridgeWidth:1.2,coast:1.2,basin:0},
  'verdant-kingdoms': {name:'Verdant Kingdoms',description:'Extensive forests, open clearings and wooded river valleys.',forest:.58,hills:.16,ranges:2,axis:'diagonal',ridgeWidth:1,coast:1.5,basin:0},
  'great-basin': {name:'Great Basin',description:'An inland lake and fertile basin framed by highland ridges.',forest:.3,hills:.28,ranges:2,axis:'vertical',ridgeWidth:1.2,coast:1.2,basin:2.4},
  'broken-coast': {name:'Broken Coast',description:'Deep coastal inlets around connected inland plains and forests.',forest:.38,hills:.2,ranges:2,axis:'diagonal',ridgeWidth:1,coast:4.8,basin:0}
});
const LEGACY_PROFILES={crossroads:'heartlands',highlands:'highland-crown'};
export function selectMapProfile(seed, requested='random') {
  requested=LEGACY_PROFILES[requested]||requested;
  if(requested==='random')return Object.keys(MAP_PROFILES)[hash(seed,'map-profile')%Object.keys(MAP_PROFILES).length];
  if(!Object.hasOwn(MAP_PROFILES,requested))throw new Error('Unknown map profile.');
  return requested;
}
export function mapOptions(selected='random') {
  selected=LEGACY_PROFILES[selected]||selected;
  return [['random','Random map'],...Object.entries(MAP_PROFILES).map(([id,p])=>[id,p.name])].map(([id,name])=>`<option value="${id}" ${selected===id?'selected':''}>${name}</option>`).join('');
}
