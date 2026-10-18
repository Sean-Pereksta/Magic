/* Original, locally bundled Cats / Mice operators and armory. No server required. */
(function(root){
'use strict';
const version='dominion-resistance-2026-10-18.1';
const factions=Object.freeze({
  cats:Object.freeze({id:'cats',name:'Cat Dominion',shortName:'CATS',color:'#e8b66c',tint:[.91,.71,.42],icon:'factions/cats/icon.svg',description:'Disciplined armored infantry holding the Dominion frontier.'}),
  mice:Object.freeze({id:'mice',name:'Mouse Resistance',shortName:'MICE',color:'#82dfbd',tint:[.51,.87,.74],icon:'factions/mice/icon.svg',description:'Mobile resistance soldiers fighting for their home districts.'})
});
const characters=Object.freeze([
  ['cat_assault','cats','Cat Assault Trooper','assault','whisker','Armored rifle infantry with field radio and grenade harness.'],
  ['cat_heavy','cats','Cat Heavy Enforcer','heavy','thunderpaw','Blast collar, plated shoulders and linked support ammunition.'],
  ['cat_scout','cats','Cat Scout','scout','purr90','Light carrier, hydration rig and reconnaissance goggles.'],
  ['cat_marksman','cats','Cat Marksman','marksman','squeakshot','Tactical hood, camouflage cape and optical rangefinder.'],
  ['cat_stalker','cats','Cat Night Stalker','stalker','burrower','Subdued armor, twin low-light optics and suppressed carbine.'],
  ['cat_officer','cats','Cat Officer','officer','whisker','Peaked command helmet, rank bars and field command tablet.'],
  ['mouse_rifleman','mice','Mouse Rifleman','assault','whisker','Salvaged ceramic armor, radio and compact ammunition webbing.'],
  ['mouse_runner','mice','Mouse Runner','scout','purr90','Minimal armor, compact weapon and streamlined supply pack.'],
  ['mouse_engineer','mice','Mouse Engineer','engineer','clawbreaker','Tool rack, demolition charges, work lamp and reinforced gloves.'],
  ['mouse_sniper','mice','Mouse Recon Sniper','marksman','squeakshot','Reconnaissance hood, camouflage cloak and precision optics.'],
  ['mouse_shieldbearer','mice','Mouse Shieldbearer','heavy','clawbreaker','Heavy defensive armor and folded field shield on the pack.'],
  ['mouse_commander','mice','Mouse Commander','officer','burrower','Resistance command colors, field tablet and ceremonial cord.']
].map(([id,faction,name,role,weapon,description])=>Object.freeze({id,faction,name,role,weapon,description,model:'factions/'+faction+'/'+id+'.glb',portrait:'factions/'+faction+'/'+id+'.svg'})));
const weapons=Object.freeze([
  {id:'whisker',name:'WHISKER AR',label:'Whisker AR',type:'Assault rifle',damage:42,range:102,cool:.092,spread:.013,mag:30,reload:2.1,tracers:1,color:'#e5bd75',falloff:.29,kind:'hitscan',recoil:.80,recovery:6.0,movementPenalty:1.25,hearingRange:48,pattern:[.12,-.08,.16,.09,-.12,.10]},
  {id:'clawbreaker',name:'CLAWBREAKER',label:'Clawbreaker',type:'Heavy shotgun',damage:40,range:34,cool:.67,spread:.112,mag:7,reload:2.9,tracers:8,color:'#ef9e62',falloff:1.08,kind:'shotgun',recoil:1.70,recovery:4.8,movementPenalty:1.05,hearingRange:56,pattern:[-.08,.12]},
  {id:'purr90',name:'PURR-90',label:'Purr-90',type:'Compact SMG',damage:29,range:64,cool:.057,spread:.031,mag:50,reload:1.95,tracers:1,color:'#80d4c5',falloff:.70,kind:'hitscan',recoil:.52,recovery:7.4,movementPenalty:1.08,hearingRange:42,pattern:[-.10,.08,-.07,.13,-.06,.04]},
  {id:'burrower',name:'BURROWER',label:'Burrower',type:'Suppressed carbine',damage:38,range:88,cool:.115,spread:.011,mag:24,reload:1.95,tracers:1,color:'#a4cdb5',falloff:.36,kind:'hitscan',suppressed:true,recoil:.64,recovery:6.4,movementPenalty:1.20,hearingRange:14,pattern:[.07,-.05,.09,-.04]},
  {id:'squeakshot',name:'SQUEAKSHOT',label:'Squeakshot',type:'Marksman rifle',damage:79,range:146,cool:.43,spread:.0035,mag:10,reload:2.55,tracers:1,color:'#d3d9ae',falloff:.10,kind:'hitscan',recoil:1.32,recovery:5.1,movementPenalty:1.65,hearingRange:60,pattern:[.04,-.04]},
  {id:'thunderpaw',name:'THUNDERPAW',label:'Thunderpaw',type:'Heavy machine gun',damage:43,range:116,cool:.108,spread:.022,mag:80,reload:3.9,tracers:1,color:'#e9b565',falloff:.31,kind:'hitscan',recoil:1.28,recovery:4.0,movementPenalty:1.80,hearingRange:66,pattern:[.08,.12,.17,-.12,-.16,.11]}
].map(w=>Object.freeze({...w,pattern:Object.freeze(w.pattern),model:'weapons/new/'+w.id+'.glb',preview:'weapons/new/'+w.id+'.svg'})));
const characterById=Object.freeze(Object.fromEntries(characters.map(c=>[c.id,c]))),weaponById=Object.freeze(Object.fromEntries(weapons.map(w=>[w.id,w])));
function character(id,faction='cats'){return characterById[id]&&characterById[id].faction===faction?characterById[id]:characters.find(c=>c.faction===faction)||characters[0];}
function modelPath(id){return characterById[id]?.model||weaponById[id]?.model||'';}
const assetPaths=Object.freeze([...characters.flatMap(c=>[c.model,c.portrait]),...weapons.flatMap(w=>[w.model,w.preview]),...Object.values(factions).map(f=>f.icon),'campaign/ui/faction-war.svg',...'ABCDE'.split('').map(s=>'conquest/objectives/sector-'+s.toLowerCase()+'.svg')]);
root.NeonFactions=Object.freeze({version,factions,characters,weapons,characterById,weaponById,character,modelPath,assetPaths});
if(typeof module!=='undefined'&&module.exports)module.exports=root.NeonFactions;
})(typeof window!=='undefined'?window:globalThis);
