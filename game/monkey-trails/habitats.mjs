// Persistent neighborhood habitats. These zones are independent of rendering
// cells, short encounter vectors and expedition distance: returning to a place
// always returns to its same habitat. Geometry and lookup share the same edges.
import {LANDS} from './lands.mjs';

export const HABITAT_SIZE_METERS = 480;
const LIMIT=1e7,DEFAULT_LAND=LANDS[0].id;
const landIds=new Set(LANDS.map(l=>l.id));
const cycle=['grove','cave','riverbank','ruins','trail','landmark'];
const elements=['leaf','fire','water','storm','earth','ice','light','shadow'];
const weights=overrides=>Object.freeze(Object.fromEntries([...['fruit','ingot','item','fragment','charmMaterial','essence'],...elements].map(id=>[id,overrides[id]??1])));
export const HABITATS=Object.freeze({
 grove:Object.freeze({id:'grove',name:'Sunfruit Grove',shortName:'Grove',element:'leaf',types:Object.freeze(['leaf','shadow']),color:'#44864f',ink:'#eef9d1',resources:'More fruit and Leaf essence',description:'A broad grove with abundant fruit and leafy essence.',resourceWeights:weights({fruit:2.8,ingot:.4,fragment:.75,charmMaterial:.75,leaf:3.2,shadow:1.5})}),
 cave:Object.freeze({id:'cave',name:'Cave Formation',shortName:'Caves',element:'earth',types:Object.freeze(['earth','ice']),color:'#7b7494',ink:'#f2eaff',resources:'More ingots and Earth essence',description:'A long cave formation rich in rare ingots and Earth essence.',resourceWeights:weights({fruit:.45,ingot:4,fragment:1.8,charmMaterial:1.25,earth:3.4,ice:1.7})}),
 riverbank:Object.freeze({id:'riverbank',name:'Reed Wetland',shortName:'Wetland',element:'water',types:Object.freeze(['water','ice']),color:'#368d96',ink:'#e2ffff',resources:'More supplies and Water essence',description:'A wide reed wetland with useful supplies and Water essence.',resourceWeights:weights({fruit:1.2,ingot:.55,item:1.9,water:3.3,ice:1.8})}),
 ruins:Object.freeze({id:'ruins',name:'Ancient Ruins',shortName:'Ruins',element:'light',types:Object.freeze(['light','shadow']),color:'#ad9554',ink:'#fff7d7',resources:'More charm materials and Light essence',description:'A sprawling ruin with charm materials and Light essence.',resourceWeights:weights({fruit:.7,ingot:1.5,charmMaterial:2.8,fragment:2.1,light:3.1,shadow:1.9})}),
 trail:Object.freeze({id:'trail',name:'Wildflower Meadow',shortName:'Meadow',element:'leaf',types:Object.freeze([]),color:'#9b9f50',ink:'#fffbdc',resources:'More fruit and trail supplies',description:'An open meadow with fruit and useful trail supplies.',resourceWeights:weights({fruit:1.6,ingot:.6,item:1.7,leaf:1.5})}),
 landmark:Object.freeze({id:'landmark',name:'Sunstone Ridge',shortName:'Ridge',element:'storm',types:Object.freeze(['fire','storm','light']),color:'#b7825b',ink:'#fff0d8',resources:'More fragments and Storm essence',description:'A broad rocky ridge with fragments and Storm essence.',resourceWeights:weights({fruit:.65,ingot:2,fragment:2.5,charmMaterial:1.4,storm:3,fire:2,light:1.5})})
});
const origins={'emerald-jungle':0,'sunstone-savannah':4,'crystal-caverns':1,'whispering-marsh':2,'frostpeak-mountains':1,'thunder-highlands':5,'ember-volcano':5,'moonlit-forest':0,'celestial-ruins':3,'ancient-monkey-kingdom':3};
const names={
 'crystal-caverns':{grove:'Glowmoss Grove',cave:'Crystal Vein Caves',riverbank:'Underground Wetland',trail:'Luminous Clearing',landmark:'Prismatic Ridge'},
 'frostpeak-mountains':{grove:'Snowpine Grove',cave:'Froststone Caves',riverbank:'Glacial Wetland',trail:'Alpine Meadow',landmark:'Aurora Ridge'},
 'ember-volcano':{grove:'Ashwood Grove',cave:'Basalt Caves',riverbank:'Thermal Wetland',trail:'Ember Meadow',landmark:'Obsidian Ridge'},
 'moonlit-forest':{grove:'Moonfruit Grove',cave:'Moonstone Caves',riverbank:'Silver Reed Wetland',trail:'Moonflower Meadow'},
 'celestial-ruins':{grove:'Starlit Grove',cave:'Skycrystal Caves',ruins:'Starlight Ruins',trail:'Celestial Meadow'},
 'ancient-monkey-kingdom':{grove:'Royal Fruit Grove',ruins:'Golden Kingdom Ruins',landmark:'Crownstone Ridge'}
};
const finite=(n,fallback=0)=>Number.isFinite(n)?Math.max(-LIMIT,Math.min(LIMIT,n)):fallback;
const point=p=>({east:finite(p?.east),north:finite(p?.north)});
const land=id=>landIds.has(id)?id:DEFAULT_LAND;
const mod=(n,m)=>((n%m)+m)%m;
const seedOf=id=>[...id].reduce((a,c)=>Math.imul(a^c.charCodeAt(0),16777619),2166136261)>>>0;
function noise(x,y,seed){let h=Math.imul(x|0,374761393)^Math.imul(y|0,668265263)^seed;h=Math.imul(h^(h>>>13),1274126177);return((h^(h>>>16))>>>0)/4294967296;}
function vertex(x,y,seed){return{east:(x-.5)*HABITAT_SIZE_METERS+(noise(x,y,seed)-.5)*90,north:(y-.5)*HABITAT_SIZE_METERS+(noise(x,y,seed+1)-.5)*90};}
function patch(id,x,y){
 const seed=seedOf(id),kind=cycle[mod(x+2*y+origins[id],cycle.length)],definition=HABITATS[kind];
 const polygon=[vertex(x,y,seed),vertex(x+1,y,seed),vertex(x+1,y+1,seed),vertex(x,y+1,seed)];
 const center={east:polygon.reduce((sum,p)=>sum+p.east,0)/4,north:polygon.reduce((sum,p)=>sum+p.north,0)/4};
 return{...definition,name:names[id]?.[kind]||definition.name,landId:id,patchId:`${id}:${x}:${y}`,cell:{east:x,north:y},sizeMeters:HABITAT_SIZE_METERS,center,polygon};
}
function contains(p,polygon){
 // A point on a shared edge belongs to the first candidate in lookup order.
 let inside=false;
 for(let i=0,j=polygon.length-1;i<polygon.length;j=i++){
  const a=polygon[j],b=polygon[i],cross=(p.east-a.east)*(b.north-a.north)-(p.north-a.north)*(b.east-a.east);
  if(Math.abs(cross)<1e-6&&p.east>=Math.min(a.east,b.east)-1e-7&&p.east<=Math.max(a.east,b.east)+1e-7&&p.north>=Math.min(a.north,b.north)-1e-7&&p.north<=Math.max(a.north,b.north)+1e-7)return true;
  if((a.north>p.north)!==(b.north>p.north)&&p.east<(b.east-a.east)*(p.north-a.north)/(b.north-a.north)+a.east)inside=!inside;
 }
 return inside;
}

export function habitatAtPosition(landId,east=0,north=0){
 const id=land(landId),position={east:finite(east),north:finite(north)},x=Math.round(position.east/HABITAT_SIZE_METERS),y=Math.round(position.north/HABITAT_SIZE_METERS);
 for(let dx=-1;dx<=1;dx++)for(let dy=-1;dy<=1;dy++){const candidate=patch(id,x+dx,y+dy);if(contains(position,candidate.polygon))return{...candidate,position};}
 return{...patch(id,x,y),position};
}
export function habitatAt(state){
 const id=land(state?.world?.activeLand),position=point(state?.world?.regions?.[id]?.position||state?.run?.vector);
 return habitatAtPosition(id,position.east,position.north);
}

// Only nearby world patches are generated. Zoom changes their projection, never
// their size or identity. The same function drives visible borders and lookup.
export function visibleHabitatPatches(landId,{position={east:0,north:0},zoom=1,width=600,height=360,scale}={}){
 const id=land(landId),camera=point(position),pixels=Number.isFinite(scale)?Math.max(.1,Math.min(10,scale)):2.2*(Number.isFinite(zoom)?Math.max(.65,Math.min(2,zoom)):1);
 width=Number.isFinite(width)?Math.max(1,Math.min(2000,width)):600;height=Number.isFinite(height)?Math.max(1,Math.min(2000,height)):360;
 const minE=camera.east-width/2/pixels,maxE=camera.east+width/2/pixels,minN=camera.north-height/2/pixels,maxN=camera.north+height/2/pixels;
 const result=[];
 for(let x=Math.floor(minE/HABITAT_SIZE_METERS)-1;x<=Math.ceil(maxE/HABITAT_SIZE_METERS)+1;x++)for(let y=Math.floor(minN/HABITAT_SIZE_METERS)-1;y<=Math.ceil(maxN/HABITAT_SIZE_METERS)+1;y++){
  const candidate=patch(id,x,y),e=candidate.polygon.map(p=>p.east),n=candidate.polygon.map(p=>p.north);
  if(Math.max(...e)<minE||Math.min(...e)>maxE||Math.max(...n)<minN||Math.min(...n)>maxN)continue;result.push(candidate);
 }
 return result;
}
const fmt=n=>String(Math.round(n*100)/100);
const esc=value=>String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const svgPoint=(p,camera,scale)=>({x:300+(p.east-camera.east)*scale,y:180-(p.north-camera.north)*scale});
function motif(kind,color){
 if(kind==='cave')return`<path d="M-12 6-10-5-2-12 10-6 14 5 3 10Z" fill="${color}" stroke="#e4dcf4" stroke-width="1"/><path d="M-2-12 1 0 14 5M1 0-12 6" fill="none" stroke="#e4dcf4" stroke-width="1"/>`;
 if(kind==='riverbank')return`<path d="M-12 5Q-5 1 1 5T13 5M-9 10Q-3 6 4 10M-4 2V-10M2 2V-7" fill="none" stroke="${color}" stroke-width="2"/><path d="M-4-9-8-13M2-5 7-11" fill="none" stroke="${color}" stroke-width="2"/>`;
 if(kind==='ruins')return`<path d="M-11 8V-9H-4V3H10V10H-11Z" fill="${color}" stroke="#f8edc8" stroke-width="1"/><path d="M-11-4H-4M-3 3V10M5 3V10" fill="none" stroke="#f8edc8" stroke-width="1"/>`;
 if(kind==='landmark')return`<path d="M-14 10-3-11 3-2 8-9 17 10Z" fill="${color}" stroke="#f9debf" stroke-width="1"/><path d="M-3-11 1 9M8-9 11 10" fill="none" stroke="#f9debf" stroke-width="1"/>`;
 if(kind==='grove')return`<path d="M-11 0Q-12-10-3-9Q5-15 10-5Q18 0 9 8Q1 16-6 9Q-15 10-11 0Z" fill="${color}" stroke="#c4e6a4" stroke-width="1"/><circle cx="-4" cy="-3" r="2" fill="#eecb73"/><circle cx="5" cy="4" r="2" fill="#eecb73"/>`;
 return`<path d="M-5 7-6-5M2 8 5-8M-5 3-10-1M3 1 9-3" fill="none" stroke="${color}" stroke-width="2"/><circle cx="-6" cy="-6" r="3" fill="#f2da93"/><circle cx="5" cy="-9" r="3" fill="#f6e9c0"/>`;
}
export function habitatMapLayer(landId,{position={east:0,north:0},zoom=1,scale=2.2*zoom}={}){
 const camera=point(position),patches=visibleHabitatPatches(landId,{position:camera,zoom,scale});
 let svg='';
 for(const row of patches){
  const polygon=row.polygon.map(p=>svgPoint(p,camera,scale)),d=polygon.map((p,i)=>`${i?'L':'M'}${fmt(p.x)} ${fmt(p.y)}`).join('')+'Z';
  svg+=`<g class="terrain-habitat" data-habitat="${row.id}" data-habitat-patch="${row.patchId}" data-habitat-size="${row.sizeMeters}"><path d="${d}" fill="${row.color}" fill-opacity=".19" stroke="${row.ink}" stroke-opacity=".75" stroke-width="3" stroke-dasharray="8 8"/>`;
  for(let x=Math.floor((camera.east-160/Math.max(.65,zoom))/70);x<=Math.ceil((camera.east+160/Math.max(.65,zoom))/70);x++)for(let y=Math.floor((camera.north-100/Math.max(.65,zoom))/70);y<=Math.ceil((camera.north+100/Math.max(.65,zoom))/70);y++){
   const p={east:x*70+noise(x,y,seedOf(row.landId)+8)*22,north:y*70+noise(x,y,seedOf(row.landId)+9)*22};
   if(!contains(p,row.polygon))continue;const a=svgPoint(p,camera,scale);
   if(a.x< -30||a.x>630||a.y< -30||a.y>390)continue;
   svg+=`<g class="habitat-ground-mark" transform="translate(${fmt(a.x)} ${fmt(a.y)}) scale(${fmt(scale*.6)})" opacity=".57">${motif(row.id,row.color)}</g>`;
  }
  const a=svgPoint(row.center,camera,scale);
  if(a.x>70&&a.x<530&&a.y>90&&a.y<290&&Math.hypot(a.x-300,a.y-180)>65)svg+=`<text x="${fmt(a.x)}" y="${fmt(a.y)}" text-anchor="middle" font-size="14" font-weight="800" font-family="system-ui,sans-serif" fill="${row.ink}" stroke="${row.color}" stroke-width="3" paint-order="stroke">${esc(row.name)}</text>`;
  svg+='</g>';
 }
 return`<g class="terrain-habitats">${svg}</g>`;
}
export function habitatMapLegend(landId,{position={east:0,north:0}}={}){
 const camera=point(position),here=habitatAtPosition(landId,camera.east,camera.north);
 const neighbors=[patch(here.landId,here.cell.east+1,here.cell.north),patch(here.landId,here.cell.east-1,here.cell.north),patch(here.landId,here.cell.east,here.cell.north+1),patch(here.landId,here.cell.east,here.cell.north-1)];
 const nearest=neighbors.sort((a,b)=>Math.hypot(a.center.east-camera.east,a.center.north-camera.north)-Math.hypot(b.center.east-camera.east,b.center.north-camera.north))[0];
 const e=nearest.center.east-camera.east,n=nearest.center.north-camera.north,direction=Math.abs(e)>Math.abs(n)?e>0?'E':'W':n>0?'N':'S';
 return`<g class="habitat-map-legend" data-current-habitat="${here.id}" data-current-patch="${here.patchId}" aria-label="${esc(here.name)}: ${esc(here.resources)}"><rect x="12" y="12" width="328" height="60" rx="10" fill="#142d29" fill-opacity=".88"/><circle cx="27" cy="31" r="5" fill="${here.color}" stroke="${here.ink}"/><text x="40" y="35" fill="${here.ink}" font-size="14" font-weight="800" font-family="system-ui,sans-serif">${esc(here.name)} · 480 m habitat</text><text x="25" y="55" fill="#e4f0da" font-size="11" font-family="system-ui,sans-serif">${esc(here.resources)}</text><text x="585" y="333" text-anchor="end" fill="#fff8dd" stroke="#243b34" stroke-width="3" paint-order="stroke" font-size="11" font-weight="700" font-family="system-ui,sans-serif">${esc(nearest.shortName)} ${direction} · ~${Math.round(Math.hypot(e,n)/10)*10} m</text></g>`;
}
