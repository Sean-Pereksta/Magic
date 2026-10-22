// A fantasy walking map. Geometry lives in meters; the camera alone moves.
// World features use absolute coordinates, including negative coordinates, so
// returning to a place (or crossing a rendering-cell boundary) preserves it.
export const MAP_METERS_TO_PIXELS = 2.2;
export const MAP_PIXELS_PER_METER = MAP_METERS_TO_PIXELS;
const WIDTH = 600, HEIGHT = 360, CELL = 28, LIMIT = 1e7;
const THEMES = Object.freeze({
  'emerald-jungle': { name:'Emerald Jungle', ground:'#82b85b', patch:'#a1cb70', dark:'#4b854b', leaf:'#347b4e', light:'#69ab57', water:'#47b8b7', bank:'#b7cf78', foam:'#b6f1dc', path:'#e9cd8d', edge:'#b2995d', rock:'#8b9c73', accent:'#f3b18b', surface:'grass', flora:'jungle', river:10 },
  'sunstone-savannah': { name:'Sunstone Savannah', ground:'#d3b85e', patch:'#e8ce7a', dark:'#a09341', leaf:'#708149', light:'#a3aa54', water:'#72b8ad', bank:'#e9da98', foam:'#dcf4cb', path:'#f5dda2', edge:'#b69b5a', rock:'#b99a63', accent:'#f3a358', surface:'dry-grass', flora:'savannah', river:8 },
  'crystal-caverns': { name:'Crystal Caverns', ground:'#58637f', patch:'#73718e', dark:'#3b4764', leaf:'#aaa3ed', light:'#d0c1ff', water:'#469bb8', bank:'#8c9cad', foam:'#b9f5ef', path:'#b9acba', edge:'#84788e', rock:'#7d7c9c', accent:'#a2f2e1', surface:'stone', flora:'crystal', river:10 },
  'whispering-marsh': { name:'Whispering Marsh', ground:'#729f79', patch:'#91b58a', dark:'#4f7f6d', leaf:'#3f7867', light:'#76a77b', water:'#4b9e9c', bank:'#9cba80', foam:'#b2dfbf', path:'#c9bd89', edge:'#8f8b61', rock:'#80998b', accent:'#f0d78a', surface:'bog', flora:'marsh', river:19 },
  'frostpeak-mountains': { name:'Frostpeak Mountains', ground:'#d7eae2', patch:'#eff7e8', dark:'#9fbfbe', leaf:'#4f8790', light:'#9bbeb4', water:'#8ec7d6', bank:'#edf6e7', foam:'#f6fff3', path:'#c9c9b9', edge:'#99acaa', rock:'#a1b8c0', accent:'#9de1f5', surface:'snow', flora:'snow', river:10 },
  'thunder-highlands': { name:'Thunder Highlands', ground:'#89a08e', patch:'#aeb59b', dark:'#647f81', leaf:'#607d79', light:'#97aa8b', water:'#728eaf', bank:'#b7bdaa', foam:'#d8e8ed', path:'#d8c5a2', edge:'#9d967f', rock:'#7e879d', accent:'#e8da84', surface:'highland', flora:'thunder', river:9 },
  'ember-volcano': { name:'Ember Volcano', ground:'#685d67', patch:'#827079', dark:'#473e4f', leaf:'#4b414b', light:'#8d6460', water:'#ef8c49', bank:'#a55d53', foam:'#ffe493', path:'#bc9b86', edge:'#7e665f', rock:'#6b6073', accent:'#ffd374', surface:'basalt', flora:'volcano', river:11 },
  'moonlit-forest': { name:'Moonlit Forest', ground:'#586f79', patch:'#778384', dark:'#3b5369', leaf:'#4c637a', light:'#8793af', water:'#789cc2', bank:'#a3afb6', foam:'#d6e4f8', path:'#bcb5bd', edge:'#7b8194', rock:'#8d93ab', accent:'#9fe7df', surface:'moss', flora:'moon', river:10 },
  'celestial-ruins': { name:'Celestial Ruins', ground:'#a4bfa6', patch:'#ced7b7', dark:'#789d94', leaf:'#8daea4', light:'#c9d9bd', water:'#8ecbd1', bank:'#e7e5c6', foam:'#f4f5de', path:'#e8e0c3', edge:'#b9b599', rock:'#cfceba', accent:'#f4d98c', surface:'garden', flora:'celestial', river:11 },
  'ancient-monkey-kingdom': { name:'Ancient Monkey Kingdom', ground:'#9cac67', patch:'#c1c17d', dark:'#6f9059', leaf:'#477d5c', light:'#86a75e', water:'#6db8ad', bank:'#d0cc8c', foam:'#d0ebc9', path:'#e6c987', edge:'#ab985a', rock:'#b4ad76', accent:'#f4cc6d', surface:'grass', flora:'kingdom', river:10 }
});

const finite = (n, fallback=0) => Number.isFinite(n) ? Math.max(-LIMIT, Math.min(LIMIT,n)) : fallback;
const point = p => ({ east:finite(p?.east), north:finite(p?.north) });
const zoomValue = z => Number.isFinite(z) ? Math.max(.65,Math.min(2,z)) : 1;
const fmt = n => String(Math.round(n*100)/100);
const theme = id => THEMES[id] || THEMES['emerald-jungle'];
const landId = id => Object.hasOwn(THEMES,id) ? id : 'emerald-jungle';
const seedOf = id => [...landId(id)].reduce((a,c)=>Math.imul(a^c.charCodeAt(0),16777619),2166136261) >>> 0;
function noise(x,y,seed=0) {
  let h=Math.imul(x|0,374761393)^Math.imul(y|0,668265263)^seed;
  h=Math.imul(h^(h>>>13),1274126177);return ((h^(h>>>16))>>>0)/4294967296;
}

export function worldToMap(value, position={east:0,north:0}, zoom=1) {
  const p=point(value), camera=point(position), scale=MAP_METERS_TO_PIXELS*zoomValue(zoom);
  return { x:WIDTH/2+(p.east-camera.east)*scale, y:HEIGHT/2-(p.north-camera.north)*scale };
}

// These continuous curves are shared by rendering and surface queries. There
// are no independently generated tiles whose banks or paths can disagree.
const riverNorth = (east,band,seed) => band*420-48+17*Math.sin(east/97+(seed%11)*.12)+8*Math.sin(east/43);
const pathEast = (north,lane) => lane*260+15*Math.sin(north/83)+7*Math.sin(north/151);
const branchNorth = (east,lane) => lane*310+44+13*Math.sin(east/107);
function distances(id,east,north) {
  const seed=seedOf(id), band=Math.round((north+48)/420);
  let river=Infinity, nearestBand=band;
  for(let k=band-1;k<=band+1;k++) {const d=Math.abs(north-riverNorth(east,k,seed));if(d<river){river=d;nearestBand=k;}}
  const lane=Math.round(east/260), branch=Math.round((north-44)/310);
  let path=Infinity, horizontal=Infinity;
  for(let k=lane-1;k<=lane+1;k++)path=Math.min(path,Math.abs(east-pathEast(north,k)));
  for(let k=branch-1;k<=branch+1;k++)horizontal=Math.min(horizontal,Math.abs(north-branchNorth(east,k)));
  return {river,path,horizontal,band:nearestBand};
}

export function terrainAt(id,east,north) {
  id=landId(id);east=finite(east);north=finite(north);
  const p=theme(id),d=distances(id,east,north),onPath=d.path<5.5||d.horizontal<3.5;
  return {land:id,kind:onPath?(d.river<p.river?'bridge':'trail'):(d.river<p.river?(p.flora==='volcano'?'lava':p.flora==='snow'?'ice':'water'):p.surface),riverDistance:d.river,pathDistance:Math.min(d.path,d.horizontal)};
}

const circle = (x,y,r,fill,attrs='') => `<circle cx="${fmt(x)}" cy="${fmt(y)}" r="${fmt(r)}" fill="${fill}" ${attrs}/>`;
const ellipse = (x,y,rx,ry,fill,attrs='') => `<ellipse cx="${fmt(x)}" cy="${fmt(y)}" rx="${fmt(rx)}" ry="${fmt(ry)}" fill="${fill}" ${attrs}/>`;
const shape = (d,fill,attrs='') => `<path d="${d}" fill="${fill}" ${attrs}/>`;
const stroke = (d,color,width=1,attrs='') => shape(d,'none',`stroke="${color}" stroke-width="${fmt(width)}" stroke-linecap="round" stroke-linejoin="round" ${attrs}`);
function curve(points,closed=false) {
  if(!points.length)return '';
  let d=`M${fmt(points[0].x)} ${fmt(points[0].y)}`;
  for(let i=0;i<(closed?points.length:points.length-1);i++) {
    const a=points[(i-1+points.length)%points.length],b=points[i],c=points[(i+1)%points.length],e=points[(i+2)%points.length];
    const prev=!closed&&i===0?b:a,next=!closed&&i===points.length-2?c:e;
    d+=`C${fmt(b.x+(c.x-prev.x)/6)} ${fmt(b.y+(c.y-prev.y)/6)} ${fmt(c.x-(next.x-b.x)/6)} ${fmt(c.y-(next.y-b.y)/6)} ${fmt(c.x)} ${fmt(c.y)}`;
  }
  return d+(closed?'Z':'');
}

// Prop symbols are deliberately drawn from above. Small shadows and facets
// add depth without hiding the walkable ground behind a scenic foreground.
function symbols(p,uid) {
  const outline=`stroke="${p.dark}" stroke-width=".9" stroke-linejoin="round"`;
  const shadow=ellipse(1.8,2.5,13,10,p.dark,'opacity=".2"');
  const crown=shadow+shape('M-13 0Q-17-7-9-10Q-8-17 0-13Q8-18 12-9Q19-6 13 2Q17 11 7 12Q0 18-6 12Q-16 14-13 5Z',p.leaf,outline)+shape('M-9-6Q-5-13 1-10Q7-13 10-6Q13 0 6 4Q-1 10-7 5Q-13 1-9-6Z',p.light)+stroke('M-7-3 0 0 7-4M0 0 1 8',p.leaf,1.2,'opacity=".65"')+ellipse(-4,-5,3,1.6,'#ecedbc','opacity=".35"');
  const palm=shadow+circle(0,0,3,'#a88a50')+Array.from({length:7},(_,i)=>`<g transform="rotate(${i*360/7})">${shape('M0 1Q-11-2-8-15Q-4-11 0-2Q1-11 7-17Q10-6 0 1Z',i%2?p.light:p.leaf,outline)}${stroke('M0 0 0-10',p.dark,.55,'opacity=".6"')}</g>`).join('')+circle(0,0,2.2,'#e5c475');
  const fir=shadow+Array.from({length:3},(_,i)=>`<g transform="rotate(${i*120})">${shape('M0 12-8 5-4 4-10-3-5-2-7-10 0-16 7-10 5-2 10-3 4 4 8 5Z',p.leaf,outline)}${p.flora==='snow'?shape('M0 8-4 3-2 3-5-3-2-2-3-9 0-13 3-9 2-2 5-3 2 3 4 3Z','#eff8e9'):''}</g>`).join('')+circle(0,0,2.5,p.light);
  const crystal=shadow+shape('M-10 5-13-5-7-10-2-4 0-15 7-20 14-7 9 3 2 7Z',p.leaf,outline)+shape('M7-20 6-7 9 3 14-7Z',p.light)+shape('M0-15 6-7 2 7-2-4Z',p.accent,'opacity=".85"')+stroke('M-7-10-5 0-10 5M0-15 6-7 14-7',p.foam,.6,'opacity=".75"');
  const stones=shadow+shape('M-12 5-11-5-3-12 8-10 14-1 11 7 0 10Z',p.rock,outline)+shape('M-3-12 8-10 14-1 1 1Z',p.light,'opacity=".48"')+stroke('M-3-12 1 1 11 7M-11-5 1 1',p.dark,.7,'opacity=".45"');
  const pillar=ellipse(2,3,9,7,p.dark,'opacity=".22"')+shape('M-7 4h14V-13H-7Z',p.rock,outline)+shape('M-8-13 0-17 8-13 0-9Z',p.path,outline)+shape('M0-9 8-13V4H0Z',p.edge,'opacity=".5"')+stroke('M-4-8V0M-1-7V2',p.path,.75);
  const mushroom=shadow+shape('M-3 6-2-4H3L4 6Z',p.path,outline)+shape('M-11-2Q-11-14 0-14Q11-14 12-2Q0 6-11-2Z',p.accent,outline)+circle(-5,-6,2.1,p.foam)+circle(4,-9,2.3,p.foam)+circle(7,-3,1.4,p.foam);
  const crater=ellipse(1,2,14,11,p.dark,'opacity=".22"')+ellipse(0,0,13,10,p.rock,outline)+ellipse(0,-1,9,6,p.dark)+ellipse(0,-1,6.5,4,p.water)+ellipse(-1,-2,3.5,1.8,p.foam,'class="terrain-glow"');
  const grass=stroke('M-5 3Q-5-2-9-5M-1 4Q0-4-2-8M3 3Q4-2 8-4',p.dark,1.2,'opacity=".65"')+stroke('M-1 2Q2-1 3-7',p.light,1.2);
  const fern=Array.from({length:5},(_,i)=>`<g transform="rotate(${i*72})">${shape('M0 0Q-7-4-3-10Q0-9 0-3Q2-10 5-11Q8-3 0 0Z',i%2?p.light:p.leaf)}</g>`).join('');
  const flowers=grass+circle(-4,-4,2.1,p.accent)+circle(3,-7,2.4,p.accent)+circle(7,0,1.7,p.accent)+circle(3,-7,.85,p.path);
  const charge=stones+shape('M1-8-4 0H0L-2 8 6-3H2L5-8Z',p.accent,'class="terrain-glow"');
  let flora=crown,special=fern,detail=flowers;
  if(['kingdom','jungle'].includes(p.flora)){flora=p.flora==='kingdom'?palm:crown;special=p.flora==='kingdom'?pillar:palm;}
  if(p.flora==='savannah'){flora=`<g transform="scale(1.18 .8)">${crown}</g>`;special=stones;detail=grass+stroke('M0 1V-8M-4-6 0-3 5-6',p.edge,1.2);}
  if(p.flora==='crystal'){flora=crystal;special=`<g transform="scale(.75) rotate(-25)">${crystal}</g>`;detail=mushroom;}
  if(p.flora==='marsh'){flora=crown;special=fern;detail=grass+stroke('M-2 3V-10M3 3V-8',p.leaf,1.1)+stroke('M-2-10V-14M3-8V-12',p.accent,2.2);}
  if(p.flora==='snow'){flora=fir;special=stones+shape('M-11-5-3-12 8-10 13-2 1 1Z','#f1f9ee');detail=grass;}
  if(p.flora==='thunder'){flora=fir;special=charge;detail=flowers;}
  if(p.flora==='volcano'){flora=crater;special=stones;detail=stroke('M-6-3-1 0-4 3 2 4 6 8',p.water,1.3)+circle(4,-5,1.4,p.accent,'class="terrain-glow"');}
  if(p.flora==='moon'){flora=crown;special=mushroom;detail=flowers+circle(-6,2,1.1,p.accent,'class="terrain-glow"');}
  if(p.flora==='celestial'){flora=`<g transform="scale(.85)">${crown}</g>`;special=pillar;detail=flowers;}
  return `<defs><g id="${uid}-flora">${flora}</g><g id="${uid}-stone">${stones}</g><g id="${uid}-special">${special}</g><g id="${uid}-detail">${detail}</g></defs>`;
}

function landmark(p) {
  const ink=`stroke="${p.dark}" stroke-width="1.1" stroke-linejoin="round"`;
  const base=ellipse(2,8,23,14,p.dark,'opacity=".2"');
  const steps=shape('M-23 12H23V5H18V-1H13V-7H-13V-1H-18V5H-23Z',p.rock,ink)+stroke('M-18 5H18M-13-1H13',p.path,1);
  const shrine=base+steps+shape('M-12-7V-25H12V-7Z',p.path,ink)+shape('M-17-25 0-36 17-25Z',p.accent,ink)+shape('M-5-7V-18Q0-23 5-18V-7Z',p.dark)+circle(0,-27,3,p.accent)+stroke('M-10-21V-10M10-21V-10',p.edge,1.1);
  if(['jungle','kingdom'].includes(p.flora))return shrine+(p.flora==='kingdom'?shape('M-9-33-12-41-4-38 0-45 5-38 13-41 9-33Z',p.accent,ink):stroke('M-14-17Q-26-19-19 3M15-14Q25-6 17 10',p.leaf,2.3));
  if(p.flora==='savannah')return base+shape('M-23 10-19-23-9-32H12L23-22 26 10H13L10-16Q0-24-8-15L-11 10Z',p.rock,ink)+shape('M-19-23-9-32H12L23-22 10-21-8-22Z',p.path)+stroke('M-15-17-16-2M17-13 19 2',p.edge,1.5);
  if(p.flora==='crystal')return base+ellipse(0,4,24,13,p.water)+shape('M-15 5-18-15-10-25-2-14 3-35 14-41 25-19 17 7 5 13Z',p.leaf,ink)+shape('M14-41 12-16 17 7 25-19Z',p.light)+shape('M3-35 12-16 5 13-2-14Z',p.accent)+stroke('M3-35 12-16 25-19',p.foam,1.1);
  if(p.flora==='marsh'||p.flora==='moon')return base+ellipse(0,0,25,19,p.bank,ink)+ellipse(0,-1,19,13,p.water)+ellipse(0,-1,12,8,p.foam,'opacity=".4"')+[-17,-7,8,20].map((x,i)=>circle(x,i%2?11:-10,2,p.accent,'class="terrain-glow"')).join('')+shape('M-12 1-5-4-1 1-6 6Z',p.leaf)+circle(-6,0,2.2,p.accent);
  if(p.flora==='snow')return base+shape('M-28 10-13-23-2-12 10-39 31 10Z',p.rock,ink)+shape('M10-39 0-16 8-19 13-12 17-18 23-8Z','#f3fbec')+shape('M-13-23-21-6-13-9-7-4-2-12Z','#f3fbec')+stroke('M10-39 10 6',p.dark,.8,'opacity=".3"');
  if(p.flora==='volcano')return base+ellipse(0,0,29,23,p.dark,ink)+ellipse(0,-3,23,17,p.rock)+ellipse(0,-3,17,12,p.water)+ellipse(-1,-5,11,7,p.foam,'class="terrain-glow"')+stroke('M14 8 19 16 16 25',p.water,4);
  if(p.flora==='thunder')return base+shape('M-15 9-12-25 0-34 11-24 16 9Z',p.rock,ink)+shape('M1-26-6-9H0L-4 4 8-14H2L7-26Z',p.accent,'class="terrain-glow"')+stroke('M-23 13H24',p.path,3);
  return base+ellipse(0,4,24,17,p.path,ink)+ellipse(0,4,17,11,p.bank)+[-18,18].map(x=>shape(`M${x-4} 1V-25H${x+4}V1Z`,p.rock,ink)+shape(`M${x-6}-25H${x+6}V-30H${x-6}Z`,p.path,ink)).join('')+shape('M-9-27 0-40 9-27 0-15Z',p.accent,ink)+stroke('M-17 5 0 13 17 5',p.accent,1.5);
}

export function terrainMap(id,{position={east:0,north:0},trail=[],uid='terrain',zoom=1}={}) {
  id=landId(id);const p=theme(id),seed=seedOf(id),camera=point(position),scale=MAP_METERS_TO_PIXELS*zoomValue(zoom);
  uid=String(uid).replace(/[^a-zA-Z0-9_-]/g,'').slice(0,64)||'terrain';
  const at=(east,north)=>worldToMap({east,north},camera,zoom);
  const minE=camera.east-WIDTH/2/scale,maxE=camera.east+WIDTH/2/scale,minN=camera.north-HEIGHT/2/scale,maxN=camera.north+HEIGHT/2/scale;
  const inside=(e,n,margin=25)=>e>minE-margin&&e<maxE+margin&&n>minN-margin&&n<maxN+margin;
  const toPoints=world=>world.map(([e,n])=>at(e,n));
  const track=(world,color,width,attrs='')=>stroke(curve(toPoints(world)),color,width*scale,attrs);
  let ground='',water='',paths='',bridges='',props='',details='',landmarks='',ridges='';let propCount=0;

  for(let x=Math.floor((minE-60)/72);x<=Math.ceil((maxE+60)/72);x++)for(let y=Math.floor((minN-60)/72);y<=Math.ceil((maxN+60)/72);y++) {
    const e=x*72+(noise(x,y,seed)-.5)*30,n=y*72+(noise(x,y,seed+1)-.5)*30;
    const blob=Array.from({length:8},(_,i)=>{const angle=i*Math.PI/4,r=28+noise(x*9+i,y,seed+2)*22;return at(e+Math.cos(angle)*r,n+Math.sin(angle)*r*.72);});
    ground+=shape(curve(blob,true),noise(x,y,seed+3)>.43?p.patch:p.dark,`opacity="${noise(x,y,seed+4)>.5?'.22':'.11'}"`);
    if(['snow','thunder','volcano','crystal'].includes(p.flora)&&noise(x,y,seed+5)>.43) {
      const ridge=Array.from({length:7},(_,i)=>[e-22+i*7,n+Math.sin(i*1.3+noise(x,y,seed))*5]);
      ridges+=track(ridge,p.dark,4,'opacity=".2"')+track(ridge,p.flora==='snow'?'#f4f9ea':p.patch,1.1,'opacity=".65"');
    }
  }

  const minBand=Math.floor((minN-40+48)/420),maxBand=Math.ceil((maxN+40+48)/420);
  const startE=Math.floor((minE-60)/14)*14,endE=Math.ceil((maxE+60)/14)*14;
  for(let band=minBand;band<=maxBand;band++) {
    const center=[];for(let e=startE;e<=endE;e+=14)center.push([e,riverNorth(e,band,seed)]);
    const bank=half=>toPoints(center.map(([e,n])=>[e,n+half]).concat([...center].reverse().map(([e,n])=>[e,n-half])));
    water+=shape(curve(bank(p.river+2.8),true),p.bank)+shape(curve(bank(p.river),true),p.water,'class="terrain-water"');
    water+=track(center,p.foam,p.flora==='volcano'?4:.6,`opacity="${p.flora==='volcano'?'.7':'.28'}" class="${p.flora==='volcano'?'terrain-glow':'terrain-current'}"`);
    for(let x=Math.floor((minE-20)/28);x<=Math.ceil((maxE+20)/28);x++) {
      const e=x*28+noise(x,band,seed+8)*11,n=riverNorth(e,band,seed)+(noise(x,band,seed+9)-.5)*p.river;
      if(!inside(e,n,10))continue;const a=at(e,n),s=scale;
      water+=stroke(`M${fmt(a.x-5*s)} ${fmt(a.y)}q${fmt(4*s)} ${fmt(2*s)} ${fmt(10*s)} 0`,p.foam,.8*s,'opacity=".58" class="terrain-water"');
      if(p.flora==='marsh')water+=ellipse(a.x+4*s,a.y-4*s,4.2*s,2.7*s,p.leaf)+circle(a.x+5*s,a.y-5*s,1.6*s,p.accent);
      if(p.flora==='snow')water+=stroke(`M${fmt(a.x)} ${fmt(a.y-3*s)}l${fmt(4*s)} ${fmt(3*s)} ${fmt(-1*s)} ${fmt(5*s)}`,p.foam,.7*s,'opacity=".7"');
    }
  }

  const minLane=Math.floor((minE-35)/260),maxLane=Math.ceil((maxE+35)/260);
  const minBranch=Math.floor((minN-65)/310),maxBranch=Math.ceil((maxN+65)/310);
  function addPath(world,width) {paths+=track(world,p.dark,width+2,'opacity=".18"')+track(world,p.edge,width+.8)+track(world,p.path,width)+track(world,'#fff5d0',.55,'opacity=".5"');}
  for(let lane=minLane;lane<=maxLane;lane++) {
    const route=[];for(let n=Math.floor((minN-45)/16)*16;n<=maxN+45;n+=16)route.push([pathEast(n,lane),n]);addPath(route,11);
    for(let band=minBand;band<=maxBand;band++) {
      let n=band*420-48;for(let i=0;i<10;i++)n=riverNorth(pathEast(n,lane),band,seed);
      const e=pathEast(n,lane);if(!inside(e,n,40))continue;
      const a=at(e,n),angle=Math.atan((pathEast(n+.2,lane)-pathEast(n-.2,lane))/.4)*180/Math.PI,length=p.river+5.5;
      let boards='';for(let y=-length+2;y<length;y+=3.2)boards+=stroke(`M-6.6 ${fmt(y)}H6.6`,'#8e7654',.65);
      bridges+=`<g class="terrain-bridge" transform="translate(${fmt(a.x)} ${fmt(a.y)}) rotate(${fmt(angle)}) scale(${fmt(scale)})">${shape(`M-8 ${fmt(-length)}H8V${fmt(length)}H-8Z`,p.dark,'opacity=".22" transform="translate(1.5 1.5)"')}${shape(`M-7.2 ${fmt(-length)}H7.2V${fmt(length)}H-7.2Z`,'#d9b77d','stroke="#8d7955" stroke-width=".9"')}${boards}${stroke(`M-7.5 ${fmt(-length)}V${fmt(length)}M7.5 ${fmt(-length)}V${fmt(length)}`,'#f1d399',1.8)}${[-length,length].map(y=>circle(-7.5,y,1.5,'#887652')+circle(7.5,y,1.5,'#887652')).join('')}</g>`;
    }
  }
  for(let lane=minBranch;lane<=maxBranch;lane++) {
    const route=[];for(let e=startE;e<=endE;e+=14)route.push([e,branchNorth(e,lane)]);addPath(route,7);
    // A side path can cross a river again far from the origin. Planks follow
    // its actual curve, including long crossings where the banks run beside it.
    for(let k=Math.floor((minE-12)/3.2);k<=Math.ceil((maxE+12)/3.2);k++) {
      const e=k*3.2,n=branchNorth(e,lane);if(!inside(e,n,12)||distances(id,e,n).river>p.river+2)continue;
      const a=at(e,n),angle=-Math.atan((branchNorth(e+.2,lane)-branchNorth(e-.2,lane))/.4)*180/Math.PI;
      bridges+=`<g class="terrain-side-bridge" transform="translate(${fmt(a.x)} ${fmt(a.y)}) rotate(${fmt(angle)}) scale(${fmt(scale)})">${shape('M-1.65-4.2H1.65V4.2H-1.65Z','#d9b77d','stroke="#8e7654" stroke-width=".4"')}${stroke('M-1.8-4.2H1.8M-1.8 4.2H1.8','#f1d399',.9)}</g>`;
    }
  }

  const landmarkPoints=[];
  for(let x=Math.floor((minE-70)/540);x<=Math.ceil((maxE+70)/540);x++)for(let y=Math.floor((minN-70)/540);y<=Math.ceil((maxN+70)/540);y++) {
    const e=x*540+62,n=y*540+36;if(!inside(e,n,50))continue;landmarkPoints.push({east:e,north:n});
    const a=at(e,n);landmarks+=`<g class="terrain-landmark" data-world-east="${fmt(e)}" data-world-north="${fmt(n)}" transform="translate(${fmt(a.x)} ${fmt(a.y)}) scale(${fmt(scale*.8)})">${landmark(p)}</g>`;
  }

  for(let x=Math.floor((minE-24)/CELL);x<=Math.ceil((maxE+24)/CELL);x++)for(let y=Math.floor((minN-24)/CELL);y<=Math.ceil((maxN+24)/CELL);y++) {
    const e=x*CELL+(noise(x,y,seed+13)-.5)*15,n=y*CELL+(noise(x,y,seed+14)-.5)*15;
    if(!inside(e,n,24)||Math.hypot(e,n)<15||landmarkPoints.some(v=>Math.hypot(e-v.east,n-v.north)<25))continue;
    const d=distances(id,e,n),choice=noise(x,y,seed+15),isSmall=choice>.77,clearance=isSmall?9:17;
    if(d.river<p.river+(isSmall?3:12)||d.path<clearance||d.horizontal<clearance-2)continue;
    const kind=choice<.55?'flora':choice<.7?'special':choice<.81?'stone':'detail';
    const a=at(e,n),size=(isSmall?.65:.8)+noise(x,y,seed+16)*.35,rotation=(noise(x,y,seed+17)-.5)*(kind==='detail'?120:35);
    props+=`<use href="#${uid}-${kind}" data-cell="${x}:${y}" data-world-east="${fmt(e)}" data-world-north="${fmt(n)}" transform="translate(${fmt(a.x)} ${fmt(a.y)}) rotate(${fmt(rotation)}) scale(${fmt(scale*size)})"/>`;propCount++;
  }

  // Tiny tufts and speckles are also world-anchored, never screen decoration.
  for(let x=Math.floor(minE/22);x<=Math.ceil(maxE/22);x++)for(let y=Math.floor(minN/22);y<=Math.ceil(maxN/22);y++) {
    const e=x*22+noise(x,y,seed+22)*14,n=y*22+noise(x,y,seed+23)*14,d=distances(id,e,n);
    if(d.river<p.river+2||d.path<7||d.horizontal<5)continue;const a=at(e,n);
    details+=stroke(`M${fmt(a.x-2*scale)} ${fmt(a.y)}l${fmt(scale)} ${fmt(-2*scale)}m${fmt(2*scale)} ${fmt(2*scale)} ${fmt(scale)} ${fmt(-3*scale)}`,p.dark,.7*scale,'opacity=".27"')+circle(a.x+4*scale,a.y+2*scale,.55*scale,p.patch,'opacity=".6"');
  }

  const safeTrail=Array.isArray(trail)?trail.slice(-128).filter(v=>Number.isFinite(v?.east)&&Number.isFinite(v?.north)).map(point):[];
  const last=safeTrail.at(-1);
  if(last&&(last.east!==camera.east||last.north!==camera.north))safeTrail.push(camera);
  let route='';
  if(safeTrail.length>1) {
    const d=safeTrail.map((v,i)=>{const a=at(v.east,v.north);return `${i?'L':'M'}${fmt(a.x)} ${fmt(a.y)}`;}).join('');
    route=stroke(d,'#fff9d9',4.2,'opacity=".8"')+stroke(d,'#db913e',2.2,'stroke-dasharray="2 5" class="terrain-traveled"');
  }
  if(inside(-19,-6,12)) {
    const a=at(-19,-6);landmarks+=`<g class="terrain-start" transform="translate(${fmt(a.x)} ${fmt(a.y)}) scale(${fmt(scale)})">${ellipse(0,4,8,3,p.dark,'opacity=".18"')}${stroke('M0 4V-13','#816a49',1.7)}${shape('M1-12H12L9-8 12-4H1Z',p.accent,'stroke="#846c48" stroke-width=".65"')}${circle(0,-14,1.2,p.path)}</g>`;
  }
  const bar=20*scale;
  return `<svg class="terrain-map" viewBox="0 0 ${WIDTH} ${HEIGHT}" role="img" aria-label="${p.name} walking map" data-land="${id}" data-map-east="${camera.east}" data-map-north="${camera.north}" data-map-scale="${scale}" data-prop-count="${propCount}" xmlns="http://www.w3.org/2000/svg"><title>${p.name}: explore in every direction</title><desc>A north-up fantasy terrain map. Your position stays at the center while trails, terrain and landmarks follow your actual walking direction.</desc>${symbols(p,uid)}<rect width="${WIDTH}" height="${HEIGHT}" fill="${p.ground}"/><g class="terrain-camera"><g class="terrain-ground">${ground}${ridges}${details}</g><g class="terrain-rivers">${water}</g><g class="terrain-paths">${paths}</g>${bridges}<g class="terrain-props">${props}</g><g class="terrain-landmarks">${landmarks}</g><g class="terrain-route">${route}</g></g><g class="terrain-scale" transform="translate(16 323)"><rect x="-7" y="-5" width="${fmt(bar+15)}" height="30" rx="7" fill="${p.dark}" opacity=".72"/>${stroke(`M0 5V12H${fmt(bar)}V5`,'#fff5da',1.7)}<text x="${fmt(bar/2)}" y="1" text-anchor="middle" fill="#fff5da" font-size="11" font-weight="700" font-family="system-ui,sans-serif">20 m</text></g></svg>`;
}
