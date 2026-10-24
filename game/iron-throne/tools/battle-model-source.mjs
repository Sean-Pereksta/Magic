// Original low-poly artwork, authored for Iron Thrones. CC0. No external runtime.
// Meshes are contour-built surfaces, with articulated hierarchies and reusable clips.
import { CAMPAIGN_HOUSES, UNITS } from '../data.mjs';

export function buildBattlePack() {
  const meshes = {}, models = {};
  const mesh = (name, vertices, faces) => {
    const positions=[], normals=[];
    for (const face of faces) for(let i=1;i<face.length-1;i++) {
      const a=vertices[face[0]], b=vertices[face[i]], c=vertices[face[i+1]];
      const u=b.map((v,k)=>v-a[k]),v=c.map((n,k)=>n-a[k]);
      const n=[u[1]*v[2]-u[2]*v[1],u[2]*v[0]-u[0]*v[2],u[0]*v[1]-u[1]*v[0]],d=Math.hypot(...n)||1;
      for(const p of [a,b,c]) { positions.push(...p.map(n=>+n.toFixed(4)));normals.push(...n.map(x=>+(x/d).toFixed(4))); }
    }
    meshes[name]={positions,normals};return name;
  };
  const rings = (name, levels, segments=8) => {
    const vs=[], fs=[];
    for(const [y,rx,rz,cx=0,cz=0] of levels) for(let i=0;i<segments;i++) {
      const a=i*2*Math.PI/segments;vs.push([cx+Math.cos(a)*rx,y,cz+Math.sin(a)*rz]);
    }
    for(let r=1;r<levels.length;r++)for(let i=0;i<segments;i++) {
      const n=(i+1)%segments;fs.push([(r-1)*segments+i,r*segments+i,r*segments+n,(r-1)*segments+n]);
    }
    fs.push(Array.from({length:segments},(_,i)=>i));
    fs.push(Array.from({length:segments},(_,i)=>(levels.length-1)*segments+segments-i-1));
    return mesh(name,vs,fs);
  };
  rings('torso',[[0,.18,.13],[.12,.27,.17],[.4,.32,.18],[.52,.24,.16]],8);
  rings('tunic',[[0,.31,.22],[.26,.22,.16],[.39,.2,.15]],8);
  rings('head',[[-.21,.08,.09],[-.13,.14,.12],[.05,.15,.13],[.2,.1,.11],[.23,.04,.05]],8);
  rings('helm',[[-.05,.17,.15],[.15,.18,.15],[.25,.12,.12],[.29,.025,.025]],8);
  rings('nose',[[0,.027,.025],[.12,.02,.06]],4);
  rings('limb',[[0,.105,.1],[-.12,.12,.1],[-.37,.075,.07]],7);
  rings('forearm',[[0,.085,.08],[-.23,.07,.06],[-.33,.055,.055]],7);
  rings('thigh',[[0,.14,.14],[-.25,.13,.12],[-.46,.085,.08]],7);
  rings('shin',[[0,.085,.09],[-.26,.085,.07],[-.43,.055,.06]],7);
  rings('boot',[[-.02,.09,.18,0,.06],[.12,.09,.18,0,.06],[.23,.07,.09]],6);
  rings('hand',[[0,.055,.055],[-.13,.055,.07]],6);
  rings('shaft',[[0,.025,.025],[1,.025,.025]],6);
  rings('beam',[[0,.1,.1],[1,.1,.1]],4);
  rings('wheel',[[-.065,.38,.38],[.065,.38,.38]],12);
  rings('boss',[[0,.075,.075],[.07,.065,.065],[.1,.01,.01]],8);
  rings('stone',[[-.2,.08,.09],[-.12,.2,.18],[.14,.21,.17],[.24,.03,.02]],7);
  rings('foliage',[[0,.1,.1],[.55,1.4,1.3],[1.4,1.6,1.2],[2.5,.85,.8],[3.4,.05,.05]],7);
  rings('pine',[[0,1.4,1.4],[2,.05,.05],[1,1.0,1],[3,.03,.03],[2,.6,.6],[4,.01,.01]],7);
  rings('horseBody',[[-.42,.12,.24],[-.22,.35,.7],[.1,.39,.85],[.35,.28,.72],[.43,.13,.55]],10);
  rings('horseNeck',[[0,.23,.26],[.45,.24,.27,0,.12],[.85,.16,.22,0,.26]],8);
  rings('horseHead',[[-.15,.15,.34],[.1,.2,.32],[.3,.13,.2,0,-.12]],8);
  rings('horseLeg',[[0,.1,.12],[-.4,.07,.065],[-.72,.04,.045]],6);
  rings('hoof',[[0,.075,.1],[.14,.06,.07]],6);
  mesh('blade',[[-.035,0,0],[.035,0,0],[-.045,.75,0],[.045,.75,0],[0,.94,0],[0,0,.018],[0,.75,.025]],[[0,2,4,6,5],[1,5,6,4,3],[0,5,1],[2,3,4],[0,1,3,2]]);
  mesh('axe',[[0,0,0],[.3,.14,0],[.33,.44,0],[0,.35,0],[0,0,.07],[.3,.14,.07],[.33,.44,.07],[0,.35,.07]],[[0,1,2,3],[7,6,5,4],[0,4,5,1],[1,5,6,2],[2,6,7,3]]);
  mesh('shield',[[-.28,.4,0],[.28,.4,0],[.32,.12,.03],[.2,-.3,.05],[0,-.49,.08],[-.2,-.3,.05],[-.32,.12,.03],[0,.05,.1]],[[0,1,7],[1,2,7],[2,3,7],[3,4,7],[4,5,7],[5,6,7],[6,0,7]]);
  mesh('cape',[[-.24,0,0],[.24,0,0],[.36,-.8,-.13],[0,-.89,-.2],[-.36,-.8,-.13]],[[0,1,3],[1,2,3],[0,3,4]]);
  mesh('banner',[[0,0,0],[.85,0,.04],[.86,-.5,.1],[.45,-.42,.04],[0,-.5,0]],[[0,1,3],[1,2,3],[0,3,4]]);
  mesh('roof',[[-1,0,-1],[1,0,-1],[1,0,1],[-1,0,1],[0,.9,-1],[0,.9,1]],[[0,4,5,3],[4,1,2,5],[0,1,4],[3,5,2]]);
  mesh('box',[[-.5,0,-.5],[.5,0,-.5],[.5,1,-.5],[-.5,1,-.5],[-.5,0,.5],[.5,0,.5],[.5,1,.5],[-.5,1,.5]],[[0,3,2,1],[4,5,6,7],[0,4,7,3],[1,2,6,5],[3,7,6,2],[0,1,5,4]]);
  const bowVs=[],bowFs=[];
  for(let i=0;i<9;i++){const a=-1.2+i*.3;bowVs.push([Math.cos(a)*.21-.15,Math.sin(a)*.65,-.025],[Math.cos(a)*.21-.15,Math.sin(a)*.65,.025]);if(i)bowFs.push([i*2-2,i*2,i*2+1,i*2-1]);}
  mesh('bow',bowVs,bowFs);
  const makeNode=(name,geometry,material,position=[0,0,0],rotation=[0,0,0],scale=[1,1,1],parent=null)=>({name,mesh:geometry,material,position,rotation,scale,parent});
  function human(type,mounted=false) {
    const heavy=['heavyInfantry','knight','cavalry','crossbow'].includes(type),mail=heavy||type==='menAtArms',veteran=type==='veteranArcher';
    const n=[],add=(...args)=>n.push(makeNode(...args));
    add('rider',null,null,[0,mounted?1.02:0,0]);
    add('torso','torso',mail?'steel':'house',[0,1.03,0],[0,0,0],[1,1,1],'rider');
    add('tabard','tunic','house',[0,.86,0],[0,0,0],[1,1,1],'rider');
    add('head','head','skin',[0,1.78,0],[0,0,0],[1,1,1],'rider');
    add('helmet','helm',type==='levy'?'leather':mail?'steel':'darkSteel',[0,.02,0],[0,0,0],[1,1,1],'head');
    add('nose','nose','skin',[0,-.06,.13],[0,0,0],[1,1,1],'head');
    for(const s of [-1,1])add('eye'+s,'beam','darkSteel',[s*.065,.015,.135],[0,0,Math.PI/2],[.1,.05,.1],'head');
    if(heavy)add('visor','beam','darkSteel',[0,-.07,.155],[0,0,Math.PI/2],[.22,1.15,.15],'head');
    if(veteran||type==='knight')add('plume','cape','accent',[0,.28,0],[.3,0,0],[.2,.4,.4],'head');
    if(heavy||veteran)add('cape','cape','house',[0,1.53,-.15],[0,0,0],[1,1,1],'rider');
    for(const s of [-1,1]) {
      const suffix=s<0?'L':'R';
      add(`arm${suffix}`,'limb',mail?'steel':'house',[s*.34,1.48,0],[0,0,s*.12],[1,1,1],'rider');
      add(`forearm${suffix}`,'forearm',mail?'steel':'leather',[0,-.36,0],[-.12,0,0],[1,1,1],`arm${suffix}`);
      add(`hand${suffix}`,'hand','skin',[0,-.3,0],[0,0,0],[1,1,1],`forearm${suffix}`);
      add(`thigh${suffix}`,'thigh',heavy?'steel':'cloth',[s*.15,.96,0],mounted?[-.9,0,s*.5]:[0,0,0],[1,1,1],'rider');
      add(`shin${suffix}`,'shin',heavy?'steel':'leather',[0,-.44,0],mounted?[1.2,0,0]:[0,0,0],[1,1,1],`thigh${suffix}`);
      add(`boot${suffix}`,'boot','darkSteel',[0,-.42,0],[0,0,0],[1,1,1],`shin${suffix}`);
    }
    let weapon=['archer','veteranArcher'].includes(type)?'bow':type==='crossbow'?'crossbow':['spearman','cavalry','knight'].includes(type)?'spear':type==='heavyInfantry'?'mace':type==='levy'?'axe':'sword';
    add('weapon',null,null,[0,-.1,0],[-1.4,0,0],[1,1,1],'handR');
    if(weapon==='bow'||weapon==='crossbow'){
      add('bow','bow','wood',[0,0,.1],weapon==='crossbow'?[0,0,Math.PI/2]:[0,0,0],[1,weapon==='crossbow'?.7:1,1],'weapon');
      add('bowstring','shaft','ivory',[-.075,-.61,.1],[0,0,0],[.23,1.22,.23],'weapon');
      if(weapon==='crossbow')add('stock','beam','wood',[0,-.5,0],[0,0,0],[.45,.8,.45],'weapon');
      add('quiver','limb','leather',[.13,1.5,-.25],[-.2,0,-.2],[1.2,1.2,1.2],'rider');
      for(let i=0;i<3;i++)add('arrow'+i,'shaft','wood',[.09+i*.045,1.1,-.25],[0,0,.2],[.5,.65,.5],'rider');
    } else {
      add('grip','shaft','wood',[0,-.13,0],[0,0,0],[1,weapon==='spear'?2.5:.36,1],'weapon');
      if(weapon==='sword'){
        add('blade','blade','silver',[0,.22,0],[0,0,0],[1,1,1],'weapon');
        add('hilt','beam','gold',[-.13,.22,0],[0,0,-Math.PI/2],[.2,.26,.2],'weapon');
      } else if(weapon==='spear')add('point','blade','silver',[0,2.3,0],[0,0,0],[1,.5,1],'weapon');
      else if(weapon==='axe')add('axe','axe','silver',[0,.2,0],[0,0,0],[.85,.85,.85],'weapon');
      else add('mace','stone','steel',[0,.48,0],[0,0,0],[.7,.7,.7],'weapon');
      if(type!=='scout'){
        add('shield','shield','house',[0,-.05,.08],[0,-.35,0],[heavy?1.15:.95,heavy?1.15:.95,1],'handL');
        add('emblem','heraldry','ivory',[0,.03,.13],[0,0,0],[.22,.22,.22],'shield');
      }
    }
    return n;
  }
  function horse(armored) {
    const n=[],add=(...args)=>n.push(makeNode(...args));
    add('horseBody','horseBody',armored?'darkSteel':'horse',[0,1.22,0]);
    add('horseNeck','horseNeck','horse',[0,1.4,.55]);
    add('horseHead','horseHead','horse',[0,.83,.35],[.3,0,0],[1,1,1],'horseNeck');
    for(const s of [-1,1])add('ear'+s,'blade','horse',[s*.1,.2,-.13],[0,0,s*-.2],[1,.22,1],'horseHead');
    add('mane','cape','darkSteel',[0,2.1,.55],[0,Math.PI/2,0],[.12,.8,1]);
    add('tail','limb','darkSteel',[0,1.47,-.78],[1,0,0],[1,2,1]);
    add('saddle','tunic','leather',[0,1.58,-.05],[0,0,0],[1.6,.6,2]);
    if(armored){add('horseCloth','cape','house',[.38,1.5,-.1],[0,Math.PI/2,0],[1.9,1,1]);add('horseCloth2','cape','house',[-.38,1.5,-.1],[0,-Math.PI/2,0],[1.9,1,1]);}
    for(const x of [-1,1])for(const z of [-1,1]){
      const name='horseLeg'+(z>0?'F':'B')+(x>0?'R':'L');
      add(name,'horseLeg','horse',[x*.26,1.15,z*.57]);
      add(name+'lower','horseLeg','horse',[0,-.65,0],[0,0,0],[.7,.55,.7],name);
      add(name+'hoof','hoof','darkSteel',[0,-.99,.02],[0,0,0],[1,1,1],name);
    }
    return n;
  }
  function engine(type) {
    const n=[],add=(...args)=>n.push(makeNode(...args));
    add('engine',null,null);
    for(const s of [-1,1]){
      add('rail'+s,'beam','wood',[s*.9,.5,-1.4],[Math.PI/2,0,0],[1,2.8,1],'engine');
      for(const z of [-1,1])add('wheel'+s+z,'wheel','wood',[s*1.05,.4,z], [0,0,Math.PI/2],[1,1,1],'engine');
      add('support'+s,'beam','wood',[s*.9,.6,0],[0,0,s*.25],[1.5,type==='trebuchet'?3.2:1.5,1.5],'engine');
    }
    for(const z of [-1,1])add('axle'+z,'beam','darkSteel',[-1.15,.4,z],[0,0,-Math.PI/2],[.5,2.3,.5],'engine');
    if(type==='ram'){
      add('roof','roof','wood',[0,2.2,0],[0,0,0],[1.4,.6,1.8],'engine');
      add('beam','shaft','wood',[0,1.4,-1.5],[Math.PI/2,0,0],[8,3.6,8],'engine');
      add('ramHead','stone','steel',[0,1.4,2.2],[0,0,0],[1.4,1.4,1.4],'engine');
    } else {
      add('beam','beam','wood',[0,type==='trebuchet'?3.5:1.8,0],[-.7,0,0],[1.3,type==='trebuchet'?3.4:2.5,1.3],'engine');
      add('cup','shield','wood',[0,1,.02],[Math.PI/2,0,0],[1.15,.45,1.15],'beam');
      if(type==='trebuchet')add('counterweight','stone','stone',[0,-.3,0],[0,0,0],[2.3,.65,1.5],'beam');
      else add('torsion','wheel','ivory',[0,1.8,0],[0,0,Math.PI/2],[.8,1.5,1.5],'engine');
    }
    for(const s of [-1,1])for(const node of human('levy'))n.push({...node,name:`crew${s}_${node.name}`,parent:node.parent?`crew${s}_${node.parent}`:'engine',position:node.parent?node.position:[s*1.55,0,-.6]});
    return n;
  }
  for(const [id,u] of Object.entries(UNITS))models[id]={name:u.name,family:u.family,nodes:u.family==='siege'?engine(id==='siege'?'catapult':id):[...(u.family==='mounted'?horse(id==='knight'):[]),...human(id,u.family==='mounted')],clips:['idle','walk','run','attack','block','shoot','reload','death',...(u.family==='mounted'?['gallop']:u.family==='siege'?['setup','fire']:[])]};
  models.horse={name:'Horse',family:'mount',nodes:horse(false),clips:['idle','walk','gallop','death']};
  // Each emblem is an authored silhouette, placed on shields and standards.
  const emblemPoints={
    ashen:[[-1,-.5],[1,-.5],[1,.5],[.5,.1],[0,.9],[-.5,.1],[-1,.5]],
    wintermere:[[-.12,-1],[.12,-1],[.12,-.3],[.8,-.7],[.95,-.45],[.3,0],[.95,.45],[.8,.7],[.12,.3],[.12,1],[-.12,1],[-.12,.3],[-.8,.7],[-.95,.45],[-.3,0],[-.95,-.45],[-.8,-.7],[-.12,-.3]],
    thornwall:[[-.8,-1],[.8,-1],[.8,.3],[1,.3],[1,1],[.6,1],[.6,.6],[.2,.6],[.2,1],[-.2,1],[-.2,.6],[-.6,.6],[-.6,1],[-1,1],[-1,.3],[-.8,.3]],
    sunspire:Array.from({length:16},(_,i)=>[Math.cos(i*Math.PI/8)*(i%2?.55:1),Math.sin(i*Math.PI/8)*(i%2?.55:1)]),
    vesper:[[-.4,-.9],[.4,-.8],[.9,-.3],[.4,-.4],[0,-.1],[-.1,.4],[.2,.9],[-.5,.7],[-.9,.2],[-.8,-.5]],
    redharbor:[[-.1,-1],[.1,-1],[.1,.2],[.9,.3],[.7,.8],[.1,.8],[.1,1],[-.1,1]],
    stormholt:[[.1,1],[-.7,-.1],[-.1,-.1],[-.3,-1],[.8,.2],[.2,.2]],
    goldmere:[[0,1],[.8,0],[0,-1],[-.8,0]],
    ravenfell:[[-1,.7],[-.4,.45],[0,.7],[.4,.45],[1,.7],[.65,0],[.25,-.3],[0,-1],[-.25,-.3],[-.65,0]],
    oakwarden:[[-.1,-1],[.1,-1],[.1,-.3],[.7,-.1],[1,.4],[.5,.65],[0,1],[-.5,.65],[-1,.4],[-.7,-.1],[-.1,-.3]],
    dawnreach:[[0,1],[.2,.25],[1,0],[.2,-.25],[0,-1],[-.2,-.25],[-1,0],[-.2,.25]],
    saltwynd:[[-1,-.6],[-.5,-.3],[0,-.6],[.5,-.3],[1,-.6],[1,-.2],[.5,.1],[0,-.2],[-.5,.1],[-1,-.2],[-1,.6],[-.5,.9],[0,.6],[.5,.9],[1,.6],[1,.25],[.5,.5],[0,.25],[-.5,.5],[-1,.25]]
  };
  for(const [id,points]of Object.entries(emblemPoints))mesh('crest_'+id,[[0,0,.005],...points.map(p=>[...p,0])],points.map((_,i)=>[0,i+1,(i+1)%points.length+1]));
  const weapons=Object.fromEntries(Object.entries({sword:'menAtArms',axe:'levy',mace:'heavyInfantry',bow:'archer',crossbow:'crossbow',spear:'spearman',lance:'cavalry'}).map(([weapon,type])=>[weapon,human(type).filter(n=>n.name==='weapon'||n.parent==='weapon')]));
  return {format:'iron-thrones-rigged-mesh',version:1,license:'CC0-1.0',units:'meters',up:'Y',front:'+Z',meshes,models,weapons,houses:Object.fromEntries(CAMPAIGN_HOUSES.map(h=>[h.id,{color:h.color,sigil:h.sigil,name:h.name,mesh:'crest_'+h.id}])),materials:{skin:'#cda580',steel:'#91a2aa',silver:'#dce2dd',darkSteel:'#39474b',leather:'#654932',wood:'#785337',ivory:'#eee0b4',gold:'#bda466',horse:'#684b3b',cloth:'#46505a',stone:'#7e8584',house:'#ae684e',accent:'#ead8a6'},animations:{idle:{duration:2,loop:true},walk:{duration:.8,loop:true,swing:.55},run:{duration:.5,loop:true,swing:.8},gallop:{duration:.42,loop:true,swing:.65},attack:{duration:.9,loop:false},block:{duration:.5,loop:true},shoot:{duration:1,loop:false},reload:{duration:2.5,loop:false},death:{duration:1,loop:false},setup:{duration:2,loop:false},fire:{duration:2.5,loop:false}}};
}
