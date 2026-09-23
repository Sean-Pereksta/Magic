'use strict';
/* Install before the campaign adapter builds its coordinate index. */
(() => {
  const D=AWCampaignData,c=D.continent('verdant');
  Object.assign(MATERIALS,{
    timber:{name:'Timber',icon:'🪵',color:'#bca078',desc:'Hearthglade building supplies.'},
    stone:{name:'Stone',icon:'🪨',color:'#a7b3bc',desc:'Foundations, hearths and portals.'},
    fiber:{name:'Plant Fiber',icon:'🌾',color:'#c3d28c',desc:'Garden beds, woven furnishings and fertilizer.'}
  });
  const anchor=Object.values(D.nodes).find(n=>n.continent==='verdant'&&n.gx===0&&n.gy===3);
  if(!anchor||anchor.exits.W)throw new Error('Hearthglade requires the western Sunmere road.');
  const index=Math.max(...Object.values(D.nodes).map(n=>n.index))+1;
  const n={id:'verdant-hearthglade',index,continent:'verdant',name:'Hearthglade · Your Homestead',type:'sanctuary',homestead:true,biome:'meadow',roomCount:2,threat:1,gx:-1,gy:3,x:anchor.x-130,y:anchor.y,connections:[anchor.id],exits:{E:anchor.id},region:'hearthglade',material:'timber'};
  D.nodes[n.id]=n;anchor.exits.W=n.id;anchor.connections.push(n.id);c.nodes.push(n.id);
  for(const id of c.nodes)D.nodes[id].x+=150;c.mapWidth+=150;
  D.regions.hearthglade={id:'hearthglade',continent:'verdant',name:'Hearthglade',biome:'meadow',culture:'A place to return',nodes:[n.id]};c.regions.push('hearthglade');
  // Guarantee dangerous wave sites in every continent; do not change safe/boss nodes.
  for(const continent of D.continents){
    const candidates=continent.nodes.map(id=>D.nodes[id]).filter(n=>['danger','wildland','dungeon','event'].includes(n.type));
    const selected=[...candidates.filter(n=>n.type==='danger'||n.type==='dungeon'),...candidates.filter(n=>n.type==='event'),...candidates.filter(n=>n.type==='wildland').sort((a,b)=>b.threat-a.threat)];
    for(const n of selected.slice(0,Math.max(4,Math.ceil(candidates.length*.25))))n.multiWave=true;
  }
  const fresh=D.fresh,normalize=D.normalize,visible=D.visible;
  D.fresh=()=>({...fresh(),waveProgress:{}});
  D.normalize=raw=>{const s=normalize(raw);s.waveProgress={};for(const[k,v]of Object.entries(raw?.waveProgress||{}).slice(-500))if(/^\d+,\d+$/.test(k)&&Number.isInteger(v)&&v>=0&&v<=4)s.waveProgress[k]=v;return s;};
  D.visible=(s,node)=>node.homestead||visible(s,node);
})();
