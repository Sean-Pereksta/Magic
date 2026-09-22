'use strict';
/* Authored destinations on a cardinal grid. Stable legacy IDs keep completed
   bosses, quests, spells and old saves attached to the same places. */
(() => {
  const D=window.AWCampaignData,opposite={N:'S',S:'N',E:'W',W:'E'};
  const regions=[
    [
      ['Greenveil','forest','hide','Lanternfolk','greenveil','Foxglove Road|Fernbridge|Old Mill Crossing|Copperleaf Copse|Badger Den|Lark Meadow|Raincatcher Camp|Bumble Hollow|Greenveil Waystone|Oakheart Outpost|Golden Orchard|Grove of Echoes'],
      ['Mistfen','swamp','herbs','Reedwalkers','mistfen','Reedbank Village|Mistglass Pool|Hollow Reed Path|Leechwater Ford|Sunken Bell|Toadstool Grotto|Witchlight Camp|Mirewatch Tower|Herbalist Enclave|Drowned Reliquary|Fogbound Ferry|Spore Garden'],
      ['Ember Hills','volcanic','iron','Hillforged','emberhills','Copperhearth|Coalcut Path|Ember Ant Mound|Sootwind Ridge|Old Prospect|Charcoal Bridge|Flintwatch Camp|Basalt Quarry|Ironroot Mine|Forge Pilgrims|Warmstone Rest|Smelter Ruins'],
      ['Silverwood','thornwild','moonstone','Moonwardens','silverwood','Silverleaf Hamlet|Moonstone Vein|Whitehart Trail|Glassmoth Grove|Moonlit Spring|Fox Shrine|Elk Sanctuary|Secret Fernway|Stargazer Hollow|Moonwell Steps|Ivy Library|Whispering Gate'],
      ['Fallen Crown','ruins','dust','Crown Remnants','fallencrown','Crownrest|Broken Standard|Kingsroad Toll|Fallen Observatory|Silent Barracks|Royal Catacomb|Thronewatch|Bannerless Camp|Oathstone|Royal Vault|Warden Passage|Last Orchard']
    ],
    [
      ['Sunscar Expanse','desert','iron','Dune Traders','sunscar','Dunecross Caravan|Amber Dunes|Sundial Bridge|Saltwind Refuge|Buried Market|Scorpion Causeway|Sapphire Well|Sunscar Mine|Old Survey Camp|Mirage Shrine|Caravan Wreck|Sandglass Tower|Desert Waystone|Sunken Aqueduct'],
      ['Prismglass Basin','crystal','frost','Glasswrights','prismbasin','Facetwatch|Chiming Quarry|Broken Lens|Sapphire Causeway|Mirror Lake|Glass Wasp Nest|Shardcutters Camp|Refraction Tower|Prism Reservoir|Frozen Workshop|Crystal Reliquary|Gem Merchant|Deepglass Vault|Lens of Dawn'],
      ['Stormsteppe','stormlands','dust','Stormguard','stormsteppe','Thunderpost|Charged Grassland|Lightning Fence|Skyglass Bridge|Windrider Camp|Stormhound Den|Conductors Rise|Old Relay|Thunderclap Forge|Gale Refuge|Storm Path|Windwalker Shrine|Cloudbreak Tower|Rain Drum Hollow'],
      ['Rimebound Peaks','frost','frost','Snowbound','rimebound','Rimegate|Whitefall Pass|Avalanche Shelter|Icebound Stair|Glacier Teeth|Snowmelt Bridge|Frostvein Mine|Silent Summit|Rime Lanterns|Frosthunters Camp|Snowblind Ruins|Ice Mirror Shrine|Frozen Reservoir|Winter Waystone'],
      ['Cinder March','volcanic','ember','Cinderforged','cindermarch','Ashdock|Cinder Bridge|Magma Sluice|Obsidian Stair|Wasp Furnace|Blackflame Mine|Forgeward Camp|Molten Bell|Burning Foundry|Ashen Pilgrimage|Ember Reliquary|Lava Observatory|Scorched Caravan|Cinder Waystone']
    ],
    [
      ['Starbound Heights','celestial','stardust','Starbound','starbound','Dawnspire Outpost|Starbridge|Comet Garden|Fallen Astrolabe|Aerie Steps|Sunshard Mine|Luminous Chapel|Dawnwing Roost|Starweaver Camp|Fallen Zodiac|Twilight School|Quiet Constellation|Heavens Waystone|Sun Dial Shrine'],
      ['Nightglass Hollow','gloam','dust','Nightwatch','nightglass','Nightglass Haven|Duskmirror Bridge|Hollow Lanterns|Moonless Orchard|Umbral Stair|Nightmarket|Hidden Orrery|Glassshade Den|Starless Library|Voidwell|Midnight Causeway|Sable Survey Camp|Nocturne Shrine|Nightglass Vault'],
      ['Kingdom of Graves','crypt','bone','Gravewardens','graves','Lanterns End|Bonebridge|Tombkeepers Road|Silent Procession|Hollow Crypt|Last Bell Tower|Gravekeeper Camp|Broken Ossuary|Ashen Ancestors|Marble Sepulcher|Kings Mourning|Old Bone Market|Morrow Reliquary|Graveward Waystone'],
      ['Bloodmoon Forest','bloodroot','hide','Ash Rangers','bloodmoon','Redbark Village|Bloodmoon Crossing|Crimson Rootway|Hunterless Camp|Fang Grove|Thirsting Hollow|Redleaf Shrine|Sanguine Quarry|Old Ranger Tower|Thornblood Den|Ash Rangers Rest|Scarlet Run|Redwing Aerie|Bloodroot Vault'],
      ['Void Coast','gloam','voidshard','Rift Sailors','voidcoast','Riftshore|Phantom Pier|Broken Tide|Starless Shore|Abyss Bridge|Nightwind Camp|Voidfracture|Gryphon Watch|Driftglass Reef|Blackwater Shrine|Hollow Lighthouse|Ocean Without Stars|Rift Survey|Coastal Waystone'],
      ['Ashes of Heaven','volcanic','ember','Fallen Starforged','ashenheaven','Sunfall Forge|Ashwing Pass|Fallen Sun Pit|Obsidian Choir|Burnt Horizon|Last Light Camp|Eclipse Monastery|Crownless Stair|Cinderstar Mine|Heavens Grave|Final Observatory|Sunless Reliquary|Twilight Waystone|Kings of Ash']
    ]
  ];
  Object.assign(MATERIALS,{herbs:{name:'Mistfen Herbs',icon:'🌱',color:'#a6d889'},moonstone:{name:'Silverwood Moonstone',icon:'◈',color:'#d5d9ff'},stardust:{name:'Starbound Dust',icon:'✧',color:'#ffe6a1'},voidshard:{name:'Void Shard',icon:'◆',color:'#bc8bff'}});
  const types=['wildland','resource','event','wildland','danger','landmark','shrine','wildland','merchant','dungeon','wildland','treasure','tower','puzzle'];
  let nextIndex=Object.keys(D.nodes).length;
  D.regions={};
  D.continents.forEach((c,ci)=>{
    const width=8+ci*2,height=7,old=c.nodes.map(id=>D.nodes[id]),grid=new Map();
    const placement=[[1,3],[2,3],[2,2],[3,1],[4,1],[5,1],[3,3],[4,3],[2,5],[4,5],[5,5],[5,3],[6,2],[6,4],[3,0],[0,3],[3,5],[1,5],[6,6],[width-1,3],[0,0]];
    old.forEach((n,i)=>{n.gx=placement[i][0];n.gy=placement[i][1];grid.set(`${n.gx},${n.gy}`,n);});
    const used=new Map();let added=0;
    for(let y=0;y<height;y++)for(let x=0;x<width;x++){
      const ri=Math.min(regions[ci].length-1,Math.floor((y*width+x)*regions[ci].length/(width*height))),[name,biome,material,culture,rid,names]=regions[ci][ri];
      const regionId=`${c.id}-${rid}`;D.regions[regionId]||={id:regionId,name,biome,material,culture,continent:c.id};
      let n=grid.get(`${x},${y}`);
      if(!n){const list=names.split('|'),j=used.get(rid)||0;used.set(rid,j+1);const id=`${c.id}-${rid}-${j+1}`,type=types[(added+ci*3)%types.length];
        n={id,continent:c.id,name:list[j%list.length],type,biome,index:nextIndex++,roomCount:type==='dungeon'?5:1,threat:c.range[0]+Math.min(c.range[1]-c.range[0],Math.floor(ri*1.6)),gx:x,gy:y};
        if(type==='event')n.event=['rescue','defend','hunt','crystals','waves'][added%5];
        if(type==='resource')n.resource=material;
        if(type==='puzzle')n.puzzle=['Dawn','Zenith','Dusk'];
        if(type==='treasure'){n.hidden=true;n.rewardGold=80+ci*100;}
        D.nodes[id]=n;grid.set(`${x},${y}`,n);added++;
      }
      n.region=regionId;n.material=material;n.x=100+x*130;n.y=90+y*120;n.exits={};n.connections=[];
      n.waystone=!!n.town||/Waystone/.test(n.name);
    }
    const link=(a,b,route=null)=>{
      if(!a||!b||a.type==='ruler'||b.type==='ruler')return;
      const dir=a.gx===b.gx?(a.gy>b.gy?'N':'S'):a.gx>b.gx?'W':'E';
      if(a.exits[dir]||b.exits[opposite[dir]])return;
      a.exits[dir]=b.id;b.exits[opposite[dir]]=a.id;a.connections.push(b.id);b.connections.push(a.id);
      if(route){(a.routeRequirements||={})[dir]=route;(b.routeRequirements||={})[opposite[dir]]=route;}
    };
    // A connected road spine with additional cross-links, loops and quiet dead ends.
    for(let y=0;y<height;y++)for(let x=0;x<width-1;x++)link(grid.get(`${x},${y}`),grid.get(`${x+1},${y}`));
    for(let y=0;y<height-1;y++){
      const end=y%2?0:width-1;link(grid.get(`${end},${y}`),grid.get(`${end},${y+1}`));
      for(let x=1;x<width-1;x++)if((x+y*2)%4===0)link(grid.get(`${x},${y}`),grid.get(`${x},${y+1}`));
    }
    // The ruler remains reachable only through the city portal, not an ordinary road.
    D.nodes[c.start].connections.push(c.boss);D.nodes[c.boss].connections=[c.start];D.nodes[c.boss].exits={S:c.start};
    const candidates=[...grid.values()].filter(n=>!old.includes(n)&&!n.hidden);
    const extras=regions[ci].slice(0,5).map(r=>candidates.find(n=>n.region===`${c.id}-${r[4]}`)).filter(Boolean);
    const specs=[['Outpost','Merchant'],['Village','Alchemist'],['Town','Blacksmith'],['Village','Spell Scribe'],['Camp','Relic Dealer']];
    extras.forEach((n,j)=>{
      const reg=D.regions[n.region],id=`${c.id}_settlement_${j}`;n.town=id;n.type=j===2?'town':'village';n.waystone=true;
      const material=reg.material;const materialItem=`regional_${material}`;D.items[materialItem]||={name:`${MATERIALS[material].name} bundle`,kind:'material',material,price:70+ci*45,count:5};
      D.towns[id]={name:n.name,culture:reg.culture,theme:reg.biome,roles:[specs[j][1],'Quest Keeper','Cartographer'],stock:[materialItem,'tonic'],spells:[],mounts:[],quest:`${c.id}-boss${j%4+1}`,line:`These are the ${reg.name}. Our trade is ${MATERIALS[material].name.toLowerCase()}. Ask our cartographer about unmarked trails.`};
    });
    // Optional mount-only shortcuts have no bearing on required boss access.
    let shortcut=false;
    for(let x=1;x<width-1&&!shortcut;x++)for(let y=0;y<height-2&&!shortcut;y++){
      const a=grid.get(`${x},${y}`),b=grid.get(`${x},${y+2}`);
      if(a&&!a.exits.S&&b&&!b.exits.N){link(a,b,['forestTrail','stormPath','flightRoute'][ci]);shortcut=true;}
    }
    c.nodes=[...grid.values()].map(n=>n.id);c.mapWidth=200+(width-1)*130;c.mapHeight=200+(height-1)*120;c.regions=Object.values(D.regions).filter(r=>r.continent===c.id).map(r=>r.id);
  });
  // Dungeon geometry is cardinal and reciprocal, including a real return at the entrance.
  D.dungeonLinks=[{N:1},{S:0,W:2,E:3,N:4},{E:1},{W:1},{S:1}];
  D.routeMount={forestTrail:'verdantElk',stormPath:'stormclaw',flightRoute:'astralGryphon'};
  const baseFresh=D.fresh,baseNormalize=D.normalize,baseVisible=D.visible,baseReason=D.travelReason;
  D.fresh=()=>({...baseFresh(),version:2,waystones:['verdant-city'],secrets:[],itemClaims:[],worldQuests:{},provisions:{},dungeonEntrances:{},cacheClaims:{}});
  D.normalize=raw=>{
    const s={...D.fresh(),...baseNormalize(raw)};s.version=2;s.rewards=(raw?.rewards||[]).filter(id=>D.items[id]).slice(0,100);
    for(const key of ['waystones','secrets','itemClaims'])s[key]=[...new Set((Array.isArray(raw?.[key])?raw[key]:key==='waystones'&&raw?.version!==2?s.visited.filter(id=>D.nodes[id]?.town):[]).filter(id=>key==='itemClaims'?typeof id==='string':D.nodes[id]))];
    if(!s.waystones.includes('verdant-city'))s.waystones.unshift('verdant-city');
    s.worldQuests=raw?.worldQuests&&typeof raw.worldQuests==='object'?raw.worldQuests:{};s.provisions=raw?.provisions&&typeof raw.provisions==='object'?raw.provisions:{};
    s.cacheClaims={};for(const [key,v] of Object.entries(raw?.cacheClaims||{}))if(Array.isArray(v))s.cacheClaims[key]=v.slice(0,10).map(Boolean);
    s.dungeonEntrances={};for(const [id,e] of Object.entries(raw?.dungeonEntrances||{}))if(D.nodes[id]?.type==='dungeon'&&D.nodes[e?.node]&&['N','S','E','W'].includes(e.side))s.dungeonEntrances[id]={node:e.node,side:e.side};
    return s;
  };
  D.visible=(s,n)=>n.hidden?!n.shadow&&(s.visited.includes(n.id)||s.secrets?.includes(n.id)||s.scouted.includes(n.id)):baseVisible(s,n);
  D.travelReason=(s,id,mode='road')=>{
    const from=D.nodes[s.current],to=D.nodes[id];
    if(mode==='portal'||mode==='waystone')return s.waystones?.includes(from?.id)&&s.waystones.includes(id)&&s.unlocked.includes(to?.continent)?'':'Activate both waystones before fast travel.';
    const reason=baseReason(s,id,mode);if(reason)return reason;
    const dir=Object.keys(from.exits||{}).find(d=>from.exits[d]===id),route=from.routeRequirements?.[dir];
    if(route&&!s.mounts.some(id=>D.mounts[id]?.route===route))return `This route requires the ${D.mounts[D.routeMount[route]]?.name||route}.`;
    return '';
  };
  D.opposite=opposite;
})();
