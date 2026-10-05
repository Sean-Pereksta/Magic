// In-memory workload for the actual core, shared by regression tests and the
// CPU benchmark. It does not connect to Firebase or generate preview files.
export const lateGameFixture = String.raw`
  let allStructureReads=0;
  const originalGetAllStructures=getAllStructures;
  getAllStructures=function(){allStructureReads++;return originalGetAllStructures();};
  let terrainReads=0;
  const originalGetTileTexture=getTileTexture;
  getTileTexture=function(...args){terrainReads++;return originalGetTileTexture(...args);};
  Object.assign(window.__battleFixture,{
    lateGame(count=420){
      const types=['🔫','💎','🛡️','🧱','🔫','🏭','⚡','🪳'];
      let placed=structureIndex.all.size;
      for(let y=0;y<gridSize && placed<count;y++)for(let x=0;x<gridSize && placed<count;x++){
        if(grid[y][x] || (x>=7 && x<=17 && y>=8 && y<=15))continue;
        const type=types[placed%types.length],health=STRUCTURE_MAX_HP[type];
        grid[y][x]=type;structureHealth[x+'_'+y]=health;
        addToStructureIndex(x,y,type,health);structures.set(x+'_'+y,{x,y,type,health});placed++;
      }
      stinkRats.push({id:'late-stink',kind:'stinkrat',x:5,y:5,health:100,createdAt:Date.now()});
      catPower=60;ratPower=8;catHealth=maxCatHealth=1000000;
      markBlockedGridDirty();markStructureIndexDirty();renderGrid();
      return placed;
    },
    draw(){renderGrid();},
    lateActors(){
      currentDifficulty='hard';players.third={uid:'third',x:9,y:9,alive:true};players.fourth={uid:'fourth',x:10,y:9,alive:true};
      const spots=[];
      for(let y=8;y<=15;y++)for(let x=7;x<=17;x++)if(!STRUCTURE_TYPES.has(grid[y][x]) && !Object.values(players).some(p=>p.x===x && p.y===y))spots.push({x,y});
      let cursor=0;
      const fill=(list,kind,count)=>{while(list.length<count){const spot=spots[cursor++%spots.length];list.push({id:'late-'+kind+'-'+list.length,kind,...spot,health:100000,lifetime:30,createdAt:Date.now()});}};
      fill(rats,'rat',10);fill(vultures,'vulture',2);fill(rabbits,'rabbit',12);fill(fleas,'flea',8);
      for(const list of [rats,stinkRats,oxen,ratKings,vultures,termites,rabbits])for(const e of list)e.health=100000;
      tacticalAI.reset();renderGrid();return {living:getTotalLivingEnemyCount(),cap:getEnemyCaps().total};
    },
    async shields(){await shieldTowerTick();},
    reads(){return allStructureReads;},
    terrainReads(){return terrainReads;},
    terrainEffect(){setTileEffect(1,28,'blood');renderGrid();},
    pickups(){grid[28][3]=CHEESE_PICKUP;traps.push({x:6,y:28});renderGrid();},
    firstStructure(){return structureIndex.all.values().next().value;},
    removeStructure(x,y){applyLocalRuntimeMutation('delete',ccRef(ID.structure(x,y)));},
    slimeSize(){return slimeAuraMemory.size;},
    structureRevision(){return tacticalRevision;},
    hpUpdate(x,y){const key=x+'_'+y;applyLocalRuntimeMutation('update',ccRef(ID.structure(x,y)),{health:structureHealth[key]-1});},
    stinkKey(){return getStinkRatDisabledTowerKey(stinkRats.at(-1));},
    addSlimeHistory(){for(let i=0;i<5000;i++)slimeAuraMemory.set('retired:'+i,Date.now()-60000);},
    unitCounts(){return {rats:rats.length,fleas:fleas.length,rabbits:rabbits.length};}
  });
`;
