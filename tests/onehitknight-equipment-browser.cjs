/* Complete armored knight PNGs, separate weapons, and the actual player renderer. */
module.exports=async function equipmentBrowserChecks(page,exec,check,assert){
 const setup=String.raw`
  const weaponIds=['sword','long-sword','greatsword','rapier','axe','hammer','flame','frost','thunder'];
  const knightIds=['steel','gold','shadow','phoenix','vampire','thunder'];
  const states=['idle','run','slash','dash','block','death','victory'];
  const phaseSamples=[['anticipate',.25],['anticipate',.75],['active',.125],['active',.375],['active',.625],['active',.875],['recover',.25],['recover',.75]];
  function must(ok,message){if(!ok)throw new Error(message);}
  function canvas(){const c=document.createElement('canvas');c.width=192;c.height=192;return c;}
  function pixelHash(c){const data=c.getContext('2d').getImageData(0,0,c.width,c.height).data;let hash=2166136261,visible=0;for(let i=0;i<data.length;i++){hash=Math.imul(hash^data[i],16777619)>>>0;if(i%4===3&&data[i]>0)visible++;}must(visible>0,'Sprite contains visible canvas pixels');return hash.toString(16);}
  function spriteHash(c,id,state,dir,progress){const context=c.getContext('2d');context.clearRect(0,0,c.width,c.height);must(OHKAssets.draw(context,id,state,96,132,{dir,progress,scale:1,optional:true}),id+' actual PNG is loaded');return pixelHash(c);}
  function animationOf(sp,state,dir){const direction=['right','down','left','up'][dir];let a=sp.animations[state+'.'+direction]||sp.animations[state+'_'+direction]||sp.animations[state];if(a&&a[direction])a=a[direction];must(a&&a.frames&&a.frames.length,state+' has declared poses');return a;}
  function poseProgress(sp,state,dir,index){const a=animationOf(sp,state,dir);must(a.frames.length>index,state+' has pose '+index);const durations=a.frames.map((_,i)=>Math.max(1,Number(a.durations?.[i]||a.frameDuration||a.duration||sp.frameDuration||100)||100));return (durations.slice(0,index).reduce((n,v)=>n+v,0)+durations[index]/2)/durations.reduce((n,v)=>n+v,0);}
  function footPivot(sp,state,dir){const pivot=OHKAssets.frameInfo(sp,state,{dir,progress:0}).pivot;return Array.isArray(pivot)?pivot:[pivot.x,pivot.y];}
 `;
 const run=body=>exec('(async function(){'+setup+body+'})()');

 await check('nine separate weapon PNGs display all four directions and eight distinct slash poses',async()=>{
  const result=await run(String.raw`
   await OHKAssets.ready;await OHKAssets.preload(weaponIds.map(id=>'weapon.'+id));
   const files=new Set(),c=canvas(),checked=[];
   for(const name of weaponIds){const id='weapon.'+name,sp=OHKAssets.lookup(id);must(sp&&sp.image&&sp.animations,id+' new manifest entry');files.add(sp.image);
    for(let dir=0;dir<4;dir++){const hashes=new Set();for(let frame=0;frame<8;frame++)hashes.add(spriteHash(c,id,'slash',dir,poseProgress(sp,'slash',dir,frame)));must(hashes.size===8,id+' direction '+dir+' eight different PNG poses');const p=footPivot(sp,'slash',dir);must(p[0]===56&&p[1]===76,id+' padded foot anchor');checked.push(id+'.'+dir);}
   }
   must(files.size===9,'Long Sword and Greatsword have separate image files');return {weapons:weaponIds.length,directionalViews:checked.length};
  `);assert.equal(result.weapons,9);assert.equal(result.directionalViews,36);
 });

 await check('six complete armored knight PNGs animate seven states in all four directions',async()=>{
  const result=await run(String.raw`
   await OHKAssets.preload(knightIds.map(id=>'knight.'+id));const files=new Set(),c=canvas();let views=0,poses=0;
   for(const name of knightIds){const id='knight.'+name,sp=OHKAssets.lookup(id);must(sp&&sp.image&&sp.animations,id+' complete knight entry');must(sp.frameWidth===48&&sp.frameHeight===48&&sp.columns===8&&sp.rows===28&&sp.frames.length===224,id+' complete 224-cell body sheet');files.add(sp.image);
    for(const state of states)for(let dir=0;dir<4;dir++){const a=animationOf(sp,state,dir),hashes=new Set();for(let frame=0;frame<a.frames.length;frame++)hashes.add(spriteHash(c,id,state,dir,poseProgress(sp,state,dir,frame)));must(hashes.size===a.frames.length,id+' '+state+' direction '+dir+' distinct complete body poses');const p=footPivot(sp,state,dir);must(p[0]===24&&p[1]===44,id+' body foot anchor aligns with weapon padding');views++;poses+=a.frames.length;}
   }
   must(files.size===6,'Each armor style owns a complete knight PNG');
   for(const name of knightIds.filter(id=>id!=='steel'))must(OHKAssets.lookup('armor.'+name).image===OHKAssets.lookup('knight.'+name).image,name+' upgrade preview aliases the complete body');
   for(const alias of ['knight','armor.base','armor.default'])must(OHKAssets.lookup(alias).image===OHKAssets.lookup('knight.steel').image,alias+' resolves to the steel body');
   return {knights:knightIds.length,stateViews:views,poses};
  `);assert.equal(result.knights,6);assert.equal(result.stateViews,168);assert(result.poses>=1248);
 });

 await check('every weapon and knight combination shares combat frames without armor overlays',async()=>{
  const result=await run(String.raw`
   const c=canvas(),target=c.getContext('2d'),original=OHKAssets.draw,seen=[];let combinations=0,bodyStates=0,deathViews=0;
   function selectedBody(style,state){const bodies=seen.filter(item=>/^knight(?:\.|$)/.test(item.id));must(bodies.length===1,'Exactly one complete knight body is drawn');must(bodies[0].id==='knight.'+style,style+' body is selected');must(bodies[0].state===state,style+' body uses '+state);must(!seen.some(item=>item.id.startsWith('armor.')),'No armor image overlays the complete knight');return bodies[0];}
   OHKAssets.draw=function(context,id,state,x,y,options={}){seen.push({id,state,options:{...options}});return original.call(OHKAssets,context,id,state,x,y,options);};
   try{
    for(const armor of knightIds)for(const weapon of weaponIds)for(let dir=0;dir<4;dir++){
     const poseKeys=new Set();
     for(let frame=0;frame<phaseSamples.length;frame++){
      const [phase,phaseProgress]=phaseSamples[frame];seen.length=0;target.clearRect(0,0,c.width,c.height);
      OHKRender.knight(target,96,132,{player:{alive:true,faceDir:dir,equipment:{armor,weapon},weapon,buffs:{},attack:{phase,phaseProgress,faceDir:dir,reach:WEAPONS[weapon]?.reach||29}},scale:1,alpha:.8});
      const body=selectedBody(armor,'slash'),blade=seen.find(item=>item.id==='weapon.'+weapon);
      must(body.options.frameIndex===frame,armor+' explicit body swing frame '+frame);must(blade&&blade.options.frameIndex===frame,weapon+' shares body swing frame '+frame);must(body.options.scale===blade.options.scale,'Weapon grip and knight use matching scale at base reach');
      const f=OHKAssets.frameInfo(OHKAssets.lookup(body.id),'slash',body.options).frame;poseKeys.add(f.x+','+f.y);
     }
     must(poseKeys.size===8,armor+' advances all eight synchronized slash poses');pixelHash(c);combinations++;
    }
    for(const armor of knightIds)for(let dir=0;dir<4;dir++)for(const state of states){
     seen.length=0;target.clearRect(0,0,c.width,c.height);
     OHKRender.knight(target,96,132,{player:{alive:state!=='death',deathTimer:state==='death'?.1625:0,faceDir:dir,equipment:{armor,weapon:'sword'},weapon:'sword',buffs:{}},state,progress:.75,scale:1,alpha:.8});
     const body=selectedBody(armor,state);must(body.options.alpha>0,armor+' full body remains visible in '+state);pixelHash(c);bodyStates++;
     if(state==='death'){const only=canvas();must(original.call(OHKAssets,only.getContext('2d'),body.id,'death',96,132,body.options),armor+' death PNG is loaded');pixelHash(only);deathViews++;}
    }
    for(const armor of ['default','base']){seen.length=0;OHKRender.knight(target,96,132,{player:{alive:true,equipment:{armor,weapon:'sword'},weapon:'sword',buffs:{}}});selectedBody('steel','idle');}
   }finally{OHKAssets.draw=original;}
   return {combinations,bodyStates,deathViews};
  `);assert.equal(result.combinations,216);assert.equal(result.bodyStates,168);assert.equal(result.deathViews,24);
 });
 await check('armor upgrade cards select their complete knight variants',async()=>{
  const selected=await run(String.raw`
   const result=[];
   for(const [item,style] of [['golden-knight','gold'],['shadow-knight','shadow'],['phoenix-armor','phoenix'],['vampire-gem','vampire'],['thunder-knight','thunder']]){
    await reset('CLASSIC');enemies=[];wavePause=999;spawnTimer=999;
    const upgrade=choices.find(c=>c.item===item);must(upgrade,item+' choice exists');upgrade[2]();
    must(player.equipment.armor===style,item+' equips '+style);must(player.equipment.cape==='red',item+' does not add a duplicate cape');
    const c=canvas(),draw=OHKAssets.draw,seen=[];
    OHKAssets.draw=function(context,id,...args){seen.push(id);return draw.call(OHKAssets,context,id,...args);};
    try{OHKRender.knight(c.getContext('2d'),96,132,{player,scale:1});}finally{OHKAssets.draw=draw;}
    must(seen.includes('knight.'+style),item+' draws its complete character');must(!seen.some(id=>id.startsWith('armor.')),item+' has no armor layer');
    if(item==='thunder-knight')must(player.weapon==='thunder',item+' equips its matching weapon');
    pixelHash(c);result.push(item);
   }
   return result;
  `);assert.equal(selected.length,5);
 });
};
