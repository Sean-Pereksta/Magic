// Cheesehold campaign runtime hardening.
// Loaded after campaign threats to provide stable ownership for enemy-built objectives,
// make Warlord RAID orders actionable, and keep very late arena growth performance-conscious.

const cheeseholdCampaignRuntimeBaseConfigureEnemy=cheeseholdCampaignConfigureEnemy;
cheeseholdCampaignConfigureEnemy=function(e){
 const out=cheeseholdCampaignRuntimeBaseConfigureEnemy(e);
 if(out&&out.campaignId==null)out.campaignId=idCounter++;
 return out;
};

const cheeseholdCampaignRuntimeBaseSpawnNode=cheeseholdCampaignSpawnNode;
cheeseholdCampaignSpawnNode=function(kind,gx,gy,owner=null){
 if(owner&&owner.campaignId==null)owner.campaignId=idCounter++;
 const node=cheeseholdCampaignRuntimeBaseSpawnNode(kind,gx,gy,owner);
 if(node){
  if(node.campaignId==null)node.campaignId=idCounter++;
  node.campaignOwner=owner?.campaignId??null;
 }
 return node;
};

const cheeseholdCampaignRuntimeBaseSupportPulse=cheeseholdCampaignSupportPulse;
cheeseholdCampaignSupportPulse=function(e){
 if(!e||e.dead)return;
 if(e.campaignId==null)e.campaignId=idCounter++;
 if(e.campaignMechanic==='queen'){
  const owned=enemies.filter(x=>!x.dead&&x.campaignMechanic==='tunnelNode'&&x.campaignOwner===e.campaignId).length;
  if(game.time>=e.campaignNextSpecial&&owned<2){
   e.campaignNextSpecial=game.time+7.5;
   const node=cheeseholdCampaignSpawnNode('tunnelNode',e.gx,e.gy,e);
   if(node)toast('Burrow Queen opened a tunnel nest!');
  }
  return;
 }
 if(e.campaignMechanic==='engineer'){
  const owned=enemies.filter(x=>!x.dead&&(x.campaignMechanic==='healNode'||x.campaignMechanic==='mortarNode')&&x.campaignOwner===e.campaignId).length;
  if(game.time>=e.campaignNextSpecial&&owned<2){
   e.campaignNextSpecial=game.time+8;
   const kind=((Math.floor(game.time)+(e.campaignId||0))%2)?'healNode':'mortarNode';
   const node=cheeseholdCampaignSpawnNode(kind,e.gx,e.gy,e);
   if(node)toast('Dread Engineer built an enemy '+(kind==='healNode'?'repair nest':'mortar nest')+'!');
  }
  return;
 }
 return cheeseholdCampaignRuntimeBaseSupportPulse(e);
};

const cheeseholdCampaignRuntimeBaseEnemyUpdate=enemyUpdate;
enemyUpdate=function(e,dt,isCat=false){
 if(e&&!e.dead&&!isCat&&e.campaignOrderUntil>game.time&&e.campaignOrder==='raid'&&!e.moving&&game.time>=e.nextMove&&typeof cheeseholdPreferredRoleBuilding==='function'&&typeof cheeseholdMoveRoleToward==='function'){
  const target=cheeseholdPreferredRoleBuilding(e,'raider');
  if(target&&cheeseholdMoveRoleToward(e,target))return;
 }
 return cheeseholdCampaignRuntimeBaseEnemyUpdate(e,dt,isCat);
};

const cheeseholdCampaignRuntimeBaseAttackBuilding=attackBuilding;
attackBuilding=function(e,b){
 if(!e||!b)return cheeseholdCampaignRuntimeBaseAttackBuilding(e,b);
 const old=e.damage;
 if(e.campaignOrderUntil>game.time&&e.campaignOrder==='raid'&&(b.type==='generator'||b.type==='turret'||b.type==='tesla'||b.type==='phoenix'))e.damage*=1.18;
 const out=cheeseholdCampaignRuntimeBaseAttackBuilding(e,b);
 e.damage=old;
 return out;
};

// Keep late battlefields broader without letting tile count become the dominant performance cost.
const cheeseholdCampaignRuntimeBaseArenaSize=cheeseholdArenaSize;
cheeseholdArenaSize=function(round=game?.round||1){
 if(round>=40)return[53,39];
 if(round>=35)return[51,37];
 if(round>=30)return[49,35];
 return cheeseholdCampaignRuntimeBaseArenaSize(round);
};

console.info('[Cheesehold] campaign command runtime + objective ownership loaded');
