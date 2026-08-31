/* Arcane Wilds cloud restore finalization.
   The base loader calculates stats before expansion trinkets are restored, so
   recalculate once after the encrypted bundle has fully reopened. */
const _awCloudLoadNamedWithExpansionStats=awCloudLoadNamed;
awCloudLoadNamed=async function(name,password){
  const clean=await _awCloudLoadNamedWithExpansionStats(name,password);
  if(game?.player&&typeof recomputePlayerStats==='function')recomputePlayerStats(false);
  if(typeof updateExpansionHUD==='function')updateExpansionHUD();
  return clean;
};
