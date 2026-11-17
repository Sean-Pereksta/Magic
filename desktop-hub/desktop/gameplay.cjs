// Used by release tests to verify that only game routes can contribute to activity.
function gameOnPage(catalog, raw, declared){
  let url;
  try{url=new URL(raw);}catch{return null;}
  const pathMatches=g=>[g.route,g.singleRoute,g.multiplayerRoute,g.dmRoute].filter(Boolean).some(route=>{
    const entry=new URL(route,'https://www.catnmice.com/');
    const canonical=p=>p.replace(/\/index\.html$/i,'/').replace(/\/$/,'');
    return entry.hostname===url.hostname && canonical(entry.pathname)===canonical(url.pathname);
  });
  const declaredGame=catalog.games.find(g=>g.id===declared);
  if(declaredGame && pathMatches(declaredGame))return declaredGame;
  return catalog.games.find(pathMatches)||null;
}
module.exports={gameOnPage};
