/* Keep the shared player-pool filter compatible with the rankings-first control IDs. */
filteredPool=function(prefix,includeDrafted=true){
  const main=prefix==='rank'?'Main':'';
  const q=$(`#${prefix}Search${main}`).value.toLowerCase().trim();
  const pos=$(`#${prefix}Position${main}`).value;
  const risk=$(`#${prefix}Risk${main}`).value;
  const unavailable=unavailableSet();
  return eligible().filter(p=>(includeDrafted||!unavailable.has(idOf(p)))&&(!q||(p.full_name||'').toLowerCase().includes(q)||(p.team||'').toLowerCase().includes(q)||(p.position||'').toLowerCase().includes(q))&&(pos==='ALL'||p.position===pos)&&(risk==='ALL'||riskFor(p)===risk));
};
