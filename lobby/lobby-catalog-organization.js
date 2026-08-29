(()=>{
  "use strict";

  const STATUS_ORDER=["showcase","playable","tools","prototype"];
  const STATUS_META={
    showcase:{label:"Current & Showcase",icon:"⭐",short:"Current",description:"The most actively developed and polished games in the hub.",order:0},
    playable:{label:"Playable Library",icon:"🎮",short:"Playable",description:"Established games that remain available to play.",order:1000},
    prototype:{label:"Prototypes & Incomplete",icon:"🧪",short:"Prototype",description:"Older experiments and incomplete builds, kept available without crowding the main library.",order:3000},
    tools:{label:"Tools & Learning",icon:"🧰",short:"Tool",description:"Useful non-game experiences that live alongside the library.",order:2000}
  };

  const SHOWCASE_TITLES=new Set([
    "War Realms",
    "Bible Game",
    "Arcane Wilds",
    "Space Tyrants: Galactic Mandate",
    "Cheesehold 3D",
    "Hearthmouse",
    "Gridbound Realms",
    "Chess Warlord"
  ]);
  const TOOL_TITLES=new Set(["PDF Reader"]);
  const PROTOTYPE_ALIASES=new Set([
    "Mountain Blade Skirmish"
  ]);

  const cleanTitle=value=>String(value||"")
    .replace(/\s*\(Incomplete\)\s*/gi," ")
    .replace(/\s{2,}/g," ")
    .trim();

  function statusForTitle(value){
    const raw=String(value||"").trim();
    const cleaned=cleanTitle(raw);
    if(TOOL_TITLES.has(cleaned))return "tools";
    if(SHOWCASE_TITLES.has(cleaned))return "showcase";
    if(/\(Incomplete\)/i.test(raw)||PROTOTYPE_ALIASES.has(cleaned))return "prototype";
    return "playable";
  }

  function statusForText(value){
    const raw=String(value||"");
    if(/\(Incomplete\)/i.test(raw))return "prototype";
    const normalized=cleanTitle(raw);
    for(const title of TOOL_TITLES)if(normalized.includes(title))return "tools";
    for(const title of SHOWCASE_TITLES)if(normalized.includes(title))return "showcase";
    for(const title of PROTOTYPE_ALIASES)if(normalized.includes(title))return "prototype";
    return "playable";
  }

  function injectStyles(){
    if(document.getElementById("catalogOrganizationStyles"))return;
    const style=document.createElement("style");
    style.id="catalogOrganizationStyles";
    style.textContent=`
      .catalog-shelf-heading{grid-column:1/-1;display:flex;align-items:flex-end;justify-content:space-between;gap:14px;margin:15px 0 1px;padding:14px 16px;border:1px solid var(--line);border-radius:16px;background:linear-gradient(115deg,#fff,var(--surface-2));box-shadow:0 6px 18px rgba(16,24,40,.05)}
      .catalog-shelf-heading:first-child{margin-top:0}
      .catalog-shelf-title{display:flex;align-items:center;gap:9px;font-size:1.02rem;font-weight:950;letter-spacing:-.015em}
      .catalog-shelf-count{display:inline-grid;place-items:center;min-width:26px;height:24px;padding:0 7px;border-radius:999px;background:#eef2f6;color:#344054;font-size:.72rem;font-weight:950}
      .catalog-shelf-description{margin-top:4px;color:var(--muted);font-size:.79rem;line-height:1.4}
      .catalog-prototype-toggle{min-height:34px;border:1px solid #fed7aa;border-radius:10px;background:#fff7ed;color:#9a3412;padding:6px 10px;font-weight:900;font-size:.75rem;cursor:pointer;white-space:nowrap}
      .catalog-status-badge{font-weight:950!important}
      .catalog-status-badge[data-status="showcase"]{background:#fff7ed!important;color:#9a3412!important;border-color:#fed7aa!important}
      .catalog-status-badge[data-status="prototype"]{background:#fef2f2!important;color:#b42318!important;border-color:#fecaca!important}
      .catalog-status-badge[data-status="tools"]{background:#eff8ff!important;color:#175cd3!important;border-color:#b2ddff!important}
      .catalog-status-badge[data-status="playable"]{background:#f0fdf4!important;color:#166534!important;border-color:#bbf7d0!important}
      .catalog-organized-grid[data-prototypes-open="false"] > .catalog-prototype-card{display:none!important}
      .catalog-organized-grid > .empty-state{order:9999}
      .catalog-library-note{margin:-3px 0 16px;padding:11px 14px;border-radius:13px;background:#f8fafc;border:1px solid var(--line);color:#475467;font-size:.8rem;line-height:1.45}
      select optgroup{font-weight:900;color:#344054}
      select option{font-weight:500;color:#101828}
      @media(max-width:760px){.catalog-shelf-heading{align-items:flex-start;flex-direction:column}.catalog-prototype-toggle{width:100%}}
    `;
    document.head.appendChild(style);
  }

  function addStatusBadge(card,status,targetSelector){
    const target=card.querySelector(targetSelector);
    if(!target||target.querySelector(`.catalog-status-badge[data-status="${status}"]`))return;
    const badge=document.createElement("span");
    badge.className="tag catalog-status-badge";
    badge.dataset.status=status;
    badge.textContent=STATUS_META[status].short;
    target.appendChild(badge);
  }

  function decorateGameCard(card){
    if(!(card instanceof HTMLElement))return;
    const name=card.querySelector(".game-name");
    if(!name)return;
    const raw=name.dataset.catalogRawTitle||name.textContent.trim();
    name.dataset.catalogRawTitle=raw;
    const status=statusForTitle(raw);
    const cleaned=cleanTitle(raw);
    if(name.textContent.trim()!==cleaned)name.textContent=cleaned;
    card.dataset.catalogStatus=status;
    addStatusBadge(card,status,".game-meta");
  }

  function decorateLobbyCard(card){
    const game=card.querySelector(".lobby-game");
    if(!game)return null;
    const raw=game.dataset.catalogRawTitle||game.childNodes[0]?.textContent?.trim()||game.textContent.trim();
    game.dataset.catalogRawTitle=raw;
    const status=statusForTitle(raw);
    if(game.childNodes[0]?.nodeType===Node.TEXT_NODE)game.childNodes[0].textContent=cleanTitle(raw)+" ";
    else game.textContent=cleanTitle(raw);
    card.dataset.catalogStatus=status;
    if(!game.querySelector(".catalog-status-badge")){
      const badge=document.createElement("span");
      badge.className="tag catalog-status-badge";
      badge.dataset.status=status;
      badge.textContent=STATUS_META[status].short;
      badge.style.marginLeft="6px";
      game.appendChild(badge);
    }
    return status;
  }

  function decorateOnlineCard(card){
    const title=card.querySelector(".online-world-title");
    if(!title)return null;
    const raw=title.dataset.catalogRawTitle||title.textContent.trim();
    title.dataset.catalogRawTitle=raw;
    const status=statusForTitle(raw);
    title.textContent=cleanTitle(raw);
    card.dataset.catalogStatus=status;
    addStatusBadge(card,status,".online-world-badges");
    return status;
  }

  function makeHeading(status,count,grid){
    const meta=STATUS_META[status];
    const heading=document.createElement("div");
    heading.className="catalog-shelf-heading";
    heading.dataset.catalogShelf=status;
    heading.style.order=String(meta.order);
    heading.innerHTML=`<div><div class="catalog-shelf-title"><span>${meta.icon}</span><span>${meta.label}</span><span class="catalog-shelf-count">${count}</span></div><div class="catalog-shelf-description">${meta.description}</div></div>`;
    if(status==="prototype"){
      const button=document.createElement("button");
      button.type="button";
      button.className="catalog-prototype-toggle";
      const refresh=()=>{button.textContent=grid.dataset.prototypesOpen==="true"?"Hide prototypes":"Show prototypes";};
      button.addEventListener("click",()=>{grid.dataset.prototypesOpen=grid.dataset.prototypesOpen==="true"?"false":"true";refresh();});
      refresh();
      heading.appendChild(button);
    }
    return heading;
  }

  function organizeGrid(grid,cardSelector,decorator,{includeTools=false}={}){
    if(!grid)return;
    grid.classList.add("catalog-organized-grid");
    if(!grid.dataset.prototypesOpen)grid.dataset.prototypesOpen="false";
    grid.querySelectorAll(":scope > .catalog-shelf-heading").forEach(node=>node.remove());
    const cards=[...grid.querySelectorAll(`:scope > ${cardSelector}`)];
    if(!cards.length)return;
    const counts={showcase:0,playable:0,prototype:0,tools:0};
    cards.forEach((card,index)=>{
      const status=decorator(card)||card.dataset.catalogStatus||"playable";
      counts[status]=(counts[status]||0)+1;
      card.classList.toggle("catalog-prototype-card",status==="prototype");
      card.style.order=String(STATUS_META[status].order+10+index);
    });
    STATUS_ORDER.filter(status=>includeTools||status!=="tools").forEach(status=>{
      if(counts[status])grid.appendChild(makeHeading(status,counts[status],grid));
    });
  }

  function groupSelect(select,{keepAll=false}={}){
    if(!select||select.dataset.catalogGroupingBusy==="1")return;
    const options=[...select.querySelectorAll("option")];
    if(!options.length)return;
    const signature=options.map(option=>`${option.value}:${option.textContent}`).join("|");
    if(select.dataset.catalogGroupedSignature===signature&&select.querySelector("optgroup"))return;
    select.dataset.catalogGroupingBusy="1";
    const current=select.value;
    const allOption=keepAll?options.find(option=>option.value==="all"):null;
    const buckets={showcase:[],playable:[],prototype:[],tools:[]};
    options.forEach(option=>{
      if(option===allOption)return;
      const raw=option.dataset.catalogRawTitle||option.textContent;
      option.dataset.catalogRawTitle=raw;
      const status=statusForText(raw);
      option.dataset.catalogStatus=status;
      option.textContent=cleanTitle(raw)+(status==="prototype"?" — Prototype":"");
      buckets[status].push(option);
    });
    select.innerHTML="";
    if(allOption){allOption.textContent="All Hosted Games";select.appendChild(allOption);}
    STATUS_ORDER.forEach(status=>{
      if(!buckets[status].length)return;
      const group=document.createElement("optgroup");
      group.label=`${STATUS_META[status].icon} ${STATUS_META[status].label}`;
      buckets[status].forEach(option=>group.appendChild(option));
      select.appendChild(group);
    });
    if([...select.options].some(option=>option.value===current))select.value=current;
    select.dataset.catalogGroupedSignature=[...select.querySelectorAll("option")].map(option=>`${option.value}:${option.textContent}`).join("|");
    select.dataset.catalogGroupingBusy="0";
  }

  function observeGrid(grid,organize){
    if(!grid)return;
    let queued=false;
    const run=()=>{
      if(queued)return;
      queued=true;
      requestAnimationFrame(()=>{queued=false;organize();});
    };
    new MutationObserver(records=>{
      const relevant=records.some(record=>[...record.addedNodes,...record.removedNodes].some(node=>
        node.nodeType===Node.ELEMENT_NODE&&!node.classList?.contains("catalog-shelf-heading")
      ));
      if(relevant)run();
    }).observe(grid,{childList:true});
    run();
  }

  function observeSelect(select,options){
    if(!select)return;
    let timer=0;
    const run=()=>{clearTimeout(timer);timer=setTimeout(()=>groupSelect(select,options),0);};
    new MutationObserver(run).observe(select,{childList:true,subtree:true});
    run();
  }

  function setCopy(){
    const singleTitle=document.querySelector("#view-singleplayer > .section-head .section-title");
    const singleSub=document.querySelector("#view-singleplayer > .section-head .section-sub");
    if(singleTitle)singleTitle.textContent="Singleplayer Library";
    if(singleSub)singleSub.textContent="Current showcase games first, established games next, and prototypes kept in a separate shelf.";

    const hostedTitle=document.querySelector("#view-multiplayer > .section-head .section-title");
    const hostedSub=document.querySelector("#view-multiplayer > .section-head .section-sub");
    if(hostedTitle)hostedTitle.textContent="Hosted Multiplayer Lobbies";
    if(hostedSub)hostedSub.textContent="Open rooms are grouped by game quality tier; prototype lobbies remain available but stay out of the way by default.";

    const singleGrid=document.getElementById("singleGameGrid");
    if(singleGrid&&!document.getElementById("catalogLibraryNote")){
      const note=document.createElement("div");
      note.id="catalogLibraryNote";
      note.className="catalog-library-note";
      note.textContent="Prototype games are preserved, not deleted. Use Show prototypes when you want to browse experiments and incomplete builds.";
      singleGrid.before(note);
    }
  }

  function wireSearchExpansion(input,grids){
    if(!input)return;
    input.addEventListener("input",()=>{
      const open=!!input.value.trim();
      grids.forEach(grid=>{if(grid&&open)grid.dataset.prototypesOpen="true";});
    });
  }

  function boot(){
    injectStyles();
    setCopy();

    const singleGrid=document.getElementById("singleGameGrid");
    const lobbyGrid=document.getElementById("lobbyGrid");
    const onlineGrid=document.getElementById("onlineWorldGrid");

    observeGrid(singleGrid,()=>organizeGrid(singleGrid,".game-card",card=>{decorateGameCard(card);return card.dataset.catalogStatus;},{includeTools:true}));
    observeGrid(lobbyGrid,()=>organizeGrid(lobbyGrid,".lobby-card",decorateLobbyCard));
    observeGrid(onlineGrid,()=>organizeGrid(onlineGrid,".online-world-card",decorateOnlineCard));

    ["continueStrip","favoritesStrip","profileFavorites"].forEach(id=>{
      const node=document.getElementById(id);
      if(!node)return;
      const decorate=()=>node.querySelectorAll(".game-card").forEach(decorateGameCard);
      new MutationObserver(decorate).observe(node,{childList:true,subtree:true});
      decorate();
    });

    observeSelect(document.getElementById("newLobbyGame"),{keepAll:false});
    observeSelect(document.getElementById("lobbyGameFilter"),{keepAll:true});

    wireSearchExpansion(document.getElementById("gameSearch"),[singleGrid]);
    wireSearchExpansion(document.getElementById("lobbySearch"),[lobbyGrid]);
  }

  if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",boot,{once:true});
  else boot();
})();
