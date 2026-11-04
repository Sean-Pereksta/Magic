(function(root){
  'use strict';
  const MARKER='<!-- catnmice-arcade:v1 -->';
  const paths={search:'m21 21-4.6-4.6 M19 11a8 8 0 1 1-16 0 8 8 0 0 1 16 0',star:'m12 3 2.8 5.8 6.4.9-4.6 4.5 1.1 6.3-5.7-3-5.7 3 1.1-6.3-4.6-4.5 6.4-.9Z',grid:'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',people:'M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2 M16 3a4 4 0 0 1 0 8 M22 21v-2a4 4 0 0 0-3-3.9 M13 7a4 4 0 1 1-8 0 4 4 0 0 1 8 0',user:'M20 21v-2a7 7 0 0 0-14 0v2 M17 7a4 4 0 1 1-8 0 4 4 0 0 1 8 0',compass:'M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0 M16 8l-3 5-5 3 3-5Z',arrow:'M5 12h14 m-5-5 5 5-5 5',bolt:'m13 2-9 12h7l-1 8 10-12h-7Z',shield:'m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6Z',sword:'m14 3 7 0 0 7-12 12-7-7Z M3 21l4-4 M4 11l9 9',leaf:'M20 3C8 2 2 8 5 16c8 5 15-1 15-13Z M4 21 16 8',tool:'M14 6a5 5 0 0 0-6 6L2 18l4 4 6-6a5 5 0 0 0 6-6l-4 3-3-3Z',mail:'M3 5h18v14H3z m0 0 9 7 9-7',clock:'M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0 M12 7v5l3 2',game:'M8 7h8c4 0 7 12 3 12l-4-3H9l-4 3C1 19 4 7 8 7Z M7 10v5 M4.5 12.5h5 M16 11h.01 M18 14h.01'};
  function icon(name){return `<svg class="arcade-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="${paths[name]||paths.game}"/></svg>`;}
  const HOME=`<section id="view-discover" class="view" aria-labelledby="cnmDiscoverTitle">
    <div class="arcade-welcome"><picture class="arcade-universe"><source media="(max-width:600px)" srcset="./assets/arcade-hub/arcade-universe-mobile.webp"><img src="./assets/arcade-hub/arcade-universe.webp" srcset="./assets/arcade-hub/arcade-universe-small.webp 1200w, ./assets/arcade-hub/arcade-universe.webp 1920w" sizes="(max-width:1280px) 100vw, 1280px" width="1920" height="640" alt="" fetchpriority="high" decoding="async"></picture><div class="arcade-welcome-copy"><p class="cnm-kicker">WELCOME TO YOUR NEXT ADVENTURE</p><h1 id="cnmDiscoverTitle">A little curiosity.<br><em>A whole lot of play.</em></h1><p>Discover independent games, explore new worlds,<br class="desktop-break"> and bring your friends along.</p><form id="cnmSearchForm" class="cnm-search" role="search"><label for="cnmSearch">Find your next game</label><div>${icon('search')}<input id="cnmSearch" type="search" placeholder="Search games, worlds, and more…" maxlength="200"><button type="submit" class="btn">Search</button></div></form></div></div>
    <section id="cnmRecent" hidden aria-labelledby="cnmRecentTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">PICK UP WHERE YOU LEFT OFF</p><h2 id="cnmRecentTitle">Continue playing</h2></div><button class="btn secondary small" data-view="mygames" type="button">My games ${icon('arrow')}</button></div><div id="cnmRecentGrid" class="cnm-recent-grid"></div></section>
    <section aria-labelledby="cnmFlagshipTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">HANDPICKED FOR YOUR NEXT ROUND</p><h2 id="cnmFlagshipTitle">Meet the flagships</h2></div><button class="arcade-text-link" data-view="singleplayer" type="button">All games ${icon('arrow')}</button></div><div id="cnmFlagshipGrid" class="cnm-flagship-grid" aria-label="Our four flagship games"></div></section>
    <section class="cnm-quick" aria-labelledby="cnmQuickTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">THERE’S A WORLD FOR EVERY MOOD</p><h2 id="cnmQuickTitle">Play your way</h2></div></div><div class="arcade-collections"><button type="button" data-quick="solo">${icon('compass')}<span><strong>Solo adventures</strong><small>Your world. Your pace.</small></span>${icon('arrow')}</button><button type="button" data-view="multiplayer">${icon('people')}<span><strong>Better together</strong><small>Find a lobby or an online world.</small></span>${icon('arrow')}</button><button type="button" id="arcadePopular">${icon('bolt')}<span><strong>One more round</strong><small>Explore the most played games.</small></span>${icon('arrow')}</button></div></section>
    <aside id="cnmProfileOffer" class="cnm-profile-offer"><img src="./assets/catnmice-mark.svg" width="50" height="50" alt=""><div><strong>Make yourself at home.</strong><p>An optional profile keeps favorites close and lets you play together. No email required.</p></div><button id="cnmCreateProfile" class="btn secondary" type="button">Create profile ${icon('arrow')}</button></aside>
    <details id="cnmMore" class="arcade-more"><summary>More from the arcade <span>Staff picks, new adventures, and the spotlight</span></summary><section id="cnmPremium" aria-labelledby="cnmPremiumTitle"><div class="cnm-section-head"><h2 id="cnmPremiumTitle">Staff picks</h2></div><div id="cnmPremiumGrid" class="cnm-premium-grid"></div></section><section id="cnmBuiltOut" aria-labelledby="cnmBuiltOutTitle"><div class="cnm-section-head"><h2 id="cnmBuiltOutTitle">More complete worlds</h2></div><div id="cnmBuiltOutGrid" class="cnm-premium-grid"></div></section><section aria-label="Game spotlight"><div class="cnm-section-head"><h2>In the spotlight</h2></div><div id="cnmMoreMount"></div></section></details>
    <section class="arcade-live-section" aria-labelledby="cnmLiveTitle"><div class="cnm-section-head"><h2 id="cnmLiveTitle">Around the arcade</h2><button class="arcade-text-link" type="button" data-view="multiplayer">Multiplayer hub ${icon('arrow')}</button></div><div id="cnmLiveGrid" class="cnm-live-grid" aria-live="polite"></div></section>
    <div class="cnm-library-invite"><div><h2>Follow your curiosity.</h2><p><span id="cnmCatalogCount"></span> games, worlds, experiments, and tools. All in your browser.</p></div><button type="button" class="btn" data-view="singleplayer">Explore the library ${icon('arrow')}</button></div><footer class="cnm-footer"><span>CAT’N MICE <b>INDEPENDENT ARCADE</b></span><span>Small games. Big worlds.</span></footer>
  </section>`;
  function replaceOnce(html,before,after,label){if(html.split(before).length!==2)throw new Error(`Arcade layout: missing or ambiguous ${label}.`);return html.replace(before,()=>after);}
  function upgradeHtml(input){
    if(typeof input!=='string')throw new TypeError('Arcade layout requires HTML text.');
    if(input.includes(MARKER))return input;
    let html=input;
    const start=html.indexOf('<section id="view-discover"'),end=html.indexOf('<section id="view-mygames"',start);
    if(start<0||end<start)throw new Error('Arcade layout requires the shared hub.');
    html=html.slice(0,start)+HOME+'\n  '+html.slice(end);
    html=replaceOnce(html,'</head>',`${MARKER}<script src="./lobby-catalog-organization.js?v=5"></script><link rel="stylesheet" href="./lobby-arcade.css?v=1"><link rel="icon" type="image/svg+xml" href="./assets/catnmice-mark.svg"></head>`,'head');
    html=replaceOnce(html,'<div id="categoryPills" class="category-pills"></div>','<div id="categoryPills" class="category-pills" role="group" aria-label="Filter by genre"></div><div class="arcade-result-row"><p id="gameResultCount" role="status" aria-live="polite"></p><button id="clearGameFilters" class="arcade-text-link" type="button" hidden>Clear filters</button></div><div id="gameActiveFilters" class="arcade-active-filters" aria-label="Active filters"></div>','filters');
    html=replaceOnce(html,'<select id="gameMode"','<select id="gameStatus" class="select" aria-label="Filter development status"><option value="all">All development stages</option><option value="showcase">Complete</option><option value="playable">In production</option><option value="prototype">Experimental</option><option value="tools">Tools</option></select><select id="gameMode"','status control');
    html=replaceOnce(html,'let games=GAMES.filter(g=>','let games=GAMES.filter(g=>(!globalThis.GameHubCatalog||($("gameStatus")?.value||"all")==="all"||globalThis.GameHubCatalog.statusForTitle(g.title,g.key)===$("gameStatus").value)&&','status predicate');
    html=replaceOnce(html,'      libraryDirty=false;','      globalThis.GameHubArcade.libraryRendered({selectedCategory});\n      libraryDirty=false;','result hook');
    html=replaceOnce(html,'    globalThis.GameHubFlagships.mount({','    globalThis.GameHubArcade.mount({document,syncSpotlight:()=>startFeaturedTimer(),closeModal:()=>closeModal(),resetFilters:()=>{selectedCategory="All";$("gameSearch").value="";$("gameMode").value="all";$("gameStatus").value="all";populateCategoriesOnly();renderSingleLibrary();$("gameSearch").focus();},renderLibrary:()=>renderSingleLibrary(),popular:()=>{$("gameSort").value="popular";showView("singleplayer");renderSingleLibrary();}});\n    globalThis.GameHubFlagships.mount({','arcade setup');
    html=replaceOnce(html,'      if(featuredGames().length>1','      if(document.hidden||!$("cnmMore").open||!$("view-discover").classList.contains("active"))return;\n      if(featuredGames().length>1','spotlight visibility');
    html=replaceOnce(html,'browse:(query,mode="all")=>{selectedCategory="All";','browse:(query,mode="all")=>{$("gameStatus").value="all";selectedCategory="All";','browse reset');
    html=replaceOnce(html,'      globalThis.GameHubFlagships.viewChanged(view);','      globalThis.GameHubFlagships.viewChanged(view);globalThis.GameHubArcade.viewChanged(view);','view hook');
    return html;
  }
  let context=null,lastCategory='All';
  function emptyIllustration(kind){return `<svg class="arcade-empty-art" viewBox="0 0 120 80" fill="none" aria-hidden="true"><ellipse cx="60" cy="69" rx="42" ry="5" fill="#26324d"/><rect x="28" y="13" width="64" height="50" rx="13" fill="#18243c" stroke="#6375a3" stroke-width="2"/><path d="M35 21h50" stroke="#364967" stroke-width="2"/><g transform="translate(47 29) scale(1.1)" stroke="#a9a8ff" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="${paths[kind]||paths.game}"/></g><circle cx="17" cy="28" r="3" fill="#63bfcf"/><path d="m100 45 3 5 5 2-5 2-3 5-2-5-5-2 5-2Z" fill="#8a87de"/></svg>`;}
  function emptyState(node,{kind='game',title,description,action='Explore games',onAction}={}){
    const doc=context?.document||root.document;
    node.replaceChildren();node.classList.add('arcade-empty');node.innerHTML=emptyIllustration(kind);
    const heading=doc.createElement('strong');heading.textContent=title;node.appendChild(heading);
    const copy=doc.createElement('p');copy.textContent=description;node.appendChild(copy);
    const button=doc.createElement('button');button.className='btn secondary small';button.type='button';button.textContent=action;button.addEventListener('click',onAction||(()=>doc.querySelector('.main-nav [data-view="singleplayer"]').click()));node.appendChild(button);
  }
  function decorateCards(){
    if(!root.GameHubCatalog)return;
    const doc=context.document;
    doc.querySelectorAll('.game-card').forEach(card=>{
      const title=card.querySelector('.game-name');if(!title)return;
      const status=root.GameHubCatalog.statusForTitle(title.dataset.catalogRawTitle||title.textContent,card.dataset.gameKey);
      title.dataset.catalogRawTitle=title.dataset.catalogRawTitle||title.textContent;
      title.textContent=root.GameHubCatalog.cleanTitle(title.dataset.catalogRawTitle);
      title.setAttribute('role','heading');title.setAttribute('aria-level','3');
      card.dataset.catalogStatus=status;
      if(!card.querySelector('.catalog-status-badge')){const badge=doc.createElement('span');badge.className='tag catalog-status-badge';badge.dataset.status=status;badge.textContent=root.GameHubCatalog.statusMeta[status].short;card.querySelector('.game-meta')?.appendChild(badge);}
    });
  }
  function libraryRendered({selectedCategory=lastCategory}={}){
    lastCategory=selectedCategory;
    if(!context)return;
    const doc=context.document,grid=doc.getElementById('singleGameGrid'),cards=grid.querySelectorAll('.game-card');
    decorateCards();
    const active=[doc.getElementById('gameSearch').value.trim(),selectedCategory!=='All'?selectedCategory:'',doc.getElementById('gameMode').value!=='all'?doc.getElementById('gameMode').selectedOptions[0].textContent:'',doc.getElementById('gameStatus').value!=='all'?doc.getElementById('gameStatus').selectedOptions[0].textContent:''].filter(Boolean);
    doc.getElementById('gameResultCount').textContent=`${cards.length} ${cards.length===1?'result':'results'}${active.length?' · '+active.length+' active '+(active.length===1?'filter':'filters'):''}`;
    doc.getElementById('clearGameFilters').hidden=!active.length;
    const filters=doc.getElementById('gameActiveFilters');filters.replaceChildren();for(const value of active){const span=doc.createElement('span');span.className='tag';span.textContent=value;filters.appendChild(span);}
    if(!cards.length){const empty=grid.querySelector('.empty-state');if(empty)emptyState(empty,{kind:'search',title:'No games found',description:'Try another title, genre, or development stage.',action:'Clear filters',onAction:context.resetFilters});}
  }
  function viewChanged(view){
    if(!context)return;
    decorateCards();
    if(view==='singleplayer')libraryRendered();
    context.syncSpotlight();
    // Navigation remains in the same page; begin each new view below the shell.
    context.document.defaultView.scrollTo({top:0,behavior:'instant'});
  }
  function mount(hooks){
    if(context)return;context=hooks;const doc=hooks.document;doc.body.classList.add('arcade-hub');
    doc.querySelector('.brand-mark').innerHTML='<img src="./assets/catnmice-mark.svg" width="44" height="44" alt="">';
    const navIcons={discover:'compass',singleplayer:'grid',multiplayer:'people',mygames:'star',profile:'user'};
    for(const button of doc.querySelectorAll('.main-nav [data-view],.mobile-nav [data-view]')){const labels={discover:'Discover',singleplayer:'All games',multiplayer:'Multiplayer',mygames:'My games',profile:'Profile'};button.innerHTML=icon(navIcons[button.dataset.view])+`<span>${labels[button.dataset.view]}</span>`;button.type='button';}
    for(const [id,name,label]of [['inviteToggle','mail','Invites'],['memberToggle','people','Members'],['accountButton','user','Account']]){const button=doc.getElementById(id);button.childNodes[0].textContent='';button.insertAdjacentHTML('afterbegin',icon(name));button.setAttribute('aria-label',label);}
    for(const [id,panel]of [['inviteToggle','invitePanel'],['memberToggle','memberPanel']]){const button=doc.getElementById(id),node=doc.getElementById(panel);button.setAttribute('aria-controls',panel);const sync=()=>button.setAttribute('aria-expanded',String(!node.hidden));new MutationObserver(sync).observe(node,{attributes:true,attributeFilter:['hidden']});sync();}
    doc.addEventListener('keydown',event=>{if(event.key==='Escape'){for(const id of ['invitePanel','memberPanel']){const panel=doc.getElementById(id);if(!panel.hidden){panel.hidden=true;doc.getElementById(id==='invitePanel'?'inviteToggle':'memberToggle').focus();}}}});
    doc.getElementById('gameStatus').addEventListener('change',hooks.renderLibrary);doc.getElementById('clearGameFilters').addEventListener('click',hooks.resetFilters);doc.getElementById('arcadePopular').addEventListener('click',hooks.popular);
    doc.getElementById('cnmMore').addEventListener('toggle',hooks.syncSpotlight);
    if(!root.GameHubCatalog){const status=doc.getElementById('gameStatus');status.disabled=true;status.options[0].textContent='Development stages unavailable';}
    // Preserve the visitor counts in a supporting position without crowding play.
    doc.querySelector('.cnm-footer').appendChild(doc.getElementById('dailyUsersPill'));
    for(const title of doc.querySelectorAll('.view > .section-head .section-title')){title.setAttribute('role','heading');title.setAttribute('aria-level','1');}
    const layer=doc.getElementById('modalLayer');let opener=null,activeModal=null;
    for(const modal of layer.querySelectorAll(':scope > .modal')){modal.setAttribute('role','dialog');modal.setAttribute('aria-modal','true');const title=modal.querySelector('.modal-title');if(title){if(!title.id)title.id=modal.id+'Title';modal.setAttribute('aria-labelledby',title.id);}modal.querySelectorAll('[data-close-modal]').forEach(button=>{if(button.classList.contains('modal-close'))button.setAttribute('aria-label','Close dialog');});}
    const controls=modal=>[...modal.querySelectorAll('button,input,select,textarea,a[href],[tabindex="0"]')].filter(node=>!node.disabled&&node.getClientRects().length);
    new MutationObserver(()=>{const next=layer.hidden?null:layer.querySelector(':scope > .modal:not([hidden])');if(next===activeModal)return;if(next){if(!activeModal)opener=doc.activeElement;activeModal=next;queueMicrotask(()=>{if(!next.contains(doc.activeElement))controls(next)[0]?.focus();});}else{activeModal=null;if(opener?.isConnected&&opener.getClientRects().length)opener.focus();opener=null;}}).observe(layer,{attributes:true,subtree:true,attributeFilter:['hidden']});
    doc.addEventListener('keydown',event=>{if(!activeModal)return;if(event.key==='Escape'){event.preventDefault();event.stopImmediatePropagation();hooks.closeModal();}else if(event.key==='Tab'){const items=controls(activeModal),first=items[0],last=items.at(-1);if(event.shiftKey&&doc.activeElement===first){event.preventDefault();last?.focus();}else if(!event.shiftKey&&doc.activeElement===last){event.preventDefault();first?.focus();}}},true);
    const multi=doc.getElementById('view-multiplayer'),worlds=doc.getElementById('onlineWorldSection'),leaders=doc.getElementById('leaderboardEntries'),leaderHeading=leaders.previousElementSibling;
    const heading=multi.querySelector(':scope > .section-head');heading.querySelector('.section-title').textContent='Play together';heading.querySelector('.section-sub').textContent='Find your people. Choose a hosted lobby or enter a shared online world.';
    const choices=doc.createElement('div');choices.className='arcade-multiplayer-choices';choices.innerHTML=`<button type="button" data-multi-target="lobbySearch">${icon('people')}<span><strong>Find or create a lobby</strong><small>Gather a group and start together</small></span>${icon('arrow')}</button><button type="button" data-multi-target="onlineWorldSection">${icon('compass')}<span><strong>Enter an online world</strong><small>Jump into a persistent world or match</small></span>${icon('arrow')}</button>`;heading.after(choices);
    choices.querySelectorAll('button').forEach(button=>button.addEventListener('click',()=>{const target=doc.getElementById(button.dataset.multiTarget);if(target===worlds)target.tabIndex=-1;target.focus({preventScroll:true});target.scrollIntoView({block:'start',behavior:'instant'});}));
    const leaderboard=doc.createElement('details');leaderboard.className='arcade-more';leaderboard.innerHTML='<summary>Arcade leaderboard <span>See the current win leaders</span></summary>';leaderboard.append(leaderHeading,leaders);multi.appendChild(worlds);multi.appendChild(leaderboard);
    const grid=doc.getElementById('lobbyGrid');const decorateEmpty=()=>{const empty=grid.querySelector('.empty-state');if(empty&&!empty.classList.contains('arcade-empty')&&/No matching open lobbies/.test(empty.textContent)){emptyState(empty,{kind:'people',title:'There’s room for your next group',description:'No lobbies match right now. Create one or explore an online world.',action:'Create a lobby',onAction:()=>doc.getElementById('openCreateLobby').click()});const browse=doc.createElement('button');browse.type='button';browse.className='arcade-text-link';browse.textContent='Explore online worlds';browse.addEventListener('click',()=>worlds.scrollIntoView({block:'start'}));empty.appendChild(browse);}};new MutationObserver(decorateEmpty).observe(grid,{childList:true});decorateEmpty();
    doc.getElementById('categoryPills').addEventListener('click',()=>{for(const button of doc.querySelectorAll('#categoryPills button'))button.setAttribute('aria-pressed',String(button.classList.contains('active')));});
    const genreIcons={Strategy:'shield',Action:'bolt',RPG:'sword',Survival:'leaf',Arcade:'game',Apps:'tool',Adventure:'compass',All:'grid'};
    const decorateGenres=()=>{for(const button of doc.querySelectorAll('#categoryPills button')){button.setAttribute('aria-pressed',String(button.classList.contains('active')));if(!button.querySelector('svg'))button.insertAdjacentHTML('afterbegin',icon(genreIcons[button.textContent]||'game'));}};new MutationObserver(decorateGenres).observe(doc.getElementById('categoryPills'),{childList:true});decorateGenres();
    doc.querySelector('#profileGuest p').textContent='Create an optional profile to save favorites, join friends, and receive invites. Each game manages its own progress.';
  }
  const api=Object.freeze({MARKER,upgradeHtml,mount,libraryRendered,viewChanged,emptyState,icon});
  if(typeof module==='object'&&module.exports)module.exports=api;else root.GameHubArcade=api;
})(globalThis);
