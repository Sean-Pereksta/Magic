(function (root) {
  'use strict';

  // One editorial configuration; the existing registry continues to own gameplay.
  const config = root.GameHubShowcase || (typeof require === 'function' ? require('./lobby-showcase.js') : {flagships:[],premium:[]});
  const PICKS = config.flagships;
  const MARKER = '<!-- catnmice-flagships:v1 -->';
  const STARTUP = 'populateStaticUI();renderFeatured();renderOnlineWorlds();renderAllGames();renderCurrentLobby();renderInvites();updateAccountUI();showView(read(SK.lastView)||"multiplayer");';
  const BACKGROUND = `<div class="cnm-ambient" aria-hidden="true"><svg class="cnm-pathways" viewBox="0 0 1440 900" preserveAspectRatio="none"><path d="M0 180H115Q140 180 140 205V350Q140 375 165 375H460M1440 80H1270Q1240 80 1240 110V270H1120M0 760H200Q225 760 225 735V650H500M1440 710H1300V835H850"/><circle cx="140" cy="375" r="4"/><circle cx="1240" cy="110" r="4"/><circle cx="225" cy="650" r="4"/></svg>
    <svg width="0" height="0" class="cnm-sprite-defs"><defs><symbol id="cnm-mouse" viewBox="0 0 48 32"><path d="M14 20C1 16 0 28 8 27" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"/><ellipse cx="27" cy="21" rx="13" ry="8" fill="currentColor"/><circle cx="26" cy="10" r="7" fill="currentColor"/><circle cx="26" cy="10" r="3.5" fill="#0a1424"/><path d="m36 16 9 6-10 5" fill="currentColor"/><circle cx="36" cy="19" r="1.7" fill="#081020"/><path d="m22 26-4 4m14-4 4 4" stroke="currentColor" stroke-width="3"/></symbol><symbol id="cnm-cat" viewBox="0 0 64 40"><path d="M16 26C2 21 2 6 11 5" fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round"/><ellipse cx="32" cy="26" rx="18" ry="9" fill="currentColor"/><path d="m42 22-1-17 8 7 9-4 1 17-8 7Z" fill="currentColor"/><path d="m23 31-7 6m22-6 7 6" stroke="currentColor" stroke-width="4"/><circle cx="53" cy="19" r="1.8" fill="#081020"/></symbol></defs></svg>
    <span class="cnm-chase cnm-chase-a"><svg class="cnm-runner cnm-mouse"><use href="#cnm-mouse"/></svg><svg class="cnm-runner cnm-cat"><use href="#cnm-cat"/></svg></span><span class="cnm-chase cnm-chase-b"><svg class="cnm-runner cnm-mouse"><use href="#cnm-mouse"/></svg><svg class="cnm-runner cnm-cat"><use href="#cnm-cat"/></svg></span><span class="cnm-chase cnm-chase-c"><svg class="cnm-runner cnm-mouse"><use href="#cnm-mouse"/></svg><svg class="cnm-runner cnm-cat"><use href="#cnm-cat"/></svg></span></div>`;
  const HOME = `${BACKGROUND}<section id="view-discover" class="view" aria-labelledby="cnmDiscoverTitle">
    <div class="cnm-intro"><div><p class="cnm-kicker"><span class="cnm-spark" aria-hidden="true">✦</span> INDEPENDENT WORLDS. ENDLESS POSSIBILITIES.</p><h1 id="cnmDiscoverTitle">Find your next <em>obsession.</em></h1><p>Big adventures, brilliant little games, and friends along for the ride.</p></div><form id="cnmSearchForm" class="cnm-search" role="search"><label for="cnmSearch">What will you play today?</label><div><span aria-hidden="true">⌕</span><input id="cnmSearch" type="search" placeholder="Find your next game…" maxlength="200"><button class="btn" type="submit">Search</button></div></form></div>
    <div class="cnm-section-head cnm-flagship-heading"><div><p class="cnm-kicker">THE MAIN EVENT</p><h2>Flagship worlds</h2></div><span class="cnm-section-note">Four worlds. All yours to explore.</span></div>
    <div id="cnmFlagshipGrid" class="cnm-flagship-grid" aria-label="Our four flagship games"></div>
    <section class="cnm-quick" aria-labelledby="cnmQuickTitle"><h2 id="cnmQuickTitle" class="cnm-kicker">JUMP RIGHT IN</h2><div class="cnm-quick-grid"><button type="button" data-quick="recent"><span class="cnm-quick-icon" aria-hidden="true">↺</span><span><strong>Recently played</strong><small>Your next round awaits</small></span><span aria-hidden="true">↗</span></button><button type="button" data-view="multiplayer"><span class="cnm-quick-icon" aria-hidden="true">♧</span><span><strong>Find multiplayer</strong><small>Find your people</small></span><span aria-hidden="true">↗</span></button><button type="button" data-quick="solo"><span class="cnm-quick-icon" aria-hidden="true">▷</span><span><strong>Play solo</strong><small>Go on an adventure</small></span><span aria-hidden="true">↗</span></button><button type="button" data-quick="favorites"><span class="cnm-quick-icon" aria-hidden="true">☆</span><span><strong>Your favorites</strong><small>Keep the good ones close</small></span><span aria-hidden="true">↗</span></button></div></section>
    <section id="cnmPremium" aria-labelledby="cnmPremiumTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">WORTH YOUR NEXT ROUND</p><h2 id="cnmPremiumTitle">Top arcade experiences</h2></div><button class="btn secondary small" type="button" data-view="singleplayer">All games ↗</button></div><div id="cnmPremiumGrid" class="cnm-premium-grid"></div></section>
    <section id="cnmBuiltOut" aria-labelledby="cnmBuiltOutTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">MORE OF OUR MOST COMPLETE GAMES</p><h2 id="cnmBuiltOutTitle">More built-out worlds</h2></div><button class="btn secondary small" type="button" data-view="singleplayer">Best games &amp; library ↗</button></div><div id="cnmBuiltOutGrid" class="cnm-premium-grid"></div></section>
    <section id="cnmRecent" hidden aria-labelledby="cnmRecentTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">BACK FOR MORE</p><h2 id="cnmRecentTitle">Recently played</h2></div><button class="btn secondary small" type="button" data-view="mygames">My games ↗</button></div><div id="cnmRecentGrid" class="cnm-recent-grid"></div></section>
    <section aria-labelledby="cnmLiveTitle"><div class="cnm-section-head"><div><p class="cnm-kicker cnm-live-label">PLAY TOGETHER</p><h2 id="cnmLiveTitle">The multiplayer corner</h2></div><button class="btn secondary small" type="button" data-view="multiplayer">Browse lobbies ↗</button></div><div id="cnmLiveGrid" class="cnm-live-grid" aria-live="polite"></div></section>
    <aside id="cnmProfileOffer" class="cnm-profile-offer"><div><p class="cnm-kicker">YOUR PLACE IN THE ARCADE</p><strong>Make yourself at home.</strong><p>Create an optional profile for favorites and multiplayer. No email required.</p></div><button id="cnmCreateProfile" class="btn secondary" type="button">Create profile ↗</button></aside>
    <section id="cnmMore" aria-labelledby="cnmMoreTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">TAKE THE SCENIC ROUTE</p><h2 id="cnmMoreTitle">More to explore</h2></div><button class="btn secondary small" type="button" data-view="singleplayer">Explore the library ↗</button></div><div id="cnmMoreMount"></div></section>
    <div class="cnm-library-invite"><span class="cnm-library-symbol" aria-hidden="true">✦</span><div><p class="cnm-kicker">SOMETHING FOR EVERY MOOD</p><h2>A whole arcade in your browser.</h2><p><span id="cnmCatalogCount"></span> games, worlds, experiments, and useful tools. Find something you love.</p></div><button type="button" class="btn" data-view="singleplayer">Explore all games ↗</button></div>
    <footer class="cnm-footer"><span>CAT’N MICE <b>ARCADE NEXUS</b></span><span>Small games. Big worlds.</span></footer>
  </section>
  <section id="view-mygames" class="view" aria-labelledby="cnmMyGamesTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">YOUR PERSONAL ARCADE</p><h1 id="cnmMyGamesTitle">My games</h1><p class="section-sub">Your favorites and recent adventures, all together.</p></div><button type="button" class="btn secondary" data-view="singleplayer">Find a new favorite ↗</button></div><section id="cnmMyRecent" tabindex="-1" aria-labelledby="cnmMyRecentTitle"><h2 id="cnmMyRecentTitle">Recently played</h2><p class="section-sub">Open a game again. Saved progress is managed by each game.</p><div id="cnmMyRecentGrid" class="game-grid"></div></section><section id="cnmMyFavorites" tabindex="-1" aria-labelledby="cnmMyFavoritesTitle"><h2 id="cnmMyFavoritesTitle">Your favorites</h2><div id="cnmMyFavoritesGrid" class="game-grid"></div></section></section>`;

  function replaceOnce(html, before, after, label) {
    if (html.split(before).length !== 2) throw new Error(`Flagship layout: missing or ambiguous ${label}.`);
    return html.replace(before, () => after);
  }
  function initialView(saved, invitation) {
    if (invitation) return 'multiplayer';
    return ['discover','singleplayer','multiplayer','mygames','profile'].includes(saved) ? saved : 'discover';
  }
  function selectPicks(games, picks = PICKS) {
    const byKey = new Map(games.map(game => [game.key, game]));
    return picks.filter(pick => byKey.has(pick.key)).map(pick => ({...pick, game:byKey.get(pick.key)}));
  }
  function recentKeys(recent, games, limit = 4) {
    const known = new Set(games.map(game => game.key));
    return [...new Set((Array.isArray(recent) ? recent : []).map(entry => typeof entry === 'string' ? entry : entry?.key))].filter(key => known.has(key)).slice(0,limit);
  }
  function upgradeHtml(input) {
    if (typeof input !== 'string') throw new TypeError('Flagship layout requires HTML text.');
    if (input.includes(MARKER)) return input;
    if (!PICKS.length) throw new Error('Flagship configuration unavailable.');
    let html = input;
    html = replaceOnce(html, '</head>', `${MARKER}\n<link rel="stylesheet" href="./lobby-flagships.css?v=2">\n</head>`, 'head');
    const mainAnchor = html.includes('<main class="shell" id="mainContent" tabindex="-1">') ? '<main class="shell" id="mainContent" tabindex="-1">' : '<main class="shell">';
    html = replaceOnce(html, mainAnchor, '<main class="shell" id="mainContent" tabindex="-1">\n'+HOME, 'main');
    html = replaceOnce(html, '<nav class="main-nav" aria-label="Primary navigation">', '<nav class="main-nav" aria-label="Primary navigation"><button class="nav-btn" type="button" data-view="discover">✦ Discover</button><button class="nav-btn" type="button" data-view="mygames">☆ My games</button>', 'desktop navigation');
    html = replaceOnce(html, '<nav class="mobile-nav" aria-label="Mobile navigation">', '<nav class="mobile-nav" aria-label="Mobile navigation"><button type="button" data-view="discover"><span aria-hidden="true">✦</span>Discover</button><button type="button" data-view="mygames"><span aria-hidden="true">☆</span>My games</button>', 'mobile navigation');
    html = replaceOnce(html, '<section id="view-singleplayer" class="view">', '<section id="view-singleplayer" class="view"><section id="cnmLibraryPicks" class="cnm-library-picks" aria-label="Quick play flagship games"><div class="cnm-section-head"><h2>Flagship quick play</h2><button class="btn secondary small" type="button" data-view="discover">Explore the flagships</button></div><div id="cnmLibraryGrid" class="cnm-library-grid"></div></section>', 'library');
    html = replaceOnce(html, 'function updateAccountUI(){', 'function updateAccountUI(){\n      globalThis.GameHubFlagships.refresh();', 'account state hook');
    html = replaceOnce(html, 'function renderAllGames(){renderSingleLibrary();}', 'function renderAllGames(){renderSingleLibrary();globalThis.GameHubFlagships.refresh();}', 'render hook');
    html = replaceOnce(html, 'function renderSingleLibrary(){', 'function renderSingleLibrary(){\n      globalThis.GameHubFlagships.filterLibrary(!!normalize($("gameSearch").value)||selectedCategory!=="All"||!!$("gameMode")?.value&&$("gameMode").value!=="all");', 'filter hook');
    html = replaceOnce(html, 'save(SK.lastView,view);', 'save(SK.lastView,view);\n      globalThis.GameHubFlagships.viewChanged(view);', 'view hook');
    html = replaceOnce(html, 'function renderLobbyBrowser(){', 'function renderLobbyBrowser(){\n      globalThis.GameHubFlagships.refreshActivity();', 'activity hook');
    html = replaceOnce(html, 'closeModal();listenToCurrentLobby();toast("Lobby created.","good");', 'closeModal();showView("multiplayer");listenToCurrentLobby();globalThis.GameHubFlagships.revealLobby();toast("Lobby created.","good");', 'create-lobby success');
    html = replaceOnce(html, 'save(SK.lastLobby,id);save(SK.lastGame,currentLobbyData.gameType);listenToCurrentLobby();', 'save(SK.lastLobby,id);save(SK.lastGame,currentLobbyData.gameType);showView("multiplayer");listenToCurrentLobby();globalThis.GameHubFlagships.revealLobby();', 'join-lobby success');
    const setup = `globalThis.GameHubFlagships.mount({
      document, games:GAMES, createCard:game=>gameCard(game),
      state:()=>({favorites,gamePlayCounts,recentGames,username}),
      browse:(query,mode="all")=>{selectedCategory="All";$("gameSearch").value=query;$("gameMode").value=mode;document.querySelectorAll("#categoryPills button").forEach(button=>button.classList.toggle("active",button.textContent.trim()==="All"));renderSingleLibrary();showView("singleplayer");$("gameSearch").focus();$("gameSearch").scrollIntoView({block:"start"});},
      myGames:section=>{showView("mygames");const target=$(section);target.focus({preventScroll:true});target.scrollIntoView({block:"start"});},
      lobbies:()=>[...lobbyCache.values()].filter(l=>l.status==="waiting"&&Array.isArray(l.players)&&l.players.length&&visibleLobby(l)&&getLaunchMode(getGame(l.gameType))===MULTIPLAYER_LAUNCH_MODES.LOBBY).sort((a,b)=>(b.createdAt?.toMillis?.()||b.createdAt||0)-(a.createdAt?.toMillis?.()||a.createdAt||0)).slice(0,3),
      connection:()=>({ready:firebaseAuthReady,error:firebaseAuthError}),createLobbyCard:lobby=>lobbyCard(lobby),
      findLobby:key=>{$("lobbyGameFilter").value=key;$("lobbySearch").value="";$("lobbyVisibilityFilter").value="all";$("lobbySlotFilter").value="open";renderLobbyBrowser();showView("multiplayer");$("lobbySearch").focus();$("lobbySearch").scrollIntoView({block:"start"});},
      createProfile:()=>openAccount("create")
    });\n    `;
    html = replaceOnce(html, STARTUP, setup+STARTUP.replace('read(SK.lastView)||"multiplayer"', 'globalThis.GameHubFlagships.initialView(read(SK.lastView),initialParams.has("lobby"))'), 'startup');
    html = replaceOnce(html, '    showFirstVisitProfilePrompt();', '    // Optional signup is an inline invitation; required account gates are unchanged.', 'optional signup');
    return html;
  }

  let context = null;
  const cardMaps = new Map();
  const signatures = new Map();
  function syncCard(card, game, state) {
    const favorite = state.favorites.has(game.key), button = card.querySelector('.favorite-btn');
    if (button) {button.classList.toggle('active',favorite);button.setAttribute('aria-pressed',String(favorite));button.textContent=favorite?'★':'☆';}
    const count = card.querySelector('.play-count');
    if(count)count.textContent=`${Number(state.gamePlayCounts[game.key])||0} plays`;
  }
  function featureCard(pick, type) {
    const card=context.createCard(pick.game);
    card.classList.add('cnm-pick');card.style.setProperty('--cnm-accent',pick.color);
    if(type==='flagship'&&pick.key===PICKS[0]?.key)card.classList.add('cnm-hero');
    if(type==='compact')card.classList.add('cnm-compact');
    if(type==='premium')card.classList.add('cnm-premium');
    const label=context.document.createElement('p');label.className='cnm-card-kicker';label.textContent=pick.label;
    card.querySelector('.game-content').prepend(label);
    const meta=card.querySelector('.game-meta');
    if(pick.categoryLabel&&meta?.firstElementChild)meta.firstElementChild.textContent=pick.categoryLabel;
    for(const text of Array.isArray(pick.badges)?pick.badges:[]){const badge=context.document.createElement('span');badge.className='tag cnm-editorial-badge';badge.textContent=String(text);meta?.appendChild(badge);}
    if(pick.pitch)card.querySelector('.game-description').textContent=pick.pitch;
    const single=card.querySelector('[data-game-action="single"]');
    if(single){single.textContent=pick.game.modes.includes('multi')?'Play solo':'Play now';single.setAttribute('aria-label',`${single.textContent}: ${pick.game.title}`);single.classList.remove('secondary');}
    if(type==='flagship'&&root.GameHubLibrary.getActions(pick.game).some(action=>action.type==='lobby')){
      const find=context.document.createElement('button');find.className='btn secondary small cnm-find-lobby';find.type='button';find.textContent='Find lobby';find.setAttribute('aria-label',`Find a lobby for ${pick.game.title}`);find.addEventListener('click',()=>context.findLobby(pick.key));card.querySelector('.game-actions').appendChild(find);
    }
    const image=card.querySelector('.game-art img');
    if(image&&type==='flagship'){
      image.loading='eager';image.sizes=pick.key===PICKS[0]?.key?'(max-width:900px) 92vw, 48vw':'(max-width:520px) 28vw, 24vw';
      const hero=root.GameHubArtwork?.image(pick.game,'hero');if(hero){image.removeAttribute('srcset');image.src=hero;}
      if(pick.key===PICKS[0]?.key)image.setAttribute('fetchpriority','high');
    }
    return card;
  }
  function renderShelf(id, picks, type, state) {
    const grid=context.document.getElementById(id);if(!grid)return;
    if(!cardMaps.has(id))cardMaps.set(id,new Map());const cards=cardMaps.get(id);
    for(const pick of selectPicks(context.games,picks)){
      if(!cards.has(pick.key)){const card=featureCard(pick,type);cards.set(pick.key,card);grid.appendChild(card);}
      syncCard(cards.get(pick.key),pick.game,state);
    }
  }
  function renderPersonal(id,keys,state,empty) {
    const grid=context.document.getElementById(id);if(!grid)return;
    const signature=keys.join('|');
    if(signatures.get(id)!==signature){signatures.set(id,signature);grid.replaceChildren(...keys.map(key=>context.createCard(context.games.find(game=>game.key===key))));if(!keys.length&&empty){const node=context.document.createElement('p');node.className='empty-state';node.textContent=empty;grid.appendChild(node);}}
    for(const card of grid.querySelectorAll('.game-card')){const game=context.games.find(game=>game.key===card.dataset.gameKey);if(game)syncCard(card,game,state);}
  }
  function refresh() {
    if(!context)return;
    const state=context.state();
    renderShelf('cnmFlagshipGrid',PICKS,'flagship',state);
    // Build secondary views when opened so hidden rows do not load artwork.
    if(context.document.getElementById('view-singleplayer').classList.contains('active'))renderShelf('cnmLibraryGrid',PICKS,'compact',state);
    renderShelf('cnmPremiumGrid',config.premium,'premium',state);
    renderShelf('cnmBuiltOutGrid',config.builtOut||[],'premium',state);
    context.document.getElementById('cnmBuiltOut').hidden=!config.builtOut?.length;
    const keys=recentKeys(state.recentGames,context.games);
    renderPersonal('cnmRecentGrid',keys,state);
    context.document.getElementById('cnmRecent').hidden=!keys.length;
    context.document.getElementById('cnmProfileOffer').hidden=!!state.username;
    if(context.document.getElementById('view-mygames').classList.contains('active'))refreshMyGames(state);
  }
  function refreshMyGames(state=context.state()) {
    renderPersonal('cnmMyRecentGrid',recentKeys(state.recentGames,context.games,12),state,'Play a game to start your recent history.');
    const known=new Set(context.games.map(game=>game.key));
    renderPersonal('cnmMyFavoritesGrid',[...state.favorites].filter(key=>known.has(key)),state,'Use the star on any game to save a favorite.');
  }
  function refreshActivity() {
    if(!context)return;
    const grid=context.document.getElementById('cnmLiveGrid'),lobbies=context.lobbies();
    const signature=JSON.stringify(lobbies)+'|'+JSON.stringify(context.connection())+'|'+context.state().username;
    if(signatures.get('live')===signature)return;signatures.set('live',signature);
    grid.replaceChildren(...lobbies.map(lobby=>context.createLobbyCard(lobby)));
    if(!lobbies.length){const empty=context.document.createElement('div');empty.className='cnm-live-empty';const status=context.connection();empty.textContent=status.error?'Multiplayer is temporarily unavailable. Open Multiplayer to retry.':status.ready?'No waiting lobbies right now. Start one in Multiplayer, or explore a shared world.':'Connecting to multiplayer…';grid.appendChild(empty);}
  }
  function filterLibrary(active) {root.document?.body?.classList.toggle('cnm-filtered',!!active);}
  function viewChanged(view) {
    if(!context)return;
    const doc=context.document;doc.body.dataset.cnmView=view;
    for(const button of doc.querySelectorAll('.main-nav [data-view],.mobile-nav [data-view]')){if(button.dataset.view===view)button.setAttribute('aria-current','page');else button.removeAttribute('aria-current');}
    if(view==='singleplayer')renderShelf('cnmLibraryGrid',PICKS,'compact',context.state());
    if(view==='mygames')refreshMyGames();
    if(view==='discover')refreshActivity();
  }
  function revealLobby() {const panel=context?.document.getElementById('currentLobbyPanel');if(!panel)return;panel.tabIndex=-1;panel.focus({preventScroll:true});panel.scrollIntoView({block:'start',behavior:'auto'});}
  function mount(hooks) {
    if(context)return;
    context=hooks;const doc=hooks.document;doc.body.classList.add('cnm-layout');
    const hero=doc.getElementById('featuredHero');if(hero)doc.getElementById('cnmMoreMount').appendChild(hero);
    const library=doc.getElementById('view-singleplayer'),picks=doc.getElementById('cnmLibraryPicks');
    const heading=[...library.children].find(node=>node.classList.contains('section-head')),toolbar=doc.getElementById('gameSearch').closest('.toolbar'),categories=doc.getElementById('categoryPills');
    if(heading&&toolbar&&categories){heading.after(toolbar);toolbar.after(categories);categories.after(picks);}
    // Navigation controls are shared by desktop and mobile, retaining saved IDs.
    for(const nav of doc.querySelectorAll('.main-nav,.mobile-nav'))for(const view of ['discover','singleplayer','multiplayer','mygames','profile']){const button=nav.querySelector(`[data-view="${view}"]`);if(button)nav.appendChild(button);}
    doc.querySelectorAll('.main-nav [data-view="singleplayer"],.mobile-nav [data-view="singleplayer"]').forEach(button=>{const icon=button.querySelector('span');if(icon){button.lastChild.textContent='All games';}else button.textContent='▦ All games';});
    doc.getElementById('cnmCatalogCount').textContent=String(hooks.games.length);
    doc.getElementById('cnmSearchForm').addEventListener('submit',event=>{event.preventDefault();hooks.browse(doc.getElementById('cnmSearch').value.trim());});
    doc.getElementById('cnmSearch').addEventListener('input',showSuggestions);
    doc.getElementById('cnmCreateProfile').addEventListener('click',hooks.createProfile);
    doc.querySelectorAll('[data-quick]').forEach(button=>button.addEventListener('click',()=>button.dataset.quick==='solo'?hooks.browse('','single'):hooks.myGames(button.dataset.quick==='recent'?'cnmMyRecent':'cnmMyFavorites')));
    const pause=()=>doc.body.classList.toggle('cnm-paused',doc.hidden);
    doc.addEventListener('visibilitychange',pause);pause();
    if(root.navigator?.connection?.saveData||root.navigator?.hardwareConcurrency&&root.navigator.hardwareConcurrency<=2)doc.body.classList.add('cnm-low-power');
    refresh();refreshActivity();
  }
  function showSuggestions() {
    const doc=context.document,input=doc.getElementById('cnmSearch'),query=input.value.trim().toLowerCase();
    let panel=doc.getElementById('cnmSearchResults');
    if(!panel){panel=doc.createElement('div');panel.id='cnmSearchResults';panel.className='cnm-search-results';panel.setAttribute('aria-label','Matching games');doc.getElementById('cnmSearchForm').appendChild(panel);input.setAttribute('aria-controls',panel.id);input.addEventListener('keydown',event=>{if(event.key==='Escape'){panel.hidden=true;input.setAttribute('aria-expanded','false');}});doc.addEventListener('click',event=>{if(!event.target.closest('#cnmSearchForm')){panel.hidden=true;input.setAttribute('aria-expanded','false');}});}
    panel.replaceChildren();panel.hidden=!query;input.setAttribute('aria-expanded',String(!!query));if(!query)return;
    const matches=context.games.filter(game=>`${game.title} ${game.desc} ${game.category}`.toLowerCase().includes(query)).slice(0,5);
    for(const game of matches){const button=doc.createElement('button');button.type='button';button.className='cnm-search-result';if(root.GameHubArtwork){const img=doc.createElement('img');img.src=root.GameHubArtwork.image(game,'thumb');img.alt='';img.width=60;img.height=38;img.onerror=()=>{img.hidden=true;};button.appendChild(img);}const text=doc.createElement('span');text.textContent=game.title;button.appendChild(text);button.addEventListener('click',()=>{panel.hidden=true;input.setAttribute('aria-expanded','false');context.browse(game.title);});panel.appendChild(button);}
    if(!matches.length){const empty=doc.createElement('p');empty.textContent='No matching games. Try a category or another title.';panel.appendChild(empty);}
  }
  const api=Object.freeze({PICKS,MARKER,STARTUP,upgradeHtml,initialView,selectPicks,recentKeys,mount,refresh,refreshActivity,filterLibrary,viewChanged,revealLobby});
  if(typeof module==='object'&&module.exports)module.exports=api;else root.GameHubFlagships=api;
})(globalThis);
