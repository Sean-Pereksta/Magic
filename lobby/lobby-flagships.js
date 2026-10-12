(function (root) {
  'use strict';

  // Presentation only. Routes, account gates and actions belong to the hub.
  const PICKS = Object.freeze([
    {key:'apestogetherstrong', label:'The flagship adventure', pitch:'Rescue your tribe. Build your kingdom. Lead the uprising.', color:'#73d6af'},
    {key:'warrealms', label:'Build your winning strategy', pitch:'Craft your command deck and take on a Warbot or rival players.', color:'#eec578'},
    {key:'cat', label:'Bring your friends', pitch:'Build your defenses and face the chase together.', color:'#f59ba9'},
    {key:'ironthrone', label:'Your kingdom. Your decisions.', pitch:'Command armies, negotiate with rulers and shape your kingdom.', color:'#ada6ff'}
  ]);
  const MARKER = '<!-- catnmice-flagships:v1 -->';
  const STARTUP = 'populateStaticUI();renderFeatured();renderOnlineWorlds();renderAllGames();renderCurrentLobby();renderInvites();updateAccountUI();showView(read(SK.lastView)||"multiplayer");';
  const HOME = `<section id="view-discover" class="view" aria-labelledby="cnmDiscoverTitle">
    <div class="cnm-intro">
      <div><p class="cnm-kicker">CAT’N MICE / THE FLAGSHIP COLLECTION</p><h1 id="cnmDiscoverTitle">Your next great game.</h1><p>Four standout worlds. Pick a favorite and jump in.</p></div>
      <form id="cnmSearchForm" class="cnm-search" role="search"><label for="cnmSearch">Find a game</label><div><input id="cnmSearch" type="search" placeholder="Search the full library…" maxlength="200"><button class="btn" type="submit">Search</button></div></form>
    </div>
    <div id="cnmFlagshipGrid" class="cnm-flagship-grid" aria-label="Our four flagship games"></div>
    <div class="cnm-paths"><button class="btn secondary" type="button" data-view="singleplayer">Browse every game <span aria-hidden="true">↗</span></button><button class="btn secondary" type="button" data-view="multiplayer">Find a multiplayer lobby <span aria-hidden="true">↗</span></button><span>Solo adventures or games with friends. Your choice.</span></div>
    <section id="cnmRecent" hidden aria-labelledby="cnmRecentTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">PICK UP WHERE YOU LEFT OFF</p><h2 id="cnmRecentTitle">Recently played</h2></div><button class="btn secondary small" type="button" data-view="singleplayer">Recents &amp; favorites</button></div><div id="cnmRecentGrid" class="cnm-recent-grid"></div></section>
    <aside id="cnmProfileOffer" class="cnm-profile-offer"><div><strong>Make yourself at home.</strong><p>Create an optional profile for favorites and multiplayer. No email required.</p></div><button id="cnmCreateProfile" class="btn secondary" type="button">Create profile</button></aside>
    <section id="cnmMore" aria-labelledby="cnmMoreTitle"><div class="cnm-section-head"><div><p class="cnm-kicker">THERE’S MORE TO EXPLORE</p><h2 id="cnmMoreTitle">Around the library</h2></div><button class="btn secondary small" type="button" data-view="singleplayer">View all games</button></div><div id="cnmMoreMount"></div></section>
  </section>`;

  function replaceOnce(html, before, after, label) {
    if (html.split(before).length !== 2) throw new Error(`Flagship layout: missing or ambiguous ${label}.`);
    return html.replace(before, () => after);
  }

  function initialView(saved, invitation) {
    if (invitation) return 'multiplayer';
    return ['discover','singleplayer','multiplayer','profile'].includes(saved) ? saved : 'discover';
  }

  function selectPicks(games) {
    const byKey = new Map(games.map(game => [game.key, game]));
    return PICKS.filter(pick => byKey.has(pick.key)).map(pick => ({...pick, game:byKey.get(pick.key)}));
  }

  function recentKeys(recent, games) {
    const known = new Set(games.map(game => game.key));
    return [...new Set((Array.isArray(recent) ? recent : []).map(entry => typeof entry === 'string' ? entry : entry?.key))].filter(key => known.has(key)).slice(0,4);
  }

  function upgradeHtml(input) {
    if (typeof input !== 'string') throw new TypeError('Flagship layout requires HTML text.');
    if (input.includes(MARKER)) return input;
    let html = input;
    html = replaceOnce(html, '</head>', `${MARKER}\n<link rel="stylesheet" href="./lobby-flagships.css?v=1">\n</head>`, 'head');
    html = replaceOnce(html, '<main class="shell">', '<main class="shell">\n'+HOME, 'main');
    html = replaceOnce(html, '<nav class="main-nav" aria-label="Primary navigation">', '<nav class="main-nav" aria-label="Primary navigation"><button class="nav-btn" type="button" data-view="discover">✦ Discover</button>', 'desktop navigation');
    html = replaceOnce(html, '<nav class="mobile-nav" aria-label="Mobile navigation">', '<nav class="mobile-nav" aria-label="Mobile navigation"><button type="button" data-view="discover"><span aria-hidden="true">✦</span>Discover</button>', 'mobile navigation');
    html = replaceOnce(html, '<section id="view-singleplayer" class="view">', '<section id="view-singleplayer" class="view"><section id="cnmLibraryPicks" class="cnm-library-picks" aria-label="Quick play flagship games"><div class="cnm-section-head"><h2>Flagship quick play</h2><button class="btn secondary small" type="button" data-view="discover">Explore the flagships</button></div><div id="cnmLibraryGrid" class="cnm-library-grid"></div></section>', 'library');
    html = replaceOnce(html, 'function renderAllGames(){renderSingleLibrary();}', 'function renderAllGames(){renderSingleLibrary();globalThis.GameHubFlagships.refresh();}', 'render hook');
    html = replaceOnce(html, 'function renderSingleLibrary(){', 'function renderSingleLibrary(){\n      globalThis.GameHubFlagships.filterLibrary(!!normalize($("gameSearch").value)||selectedCategory!=="All");', 'filter hook');
    html = replaceOnce(html, 'save(SK.lastView,view);', 'save(SK.lastView,view);\n      globalThis.GameHubFlagships.viewChanged(view);', 'view hook');
    // Change views only after the existing successful writes. No automatic joins/starts.
    html = replaceOnce(html, 'closeModal();listenToCurrentLobby();toast("Lobby created.","good");', 'closeModal();showView("multiplayer");listenToCurrentLobby();globalThis.GameHubFlagships.revealLobby();toast("Lobby created.","good");', 'create-lobby success');
    html = replaceOnce(html, 'save(SK.lastLobby,id);save(SK.lastGame,currentLobbyData.gameType);listenToCurrentLobby();', 'save(SK.lastLobby,id);save(SK.lastGame,currentLobbyData.gameType);showView("multiplayer");listenToCurrentLobby();globalThis.GameHubFlagships.revealLobby();', 'join-lobby success');
    const setup = `globalThis.GameHubFlagships.mount({
      document, games:GAMES, createCard:game=>gameCard(game),
      state:()=>({favorites,gamePlayCounts,recentGames,username}),
      browse:query=>{selectedCategory="All";$("gameSearch").value=query;document.querySelectorAll("#categoryPills button").forEach(button=>button.classList.toggle("active",button.textContent.trim()==="All"));renderSingleLibrary();showView("singleplayer");$("gameSearch").focus();$("gameSearch").scrollIntoView({block:"start"});},
      findLobby:key=>{$("lobbyGameFilter").value=key;$("lobbySearch").value="";$("lobbyVisibilityFilter").value="all";$("lobbySlotFilter").value="open";renderLobbyBrowser();showView("multiplayer");$("lobbySearch").focus();$("lobbySearch").scrollIntoView({block:"start"});},
      createProfile:()=>openAccount("create")
    });\n    `;
    html = replaceOnce(html, STARTUP, setup+STARTUP.replace('read(SK.lastView)||"multiplayer"', 'globalThis.GameHubFlagships.initialView(read(SK.lastView),initialParams.has("lobby"))'), 'startup');
    html = replaceOnce(html, '    showFirstVisitProfilePrompt();', '    // Optional signup is now an inline invitation; required account gates are unchanged.', 'optional signup');
    return html;
  }

  let context = null;
  let renderedRecent = '';
  const cardMaps = new Map();

  function syncCard(card, game, state) {
    const favorite = state.favorites.has(game.key);
    const button = card.querySelector('.favorite-btn');
    if (button) {
      button.classList.toggle('active', favorite);
      button.setAttribute('aria-pressed', String(favorite));
      button.textContent = favorite ? '★' : '☆';
    }
    const count = card.querySelector('.play-count');
    if (count) count.textContent = `${Number(state.gamePlayCounts[game.key]) || 0} plays`;
  }

  function featureCard(pick, compact) {
    const card = context.createCard(pick.game);
    card.classList.add('cnm-pick');
    card.style.setProperty('--cnm-accent', pick.color);
    if (!compact && pick.key === PICKS[0].key) card.classList.add('cnm-hero');
    if (compact) card.classList.add('cnm-compact');
    const label = context.document.createElement('p');
    label.className = 'cnm-card-kicker';
    label.textContent = pick.label;
    card.querySelector('.game-content').prepend(label);
    card.querySelector('.game-description').textContent = pick.pitch;
    const single = card.querySelector('[data-game-action="single"]');
    if (single) {
      single.textContent = pick.game.modes.includes('multi') ? 'Play solo' : 'Play now';
      single.setAttribute('aria-label', `${single.textContent}: ${pick.game.title}`);
      single.classList.remove('secondary');
    }
    if (root.GameHubLibrary.getActions(pick.game).some(action => action.type === 'lobby')) {
      const find = context.document.createElement('button');
      find.className = 'btn secondary small cnm-find-lobby';
      find.type = 'button';
      find.textContent = 'Find lobby';
      find.setAttribute('aria-label', `Find a lobby for ${pick.game.title}`);
      find.addEventListener('click', () => context.findLobby(pick.key));
      card.querySelector('.game-actions').appendChild(find);
    }
    const image = card.querySelector('.game-art img');
    if (image && !compact) {
      image.loading = 'eager';
      if (pick.key === PICKS[0].key) image.setAttribute('fetchpriority', 'high');
    }
    return card;
  }

  function refresh() {
    if (!context) return;
    const state = context.state();
    for (const [gridId, compact] of [['cnmFlagshipGrid', false], ['cnmLibraryGrid', true]]) {
      const grid = context.document.getElementById(gridId);
      if (!grid) continue;
      if (!cardMaps.has(gridId)) cardMaps.set(gridId, new Map());
      const cards = cardMaps.get(gridId);
      for (const pick of selectPicks(context.games)) {
        if (!cards.has(pick.key)) {
          const card = featureCard(pick, compact);
          cards.set(pick.key, card);
          grid.appendChild(card);
        }
        syncCard(cards.get(pick.key), pick.game, state);
      }
    }
    const keys = recentKeys(state.recentGames, context.games);
    const signature = keys.join('|');
    const grid = context.document.getElementById('cnmRecentGrid');
    if (signature !== renderedRecent) {
      renderedRecent = signature;
      grid.replaceChildren(...keys.map(key => context.createCard(context.games.find(game => game.key === key))));
    }
    for (const card of grid.children) {
      const game = context.games.find(item => item.key === card.dataset.gameKey);
      if (game) syncCard(card, game, state);
    }
    context.document.getElementById('cnmRecent').hidden = !keys.length;
    context.document.getElementById('cnmProfileOffer').hidden = !!state.username;
  }

  function filterLibrary(active) {
    root.document?.body?.classList.toggle('cnm-filtered', !!active);
  }

  function viewChanged(view) {
    if (!context) return;
    const doc = context.document;
    for (const button of doc.querySelectorAll('.main-nav [data-view],.mobile-nav [data-view]')) {
      if (button.dataset.view === view) button.setAttribute('aria-current', 'page');
      else button.removeAttribute('aria-current');
    }
  }

  function revealLobby() {
    const panel = context?.document.getElementById('currentLobbyPanel');
    if (!panel) return;
    panel.tabIndex = -1;
    panel.focus({preventScroll:true});
    panel.scrollIntoView({block:'start', behavior:'auto'});
  }

  function mount(hooks) {
    if (context) return; // Repeated refreshes must not duplicate listeners/cards.
    context = hooks;
    const doc = hooks.document;
    doc.body.classList.add('cnm-layout');
    const hero = doc.getElementById('featuredHero');
    if (hero) doc.getElementById('cnmMoreMount').appendChild(hero);
    // Put search before recents/favorites without replacing any original control.
    const library = doc.getElementById('view-singleplayer');
    const picks = doc.getElementById('cnmLibraryPicks');
    const heading = [...library.children].find(node => node.classList.contains('section-head'));
    const toolbar = doc.getElementById('gameSearch').closest('.toolbar');
    const categories = doc.getElementById('categoryPills');
    if (heading && toolbar && categories) {
      heading.after(toolbar);
      toolbar.after(categories);
      categories.after(picks);
    }
    doc.getElementById('cnmSearchForm').addEventListener('submit', event => {
      event.preventDefault();
      hooks.browse(doc.getElementById('cnmSearch').value.trim());
    });
    doc.getElementById('cnmCreateProfile').addEventListener('click', hooks.createProfile);
    refresh();
  }

  const api = Object.freeze({PICKS, MARKER, STARTUP, upgradeHtml, initialView, selectPicks, recentKeys, mount, refresh, filterLibrary, viewChanged, revealLobby});
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.GameHubFlagships = api;
})(globalThis);
